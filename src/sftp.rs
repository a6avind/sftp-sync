//! Ready-made SFTP backend (`sftp` feature): [`russh`] transport plus a built-in
//! SFTP client, no external SFTP-protocol dependency. [`SftpFs`] implements
//! [`RemoteFs`] over a live session — connect with [`SftpFs::connect`] (password
//! / private-key) or wrap your own with [`SftpFs::from_session`].

use std::sync::Arc;

use russh::client;
use russh::keys::ssh_key::HashAlg;
use russh::keys::{PrivateKeyWithHashAlg, load_secret_key};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::proto::client::SftpSession;
use crate::proto::protocol::{FileAttributes, FileType, OpenFlags};
use crate::{RemoteEntry, RemoteFs, SyncError};

fn be(e: impl std::fmt::Display) -> SyncError {
    SyncError::backend(e)
}

fn remote_entry(name: String, m: FileAttributes, ft: FileType) -> Result<RemoteEntry, SyncError> {
    if ft.is_other() {
        return Err(be(format!("unrecognized file type for {name:?}")));
    }
    let (mtime, size) = if ft.is_dir() || ft.is_symlink() {
        (m.mtime.unwrap_or(0), m.size.unwrap_or(0))
    } else {
        (
            m.mtime
                .ok_or_else(|| be(format!("missing mtime for {name:?}")))?,
            m.size
                .ok_or_else(|| be(format!("missing size for {name:?}")))?,
        )
    };
    Ok(RemoteEntry {
        is_dir: ft.is_dir(),
        is_symlink: ft.is_symlink(),
        mtime: i64::from(mtime),
        size,
        name,
    })
}

fn sftp_mtime(mtime: i64) -> Result<u32, SyncError> {
    u32::try_from(mtime).map_err(|_| be("mtime is outside the SFTP v3 timestamp range"))
}

pub enum Auth {
    Password(String),
    /// OpenSSH/PEM private-key file, with optional passphrase.
    KeyFile {
        path: std::path::PathBuf,
        passphrase: Option<String>,
    },
}

pub enum HostKey {
    /// The server's `SHA256:…` fingerprint (`ssh-keygen -lf` / `known_hosts`);
    /// connection refused on mismatch.
    Fingerprint(String),
    /// Accept any host key. MITM-prone — trusted paths only.
    AcceptAny,
}

struct Verifier {
    expected: HostKey,
}

impl client::Handler for Verifier {
    type Error = russh::Error;
    async fn check_server_key(
        &mut self,
        key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        let key = key.public_key();
        Ok(match &self.expected {
            HostKey::AcceptAny => true,
            HostKey::Fingerprint(fp) => key.fingerprint(HashAlg::Sha256).to_string() == *fp,
        })
    }
}

/// An SFTP-backed [`RemoteFs`]. Holds the SSH session alive for the sync's lifetime.
pub struct SftpFs {
    sftp: SftpSession,
    #[allow(dead_code)] // kept solely to keep the underlying SSH connection open
    keepalive: Box<dyn Send + Sync>,
}

impl SftpFs {
    /// Wrap an established [`SftpSession`], for custom transport/auth/host-key
    /// handling. `keepalive` owns the transport (e.g. the russh `Handle`) and is
    /// held for the [`SftpFs`]'s lifetime to keep the connection open.
    pub fn from_session(sftp: SftpSession, keepalive: impl Send + Sync + 'static) -> Self {
        SftpFs {
            sftp,
            keepalive: Box::new(keepalive),
        }
    }

    /// Connect, authenticate, and open the SFTP subsystem. For host-key handling
    /// beyond fingerprint pinning, drive russh yourself and use [`from_session`].
    ///
    /// [`from_session`]: SftpFs::from_session
    pub async fn connect(
        addr: impl tokio::net::ToSocketAddrs,
        user: &str,
        auth: Auth,
        host_key: HostKey,
    ) -> Result<Self, SyncError> {
        let config = Arc::new(client::Config::default());
        let mut handle = client::connect(config, addr, Verifier { expected: host_key })
            .await
            .map_err(be)?;

        let ok = match auth {
            Auth::Password(pw) => handle
                .authenticate_password(user, pw)
                .await
                .map_err(be)?
                .success(),
            Auth::KeyFile { path, passphrase } => {
                let key = load_secret_key(path, passphrase.as_deref()).map_err(be)?;
                handle
                    .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), None))
                    .await
                    .map_err(be)?
                    .success()
            }
        };
        if !ok {
            return Err(SyncError::backend("sftp authentication failed"));
        }

        let channel = handle.channel_open_session().await.map_err(be)?;
        channel.request_subsystem(true, "sftp").await.map_err(be)?;
        let sftp = SftpSession::new(channel.into_stream()).await.map_err(be)?;
        Ok(SftpFs {
            sftp,
            keepalive: Box::new(handle),
        })
    }
}

impl RemoteFs for SftpFs {
    async fn read_dir(&self, path: &str) -> Result<Vec<RemoteEntry>, SyncError> {
        let mut out = Vec::new();
        for e in self.sftp.read_dir(path).await.map_err(be)? {
            let name = e.file_name();
            if name == "." || name == ".." {
                continue;
            }
            let m = e.metadata();
            // Type via the DirEntry (longname fallback) not the attrs alone, so a
            // server that omits PERMISSIONS doesn't get every subdir misread as a file.
            let ft = e.file_type();
            out.push(remote_entry(name, m, ft)?);
        }
        Ok(out)
    }

    async fn open_read(&self, path: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, SyncError> {
        let f = self
            .sftp
            .open_with_flags(path, OpenFlags::READ)
            .await
            .map_err(be)?;
        Ok(Box::new(f))
    }

    async fn create_new(
        &self,
        path: &str,
    ) -> Result<Box<dyn AsyncWrite + Send + Unpin>, SyncError> {
        let f = self
            .sftp
            .open_with_flags(
                path,
                OpenFlags::WRITE | OpenFlags::CREATE | OpenFlags::EXCLUDE,
            )
            .await
            .map_err(be)?;
        Ok(Box::new(f))
    }

    async fn set_mtime(&self, path: &str, mtime: i64) -> Result<(), SyncError> {
        // SFTP v3 sends atime and mtime together.
        let secs = sftp_mtime(mtime)?;
        let mut attrs = FileAttributes::empty();
        attrs.atime = Some(secs);
        attrs.mtime = Some(secs);
        self.sftp.set_metadata(path, attrs).await.map_err(be)?;
        Ok(())
    }

    async fn remove_file(&self, path: &str) -> Result<(), SyncError> {
        self.sftp.remove_file(path).await.map_err(be)
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), SyncError> {
        self.sftp.rename(from, to).await.map_err(be)
    }

    /// Non-recursive (parents must exist); generic SFTP v3 failures are accepted
    /// only when STAT confirms that the existing path is actually a directory.
    async fn mkdir(&self, path: &str) -> Result<(), SyncError> {
        use crate::proto::client::error::Error as E;
        use crate::proto::protocol::StatusCode;
        match self.sftp.create_dir(path).await {
            Ok(()) => Ok(()),
            Err(E::Status(s)) if s.status_code == StatusCode::Failure => {
                match self.sftp.metadata(path).await {
                    Ok(attrs) if attrs.is_dir() => Ok(()),
                    Ok(_) => Err(SyncError::backend(format!(
                        "{path} exists and is not a directory"
                    ))),
                    Err(e) => Err(SyncError::backend(format!("mkdir {path}: {e}"))),
                }
            }
            Err(e) => Err(be(e)),
        }
    }
}

#[cfg(feature = "check-file")]
impl SftpFs {
    /// Server-computed SHA-256 of a remote file (no download), via the
    /// `check-file` extension. `Ok(None)` if the server doesn't advertise it
    /// (e.g. stock OpenSSH). For content-drift detection or transfer verify.
    pub async fn remote_sha256(&self, path: &str) -> Result<Option<[u8; 32]>, SyncError> {
        use crate::proto::client::error::Error as E;
        match self.sftp.check_file(path, "sha256", 0, 0, 0).await {
            Ok(cf) => validate_sha256(cf).map(Some),
            Err(E::Unsupported(_)) => Ok(None),
            Err(e) => Err(SyncError::backend(e)),
        }
    }
}

#[cfg(feature = "check-file")]
fn validate_sha256(cf: crate::proto::protocol::CheckFile) -> Result<[u8; 32], SyncError> {
    if cf.hash_algorithm != "sha256" {
        return Err(be(format!(
            "check-file returned unexpected algorithm {:?}, expected sha256",
            cf.hash_algorithm
        )));
    }
    let len = cf.hashes.len();
    cf.hashes
        .try_into()
        .map_err(|_| be(format!("check-file returned {len} hash bytes, expected 32")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_requires_known_type_and_metadata() {
        let attrs = FileAttributes {
            mtime: Some(0),
            size: Some(0),
            ..FileAttributes::empty()
        };
        assert!(remote_entry("file".into(), attrs, FileType::File).is_ok());
        assert!(remote_entry("file".into(), attrs, FileType::Other).is_err());
        assert!(
            remote_entry(
                "file".into(),
                FileAttributes {
                    mtime: None,
                    ..attrs
                },
                FileType::File
            )
            .is_err()
        );
        assert!(
            remote_entry(
                "file".into(),
                FileAttributes {
                    size: None,
                    ..attrs
                },
                FileType::File
            )
            .is_err()
        );
    }

    #[test]
    fn mtime_rejects_values_outside_wire_range() {
        assert!(sftp_mtime(-1).is_err());
        assert_eq!(sftp_mtime(42).unwrap(), 42);
        assert!(sftp_mtime(i64::MAX).is_err());
    }

    #[cfg(feature = "check-file")]
    #[test]
    fn sha256_requires_exact_algorithm_and_length() {
        use crate::proto::protocol::CheckFile;
        let check = |algorithm: &str, len| CheckFile {
            hash_algorithm: algorithm.into(),
            hashes: vec![7; len],
        };
        assert_eq!(validate_sha256(check("sha256", 32)).unwrap(), [7; 32]);
        assert!(validate_sha256(check("sha1", 32)).is_err());
        assert!(validate_sha256(check("sha256", 31)).is_err());
        assert!(validate_sha256(check("sha256", 33)).is_err());
    }
}
