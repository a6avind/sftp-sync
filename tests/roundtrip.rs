//! End-to-end tests: drive `sync_once` against a `LocalFs` implementation of
//! `RemoteFs` (the "remote" is just a second real directory). This exercises the
//! whole scan/push/pull/apply/mtime path without any SFTP — the backend seam is a
//! trait, so tests supply the simplest possible backend.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use sftp_sync::{RemoteEntry, RemoteFs, SyncError};
use tokio::io::{AsyncRead, AsyncWrite};

struct LocalFs;

struct CountingFs {
    unrelated_reads: AtomicU32,
}

impl RemoteFs for CountingFs {
    async fn read_dir(&self, path: &str) -> Result<Vec<RemoteEntry>, SyncError> {
        if path.ends_with("/unrelated") {
            self.unrelated_reads.fetch_add(1, Ordering::Relaxed);
        }
        LocalFs.read_dir(path).await
    }
    async fn open_read(&self, path: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, SyncError> {
        LocalFs.open_read(path).await
    }
    async fn create_new(
        &self,
        path: &str,
    ) -> Result<Box<dyn AsyncWrite + Send + Unpin>, SyncError> {
        LocalFs.create_new(path).await
    }
    async fn set_mtime(&self, path: &str, mtime: i64) -> Result<(), SyncError> {
        LocalFs.set_mtime(path, mtime).await
    }
    async fn remove_file(&self, path: &str) -> Result<(), SyncError> {
        LocalFs.remove_file(path).await
    }
    async fn rename(&self, from: &str, to: &str) -> Result<(), SyncError> {
        LocalFs.rename(from, to).await
    }
    async fn mkdir(&self, path: &str) -> Result<(), SyncError> {
        LocalFs.mkdir(path).await
    }
}

// Mutate after returning the first remote directory snapshot, i.e. after the
// local scan and before the action's validation.
struct ChangeAfterScan {
    target: PathBuf,
    fired: std::sync::atomic::AtomicBool,
}

struct ChangeDuringTransfer {
    target: PathBuf,
    during_pull: bool,
}

struct CorruptStaging;

impl RemoteFs for CorruptStaging {
    async fn read_dir(&self, path: &str) -> Result<Vec<RemoteEntry>, SyncError> {
        LocalFs.read_dir(path).await
    }
    async fn open_read(&self, path: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, SyncError> {
        if path.ends_with(".sync-tmp") {
            Ok(Box::new(&b"evil"[..]))
        } else {
            LocalFs.open_read(path).await
        }
    }
    async fn create_new(
        &self,
        path: &str,
    ) -> Result<Box<dyn AsyncWrite + Send + Unpin>, SyncError> {
        LocalFs.create_new(path).await
    }
    async fn set_mtime(&self, path: &str, mtime: i64) -> Result<(), SyncError> {
        LocalFs.set_mtime(path, mtime).await
    }
    async fn remove_file(&self, path: &str) -> Result<(), SyncError> {
        LocalFs.remove_file(path).await
    }
    async fn rename(&self, from: &str, to: &str) -> Result<(), SyncError> {
        LocalFs.rename(from, to).await
    }
    async fn mkdir(&self, path: &str) -> Result<(), SyncError> {
        LocalFs.mkdir(path).await
    }
}

impl RemoteFs for ChangeDuringTransfer {
    async fn read_dir(&self, path: &str) -> Result<Vec<RemoteEntry>, SyncError> {
        LocalFs.read_dir(path).await
    }
    async fn open_read(&self, path: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, SyncError> {
        let file = LocalFs.open_read(path).await?;
        if self.during_pull {
            std::fs::write(&self.target, "concurrent local edit").unwrap();
        }
        Ok(file)
    }
    async fn create_new(
        &self,
        path: &str,
    ) -> Result<Box<dyn AsyncWrite + Send + Unpin>, SyncError> {
        LocalFs.create_new(path).await
    }
    async fn set_mtime(&self, path: &str, mtime: i64) -> Result<(), SyncError> {
        LocalFs.set_mtime(path, mtime).await?;
        if !self.during_pull {
            std::fs::write(&self.target, "concurrent remote edit").unwrap();
        }
        Ok(())
    }
    async fn remove_file(&self, path: &str) -> Result<(), SyncError> {
        LocalFs.remove_file(path).await
    }
    async fn rename(&self, from: &str, to: &str) -> Result<(), SyncError> {
        LocalFs.rename(from, to).await
    }
    async fn mkdir(&self, path: &str) -> Result<(), SyncError> {
        LocalFs.mkdir(path).await
    }
}

impl RemoteFs for ChangeAfterScan {
    async fn read_dir(&self, path: &str) -> Result<Vec<RemoteEntry>, SyncError> {
        let entries = LocalFs.read_dir(path).await?;
        if !self.fired.swap(true, Ordering::SeqCst) {
            std::fs::write(&self.target, "concurrent change").unwrap();
        }
        Ok(entries)
    }
    async fn open_read(&self, path: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, SyncError> {
        LocalFs.open_read(path).await
    }
    async fn create_new(
        &self,
        path: &str,
    ) -> Result<Box<dyn AsyncWrite + Send + Unpin>, SyncError> {
        LocalFs.create_new(path).await
    }
    async fn set_mtime(&self, path: &str, mtime: i64) -> Result<(), SyncError> {
        LocalFs.set_mtime(path, mtime).await
    }
    async fn remove_file(&self, path: &str) -> Result<(), SyncError> {
        LocalFs.remove_file(path).await
    }
    async fn rename(&self, from: &str, to: &str) -> Result<(), SyncError> {
        LocalFs.rename(from, to).await
    }
    async fn mkdir(&self, path: &str) -> Result<(), SyncError> {
        LocalFs.mkdir(path).await
    }
}

impl RemoteFs for LocalFs {
    async fn read_dir(&self, path: &str) -> Result<Vec<RemoteEntry>, SyncError> {
        let mut out = Vec::new();
        for e in std::fs::read_dir(path).map_err(SyncError::backend)? {
            let e = e.map_err(SyncError::backend)?;
            let ft = e.file_type().map_err(SyncError::backend)?; // does not follow symlinks
            let meta = e.metadata().map_err(SyncError::backend)?;
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            out.push(RemoteEntry {
                name: e.file_name().to_string_lossy().into_owned(),
                is_dir: ft.is_dir(),
                is_symlink: ft.is_symlink(),
                mtime,
                size: meta.len(),
            });
        }
        Ok(out)
    }
    async fn open_read(&self, path: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, SyncError> {
        Ok(Box::new(
            tokio::fs::File::open(path)
                .await
                .map_err(SyncError::backend)?,
        ))
    }
    async fn create_new(
        &self,
        path: &str,
    ) -> Result<Box<dyn AsyncWrite + Send + Unpin>, SyncError> {
        Ok(Box::new(
            tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .await
                .map_err(SyncError::backend)?,
        ))
    }
    async fn set_mtime(&self, path: &str, mtime: i64) -> Result<(), SyncError> {
        set_mtime(Path::new(path), mtime);
        Ok(())
    }
    async fn remove_file(&self, path: &str) -> Result<(), SyncError> {
        std::fs::remove_file(path).map_err(SyncError::backend)
    }
    async fn rename(&self, from: &str, to: &str) -> Result<(), SyncError> {
        std::fs::hard_link(from, to).map_err(SyncError::backend)?;
        std::fs::remove_file(from).map_err(SyncError::backend)
    }
    async fn mkdir(&self, path: &str) -> Result<(), SyncError> {
        let _ = std::fs::create_dir(path);
        Ok(())
    }
}

/// `LocalFs` but every `create` (push write) fails — models a flaky remote.
struct FailingPush;

impl RemoteFs for FailingPush {
    async fn read_dir(&self, path: &str) -> Result<Vec<RemoteEntry>, SyncError> {
        LocalFs.read_dir(path).await
    }
    async fn open_read(&self, path: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, SyncError> {
        LocalFs.open_read(path).await
    }
    async fn create_new(
        &self,
        _path: &str,
    ) -> Result<Box<dyn AsyncWrite + Send + Unpin>, SyncError> {
        Err(SyncError::backend(std::io::Error::other("push disabled")))
    }
    async fn set_mtime(&self, path: &str, mtime: i64) -> Result<(), SyncError> {
        LocalFs.set_mtime(path, mtime).await
    }
    async fn remove_file(&self, path: &str) -> Result<(), SyncError> {
        LocalFs.remove_file(path).await
    }
    async fn rename(&self, from: &str, to: &str) -> Result<(), SyncError> {
        LocalFs.rename(from, to).await
    }
    async fn mkdir(&self, path: &str) -> Result<(), SyncError> {
        LocalFs.mkdir(path).await
    }
}

/// Fails only when publishing the staged file over `a.txt`; moving the original
/// aside and restoring it remain available.
struct FailPublish;

impl RemoteFs for FailPublish {
    async fn read_dir(&self, path: &str) -> Result<Vec<RemoteEntry>, SyncError> {
        LocalFs.read_dir(path).await
    }
    async fn open_read(&self, path: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, SyncError> {
        LocalFs.open_read(path).await
    }
    async fn create_new(
        &self,
        path: &str,
    ) -> Result<Box<dyn AsyncWrite + Send + Unpin>, SyncError> {
        LocalFs.create_new(path).await
    }
    async fn set_mtime(&self, path: &str, mtime: i64) -> Result<(), SyncError> {
        LocalFs.set_mtime(path, mtime).await
    }
    async fn remove_file(&self, path: &str) -> Result<(), SyncError> {
        LocalFs.remove_file(path).await
    }
    async fn rename(&self, from: &str, to: &str) -> Result<(), SyncError> {
        if to.ends_with("/a.txt")
            && from.ends_with(".sync-tmp")
            && !from.ends_with(".sync-backup.sync-tmp")
        {
            return Err(SyncError::backend("simulated publish failure"));
        }
        LocalFs.rename(from, to).await
    }
    async fn mkdir(&self, path: &str) -> Result<(), SyncError> {
        LocalFs.mkdir(path).await
    }
}

/// A hostile backend that injects unsafe directory-entry names.
struct HostileFs;

impl RemoteFs for HostileFs {
    async fn read_dir(&self, _path: &str) -> Result<Vec<RemoteEntry>, SyncError> {
        let bad = |n: &str| RemoteEntry {
            name: n.into(),
            is_dir: false,
            is_symlink: false,
            mtime: 1,
            size: 1,
        };
        Ok(vec![
            bad("."),
            bad(".."),
            bad("../escape"),
            bad("a/b"),
            bad("ok.txt"),
        ])
    }
    async fn open_read(&self, _p: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, SyncError> {
        Ok(Box::new(&b"x"[..]))
    }
    async fn create_new(&self, _p: &str) -> Result<Box<dyn AsyncWrite + Send + Unpin>, SyncError> {
        Ok(Box::new(Vec::new()))
    }
    async fn set_mtime(&self, _p: &str, _m: i64) -> Result<(), SyncError> {
        Ok(())
    }
    async fn remove_file(&self, _p: &str) -> Result<(), SyncError> {
        Ok(())
    }
    async fn rename(&self, _f: &str, _t: &str) -> Result<(), SyncError> {
        Ok(())
    }
    async fn mkdir(&self, _p: &str) -> Result<(), SyncError> {
        Ok(())
    }
}

static SEQ: AtomicU32 = AtomicU32::new(0);

fn tmpdir(tag: &str) -> PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let d = std::env::temp_dir().join(format!("sftp-sync-{}-{n}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn set_mtime(p: &Path, secs: i64) {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(p.as_os_str().as_bytes()).unwrap();
    let t = libc::timespec {
        tv_sec: secs as libc::time_t,
        tv_nsec: 0,
    };
    let times = [t, t];
    unsafe {
        libc::utimensat(libc::AT_FDCWD, c.as_ptr(), times.as_ptr(), 0);
    }
}

fn put(dir: &Path, rel: &str, content: &str, mtime: i64) {
    let p = dir.join(rel);
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&p, content).unwrap();
    set_mtime(&p, mtime);
}

fn read(dir: &Path, rel: &str) -> Option<String> {
    std::fs::read_to_string(dir.join(rel)).ok()
}

async fn sync(
    remote: &Path,
    local: &Path,
    base: &sftp_sync::Tree,
    del: bool,
) -> sftp_sync::Outcome {
    let opts = sftp_sync::SyncOptions {
        allow_delete: del,
        ..Default::default()
    };
    sftp_sync::sync_once(
        &LocalFs,
        remote.to_str().unwrap(),
        local,
        base,
        &opts,
        "testhost",
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn first_run_unions_both_sides() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "only_local.txt", "L", 1000);
    put(&remote, "only_remote.txt", "R", 1000);

    let out = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;

    assert_eq!(
        read(&remote, "only_local.txt").as_deref(),
        Some("L"),
        "local pushed"
    );
    assert_eq!(
        read(&local, "only_remote.txt").as_deref(),
        Some("R"),
        "remote pulled"
    );
    assert_eq!(out.baseline.len(), 2);
}

#[tokio::test]
async fn changed_destination_during_pull_is_not_overwritten() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&remote, "file", "remote content", 1000);
    let fs = ChangeDuringTransfer {
        target: local.join("file"),
        during_pull: true,
    };
    let out = sftp_sync::sync_once(
        &fs,
        remote.to_str().unwrap(),
        &local,
        &sftp_sync::Tree::new(),
        &Default::default(),
        "h",
    )
    .await
    .unwrap();
    assert_eq!(out.errors.len(), 1);
    assert_eq!(
        read(&local, "file").as_deref(),
        Some("concurrent local edit")
    );
}

#[tokio::test]
async fn changed_destination_during_push_is_not_overwritten() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "file", "local content", 1000);
    let fs = ChangeDuringTransfer {
        target: remote.join("file"),
        during_pull: false,
    };
    let out = sftp_sync::sync_once(
        &fs,
        remote.to_str().unwrap(),
        &local,
        &sftp_sync::Tree::new(),
        &Default::default(),
        "h",
    )
    .await
    .unwrap();
    assert_eq!(out.errors.len(), 1);
    assert_eq!(
        read(&remote, "file").as_deref(),
        Some("concurrent remote edit")
    );
}

#[tokio::test]
async fn matching_files_enter_baseline_and_deletions_propagate() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "same.txt", "same", 1000);
    put(&remote, "same.txt", "same", 1000);
    let out = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;
    assert_eq!(out.changed, 0);
    assert!(out.baseline.contains_key("same.txt"));
    std::fs::remove_file(local.join("same.txt")).unwrap();
    sync(&remote, &local, &out.baseline, true).await;
    assert!(!remote.join("same.txt").exists());
}

#[tokio::test]
async fn missing_established_remote_root_does_not_delete_local_files() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "precious", "keep", 1000);
    let first = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;
    std::fs::remove_file(remote.join("precious")).unwrap();
    std::fs::remove_dir(&remote).unwrap();
    let result = sftp_sync::sync_once(
        &LocalFs,
        remote.to_str().unwrap(),
        &local,
        &first.baseline,
        &Default::default(),
        "h",
    )
    .await;
    assert!(result.is_err());
    assert!(!remote.exists());
    assert_eq!(read(&local, "precious").as_deref(), Some("keep"));
}

#[tokio::test]
async fn exclusive_remote_create_refuses_existing_symlinks() {
    let remote = tmpdir("r");
    let canary = remote.join("canary");
    std::fs::write(&canary, "safe").unwrap();
    let staging = remote.join("staging.sync-tmp");
    std::os::unix::fs::symlink(&canary, &staging).unwrap();
    assert!(LocalFs.create_new(staging.to_str().unwrap()).await.is_err());
    assert_eq!(std::fs::read_to_string(canary).unwrap(), "safe");
}

#[tokio::test]
async fn unfinished_remote_backup_stops_reconciliation() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "file", "keep", 1000);
    put(&remote, "file.123.sync-backup.sync-tmp", "old", 1000);
    let mut baseline = sftp_sync::Tree::new();
    baseline.insert(
        "file".into(),
        sftp_sync::Meta {
            mtime: 1000,
            size: 4,
            sha256: None,
        },
    );
    let opts = sftp_sync::SyncOptions {
        allow_delete: true,
        ..Default::default()
    };
    let result = sftp_sync::sync_once(
        &LocalFs,
        remote.to_str().unwrap(),
        &local,
        &baseline,
        &opts,
        "h",
    )
    .await;
    assert!(result.is_err());
    assert_eq!(read(&local, "file").as_deref(), Some("keep"));
}

#[test]
fn baseline_errors_are_reported() {
    let root = tmpdir("state");
    let path = root.join("baseline.json");
    assert!(sftp_sync::load_baseline(&path).unwrap().is_empty());
    std::fs::write(&path, "not json").unwrap();
    assert!(sftp_sync::load_baseline(&path).is_err());
    assert!(sftp_sync::save_baseline(&root, &sftp_sync::Tree::new()).is_err());
    sftp_sync::save_baseline(&path, &sftp_sync::Tree::new()).unwrap();
    assert!(sftp_sync::load_baseline(&path).unwrap().is_empty());
}

#[tokio::test]
async fn symlinked_directory_shadows_descendants() {
    let (local, remote, outside) = (tmpdir("l"), tmpdir("r"), tmpdir("outside"));
    put(&remote, "dir/file", "remote", 1000);
    put(&outside, "file", "outside", 1000);
    std::os::unix::fs::symlink(&outside, local.join("dir")).unwrap();
    let out = sync(&remote, &local, &sftp_sync::Tree::new(), false).await;
    assert_eq!(out.changed, 0);
    assert_eq!(read(&outside, "file").as_deref(), Some("outside"));
    assert_eq!(read(&remote, "dir/file").as_deref(), Some("remote"));
}

#[tokio::test]
async fn steady_state_is_noop_mtime_preserved() {
    // The key risk: a second clean pass must do NOTHING (mtime preserved), else
    // it re-copies forever.
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "a.txt", "hello", 1234);

    let out = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;
    assert_eq!(read(&remote, "a.txt").as_deref(), Some("hello"));

    let out2 = sync(&remote, &local, &out.baseline, true).await;
    assert_eq!(out2.changed, 0, "clean second pass does nothing");
}

#[tokio::test]
async fn local_edit_pushes_remote_edit_pulls() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "a.txt", "v1", 1000);
    put(&local, "b.txt", "v1", 1000);
    let out = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;

    put(&local, "a.txt", "v2-local", 2000);
    put(&remote, "b.txt", "v2-remote", 2000);
    sync(&remote, &local, &out.baseline, true).await;

    assert_eq!(read(&remote, "a.txt").as_deref(), Some("v2-local"));
    assert_eq!(read(&local, "b.txt").as_deref(), Some("v2-remote"));
}

#[tokio::test]
async fn same_size_and_mtime_edit_is_detected() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "file", "old!", 1000);
    let first = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;
    put(&local, "file", "new!", 1000);
    let out = sync(&remote, &local, &first.baseline, true).await;
    assert!(out.errors.is_empty());
    assert_eq!(read(&remote, "file").as_deref(), Some("new!"));
}

#[tokio::test]
async fn simultaneous_same_metadata_edits_keep_both_versions() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "file", "base", 1000);
    let first = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;
    put(&local, "file", "left", 1000);
    put(&remote, "file", "rght", 1000);
    let out = sync(&remote, &local, &first.baseline, true).await;
    assert!(out.errors.is_empty());
    assert_eq!(out.report.conflicts, 1);
    assert_eq!(read(&local, "file").as_deref(), Some("left"));
    assert_eq!(read(&remote, "file").as_deref(), Some("left"));
    let copy = std::fs::read_dir(&local)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("file.conflict-")
        })
        .unwrap();
    assert_eq!(std::fs::read_to_string(copy).unwrap(), "rght");
}

#[tokio::test]
async fn corrupt_staging_content_is_not_published() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "file", "good", 1000);
    let out = sftp_sync::sync_once(
        &CorruptStaging,
        remote.to_str().unwrap(),
        &local,
        &sftp_sync::Tree::new(),
        &Default::default(),
        "h",
    )
    .await
    .unwrap();
    assert_eq!(out.errors.len(), 1);
    assert!(!remote.join("file").exists());
    assert!(!out.baseline.contains_key("file"));
}

#[tokio::test]
async fn legacy_baseline_without_hash_does_not_delete_surviving_file() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "file", "safe", 1000);
    let mut baseline = sftp_sync::Tree::new();
    baseline.insert(
        "file".into(),
        sftp_sync::Meta {
            mtime: 1000,
            size: 4,
            sha256: None,
        },
    );
    let out = sync(&remote, &local, &baseline, true).await;
    assert!(out.errors.is_empty());
    assert_eq!(read(&local, "file").as_deref(), Some("safe"));
    assert_eq!(read(&remote, "file").as_deref(), Some("safe"));
}

#[tokio::test]
async fn delete_propagates_after_baseline() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "a.txt", "x", 1000);
    let out = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;
    assert!(read(&remote, "a.txt").is_some());

    std::fs::remove_file(local.join("a.txt")).unwrap();
    sync(&remote, &local, &out.baseline, true).await;

    assert!(read(&remote, "a.txt").is_none(), "local delete propagated");
}

#[tokio::test]
async fn conflict_keeps_both() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "a.txt", "base", 1000);
    let out = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;

    put(&local, "a.txt", "local-newer", 3000);
    put(&remote, "a.txt", "remote-older", 2000);
    sync(&remote, &local, &out.baseline, true).await;

    assert_eq!(
        read(&remote, "a.txt").as_deref(),
        Some("local-newer"),
        "newer wins the name"
    );
    let has_conflict = std::fs::read_dir(&local)
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().contains(".conflict-"));
    assert!(has_conflict, "loser preserved as a .conflict- copy");
}

#[tokio::test]
async fn nested_dirs_sync() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "src/deep/a.rs", "fn main(){}", 1000);
    sync(&remote, &local, &sftp_sync::Tree::new(), true).await;
    assert_eq!(
        read(&remote, "src/deep/a.rs").as_deref(),
        Some("fn main(){}"),
        "nested path recreated remotely"
    );
}

#[tokio::test]
async fn dry_run_changes_nothing() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "a.txt", "L", 1000);
    let opts = sftp_sync::SyncOptions {
        dry_run: true,
        ..Default::default()
    };
    let out = sftp_sync::sync_once(
        &LocalFs,
        remote.to_str().unwrap(),
        &local,
        &sftp_sync::Tree::new(),
        &opts,
        "h",
    )
    .await
    .unwrap();

    assert_eq!(out.changed, 0);
    assert_eq!(out.report.pushed, 1, "reports intended push");
    assert!(read(&remote, "a.txt").is_none(), "applies nothing");
}

#[tokio::test]
async fn push_only_ignores_remote_side() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "mine.txt", "L", 1000);
    put(&remote, "theirs.txt", "R", 1000);
    let opts = sftp_sync::SyncOptions {
        direction: sftp_sync::Direction::PushOnly,
        ..Default::default()
    };
    let out = sftp_sync::sync_once(
        &LocalFs,
        remote.to_str().unwrap(),
        &local,
        &sftp_sync::Tree::new(),
        &opts,
        "h",
    )
    .await
    .unwrap();

    assert_eq!(
        read(&remote, "mine.txt").as_deref(),
        Some("L"),
        "local pushed"
    );
    assert!(
        read(&local, "theirs.txt").is_none(),
        "remote-only NOT pulled"
    );
    assert_eq!(out.report.pulled, 0);
}

#[tokio::test]
async fn one_way_restores_authoritative_version_after_destination_edit() {
    for direction in [
        sftp_sync::Direction::PushOnly,
        sftp_sync::Direction::PullOnly,
    ] {
        let (local, remote) = (tmpdir("l"), tmpdir("r"));
        put(&local, "file", "original", 1000);
        let first = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;
        let destination = if direction == sftp_sync::Direction::PushOnly {
            &remote
        } else {
            &local
        };
        put(destination, "file", "edited", 2000);
        let opts = sftp_sync::SyncOptions {
            direction,
            ..Default::default()
        };
        let out = sftp_sync::sync_once(
            &LocalFs,
            remote.to_str().unwrap(),
            &local,
            &first.baseline,
            &opts,
            "h",
        )
        .await
        .unwrap();
        assert!(out.errors.is_empty(), "{:?}", out.errors);
        assert_eq!(read(destination, "file").as_deref(), Some("original"));
        assert_eq!(out.changed, 1);
    }
}

#[tokio::test]
async fn failed_push_does_not_poison_baseline() {
    // Regression: a failed transfer must not be recorded as synced. If it were,
    // the next reconcile would read "local exists, remote doesn't" as a remote
    // deletion and delete the local file — permanent data loss from one hiccup.
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "keep.txt", "precious", 1000);

    let opts = sftp_sync::SyncOptions::default();
    let out = sftp_sync::sync_once(
        &FailingPush,
        remote.to_str().unwrap(),
        &local,
        &sftp_sync::Tree::new(),
        &opts,
        "h",
    )
    .await
    .unwrap();
    assert!(
        !out.baseline.contains_key("keep.txt"),
        "failed push must not enter the baseline"
    );
    assert_eq!(
        out.errors.len(),
        1,
        "the failed action is returned to the caller"
    );

    // Next run with a working backend + deletes on: the file must be pushed, never deleted.
    let out2 = sync(&remote, &local, &out.baseline, true).await;
    assert_eq!(
        read(&remote, "keep.txt").as_deref(),
        Some("precious"),
        "retried push lands"
    );
    assert_eq!(
        read(&local, "keep.txt").as_deref(),
        Some("precious"),
        "local file survives"
    );
    assert!(out2.baseline.contains_key("keep.txt"));
}

#[tokio::test]
async fn failed_remote_publish_restores_the_previous_file() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "a.txt", "old", 1000);
    let first = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;
    put(&local, "a.txt", "new", 2000);

    let opts = sftp_sync::SyncOptions::default();
    let out = sftp_sync::sync_once(
        &FailPublish,
        remote.to_str().unwrap(),
        &local,
        &first.baseline,
        &opts,
        "testhost",
    )
    .await
    .unwrap();

    assert_eq!(read(&remote, "a.txt").as_deref(), Some("old"));
    assert_eq!(out.errors.len(), 1);
    assert!(
        std::fs::read_dir(&remote)
            .unwrap()
            .filter_map(Result::ok)
            .all(|entry| !entry.file_name().to_string_lossy().ends_with(".sync-tmp"))
    );
}

#[tokio::test]
async fn pull_does_not_follow_a_preexisting_staging_symlink() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    let canary = local
        .parent()
        .unwrap()
        .join(format!("sync-canary-{}", std::process::id()));
    std::fs::write(&canary, "untouched").unwrap();
    std::os::unix::fs::symlink(&canary, local.join("doc.txt.sync-tmp")).unwrap();
    put(&remote, "doc.txt", "downloaded", 1000);

    let out = sync(&remote, &local, &sftp_sync::Tree::new(), false).await;

    assert!(out.errors.is_empty());
    assert_eq!(read(&local, "doc.txt").as_deref(), Some("downloaded"));
    assert_eq!(std::fs::read_to_string(&canary).unwrap(), "untouched");
    assert!(
        local
            .join("doc.txt.sync-tmp")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink()
    );
    let _ = std::fs::remove_file(&canary);
}

#[tokio::test]
async fn sync_does_not_overwrite_symlink_paths() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    let canary = local
        .parent()
        .unwrap()
        .join(format!("sync-link-canary-{}", std::process::id()));
    std::fs::write(&canary, "untouched").unwrap();
    put(&local, "remote-link", "local file", 1000);
    std::os::unix::fs::symlink(&canary, remote.join("remote-link")).unwrap();
    put(&remote, "local-link", "remote file", 1000);
    std::os::unix::fs::symlink(&canary, local.join("local-link")).unwrap();

    let out = sync(&remote, &local, &sftp_sync::Tree::new(), false).await;

    assert!(out.errors.is_empty());
    assert_eq!(std::fs::read_to_string(&canary).unwrap(), "untouched");
    assert!(
        remote
            .join("remote-link")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert!(
        local
            .join("local-link")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(out.changed, 0);
    let _ = std::fs::remove_file(&canary);
}

#[tokio::test]
async fn file_becoming_symlink_does_not_delete_other_side() {
    // Regression: a synced file replaced by a same-named symlink drops out of the
    // scan. Without a shadow guard reconcile reads that as a deletion and wipes the
    // intact, unchanged copy on the other side — permanent data loss.
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "doc.txt", "precious", 1000);
    let out = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;
    assert_eq!(read(&remote, "doc.txt").as_deref(), Some("precious"));

    std::fs::remove_file(local.join("doc.txt")).unwrap();
    std::os::unix::fs::symlink("/etc/hostname", local.join("doc.txt")).unwrap();

    let out2 = sync(&remote, &local, &out.baseline, true).await;
    assert_eq!(
        read(&remote, "doc.txt").as_deref(),
        Some("precious"),
        "intact remote file survives"
    );
    assert_eq!(
        out2.report.deleted_remote, 0,
        "no deletion planned for a symlink-shadowed path"
    );
    assert!(
        out2.baseline.contains_key("doc.txt"),
        "baseline keeps the path"
    );
}

#[tokio::test]
async fn hostile_remote_names_cannot_escape_root() {
    // `.`/`..`/`../escape`/`a/b` from a hostile backend must be dropped; only the
    // safe entry survives, and nothing is written outside the local root.
    let local = tmpdir("l");
    let canary = local.parent().unwrap().join("ESCAPED");
    let _ = std::fs::remove_file(&canary);

    let tree = sftp_sync::scan_remote(&HostileFs, "/base").await.unwrap();
    assert_eq!(
        tree.keys().cloned().collect::<Vec<_>>(),
        vec!["ok.txt".to_string()]
    );

    let opts = sftp_sync::SyncOptions::default();
    sftp_sync::sync_once(
        &HostileFs,
        "/base",
        &local,
        &sftp_sync::Tree::new(),
        &opts,
        "h",
    )
    .await
    .unwrap();
    assert!(!canary.exists(), "no write escaped the local root");
    assert!(local.join("ok.txt").exists(), "safe file pulled");
}

#[tokio::test]
async fn newly_ignored_files_and_directories_do_not_delete_remote() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    std::fs::create_dir(local.join(".git")).unwrap();
    put(&local, "file", "keep", 1000);
    put(&local, "dir/nested", "keep too", 1000);
    let first = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;
    put(&local, ".gitignore", "file\ndir/\n", 1000);
    let opts = sftp_sync::SyncOptions {
        git: true,
        ..Default::default()
    };
    let out = sftp_sync::sync_once(
        &LocalFs,
        remote.to_str().unwrap(),
        &local,
        &first.baseline,
        &opts,
        "h",
    )
    .await
    .unwrap();
    assert_eq!(out.report.deleted_remote, 0);
    assert_eq!(read(&remote, "file").as_deref(), Some("keep"));
    assert_eq!(read(&remote, "dir/nested").as_deref(), Some("keep too"));
    assert!(out.baseline.contains_key("dir/nested"));
}

#[tokio::test]
async fn changed_after_scan_is_not_deleted_or_overwritten() {
    for remote_deleted in [false, true] {
        let (local, remote) = (tmpdir("race-l"), tmpdir("race-r"));
        put(&local, "file", "old", 1000);
        let first = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;
        let (deleted, survivor) = if remote_deleted {
            (&remote, &local)
        } else {
            (&local, &remote)
        };
        std::fs::remove_file(deleted.join("file")).unwrap();
        let fs = ChangeAfterScan {
            target: survivor.join("file"),
            fired: false.into(),
        };
        let out = sftp_sync::sync_once(
            &fs,
            remote.to_str().unwrap(),
            &local,
            &first.baseline,
            &Default::default(),
            "h",
        )
        .await;
        assert_eq!(read(survivor, "file").as_deref(), Some("concurrent change"));
        if let Ok(out) = out {
            assert_eq!(out.changed, 0);
            assert_eq!(out.errors.len(), 1);
            assert_eq!(out.baseline, first.baseline);
        }
    }
}

#[tokio::test]
async fn changed_after_scan_prevents_stale_transfer() {
    for push in [false, true] {
        let (local, remote) = (tmpdir("transfer-l"), tmpdir("transfer-r"));
        put(&local, "file", "old", 1000);
        let first = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;
        let (source, destination) = if push {
            (&local, &remote)
        } else {
            (&remote, &local)
        };
        put(source, "file", "planned update", 2000);
        let fs = ChangeAfterScan {
            target: destination.join("file"),
            fired: false.into(),
        };
        let out = sftp_sync::sync_once(
            &fs,
            remote.to_str().unwrap(),
            &local,
            &first.baseline,
            &Default::default(),
            "h",
        )
        .await;
        assert_eq!(
            read(destination, "file").as_deref(),
            Some("concurrent change")
        );
        if let Ok(out) = out {
            assert_eq!(out.changed, 0);
            assert_eq!(out.errors.len(), 1);
            assert_eq!(out.baseline, first.baseline);
        }
    }
}

#[tokio::test]
async fn git_mode_preserves_remote_only_ignored_paths() {
    let (local, remote) = (tmpdir("git-l"), tmpdir("git-r"));
    std::fs::create_dir(local.join(".git")).unwrap();
    put(&local, ".gitignore", "*.log\ndir/\n", 1000);
    put(&remote, "missing.log", "remote", 1000);
    put(&remote, "dir/file", "remote nested", 1000);
    let mut base = sftp_sync::Tree::new();
    base.insert(
        "missing.log".into(),
        sftp_sync::Meta {
            mtime: 1000,
            size: 6,
            sha256: None,
        },
    );
    let opts = sftp_sync::SyncOptions {
        git: true,
        ..Default::default()
    };
    let out = sftp_sync::sync_once(
        &LocalFs,
        remote.to_str().unwrap(),
        &local,
        &base,
        &opts,
        "h",
    )
    .await
    .unwrap();
    assert_eq!(read(&remote, "missing.log").as_deref(), Some("remote"));
    assert!(!local.join("dir/file").exists());
    assert!(out.baseline.contains_key("missing.log"));
}

#[tokio::test]
async fn git_mode_pulls_unignored_remote_only_paths_and_respects_negation() {
    let (local, remote) = (tmpdir("git-pull-l"), tmpdir("git-pull-r"));
    std::fs::create_dir(local.join(".git")).unwrap();
    put(&local, ".gitignore", "*.log\n!important.log\n", 1000);
    put(&local, "nested/.gitignore", "*.tmp\n!keep.tmp\n", 1000);
    put(&remote, "normal.txt", "normal", 1000);
    put(&remote, "important.log", "allowed", 1000);
    put(&remote, "nested/keep.tmp", "nested allowed", 1000);
    put(&remote, "nested/drop.tmp", "nested ignored", 1000);
    let opts = sftp_sync::SyncOptions {
        git: true,
        ..Default::default()
    };
    let out = sftp_sync::sync_once(
        &LocalFs,
        remote.to_str().unwrap(),
        &local,
        &sftp_sync::Tree::new(),
        &opts,
        "h",
    )
    .await
    .unwrap();
    assert!(out.errors.is_empty(), "{:?}", out.errors);
    assert_eq!(read(&local, "normal.txt").as_deref(), Some("normal"));
    assert_eq!(read(&local, "important.log").as_deref(), Some("allowed"));
    assert_eq!(
        read(&local, "nested/keep.tmp").as_deref(),
        Some("nested allowed")
    );
    assert!(!local.join("nested/drop.tmp").exists());
}

#[tokio::test]
async fn validation_does_not_rescan_unrelated_subtrees() {
    let (local, remote) = (tmpdir("bounded-l"), tmpdir("bounded-r"));
    put(&remote, "unrelated/unchanged", "x", 1000);
    put(&local, "unrelated/unchanged", "x", 1000);
    for n in 0..12 {
        put(&local, &format!("file{n}"), "x", 1000);
    }
    let fs = CountingFs {
        unrelated_reads: AtomicU32::new(0),
    };
    let out = sftp_sync::sync_once(
        &fs,
        remote.to_str().unwrap(),
        &local,
        &sftp_sync::Tree::new(),
        &Default::default(),
        "h",
    )
    .await
    .unwrap();
    assert!(out.errors.is_empty(), "{:?}", out.errors);
    assert_eq!(out.changed, 12);
    assert_eq!(fs.unrelated_reads.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn non_utf8_local_name_aborts_before_deletions() {
    use std::os::unix::ffi::OsStringExt;
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "victim", "keep", 1000);
    let first = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;
    std::fs::remove_file(local.join("victim")).unwrap();
    std::fs::write(local.join(std::ffi::OsString::from_vec(vec![0xff])), "x").unwrap();
    let result = sftp_sync::sync_once(
        &LocalFs,
        remote.to_str().unwrap(),
        &local,
        &first.baseline,
        &Default::default(),
        "h",
    )
    .await;
    assert!(result.is_err());
    assert_eq!(read(&remote, "victim").as_deref(), Some("keep"));
}

#[tokio::test]
async fn absent_baseline_entries_are_pruned_except_shadowed_descendants() {
    let (local, remote, outside) = (tmpdir("l"), tmpdir("r"), tmpdir("outside"));
    put(&local, "gone", "old", 1000);
    put(&local, "dir/file", "old", 1000);
    let first = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;
    std::fs::remove_file(local.join("gone")).unwrap();
    std::fs::remove_file(remote.join("gone")).unwrap();
    std::fs::remove_file(local.join("dir/file")).unwrap();
    std::fs::remove_dir(local.join("dir")).unwrap();
    std::fs::remove_file(remote.join("dir/file")).unwrap();
    std::os::unix::fs::symlink(&outside, local.join("dir")).unwrap();
    let second = sync(&remote, &local, &first.baseline, true).await;
    assert!(!second.baseline.contains_key("gone"));
    assert!(second.baseline.contains_key("dir/file"));
    put(&local, "gone", "old", 1000);
    let third = sync(&remote, &local, &second.baseline, true).await;
    assert_eq!(read(&remote, "gone").as_deref(), Some("old"));
    assert!(third.errors.is_empty());
}

#[tokio::test]
async fn conflict_host_cannot_add_path_components() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "a", "base", 1000);
    let first = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;
    put(&local, "a", "local", 3000);
    put(&remote, "a", "remote", 2000);
    let out = sftp_sync::sync_once(
        &LocalFs,
        remote.to_str().unwrap(),
        &local,
        &first.baseline,
        &Default::default(),
        "../../escape",
    )
    .await
    .unwrap();
    assert!(out.errors.is_empty());
    assert_eq!(out.report.conflicts, 1);
    assert_eq!(read(&remote, "a").as_deref(), Some("local"));
    assert!(std::fs::read_dir(&local).unwrap().any(|e| {
        e.unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".conflict-")
    }));
}
