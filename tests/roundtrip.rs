//! End-to-end tests: drive `sync_once` against a `LocalFs` implementation of
//! `RemoteFs` (the "remote" is just a second real directory). This exercises the
//! whole scan/push/pull/apply/mtime path without any SFTP — the backend seam is a
//! trait, so tests supply the simplest possible backend.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use sftp_sync::{RemoteEntry, RemoteFs, SyncError};
use tokio::io::{AsyncRead, AsyncWrite};

// ---- a filesystem-backed RemoteFs (the "remote" is a real dir) --------------

struct LocalFs;

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
        Ok(Box::new(tokio::fs::File::open(path).await.map_err(SyncError::backend)?))
    }
    async fn create(&self, path: &str) -> Result<Box<dyn AsyncWrite + Send + Unpin>, SyncError> {
        Ok(Box::new(tokio::fs::File::create(path).await.map_err(SyncError::backend)?))
    }
    async fn set_mtime(&self, path: &str, mtime: i64) -> Result<(), SyncError> {
        set_mtime(Path::new(path), mtime);
        Ok(())
    }
    async fn remove_file(&self, path: &str) -> Result<(), SyncError> {
        std::fs::remove_file(path).map_err(SyncError::backend)
    }
    async fn rename(&self, from: &str, to: &str) -> Result<(), SyncError> {
        std::fs::rename(from, to).map_err(SyncError::backend)
    }
    async fn mkdir(&self, path: &str) -> Result<(), SyncError> {
        let _ = std::fs::create_dir(path);
        Ok(())
    }
}

// ---- helpers ----------------------------------------------------------------

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
    let t = libc::timespec { tv_sec: secs as libc::time_t, tv_nsec: 0 };
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

async fn sync(remote: &Path, local: &Path, base: &sftp_sync::Tree, del: bool) -> sftp_sync::Outcome {
    let opts = sftp_sync::SyncOptions { allow_delete: del, ..Default::default() };
    sftp_sync::sync_once(&LocalFs, remote.to_str().unwrap(), local, base, &opts, "testhost").await.unwrap()
}

// ---- tests ------------------------------------------------------------------

#[tokio::test]
async fn first_run_unions_both_sides() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "only_local.txt", "L", 1000);
    put(&remote, "only_remote.txt", "R", 1000);

    let out = sync(&remote, &local, &sftp_sync::Tree::new(), true).await;

    assert_eq!(read(&remote, "only_local.txt").as_deref(), Some("L"), "local pushed");
    assert_eq!(read(&local, "only_remote.txt").as_deref(), Some("R"), "remote pulled");
    assert_eq!(out.baseline.len(), 2);
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

    assert_eq!(read(&remote, "a.txt").as_deref(), Some("local-newer"), "newer wins the name");
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
    assert_eq!(read(&remote, "src/deep/a.rs").as_deref(), Some("fn main(){}"), "nested path recreated remotely");
}

#[tokio::test]
async fn dry_run_changes_nothing() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "a.txt", "L", 1000);
    let opts = sftp_sync::SyncOptions { dry_run: true, ..Default::default() };
    let out = sftp_sync::sync_once(&LocalFs, remote.to_str().unwrap(), &local, &sftp_sync::Tree::new(), &opts, "h").await.unwrap();

    assert_eq!(out.changed, 0);
    assert_eq!(out.report.pushed, 1, "reports intended push");
    assert!(read(&remote, "a.txt").is_none(), "applies nothing");
}

#[tokio::test]
async fn push_only_ignores_remote_side() {
    let (local, remote) = (tmpdir("l"), tmpdir("r"));
    put(&local, "mine.txt", "L", 1000);
    put(&remote, "theirs.txt", "R", 1000);
    let opts = sftp_sync::SyncOptions { direction: sftp_sync::Direction::PushOnly, ..Default::default() };
    let out = sftp_sync::sync_once(&LocalFs, remote.to_str().unwrap(), &local, &sftp_sync::Tree::new(), &opts, "h").await.unwrap();

    assert_eq!(read(&remote, "mine.txt").as_deref(), Some("L"), "local pushed");
    assert!(read(&local, "theirs.txt").is_none(), "remote-only NOT pulled");
    assert_eq!(out.report.pulled, 0);
}
