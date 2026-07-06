//! Live round-trip test for the `sftp` backend against a real SSH/SFTP server.
//!
//! Skipped unless the server env vars are set, so `cargo test` stays green
//! offline. To run it:
//!
//! ```sh
//! SFTP_ADDR=host:22 SFTP_USER=me SFTP_KEY=~/.ssh/id_ed25519 \
//! SFTP_HOSTKEY_FP=SHA256:… SFTP_REMOTE_DIR=/tmp/sftp-sync-live \
//! cargo test --features sftp --test sftp_live -- --nocapture
//! ```
#![cfg(feature = "sftp")]

use std::path::{Path, PathBuf};

use sftp_sync::{Auth, HostKey, RemoteFs, SftpFs, SyncOptions, Tree};

struct Env {
    addr: String,
    user: String,
    key: PathBuf,
    fp: String,
    remote_dir: String,
}

fn env() -> Option<Env> {
    Some(Env {
        addr: std::env::var("SFTP_ADDR").ok()?,
        user: std::env::var("SFTP_USER").ok()?,
        key: std::env::var("SFTP_KEY").ok()?.into(),
        fp: std::env::var("SFTP_HOSTKEY_FP").ok()?,
        remote_dir: std::env::var("SFTP_REMOTE_DIR").ok()?,
    })
}

async fn connect(e: &Env, host_key: HostKey) -> Result<SftpFs, sftp_sync::SyncError> {
    SftpFs::connect(
        &e.addr,
        &e.user,
        Auth::KeyFile { path: e.key.clone(), passphrase: None },
        host_key,
    )
    .await
}

fn local_tmp(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("sftp-sync-live-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn write(dir: &Path, rel: &str, body: &str) {
    let p = dir.join(rel);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, body).unwrap();
}

fn read(dir: &Path, rel: &str) -> Option<String> {
    std::fs::read_to_string(dir.join(rel)).ok()
}

async fn sync(fs: &SftpFs, remote: &str, local: &Path, base: &Tree, del: bool) -> sftp_sync::Outcome {
    let opts = SyncOptions { allow_delete: del, ..Default::default() };
    sftp_sync::sync_once(fs, remote, local, base, &opts, "livehost").await.unwrap()
}

// Wipe the remote scratch dir between runs so state doesn't leak.
async fn clean_remote(fs: &SftpFs, base: &str) {
    if let Ok(tree) = sftp_sync::scan_remote(fs, base).await {
        for rel in tree.keys() {
            let _ = fs.remove_file(&format!("{}/{rel}", base.trim_end_matches('/'))).await;
        }
    }
}

#[tokio::test]
async fn live_sftp_roundtrip() {
    let Some(e) = env() else {
        eprintln!("SKIP live_sftp_roundtrip: set SFTP_ADDR/USER/KEY/HOSTKEY_FP/REMOTE_DIR to run");
        return;
    };

    let fs = connect(&e, HostKey::Fingerprint(e.fp.clone())).await.expect("connect + host-key verify");
    // Create only the PARENT of the sync root; sync_once must create the root
    // itself (push-capable directions). Proves the base-dir contract fix.
    let base = e.remote_dir.trim_end_matches('/').to_string();
    let remote = format!("{base}/rt-{}", std::process::id());
    fs.mkdir(&base).await.unwrap();
    clean_remote(&fs, &remote).await;

    // 1. First run unions both sides (local file pushed, remote file pulled), no delete.
    let local = local_tmp("l");
    write(&local, "only_local.txt", "L");
    write(&local, "nested/deep.txt", "deep");
    // seed a remote-only file by pushing from a throwaway dir
    let seed = local_tmp("seed");
    write(&seed, "only_remote.txt", "R");
    sync(&fs, &remote, &seed, &Tree::new(), false).await;

    let out1 = sync(&fs, &remote, &local, &Tree::new(), false).await;
    assert_eq!(read(&local, "only_remote.txt").as_deref(), Some("R"), "remote-only pulled to local");
    assert!(out1.baseline.contains_key("only_local.txt"), "local file in baseline");
    assert!(out1.baseline.contains_key("nested/deep.txt"), "nested push tracked");

    // verify the push actually landed remotely by pulling into a fresh dir
    let verify = local_tmp("v");
    let opts = SyncOptions { direction: sftp_sync::Direction::PullOnly, ..Default::default() };
    sftp_sync::sync_once(&fs, &remote, &verify, &Tree::new(), &opts, "livehost").await.unwrap();
    assert_eq!(read(&verify, "only_local.txt").as_deref(), Some("L"), "local push visible remotely");
    assert_eq!(read(&verify, "nested/deep.txt").as_deref(), Some("deep"), "nested dir recreated remotely");

    // 2. Steady state is a no-op — proves mtime is preserved across SFTP (setstat).
    let out2 = sync(&fs, &remote, &local, &out1.baseline, true).await;
    assert_eq!(out2.changed, 0, "clean second pass does nothing (mtime preserved over SFTP)");

    // 3. Local edit pushes.
    write(&local, "only_local.txt", "L2");
    let out3 = sync(&fs, &remote, &local, &out2.baseline, true).await;
    assert!(out3.report.pushed >= 1, "edit pushed");
    let verify2 = local_tmp("v2");
    sftp_sync::sync_once(&fs, &remote, &verify2, &Tree::new(), &opts, "livehost").await.unwrap();
    assert_eq!(read(&verify2, "only_local.txt").as_deref(), Some("L2"), "edit propagated remotely");

    // 4. Local delete propagates to remote.
    std::fs::remove_file(local.join("only_local.txt")).unwrap();
    sync(&fs, &remote, &local, &out3.baseline, true).await;
    let after = sftp_sync::scan_remote(&fs, &remote).await.unwrap();
    assert!(!after.contains_key("only_local.txt"), "local delete propagated to remote");

    clean_remote(&fs, &remote).await;
    println!("live_sftp_roundtrip OK against {}", e.addr);
}

#[cfg(feature = "check-file")]
#[tokio::test]
async fn live_check_file_degrades_on_openssh() {
    let Some(e) = env() else {
        eprintln!("SKIP live_check_file_degrades_on_openssh: set SFTP_ADDR/USER/KEY/HOSTKEY_FP/REMOTE_DIR to run");
        return;
    };
    let fs = connect(&e, HostKey::Fingerprint(e.fp.clone())).await.expect("connect");
    let base = e.remote_dir.trim_end_matches('/').to_string();
    fs.mkdir(&base).await.unwrap();
    let probe = format!("{base}/cf-probe-{}", std::process::id());
    // seed one file so the path exists
    let seed = local_tmp("cf");
    write(&seed, "x.txt", "hello check-file");
    let opts = SyncOptions::default();
    sftp_sync::sync_once(&fs, &probe, &seed, &Tree::new(), &opts, "livehost").await.unwrap();
    // OpenSSH does not advertise check-file → Ok(None), not an error.
    let got = fs.remote_sha256(&format!("{probe}/x.txt")).await.expect("remote_sha256 call");
    assert!(got.is_none(), "stock OpenSSH lacks check-file → expected None, got {got:?}");
    clean_remote(&fs, &probe).await;
    println!("check-file gracefully unsupported on {}", e.addr);
}

#[tokio::test]
async fn live_sftp_wrong_fingerprint_is_rejected() {
    let Some(e) = env() else {
        eprintln!("SKIP live_sftp_wrong_fingerprint_is_rejected: set SFTP_ADDR/USER/KEY/HOSTKEY_FP/REMOTE_DIR to run");
        return;
    };
    let bogus = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let r = connect(&e, HostKey::Fingerprint(bogus.into())).await;
    assert!(r.is_err(), "connection must be refused on host-key fingerprint mismatch");
}
