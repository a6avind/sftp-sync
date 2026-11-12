# sftp-sync

Two-way folder sync between a local directory and any remote, in Rust. Keeps
**real files on both sides** and reconciles changes each way.

**Backend-agnostic.** The engine talks to the remote through a small `RemoteFs`
trait (`read_dir` / `open_read` / `create_new` / `rename` / `remove_file` / `mkdir` /
`set_mtime`), so it has **no SFTP — or any network — dependency of its own**.
Point it at SFTP, another local dir, or an in-memory fake. Implement
the required filesystem operations over your transport. [Flow](https://github.com/a6avind/flow)
uses the built-in SFTP backend.

## Safe by design

A naive mirror can't tell a *delete* from a *not-yet-created* file, and a first
run against an empty side can wipe the other. This avoids both:

- **Three-way reconcile** against a persisted **baseline** (local vs remote vs
  last-synced) — a delete only propagates if the file was in the baseline *and*
  unchanged on the other side.
- **First run never deletes** — with `allow_delete=false` a missing side is
  treated as *new*, so an empty baseline union-merges instead of wiping.
- **Conflicts** (both edited since the baseline) → newer mtime keeps the real
  name; the loser is preserved as a conflict copy.
- **mtime preserved** on transfer, so a synced pair compares equal next scan and
  the steady state is a no-op.

## Usage

```rust
use std::path::Path;

// `fs`: your RemoteFs backend (e.g. SftpFs or a compatible local backend).
async fn run(fs: &impl sftp_sync::RemoteFs) {
    let local = Path::new("/home/me/project");
    let state = sftp_sync::state_file("host", "/remote/project", local);
    let mut base = sftp_sync::load_baseline(&state).unwrap();
    loop {
        let opts = sftp_sync::SyncOptions::default(); // git, allow_delete, direction, dry_run
        let out = sftp_sync::sync_once(fs, "/remote/project", local, &base, &opts, "host").await.unwrap();
        // Check out.errors; successful actions are reflected in out.baseline.
        if out.baseline != base {
            base = out.baseline;
            sftp_sync::save_baseline(&state, &base).unwrap();
        }
        // sleep, repeat…
        break;
    }
}
```

The pure `reconcile(local, remote, base, allow_delete) -> Vec<Action>` is exposed
too, if you want to drive the apply step yourself.

## Backends

### Built-in SFTP (optional)

Enable the `sftp` feature for a ready-made `RemoteFs`. Transport is [russh]; the
SFTP v3 protocol layer is an original built-in implementation — no external
SFTP-protocol dependency. It is also exposed for direct reuse:
`sftp_sync::{client::SftpSession, protocol, server}` — an async client (seekable
file handles, `read_dir`, stat/rename/symlink, the `check-file` extension) plus a
handler-driven in-process server (`server::run` + the `server::Handler` trait),
useful for tests or loopback. `SftpFs` is the sync-focused wrapper over it.

```toml
sftp-sync = { version = "0.3", features = ["sftp"] }
```

```rust
use sftp_sync::{SftpFs, Auth, HostKey};

let fs = SftpFs::connect(
    ("host", 22),
    "user",
    Auth::Password("…".into()),                 // or Auth::KeyFile { path, passphrase }
    HostKey::Fingerprint("SHA256:…".into()),    // from `ssh-keygen -lf` / known_hosts
).await?;
```

Verify the server with `HostKey::Fingerprint(...)`. `HostKey::AcceptAny` skips
verification (MITM-prone — trusted paths only). For full `known_hosts` handling,
drive russh yourself and pass the session to `SftpFs::from_session`.

The `check-file` feature adds `SftpFs::remote_sha256(path)` — server-side content
hashing (no download) for drift detection / transfer verification, on servers
that advertise the `check-file` extension (returns `None` on ones that don't,
e.g. stock OpenSSH).

### Any other remote

The feature is **off by default**; without it the crate pulls in no transport
deps. Implement `RemoteFs` over a compatible filesystem (another local dir or an
in-memory fake), provided it supports exclusive creation and no-replace rename:

```rust
use sftp_sync::{RemoteEntry, RemoteFs, SyncError};

impl RemoteFs for MyBackend {
    async fn read_dir(&self, path: &str) -> Result<Vec<RemoteEntry>, SyncError> { /* … */ }
    async fn open_read(&self, path: &str) -> Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>, SyncError> { /* … */ }
    async fn create_new(&self, path: &str) -> Result<Box<dyn tokio::io::AsyncWrite + Send + Unpin>, SyncError> { /* … */ }
    async fn set_mtime(&self, path: &str, mtime: i64) -> Result<(), SyncError> { /* … */ }
    async fn remove_file(&self, path: &str) -> Result<(), SyncError> { /* … */ }
    async fn rename(&self, from: &str, to: &str) -> Result<(), SyncError> { /* … */ }
    async fn mkdir(&self, path: &str) -> Result<(), SyncError> { /* … */ }
}
```

Map your backend's own errors with `SyncError::backend(e)`; local `io::Error`
converts via `?`.

[russh]: https://crates.io/crates/russh

## Options

`SyncOptions { git, allow_delete, direction, dry_run }`:

- `direction`: `Both` (default) / `PushOnly` (local authoritative) / `PullOnly`.
- `dry_run`: compute the plan and return the `Report` without touching disk.
- `allow_delete`: propagate deletions (never on the first run regardless).

Every transfer is written to a temp path and renamed over the target, so an
interrupted push can't leave a truncated file. `sync_once` returns an `Outcome`
with the new baseline, planned action counts, and per-action errors.

## Notes

- `git = true` honours `.gitignore` (and global/exclude) on the local walk.
- Symlinks are skipped; directories are implicit.
- Local mtime is set via `utimensat` (unix); a no-op elsewhere.
- Files are compared by size and whole-second mtime, not content hashes; changes
  that preserve both can be missed. Sync is not a replacement for backups.
- Only one sync process should operate on a given pair of trees at a time.
- A leftover remote `.sync-backup.sync-tmp` stops sync for manual recovery.
- Backend-agnostic core: no network deps unless you enable the `sftp` feature.

## License

Apache-2.0 — see `LICENSE`.
