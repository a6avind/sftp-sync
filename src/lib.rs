//! Two-way folder sync between a local directory and any remote, with a safe
//! three-way reconcile.
//!
//! The engine talks to the remote through the small [`RemoteFs`] trait, so it
//! has no SFTP (or network) dependency of its own — back it with SFTP, S3,
//! another local dir, a fake. A ready-made [`SftpFs`] ships behind the
//! off-by-default `sftp` feature.
//!
//! [`reconcile`] is a three-way merge against a persisted baseline: it tells a
//! *delete* from a *not-yet-created* file (which naive mirrors get wrong), never
//! deletes on the first run (empty baseline), and resolves a both-sides edit by
//! keeping the newer mtime and preserving the loser as `name.conflict-<host>-<epoch>`.
//!
//! ```no_run
//! # async fn f(fs: &impl sftp_sync::RemoteFs) {
//! use std::path::Path;
//! let local = Path::new("/home/me/proj");
//! let state = sftp_sync::state_file("host", "/remote/proj", local);
//! let mut base = sftp_sync::load_baseline(&state);
//! let opts = sftp_sync::SyncOptions::default();
//! let out = sftp_sync::sync_once(fs, "/remote/proj", local, &base, &opts, "host").await.unwrap();
//! if out.baseline != base { base = out.baseline; sftp_sync::save_baseline(&state, &base); }
//! # }
//! ```

use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

#[cfg(feature = "sftp")]
mod proto;
#[cfg(feature = "sftp")]
mod sftp;
#[cfg(feature = "sftp")]
pub use sftp::{Auth, HostKey, SftpFs};

/// The bundled SFTP client, protocol types, and in-process server — reusable directly.
#[cfg(feature = "sftp")]
pub use proto::{client, protocol, server};

/// Everything the sync engine and its backends can fail with.
#[derive(Debug)]
pub enum SyncError {
    /// An error reported by a [`RemoteFs`] backend, via [`SyncError::backend`].
    Backend(String),
    /// A local-filesystem I/O error.
    Io(std::io::Error),
}

impl SyncError {
    /// Wrap a backend-specific error (message or any `Display` value).
    pub fn backend(msg: impl std::fmt::Display) -> Self {
        SyncError::Backend(msg.to_string())
    }
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncError::Backend(m) => write!(f, "remote backend error: {m}"),
            SyncError::Io(e) => write!(f, "local I/O error: {e}"),
        }
    }
}

impl std::error::Error for SyncError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SyncError::Io(e) => Some(e),
            SyncError::Backend(_) => None,
        }
    }
}

impl From<std::io::Error> for SyncError {
    fn from(e: std::io::Error) -> Self {
        SyncError::Io(e)
    }
}

/// A remote directory entry, as surfaced by [`RemoteFs::read_dir`].
pub struct RemoteEntry {
    pub name: String,
    pub is_dir: bool,
    pub is_symlink: bool,
    /// Modification time, whole seconds since the epoch.
    pub mtime: i64,
    pub size: u64,
}

/// The backend seam: the minimum set of remote filesystem ops the sync engine
/// needs. Paths are absolute, `/`-separated. Backends report failures as
/// [`SyncError::Backend`] (build one with [`SyncError::backend`]); an
/// `io::Error` converts automatically via `?`. All futures are `Send` so the
/// engine can run under a multi-thread runtime.
pub trait RemoteFs {
    fn read_dir(&self, path: &str) -> impl Future<Output = Result<Vec<RemoteEntry>, SyncError>> + Send;
    fn open_read(&self, path: &str) -> impl Future<Output = Result<Box<dyn AsyncRead + Send + Unpin>, SyncError>> + Send;
    /// Create/truncate a file for writing.
    fn create(&self, path: &str) -> impl Future<Output = Result<Box<dyn AsyncWrite + Send + Unpin>, SyncError>> + Send;
    /// Set a file's mtime (whole seconds). Best-effort; keeps synced pairs equal.
    fn set_mtime(&self, path: &str, mtime: i64) -> impl Future<Output = Result<(), SyncError>> + Send;
    fn remove_file(&self, path: &str) -> impl Future<Output = Result<(), SyncError>> + Send;
    fn rename(&self, from: &str, to: &str) -> impl Future<Output = Result<(), SyncError>> + Send;
    /// Create a directory; must succeed (or no-op) if it already exists.
    fn mkdir(&self, path: &str) -> impl Future<Output = Result<(), SyncError>> + Send;
}

/// One tracked file: mtime (whole seconds) + size. Equality is the in-sync test;
/// mtime is preserved on transfer so a synced pair compares equal.
#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Debug)]
pub struct Meta {
    pub mtime: i64,
    pub size: u64,
}

/// Relative-path → metadata map of a tree's files (directories are implicit).
pub type Tree = BTreeMap<String, Meta>;

#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    Push(String),
    Pull(String),
    DelRemote(String),
    DelLocal(String),
    /// Both sides changed since baseline; `local_newer` decides which keeps the
    /// real name (the other becomes a `.conflict-…` copy).
    Conflict { rel: String, local_newer: bool },
}

/// Which way changes flow.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Direction {
    /// Full two-way sync (default).
    #[default]
    Both,
    /// Local is authoritative: only push/delete-remote (never pull/delete-local).
    PushOnly,
    /// Remote is authoritative: only pull/delete-local.
    PullOnly,
}

/// Knobs for a sync run. Construct with `..Default::default()`.
#[derive(Clone, Copy, Debug)]
pub struct SyncOptions {
    /// Honour `.gitignore` (and git global/exclude) on the local walk.
    pub git: bool,
    /// Propagate deletions (still never on the first run — an empty baseline).
    pub allow_delete: bool,
    pub direction: Direction,
    /// Compute the action plan but change nothing (disk and baseline untouched).
    pub dry_run: bool,
}

impl Default for SyncOptions {
    fn default() -> Self {
        SyncOptions { git: false, allow_delete: true, direction: Direction::Both, dry_run: false }
    }
}

/// What a cycle did (or, for `dry_run`, would do).
#[derive(Default, Debug, Clone)]
pub struct Report {
    pub pushed: usize,
    pub pulled: usize,
    pub deleted_remote: usize,
    pub deleted_local: usize,
    pub conflicts: usize,
}

impl Report {
    fn tally(acts: &[Action]) -> Self {
        let mut r = Report::default();
        for a in acts {
            match a {
                Action::Push(_) => r.pushed += 1,
                Action::Pull(_) => r.pulled += 1,
                Action::DelRemote(_) => r.deleted_remote += 1,
                Action::DelLocal(_) => r.deleted_local += 1,
                Action::Conflict { .. } => r.conflicts += 1,
            }
        }
        r
    }
    pub fn total(&self) -> usize {
        self.pushed + self.pulled + self.deleted_remote + self.deleted_local + self.conflicts
    }
}

/// Result of one [`sync_once`] cycle.
pub struct Outcome {
    /// The new baseline (both sides now equal). Persist it if it changed. On
    /// `dry_run` this equals the input baseline (nothing was applied).
    pub baseline: Tree,
    /// Number of actions applied this cycle (0 on `dry_run`).
    pub changed: usize,
    /// Per-category breakdown of the plan (applied, or intended on `dry_run`).
    pub report: Report,
}

fn filter_direction(acts: Vec<Action>, dir: Direction) -> Vec<Action> {
    match dir {
        Direction::Both => acts,
        Direction::PushOnly => acts
            .into_iter()
            .filter_map(|a| match a {
                Action::Push(_) | Action::DelRemote(_) => Some(a),
                Action::Pull(_) | Action::DelLocal(_) => None,
                Action::Conflict { rel, .. } => Some(Action::Push(rel)),
            })
            .collect(),
        Direction::PullOnly => acts
            .into_iter()
            .filter_map(|a| match a {
                Action::Pull(_) | Action::DelLocal(_) => Some(a),
                Action::Push(_) | Action::DelRemote(_) => None,
                Action::Conflict { rel, .. } => Some(Action::Pull(rel)),
            })
            .collect(),
    }
}

/// Three-way merge of local vs remote vs the last-synced baseline. Pure. With
/// `allow_delete=false` a side-missing file is treated as new (never a deletion) —
/// use that on the first run so an empty baseline can't wipe a populated side.
pub fn reconcile(local: &Tree, remote: &Tree, base: &Tree, allow_delete: bool) -> Vec<Action> {
    let mut acts = Vec::new();
    let keys: std::collections::BTreeSet<&String> =
        local.keys().chain(remote.keys()).chain(base.keys()).collect();
    for k in keys {
        let l = local.get(k);
        let r = remote.get(k);
        let b = base.get(k);
        match (l, r) {
            (Some(l), Some(r)) => {
                if l == r {
                    continue;
                }
                let lchanged = b != Some(l);
                let rchanged = b != Some(r);
                match (lchanged, rchanged) {
                    (true, true) => acts.push(Action::Conflict { rel: k.clone(), local_newer: l.mtime >= r.mtime }),
                    (true, false) => acts.push(Action::Push(k.clone())),
                    (false, true) => acts.push(Action::Pull(k.clone())),
                    (false, false) => acts.push(if l.mtime >= r.mtime { Action::Push(k.clone()) } else { Action::Pull(k.clone()) }),
                }
            }
            (Some(l), None) => {
                if b.is_some() && allow_delete {
                    if b == Some(l) { acts.push(Action::DelLocal(k.clone())) } else { acts.push(Action::Push(k.clone())) }
                } else {
                    acts.push(Action::Push(k.clone()));
                }
            }
            (None, Some(r)) => {
                if b.is_some() && allow_delete {
                    if b == Some(r) { acts.push(Action::DelRemote(k.clone())) } else { acts.push(Action::Pull(k.clone())) }
                } else {
                    acts.push(Action::Pull(k.clone()));
                }
            }
            (None, None) => {}
        }
    }
    acts
}

/// Reconcile local vs remote against `baseline`, then apply the plan (unless
/// `opts.dry_run`). Returns the new baseline + a [`Report`]. `host` labels
/// conflict copies.
///
/// `base_remote`'s parent must exist; the sync root itself is created if missing
/// on push-capable runs (like `rsync`/`scp`). [`Direction::PullOnly`] never
/// creates it — a missing source is an error.
pub async fn sync_once<R: RemoteFs>(
    fs: &R,
    base_remote: &str,
    local_root: &Path,
    baseline: &Tree,
    opts: &SyncOptions,
    host: &str,
) -> Result<Outcome, SyncError> {
    // Skip on pull-only: silently creating a missing source could delete-local everything.
    if !opts.dry_run && opts.direction != Direction::PullOnly {
        let _ = fs.mkdir(base_remote).await;
    }
    let local = scan_local(local_root, opts.git);
    let remote = scan_remote(fs, base_remote).await?;
    let allow = opts.allow_delete && !baseline.is_empty(); // first run never deletes
    let acts = filter_direction(reconcile(&local, &remote, baseline, allow), opts.direction);
    let report = Report::tally(&acts);

    if opts.dry_run {
        return Ok(Outcome { baseline: baseline.clone(), changed: 0, report });
    }
    // Advance the baseline only for actions that landed: a path recorded as synced
    // after a failed transfer reads next cycle as a remote-side delete → local data loss.
    let mut new_base = baseline.clone();
    let mut changed = 0usize;
    for a in &acts {
        match apply(fs, base_remote, local_root, a, host, &remote).await {
            Ok(()) => {
                commit_baseline(&mut new_base, a, &local, &remote);
                changed += 1;
            }
            Err(e) => eprintln!("sftp-sync: {a:?} failed: {e}"),
        }
    }
    Ok(Outcome { baseline: new_base, changed, report })
}

/// Fold one successfully-applied action into the running baseline, deriving the
/// entry from whichever side is now authoritative (never a blind local rescan).
fn commit_baseline(base: &mut Tree, a: &Action, local: &Tree, remote: &Tree) {
    match a {
        Action::Push(rel) => {
            if let Some(m) = local.get(rel) { base.insert(rel.clone(), *m); }
        }
        Action::Pull(rel) => {
            if let Some(m) = remote.get(rel) { base.insert(rel.clone(), *m); }
        }
        Action::DelRemote(rel) | Action::DelLocal(rel) => {
            base.remove(rel);
        }
        Action::Conflict { rel, local_newer } => {
            let side = if *local_newer { local.get(rel) } else { remote.get(rel) };
            if let Some(m) = side { base.insert(rel.clone(), *m); }
        }
    }
}

async fn apply<R: RemoteFs>(fs: &R, base: &str, local_root: &Path, a: &Action, host: &str, remote: &Tree) -> Result<(), SyncError> {
    let rmtime = |rel: &str| remote.get(rel).map(|m| m.mtime).unwrap_or(0);
    match a {
        Action::Push(rel) => push(fs, base, local_root, rel).await,
        Action::Pull(rel) => pull(fs, base, local_root, rel, rel, rmtime(rel)).await,
        Action::DelRemote(rel) => fs.remove_file(&join(base, rel)).await,
        Action::DelLocal(rel) => std::fs::remove_file(local_root.join(rel)).map_err(SyncError::from),
        Action::Conflict { rel, local_newer } => {
            // Counter suffix: two conflicts on the same path within one wall-clock
            // second would otherwise share a tag and clobber the first aside-copy.
            static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let tag = format!("{rel}.conflict-{host}-{}-{n}", now_secs());
            if *local_newer {
                pull(fs, base, local_root, rel, &tag, rmtime(rel)).await?; // save remote aside
                push(fs, base, local_root, rel).await // local wins
            } else {
                copy_local(local_root, rel, &tag)?; // save local aside
                pull(fs, base, local_root, rel, rel, rmtime(rel)).await // remote wins
            }
        }
    }
}

async fn push<R: RemoteFs>(fs: &R, base: &str, local_root: &Path, rel: &str) -> Result<(), SyncError> {
    let lpath = local_root.join(rel);
    let rpath = join(base, rel);
    ensure_remote_dirs(fs, base, rel).await;
    let mtime = local_mtime_secs(&lpath);
    // Write to a temp path then rename over the target: an interrupted transfer
    // leaves the `.sync-tmp` (skipped by scans), never a truncated real file.
    let tmp = format!("{rpath}.sync-tmp");
    let mut lf = tokio::fs::File::open(&lpath).await?;
    let mut w = fs.create(&tmp).await?;
    tokio::io::copy(&mut lf, &mut w).await?;
    w.shutdown().await?; // flush + close (adapter awaits SFTP CLOSE)
    drop(w);
    let _ = fs.set_mtime(&tmp, mtime).await;
    let _ = fs.remove_file(&rpath).await; // rename may not overwrite; single-writer so safe
    fs.rename(&tmp, &rpath).await
}

async fn pull<R: RemoteFs>(fs: &R, base: &str, local_root: &Path, src_rel: &str, dst_rel: &str, mtime: i64) -> Result<(), SyncError> {
    let rpath = join(base, src_rel);
    let lpath = local_root.join(dst_rel);
    if let Some(parent) = lpath.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut r = fs.open_read(&rpath).await?;
    let mut lf = tokio::fs::File::create(&lpath).await?;
    tokio::io::copy(&mut r, &mut lf).await?;
    lf.flush().await?;
    set_local_mtime(&lpath, mtime);
    Ok(())
}

fn copy_local(local_root: &Path, rel: &str, dst_rel: &str) -> Result<(), SyncError> {
    std::fs::copy(local_root.join(rel), local_root.join(dst_rel)).map(|_| ()).map_err(SyncError::from)
}

async fn ensure_remote_dirs<R: RemoteFs>(fs: &R, base: &str, rel: &str) {
    let parts: Vec<&str> = rel.split('/').collect();
    let mut cur = base.to_string();
    for p in &parts[..parts.len().saturating_sub(1)] {
        cur = join(&cur, p);
        let _ = fs.mkdir(&cur).await;
    }
}

// ---- scanning ---------------------------------------------------------------

/// Walk `root` and return its files as a [`Tree`]. `git` honours `.gitignore`
/// (and hidden/global/exclude); else a raw recursive walk. Conflict/temp copies
/// are skipped so they don't ping-pong.
pub fn scan_local(root: &Path, git: bool) -> Tree {
    let mut out = Tree::new();
    let walker = ignore::WalkBuilder::new(root)
        .standard_filters(git)
        .hidden(false)
        .git_ignore(git)
        .git_global(git)
        .git_exclude(git)
        .parents(git)
        .build();
    for entry in walker.flatten() {
        if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }
        let Ok(rel) = entry.path().strip_prefix(root) else { continue };
        let Some(rel) = rel.to_str() else { continue };
        if is_skippable(rel) {
            continue;
        }
        if let Ok(m) = entry.metadata() {
            let mtime = m
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            out.insert(rel.replace('\\', "/"), Meta { mtime, size: m.len() });
        }
    }
    out
}

/// Recursively list the remote tree under `base` via [`RemoteFs`] (files only;
/// symlinks skipped).
pub async fn scan_remote<R: RemoteFs>(fs: &R, base: &str) -> Result<Tree, SyncError> {
    const MAX_DEPTH: usize = 64;
    let mut out = Tree::new();
    let mut stack = vec![String::new()];
    while let Some(rel) = stack.pop() {
        let dir = if rel.is_empty() { base.to_string() } else { join(base, &rel) };
        for e in fs.read_dir(&dir).await? {
            // Untrusted backend name: `.`/`..`/`a/../../etc` joined unchecked escapes
            // `base` on pull and drives `..` recursion. One choke point for every entry.
            if !is_safe_component(&e.name) {
                continue;
            }
            if e.is_symlink || is_skippable(&e.name) {
                continue;
            }
            let childrel = if rel.is_empty() { e.name.clone() } else { format!("{rel}/{}", e.name) };
            if e.is_dir {
                if childrel.matches('/').count() < MAX_DEPTH {
                    stack.push(childrel);
                }
            } else {
                out.insert(childrel, Meta { mtime: e.mtime, size: e.size });
            }
        }
    }
    Ok(out)
}

/// A remote path component safe to join under the sync root: non-empty, not `.`/`..`,
/// no separators or NUL — else a hostile backend traverses out (see [`scan_remote`]).
fn is_safe_component(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains('\0')
}

fn is_skippable(rel: &str) -> bool {
    let b = basename(rel);
    b.contains(".conflict-") || b.ends_with(".sync-tmp")
}

// ---- baseline persistence ---------------------------------------------------

/// Path to the baseline file for a (host, remote, local) triple, under the OS
/// config dir (`…/sftp-sync/<sha256>.json`).
pub fn state_file(host: &str, base_remote: &str, local_root: &Path) -> std::path::PathBuf {
    let key = format!("{host}\0{base_remote}\0{}", local_root.display());
    let digest = <sha2::Sha256 as sha2::Digest>::digest(key.as_bytes());
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    let dir = config_dir().join("sftp-sync");
    let _ = std::fs::create_dir_all(&dir);
    dir.join(format!("{hex}.json"))
}

pub fn load_baseline(path: &Path) -> Tree {
    std::fs::read_to_string(path).ok().and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default()
}

pub fn save_baseline(path: &Path, tree: &Tree) {
    if let Ok(s) = serde_json::to_string(tree) {
        let tmp = path.with_extension("json.tmp");
        if std::fs::write(&tmp, s).is_ok() {
            let _ = std::fs::rename(&tmp, path);
        }
    }
}

// ---- small self-contained helpers -------------------------------------------

fn now_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}

fn local_mtime_secs(p: &Path) -> i64 {
    std::fs::metadata(p)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or_else(now_secs)
}

#[cfg(unix)]
fn set_local_mtime(p: &Path, secs: i64) {
    use std::os::unix::ffi::OsStrExt;
    let Ok(c) = std::ffi::CString::new(p.as_os_str().as_bytes()) else { return };
    let t = libc::timespec { tv_sec: secs as libc::time_t, tv_nsec: 0 };
    let times = [t, t];
    unsafe {
        libc::utimensat(libc::AT_FDCWD, c.as_ptr(), times.as_ptr(), 0);
    }
}

#[cfg(not(unix))]
fn set_local_mtime(_p: &Path, _secs: i64) {}

fn config_dir() -> std::path::PathBuf {
    #[cfg(target_os = "macos")]
    let v = std::env::var_os("HOME").map(|h| Path::new(&h).join("Library/Application Support"));
    #[cfg(not(target_os = "macos"))]
    let v = std::env::var_os("XDG_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| Path::new(&h).join(".config")));
    v.unwrap_or_else(|| std::path::PathBuf::from("."))
}

/// POSIX path join (remote paths are always `/`-separated). Root-safe.
fn join(base: &str, name: &str) -> String {
    if base.is_empty() || base == "/" {
        format!("/{name}")
    } else {
        format!("{}/{name}", base.trim_end_matches('/'))
    }
}

fn basename(path: &str) -> &str {
    let t = path.trim_end_matches('/');
    t.rsplit('/').next().unwrap_or(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(mtime: i64, size: u64) -> Meta {
        Meta { mtime, size }
    }
    fn t(pairs: &[(&str, Meta)]) -> Tree {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    #[test]
    fn first_run_unions_never_deletes() {
        let local = t(&[("a", m(1, 1))]);
        let remote = t(&[("b", m(1, 1))]);
        let acts = reconcile(&local, &remote, &Tree::new(), false);
        assert!(acts.contains(&Action::Push("a".into())));
        assert!(acts.contains(&Action::Pull("b".into())));
        assert!(!acts.iter().any(|a| matches!(a, Action::DelLocal(_) | Action::DelRemote(_))));
    }

    #[test]
    fn in_sync_pair_is_noop() {
        let both = t(&[("a", m(5, 10))]);
        assert!(reconcile(&both, &both, &both, true).is_empty());
    }

    #[test]
    fn local_edit_pushes_remote_edit_pulls() {
        let base = t(&[("a", m(5, 10)), ("b", m(5, 10))]);
        let local = t(&[("a", m(9, 12)), ("b", m(5, 10))]);
        let remote = t(&[("a", m(5, 10)), ("b", m(8, 11))]);
        let acts = reconcile(&local, &remote, &base, true);
        assert!(acts.contains(&Action::Push("a".into())));
        assert!(acts.contains(&Action::Pull("b".into())));
    }

    #[test]
    fn delete_propagates_only_when_unchanged_other_side() {
        let base = t(&[("a", m(5, 10)), ("b", m(5, 10))]);
        let local = t(&[("b", m(9, 12))]);
        let remote = t(&[("a", m(5, 10))]);
        let acts = reconcile(&local, &remote, &base, true);
        assert!(acts.contains(&Action::DelRemote("a".into())));
        assert!(acts.contains(&Action::Push("b".into())));
    }

    #[test]
    fn both_edited_is_conflict_newer_wins() {
        let base = t(&[("a", m(5, 10))]);
        let local = t(&[("a", m(20, 11))]);
        let remote = t(&[("a", m(9, 13))]);
        let acts = reconcile(&local, &remote, &base, true);
        assert_eq!(acts, vec![Action::Conflict { rel: "a".into(), local_newer: true }]);
    }
}
