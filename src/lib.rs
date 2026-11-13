//! Two-way folder sync between a local directory and any remote, with a safe
//! three-way reconcile.
//!
//! The engine talks to the remote through the small [`RemoteFs`] trait, so it
//! has no SFTP (or network) dependency of its own — back it with SFTP,
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
//! let mut base = sftp_sync::load_baseline(&state).unwrap();
//! let opts = sftp_sync::SyncOptions::default();
//! let out = sftp_sync::sync_once(fs, "/remote/proj", local, &base, &opts, "host").await.unwrap();
//! if out.baseline != base { base = out.baseline; sftp_sync::save_baseline(&state, &base).unwrap(); }
//! # }
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::Digest;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

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
    fn read_dir(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<Vec<RemoteEntry>, SyncError>> + Send;
    fn open_read(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<Box<dyn AsyncRead + Send + Unpin>, SyncError>> + Send;
    /// Atomically create a new file, failing if the path already exists.
    fn create_new(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<Box<dyn AsyncWrite + Send + Unpin>, SyncError>> + Send;
    /// Set a file's mtime (whole seconds). Required to keep synced pairs equal.
    fn set_mtime(
        &self,
        path: &str,
        mtime: i64,
    ) -> impl Future<Output = Result<(), SyncError>> + Send;
    fn remove_file(&self, path: &str) -> impl Future<Output = Result<(), SyncError>> + Send;
    /// Rename a file without replacing an existing destination.
    fn rename(&self, from: &str, to: &str) -> impl Future<Output = Result<(), SyncError>> + Send;
    /// Create a directory; must succeed (or no-op) if it already exists.
    fn mkdir(&self, path: &str) -> impl Future<Output = Result<(), SyncError>> + Send;
}

/// A tracked file's timestamp, size, and optional SHA-256 digest.
#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Debug)]
pub struct Meta {
    pub mtime: i64,
    pub size: u64,
    /// Missing on baselines written before content verification was introduced.
    #[serde(default)]
    pub sha256: Option<[u8; 32]>,
}

/// Relative-path → metadata map of a tree's files (directories are implicit).
pub type Tree = BTreeMap<String, Meta>;

struct RemoveFileOnDrop(std::path::PathBuf);

impl Drop for RemoveFileOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    Push(String),
    Pull(String),
    DelRemote(String),
    DelLocal(String),
    /// Both sides changed since baseline; `local_newer` decides which keeps the
    /// real name (the other becomes a `.conflict-…` copy).
    Conflict {
        rel: String,
        local_newer: bool,
    },
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
    /// Honour `.ignore`, `.gitignore`, and git global/exclude rules on local and
    /// remote-only paths. Ignore files must be readable to classify absent paths.
    pub git: bool,
    /// Propagate deletions (still never on the first run — an empty baseline).
    pub allow_delete: bool,
    pub direction: Direction,
    /// Compute the action plan but change nothing (disk and baseline untouched).
    pub dry_run: bool,
}

impl Default for SyncOptions {
    fn default() -> Self {
        SyncOptions {
            git: false,
            allow_delete: true,
            direction: Direction::Both,
            dry_run: false,
        }
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
    /// Planned action counts; failures are listed in `errors`.
    pub report: Report,
    /// Per-action failures; successful actions remain reflected in `baseline`.
    pub errors: Vec<String>,
}

fn filter_direction(acts: Vec<Action>, dir: Direction, local: &Tree, remote: &Tree) -> Vec<Action> {
    match dir {
        Direction::Both => acts,
        Direction::PushOnly => acts
            .into_iter()
            .filter_map(|a| match a {
                Action::Push(_) | Action::DelRemote(_) => Some(a),
                Action::Pull(rel) | Action::DelLocal(rel) if local.contains_key(&rel) => {
                    Some(Action::Push(rel))
                }
                Action::Pull(_) | Action::DelLocal(_) => None,
                Action::Conflict { rel, .. } => Some(Action::Push(rel)),
            })
            .collect(),
        Direction::PullOnly => acts
            .into_iter()
            .filter_map(|a| match a {
                Action::Pull(_) | Action::DelLocal(_) => Some(a),
                Action::Push(rel) | Action::DelRemote(rel) if remote.contains_key(&rel) => {
                    Some(Action::Pull(rel))
                }
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
    let keys: std::collections::BTreeSet<&String> = local
        .keys()
        .chain(remote.keys())
        .chain(base.keys())
        .collect();
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
                    (true, true) => acts.push(Action::Conflict {
                        rel: k.clone(),
                        local_newer: l.mtime >= r.mtime,
                    }),
                    (true, false) => acts.push(Action::Push(k.clone())),
                    (false, true) => acts.push(Action::Pull(k.clone())),
                    (false, false) => acts.push(if l.mtime >= r.mtime {
                        Action::Push(k.clone())
                    } else {
                        Action::Pull(k.clone())
                    }),
                }
            }
            (Some(l), None) => {
                if b.is_some() && allow_delete {
                    if b == Some(l) {
                        acts.push(Action::DelLocal(k.clone()))
                    } else {
                        acts.push(Action::Push(k.clone()))
                    }
                } else {
                    acts.push(Action::Push(k.clone()));
                }
            }
            (None, Some(r)) => {
                if b.is_some() && allow_delete {
                    if b == Some(r) {
                        acts.push(Action::DelRemote(k.clone()))
                    } else {
                        acts.push(Action::Pull(k.clone()))
                    }
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
/// `base_remote`'s parent must exist; the sync root may be created on an initial
/// push-capable run. On subsequent runs a missing root is an error, not a
/// deletion of every remote file. [`Direction::PullOnly`] never creates it.
///
/// Before each action, affected paths are checked again using size and
/// whole-second mtime. This is not atomic: changes after the check, during a
/// transfer, or changes with identical metadata can still be missed. The remote
/// interface has no stat operation, so checks list each ancestor directory;
/// many actions in one large directory can still be expensive.
pub async fn sync_once<R: RemoteFs>(
    fs: &R,
    base_remote: &str,
    local_root: &Path,
    baseline: &Tree,
    opts: &SyncOptions,
    host: &str,
) -> Result<Outcome, SyncError> {
    // Pull-only must not create a missing source and then read it as empty.
    if !opts.dry_run && opts.direction != Direction::PullOnly && baseline.is_empty() {
        fs.mkdir(base_remote).await?;
    }
    // Walking a large tree is the single biggest blocking chunk in the cycle;
    // offload it so it can't stall the runtime thread (and thus the co-located
    // transport keepalive) on a current-thread executor.
    let (local, mut local_sym) = {
        let root = local_root.to_path_buf();
        let git = opts.git;
        tokio::task::spawn_blocking(move || scan_local_shadowed(&root, git))
            .await
            .map_err(SyncError::backend)??
    };
    let (remote, remote_sym) = scan_remote_shadowed(fs, base_remote).await?;
    if opts.git {
        for path in remote.keys().chain(baseline.keys()) {
            if !local.contains_key(path) && ignored_absent_path(local_root, path)? {
                local_sym.insert(path.clone());
            }
        }
    }
    let allow = opts.allow_delete && !baseline.is_empty(); // first run never deletes
    let mut acts = filter_direction(
        reconcile(&local, &remote, baseline, allow),
        opts.direction,
        &local,
        &remote,
    );
    // A symlink also shadows all paths beneath it.
    acts.retain(|a| match a {
        Action::Push(rel)
        | Action::Pull(rel)
        | Action::DelLocal(rel)
        | Action::DelRemote(rel)
        | Action::Conflict { rel, .. } => {
            !is_shadowed(rel, &local_sym) && !is_shadowed(rel, &remote_sym)
        }
    });
    let report = Report::tally(&acts);

    if opts.dry_run {
        return Ok(Outcome {
            baseline: baseline.clone(),
            changed: 0,
            report,
            errors: Vec::new(),
        });
    }
    // Advance only successful actions to avoid treating failed transfers as deletions.
    let mut new_base = baseline.clone();
    new_base.retain(|path, _| {
        local.contains_key(path)
            || remote.contains_key(path)
            || is_shadowed(path, &local_sym)
            || is_shadowed(path, &remote_sym)
    });
    for (path, meta) in &local {
        if remote.get(path) == Some(meta) {
            new_base.insert(path.clone(), *meta);
        }
    }
    let mut changed = 0usize;
    let mut errors = Vec::new();
    for a in &acts {
        let rel = match a {
            Action::Push(rel)
            | Action::Pull(rel)
            | Action::DelRemote(rel)
            | Action::DelLocal(rel)
            | Action::Conflict { rel, .. } => rel,
        };
        // Best-effort validation only: neither backend exposes conditional
        // mutation, and metadata is only second-resolution mtime plus size.
        let validation = async {
            let current_local = checked_local_meta(local_root, rel).await?;
            let current_remote = remote_path_meta(fs, base_remote, rel).await?;
            if (opts.git && ignored_absent_path(local_root, rel)?)
                || current_local.as_ref() != local.get(rel)
                || current_remote.as_ref() != remote.get(rel)
            {
                return Err(SyncError::backend("path changed since scan; retry sync"));
            }
            Ok(())
        }
        .await;
        let result = match validation {
            Ok(()) => apply(fs, base_remote, local_root, a, host, &local, &remote).await,
            Err(e) => Err(e),
        };
        match result {
            Ok(()) => {
                commit_baseline(&mut new_base, a, &local, &remote);
                changed += 1;
            }
            Err(e) => errors.push(format!("{a:?}: {e}")),
        }
    }
    Ok(Outcome {
        baseline: new_base,
        changed,
        report,
        errors,
    })
}

fn is_shadowed(path: &str, symlinks: &BTreeSet<String>) -> bool {
    symlinks.contains(path)
        || path
            .match_indices('/')
            .any(|(i, _)| symlinks.contains(&path[..i]))
}

fn hash_local(path: &Path, expected: &std::fs::Metadata) -> Result<[u8; 32], SyncError> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let before = file.metadata()?;
    if before.len() != expected.len() || before.modified()? != expected.modified()? {
        return Err(SyncError::backend("local file changed before hashing"));
    }
    let mut hash = sha2::Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
    }
    let after = file.metadata()?;
    if before.len() != after.len() || before.modified()? != after.modified()? {
        return Err(SyncError::backend("local file changed while hashing"));
    }
    Ok(hash.finalize().into())
}

async fn hash_remote<R: RemoteFs>(fs: &R, path: &str, size: u64) -> Result<[u8; 32], SyncError> {
    let mut file = fs.open_read(path).await?;
    let mut hash = sha2::Sha256::new();
    let mut total = 0u64;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        total = total
            .checked_add(n as u64)
            .ok_or_else(|| SyncError::backend("remote file size overflow"))?;
        if total > size {
            return Err(SyncError::backend("remote file changed while hashing"));
        }
        hash.update(&buf[..n]);
    }
    if total != size {
        return Err(SyncError::backend("remote file changed while hashing"));
    }
    Ok(hash.finalize().into())
}

// Revalidation narrows the race window, but the backends cannot conditionally
// replace or delete files atomically.
fn local_path_meta(root: &Path, rel: &str) -> Result<Option<Meta>, SyncError> {
    let mut path = root.to_path_buf();
    let mut parts = rel.split('/').peekable();
    while let Some(part) = parts.next() {
        path.push(part);
        let meta = match std::fs::symlink_metadata(&path) {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        if meta.file_type().is_symlink() {
            return Err(SyncError::backend("local path became a symlink"));
        }
        if parts.peek().is_some() {
            if !meta.is_dir() {
                return Err(SyncError::backend("local parent is not a directory"));
            }
        } else if meta.is_file() {
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            return Ok(Some(Meta {
                mtime,
                size: meta.len(),
                sha256: Some(hash_local(&path, &meta)?),
            }));
        } else {
            return Err(SyncError::backend("local path is not a regular file"));
        }
    }
    Ok(None)
}

async fn checked_local_meta(root: &Path, rel: &str) -> Result<Option<Meta>, SyncError> {
    let root = root.to_path_buf();
    let rel = rel.to_owned();
    tokio::task::spawn_blocking(move || local_path_meta(&root, &rel))
        .await
        .map_err(SyncError::backend)?
}

async fn remote_path_meta<R: RemoteFs>(
    fs: &R,
    base: &str,
    rel: &str,
) -> Result<Option<Meta>, SyncError> {
    let mut dir = base.to_string();
    let mut parts = rel.split('/').peekable();
    while let Some(part) = parts.next() {
        let entries = fs.read_dir(&dir).await?;
        let Some(entry) = entries.into_iter().find(|e| e.name == part) else {
            return Ok(None);
        };
        if entry.is_symlink {
            return Err(SyncError::backend("remote path became a symlink"));
        }
        if parts.peek().is_some() {
            if !entry.is_dir {
                return Err(SyncError::backend("remote parent is not a directory"));
            }
            dir = join(&dir, part);
        } else if !entry.is_dir {
            return Ok(Some(Meta {
                mtime: entry.mtime,
                size: entry.size,
                sha256: Some(hash_remote(fs, &join(&dir, part), entry.size).await?),
            }));
        } else {
            return Err(SyncError::backend("remote path is a directory"));
        }
    }
    Ok(None)
}

// Match each component top-down: an ignored parent cannot be re-included by
// a rule in a directory that the walker would never enter. Rules in .ignore
// have priority over git rules, as in ignore::WalkBuilder.
fn ignored_absent_path(root: &Path, rel: &str) -> Result<bool, SyncError> {
    use ignore::gitignore::{Gitignore, GitignoreBuilder};
    let root = std::fs::canonicalize(root)?;
    let repo = root
        .ancestors()
        .find(|p| p.join(".git").exists())
        .map(Path::to_path_buf);
    let mut ignores = Vec::new();
    let mut gitignores = Vec::new();
    let load = |dir: &Path, name: &str| -> Result<Gitignore, SyncError> {
        let path = dir.join(name);
        let mut b = GitignoreBuilder::new(dir);
        match std::fs::metadata(&path) {
            Ok(_) => {
                if let Some(e) = b.add(&path) {
                    return Err(SyncError::backend(e));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        b.build().map_err(SyncError::backend)
    };
    let mut ancestors: Vec<_> = root.ancestors().map(Path::to_path_buf).collect();
    ancestors.reverse();
    for dir in &ancestors {
        ignores.push(load(dir, ".ignore")?);
        if repo.as_ref().is_some_and(|r| dir.starts_with(r)) {
            gitignores.push(load(dir, ".gitignore")?);
        }
    }
    let (global, exclude) = if let Some(repo) = &repo {
        let (global, error) = GitignoreBuilder::new(repo).build_global();
        if let Some(e) = error {
            return Err(SyncError::backend(e));
        }
        let dotgit = repo.join(".git");
        let gitdir = if dotgit.is_dir() {
            dotgit
        } else {
            let text = std::fs::read_to_string(&dotgit)?;
            let path = text
                .trim()
                .strip_prefix("gitdir: ")
                .ok_or_else(|| SyncError::backend("invalid .git file"))?;
            repo.join(path)
        };
        let common = match std::fs::read_to_string(gitdir.join("commondir")) {
            Ok(path) => gitdir.join(path.trim()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => gitdir,
            Err(e) => return Err(e.into()),
        };
        let exclude_path = common.join("info/exclude");
        let mut builder = GitignoreBuilder::new(repo);
        match std::fs::metadata(&exclude_path) {
            Ok(_) => {
                if let Some(e) = builder.add(&exclude_path) {
                    return Err(SyncError::backend(e));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        (global, builder.build().map_err(SyncError::backend)?)
    } else {
        (Gitignore::empty(), Gitignore::empty())
    };
    let mut path = root.clone();
    let mut parts = rel.split('/').peekable();
    while let Some(part) = parts.next() {
        path.push(part);
        let is_dir = parts.peek().is_some();
        let mut result = None;
        for matcher in ignores
            .iter()
            .rev()
            .chain(gitignores.iter().rev())
            .chain([&exclude, &global])
        {
            let m = matcher.matched(&path, is_dir);
            if !m.is_none() {
                result = Some(m.is_ignore());
                break;
            }
        }
        if result == Some(true) {
            return Ok(true);
        }
        if is_dir {
            ignores.push(load(&path, ".ignore")?);
            if repo.is_some() {
                gitignores.push(load(&path, ".gitignore")?);
            }
        }
    }
    Ok(false)
}

/// Fold one successfully-applied action into the running baseline, deriving the
/// entry from whichever side is now authoritative (never a blind local rescan).
fn commit_baseline(base: &mut Tree, a: &Action, local: &Tree, remote: &Tree) {
    match a {
        Action::Push(rel) => {
            if let Some(m) = local.get(rel) {
                base.insert(rel.clone(), *m);
            }
        }
        Action::Pull(rel) => {
            if let Some(m) = remote.get(rel) {
                base.insert(rel.clone(), *m);
            }
        }
        Action::DelRemote(rel) | Action::DelLocal(rel) => {
            base.remove(rel);
        }
        Action::Conflict { rel, local_newer } => {
            let side = if *local_newer {
                local.get(rel)
            } else {
                remote.get(rel)
            };
            if let Some(m) = side {
                base.insert(rel.clone(), *m);
            }
        }
    }
}

async fn apply<R: RemoteFs>(
    fs: &R,
    base: &str,
    local_root: &Path,
    a: &Action,
    host: &str,
    local: &Tree,
    remote: &Tree,
) -> Result<(), SyncError> {
    let rmtime = |rel: &str| remote.get(rel).map(|m| m.mtime).unwrap_or(0);
    let expected = |rel: &str| (local.get(rel).copied(), remote.get(rel).copied());
    match a {
        Action::Push(rel) => push(fs, base, local_root, rel, expected(rel)).await,
        Action::Pull(rel) => {
            pull(
                fs,
                base,
                local_root,
                rel,
                rel,
                rmtime(rel),
                false,
                expected(rel),
            )
            .await
        }
        Action::DelRemote(rel) => fs.remove_file(&join(base, rel)).await,
        Action::DelLocal(rel) => {
            std::fs::remove_file(local_root.join(rel)).map_err(SyncError::from)
        }
        Action::Conflict { rel, local_newer } => {
            // Counter suffix: two conflicts on the same path within one wall-clock
            // second would otherwise share a tag and clobber the first aside-copy.
            static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Host labels are untrusted and must never add path components.
            let host: String = host
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();
            let tag = format!("{rel}.conflict-{host}-{}-{n}", now_secs());
            if *local_newer {
                pull(
                    fs,
                    base,
                    local_root,
                    rel,
                    &tag,
                    rmtime(rel),
                    true,
                    expected(rel),
                )
                .await?;
                push(fs, base, local_root, rel, expected(rel)).await
            } else {
                copy_local(local_root, rel, &tag)?; // save local aside
                pull(
                    fs,
                    base,
                    local_root,
                    rel,
                    rel,
                    rmtime(rel),
                    false,
                    expected(rel),
                )
                .await
            }
        }
    }
}

async fn revalidate<R: RemoteFs>(
    fs: &R,
    base: &str,
    local_root: &Path,
    rel: &str,
    expected: (Option<Meta>, Option<Meta>),
) -> Result<(), SyncError> {
    if checked_local_meta(local_root, rel).await? != expected.0
        || remote_path_meta(fs, base, rel).await? != expected.1
    {
        return Err(SyncError::backend(
            "path changed during transfer; retry sync",
        ));
    }
    Ok(())
}

async fn push<R: RemoteFs>(
    fs: &R,
    base: &str,
    local_root: &Path,
    rel: &str,
    expected: (Option<Meta>, Option<Meta>),
) -> Result<(), SyncError> {
    let lpath = local_root.join(rel);
    let rpath = join(base, rel);
    ensure_remote_dirs(fs, base, rel).await?;
    let mtime = local_mtime_secs(&lpath);
    // Stage uploads so interrupted transfers never truncate the destination.
    static NEXT_UPLOAD: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = NEXT_UPLOAD.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tag = format!("{}-{}-{seq}", std::process::id(), now_secs());
    let tmp = format!("{rpath}.{tag}.sync-tmp");
    let backup = format!("{rpath}.{tag}.sync-backup.sync-tmp");
    let mut lf = tokio::fs::File::open(&lpath).await?;
    let mut w = fs.create_new(&tmp).await?;
    let transfer = async {
        tokio::io::copy(&mut lf, &mut w).await?;
        w.shutdown().await?; // flush + close (adapter awaits SFTP CLOSE)
        drop(w);
        fs.set_mtime(&tmp, mtime).await?;
        let source = expected.0.ok_or_else(|| SyncError::backend("missing local source metadata"))?;
        if source.sha256 != Some(hash_remote(fs, &tmp, source.size).await?) {
            return Err(SyncError::backend("uploaded content does not match scanned source"));
        }
        revalidate(fs, base, local_root, rel, expected).await?;
        let destination_exists = expected.1.is_some();
        if destination_exists {
            fs.rename(&rpath, &backup).await?;
        }
        if let Err(error) = fs.rename(&tmp, &rpath).await {
            if destination_exists
                && let Err(restore) = fs.rename(&backup, &rpath).await
            {
                return Err(SyncError::backend(format!(
                    "publish failed ({error}); restoring previous file also failed ({restore}); backup remains at {backup}"
                )));
            }
            return Err(error);
        }
        if destination_exists {
            let _ = fs.remove_file(&backup).await;
        }
        Ok(())
    }.await;
    if transfer.is_err() {
        let _ = fs.remove_file(&tmp).await;
    }
    transfer
}

#[allow(clippy::too_many_arguments)]
async fn pull<R: RemoteFs>(
    fs: &R,
    base: &str,
    local_root: &Path,
    src_rel: &str,
    dst_rel: &str,
    mtime: i64,
    exclusive: bool,
    expected: (Option<Meta>, Option<Meta>),
) -> Result<(), SyncError> {
    let rpath = join(base, src_rel);
    let lpath = local_root.join(dst_rel);
    if let Some(parent) = lpath.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Create a unique sibling without following a pre-existing symlink.
    static NEXT_TEMP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut created = None;
    for _ in 0..128 {
        let seq = NEXT_TEMP.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut name = lpath.as_os_str().to_os_string();
        name.push(format!(".flow-{}-{seq}.sync-tmp", std::process::id()));
        let candidate = std::path::PathBuf::from(name);
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
            .await
        {
            Ok(file) => {
                created = Some((candidate, file));
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    let (tmp, mut lf) = created
        .ok_or_else(|| SyncError::backend("could not allocate a unique local staging file"))?;
    let _cleanup = RemoveFileOnDrop(tmp.clone());
    let mut r = fs.open_read(&rpath).await?;
    tokio::io::copy(&mut r, &mut lf).await?;
    lf.flush().await?;
    drop(lf);
    set_local_mtime(&tmp, mtime)?;
    let source = expected
        .1
        .ok_or_else(|| SyncError::backend("missing remote source metadata"))?;
    let staged_path = tmp.clone();
    let staged_hash = tokio::task::spawn_blocking(move || {
        let staged = std::fs::metadata(&staged_path)?;
        hash_local(&staged_path, &staged)
    })
    .await
    .map_err(SyncError::backend)??;
    if source.sha256 != Some(staged_hash) {
        return Err(SyncError::backend(
            "downloaded content does not match scanned source",
        ));
    }
    revalidate(fs, base, local_root, src_rel, expected).await?;
    if exclusive {
        // Publish without replacing an existing conflict copy or symlink.
        std::fs::hard_link(&tmp, &lpath)?;
    } else {
        std::fs::rename(&tmp, &lpath)?;
    }
    Ok(())
}

fn copy_local(local_root: &Path, rel: &str, dst_rel: &str) -> Result<(), SyncError> {
    let mut source = std::fs::File::open(local_root.join(rel))?;
    let dest = local_root.join(dst_rel);
    let mut target = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&dest)?;
    let result = std::io::copy(&mut source, &mut target);
    drop(target);
    if result.is_err() {
        let _ = std::fs::remove_file(dest);
    }
    result.map(|_| ()).map_err(SyncError::from)
}

async fn ensure_remote_dirs<R: RemoteFs>(fs: &R, base: &str, rel: &str) -> Result<(), SyncError> {
    if let Some((parents, _)) = rel.rsplit_once('/') {
        let mut cur = base.to_string();
        for parent in parents.split('/') {
            cur = join(&cur, parent);
            fs.mkdir(&cur).await?;
        }
    }
    Ok(())
}

// ---- scanning ---------------------------------------------------------------

/// Walk `root` and return its files as a [`Tree`]. `git` honours `.gitignore`
/// (and hidden/global/exclude); else a raw recursive walk. Conflict/temp copies
/// are skipped so they don't ping-pong.
pub fn scan_local(root: &Path, git: bool) -> Result<Tree, SyncError> {
    Ok(scan_local_shadowed(root, git)?.0)
}

/// Like [`scan_local`] but also returns the set of paths occupied by a symlink
/// (rather than a regular file). A symlink is never synced, but its path must
/// not read as *absent* — that would let reconcile delete the intact file on the
/// other side. See [`sync_once`].
fn scan_local_shadowed(root: &Path, git: bool) -> Result<(Tree, BTreeSet<String>), SyncError> {
    let mut out = Tree::new();
    let mut shadow = BTreeSet::new();
    let mut visible = BTreeSet::new();
    let walker = ignore::WalkBuilder::new(root)
        .standard_filters(git)
        .hidden(false)
        .git_ignore(git)
        .git_global(git)
        .git_exclude(git)
        .parents(git)
        .build();
    // A dropped walk/metadata error would make an unreadable file vanish from the
    // tree, which reconcile reads as a remote-side delete → local data loss. Abort.
    for entry in walker {
        let entry = entry.map_err(SyncError::backend)?;
        let Some(ft) = entry.file_type() else {
            continue;
        };
        let Ok(rel) = entry.path().strip_prefix(root) else {
            continue;
        };
        let rel = rel.to_str().ok_or_else(|| {
            SyncError::backend(format!("non-UTF-8 local path: {}", entry.path().display()))
        })?;
        visible.insert(rel.to_owned());
        if is_skippable(rel) {
            continue;
        }
        let rel = rel.replace('\\', "/");
        if ft.is_symlink() {
            shadow.insert(rel);
            continue;
        }
        if !ft.is_file() {
            continue;
        }
        let m = entry.metadata().map_err(SyncError::backend)?;
        let mtime = m
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        out.insert(
            rel,
            Meta {
                mtime,
                size: m.len(),
                sha256: Some(hash_local(entry.path(), &m)?),
            },
        );
    }
    if git {
        // The filtered walk omits ignored files and whole directories. Record
        // those omissions as shadows rather than interpreting them as deletes.
        for entry in ignore::WalkBuilder::new(root)
            .standard_filters(false)
            .build()
        {
            let entry = entry.map_err(SyncError::backend)?;
            let rel = entry
                .path()
                .strip_prefix(root)
                .map_err(SyncError::backend)?;
            let rel = rel.to_str().ok_or_else(|| {
                SyncError::backend(format!("non-UTF-8 local path: {}", entry.path().display()))
            })?;
            if !visible.contains(rel) {
                shadow.insert(rel.replace('\\', "/"));
            }
        }
    }
    Ok((out, shadow))
}

/// Recursively list the remote tree under `base` via [`RemoteFs`] (files only;
/// symlinks skipped).
pub async fn scan_remote<R: RemoteFs>(fs: &R, base: &str) -> Result<Tree, SyncError> {
    Ok(scan_remote_shadowed(fs, base).await?.0)
}

/// Like [`scan_remote`] but also returns the set of paths occupied by a symlink.
/// A symlinked path must not read as absent, or reconcile deletes the intact file
/// on the other side. See [`sync_once`].
async fn scan_remote_shadowed<R: RemoteFs>(
    fs: &R,
    base: &str,
) -> Result<(Tree, BTreeSet<String>), SyncError> {
    const MAX_DEPTH: usize = 64;
    let mut out = Tree::new();
    let mut shadow = BTreeSet::new();
    let mut stack = vec![String::new()];
    while let Some(rel) = stack.pop() {
        let dir = if rel.is_empty() {
            base.to_string()
        } else {
            join(base, &rel)
        };
        for e in fs.read_dir(&dir).await? {
            // Untrusted backend name: `.`/`..`/`a/../../etc` joined unchecked escapes
            // `base` on pull and drives `..` recursion. One choke point for every entry.
            if !is_safe_component(&e.name) {
                continue;
            }
            if e.name.ends_with(".sync-backup.sync-tmp") {
                return Err(SyncError::backend(format!(
                    "unfinished upload backup at {}; manual recovery required",
                    join(&dir, &e.name)
                )));
            }
            if is_skippable(&e.name) {
                continue;
            }
            let childrel = if rel.is_empty() {
                e.name.clone()
            } else {
                format!("{rel}/{}", e.name)
            };
            if e.is_symlink {
                shadow.insert(childrel);
                continue;
            }
            if e.is_dir {
                // Bound recursion against a hostile/buggy backend that synthesizes an
                // infinite directory tree (every READDIR returns a subdir). Fail closed,
                // not silent-skip: a silently omitted subtree reads as a deletion and
                // reconcile removes the intact local copy.
                if childrel.matches('/').count() >= MAX_DEPTH {
                    return Err(SyncError::backend(format!(
                        "remote tree exceeds max depth {MAX_DEPTH} at {childrel}"
                    )));
                }
                stack.push(childrel);
            } else {
                out.insert(
                    childrel,
                    Meta {
                        mtime: e.mtime,
                        size: e.size,
                        sha256: Some(hash_remote(fs, &join(&dir, &e.name), e.size).await?),
                    },
                );
            }
        }
    }
    Ok((out, shadow))
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
    use std::fmt::Write;
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    let dir = config_dir().join("sftp-sync");
    let _ = std::fs::create_dir_all(&dir);
    dir.join(format!("{hex}.json"))
}

pub fn load_baseline(path: &Path) -> Result<Tree, SyncError> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(SyncError::backend),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Tree::new()),
        Err(e) => Err(e.into()),
    }
}

pub fn save_baseline(path: &Path, tree: &Tree) -> Result<(), SyncError> {
    use std::io::Write;
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let data = serde_json::to_vec(tree).map_err(SyncError::backend)?;
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut name = path.as_os_str().to_os_string();
    name.push(format!(".{}-{n}.tmp", std::process::id()));
    let tmp = std::path::PathBuf::from(name);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)?;
    let _cleanup = RemoveFileOnDrop(tmp.clone());
    file.write_all(&data)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, path)?;
    Ok(())
}

// ---- small self-contained helpers -------------------------------------------

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
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
fn set_local_mtime(p: &Path, secs: i64) -> Result<(), SyncError> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(p.as_os_str().as_bytes()).map_err(SyncError::backend)?;
    let t = libc::timespec {
        tv_sec: secs as libc::time_t,
        tv_nsec: 0,
    };
    let times = [t, t];
    if unsafe { libc::utimensat(libc::AT_FDCWD, c.as_ptr(), times.as_ptr(), 0) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(not(unix))]
fn set_local_mtime(p: &Path, secs: i64) -> Result<(), SyncError> {
    let time = if secs >= 0 {
        UNIX_EPOCH.checked_add(std::time::Duration::from_secs(secs as u64))
    } else {
        UNIX_EPOCH.checked_sub(std::time::Duration::from_secs(secs.unsigned_abs()))
    }
    .ok_or_else(|| SyncError::backend("mtime is outside the supported range"))?;
    std::fs::File::options()
        .write(true)
        .open(p)?
        .set_times(std::fs::FileTimes::new().set_modified(time))?;
    Ok(())
}

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
        Meta {
            mtime,
            size,
            sha256: None,
        }
    }
    fn t(pairs: &[(&str, Meta)]) -> Tree {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    #[test]
    fn conflict_copy_does_not_replace_existing_file() {
        let root =
            std::env::temp_dir().join(format!("sftp-sync-conflict-copy-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("source"), "source").unwrap();
        std::fs::write(root.join("conflict"), "existing").unwrap();
        assert!(copy_local(&root, "source", "conflict").is_err());
        assert_eq!(
            std::fs::read_to_string(root.join("conflict")).unwrap(),
            "existing"
        );
        std::fs::remove_file(root.join("source")).unwrap();
        std::fs::remove_file(root.join("conflict")).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

    #[test]
    fn first_run_unions_never_deletes() {
        let local = t(&[("a", m(1, 1))]);
        let remote = t(&[("b", m(1, 1))]);
        let acts = reconcile(&local, &remote, &Tree::new(), false);
        assert!(acts.contains(&Action::Push("a".into())));
        assert!(acts.contains(&Action::Pull("b".into())));
        assert!(
            !acts
                .iter()
                .any(|a| matches!(a, Action::DelLocal(_) | Action::DelRemote(_)))
        );
    }

    #[test]
    fn in_sync_pair_is_noop() {
        let both = t(&[("a", m(5, 10))]);
        assert!(reconcile(&both, &both, &both, true).is_empty());
    }

    #[test]
    fn one_way_replaces_changed_non_authoritative_file() {
        let base = t(&[("a", m(1, 1))]);
        let edited = t(&[("a", m(2, 2))]);
        assert_eq!(
            filter_direction(
                reconcile(&base, &edited, &base, true),
                Direction::PushOnly,
                &base,
                &edited
            ),
            vec![Action::Push("a".into())]
        );
        assert_eq!(
            filter_direction(
                reconcile(&edited, &base, &base, true),
                Direction::PullOnly,
                &edited,
                &base
            ),
            vec![Action::Pull("a".into())]
        );
    }

    #[test]
    fn one_way_restores_deleted_non_authoritative_file() {
        let base = t(&[("a", m(1, 1))]);
        let empty = Tree::new();
        assert_eq!(
            filter_direction(
                reconcile(&base, &empty, &base, true),
                Direction::PushOnly,
                &base,
                &empty
            ),
            vec![Action::Push("a".into())]
        );
        assert_eq!(
            filter_direction(
                reconcile(&empty, &base, &base, true),
                Direction::PullOnly,
                &empty,
                &base
            ),
            vec![Action::Pull("a".into())]
        );
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
        assert_eq!(
            acts,
            vec![Action::Conflict {
                rel: "a".into(),
                local_newer: true
            }]
        );
    }
}
