//! Async SFTP v3 client: [`SftpSession`] and the file/metadata types under
//! [`fs`]. A background task owns the stream's read half and routes each response
//! to its request by id; requests serialize writes behind a mutex.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex, oneshot};

use super::protocol::{CheckFile, File as NameFile, FileAttributes, OpenFlags, Status, StatusCode};
use super::{Cursor, PacketBuf, SFTP_VERSION, frame, fxp, read_packet};

pub use error::Error;

type SftpResult<T> = Result<T, Error>;

pub mod error {
    use super::super::protocol::Status;

    #[derive(Debug)]
    pub enum Error {
        Io(std::io::Error),
        /// Malformed or unexpected packet from the server.
        Protocol(String),
        Status(Status),
        /// The server does not implement the requested operation/extension.
        Unsupported(String),
        ConnectionLost,
    }

    impl std::fmt::Display for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Error::Io(e) => write!(f, "sftp io error: {e}"),
                Error::Protocol(m) => write!(f, "sftp protocol error: {m}"),
                Error::Status(s) => {
                    write!(f, "sftp status {:?}: {}", s.status_code, s.error_message)
                }
                Error::Unsupported(m) => write!(f, "sftp operation unsupported: {m}"),
                Error::ConnectionLost => write!(f, "sftp channel closed"),
            }
        }
    }

    impl std::error::Error for Error {}

    impl From<std::io::Error> for Error {
        fn from(e: std::io::Error) -> Self {
            Error::Io(e)
        }
    }
}

/// A parsed server response, routed to a waiting request by id.
enum Reply {
    Status(Status),
    Handle(Vec<u8>),
    Data(Vec<u8>),
    Name(Vec<NameFile>),
    Attrs(FileAttributes),
    Extended(Vec<u8>),
}

struct Inner {
    writer: Mutex<Box<dyn AsyncWrite + Send + Unpin>>,
    pending: StdMutex<Pending>,
    next_id: AtomicU32,
    extensions: HashMap<String, String>,
}

#[derive(Default)]
struct Pending {
    closed: bool,
    requests: HashMap<u32, oneshot::Sender<Reply>>,
}

// Removing a waiter on cancellation also prevents abandoned requests from
// accumulating indefinitely when the server never answers.
struct RequestGuard {
    inner: Arc<Inner>,
    id: u32,
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.inner.pending.lock().unwrap().requests.remove(&self.id);
    }
}

// Once a frame starts writing, cancellation or an error can leave an unknown
// prefix on the wire. Never permit another request to append to that stream.
struct WriteGuard(Arc<Inner>, bool);

impl Drop for WriteGuard {
    fn drop(&mut self) {
        if self.1 {
            let mut pending = self.0.pending.lock().unwrap();
            pending.closed = true;
            pending.requests.clear();
        }
    }
}

/// An SFTP v3 client session. Cheap to clone (shared inner state).
#[derive(Clone)]
pub struct SftpSession {
    inner: Arc<Inner>,
}

impl SftpSession {
    /// Handshake over `stream` and start the background reader task.
    pub async fn new<S>(stream: S) -> SftpResult<Self>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (mut rd, mut wr) = tokio::io::split(stream);

        let mut init = PacketBuf::new(fxp::INIT);
        init.u32(SFTP_VERSION);
        wr.write_all(&frame(&init.0)).await?;
        wr.flush().await?;

        let body = read_packet(&mut rd).await?;
        let mut cur = Cursor::new(&body);
        if cur.u8().map_err(|_| proto_err("short VERSION"))? != fxp::VERSION {
            return Err(proto_err("expected VERSION"));
        }
        let server_version = cur.u32().map_err(|_| proto_err("short VERSION"))?;
        if server_version != SFTP_VERSION {
            return Err(proto_err("unsupported SFTP version"));
        }
        let mut extensions = HashMap::new();
        while cur.remaining() > 0 {
            let name = cur.string().map_err(|_| proto_err("bad extension"))?;
            let data = cur.string().map_err(|_| proto_err("bad extension"))?;
            extensions.insert(name, data);
        }

        let inner = Arc::new(Inner {
            writer: Mutex::new(Box::new(wr)),
            pending: StdMutex::new(Pending::default()),
            next_id: AtomicU32::new(1),
            extensions,
        });

        let reader_inner = inner.clone();
        tokio::spawn(async move { read_loop(rd, reader_inner).await });

        Ok(SftpSession { inner })
    }

    fn next_id(&self) -> u32 {
        self.inner.next_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn request(&self, id: u32, body: Vec<u8>) -> SftpResult<Reply> {
        let (tx, mut rx) = oneshot::channel();
        {
            let mut pending = self.inner.pending.lock().unwrap();
            if pending.closed {
                return Err(Error::ConnectionLost);
            }
            pending.requests.insert(id, tx);
        }
        let _request_guard = RequestGuard {
            inner: self.inner.clone(),
            id,
        };
        let write = async {
            let mut w = self.inner.writer.lock().await;
            if self.inner.pending.lock().unwrap().closed {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "sftp channel closed",
                ));
            }
            let mut guard = WriteGuard(self.inner.clone(), true);
            w.write_all(&frame(&body)).await?;
            w.flush().await?;
            guard.1 = false;
            Ok(())
        };
        tokio::pin!(write);
        let result = std::future::poll_fn(|cx| {
            use std::future::Future;
            use std::task::Poll;

            if let Poll::Ready(reply) = std::pin::Pin::new(&mut rx).poll(cx) {
                return Poll::Ready(Some(reply.map_err(|_| Error::ConnectionLost)));
            }
            match write.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(None),
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(Error::Io(e)))),
                Poll::Pending => Poll::Pending,
            }
        })
        .await;
        if let Some(result) = result {
            return result;
        }
        rx.await.map_err(|_| Error::ConnectionLost)
    }

    async fn expect_status(&self, id: u32, body: Vec<u8>) -> SftpResult<()> {
        match self.request(id, body).await? {
            Reply::Status(s) if s.status_code == StatusCode::Ok => Ok(()),
            Reply::Status(s) => Err(Error::Status(s)),
            _ => Err(proto_err("expected STATUS")),
        }
    }

    async fn expect_handle(&self, id: u32, body: Vec<u8>) -> SftpResult<Vec<u8>> {
        match self.request(id, body).await? {
            Reply::Handle(h) => Ok(h),
            Reply::Status(s) => Err(Error::Status(s)),
            _ => Err(proto_err("expected HANDLE")),
        }
    }

    async fn expect_attrs(&self, id: u32, body: Vec<u8>) -> SftpResult<FileAttributes> {
        match self.request(id, body).await? {
            Reply::Attrs(a) => Ok(a),
            Reply::Status(s) => Err(Error::Status(s)),
            _ => Err(proto_err("expected ATTRS")),
        }
    }

    async fn expect_name(&self, id: u32, body: Vec<u8>) -> SftpResult<Vec<NameFile>> {
        match self.request(id, body).await? {
            Reply::Name(n) => Ok(n),
            Reply::Status(s) => Err(Error::Status(s)),
            _ => Err(proto_err("expected NAME")),
        }
    }

    async fn open_handle(&self, path: &str, flags: OpenFlags) -> SftpResult<Vec<u8>> {
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::OPEN, id);
        b.string(path);
        b.u32(flags.bits());
        FileAttributes::empty().encode(&mut b);
        self.expect_handle(id, b.0).await
    }

    pub async fn open(&self, path: &str) -> SftpResult<fs::File> {
        self.open_with_flags(path, OpenFlags::READ).await
    }

    pub async fn open_with_flags(&self, path: &str, flags: OpenFlags) -> SftpResult<fs::File> {
        let handle = self.open_handle(path, flags).await?;
        Ok(fs::File::new(self.clone(), handle))
    }

    /// Create (or truncate) a file for writing.
    pub async fn create(&self, path: &str) -> SftpResult<fs::File> {
        let flags = OpenFlags::WRITE | OpenFlags::CREATE | OpenFlags::TRUNCATE;
        let handle = self.open_handle(path, flags).await?;
        Ok(fs::File::new(self.clone(), handle))
    }

    async fn read_at(&self, handle: &[u8], offset: u64, len: u32) -> SftpResult<Vec<u8>> {
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::READ, id);
        b.bytes(handle);
        b.u64(offset);
        b.u32(len);
        match self.request(id, b.0).await? {
            Reply::Data(d) => Ok(d),
            Reply::Status(s) if s.status_code == StatusCode::Eof => Ok(Vec::new()),
            Reply::Status(s) => Err(Error::Status(s)),
            _ => Err(proto_err("expected DATA")),
        }
    }

    async fn write_at(&self, handle: &[u8], offset: u64, data: &[u8]) -> SftpResult<()> {
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::WRITE, id);
        b.bytes(handle);
        b.u64(offset);
        b.bytes(data);
        self.expect_status(id, b.0).await
    }

    async fn close(&self, handle: &[u8]) -> SftpResult<()> {
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::CLOSE, id);
        b.bytes(handle);
        self.expect_status(id, b.0).await
    }

    /// List a directory; each [`DirEntry`] carries its parent for [`DirEntry::path`].
    pub async fn read_dir(&self, path: &str) -> SftpResult<ReadDir> {
        const MAX_ENTRIES: usize = 1_000_000;
        const MAX_NAME_BYTES: usize = 64 * 1024 * 1024;
        let handle = {
            let id = self.next_id();
            let mut b = PacketBuf::request(fxp::OPENDIR, id);
            b.string(path);
            self.expect_handle(id, b.0).await?
        };

        let mut entries = Vec::new();
        let mut name_bytes = 0usize;
        loop {
            let id = self.next_id();
            let mut b = PacketBuf::request(fxp::READDIR, id);
            b.bytes(&handle);
            match self.request(id, b.0).await {
                Ok(Reply::Name(mut names)) => {
                    let batch_bytes = names.iter().try_fold(0usize, |sum, name| {
                        sum.checked_add(name.filename.len())?
                            .checked_add(name.longname.len())
                    });
                    let total = batch_bytes.and_then(|bytes| name_bytes.checked_add(bytes));
                    if names.is_empty()
                        || entries.len().saturating_add(names.len()) > MAX_ENTRIES
                        || total.is_none_or(|size| size > MAX_NAME_BYTES)
                    {
                        let _ = self.close(&handle).await;
                        return Err(proto_err("invalid or oversized directory listing"));
                    }
                    name_bytes = total.unwrap();
                    entries.append(&mut names);
                }
                Ok(Reply::Status(s)) if s.status_code == StatusCode::Eof => break,
                Ok(Reply::Status(s)) => {
                    let _ = self.close(&handle).await;
                    return Err(Error::Status(s));
                }
                Ok(_) => {
                    let _ = self.close(&handle).await;
                    return Err(proto_err("expected NAME"));
                }
                Err(e) => {
                    let _ = self.close(&handle).await;
                    return Err(e);
                }
            }
        }
        let _ = self.close(&handle).await;
        let dir = path.to_owned();
        let entries: Vec<DirEntry> = entries
            .into_iter()
            .map(|file| DirEntry {
                dir: dir.clone(),
                file,
            })
            .collect();
        Ok(ReadDir(entries.into_iter()))
    }

    pub async fn set_metadata(&self, path: &str, attrs: FileAttributes) -> SftpResult<()> {
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::SETSTAT, id);
        b.string(path);
        attrs.encode(&mut b);
        self.expect_status(id, b.0).await
    }

    /// Attributes of `path`, following symlinks.
    pub async fn metadata(&self, path: &str) -> SftpResult<FileAttributes> {
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::STAT, id);
        b.string(path);
        self.expect_attrs(id, b.0).await
    }

    /// Attributes of `path`, not following symlinks.
    pub async fn symlink_metadata(&self, path: &str) -> SftpResult<FileAttributes> {
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::LSTAT, id);
        b.string(path);
        self.expect_attrs(id, b.0).await
    }

    pub async fn remove_file(&self, path: &str) -> SftpResult<()> {
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::REMOVE, id);
        b.string(path);
        self.expect_status(id, b.0).await
    }

    pub async fn create_dir(&self, path: &str) -> SftpResult<()> {
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::MKDIR, id);
        b.string(path);
        FileAttributes::empty().encode(&mut b);
        self.expect_status(id, b.0).await
    }

    pub async fn remove_dir(&self, path: &str) -> SftpResult<()> {
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::RMDIR, id);
        b.string(path);
        self.expect_status(id, b.0).await
    }

    pub async fn rename(&self, from: &str, to: &str) -> SftpResult<()> {
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::RENAME, id);
        b.string(from);
        b.string(to);
        self.expect_status(id, b.0).await
    }

    pub async fn symlink(&self, target: &str, link: &str) -> SftpResult<()> {
        // OpenSSH order: linkpath then targetpath.
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::SYMLINK, id);
        b.string(link);
        b.string(target);
        self.expect_status(id, b.0).await
    }

    /// Resolve `path` to an absolute path.
    pub async fn canonicalize(&self, path: &str) -> SftpResult<String> {
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::REALPATH, id);
        b.string(path);
        let names = self.expect_name(id, b.0).await?;
        names
            .into_iter()
            .next()
            .map(|f| f.filename)
            .ok_or_else(|| proto_err("empty NAME for realpath"))
    }

    pub async fn read_link(&self, path: &str) -> SftpResult<String> {
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::READLINK, id);
        b.string(path);
        let names = self.expect_name(id, b.0).await?;
        names
            .into_iter()
            .next()
            .map(|f| f.filename)
            .ok_or_else(|| proto_err("empty NAME for readlink"))
    }

    /// The server's advertised `check-file` hash-algorithm list, if any.
    pub fn check_file_algorithms(&self) -> Option<&str> {
        self.inner.extensions.get("check-file").map(String::as_str)
    }

    /// `check-file` extension: server-computed hash of a file by name, no download.
    pub async fn check_file(
        &self,
        path: &str,
        algorithms: &str,
        start_offset: u64,
        length: u64,
        block_size: u32,
    ) -> SftpResult<CheckFile> {
        if !self.inner.extensions.contains_key("check-file") {
            return Err(Error::Unsupported("check-file".into()));
        }
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::EXTENDED, id);
        b.string("check-file-name");
        b.string(path);
        b.string(algorithms);
        b.u64(start_offset);
        b.u64(length);
        b.u32(block_size);
        match self.request(id, b.0).await? {
            Reply::Extended(data) => {
                let mut cur = Cursor::new(&data);
                let algo = cur
                    .string()
                    .map_err(|_| proto_err("bad check-file reply"))?;
                Ok(CheckFile {
                    hash_algorithm: algo,
                    hashes: cur.rest().to_vec(),
                })
            }
            Reply::Status(s) if s.status_code == StatusCode::OpUnsupported => {
                Err(Error::Unsupported(s.error_message))
            }
            Reply::Status(s) => Err(Error::Status(s)),
            _ => Err(proto_err("expected EXTENDED_REPLY")),
        }
    }
}

fn proto_err(msg: &str) -> Error {
    Error::Protocol(msg.to_owned())
}

async fn read_loop<R: AsyncRead + Unpin>(mut rd: R, inner: Arc<Inner>) {
    loop {
        let body = match read_packet(&mut rd).await {
            Ok(b) => b,
            Err(_) => break,
        };
        // Recover the id first so a malformed body still wakes its waiter with an
        // error rather than hanging it until the connection drops.
        let mut head = Cursor::new(&body);
        let (Ok(_ty), Ok(id)) = (head.u8(), head.u32()) else {
            break;
        };
        let tx = inner.pending.lock().unwrap().requests.remove(&id);
        if let Some(tx) = tx {
            let reply = parse_reply(&body).map(|(_, r)| r).unwrap_or_else(|| {
                Reply::Status(Status {
                    id,
                    status_code: StatusCode::BadMessage,
                    error_message: "malformed reply".into(),
                    language_tag: String::new(),
                })
            });
            let _ = tx.send(reply);
        }
    }
    let mut pending = inner.pending.lock().unwrap();
    pending.closed = true;
    pending.requests.clear(); // fail in-flight requests rather than hang
}

fn parse_reply(body: &[u8]) -> Option<(u32, Reply)> {
    let mut cur = Cursor::new(body);
    let ty = cur.u8().ok()?;
    let id = cur.u32().ok()?;
    let reply = match ty {
        fxp::STATUS => {
            let code = cur.u32().ok()?;
            let error_message = cur.string().ok()?;
            let language_tag = cur.string().ok()?;
            Reply::Status(Status {
                id,
                status_code: StatusCode::from_u32(code),
                error_message,
                language_tag,
            })
        }
        fxp::HANDLE => Reply::Handle(cur.bytes().ok()?),
        fxp::DATA => Reply::Data(cur.bytes().ok()?),
        fxp::ATTRS => Reply::Attrs(FileAttributes::decode(&mut cur).ok()?),
        fxp::EXTENDED_REPLY => Reply::Extended(cur.rest().to_vec()),
        fxp::NAME => {
            let count = cur.u32().ok()?;
            // Cap the reservation: a hostile `count` (e.g. 0xFFFFFFFF) would else
            // preallocate hundreds of GiB. The loop grows it as entries decode.
            let mut files = Vec::with_capacity((count as usize).min(1024));
            for _ in 0..count {
                let filename = cur.string().ok()?;
                let longname = cur.string().ok()?;
                let attrs = FileAttributes::decode(&mut cur).ok()?;
                files.push(NameFile {
                    filename,
                    longname,
                    attrs,
                });
            }
            Reply::Name(files)
        }
        _ => return None,
    };
    Some((id, reply))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inner() -> Arc<Inner> {
        Arc::new(Inner {
            writer: Mutex::new(Box::new(tokio::io::sink())),
            pending: StdMutex::new(Pending::default()),
            next_id: AtomicU32::new(1),
            extensions: HashMap::new(),
        })
    }

    #[tokio::test]
    async fn short_response_closes_reader_and_rejects_future_requests() {
        let inner = inner();
        let (tx, rx) = oneshot::channel();
        inner.pending.lock().unwrap().requests.insert(1, tx);
        let (reader, mut peer) = tokio::io::duplex(64);
        let task = tokio::spawn(read_loop(reader, inner.clone()));
        peer.write_all(&frame(&[fxp::STATUS, 0, 0])).await.unwrap();
        task.await.unwrap();
        assert!(rx.await.is_err());

        let session = SftpSession { inner };
        assert!(matches!(
            session.request(2, vec![]).await,
            Err(Error::ConnectionLost)
        ));
    }

    #[tokio::test]
    async fn eof_closes_reader_and_rejects_future_requests() {
        let inner = inner();
        let (reader, peer) = tokio::io::duplex(64);
        drop(peer);
        read_loop(reader, inner.clone()).await;
        let session = SftpSession { inner };
        assert!(matches!(
            session.request(1, vec![]).await,
            Err(Error::ConnectionLost)
        ));
    }

    #[tokio::test]
    async fn reader_death_wakes_request_blocked_on_write() {
        let (writer, _peer) = tokio::io::duplex(1);
        let inner = Arc::new(Inner {
            writer: Mutex::new(Box::new(writer)),
            pending: StdMutex::new(Pending::default()),
            next_id: AtomicU32::new(1),
            extensions: HashMap::new(),
        });
        let session = SftpSession {
            inner: inner.clone(),
        };
        let request = tokio::spawn(async move { session.request(1, vec![0; 32]).await });
        tokio::task::yield_now().await;
        assert_eq!(inner.pending.lock().unwrap().requests.len(), 1);
        let (reader, peer) = tokio::io::duplex(1);
        drop(peer);
        read_loop(reader, inner).await;
        assert!(matches!(request.await.unwrap(), Err(Error::ConnectionLost)));
    }

    #[tokio::test]
    async fn cancelling_partial_frame_closes_session() {
        let (writer, _peer) = tokio::io::duplex(1);
        let inner = Arc::new(Inner {
            writer: Mutex::new(Box::new(writer)),
            pending: StdMutex::new(Pending::default()),
            next_id: AtomicU32::new(1),
            extensions: HashMap::new(),
        });
        let session = SftpSession {
            inner: inner.clone(),
        };
        let other = session.clone();
        let task = tokio::spawn(async move { other.request(1, vec![0; 32]).await });
        tokio::task::yield_now().await;
        task.abort();
        let _ = task.await;
        assert!(inner.pending.lock().unwrap().requests.is_empty());
        assert!(matches!(
            session.request(2, vec![]).await,
            Err(Error::ConnectionLost)
        ));
    }

    #[test]
    fn status_requires_both_complete_strings() {
        let mut packet = PacketBuf::request(fxp::STATUS, 1);
        packet.u32(0);
        assert!(parse_reply(&packet.0).is_none());
        packet.string("");
        assert!(parse_reply(&packet.0).is_none());
        packet.string("");
        assert!(matches!(
            parse_reply(&packet.0),
            Some((1, Reply::Status(_)))
        ));
        packet.0.pop();
        assert!(parse_reply(&packet.0).is_none());
    }
}

/// One directory entry, carrying its parent so [`path`](DirEntry::path) can
/// return the child's full path.
pub struct DirEntry {
    dir: String,
    file: NameFile,
}

impl DirEntry {
    pub fn file_name(&self) -> String {
        self.file.filename.clone()
    }
    pub fn file_type(&self) -> super::protocol::FileType {
        self.file.file_type()
    }
    pub fn metadata(&self) -> FileAttributes {
        self.file.attrs
    }
    /// Parent directory joined with the file name.
    pub fn path(&self) -> String {
        let base = self.dir.trim_end_matches('/');
        format!("{base}/{}", self.file.filename)
    }
}

pub struct ReadDir(std::vec::IntoIter<DirEntry>);

impl Iterator for ReadDir {
    type Item = DirEntry;
    fn next(&mut self) -> Option<DirEntry> {
        self.0.next()
    }
}

pub mod fs {
    use std::future::Future;
    use std::io::{self, SeekFrom};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};

    use super::{Error, SftpSession};

    pub type Metadata = super::super::protocol::FileAttributes;

    const MAX_CHUNK: usize = 32 * 1024;

    type IoFuture<T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send>>;

    enum Operation {
        Read(IoFuture<Vec<u8>>, usize),
        Write(IoFuture<()>, Vec<u8>),
    }

    /// A remote file handle (`AsyncRead`/`AsyncWrite`/`AsyncSeek`). `shutdown`
    /// awaits an explicit CLOSE; dropping without it spawns a background CLOSE so
    /// the handle is never leaked.
    pub struct File {
        session: SftpSession,
        handle: Vec<u8>,
        offset: u64,
        closed: bool,
        closing: bool,
        op: Option<Operation>,
        close_op: Option<IoFuture<()>>,
    }

    impl File {
        pub(super) fn new(session: SftpSession, handle: Vec<u8>) -> Self {
            File {
                session,
                handle,
                offset: 0,
                closed: false,
                closing: false,
                op: None,
                close_op: None,
            }
        }

        /// Close the remote handle, awaiting the server's CLOSE acknowledgement.
        ///
        /// Prefer this over relying on `Drop` when opening many handles
        /// concurrently through the read API: `Drop` can only fire-and-forget the
        /// CLOSE (and skips it entirely when no runtime is current), so open
        /// handles can outpace retirement and trip the server's open-handle limit.
        pub async fn close(mut self) -> io::Result<()> {
            tokio::io::AsyncWriteExt::shutdown(&mut self).await
        }

        // Finish an abandoned operation before changing direction or closing.
        // A completed read has not been delivered, so it does not advance offset.
        fn poll_abandoned(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let result = match self.op.as_mut() {
                None => return Poll::Ready(Ok(())),
                Some(Operation::Read(fut, _)) => match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => result.map(|_| 0),
                },
                Some(Operation::Write(fut, data)) => match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => result.map(|_| data.len()),
                },
            };
            self.op = None;
            match result {
                Ok(n) => {
                    self.offset += n as u64;
                    Poll::Ready(Ok(()))
                }
                Err(e) => Poll::Ready(Err(e)),
            }
        }

        fn check_open(&self) -> io::Result<()> {
            if self.closed || self.closing {
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "file is closing or closed",
                ))
            } else {
                Ok(())
            }
        }
    }

    fn io_err(e: Error) -> io::Error {
        io::Error::other(e)
    }

    impl AsyncRead for File {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = &mut *self;
            this.check_open()?;
            if buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }
            if matches!(this.op, Some(Operation::Write(..))) {
                std::task::ready!(this.poll_abandoned(cx))?;
            }
            if this.op.is_none() {
                let want = buf
                    .remaining()
                    .min(MAX_CHUNK)
                    .min((u64::MAX - this.offset) as usize);
                if want == 0 {
                    return Poll::Ready(Err(io::Error::other("file offset overflow")));
                }
                let s = this.session.clone();
                let h = this.handle.clone();
                let off = this.offset;
                this.op = Some(Operation::Read(
                    Box::pin(async move { s.read_at(&h, off, want as u32).await.map_err(io_err) }),
                    want,
                ));
            }
            let Some(Operation::Read(fut, want)) = this.op.as_mut() else {
                unreachable!()
            };
            let want = *want;
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(data)) => {
                    this.op = None;
                    if data.len() > want {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "oversized read response",
                        )));
                    }
                    // A hostile/buggy server may reply with more bytes than requested;
                    // clamp to the caller's buffer instead of letting put_slice panic.
                    let n = data.len().min(buf.remaining());
                    this.offset += n as u64;
                    buf.put_slice(&data[..n]);
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    this.op = None;
                    Poll::Ready(Err(e))
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl AsyncWrite for File {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = &mut *self;
            this.check_open()?;
            let n = buf.len().min(MAX_CHUNK);
            if n == 0 {
                return Poll::Ready(Ok(0));
            }
            let matches_write = match this.op.as_ref() {
                Some(Operation::Write(_, data)) => buf.starts_with(data),
                _ => false,
            };
            if this.op.is_some() && !matches_write {
                std::task::ready!(this.poll_abandoned(cx))?;
            }
            if this.op.is_none() {
                if this.offset.checked_add(n as u64).is_none() {
                    return Poll::Ready(Err(io::Error::other("file offset overflow")));
                }
                let s = this.session.clone();
                let h = this.handle.clone();
                let off = this.offset;
                let chunk = buf[..n].to_vec();
                let sent = chunk.clone();
                this.op = Some(Operation::Write(
                    Box::pin(async move { s.write_at(&h, off, &chunk).await.map_err(io_err) }),
                    sent,
                ));
            }
            let Some(Operation::Write(fut, data)) = this.op.as_mut() else {
                unreachable!()
            };
            let written = data.len();
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    this.op = None;
                    this.offset += written as u64;
                    Poll::Ready(Ok(written))
                }
                Poll::Ready(Err(e)) => {
                    this.op = None;
                    Poll::Ready(Err(e))
                }
                Poll::Pending => Poll::Pending,
            }
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.check_open()?;
            self.poll_abandoned(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let this = &mut *self;
            if this.closed {
                return Poll::Ready(Ok(()));
            }
            this.closing = true;
            if this.close_op.is_none() {
                std::task::ready!(this.poll_abandoned(cx))?;
                let s = this.session.clone();
                let h = this.handle.clone();
                this.close_op = Some(Box::pin(async move { s.close(&h).await.map_err(io_err) }));
            }
            let fut = this.close_op.as_mut().unwrap();
            match Pin::new(fut).poll(cx) {
                Poll::Ready(res) => {
                    this.close_op = None;
                    // Mark closed only on success; a failed CLOSE leaves the handle
                    // live for `Drop` to retire.
                    this.closed = res.is_ok();
                    Poll::Ready(res)
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl AsyncSeek for File {
        fn start_seek(mut self: Pin<&mut Self>, pos: SeekFrom) -> io::Result<()> {
            self.check_open()?;
            if self.op.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "operation pending; flush before seeking",
                ));
            }
            // Every read/write carries an explicit offset, so a seek is local-only.
            // `End` needs the size we don't track here.
            let new = match pos {
                SeekFrom::Start(n) => n,
                SeekFrom::Current(d) => self
                    .offset
                    .checked_add_signed(d)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid seek"))?,
                SeekFrom::End(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "seek from end unsupported",
                    ));
                }
            };
            self.offset = new;
            Ok(())
        }
        fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
            Poll::Ready(Ok(self.offset))
        }
    }

    impl Drop for File {
        fn drop(&mut self) {
            if self.closed {
                return;
            }
            // Cancel a partially transmitted request before scheduling CLOSE.
            self.op = None;
            self.close_op = None;
            if let Ok(rt) = tokio::runtime::Handle::try_current() {
                let s = self.session.clone();
                let h = std::mem::take(&mut self.handle);
                rt.spawn(async move {
                    let _ = s.close(&h).await;
                });
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::sync::Arc;
        use std::sync::atomic::AtomicU32;
        use tokio::sync::{Mutex, oneshot};

        fn file() -> File {
            let inner = super::super::Inner {
                writer: Mutex::new(Box::new(tokio::io::sink())),
                pending: std::sync::Mutex::new(Default::default()),
                next_id: AtomicU32::new(1),
                extensions: Default::default(),
            };
            File::new(
                SftpSession {
                    inner: Arc::new(inner),
                },
                b"handle".to_vec(),
            )
        }

        #[test]
        fn pending_write_is_drained_by_flush_and_seek_is_rejected() {
            let mut file = file();
            let (tx, rx) = oneshot::channel();
            file.op = Some(Operation::Write(
                Box::pin(async move { rx.await.unwrap() }),
                b"abc".to_vec(),
            ));
            let mut cx = Context::from_waker(std::task::Waker::noop());
            assert!(Pin::new(&mut file).poll_flush(&mut cx).is_pending());
            assert_eq!(
                Pin::new(&mut file)
                    .start_seek(SeekFrom::Start(4))
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            tx.send(Ok(())).unwrap();
            assert!(Pin::new(&mut file).poll_flush(&mut cx).is_ready());
            assert_eq!(file.offset, 3);
            assert!(
                Pin::new(&mut file)
                    .start_seek(SeekFrom::Current(-4))
                    .is_err()
            );
        }

        #[test]
        fn cancelled_read_then_write_does_not_consume_read_data() {
            let mut file = file();
            file.op = Some(Operation::Read(
                Box::pin(async { Ok(b"hello".to_vec()) }),
                5,
            ));
            let mut cx = Context::from_waker(std::task::Waker::noop());
            assert!(Pin::new(&mut file).poll_write(&mut cx, b"x").is_pending());
            assert_eq!(file.offset, 0);
            assert!(matches!(file.op, Some(Operation::Write(..))));
        }

        #[test]
        fn cancelled_write_then_read_advances_before_reading() {
            let mut file = file();
            file.op = Some(Operation::Write(
                Box::pin(async { Ok(()) }),
                b"abc".to_vec(),
            ));
            let mut cx = Context::from_waker(std::task::Waker::noop());
            let mut bytes = [0; 4];
            let mut buf = ReadBuf::new(&mut bytes);
            assert!(
                Pin::new(&mut file)
                    .poll_read(&mut cx, &mut buf)
                    .is_pending()
            );
            assert_eq!(file.offset, 3);
            assert!(matches!(file.op, Some(Operation::Read(..))));
        }

        #[test]
        fn shutdown_waits_for_write_and_blocks_new_io() {
            let mut file = file();
            let (tx, rx) = oneshot::channel();
            file.op = Some(Operation::Write(
                Box::pin(async move { rx.await.unwrap() }),
                vec![1],
            ));
            let mut cx = Context::from_waker(std::task::Waker::noop());
            assert!(Pin::new(&mut file).poll_shutdown(&mut cx).is_pending());
            assert!(file.close_op.is_none());
            tx.send(Ok(())).unwrap();
            assert!(Pin::new(&mut file).poll_shutdown(&mut cx).is_pending());
            assert_eq!(file.offset, 1);
            assert!(Pin::new(&mut file).poll_write(&mut cx, b"x").is_ready());
        }
    }
}
