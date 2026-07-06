//! Async SFTP v3 client: [`SftpSession`] and the file/metadata types under
//! [`fs`]. A background task owns the stream's read half and routes each response
//! to its request by id; requests serialize writes behind a mutex.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{oneshot, Mutex};

use super::protocol::{
    CheckFile, File as NameFile, FileAttributes, OpenFlags, Status, StatusCode,
};
use super::{fxp, frame, read_packet, Cursor, PacketBuf, SFTP_VERSION};

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
    Handle(String),
    Data(Vec<u8>),
    Name(Vec<NameFile>),
    Attrs(FileAttributes),
    Extended(Vec<u8>),
}

struct Inner {
    writer: Mutex<Box<dyn AsyncWrite + Send + Unpin>>,
    pending: DashMap<u32, oneshot::Sender<Reply>>,
    next_id: AtomicU32,
    extensions: HashMap<String, String>,
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
        let _server_version = cur.u32().map_err(|_| proto_err("short VERSION"))?;
        let mut extensions = HashMap::new();
        while cur.remaining() > 0 {
            let name = cur.string().map_err(|_| proto_err("bad extension"))?;
            let data = cur.string().map_err(|_| proto_err("bad extension"))?;
            extensions.insert(name, data);
        }

        let inner = Arc::new(Inner {
            writer: Mutex::new(Box::new(wr)),
            pending: DashMap::new(),
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
        let (tx, rx) = oneshot::channel();
        self.inner.pending.insert(id, tx);
        {
            let mut w = self.inner.writer.lock().await;
            if let Err(e) = w.write_all(&frame(&body)).await.and(w.flush().await) {
                self.inner.pending.remove(&id);
                return Err(Error::Io(e));
            }
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

    async fn expect_handle(&self, id: u32, body: Vec<u8>) -> SftpResult<String> {
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

    async fn open_handle(&self, path: &str, flags: OpenFlags) -> SftpResult<String> {
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

    async fn read_at(&self, handle: &str, offset: u64, len: u32) -> SftpResult<Vec<u8>> {
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::READ, id);
        b.string(handle);
        b.u64(offset);
        b.u32(len);
        match self.request(id, b.0).await? {
            Reply::Data(d) => Ok(d),
            Reply::Status(s) if s.status_code == StatusCode::Eof => Ok(Vec::new()),
            Reply::Status(s) => Err(Error::Status(s)),
            _ => Err(proto_err("expected DATA")),
        }
    }

    async fn write_at(&self, handle: &str, offset: u64, data: &[u8]) -> SftpResult<()> {
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::WRITE, id);
        b.string(handle);
        b.u64(offset);
        b.bytes(data);
        self.expect_status(id, b.0).await
    }

    async fn close(&self, handle: &str) -> SftpResult<()> {
        let id = self.next_id();
        let mut b = PacketBuf::request(fxp::CLOSE, id);
        b.string(handle);
        self.expect_status(id, b.0).await
    }

    /// List a directory; each [`DirEntry`] carries its parent for [`DirEntry::path`].
    pub async fn read_dir(&self, path: &str) -> SftpResult<ReadDir> {
        let handle = {
            let id = self.next_id();
            let mut b = PacketBuf::request(fxp::OPENDIR, id);
            b.string(path);
            self.expect_handle(id, b.0).await?
        };

        let mut entries = Vec::new();
        loop {
            let id = self.next_id();
            let mut b = PacketBuf::request(fxp::READDIR, id);
            b.string(&handle);
            match self.request(id, b.0).await {
                Ok(Reply::Name(mut names)) => entries.append(&mut names),
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
            .map(|file| DirEntry { dir: dir.clone(), file })
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
                let algo = cur.string().map_err(|_| proto_err("bad check-file reply"))?;
                Ok(CheckFile { hash_algorithm: algo, hashes: cur.rest().to_vec() })
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
        let (Ok(_ty), Ok(id)) = (head.u8(), head.u32()) else { continue };
        if let Some((_, tx)) = inner.pending.remove(&id) {
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
    inner.pending.clear(); // fail in-flight requests rather than hang
}

fn parse_reply(body: &[u8]) -> Option<(u32, Reply)> {
    let mut cur = Cursor::new(body);
    let ty = cur.u8().ok()?;
    let id = cur.u32().ok()?;
    let reply = match ty {
        fxp::STATUS => {
            let code = cur.u32().ok()?;
            let error_message = cur.string().unwrap_or_default();
            let language_tag = cur.string().unwrap_or_default();
            Reply::Status(Status {
                id,
                status_code: StatusCode::from_u32(code),
                error_message,
                language_tag,
            })
        }
        fxp::HANDLE => Reply::Handle(cur.string().ok()?),
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
                files.push(NameFile { filename, longname, attrs });
            }
            Reply::Name(files)
        }
        _ => return None,
    };
    Some((id, reply))
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

    /// A remote file handle (`AsyncRead`/`AsyncWrite`/`AsyncSeek`). `shutdown`
    /// awaits an explicit CLOSE; dropping without it spawns a background CLOSE so
    /// the handle is never leaked.
    pub struct File {
        session: SftpSession,
        handle: String,
        offset: u64,
        closed: bool,
        op: Option<IoFuture<Vec<u8>>>,
        close_op: Option<IoFuture<()>>,
    }

    impl File {
        pub(super) fn new(session: SftpSession, handle: String) -> Self {
            File { session, handle, offset: 0, closed: false, op: None, close_op: None }
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
            if this.op.is_none() {
                let want = buf.remaining().min(MAX_CHUNK) as u32;
                if want == 0 {
                    return Poll::Ready(Ok(()));
                }
                let s = this.session.clone();
                let h = this.handle.clone();
                let off = this.offset;
                this.op = Some(Box::pin(async move { s.read_at(&h, off, want).await.map_err(io_err) }));
            }
            let fut = this.op.as_mut().unwrap();
            match Pin::new(fut).poll(cx) {
                Poll::Ready(Ok(data)) => {
                    this.op = None;
                    this.offset += data.len() as u64;
                    buf.put_slice(&data);
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
            let n = buf.len().min(MAX_CHUNK);
            if this.op.is_none() {
                let s = this.session.clone();
                let h = this.handle.clone();
                let off = this.offset;
                let chunk = buf[..n].to_vec();
                this.op = Some(Box::pin(async move {
                    s.write_at(&h, off, &chunk).await.map(|_| Vec::new()).map_err(io_err)
                }));
            }
            let fut = this.op.as_mut().unwrap();
            match Pin::new(fut).poll(cx) {
                Poll::Ready(Ok(_)) => {
                    this.op = None;
                    this.offset += n as u64;
                    Poll::Ready(Ok(n))
                }
                Poll::Ready(Err(e)) => {
                    this.op = None;
                    Poll::Ready(Err(e))
                }
                Poll::Pending => Poll::Pending,
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(())) // each write is a synchronous round-trip
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let this = &mut *self;
            if this.closed {
                return Poll::Ready(Ok(()));
            }
            if this.close_op.is_none() {
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
            // Every read/write carries an explicit offset, so a seek is local-only.
            // `End` needs the size we don't track here.
            let new = match pos {
                SeekFrom::Start(n) => n,
                SeekFrom::Current(d) => self.offset.saturating_add_signed(d),
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
            if let Ok(rt) = tokio::runtime::Handle::try_current() {
                let s = self.session.clone();
                let h = std::mem::take(&mut self.handle);
                rt.spawn(async move {
                    let _ = s.close(&h).await;
                });
            }
        }
    }
}
