//! A handler-driven SFTP v3 server, enough to back an in-process client (tests,
//! loopback). Implement [`Handler`] for the operations you support; the rest
//! default to `unimplemented`.

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use super::protocol::{Attrs, Data, FileAttributes, Handle, Name, OpenFlags, Status, StatusCode};
use super::{frame, fxp, read_packet, Cursor, PacketBuf, SFTP_VERSION};

/// Server-side request handler. Every method defaults to returning
/// [`Handler::unimplemented`]; override the ones you support. All futures are
/// `Send` so [`run`] can drive the session on a spawned task.
#[allow(unused_variables)]
pub trait Handler: Send + Sized {
    /// Error type; converted to an SFTP status code for the wire reply.
    type Error: Into<StatusCode> + Send;

    fn unimplemented(&self) -> Self::Error;

    fn realpath(
        &mut self,
        id: u32,
        path: String,
    ) -> impl std::future::Future<Output = Result<Name, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
    fn opendir(
        &mut self,
        id: u32,
        path: String,
    ) -> impl std::future::Future<Output = Result<Handle, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
    fn readdir(
        &mut self,
        id: u32,
        handle: String,
    ) -> impl std::future::Future<Output = Result<Name, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
    fn close(
        &mut self,
        id: u32,
        handle: String,
    ) -> impl std::future::Future<Output = Result<Status, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
    fn open(
        &mut self,
        id: u32,
        filename: String,
        pflags: OpenFlags,
        attrs: FileAttributes,
    ) -> impl std::future::Future<Output = Result<Handle, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
    fn read(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        len: u32,
    ) -> impl std::future::Future<Output = Result<Data, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
    fn write(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        data: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<Status, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
    fn remove(
        &mut self,
        id: u32,
        filename: String,
    ) -> impl std::future::Future<Output = Result<Status, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
    fn mkdir(
        &mut self,
        id: u32,
        path: String,
        attrs: FileAttributes,
    ) -> impl std::future::Future<Output = Result<Status, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
    fn rmdir(
        &mut self,
        id: u32,
        path: String,
    ) -> impl std::future::Future<Output = Result<Status, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
    fn rename(
        &mut self,
        id: u32,
        oldpath: String,
        newpath: String,
    ) -> impl std::future::Future<Output = Result<Status, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
    fn stat(
        &mut self,
        id: u32,
        path: String,
    ) -> impl std::future::Future<Output = Result<Attrs, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
    fn lstat(
        &mut self,
        id: u32,
        path: String,
    ) -> impl std::future::Future<Output = Result<Attrs, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
    fn fstat(
        &mut self,
        id: u32,
        handle: String,
    ) -> impl std::future::Future<Output = Result<Attrs, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
    fn setstat(
        &mut self,
        id: u32,
        path: String,
        attrs: FileAttributes,
    ) -> impl std::future::Future<Output = Result<Status, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
    fn readlink(
        &mut self,
        id: u32,
        path: String,
    ) -> impl std::future::Future<Output = Result<Name, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
    fn symlink(
        &mut self,
        id: u32,
        linkpath: String,
        targetpath: String,
    ) -> impl std::future::Future<Output = Result<Status, Self::Error>> + Send {
        async move { Err(self.unimplemented()) }
    }
}

/// Start serving `handler` over `stream` on a background task, then return.
pub async fn run<S, H>(stream: S, handler: H)
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    H: Handler + 'static,
{
    tokio::spawn(async move {
        let _ = serve(stream, handler).await;
    });
}

async fn serve<S, H>(stream: S, mut handler: H) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    H: Handler,
{
    let (mut rd, mut wr) = tokio::io::split(stream);

    // INIT -> VERSION handshake.
    let init = read_packet(&mut rd).await?;
    if init.first() != Some(&fxp::INIT) {
        return Ok(());
    }
    let mut version = PacketBuf::new(fxp::VERSION);
    version.u32(SFTP_VERSION);
    wr.write_all(&frame(&version.0)).await?;
    wr.flush().await?;

    loop {
        let body = match read_packet(&mut rd).await {
            Ok(b) => b,
            Err(_) => return Ok(()),
        };
        let reply = match dispatch(&mut handler, &body).await {
            Some(r) => r,
            // Packet too short to even recover a request id: it can't be answered
            // and its id can't be matched, so continuing would silently wedge that
            // request forever. Log and close the session instead.
            None => {
                eprintln!("sftp-sync server: unparseable request packet, closing session");
                return Ok(());
            }
        };
        wr.write_all(&frame(&reply)).await?;
        wr.flush().await?;
    }
}

/// Parse one request, run the handler, and encode the reply body (`None` when the
/// packet is too short to even recover a request id).
async fn dispatch<H: Handler>(handler: &mut H, body: &[u8]) -> Option<Vec<u8>> {
    let mut cur = Cursor::new(body);
    let ty = cur.u8().ok()?;
    let id = cur.u32().ok()?;

    // A field parse that fails once the id is known still produces a reply, so the
    // client's pending request resolves with BadMessage instead of hanging until
    // the connection drops. `None` is reserved for packets too short to even
    // recover an id (handled by the caller as a log-and-close).
    macro_rules! field {
        ($res:expr) => {
            match $res {
                Ok(v) => v,
                Err(_) => return Some(enc_status(&err_status(id, StatusCode::BadMessage))),
            }
        };
    }

    macro_rules! status_reply {
        ($res:expr) => {
            match $res {
                Ok(s) => enc_status(&s),
                Err(e) => enc_status(&err_status(id, e.into())),
            }
        };
    }

    let reply = match ty {
        fxp::REALPATH => {
            let path = field!(cur.string());
            match handler.realpath(id, path).await {
                Ok(n) => enc_name(&n),
                Err(e) => enc_status(&err_status(id, e.into())),
            }
        }
        fxp::OPENDIR => {
            let path = field!(cur.string());
            match handler.opendir(id, path).await {
                Ok(h) => enc_handle(&h),
                Err(e) => enc_status(&err_status(id, e.into())),
            }
        }
        fxp::READDIR => {
            let handle = field!(cur.string());
            match handler.readdir(id, handle).await {
                Ok(n) => enc_name(&n),
                Err(e) => enc_status(&err_status(id, e.into())),
            }
        }
        fxp::CLOSE => {
            let handle = field!(cur.string());
            status_reply!(handler.close(id, handle).await)
        }
        fxp::OPEN => {
            let filename = field!(cur.string());
            let pflags = OpenFlags::from_bits_truncate(field!(cur.u32()));
            let attrs = field!(FileAttributes::decode(&mut cur));
            match handler.open(id, filename, pflags, attrs).await {
                Ok(h) => enc_handle(&h),
                Err(e) => enc_status(&err_status(id, e.into())),
            }
        }
        fxp::READ => {
            let handle = field!(cur.string());
            let offset = field!(cur.u64());
            let len = field!(cur.u32());
            match handler.read(id, handle, offset, len).await {
                Ok(d) => enc_data(&d),
                Err(e) => enc_status(&err_status(id, e.into())),
            }
        }
        fxp::WRITE => {
            let handle = field!(cur.string());
            let offset = field!(cur.u64());
            let data = field!(cur.bytes());
            status_reply!(handler.write(id, handle, offset, data).await)
        }
        fxp::REMOVE => {
            let filename = field!(cur.string());
            status_reply!(handler.remove(id, filename).await)
        }
        fxp::MKDIR => {
            let path = field!(cur.string());
            let attrs = field!(FileAttributes::decode(&mut cur));
            status_reply!(handler.mkdir(id, path, attrs).await)
        }
        fxp::RMDIR => {
            let path = field!(cur.string());
            status_reply!(handler.rmdir(id, path).await)
        }
        fxp::RENAME => {
            let oldpath = field!(cur.string());
            let newpath = field!(cur.string());
            status_reply!(handler.rename(id, oldpath, newpath).await)
        }
        fxp::STAT => {
            let path = field!(cur.string());
            match handler.stat(id, path).await {
                Ok(a) => enc_attrs(&a),
                Err(e) => enc_status(&err_status(id, e.into())),
            }
        }
        fxp::LSTAT => {
            let path = field!(cur.string());
            match handler.lstat(id, path).await {
                Ok(a) => enc_attrs(&a),
                Err(e) => enc_status(&err_status(id, e.into())),
            }
        }
        fxp::FSTAT => {
            let handle = field!(cur.string());
            match handler.fstat(id, handle).await {
                Ok(a) => enc_attrs(&a),
                Err(e) => enc_status(&err_status(id, e.into())),
            }
        }
        fxp::SETSTAT => {
            let path = field!(cur.string());
            let attrs = field!(FileAttributes::decode(&mut cur));
            status_reply!(handler.setstat(id, path, attrs).await)
        }
        fxp::READLINK => {
            let path = field!(cur.string());
            match handler.readlink(id, path).await {
                Ok(n) => enc_name(&n),
                Err(e) => enc_status(&err_status(id, e.into())),
            }
        }
        fxp::SYMLINK => {
            let linkpath = field!(cur.string());
            let targetpath = field!(cur.string());
            status_reply!(handler.symlink(id, linkpath, targetpath).await)
        }
        _ => enc_status(&err_status(id, StatusCode::OpUnsupported)),
    };
    Some(reply)
}

fn err_status(id: u32, code: StatusCode) -> Status {
    Status { id, status_code: code, error_message: String::new(), language_tag: "en-US".into() }
}

fn enc_status(s: &Status) -> Vec<u8> {
    let mut b = PacketBuf::new(fxp::STATUS);
    b.u32(s.id);
    b.u32(s.status_code.as_u32());
    b.string(&s.error_message);
    b.string(&s.language_tag);
    b.0
}

fn enc_handle(h: &Handle) -> Vec<u8> {
    let mut b = PacketBuf::new(fxp::HANDLE);
    b.u32(h.id);
    b.string(&h.handle);
    b.0
}

fn enc_data(d: &Data) -> Vec<u8> {
    let mut b = PacketBuf::new(fxp::DATA);
    b.u32(d.id);
    b.bytes(&d.data);
    b.0
}

fn enc_attrs(a: &Attrs) -> Vec<u8> {
    let mut b = PacketBuf::new(fxp::ATTRS);
    b.u32(a.id);
    a.attrs.encode(&mut b);
    b.0
}

fn enc_name(n: &Name) -> Vec<u8> {
    let mut b = PacketBuf::new(fxp::NAME);
    b.u32(n.id);
    b.u32(n.files.len() as u32);
    for f in &n.files {
        f.encode(&mut b);
    }
    b.0
}
