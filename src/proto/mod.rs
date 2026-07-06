//! A small, self-contained SFTP v3 implementation (client + server) over any
//! `AsyncRead + AsyncWrite` stream, plus the ProFTPD `check-file` extension.
//! Original code — the wire format is hand-encoded (no serde). Layout:
//! [`protocol`] wire types, [`client`] the async client, [`server`] a handler-driven
//! server used for in-process testing.

pub mod client;
pub mod protocol;
pub mod server;

pub(crate) const SFTP_VERSION: u32 = 3;

/// SFTP packet type bytes.
pub(crate) mod fxp {
    pub const INIT: u8 = 1;
    pub const VERSION: u8 = 2;
    pub const OPEN: u8 = 3;
    pub const CLOSE: u8 = 4;
    pub const READ: u8 = 5;
    pub const WRITE: u8 = 6;
    pub const LSTAT: u8 = 7;
    pub const FSTAT: u8 = 8;
    pub const SETSTAT: u8 = 9;
    pub const OPENDIR: u8 = 11;
    pub const READDIR: u8 = 12;
    pub const REMOVE: u8 = 13;
    pub const MKDIR: u8 = 14;
    pub const RMDIR: u8 = 15;
    pub const REALPATH: u8 = 16;
    pub const STAT: u8 = 17;
    pub const RENAME: u8 = 18;
    pub const READLINK: u8 = 19;
    pub const SYMLINK: u8 = 20;
    pub const STATUS: u8 = 101;
    pub const HANDLE: u8 = 102;
    pub const DATA: u8 = 103;
    pub const NAME: u8 = 104;
    pub const ATTRS: u8 = 105;
    pub const EXTENDED: u8 = 200;
    pub const EXTENDED_REPLY: u8 = 201;
}

/// Read one length-prefixed packet body (`u32` length, then that many bytes).
pub(crate) async fn read_packet<R>(rd: &mut R) -> std::io::Result<Vec<u8>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    // A peer-supplied length is untrusted: without a cap, `0xFFFFFFFF` forces a
    // 4 GiB zeroed allocation per packet (OOM DoS). SFTP v3 packets are a few
    // hundred KB at most; 1 MiB is generous headroom over the 32 KiB data chunks.
    const MAX_PACKET: usize = 1024 * 1024;
    let mut len = [0u8; 4];
    rd.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_PACKET {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "sftp packet length exceeds maximum",
        ));
    }
    let mut body = vec![0u8; len];
    rd.read_exact(&mut body).await?;
    Ok(body)
}

/// Builds a packet body `[type, ...]`; framed with its length by [`frame`].
pub(crate) struct PacketBuf(pub Vec<u8>);

impl PacketBuf {
    pub fn new(ty: u8) -> Self {
        PacketBuf(vec![ty])
    }
    /// `[type, request_id, ...]` — the common client-request prefix.
    pub fn request(ty: u8, id: u32) -> Self {
        let mut b = PacketBuf::new(ty);
        b.u32(id);
        b
    }
    pub fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    pub fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    pub fn bytes(&mut self, b: &[u8]) {
        self.u32(b.len() as u32);
        self.0.extend_from_slice(b);
    }
    pub fn string(&mut self, s: &str) {
        self.bytes(s.as_bytes());
    }
}

/// Prepend the `u32` length to a packet body.
pub(crate) fn frame(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 4);
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// Reads big-endian scalars and length-prefixed byte strings from a packet body.
pub(crate) struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], WireError> {
        if self.remaining() < n {
            return Err(WireError);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    pub fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.take(1)?[0])
    }
    pub fn u32(&mut self) -> Result<u32, WireError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64, WireError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn bytes(&mut self) -> Result<Vec<u8>, WireError> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
    pub fn string(&mut self) -> Result<String, WireError> {
        Ok(String::from_utf8_lossy(&self.bytes()?).into_owned())
    }
    pub fn rest(&mut self) -> &'a [u8] {
        let s = &self.buf[self.pos..];
        self.pos = self.buf.len();
        s
    }
}

/// A packet ended before an expected field.
#[derive(Debug)]
pub(crate) struct WireError;
