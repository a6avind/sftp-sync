//! SFTP v3 wire types: flags, attributes, and the request/response payload structs
//! shared by the client and server.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{Cursor, PacketBuf, WireError};

// Attribute-present flags.
const ATTR_SIZE: u32 = 0x1;
const ATTR_UIDGID: u32 = 0x2;
const ATTR_PERMISSIONS: u32 = 0x4;
const ATTR_ACMODTIME: u32 = 0x8;
const ATTR_EXTENDED: u32 = 0x8000_0000;

// POSIX mode type bits.
const S_IFMT: u32 = 0xF000;
const S_IFDIR: u32 = 0x4000;
const S_IFREG: u32 = 0x8000;
const S_IFLNK: u32 = 0xA000;

/// File open flags, matching the SFTP v3 wire values.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct OpenFlags(u32);

impl OpenFlags {
    pub const READ: OpenFlags = OpenFlags(0x1);
    pub const WRITE: OpenFlags = OpenFlags(0x2);
    pub const APPEND: OpenFlags = OpenFlags(0x4);
    pub const CREATE: OpenFlags = OpenFlags(0x8);
    pub const TRUNCATE: OpenFlags = OpenFlags(0x10);
    pub const EXCLUDE: OpenFlags = OpenFlags(0x20);

    pub const fn bits(self) -> u32 {
        self.0
    }
    pub const fn from_bits_truncate(bits: u32) -> Self {
        OpenFlags(bits & 0x3F)
    }
    pub const fn contains(self, other: OpenFlags) -> bool {
        self.0 & other.0 == other.0
    }
}

impl std::ops::BitOr for OpenFlags {
    type Output = OpenFlags;
    fn bitor(self, rhs: OpenFlags) -> OpenFlags {
        OpenFlags(self.0 | rhs.0)
    }
}

/// A file's high-level type, derived from its mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Dir,
    File,
    Symlink,
    Other,
}

impl FileType {
    pub fn is_dir(&self) -> bool {
        *self == FileType::Dir
    }
    pub fn is_file(&self) -> bool {
        *self == FileType::File
    }
    pub fn is_symlink(&self) -> bool {
        *self == FileType::Symlink
    }
    pub fn is_other(&self) -> bool {
        *self == FileType::Other
    }
    fn from_mode(mode: u32) -> Self {
        match mode & S_IFMT {
            S_IFDIR => FileType::Dir,
            S_IFLNK => FileType::Symlink,
            S_IFREG => FileType::File,
            _ => FileType::Other,
        }
    }
    /// Recover the type from the leading char of an `ls -l`-style longname, for
    /// servers that omit PERMISSIONS from NAME/READDIR attrs (SFTP v3 optional).
    fn from_longname(longname: &str) -> Option<Self> {
        match longname.as_bytes().first()? {
            b'd' => Some(FileType::Dir),
            b'l' => Some(FileType::Symlink),
            b'-' => Some(FileType::File),
            _ => None,
        }
    }
}

/// SFTP status codes (also the server-side error type in the [`server`] module).
///
/// [`server`]: super::server
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusCode {
    Ok,
    Eof,
    NoSuchFile,
    PermissionDenied,
    Failure,
    BadMessage,
    NoConnection,
    ConnectionLost,
    OpUnsupported,
}

impl StatusCode {
    pub fn from_u32(v: u32) -> Self {
        match v {
            0 => StatusCode::Ok,
            1 => StatusCode::Eof,
            2 => StatusCode::NoSuchFile,
            3 => StatusCode::PermissionDenied,
            5 => StatusCode::BadMessage,
            6 => StatusCode::NoConnection,
            7 => StatusCode::ConnectionLost,
            8 => StatusCode::OpUnsupported,
            _ => StatusCode::Failure,
        }
    }
    pub fn as_u32(self) -> u32 {
        match self {
            StatusCode::Ok => 0,
            StatusCode::Eof => 1,
            StatusCode::NoSuchFile => 2,
            StatusCode::PermissionDenied => 3,
            StatusCode::Failure => 4,
            StatusCode::BadMessage => 5,
            StatusCode::NoConnection => 6,
            StatusCode::ConnectionLost => 7,
            StatusCode::OpUnsupported => 8,
        }
    }
}

/// File attributes. Absent fields are `None`. Also re-exported as
/// [`client::fs::Metadata`](super::client::fs::Metadata).
#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub struct FileAttributes {
    pub size: Option<u64>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub permissions: Option<u32>,
    pub atime: Option<u32>,
    pub mtime: Option<u32>,
}

impl FileAttributes {
    /// All-absent attributes (the wire "no attributes" value).
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn file_type(&self) -> FileType {
        self.permissions.map(FileType::from_mode).unwrap_or(FileType::Other)
    }
    pub fn is_dir(&self) -> bool {
        self.file_type().is_dir()
    }
    pub fn is_file(&self) -> bool {
        self.file_type().is_file()
    }
    pub fn is_symlink(&self) -> bool {
        self.file_type().is_symlink()
    }
    /// Size in bytes (`0` if unknown).
    pub fn len(&self) -> u64 {
        self.size.unwrap_or(0)
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn modified(&self) -> Option<SystemTime> {
        self.mtime.map(|s| UNIX_EPOCH + Duration::from_secs(s as u64))
    }

    pub(crate) fn encode(&self, out: &mut PacketBuf) {
        let mut flags = 0;
        if self.size.is_some() {
            flags |= ATTR_SIZE;
        }
        if self.uid.is_some() || self.gid.is_some() {
            flags |= ATTR_UIDGID;
        }
        if self.permissions.is_some() {
            flags |= ATTR_PERMISSIONS;
        }
        if self.atime.is_some() || self.mtime.is_some() {
            flags |= ATTR_ACMODTIME;
        }
        out.u32(flags);
        if let Some(v) = self.size {
            out.u64(v);
        }
        if flags & ATTR_UIDGID != 0 {
            // uid+gid travel as one unit; fill a missing side rather than zero it.
            out.u32(self.uid.or(self.gid).unwrap_or(0));
            out.u32(self.gid.or(self.uid).unwrap_or(0));
        }
        if let Some(v) = self.permissions {
            out.u32(v);
        }
        if flags & ATTR_ACMODTIME != 0 {
            out.u32(self.atime.or(self.mtime).unwrap_or(0));
            out.u32(self.mtime.or(self.atime).unwrap_or(0));
        }
    }

    pub(crate) fn decode(cur: &mut Cursor) -> Result<Self, WireError> {
        let flags = cur.u32()?;
        let mut a = FileAttributes::default();
        if flags & ATTR_SIZE != 0 {
            a.size = Some(cur.u64()?);
        }
        if flags & ATTR_UIDGID != 0 {
            a.uid = Some(cur.u32()?);
            a.gid = Some(cur.u32()?);
        }
        if flags & ATTR_PERMISSIONS != 0 {
            a.permissions = Some(cur.u32()?);
        }
        if flags & ATTR_ACMODTIME != 0 {
            a.atime = Some(cur.u32()?);
            a.mtime = Some(cur.u32()?);
        }
        if flags & ATTR_EXTENDED != 0 {
            let n = cur.u32()?;
            for _ in 0..n {
                cur.string()?;
                cur.string()?;
            }
        }
        Ok(a)
    }
}

impl From<&std::fs::Metadata> for FileAttributes {
    fn from(m: &std::fs::Metadata) -> Self {
        let mtime = m
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as u32);
        let atime = m
            .accessed()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as u32);
        let mut a = FileAttributes {
            size: Some(m.len()),
            atime,
            mtime,
            ..Default::default()
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            a.uid = Some(m.uid());
            a.gid = Some(m.gid());
            a.permissions = Some(m.mode());
        }
        #[cfg(not(unix))]
        {
            let ty = if m.is_dir() {
                S_IFDIR
            } else if m.file_type().is_symlink() {
                S_IFLNK
            } else {
                S_IFREG
            };
            a.permissions = Some(ty | 0o644);
        }
        a
    }
}

/// A single `SSH_FXP_NAME` entry (directory listing, realpath, readlink).
#[derive(Clone)]
pub struct File {
    pub filename: String,
    pub longname: String,
    pub attrs: FileAttributes,
}

impl File {
    pub fn new(filename: String, attrs: FileAttributes) -> Self {
        File { filename, longname: String::new(), attrs }
    }
    /// A name-only entry (no attributes), for realpath/readlink replies.
    pub fn dummy(filename: String) -> Self {
        File { filename, longname: String::new(), attrs: FileAttributes::empty() }
    }
    pub fn file_name(&self) -> String {
        self.filename.clone()
    }
    pub fn file_type(&self) -> FileType {
        let ty = self.attrs.file_type();
        if ty.is_other()
            && let Some(from_ln) = FileType::from_longname(&self.longname)
        {
            return from_ln;
        }
        ty
    }
    pub fn metadata(&self) -> FileAttributes {
        self.attrs
    }
    pub(crate) fn encode(&self, out: &mut PacketBuf) {
        out.string(&self.filename);
        out.string(&self.longname);
        self.attrs.encode(out);
    }
}

#[derive(Debug)]
pub struct Status {
    pub id: u32,
    pub status_code: StatusCode,
    pub error_message: String,
    pub language_tag: String,
}

pub struct Handle {
    pub id: u32,
    pub handle: String,
}

pub struct Data {
    pub id: u32,
    pub data: Vec<u8>,
}

pub struct Name {
    pub id: u32,
    pub files: Vec<File>,
}

pub struct Attrs {
    pub id: u32,
    pub attrs: FileAttributes,
}

pub struct CheckFile {
    pub hash_algorithm: String,
    pub hashes: Vec<u8>,
}
