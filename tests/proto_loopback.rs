//! End-to-end check of the bundled SFTP client against the bundled server over
//! an in-memory duplex pipe: the two halves must agree on the wire format.
#![cfg(feature = "sftp")]

use std::collections::HashMap;

use sftp_sync::client::SftpSession;
use sftp_sync::protocol::{
    Attrs, Data, File, FileAttributes, Handle, Name, OpenFlags, Status, StatusCode,
};
use sftp_sync::server;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// A tiny in-memory filesystem: paths -> bytes, plus a directory set.
#[derive(Default)]
struct MemFs {
    files: HashMap<String, Vec<u8>>,
    dirs: std::collections::HashSet<String>,
    times: HashMap<String, (u32, u32)>, // path -> (atime, mtime) from setstat
}

enum Kind {
    File {
        path: String,
        buf: Vec<u8>,
        writable: bool,
    },
    Dir {
        entries: Vec<(String, FileAttributes)>,
        sent: bool,
    },
}

struct Mem {
    fs: MemFs,
    handles: HashMap<Vec<u8>, Kind>,
    counter: u32,
}

impl Mem {
    fn ok(id: u32) -> Status {
        Status {
            id,
            status_code: StatusCode::Ok,
            error_message: "ok".into(),
            language_tag: "en".into(),
        }
    }
    fn alloc(&mut self) -> Vec<u8> {
        let mut h = vec![0xff, 0];
        h.extend_from_slice(&self.counter.to_be_bytes());
        self.counter += 1;
        h
    }
    fn dir_attrs() -> FileAttributes {
        FileAttributes {
            permissions: Some(0o040755),
            ..Default::default()
        }
    }
    fn file_attrs(len: u64) -> FileAttributes {
        FileAttributes {
            size: Some(len),
            permissions: Some(0o100644),
            ..Default::default()
        }
    }
}

impl server::Handler for Mem {
    type Error = StatusCode;
    fn unimplemented(&self) -> StatusCode {
        StatusCode::OpUnsupported
    }

    async fn realpath(&mut self, id: u32, path: String) -> Result<Name, StatusCode> {
        let p = if path == "." { "/".to_string() } else { path };
        Ok(Name {
            id,
            files: vec![File::dummy(p)],
        })
    }
    async fn mkdir(
        &mut self,
        id: u32,
        path: String,
        _a: FileAttributes,
    ) -> Result<Status, StatusCode> {
        self.fs.dirs.insert(path);
        Ok(Self::ok(id))
    }
    async fn opendir(&mut self, id: u32, path: String) -> Result<Handle, StatusCode> {
        let prefix = if path.ends_with('/') {
            path.clone()
        } else {
            format!("{path}/")
        };
        let mut entries = Vec::new();
        for (name, buf) in &self.fs.files {
            if let Some(rest) = name.strip_prefix(&prefix)
                && !rest.contains('/')
            {
                entries.push((rest.to_string(), Mem::file_attrs(buf.len() as u64)));
            }
        }
        for d in &self.fs.dirs {
            if let Some(rest) = d.strip_prefix(&prefix)
                && !rest.is_empty()
                && !rest.contains('/')
            {
                entries.push((rest.to_string(), Mem::dir_attrs()));
            }
        }
        let h = self.alloc();
        self.handles.insert(
            h.clone(),
            Kind::Dir {
                entries,
                sent: false,
            },
        );
        Ok(Handle { id, handle: h })
    }
    async fn readdir(&mut self, id: u32, handle: Vec<u8>) -> Result<Name, StatusCode> {
        match self.handles.get_mut(&handle) {
            Some(Kind::Dir { entries, sent }) if !*sent => {
                let files = entries
                    .iter()
                    .map(|(n, a)| File::new(n.clone(), *a))
                    .collect();
                *sent = true;
                Ok(Name { id, files })
            }
            Some(Kind::Dir { .. }) => Err(StatusCode::Eof),
            _ => Err(StatusCode::Failure),
        }
    }
    async fn open(
        &mut self,
        id: u32,
        filename: String,
        pflags: OpenFlags,
        _a: FileAttributes,
    ) -> Result<Handle, StatusCode> {
        let writable = pflags.contains(OpenFlags::WRITE);
        let buf = if writable {
            Vec::new()
        } else {
            self.fs
                .files
                .get(&filename)
                .cloned()
                .ok_or(StatusCode::NoSuchFile)?
        };
        let h = self.alloc();
        self.handles.insert(
            h.clone(),
            Kind::File {
                path: filename,
                buf,
                writable,
            },
        );
        Ok(Handle { id, handle: h })
    }
    async fn read(
        &mut self,
        id: u32,
        handle: Vec<u8>,
        offset: u64,
        len: u32,
    ) -> Result<Data, StatusCode> {
        match self.handles.get(&handle) {
            Some(Kind::File { buf, .. }) => {
                let start = offset as usize;
                if start >= buf.len() {
                    return Err(StatusCode::Eof);
                }
                let end = (start + len as usize).min(buf.len());
                Ok(Data {
                    id,
                    data: buf[start..end].to_vec(),
                })
            }
            _ => Err(StatusCode::Failure),
        }
    }
    async fn write(
        &mut self,
        id: u32,
        handle: Vec<u8>,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<Status, StatusCode> {
        match self.handles.get_mut(&handle) {
            Some(Kind::File {
                buf,
                writable: true,
                ..
            }) => {
                let start = offset as usize;
                let end = start + data.len();
                if end > buf.len() {
                    buf.resize(end, 0);
                }
                buf[start..end].copy_from_slice(&data);
                Ok(Self::ok(id))
            }
            _ => Err(StatusCode::Failure),
        }
    }
    async fn close(&mut self, id: u32, handle: Vec<u8>) -> Result<Status, StatusCode> {
        let kind = self.handles.remove(&handle).ok_or(StatusCode::Failure)?;
        if let Kind::File {
            path,
            buf,
            writable: true,
        } = kind
        {
            self.fs.files.insert(path, buf);
        }
        Ok(Self::ok(id))
    }
    async fn rename(
        &mut self,
        id: u32,
        oldpath: String,
        newpath: String,
    ) -> Result<Status, StatusCode> {
        let v = self
            .fs
            .files
            .remove(&oldpath)
            .ok_or(StatusCode::NoSuchFile)?;
        self.fs.files.insert(newpath, v);
        Ok(Self::ok(id))
    }
    async fn remove(&mut self, id: u32, filename: String) -> Result<Status, StatusCode> {
        self.fs
            .files
            .remove(&filename)
            .ok_or(StatusCode::NoSuchFile)?;
        Ok(Self::ok(id))
    }
    async fn stat(&mut self, id: u32, path: String) -> Result<Attrs, StatusCode> {
        if let Some(buf) = self.fs.files.get(&path) {
            let mut attrs = Mem::file_attrs(buf.len() as u64);
            if let Some(&(a, m)) = self.fs.times.get(&path) {
                attrs.atime = Some(a);
                attrs.mtime = Some(m);
            }
            Ok(Attrs { id, attrs })
        } else if self.fs.dirs.contains(&path) {
            Ok(Attrs {
                id,
                attrs: Mem::dir_attrs(),
            })
        } else {
            Err(StatusCode::NoSuchFile)
        }
    }
    async fn lstat(&mut self, id: u32, path: String) -> Result<Attrs, StatusCode> {
        self.stat(id, path).await
    }
    async fn setstat(
        &mut self,
        id: u32,
        path: String,
        attrs: FileAttributes,
    ) -> Result<Status, StatusCode> {
        if let (Some(a), Some(m)) = (attrs.atime, attrs.mtime) {
            self.fs.times.insert(path, (a, m));
        }
        Ok(Mem::ok(id))
    }
}

async fn session() -> SftpSession {
    let (client, srv) = tokio::io::duplex(64 * 1024);
    let mem = Mem {
        fs: MemFs::default(),
        handles: HashMap::new(),
        counter: 0,
    };
    server::run(srv, mem).await;
    SftpSession::new(client).await.expect("client handshake")
}

#[tokio::test]
async fn opaque_binary_handles_roundtrip() {
    // Mem allocates handles beginning with invalid UTF-8 and an embedded NUL.
    let s = session().await;
    s.create_dir("/binary").await.unwrap();
    let mut file = s.create("/binary/file").await.unwrap();
    file.write_all(b"opaque").await.unwrap();
    file.shutdown().await.unwrap();

    let mut file = s.open("/binary/file").await.unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"opaque");
    file.shutdown().await.unwrap();

    let entries: Vec<_> = s
        .read_dir("/binary")
        .await
        .unwrap()
        .map(|e| e.file_name())
        .collect();
    assert_eq!(entries, ["file"]);
}

#[tokio::test]
async fn client_server_roundtrip() {
    let s = session().await;

    s.create_dir("/data").await.expect("mkdir");

    let mut f = s.create("/data/hello.txt").await.expect("create");
    f.write_all(b"hello sftp").await.expect("write");
    f.shutdown().await.expect("close");

    let mut r = s.open("/data/hello.txt").await.expect("open");
    let mut got = String::new();
    r.read_to_string(&mut got).await.expect("read");
    assert_eq!(got, "hello sftp");

    let m = s.metadata("/data/hello.txt").await.expect("stat");
    assert_eq!(m.len(), 10);
    assert!(m.is_file());

    let mut r2 = s.open("/data/hello.txt").await.expect("open2");
    r2.seek(std::io::SeekFrom::Start(6)).await.expect("seek");
    let mut tail = String::new();
    r2.read_to_string(&mut tail).await.expect("read tail");
    assert_eq!(tail, "sftp");

    let names: Vec<String> = s
        .read_dir("/data")
        .await
        .expect("readdir")
        .map(|e| e.file_name())
        .collect();
    assert_eq!(names, vec!["hello.txt".to_string()]);

    // setstat with mtime only: the encoder must fill atime from mtime so the
    // server sees both — a one-sided send would leave atime zeroed.
    let attrs = FileAttributes {
        mtime: Some(5000),
        ..FileAttributes::empty()
    };
    s.set_metadata("/data/hello.txt", attrs)
        .await
        .expect("setstat");
    let after = s
        .metadata("/data/hello.txt")
        .await
        .expect("stat after setstat");
    assert_eq!(after.mtime, Some(5000), "mtime applied");
    assert_eq!(
        after.atime,
        Some(5000),
        "atime filled from mtime, not zeroed"
    );

    s.rename("/data/hello.txt", "/data/bye.txt")
        .await
        .expect("rename");
    assert!(s.metadata("/data/hello.txt").await.is_err());
    assert!(s.metadata("/data/bye.txt").await.is_ok());
    s.remove_file("/data/bye.txt").await.expect("remove");
    assert!(s.metadata("/data/bye.txt").await.is_err());

    assert_eq!(s.canonicalize(".").await.expect("realpath"), "/");
}
