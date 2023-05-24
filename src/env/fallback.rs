// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::fs::File;

use crate::env::{FileSystem, Handle, Permission, WriteExt};


/// A RAII-style low-level file. Errors occurred during automatic resource
/// release are logged and ignored.
///
/// A [`LogFd`] is essentially a thin wrapper around [`RawFd`]. It's only
/// supported on *Unix*, and primarily optimized for *Linux*.
///
/// All [`LogFd`] instances are opened with read and write permission.
pub struct LogFd(File);

impl LogFd {
    /// Opens a file with the given `path`.
    pub fn open<P: AsRef<Path>>(path: P, perm: Permission) -> IoResult<Self> {
        File::options()
            .read(true)
            .write(perm == Permission::ReadWrite)
            .open(path)
            .map(|file| Self(file))
    }

    /// Opens a file with the given `path`. The specified file will be created
    /// first if not exists.
    pub fn create<P: AsRef<Path>>(path: P) -> IoResult<Self> {
        File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .map(|file| Self(file))
    }

    /// Truncates all data after `offset`.
    pub fn truncate(&self, offset: usize) -> IoResult<()> {
        self.0.set_len(offset as u64)
    }

    pub fn allocate(&self, _offset: usize, _size: usize) -> IoResult<()> {
        Ok(())
    }

    pub fn size(&self) -> IoResult<usize> {
        self.0.metadata().map(|meta| meta.len() as usize)
    }

    pub fn sync(&self) -> IoResult<()> {
        self.0.sync_data()
    }
}

impl Handle for LogFd {
    fn truncate(&self, offset: usize) -> IoResult<()> {
        self.truncate(offset)
    }

    fn file_size(&self) -> IoResult<usize> {
        self.size()
    }

    fn sync(&self) -> IoResult<()> {
        self.sync()
    }
}

/// A low-level file adapted for standard interfaces including [`Seek`],
/// [`Write`] and [`Read`].
pub struct LogFile {
    inner: Arc<RwLock<LogFd>>,
    offset: usize,
}

impl LogFile {
    /// Creates a new [`LogFile`] from a shared [`LogFd`].
    pub fn new(fd: Arc<LogFd>) -> Self {
        let fd = unsafe { Arc::into_raw(fd).read() };
        Self {
            inner: Arc::new(RwLock::new(fd)),
            offset: 0,
        }
    }

    fn inner(&self) -> std::sync::RwLockReadGuard<'_, LogFd> {
        self.inner.read().unwrap()
    }

    fn inner_mut(&self) -> std::sync::RwLockWriteGuard<'_, LogFd> {
        self.inner.write().unwrap()
    }
}

impl Write for LogFile {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.inner_mut().0.write(buf)
    }

    fn flush(&mut self) -> IoResult<()> {
        self.inner_mut().0.flush()
    }
}

impl Read for LogFile {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.inner_mut().0.read(buf)
    }
}

impl Seek for LogFile {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.inner_mut().0.seek(pos)
    }
}

impl WriteExt for LogFile {
    fn truncate(&mut self, offset: usize) -> IoResult<()> {
        self.inner().truncate(offset)?;
        self.offset = offset;
        Ok(())
    }

    fn allocate(&mut self, offset: usize, size: usize) -> IoResult<()> {
        self.inner().allocate(offset, size)
    }
}

pub struct DefaultFileSystem;

impl FileSystem for DefaultFileSystem {
    type Handle = LogFd;
    type Reader = LogFile;
    type Writer = LogFile;

    fn create<P: AsRef<Path>>(&self, path: P) -> IoResult<Self::Handle> {
        LogFd::create(path)
    }

    fn open<P: AsRef<Path>>(&self, path: P, perm: Permission) -> IoResult<Self::Handle> {
        LogFd::open(path, perm)
    }

    fn delete<P: AsRef<Path>>(&self, path: P) -> IoResult<()> {
        std::fs::remove_file(path)
    }

    fn rename<P: AsRef<Path>>(&self, src_path: P, dst_path: P) -> IoResult<()> {
        std::fs::rename(src_path, dst_path)
    }

    fn new_reader(&self, handle: Arc<Self::Handle>) -> IoResult<Self::Reader> {
        Ok(LogFile::new(handle))
    }

    fn new_writer(&self, handle: Arc<Self::Handle>) -> IoResult<Self::Writer> {
        Ok(LogFile::new(handle))
    }
}
