use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::LockLevel;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};

use crate::traits::{Vfs, VfsFile};

/// Shared storage for all files in the memory VFS.
///
/// Each file is stored as a named byte vector. Multiple `MemoryFile` handles
/// can reference the same underlying storage via `Arc<Mutex<..>>`.
#[derive(Debug, Default)]
struct FileStorage {
    data: Vec<u8>,
}

/// Shared state for the entire memory VFS.
#[derive(Debug, Default)]
struct MemoryVfsInner {
    files: HashMap<PathBuf, Arc<Mutex<FileStorage>>>,
    next_temp_id: u64,
}

/// An in-memory VFS for testing and in-memory databases.
///
/// All files are stored in memory with no persistence. Multiple connections
/// can share the same `MemoryVfs` instance to access the same files.
#[derive(Debug, Clone, Default)]
pub struct MemoryVfs {
    inner: Arc<Mutex<MemoryVfsInner>>,
}

impl MemoryVfs {
    /// Create a new empty in-memory VFS.
    pub fn new() -> Self {
        Self::default()
    }
}

fn lock_err() -> FrankenError {
    FrankenError::internal("MemoryVfs lock poisoned")
}

impl Vfs for MemoryVfs {
    type File = MemoryFile;

    fn name(&self) -> &'static str {
        "memory"
    }

    #[allow(clippy::significant_drop_tightening)]
    fn open(&self, path: Option<&Path>, flags: VfsOpenFlags) -> Result<(Self::File, VfsOpenFlags)> {
        let mut inner = self.inner.lock().map_err(|_| lock_err())?;

        let resolved_path = if let Some(p) = path {
            p.to_path_buf()
        } else {
            // Generate a unique temporary filename.
            let id = inner.next_temp_id;
            inner.next_temp_id += 1;
            PathBuf::from(format!("__temp_{id}__"))
        };

        let is_create = flags.contains(VfsOpenFlags::CREATE);
        let storage = if let Some(existing) = inner.files.get(&resolved_path) {
            Arc::clone(existing)
        } else if is_create {
            let storage = Arc::new(Mutex::new(FileStorage::default()));
            inner
                .files
                .insert(resolved_path.clone(), Arc::clone(&storage));
            storage
        } else {
            return Err(FrankenError::CannotOpen {
                path: resolved_path,
            });
        };

        drop(inner);

        let file = MemoryFile {
            path: resolved_path,
            storage,
            lock_level: LockLevel::None,
            delete_on_close: flags.contains(VfsOpenFlags::DELETEONCLOSE),
            vfs: Arc::clone(&self.inner),
        };

        let mut out_flags = flags;
        if is_create {
            out_flags |= VfsOpenFlags::READWRITE;
        }

        Ok((file, out_flags))
    }

    fn delete(&self, path: &Path, _sync_dir: bool) -> Result<()> {
        self.inner
            .lock()
            .map_err(|_| lock_err())?
            .files
            .remove(path);
        Ok(())
    }

    fn access(&self, path: &Path, _flags: AccessFlags) -> Result<bool> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| lock_err())?
            .files
            .contains_key(path))
    }

    fn full_pathname(&self, path: &Path) -> Result<PathBuf> {
        if path.is_absolute() {
            Ok(path.to_path_buf())
        } else {
            Ok(Path::new("/").join(path))
        }
    }
}

/// A file handle in the memory VFS.
///
/// Reads and writes operate on a shared `Vec<u8>` protected by a mutex.
#[derive(Debug)]
pub struct MemoryFile {
    path: PathBuf,
    storage: Arc<Mutex<FileStorage>>,
    lock_level: LockLevel,
    delete_on_close: bool,
    vfs: Arc<Mutex<MemoryVfsInner>>,
}

impl VfsFile for MemoryFile {
    fn close(&mut self) -> Result<()> {
        if self.delete_on_close {
            self.vfs
                .lock()
                .map_err(|_| lock_err())?
                .files
                .remove(&self.path);
        }
        self.lock_level = LockLevel::None;
        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let storage = self.storage.lock().map_err(|_| lock_err())?;

        let offset = offset as usize;
        let file_len = storage.data.len();

        if offset >= file_len {
            drop(storage);
            buf.fill(0);
            return Ok(0);
        }

        let available = file_len - offset;
        let to_read = buf.len().min(available);
        buf[..to_read].copy_from_slice(&storage.data[offset..offset + to_read]);
        drop(storage);

        // Zero-fill the rest if short read.
        if to_read < buf.len() {
            buf[to_read..].fill(0);
        }

        Ok(to_read)
    }

    #[allow(clippy::cast_possible_truncation, clippy::significant_drop_tightening)]
    fn write(&mut self, buf: &[u8], offset: u64) -> Result<()> {
        let mut storage = self.storage.lock().map_err(|_| lock_err())?;

        let offset = offset as usize;
        let end = offset + buf.len();

        if end > storage.data.len() {
            storage.data.resize(end, 0);
        }

        storage.data[offset..end].copy_from_slice(buf);
        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    fn truncate(&mut self, size: u64) -> Result<()> {
        self.storage
            .lock()
            .map_err(|_| lock_err())?
            .data
            .truncate(size as usize);
        Ok(())
    }

    fn sync(&mut self, _flags: SyncFlags) -> Result<()> {
        Ok(())
    }

    fn file_size(&self) -> Result<u64> {
        Ok(self.storage.lock().map_err(|_| lock_err())?.data.len() as u64)
    }

    fn lock(&mut self, level: LockLevel) -> Result<()> {
        self.lock_level = level;
        Ok(())
    }

    fn unlock(&mut self, level: LockLevel) -> Result<()> {
        if level < self.lock_level {
            self.lock_level = level;
        }
        Ok(())
    }

    fn check_reserved_lock(&self) -> Result<bool> {
        Ok(false)
    }
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;

    fn make_vfs() -> MemoryVfs {
        MemoryVfs::new()
    }

    #[test]
    fn create_and_read_file() {
        let vfs = make_vfs();
        let path = Path::new("test.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs.open(Some(path), flags).unwrap();

        file.write(b"hello", 0).unwrap();
        assert_eq!(file.file_size().unwrap(), 5);

        let mut buf = [0u8; 5];
        let n = file.read(&mut buf, 0).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn read_past_end_zeroes() {
        let vfs = make_vfs();
        let path = Path::new("test.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs.open(Some(path), flags).unwrap();
        file.write(b"hi", 0).unwrap();

        let mut buf = [0xFFu8; 10];
        let n = file.read(&mut buf, 0).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], b"hi");
        assert!(buf[2..].iter().all(|&b| b == 0));
    }

    #[test]
    fn read_from_empty_file() {
        let vfs = make_vfs();
        let path = Path::new("empty.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs.open(Some(path), flags).unwrap();

        let mut buf = [0xFFu8; 4];
        let n = file.read(&mut buf, 0).unwrap();
        assert_eq!(n, 0);
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn write_extends_file() {
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs.open(Some(Path::new("test.db")), flags).unwrap();

        file.write(b"world", 10).unwrap();
        assert_eq!(file.file_size().unwrap(), 15);

        let mut buf = [0xFFu8; 15];
        file.read(&mut buf, 0).unwrap();
        assert!(buf[..10].iter().all(|&b| b == 0));
        assert_eq!(&buf[10..], b"world");
    }

    #[test]
    fn truncate() {
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs.open(Some(Path::new("test.db")), flags).unwrap();
        file.write(b"hello world", 0).unwrap();
        assert_eq!(file.file_size().unwrap(), 11);

        file.truncate(5).unwrap();
        assert_eq!(file.file_size().unwrap(), 5);

        let mut buf = [0u8; 5];
        file.read(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn open_nonexistent_without_create_fails() {
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE;

        let result = vfs.open(Some(Path::new("nope.db")), flags);
        assert!(result.is_err());
    }

    #[test]
    fn delete_file() {
        let vfs = make_vfs();
        let path = Path::new("test.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs.open(Some(path), flags).unwrap();
        file.write(b"data", 0).unwrap();
        file.close().unwrap();

        assert!(vfs.access(path, AccessFlags::EXISTS).unwrap());
        vfs.delete(path, false).unwrap();
        assert!(!vfs.access(path, AccessFlags::EXISTS).unwrap());
    }

    #[test]
    fn delete_on_close() {
        let vfs = make_vfs();
        let path = Path::new("temp.db");
        let flags = VfsOpenFlags::MAIN_DB
            | VfsOpenFlags::CREATE
            | VfsOpenFlags::READWRITE
            | VfsOpenFlags::DELETEONCLOSE;

        let (mut file, _) = vfs.open(Some(path), flags).unwrap();
        file.write(b"temp data", 0).unwrap();
        assert!(vfs.access(path, AccessFlags::EXISTS).unwrap());

        file.close().unwrap();
        assert!(!vfs.access(path, AccessFlags::EXISTS).unwrap());
    }

    #[test]
    fn temp_file_auto_naming() {
        let vfs = make_vfs();
        let flags = VfsOpenFlags::TEMP_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file1, _) = vfs.open(None, flags).unwrap();
        let (mut file2, _) = vfs.open(None, flags).unwrap();

        file1.write(b"file1", 0).unwrap();
        file2.write(b"file2", 0).unwrap();

        let mut buf = [0u8; 5];
        file1.read(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"file1");

        file2.read(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"file2");
    }

    #[test]
    fn shared_file_across_handles() {
        let vfs = make_vfs();
        let path = Path::new("shared.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file1, _) = vfs.open(Some(path), flags).unwrap();
        file1.write(b"shared data", 0).unwrap();

        let open_flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE;
        let (mut file2, _) = vfs.open(Some(path), open_flags).unwrap();
        let mut buf = [0u8; 11];
        let n = file2.read(&mut buf, 0).unwrap();
        assert_eq!(n, 11);
        assert_eq!(&buf, b"shared data");
    }

    #[test]
    fn full_pathname() {
        let vfs = make_vfs();
        let resolved = vfs.full_pathname(Path::new("test.db")).unwrap();
        assert!(resolved.is_absolute());

        let already_abs = vfs.full_pathname(Path::new("/tmp/test.db")).unwrap();
        assert_eq!(already_abs, Path::new("/tmp/test.db"));
    }

    #[test]
    fn locking() {
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs.open(Some(Path::new("lock.db")), flags).unwrap();

        file.lock(LockLevel::Shared).unwrap();
        file.lock(LockLevel::Reserved).unwrap();
        file.lock(LockLevel::Exclusive).unwrap();

        assert!(!file.check_reserved_lock().unwrap());

        file.unlock(LockLevel::Shared).unwrap();
        file.unlock(LockLevel::None).unwrap();
    }

    #[test]
    fn sync_is_noop() {
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(Some(Path::new("sync.db")), flags).unwrap();
        file.sync(SyncFlags::FULL).unwrap();
    }

    #[test]
    fn write_overwrite() {
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(Some(Path::new("test.db")), flags).unwrap();

        file.write(b"AAAAAAAAAA", 0).unwrap();
        file.write(b"BB", 3).unwrap();

        let mut buf = [0u8; 10];
        file.read(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"AAABBAAAAA");
    }

    #[test]
    fn vfs_name() {
        let vfs = make_vfs();
        assert_eq!(vfs.name(), "memory");
    }

    #[test]
    fn vfs_default_current_time() {
        let vfs = make_vfs();
        let time = vfs.current_time();
        assert!(time > 2_450_000.0);
        assert!(time < 2_500_000.0);
    }

    #[test]
    fn vfs_default_randomness() {
        let vfs = make_vfs();
        let mut buf1 = [0u8; 16];
        let mut buf2 = [0u8; 16];
        vfs.randomness(&mut buf1);
        vfs.randomness(&mut buf2);
        assert_eq!(buf1, buf2);
    }

    #[test]
    fn page_read_write_roundtrip() {
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(Some(Path::new("pages.db")), flags).unwrap();

        let page1 = vec![0xAA_u8; 4096];
        let page2 = vec![0xBB_u8; 4096];

        file.write(&page1, 0).unwrap();
        file.write(&page2, 4096).unwrap();
        assert_eq!(file.file_size().unwrap(), 8192);

        let mut buf = vec![0u8; 4096];
        file.read(&mut buf, 0).unwrap();
        assert!(buf.iter().all(|&b| b == 0xAA));

        file.read(&mut buf, 4096).unwrap();
        assert!(buf.iter().all(|&b| b == 0xBB));
    }
}
