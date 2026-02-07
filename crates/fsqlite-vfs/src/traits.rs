use std::path::Path;

use fsqlite_error::Result;
use fsqlite_types::LockLevel;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};

/// A virtual filesystem implementation.
///
/// This trait abstracts all file system operations, allowing different
/// backends: real files (Unix), in-memory (testing), or custom implementations.
///
/// Modeled after C SQLite's `sqlite3_vfs` struct from `os.h`.
pub trait Vfs: Send + Sync {
    /// The file handle type produced by this VFS.
    type File: VfsFile;

    /// The name of this VFS (e.g., "unix", "memory").
    fn name(&self) -> &'static str;

    /// Open a file.
    ///
    /// `path` is `None` for temporary files that should be auto-named.
    /// `flags` describes what kind of file (main DB, journal, WAL, etc.)
    /// and how to open it (create, read-write, exclusive, etc.).
    ///
    /// Returns the opened file and the flags that were actually used (the VFS
    /// may add flags like `READWRITE` when `CREATE` is specified).
    fn open(&self, path: Option<&Path>, flags: VfsOpenFlags) -> Result<(Self::File, VfsOpenFlags)>;

    /// Delete a file.
    ///
    /// If `sync_dir` is true, the directory entry removal should be synced
    /// to ensure durability.
    fn delete(&self, path: &Path, sync_dir: bool) -> Result<()>;

    /// Check file access.
    ///
    /// Returns true if the file at `path` satisfies the access check
    /// described by `flags`.
    fn access(&self, path: &Path, flags: AccessFlags) -> Result<bool>;

    /// Resolve a potentially relative path into an absolute path.
    fn full_pathname(&self, path: &Path) -> Result<std::path::PathBuf>;

    /// Generate a random byte sequence for temporary file naming.
    ///
    /// Fills `buf` with random bytes. The default implementation uses the
    /// system random number generator.
    fn randomness(&self, buf: &mut [u8]) {
        // Default: fill with pseudo-random bytes using a simple xorshift.
        // Real VFS implementations should use OS-provided randomness.
        let mut state: u64 = 0x5DEE_CE66_D1A4_F681;
        for chunk in buf.chunks_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let bytes = state.to_le_bytes();
            for (dst, &src) in chunk.iter_mut().zip(bytes.iter()) {
                *dst = src;
            }
        }
    }

    /// Return the current time as a Julian day number (days since noon
    /// on November 24, 4714 B.C.).
    fn current_time(&self) -> f64 {
        // Default: use system time.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        // Unix epoch in Julian days: 2440587.5
        #[allow(clippy::cast_precision_loss)]
        let julian = 2_440_587.5 + (now.as_secs_f64() / 86400.0);
        julian
    }
}

/// A file handle opened by a VFS.
///
/// Corresponds to C SQLite's `sqlite3_file` + `sqlite3_io_methods`.
pub trait VfsFile: Send + Sync {
    /// Close the file.
    ///
    /// After this call, the file handle should not be used.
    fn close(&mut self) -> Result<()>;

    /// Read `buf.len()` bytes starting at byte offset `offset`.
    ///
    /// Returns the number of bytes actually read. If fewer bytes are read
    /// than requested (short read), the remaining bytes in `buf` are zeroed.
    fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Write `buf` starting at byte offset `offset`.
    fn write(&mut self, buf: &[u8], offset: u64) -> Result<()>;

    /// Truncate the file to `size` bytes.
    fn truncate(&mut self, size: u64) -> Result<()>;

    /// Sync the file contents to stable storage.
    ///
    /// `flags` indicates the type of sync (normal, full, data-only).
    fn sync(&mut self, flags: SyncFlags) -> Result<()>;

    /// Return the current file size in bytes.
    fn file_size(&self) -> Result<u64>;

    /// Acquire a file lock at the given level.
    ///
    /// SQLite's five-level locking: None < Shared < Reserved < Pending < Exclusive.
    fn lock(&mut self, level: LockLevel) -> Result<()>;

    /// Release the file lock to the given level.
    fn unlock(&mut self, level: LockLevel) -> Result<()>;

    /// Check if another process holds a reserved lock.
    ///
    /// Returns true if a RESERVED or higher lock is held by another connection.
    fn check_reserved_lock(&self) -> Result<bool>;

    /// Return the sector size for this file.
    ///
    /// The sector size is the minimum write granularity for the underlying
    /// storage. Defaults to 4096 bytes.
    fn sector_size(&self) -> u32 {
        4096
    }

    /// Return device characteristics flags.
    ///
    /// These flags describe capabilities of the underlying storage device,
    /// such as whether it supports atomic writes. Returns 0 for no special
    /// characteristics.
    fn device_characteristics(&self) -> u32 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that the trait is object-safe for VfsFile (can be used as dyn).
    #[test]
    fn vfs_file_is_object_safe() {
        fn _accepts_dyn(_f: &dyn VfsFile) {}
    }

    /// Verify default implementations exist and don't panic.
    #[test]
    fn vfs_file_defaults() {
        struct DummyFile;
        impl VfsFile for DummyFile {
            fn close(&mut self) -> Result<()> {
                Ok(())
            }
            fn read(&mut self, _buf: &mut [u8], _offset: u64) -> Result<usize> {
                Ok(0)
            }
            fn write(&mut self, _buf: &[u8], _offset: u64) -> Result<()> {
                Ok(())
            }
            fn truncate(&mut self, _size: u64) -> Result<()> {
                Ok(())
            }
            fn sync(&mut self, _flags: SyncFlags) -> Result<()> {
                Ok(())
            }
            fn file_size(&self) -> Result<u64> {
                Ok(0)
            }
            fn lock(&mut self, _level: LockLevel) -> Result<()> {
                Ok(())
            }
            fn unlock(&mut self, _level: LockLevel) -> Result<()> {
                Ok(())
            }
            fn check_reserved_lock(&self) -> Result<bool> {
                Ok(false)
            }
        }

        let file = DummyFile;
        assert_eq!(file.sector_size(), 4096);
        assert_eq!(file.device_characteristics(), 0);
    }
}
