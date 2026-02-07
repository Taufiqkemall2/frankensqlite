use bitflags::bitflags;

bitflags! {
    /// Flags for `sqlite3_open_v2()`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct OpenFlags: u32 {
        /// Open for reading only.
        const READONLY       = 0x0000_0001;
        /// Open for reading and writing.
        const READWRITE      = 0x0000_0002;
        /// Create the database if it doesn't exist.
        const CREATE         = 0x0000_0004;
        /// Interpret the filename as a URI.
        const URI            = 0x0000_0040;
        /// Open an in-memory database.
        const MEMORY         = 0x0000_0080;
        /// Use the main database only (no attached databases).
        const NOMUTEX        = 0x0000_8000;
        /// Use the serialized threading mode.
        const FULLMUTEX      = 0x0001_0000;
        /// Use shared cache mode.
        const SHAREDCACHE    = 0x0002_0000;
        /// Use private cache mode.
        const PRIVATECACHE   = 0x0004_0000;
        /// Do not follow symlinks.
        const NOFOLLOW       = 0x0100_0000;
    }
}

impl Default for OpenFlags {
    fn default() -> Self {
        Self::READWRITE | Self::CREATE
    }
}

bitflags! {
    /// Flags for VFS file sync operations.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct SyncFlags: u8 {
        /// Normal sync (SQLITE_SYNC_NORMAL).
        const NORMAL   = 0x02;
        /// Full sync (SQLITE_SYNC_FULL).
        const FULL     = 0x03;
        /// Sync the data only, not the directory (SQLITE_SYNC_DATAONLY).
        const DATAONLY = 0x10;
    }
}

bitflags! {
    /// Flags for VFS file open operations.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct VfsOpenFlags: u32 {
        /// Main database file.
        const MAIN_DB          = 0x0000_0100;
        /// Main journal file.
        const MAIN_JOURNAL     = 0x0000_0800;
        /// Temporary database file.
        const TEMP_DB          = 0x0000_0200;
        /// Temporary journal file.
        const TEMP_JOURNAL     = 0x0000_1000;
        /// Sub-journal file.
        const SUBJOURNAL       = 0x0000_2000;
        /// Super-journal file (formerly master journal).
        const SUPER_JOURNAL    = 0x0000_4000;
        /// WAL file.
        const WAL              = 0x0008_0000;
        /// Open for exclusive access.
        const EXCLUSIVE        = 0x0000_0010;
        /// Create the file if it doesn't exist.
        const CREATE           = 0x0000_0004;
        /// Open for reading and writing.
        const READWRITE        = 0x0000_0002;
        /// Delete on close.
        const DELETEONCLOSE    = 0x0000_0008;
    }
}

bitflags! {
    /// Flags for VFS access checks.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct AccessFlags: u8 {
        /// Check if the file exists.
        const EXISTS    = 0;
        /// Check if the file is readable and writable.
        const READWRITE = 1;
        /// Check if the file is readable.
        const READ      = 2;
    }
}

bitflags! {
    /// Prepare statement flags.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct PrepareFlags: u32 {
        /// The statement is persistent (will be reused).
        const PERSISTENT = 0x01;
        /// The statement may be normalized.
        const NORMALIZE  = 0x02;
        /// Do not scan the schema before preparing.
        const NO_VTAB    = 0x04;
    }
}

bitflags! {
    /// Internal flags on the Mem/sqlite3_value structure.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct MemFlags: u16 {
        /// Value is NULL.
        const NULL      = 0x0001;
        /// Value is a string.
        const STR       = 0x0002;
        /// Value is an integer.
        const INT       = 0x0004;
        /// Value is a real (float).
        const REAL      = 0x0008;
        /// Value is a BLOB.
        const BLOB      = 0x0010;
        /// Value is an integer that should be treated as real (optimization).
        const INT_REAL  = 0x0020;
        /// Auxiliary data attached.
        const AFF_MASK  = 0x003F;
        /// Memory needs to be freed.
        const DYN       = 0x0040;
        /// Value is a static string (no free needed).
        const STATIC    = 0x0080;
        /// Value is stored in an ephemeral buffer.
        const EPHEM     = 0x0100;
        /// Value has been cleared/invalidated.
        const CLEARED   = 0x0200;
        /// String has a NUL terminator.
        const TERM      = 0x0400;
        /// Has a subtype value.
        const SUBTYPE   = 0x0800;
        /// Zero-filled blob.
        const ZERO      = 0x1000;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_flags_default() {
        let flags = OpenFlags::default();
        assert!(flags.contains(OpenFlags::READWRITE));
        assert!(flags.contains(OpenFlags::CREATE));
        assert!(!flags.contains(OpenFlags::READONLY));
    }

    #[test]
    fn open_flags_combinations() {
        let flags = OpenFlags::READONLY | OpenFlags::URI;
        assert!(flags.contains(OpenFlags::READONLY));
        assert!(flags.contains(OpenFlags::URI));
        assert!(!flags.contains(OpenFlags::CREATE));
    }

    #[test]
    fn sync_flags() {
        let flags = SyncFlags::FULL | SyncFlags::DATAONLY;
        assert!(flags.contains(SyncFlags::FULL));
        assert!(flags.contains(SyncFlags::DATAONLY));
    }

    #[test]
    fn vfs_open_flags() {
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        assert!(flags.contains(VfsOpenFlags::MAIN_DB));
        assert!(flags.contains(VfsOpenFlags::CREATE));
    }

    #[test]
    fn prepare_flags() {
        let flags = PrepareFlags::PERSISTENT | PrepareFlags::NORMALIZE;
        assert!(flags.contains(PrepareFlags::PERSISTENT));
        assert!(flags.contains(PrepareFlags::NORMALIZE));
    }

    #[test]
    fn mem_flags() {
        let flags = MemFlags::INT | MemFlags::STATIC;
        assert!(flags.contains(MemFlags::INT));
        assert!(flags.contains(MemFlags::STATIC));
        assert!(!flags.contains(MemFlags::NULL));
    }
}
