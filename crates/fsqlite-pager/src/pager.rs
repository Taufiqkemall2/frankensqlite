//! Concrete single-writer pager for Phase 5 persistence.
//!
//! `SimplePager` implements [`MvccPager`] with single-writer semantics over a
//! VFS-backed database file and a zero-copy [`PageCache`].
//! Full concurrent MVCC behavior is layered on top in Phase 6.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use fsqlite_types::{PageData, PageNumber, PageSize};
use fsqlite_vfs::{Vfs, VfsFile};

use crate::journal::{JournalHeader, JournalPageRecord};
use crate::page_cache::PageCache;
use crate::traits::{self, MvccPager, TransactionHandle, TransactionMode};

/// The inner mutable pager state protected by a mutex.
struct PagerInner<F: VfsFile> {
    /// Handle to the main database file.
    db_file: F,
    /// Page cache used for zero-copy read/write-through.
    cache: PageCache,
    /// Page size for this database.
    page_size: PageSize,
    /// Current database size in pages.
    db_size: u32,
    /// Next page to allocate (1-based).
    next_page: u32,
    /// Whether a writer transaction is currently active.
    writer_active: bool,
    /// Deallocated pages available for reuse.
    freelist: Vec<PageNumber>,
}

impl<F: VfsFile> PagerInner<F> {
    /// Read a page through the cache and return an owned copy.
    fn read_page_copy(&mut self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
        if let Some(data) = self.cache.get(page_no) {
            return Ok(data.to_vec());
        }

        let mut buf = self.cache.pool().clone().acquire()?;
        let page_size = self.page_size.as_usize();
        let offset = u64::from(page_no.get() - 1) * page_size as u64;
        let _ = self.db_file.read(cx, buf.as_mut_slice(), offset)?;
        let out = buf.as_slice()[..page_size].to_vec();

        let fresh = self.cache.insert_fresh(page_no)?;
        fresh.copy_from_slice(&out);
        Ok(out)
    }

    /// Flush page data to cache and file.
    fn flush_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
        if let Some(cached) = self.cache.get_mut(page_no) {
            let len = cached.len().min(data.len());
            cached[..len].copy_from_slice(&data[..len]);
        } else {
            let fresh = self.cache.insert_fresh(page_no)?;
            let len = fresh.len().min(data.len());
            fresh[..len].copy_from_slice(&data[..len]);
        }

        let page_size = self.page_size.as_usize();
        let offset = u64::from(page_no.get() - 1) * page_size as u64;
        self.db_file.write(cx, data, offset)?;
        Ok(())
    }
}

/// A concrete single-writer pager backed by a VFS file.
pub struct SimplePager<V: Vfs> {
    /// VFS used to open journal/WAL companion files.
    vfs: Arc<V>,
    /// Path to the database file.
    db_path: PathBuf,
    /// Shared mutable state used by transactions.
    inner: Arc<Mutex<PagerInner<V::File>>>,
}

impl<V: Vfs> traits::sealed::Sealed for SimplePager<V> {}

impl<V> MvccPager for SimplePager<V>
where
    V: Vfs + Send + Sync,
    V::File: Send + Sync,
{
    type Txn = SimpleTransaction<V>;

    fn begin(&self, _cx: &Cx, mode: TransactionMode) -> Result<Self::Txn> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimplePager lock poisoned"))?;

        let eager_writer = matches!(
            mode,
            TransactionMode::Immediate | TransactionMode::Exclusive
        );
        if eager_writer && inner.writer_active {
            return Err(FrankenError::Busy);
        }
        if eager_writer {
            inner.writer_active = true;
        }
        let original_db_size = inner.db_size;
        drop(inner);

        Ok(SimpleTransaction {
            vfs: Arc::clone(&self.vfs),
            journal_path: Self::journal_path(&self.db_path),
            inner: Arc::clone(&self.inner),
            write_set: HashMap::new(),
            freed_pages: Vec::new(),
            mode,
            is_writer: eager_writer,
            committed: false,
            original_db_size,
        })
    }
}

impl<V: Vfs> SimplePager<V>
where
    V::File: Send + Sync,
{
    /// Compute the journal path from the database path.
    fn journal_path(db_path: &Path) -> PathBuf {
        let mut jp = db_path.as_os_str().to_owned();
        jp.push("-journal");
        PathBuf::from(jp)
    }

    /// Open (or create) a database and return a pager.
    ///
    /// If a hot journal is detected (leftover from a crash), it is replayed
    /// to restore the database to a consistent state before returning.
    #[allow(clippy::too_many_lines)]
    pub fn open(vfs: V, path: &Path, page_size: PageSize) -> Result<Self> {
        let cx = Cx::new();
        let vfs = Arc::new(vfs);
        let flags = VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
        let (mut db_file, _actual_flags) = vfs.open(&cx, Some(path), flags)?;

        // Hot journal recovery: if a journal file exists, replay it.
        let journal_path = Self::journal_path(path);
        if vfs.access(&cx, &journal_path, AccessFlags::EXISTS)? {
            Self::replay_journal(&cx, &*vfs, &mut db_file, &journal_path, page_size)?;
            // After successful replay, delete the journal.
            let _ = vfs.delete(&cx, &journal_path, true);
        }

        let file_size = db_file.file_size(&cx)?;
        let page_size_u64 = page_size.as_usize() as u64;
        let db_pages = file_size
            .checked_div(page_size_u64)
            .ok_or_else(|| FrankenError::internal("page size must be non-zero"))?;
        let db_size = u32::try_from(db_pages).map_err(|_| FrankenError::OutOfRange {
            what: "database page count".to_owned(),
            value: db_pages.to_string(),
        })?;
        let next_page = if db_size >= 2 { db_size + 1 } else { 2 };

        Ok(Self {
            vfs,
            db_path: path.to_owned(),
            inner: Arc::new(Mutex::new(PagerInner {
                db_file,
                cache: PageCache::new(page_size, 256),
                page_size,
                db_size,
                next_page,
                writer_active: false,
                freelist: Vec::new(),
            })),
        })
    }

    /// Replay a hot journal by writing original pages back to the database.
    fn replay_journal(
        cx: &Cx,
        vfs: &V,
        db_file: &mut V::File,
        journal_path: &Path,
        page_size: PageSize,
    ) -> Result<()> {
        let jrnl_flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_JOURNAL;
        let Ok((mut jrnl_file, _)) = vfs.open(cx, Some(journal_path), jrnl_flags) else {
            return Ok(()); // Cannot open journal — treat as no journal.
        };

        let jrnl_size = jrnl_file.file_size(cx)?;
        if jrnl_size < crate::journal::JOURNAL_HEADER_SIZE as u64 {
            return Ok(()); // Truncated/empty journal — nothing to replay.
        }

        // Read and parse the journal header.
        let mut hdr_buf = vec![0u8; crate::journal::JOURNAL_HEADER_SIZE];
        let _ = jrnl_file.read(cx, &mut hdr_buf, 0)?;
        let Ok(header) = JournalHeader::decode(&hdr_buf) else {
            return Ok(()); // Corrupt header — nothing to replay.
        };

        let page_count = if header.page_count < 0 {
            header.compute_page_count_from_file_size(jrnl_size)
        } else {
            #[allow(clippy::cast_sign_loss)]
            let c = header.page_count as u32;
            c
        };

        let header_size = u64::try_from(crate::journal::JOURNAL_HEADER_SIZE)
            .expect("journal header size should fit in u64");
        let hdr_padded = u64::from(header.sector_size).max(header_size);
        let ps = page_size.as_usize();
        let record_size = 4 + ps + 4;
        let mut offset = hdr_padded;

        for _ in 0..page_count {
            let mut rec_buf = vec![0u8; record_size];
            let bytes_read = jrnl_file.read(cx, &mut rec_buf, offset)?;
            if bytes_read < record_size {
                break; // Torn record — stop replay.
            }

            #[allow(clippy::cast_possible_truncation)]
            let Ok(record) = JournalPageRecord::decode(&rec_buf, ps as u32) else {
                break; // Corrupt record — stop replay.
            };

            // Verify checksum before applying.
            if record.verify_checksum(header.nonce).is_err() {
                break; // Checksum failure — stop replay at this point.
            }

            // Write the pre-image back to the database file.
            let page_offset = u64::from(record.page_number.saturating_sub(1)) * ps as u64;
            db_file.write(cx, &record.content, page_offset)?;

            offset += record_size as u64;
        }

        // Sync the database after replaying.
        db_file.sync(cx, SyncFlags::NORMAL)?;

        // Truncate the database to the original size from the journal header.
        if header.initial_db_size > 0 {
            let target_size = u64::from(header.initial_db_size) * ps as u64;
            let current_size = db_file.file_size(cx)?;
            if current_size > target_size {
                db_file.truncate(cx, target_size)?;
            }
        }

        Ok(())
    }
}

/// Transaction handle produced by [`SimplePager`].
pub struct SimpleTransaction<V: Vfs> {
    vfs: Arc<V>,
    journal_path: PathBuf,
    inner: Arc<Mutex<PagerInner<V::File>>>,
    write_set: HashMap<PageNumber, Vec<u8>>,
    freed_pages: Vec<PageNumber>,
    mode: TransactionMode,
    is_writer: bool,
    committed: bool,
    original_db_size: u32,
}

impl<V: Vfs> traits::sealed::Sealed for SimpleTransaction<V> {}

impl<V> SimpleTransaction<V>
where
    V: Vfs + Send,
    V::File: Send + Sync,
{
    fn ensure_writer(&mut self) -> Result<()> {
        if self.is_writer {
            return Ok(());
        }

        match self.mode {
            TransactionMode::ReadOnly => Err(FrankenError::ReadOnly),
            TransactionMode::Deferred => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;
                if inner.writer_active {
                    return Err(FrankenError::Busy);
                }
                inner.writer_active = true;
                drop(inner);
                self.is_writer = true;
                Ok(())
            }
            TransactionMode::Immediate | TransactionMode::Exclusive => Err(FrankenError::internal(
                "writer transaction lost writer role",
            )),
        }
    }
}

impl<V> TransactionHandle for SimpleTransaction<V>
where
    V: Vfs + Send,
    V::File: Send + Sync,
{
    fn get_page(&self, cx: &Cx, page_no: PageNumber) -> Result<PageData> {
        if let Some(data) = self.write_set.get(&page_no) {
            return Ok(PageData::from_vec(data.clone()));
        }

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;
        let data = inner.read_page_copy(cx, page_no)?;
        drop(inner);
        Ok(PageData::from_vec(data))
    }

    fn write_page(&mut self, _cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
        self.ensure_writer()?;

        let page_size = {
            let inner = self
                .inner
                .lock()
                .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;
            inner.page_size.as_usize()
        };
        let mut owned = vec![0_u8; page_size];
        let len = owned.len().min(data.len());
        owned[..len].copy_from_slice(&data[..len]);
        self.write_set.insert(page_no, owned);
        Ok(())
    }

    fn allocate_page(&mut self, _cx: &Cx) -> Result<PageNumber> {
        self.ensure_writer()?;

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;

        if let Some(page) = inner.freelist.pop() {
            return Ok(page);
        }

        let raw = inner.next_page;
        inner.next_page = inner.next_page.saturating_add(1);
        drop(inner);
        PageNumber::new(raw).ok_or_else(|| FrankenError::OutOfRange {
            what: "allocated page number".to_owned(),
            value: raw.to_string(),
        })
    }

    fn free_page(&mut self, _cx: &Cx, page_no: PageNumber) -> Result<()> {
        self.ensure_writer()?;
        if page_no == PageNumber::ONE {
            return Err(FrankenError::OutOfRange {
                what: "free page number".to_owned(),
                value: page_no.get().to_string(),
            });
        }
        self.freed_pages.push(page_no);
        self.write_set.remove(&page_no);
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn commit(&mut self, cx: &Cx) -> Result<()> {
        if self.committed {
            return Ok(());
        }
        if !self.is_writer {
            self.committed = true;
            return Ok(());
        }

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;

        let commit_result = (|| -> Result<()> {
            if !self.write_set.is_empty() {
                // Phase 1: Write rollback journal with pre-images.
                let nonce = 0x4652_414E; // "FRAN" — deterministic nonce.
                let page_size = inner.page_size;
                let ps = page_size.as_usize();

                let jrnl_flags =
                    VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_JOURNAL;
                let (mut jrnl_file, _) = self.vfs.open(cx, Some(&self.journal_path), jrnl_flags)?;

                let header = JournalHeader {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                    page_count: self.write_set.len() as i32,
                    nonce,
                    initial_db_size: self.original_db_size,
                    sector_size: 512,
                    page_size: page_size.get(),
                };
                let hdr_bytes = header.encode_padded();
                jrnl_file.write(cx, &hdr_bytes, 0)?;

                let mut jrnl_offset = hdr_bytes.len() as u64;
                for &page_no in self.write_set.keys() {
                    // Read current on-disk content as the pre-image.
                    let mut pre_image = vec![0u8; ps];
                    if page_no.get() <= inner.db_size {
                        let disk_offset = u64::from(page_no.get() - 1) * ps as u64;
                        let _ = inner.db_file.read(cx, &mut pre_image, disk_offset)?;
                    }

                    let record = JournalPageRecord::new(page_no.get(), pre_image, nonce);
                    let rec_bytes = record.encode();
                    jrnl_file.write(cx, &rec_bytes, jrnl_offset)?;
                    jrnl_offset += rec_bytes.len() as u64;
                }

                // Sync journal to ensure durability before modifying database.
                jrnl_file.sync(cx, SyncFlags::NORMAL)?;

                // Phase 2: Write dirty pages to database.
                for (page_no, data) in &self.write_set {
                    inner.flush_page(cx, *page_no, data)?;
                    inner.db_size = inner.db_size.max(page_no.get());
                }
                inner.db_file.sync(cx, SyncFlags::NORMAL)?;

                // Phase 3: Delete journal (commit point).
                let _ = self.vfs.delete(cx, &self.journal_path, true);
            }

            for page_no in self.freed_pages.drain(..) {
                inner.freelist.push(page_no);
            }
            Ok(())
        })();

        inner.writer_active = false;
        drop(inner);
        if commit_result.is_ok() {
            self.write_set.clear();
            self.committed = true;
        }
        commit_result
    }

    fn rollback(&mut self, cx: &Cx) -> Result<()> {
        self.write_set.clear();
        self.freed_pages.clear();
        if self.is_writer {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;
            inner.db_size = self.original_db_size;
            inner.writer_active = false;
            drop(inner);
            // Delete any partial journal file.
            let _ = self.vfs.delete(cx, &self.journal_path, true);
        }
        self.committed = false;
        Ok(())
    }
}

impl<V: Vfs> Drop for SimpleTransaction<V> {
    fn drop(&mut self) {
        if self.committed || !self.is_writer {
            return;
        }
        if let Ok(mut inner) = self.inner.lock() {
            inner.writer_active = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{MvccPager, TransactionHandle, TransactionMode};
    use fsqlite_types::PageSize;
    use fsqlite_types::flags::AccessFlags;
    use fsqlite_vfs::MemoryVfs;
    use std::path::PathBuf;

    const BEAD_ID: &str = "bd-bca.1";

    fn test_pager() -> (SimplePager<MemoryVfs>, PathBuf) {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/test.db");
        let pager = SimplePager::open(vfs, &path, PageSize::DEFAULT).unwrap();
        (pager, path)
    }

    #[test]
    fn test_open_empty_database() {
        let (pager, _) = test_pager();
        let inner = pager.inner.lock().unwrap();
        assert_eq!(inner.db_size, 0, "bead_id={BEAD_ID} case=empty_db_size");
        assert_eq!(
            inner.page_size,
            PageSize::DEFAULT,
            "bead_id={BEAD_ID} case=page_size_default"
        );
        drop(inner);
    }

    #[test]
    fn test_begin_readonly_transaction() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        assert!(!txn.is_writer, "bead_id={BEAD_ID} case=readonly_not_writer");
    }

    #[test]
    fn test_begin_write_transaction() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        assert!(txn.is_writer, "bead_id={BEAD_ID} case=immediate_is_writer");
    }

    #[test]
    fn test_begin_deferred_transaction_starts_reader() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Deferred).unwrap();
        assert!(
            !txn.is_writer,
            "bead_id={BEAD_ID} case=deferred_starts_readonly"
        );
    }

    #[test]
    fn test_deferred_upgrades_on_first_write_intent() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let mut deferred = pager.begin(&cx, TransactionMode::Deferred).unwrap();
        assert!(
            !deferred.is_writer,
            "bead_id={BEAD_ID} case=deferred_pre_upgrade"
        );

        let _page = deferred.allocate_page(&cx).unwrap();
        assert!(
            deferred.is_writer,
            "bead_id={BEAD_ID} case=deferred_upgraded_to_writer"
        );
    }

    #[test]
    fn test_deferred_upgrade_busy_when_writer_active() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let _writer = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let mut deferred = pager.begin(&cx, TransactionMode::Deferred).unwrap();

        let err = deferred.allocate_page(&cx).unwrap_err();
        assert!(matches!(err, FrankenError::Busy));
    }

    #[test]
    fn test_concurrent_writer_blocked() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let _txn1 = pager.begin(&cx, TransactionMode::Exclusive).unwrap();
        let result = pager.begin(&cx, TransactionMode::Immediate);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=concurrent_writer_busy"
        );
    }

    #[test]
    fn test_multiple_readers_allowed() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let _r1 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let _r2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        // Both readers can coexist.
    }

    #[test]
    fn test_write_page_and_read_back() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        let page_no = txn.allocate_page(&cx).unwrap();
        let page_size = PageSize::DEFAULT.as_usize();
        let mut data = vec![0_u8; page_size];
        data[0] = 0xDE;
        data[1] = 0xAD;
        txn.write_page(&cx, page_no, &data).unwrap();

        let read_back = txn.get_page(&cx, page_no).unwrap();
        assert_eq!(
            read_back.as_ref()[0],
            0xDE,
            "bead_id={BEAD_ID} case=read_back_byte0"
        );
        assert_eq!(
            read_back.as_ref()[1],
            0xAD,
            "bead_id={BEAD_ID} case=read_back_byte1"
        );
    }

    #[test]
    fn test_commit_persists_pages() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        // Write in first transaction.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let page_no = txn.allocate_page(&cx).unwrap();
        let page_size = PageSize::DEFAULT.as_usize();
        let mut data = vec![0_u8; page_size];
        data[0..4].copy_from_slice(&[0xCA, 0xFE, 0xBA, 0xBE]);
        txn.write_page(&cx, page_no, &data).unwrap();
        txn.commit(&cx).unwrap();

        // Read in second transaction.
        let txn2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let read_back = txn2.get_page(&cx, page_no).unwrap();
        assert_eq!(
            &read_back.as_ref()[0..4],
            &[0xCA, 0xFE, 0xBA, 0xBE],
            "bead_id={BEAD_ID} case=commit_persists"
        );
    }

    #[test]
    fn test_rollback_discards_writes() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        // Allocate and write a page, then commit so it exists on disk.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let page_no = txn.allocate_page(&cx).unwrap();
        let page_size = PageSize::DEFAULT.as_usize();
        let original = vec![0x11_u8; page_size];
        txn.write_page(&cx, page_no, &original).unwrap();
        txn.commit(&cx).unwrap();

        // Overwrite in a new transaction, then rollback.
        let mut txn2 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let modified = vec![0x99_u8; page_size];
        txn2.write_page(&cx, page_no, &modified).unwrap();
        txn2.rollback(&cx).unwrap();

        // Read again — should see original data.
        let txn3 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let read_back = txn3.get_page(&cx, page_no).unwrap();
        assert_eq!(
            read_back.as_ref()[0],
            0x11,
            "bead_id={BEAD_ID} case=rollback_restores"
        );
    }

    #[test]
    fn test_allocate_returns_sequential_pages() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        let p1 = txn.allocate_page(&cx).unwrap();
        let p2 = txn.allocate_page(&cx).unwrap();
        assert!(
            p2.get() > p1.get(),
            "bead_id={BEAD_ID} case=sequential_alloc p1={} p2={}",
            p1.get(),
            p2.get()
        );
    }

    #[test]
    fn test_free_page_reuses_on_next_alloc() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        // Allocate two pages and commit.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        let p2 = txn.allocate_page(&cx).unwrap();
        let page_size = PageSize::DEFAULT.as_usize();
        txn.write_page(&cx, p1, &vec![1_u8; page_size]).unwrap();
        txn.write_page(&cx, p2, &vec![2_u8; page_size]).unwrap();
        txn.commit(&cx).unwrap();

        // Free p1 and commit.
        let mut txn2 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn2.free_page(&cx, p1).unwrap();
        txn2.commit(&cx).unwrap();

        // Next allocation should reuse freed page.
        let mut txn3 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p3 = txn3.allocate_page(&cx).unwrap();
        assert_eq!(
            p3,
            p1,
            "bead_id={BEAD_ID} case=freelist_reuse p3={} p1={}",
            p3.get(),
            p1.get()
        );
    }

    #[test]
    fn test_cannot_free_page_one() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let result = txn.free_page(&cx, PageNumber::ONE);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=cannot_free_page_one"
        );
    }

    #[test]
    fn test_readonly_cannot_write() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let result = txn.write_page(&cx, PageNumber::ONE, &[0_u8; 4096]);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=readonly_cannot_write"
        );
    }

    #[test]
    fn test_readonly_cannot_allocate() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let result = txn.allocate_page(&cx);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=readonly_cannot_allocate"
        );
    }

    #[test]
    fn test_drop_uncommitted_writer_releases_lock() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        {
            let _txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            // Dropped without commit or rollback.
        }

        // Should be able to begin a new writer.
        let txn2 = pager.begin(&cx, TransactionMode::Immediate);
        assert!(
            txn2.is_ok(),
            "bead_id={BEAD_ID} case=drop_releases_writer_lock"
        );
    }

    #[test]
    fn test_commit_then_drop_no_double_release() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            txn.commit(&cx).unwrap();
            // committed=true, drop should skip writer_active=false
        }

        // Writer should already be released by commit.
        let txn2 = pager.begin(&cx, TransactionMode::Immediate);
        assert!(
            txn2.is_ok(),
            "bead_id={BEAD_ID} case=commit_releases_writer"
        );
    }

    #[test]
    fn test_double_commit_is_idempotent() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn.commit(&cx).unwrap();
        // Second commit should be a no-op.
        txn.commit(&cx).unwrap();
    }

    #[test]
    fn test_multi_page_write_commit_read() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let page_size = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let mut allocated_pages = Vec::new();
        for i in 0_u8..5 {
            let p = txn.allocate_page(&cx).unwrap();
            let data = vec![i; page_size];
            txn.write_page(&cx, p, &data).unwrap();
            allocated_pages.push(p);
        }
        txn.commit(&cx).unwrap();

        let txn2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        for (i, p) in allocated_pages.iter().enumerate() {
            let data = txn2.get_page(&cx, *p).unwrap();
            #[allow(clippy::cast_possible_truncation)]
            let expected = i as u8;
            assert_eq!(
                data.as_ref()[0],
                expected,
                "bead_id={BEAD_ID} case=multi_page idx={i}"
            );
        }
    }

    // ── Journal crash recovery tests ────────────────────────────────────

    #[test]
    fn test_commit_creates_and_deletes_journal() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/jrnl_test.db");
        let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let cx = Cx::new();
        let journal_path = SimplePager::<MemoryVfs>::journal_path(&path);

        // Before commit, no journal.
        assert!(
            !vfs.access(&cx, &journal_path, AccessFlags::EXISTS).unwrap(),
            "bead_id={BEAD_ID} case=no_journal_before_commit"
        );

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p, &vec![0xAA; 4096]).unwrap();
        txn.commit(&cx).unwrap();

        // After commit, journal should be deleted.
        assert!(
            !vfs.access(&cx, &journal_path, AccessFlags::EXISTS).unwrap(),
            "bead_id={BEAD_ID} case=journal_deleted_after_commit"
        );
    }

    #[test]
    fn test_hot_journal_recovery_restores_original_data() {
        // Simulate a crash: write data, manually create a journal with pre-images,
        // then reopen. The journal should be replayed, restoring original data.
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/crash_test.db");
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Step 1: Create a database with known data via normal commit.
        {
            let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let p1 = txn.allocate_page(&cx).unwrap();
            assert_eq!(p1.get(), 2);
            txn.write_page(&cx, p1, &vec![0x11; ps]).unwrap();
            txn.commit(&cx).unwrap();
        }

        // Step 2: Corrupt the database (simulate a partial write that crashed).
        {
            let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
            let (mut db_file, _) = vfs.open(&cx, Some(&path), flags).unwrap();
            let corrupt_data = vec![0x99; ps];
            let offset = u64::from(2_u32 - 1) * ps as u64;
            db_file.write(&cx, &corrupt_data, offset).unwrap();
        }

        // Step 3: Create a hot journal with the original pre-image.
        {
            let journal_path = SimplePager::<MemoryVfs>::journal_path(&path);
            let jrnl_flags =
                VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_JOURNAL;
            let (mut jrnl, _) = vfs.open(&cx, Some(&journal_path), jrnl_flags).unwrap();

            let nonce = 42;
            let header = JournalHeader {
                page_count: 1,
                nonce,
                initial_db_size: 2,
                sector_size: 512,
                page_size: 4096,
            };
            let hdr_bytes = header.encode_padded();
            jrnl.write(&cx, &hdr_bytes, 0).unwrap();

            let record = JournalPageRecord::new(2, vec![0x11; ps], nonce);
            let rec_bytes = record.encode();
            jrnl.write(&cx, &rec_bytes, hdr_bytes.len() as u64).unwrap();
        }

        // Step 4: Reopen — should detect hot journal and replay.
        {
            let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
            let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
            let page_no_2 = PageNumber::new(2).unwrap();
            let data = txn.get_page(&cx, page_no_2).unwrap();

            assert_eq!(
                data.as_ref()[0],
                0x11,
                "bead_id={BEAD_ID} case=journal_recovery_restores"
            );
            assert_eq!(
                data.as_ref()[ps - 1],
                0x11,
                "bead_id={BEAD_ID} case=journal_recovery_restores_last_byte"
            );
        }

        // Step 5: Verify journal is deleted after recovery.
        let journal_path = SimplePager::<MemoryVfs>::journal_path(&path);
        assert!(
            !vfs.access(&cx, &journal_path, AccessFlags::EXISTS).unwrap(),
            "bead_id={BEAD_ID} case=journal_deleted_after_recovery"
        );
    }

    #[test]
    fn test_hot_journal_truncated_record_stops_replay() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/trunc_jrnl.db");
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Create DB with 2 pages.
        {
            let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let p1 = txn.allocate_page(&cx).unwrap();
            let p2 = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, p1, &vec![0xAA; ps]).unwrap();
            txn.write_page(&cx, p2, &vec![0xBB; ps]).unwrap();
            txn.commit(&cx).unwrap();
        }

        // Corrupt page 2.
        {
            let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
            let (mut db_file, _) = vfs.open(&cx, Some(&path), flags).unwrap();
            db_file
                .write(&cx, &vec![0xFF; ps], u64::from(2_u32 - 1) * ps as u64)
                .unwrap();
        }

        // Journal claims 2 records but second is truncated.
        {
            let journal_path = SimplePager::<MemoryVfs>::journal_path(&path);
            let jrnl_flags =
                VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_JOURNAL;
            let (mut jrnl, _) = vfs.open(&cx, Some(&journal_path), jrnl_flags).unwrap();

            let nonce = 7;
            let header = JournalHeader {
                page_count: 2,
                nonce,
                initial_db_size: 3,
                sector_size: 512,
                page_size: 4096,
            };
            let hdr_bytes = header.encode_padded();
            jrnl.write(&cx, &hdr_bytes, 0).unwrap();

            // First record: valid pre-image for page 3.
            let rec1 = JournalPageRecord::new(3, vec![0xCC; ps], nonce);
            let rec1_bytes = rec1.encode();
            jrnl.write(&cx, &rec1_bytes, hdr_bytes.len() as u64)
                .unwrap();

            // Second record: truncated.
            let rec2 = JournalPageRecord::new(2, vec![0xBB; ps], nonce);
            let rec2_bytes = rec2.encode();
            let trunc_len = rec2_bytes.len() / 2;
            let offset = hdr_bytes.len() as u64 + rec1_bytes.len() as u64;
            jrnl.write(&cx, &rec2_bytes[..trunc_len], offset).unwrap();
        }

        // Reopen — first record replays, second skipped.
        {
            let pager = SimplePager::open(vfs, &path, PageSize::DEFAULT).unwrap();
            let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
            let page_no_2 = PageNumber::new(2).unwrap();
            let data2 = txn.get_page(&cx, page_no_2).unwrap();
            assert_eq!(
                data2.as_ref()[0],
                0xFF,
                "bead_id={BEAD_ID} case=truncated_journal_page2_not_restored"
            );
        }
    }

    #[test]
    fn test_hot_journal_checksum_mismatch_stops_replay() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/cksum_jrnl.db");
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        {
            let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let p = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, p, &vec![0x55; ps]).unwrap();
            txn.commit(&cx).unwrap();
        }

        // Corrupt page 2.
        {
            let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
            let (mut db_file, _) = vfs.open(&cx, Some(&path), flags).unwrap();
            db_file
                .write(&cx, &vec![0xEE; ps], u64::from(2_u32 - 1) * ps as u64)
                .unwrap();
        }

        // Journal with wrong nonce in record (checksum won't verify).
        {
            let journal_path = SimplePager::<MemoryVfs>::journal_path(&path);
            let jrnl_flags =
                VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_JOURNAL;
            let (mut jrnl, _) = vfs.open(&cx, Some(&journal_path), jrnl_flags).unwrap();

            let nonce = 99;
            let header = JournalHeader {
                page_count: 1,
                nonce,
                initial_db_size: 2,
                sector_size: 512,
                page_size: 4096,
            };
            let hdr_bytes = header.encode_padded();
            jrnl.write(&cx, &hdr_bytes, 0).unwrap();

            // Wrong nonce in record.
            let record = JournalPageRecord::new(2, vec![0x55; ps], nonce + 1);
            let rec_bytes = record.encode();
            jrnl.write(&cx, &rec_bytes, hdr_bytes.len() as u64).unwrap();
        }

        // Reopen — bad checksum stops replay.
        {
            let pager = SimplePager::open(vfs, &path, PageSize::DEFAULT).unwrap();
            let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
            let page_no_2 = PageNumber::new(2).unwrap();
            let data = txn.get_page(&cx, page_no_2).unwrap();
            assert_eq!(
                data.as_ref()[0],
                0xEE,
                "bead_id={BEAD_ID} case=bad_checksum_stops_replay"
            );
        }
    }

    #[test]
    fn test_journal_not_created_for_readonly_commit() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/readonly_jrnl.db");
        let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let cx = Cx::new();
        let journal_path = SimplePager::<MemoryVfs>::journal_path(&path);

        let mut txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        txn.commit(&cx).unwrap();

        assert!(
            !vfs.access(&cx, &journal_path, AccessFlags::EXISTS).unwrap(),
            "bead_id={BEAD_ID} case=no_journal_for_readonly"
        );
    }

    #[test]
    fn test_rollback_deletes_journal() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/rollback_jrnl.db");
        let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let cx = Cx::new();
        let journal_path = SimplePager::<MemoryVfs>::journal_path(&path);

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p, &vec![0xDD; 4096]).unwrap();
        txn.rollback(&cx).unwrap();

        assert!(
            !vfs.access(&cx, &journal_path, AccessFlags::EXISTS).unwrap(),
            "bead_id={BEAD_ID} case=journal_deleted_on_rollback"
        );
    }
}
