//! Concrete single-writer pager for Phase 5 persistence.
//!
//! `SimplePager` implements [`MvccPager`] with single-writer semantics over a
//! VFS-backed database file and a zero-copy [`PageCache`].
//! Full concurrent MVCC behavior is layered on top in Phase 6.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
use fsqlite_types::{PageData, PageNumber, PageSize};
use fsqlite_vfs::{Vfs, VfsFile};

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
    /// Kept for future journal/WAL companion file operations.
    _vfs: V,
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

        let is_writer = !matches!(mode, TransactionMode::ReadOnly);
        if is_writer && inner.writer_active {
            return Err(FrankenError::Busy);
        }
        if is_writer {
            inner.writer_active = true;
        }
        let original_db_size = inner.db_size;
        drop(inner);

        Ok(SimpleTransaction {
            inner: Arc::clone(&self.inner),
            write_set: HashMap::new(),
            freed_pages: Vec::new(),
            is_writer,
            committed: false,
            original_db_size,
        })
    }
}

impl<V: Vfs> SimplePager<V>
where
    V::File: Send + Sync,
{
    /// Open (or create) a database and return a pager.
    pub fn open(vfs: V, path: &Path, page_size: PageSize) -> Result<Self> {
        let cx = Cx::new();
        let flags = VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
        let (db_file, _actual_flags) = vfs.open(&cx, Some(path), flags)?;

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
            _vfs: vfs,
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
}

/// Transaction handle produced by [`SimplePager`].
pub struct SimpleTransaction<V: Vfs> {
    inner: Arc<Mutex<PagerInner<V::File>>>,
    write_set: HashMap<PageNumber, Vec<u8>>,
    freed_pages: Vec<PageNumber>,
    is_writer: bool,
    committed: bool,
    original_db_size: u32,
}

impl<V: Vfs> traits::sealed::Sealed for SimpleTransaction<V> {}

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
        if !self.is_writer {
            return Err(FrankenError::ReadOnly);
        }

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
        if !self.is_writer {
            return Err(FrankenError::ReadOnly);
        }

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
        if !self.is_writer {
            return Err(FrankenError::ReadOnly);
        }
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
            for (page_no, data) in &self.write_set {
                inner.flush_page(cx, *page_no, data)?;
                inner.db_size = inner.db_size.max(page_no.get());
            }
            for page_no in self.freed_pages.drain(..) {
                inner.freelist.push(page_no);
            }
            inner.db_file.sync(cx, SyncFlags::NORMAL)?;
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

    fn rollback(&mut self, _cx: &Cx) -> Result<()> {
        self.write_set.clear();
        self.freed_pages.clear();
        if self.is_writer {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;
            inner.db_size = self.original_db_size;
            inner.writer_active = false;
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
