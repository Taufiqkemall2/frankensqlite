//! B-tree cursor implementation (§11, bd-2kvo).
//!
//! A cursor maintains a position within a single B-tree (either table or
//! index) and supports seek, navigation, and payload access. The cursor
//! uses a page stack of depth up to [`BTREE_MAX_DEPTH`] (20) to track
//! its path from root to leaf.
//!
//! # Architecture
//!
//! ```text
//!               ┌─────────┐
//!   stack[0]    │ Root     │  cell_idx = 1
//!               └────┬────┘
//!               ┌────▼────┐
//!   stack[1]    │ Interior│  cell_idx = 0
//!               └────┬────┘
//!               ┌────▼────┐
//!   stack[2]    │ Leaf    │  cell_idx = 3  ← current position
//!               └─────────┘
//! ```

use crate::balance;
use crate::cell::{self, BtreePageHeader, CellRef};
use crate::overflow;
use crate::traits::{BtreeCursorOps, SeekResult, sealed};
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::limits::BTREE_MAX_DEPTH;
use fsqlite_types::serial_type::write_varint;
use fsqlite_types::{PageNumber, WitnessKey};
use tracing::warn;

const MAX_MUTATION_OVERFLOW_CHAIN: usize = 1_000_000;

// ---------------------------------------------------------------------------
// Page reader trait (for testability)
// ---------------------------------------------------------------------------

/// Trait for reading raw page data by page number.
///
/// This allows the cursor to be tested with an in-memory page store.
/// The real implementation wraps a `TransactionHandle`.
pub trait PageReader {
    /// Read a page by number, returning the raw bytes.
    fn read_page(&self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>>;
}

/// Trait for writing pages (needed for insert/delete).
pub trait PageWriter: PageReader {
    /// Write raw data to a page.
    fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()>;
    /// Allocate a new page.
    fn allocate_page(&mut self, cx: &Cx) -> Result<PageNumber>;
    /// Free a page.
    fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Cursor stack entry
// ---------------------------------------------------------------------------

/// A single entry in the cursor's page stack.
#[derive(Debug, Clone)]
struct StackEntry {
    /// Page number of this page (retained for debugging and future use in
    /// mutation operations).
    #[allow(dead_code)]
    page_no: PageNumber,
    /// Cached raw page data.
    page_data: Vec<u8>,
    /// Parsed page header.
    header: BtreePageHeader,
    /// Cell pointer offsets (cached from the cell pointer array).
    cell_pointers: Vec<u16>,
    /// Current cell index. For interior pages, this indicates which child
    /// was descended into. For leaf pages, this is the current position.
    /// A value equal to `cell_count` means "past the right-most child" on
    /// interior pages, or "past the last cell" on leaf pages.
    cell_idx: u16,
}

// ---------------------------------------------------------------------------
// BtCursor
// ---------------------------------------------------------------------------

/// A B-tree cursor that navigates through B-tree pages using a page stack.
///
/// Generic over the page I/O backend for testability.
#[derive(Debug)]
pub struct BtCursor<P> {
    /// Page I/O backend.
    pager: P,
    /// Root page number of the B-tree.
    root_page: PageNumber,
    /// Usable page size (page_size - reserved_bytes).
    usable_size: u32,
    /// Whether this is a table (intkey) or index (blobkey) B-tree (retained
    /// for future validation in mutation operations).
    #[allow(dead_code)]
    is_table: bool,
    /// Page stack from root to current leaf.
    stack: Vec<StackEntry>,
    /// Whether the cursor is at EOF (past the last entry).
    at_eof: bool,
    /// Read witnesses collected for SSI evidence.
    read_witnesses: Vec<WitnessKey>,
}

impl<P: PageReader> BtCursor<P> {
    /// Create a new cursor positioned before the first entry (at EOF).
    #[must_use]
    pub fn new(pager: P, root_page: PageNumber, usable_size: u32, is_table: bool) -> Self {
        Self {
            pager,
            root_page,
            usable_size,
            is_table,
            stack: Vec::with_capacity(BTREE_MAX_DEPTH as usize),
            at_eof: true,
            read_witnesses: Vec::new(),
        }
    }

    /// Returns the read witness keys captured by the cursor.
    #[must_use]
    pub fn witness_keys(&self) -> &[WitnessKey] {
        &self.read_witnesses
    }

    /// Clears captured read witness keys.
    pub fn clear_witness_keys(&mut self) {
        self.read_witnesses.clear();
    }

    #[inline]
    fn cell_tag_from_rowid(rowid: i64) -> u64 {
        u64::from_ne_bytes(rowid.to_ne_bytes())
    }

    fn cell_tag_from_index_key(key: &[u8]) -> u64 {
        // Deterministic FNV-1a hash keeps tags stable across runs.
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for &byte in key {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3_u64);
        }
        hash
    }

    fn record_point_witness(&mut self, key: WitnessKey) {
        if let WitnessKey::Page(page) = key {
            warn!(
                root_page = self.root_page.get(),
                page = page.get(),
                "policy violation: point operation emitted page-level witness"
            );
        }
        self.read_witnesses.push(key);
    }

    fn record_range_page_witness(&mut self, page_no: PageNumber) {
        self.read_witnesses.push(WitnessKey::Page(page_no));
    }

    /// Load a page into a stack entry.
    fn load_page(&self, cx: &Cx, page_no: PageNumber) -> Result<StackEntry> {
        let page_data = self.pager.read_page(cx, page_no)?;
        let header_offset = cell::header_offset_for_page(page_no);
        let header = BtreePageHeader::parse(&page_data, header_offset)?;
        let cell_pointers = cell::read_cell_pointers(&page_data, &header, header_offset)?;

        Ok(StackEntry {
            page_no,
            page_data,
            header,
            cell_pointers,
            cell_idx: 0,
        })
    }

    /// Parse a cell at the given index on the top-of-stack page.
    fn parse_cell_at(&self, entry: &StackEntry, idx: u16) -> Result<CellRef> {
        let offset = entry.cell_pointers[idx as usize] as usize;
        CellRef::parse(
            &entry.page_data,
            offset,
            entry.header.page_type,
            self.usable_size,
        )
    }

    /// Move the cursor to the first entry in the subtree rooted at `page_no`.
    fn move_to_leftmost_leaf(
        &mut self,
        cx: &Cx,
        page_no: PageNumber,
        record_leaf_witness: bool,
    ) -> Result<bool> {
        let mut current_page = page_no;
        loop {
            if self.stack.len() >= BTREE_MAX_DEPTH as usize {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("B-tree depth exceeds maximum of {}", BTREE_MAX_DEPTH),
                });
            }

            let mut entry = self.load_page(cx, current_page)?;
            entry.cell_idx = 0;

            if entry.header.page_type.is_leaf() {
                let leaf_page_no = entry.page_no;
                if entry.header.cell_count == 0 {
                    self.stack.push(entry);
                    self.at_eof = true;
                    if record_leaf_witness {
                        self.record_range_page_witness(leaf_page_no);
                    }
                    return Ok(false);
                }
                self.stack.push(entry);
                self.at_eof = false;
                if record_leaf_witness {
                    self.record_range_page_witness(leaf_page_no);
                }
                return Ok(true);
            }

            // Interior page: follow the leftmost child (cell 0's left child).
            if entry.header.cell_count == 0 {
                // Interior page with no cells — follow right_child.
                let right =
                    entry
                        .header
                        .right_child
                        .ok_or_else(|| FrankenError::DatabaseCorrupt {
                            detail: "interior page has no right child".to_owned(),
                        })?;
                entry.cell_idx = 0;
                self.stack.push(entry);
                current_page = right;
            } else {
                let cell = self.parse_cell_at(&entry, 0)?;
                let child = cell
                    .left_child
                    .ok_or_else(|| FrankenError::DatabaseCorrupt {
                        detail: "interior cell has no left child".to_owned(),
                    })?;
                entry.cell_idx = 0;
                self.stack.push(entry);
                current_page = child;
            }
        }
    }

    /// Move the cursor to the last entry in the subtree rooted at `page_no`.
    fn move_to_rightmost_leaf(
        &mut self,
        cx: &Cx,
        page_no: PageNumber,
        record_leaf_witness: bool,
    ) -> Result<bool> {
        let mut current_page = page_no;
        loop {
            if self.stack.len() >= BTREE_MAX_DEPTH as usize {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("B-tree depth exceeds maximum of {}", BTREE_MAX_DEPTH),
                });
            }

            let mut entry = self.load_page(cx, current_page)?;

            if entry.header.page_type.is_leaf() {
                let leaf_page_no = entry.page_no;
                if entry.header.cell_count == 0 {
                    self.stack.push(entry);
                    self.at_eof = true;
                    if record_leaf_witness {
                        self.record_range_page_witness(leaf_page_no);
                    }
                    return Ok(false);
                }
                entry.cell_idx = entry.header.cell_count - 1;
                self.stack.push(entry);
                self.at_eof = false;
                if record_leaf_witness {
                    self.record_range_page_witness(leaf_page_no);
                }
                return Ok(true);
            }

            // Interior page: follow the right-most child.
            let right = entry
                .header
                .right_child
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "interior page has no right child".to_owned(),
                })?;
            entry.cell_idx = entry.header.cell_count;
            self.stack.push(entry);
            current_page = right;
        }
    }

    /// Get the child page for an interior page at the given cell index.
    ///
    /// If `cell_idx == cell_count`, returns the right child.
    /// Otherwise, returns the left child of the cell at `cell_idx`.
    fn child_page_at(&self, entry: &StackEntry, cell_idx: u16) -> Result<PageNumber> {
        if cell_idx >= entry.header.cell_count {
            entry
                .header
                .right_child
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "interior page has no right child".to_owned(),
                })
        } else {
            let cell = self.parse_cell_at(entry, cell_idx)?;
            cell.left_child
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "interior cell has no left child".to_owned(),
                })
        }
    }

    /// Seek to a rowid in a table B-tree. Returns the seek result.
    fn table_seek(&mut self, cx: &Cx, target_rowid: i64) -> Result<SeekResult> {
        self.stack.clear();
        let mut current_page = self.root_page;

        loop {
            if self.stack.len() >= BTREE_MAX_DEPTH as usize {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("B-tree depth exceeds maximum of {}", BTREE_MAX_DEPTH),
                });
            }

            let entry = self.load_page(cx, current_page)?;

            if entry.header.page_type.is_leaf() {
                // Binary search on the leaf page by rowid.
                let result = self.binary_search_table_leaf(&entry, target_rowid)?;
                match result {
                    BinarySearchResult::Found(idx) => {
                        let mut entry = entry;
                        entry.cell_idx = idx;
                        self.stack.push(entry);
                        self.at_eof = false;
                        self.record_point_witness(WitnessKey::Cell {
                            btree_root: self.root_page,
                            tag: Self::cell_tag_from_rowid(target_rowid),
                        });
                        return Ok(SeekResult::Found);
                    }
                    BinarySearchResult::NotFound(idx) => {
                        let mut entry = entry;
                        if idx >= entry.header.cell_count {
                            entry.cell_idx = entry.header.cell_count;
                            self.stack.push(entry);
                            self.at_eof = true;
                        } else {
                            entry.cell_idx = idx;
                            self.stack.push(entry);
                            self.at_eof = false;
                        }
                        self.record_point_witness(WitnessKey::Cell {
                            btree_root: self.root_page,
                            tag: Self::cell_tag_from_rowid(target_rowid),
                        });
                        return Ok(SeekResult::NotFound);
                    }
                }
            }

            // Interior table page: binary search to find which child to descend.
            let child_idx = self.binary_search_table_interior(&entry, target_rowid)?;
            let child = self.child_page_at(&entry, child_idx)?;
            let mut entry = entry;
            entry.cell_idx = child_idx;
            self.stack.push(entry);
            current_page = child;
        }
    }

    /// Binary search a leaf table page for a rowid.
    fn binary_search_table_leaf(
        &self,
        entry: &StackEntry,
        target: i64,
    ) -> Result<BinarySearchResult> {
        let count = entry.header.cell_count;
        if count == 0 {
            return Ok(BinarySearchResult::NotFound(0));
        }

        let mut lo = 0u16;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cell = self.parse_cell_at(entry, mid)?;
            let rowid = cell.rowid.ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "table leaf cell has no rowid".to_owned(),
            })?;

            match rowid.cmp(&target) {
                std::cmp::Ordering::Equal => return Ok(BinarySearchResult::Found(mid)),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        Ok(BinarySearchResult::NotFound(lo))
    }

    /// Binary search an interior table page to find which child to descend.
    ///
    /// Returns the child index (0..=cell_count). If the target is greater
    /// than all keys, returns cell_count (meaning follow right_child).
    fn binary_search_table_interior(&self, entry: &StackEntry, target: i64) -> Result<u16> {
        let count = entry.header.cell_count;
        if count == 0 {
            return Ok(0); // Follow right_child.
        }

        // Interior table cells are sorted by rowid. We want the child
        // whose subtree may contain `target`.
        //
        // Cell[i] has left_child and rowid. The left_child subtree contains
        // rowids < cell[i].rowid. If target <= cell[i].rowid, descend into
        // left_child of cell[i]. If target > last cell's rowid, follow right_child.
        let mut lo = 0u16;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cell = self.parse_cell_at(entry, mid)?;
            let rowid = cell.rowid.ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "interior table cell has no rowid".to_owned(),
            })?;

            if target <= rowid {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        Ok(lo)
    }

    /// Seek to a key in an index B-tree. Returns the seek result.
    fn index_seek(&mut self, cx: &Cx, target_key: &[u8]) -> Result<SeekResult> {
        self.stack.clear();
        let mut current_page = self.root_page;

        loop {
            if self.stack.len() >= BTREE_MAX_DEPTH as usize {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("B-tree depth exceeds maximum of {}", BTREE_MAX_DEPTH),
                });
            }

            let entry = self.load_page(cx, current_page)?;

            if entry.header.page_type.is_leaf() {
                let result = self.binary_search_index_leaf(cx, &entry, target_key)?;
                match result {
                    BinarySearchResult::Found(idx) => {
                        let mut entry = entry;
                        entry.cell_idx = idx;
                        self.stack.push(entry);
                        self.at_eof = false;
                        self.record_point_witness(WitnessKey::Cell {
                            btree_root: self.root_page,
                            tag: Self::cell_tag_from_index_key(target_key),
                        });
                        return Ok(SeekResult::Found);
                    }
                    BinarySearchResult::NotFound(idx) => {
                        let mut entry = entry;
                        if idx >= entry.header.cell_count {
                            entry.cell_idx = entry.header.cell_count;
                            self.stack.push(entry);
                            self.at_eof = true;
                        } else {
                            entry.cell_idx = idx;
                            self.stack.push(entry);
                            self.at_eof = false;
                        }
                        self.record_point_witness(WitnessKey::Cell {
                            btree_root: self.root_page,
                            tag: Self::cell_tag_from_index_key(target_key),
                        });
                        return Ok(SeekResult::NotFound);
                    }
                }
            }

            // Interior index page: binary search to find which child to descend.
            let child_idx = self.binary_search_index_interior(cx, &entry, target_key)?;
            let child = self.child_page_at(&entry, child_idx)?;
            let mut entry = entry;
            entry.cell_idx = child_idx;
            self.stack.push(entry);
            current_page = child;
        }
    }

    /// Read the full payload for a cell (resolving overflow if needed).
    fn read_cell_payload(&self, cx: &Cx, entry: &StackEntry, cell: &CellRef) -> Result<Vec<u8>> {
        let local = cell.local_payload(&entry.page_data);

        if let Some(first_overflow) = cell.overflow_page {
            overflow::read_overflow_chain(
                local,
                first_overflow,
                cell.payload_size,
                self.usable_size,
                &mut |pgno| self.pager.read_page(cx, pgno),
            )
        } else {
            Ok(local.to_vec())
        }
    }

    /// Binary search a leaf index page for a key.
    fn binary_search_index_leaf(
        &self,
        cx: &Cx,
        entry: &StackEntry,
        target: &[u8],
    ) -> Result<BinarySearchResult> {
        let count = entry.header.cell_count;
        if count == 0 {
            return Ok(BinarySearchResult::NotFound(0));
        }

        let mut lo = 0u16;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cell = self.parse_cell_at(entry, mid)?;
            let key = self.read_cell_payload(cx, entry, &cell)?;

            match key.as_slice().cmp(target) {
                std::cmp::Ordering::Equal => return Ok(BinarySearchResult::Found(mid)),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        Ok(BinarySearchResult::NotFound(lo))
    }

    /// Binary search an interior index page to find which child to descend.
    fn binary_search_index_interior(
        &self,
        cx: &Cx,
        entry: &StackEntry,
        target: &[u8],
    ) -> Result<u16> {
        let count = entry.header.cell_count;
        if count == 0 {
            return Ok(0);
        }

        let mut lo = 0u16;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cell = self.parse_cell_at(entry, mid)?;
            let key = self.read_cell_payload(cx, entry, &cell)?;

            if target <= key.as_slice() {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        Ok(lo)
    }

    /// Advance to the next entry. Returns false if at EOF.
    fn advance_next(&mut self, cx: &Cx) -> Result<bool> {
        if self.at_eof || self.stack.is_empty() {
            self.at_eof = true;
            return Ok(false);
        }

        let depth = self.stack.len();
        let top = &self.stack[depth - 1];

        // On a leaf page: try to advance to the next cell.
        if top.header.page_type.is_leaf() {
            let next_idx = top.cell_idx + 1;
            if next_idx < top.header.cell_count {
                self.stack[depth - 1].cell_idx = next_idx;
                return Ok(true);
            }

            // Past the last cell on this leaf. Pop up until we find an
            // interior page with more children to visit.
            self.stack.pop();
            while let Some(parent) = self.stack.last() {
                if parent.cell_idx < parent.header.cell_count {
                    // We came from the left child of cell[cell_idx].
                    // Move to the next child (right subtree of this separator).
                    let next_child_idx = parent.cell_idx + 1;
                    let child = self.child_page_at(parent, next_child_idx)?;
                    self.stack.last_mut().unwrap().cell_idx = next_child_idx;
                    return self.move_to_leftmost_leaf(cx, child, true);
                }
                // cell_idx == cell_count means we already visited right_child.
                self.stack.pop();
            }

            // Exhausted the entire tree.
            self.at_eof = true;
            return Ok(false);
        }

        // Should never be called on an interior page (cursor always at leaf).
        Err(FrankenError::internal(
            "cursor on interior page during next",
        ))
    }

    /// Move to the previous entry. Returns false if at the beginning.
    fn advance_prev(&mut self, cx: &Cx) -> Result<bool> {
        if self.stack.is_empty() {
            return Ok(false);
        }

        let depth = self.stack.len();
        let top = &self.stack[depth - 1];

        if top.header.page_type.is_leaf() {
            if top.cell_idx > 0 {
                self.stack[depth - 1].cell_idx -= 1;
                self.at_eof = false;
                return Ok(true);
            }

            // Before the first cell on this leaf. Pop up.
            self.stack.pop();
            while let Some(parent) = self.stack.last() {
                if parent.cell_idx > 0 {
                    let prev_child_idx = parent.cell_idx - 1;
                    let child = self.child_page_at(parent, prev_child_idx)?;
                    self.stack.last_mut().unwrap().cell_idx = prev_child_idx;
                    return self.move_to_rightmost_leaf(cx, child, true);
                }
                // cell_idx == 0 means we came from the leftmost child.
                self.stack.pop();
            }

            // At the very beginning of the tree.
            return Ok(false);
        }

        Err(FrankenError::internal(
            "cursor on interior page during prev",
        ))
    }
}

/// Result of a binary search within a page.
enum BinarySearchResult {
    /// Exact match found at this cell index.
    Found(u16),
    /// No match; the target would be inserted at this position.
    NotFound(u16),
}

impl<P: PageWriter> BtCursor<P> {
    fn write_overflow_chain_for_insert(
        &mut self,
        cx: &Cx,
        overflow_data: &[u8],
    ) -> Result<PageNumber> {
        if overflow_data.is_empty() {
            return Err(FrankenError::internal(
                "overflow writer called with empty payload",
            ));
        }
        if self.usable_size <= 4 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "invalid usable page size {} for overflow chain",
                    self.usable_size
                ),
            });
        }

        #[allow(clippy::cast_possible_truncation)]
        let bytes_per_page = (self.usable_size - 4) as usize;
        #[allow(clippy::cast_possible_truncation)]
        let page_size = self.usable_size as usize;
        let num_pages = overflow_data.len().div_ceil(bytes_per_page);

        let mut pages = Vec::with_capacity(num_pages);
        for _ in 0..num_pages {
            pages.push(self.pager.allocate_page(cx)?);
        }

        for (idx, &pgno) in pages.iter().enumerate() {
            let data_start = idx * bytes_per_page;
            let data_end = ((idx + 1) * bytes_per_page).min(overflow_data.len());
            let chunk = &overflow_data[data_start..data_end];

            let next = if idx + 1 < pages.len() {
                pages[idx + 1].get()
            } else {
                0
            };

            let mut page_buf = vec![0u8; page_size];
            page_buf[0..4].copy_from_slice(&next.to_be_bytes());
            page_buf[4..4 + chunk.len()].copy_from_slice(chunk);
            self.pager.write_page(cx, pgno, &page_buf)?;
        }

        Ok(pages[0])
    }

    fn free_overflow_chain(&mut self, cx: &Cx, first: PageNumber) -> Result<()> {
        let mut current = Some(first);
        let mut visited = 0usize;

        while let Some(pgno) = current {
            visited += 1;
            if visited > MAX_MUTATION_OVERFLOW_CHAIN {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "overflow chain exceeds {} pages while freeing",
                        MAX_MUTATION_OVERFLOW_CHAIN
                    ),
                });
            }

            let page = self.pager.read_page(cx, pgno)?;
            if page.len() < 4 {
                warn!(
                    page = pgno.get(),
                    page_len = page.len(),
                    "overflow chain corruption detected while freeing"
                );
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("overflow page {} too small while freeing", pgno.get()),
                });
            }

            let next = u32::from_be_bytes([page[0], page[1], page[2], page[3]]);
            current = PageNumber::new(next);
            self.pager.free_page(cx, pgno)?;
        }

        Ok(())
    }

    fn encode_table_leaf_cell(
        &mut self,
        cx: &Cx,
        rowid: i64,
        payload: &[u8],
    ) -> Result<(Vec<u8>, Option<PageNumber>)> {
        let payload_size_u64 = u64::try_from(payload.len()).map_err(|_| FrankenError::TooBig)?;
        #[allow(clippy::cast_possible_truncation)]
        let payload_size = payload_size_u64 as u32;
        let local_size = cell::local_payload_size(
            payload_size,
            self.usable_size,
            cell::BtreePageType::LeafTable,
        ) as usize;
        let local_size = local_size.min(payload.len());

        let mut out = Vec::with_capacity(24 + local_size + 4);
        let mut varint = [0u8; 9];
        let p_len = write_varint(&mut varint, payload_size_u64);
        out.extend_from_slice(&varint[..p_len]);

        let rowid_bits = u64::from_ne_bytes(rowid.to_ne_bytes());
        let r_len = write_varint(&mut varint, rowid_bits);
        out.extend_from_slice(&varint[..r_len]);
        out.extend_from_slice(&payload[..local_size]);

        if local_size < payload.len() {
            let first_overflow =
                self.write_overflow_chain_for_insert(cx, &payload[local_size..])?;
            out.extend_from_slice(&first_overflow.get().to_be_bytes());
            Ok((out, Some(first_overflow)))
        } else {
            Ok((out, None))
        }
    }

    fn encode_index_leaf_cell(
        &mut self,
        cx: &Cx,
        key: &[u8],
    ) -> Result<(Vec<u8>, Option<PageNumber>)> {
        let payload_size_u64 = u64::try_from(key.len()).map_err(|_| FrankenError::TooBig)?;
        #[allow(clippy::cast_possible_truncation)]
        let payload_size = payload_size_u64 as u32;
        let local_size = cell::local_payload_size(
            payload_size,
            self.usable_size,
            cell::BtreePageType::LeafIndex,
        ) as usize;
        let local_size = local_size.min(key.len());

        let mut out = Vec::with_capacity(16 + local_size + 4);
        let mut varint = [0u8; 9];
        let p_len = write_varint(&mut varint, payload_size_u64);
        out.extend_from_slice(&varint[..p_len]);
        out.extend_from_slice(&key[..local_size]);

        if local_size < key.len() {
            let first_overflow = self.write_overflow_chain_for_insert(cx, &key[local_size..])?;
            out.extend_from_slice(&first_overflow.get().to_be_bytes());
            Ok((out, Some(first_overflow)))
        } else {
            Ok((out, None))
        }
    }

    /// Try to insert a cell directly onto the leaf page at the top of the
    /// cursor stack. Returns `Ok(true)` if the cell was inserted, or
    /// `Ok(false)` if the page is full and balance is needed.
    fn try_insert_on_leaf(&mut self, cx: &Cx, insert_idx: u16, cell_data: &[u8]) -> Result<bool> {
        let depth = self.stack.len();
        if depth == 0 {
            return Err(FrankenError::internal("cursor stack is empty"));
        }
        let leaf_page_no = self.stack[depth - 1].page_no;

        let mut page_data = self.pager.read_page(cx, leaf_page_no)?;
        let header_offset = cell::header_offset_for_page(leaf_page_no);
        let mut header = BtreePageHeader::parse(&page_data, header_offset)?;
        let mut ptrs = cell::read_cell_pointers(&page_data, &header, header_offset)?;

        let content_offset = header.cell_content_offset as usize;
        let Some(new_content_offset) = content_offset.checked_sub(cell_data.len()) else {
            return Ok(false); // Page full.
        };

        let ptr_array_end = header_offset
            + usize::from(header.page_type.header_size())
            + (usize::from(header.cell_count) + 1) * 2;
        if ptr_array_end > new_content_offset {
            return Ok(false); // Page full.
        }

        // Cell fits — write directly.
        page_data[new_content_offset..new_content_offset + cell_data.len()]
            .copy_from_slice(cell_data);

        let insert_at = usize::from(insert_idx).min(ptrs.len());
        #[allow(clippy::cast_possible_truncation)]
        {
            ptrs.insert(insert_at, new_content_offset as u16);
            header.cell_count = ptrs.len() as u16;
            header.cell_content_offset = new_content_offset as u32;
        }
        header.write(&mut page_data, header_offset);
        cell::write_cell_pointers(&mut page_data, header_offset, &header, &ptrs);
        self.pager.write_page(cx, leaf_page_no, &page_data)?;

        // Refresh the top stack entry.
        let mut refreshed = self.load_page(cx, leaf_page_no)?;
        #[allow(clippy::cast_possible_truncation)]
        {
            refreshed.cell_idx = insert_at as u16;
        }
        self.stack[depth - 1] = refreshed;
        self.at_eof = false;
        Ok(true)
    }

    /// Balance the tree after an insert when the leaf page is full.
    ///
    /// `cell_data` is the cell that didn't fit. `insert_idx` is where within
    /// the leaf's cell array it should be placed.
    fn balance_for_insert(&mut self, cx: &Cx, cell_data: &[u8], insert_idx: u16) -> Result<()> {
        let depth = self.stack.len();
        if depth == 0 {
            return Err(FrankenError::internal("cursor stack empty during balance"));
        }

        if depth == 1 {
            // Leaf is the root — push root down first.
            balance::balance_deeper(cx, &mut self.pager, self.root_page, self.usable_size)?;
            // Root is now an interior page with 1 child at index 0.
            balance::balance_nonroot(
                cx,
                &mut self.pager,
                self.root_page,
                0,
                &[cell_data.to_vec()],
                insert_idx as usize,
                self.usable_size,
            )?;
        } else {
            let parent_page_no = self.stack[depth - 2].page_no;
            let child_idx = self.stack[depth - 2].cell_idx as usize;

            balance::balance_nonroot(
                cx,
                &mut self.pager,
                parent_page_no,
                child_idx,
                &[cell_data.to_vec()],
                insert_idx as usize,
                self.usable_size,
            )?;
        }

        // Tree structure changed — invalidate the cursor stack.
        self.stack.clear();
        self.at_eof = true;
        Ok(())
    }

    /// Remove the cell at the current cursor position from its leaf page.
    ///
    /// Does NOT trigger rebalancing — the caller is responsible for that.
    /// Returns the page number of the leaf and its new cell count.
    fn remove_cell_from_leaf(&mut self, cx: &Cx) -> Result<(PageNumber, u16)> {
        let depth = self.stack.len();
        if depth == 0 || self.at_eof {
            return Err(FrankenError::internal("cursor at EOF during remove"));
        }

        let top = self.stack[depth - 1].clone();
        if !top.header.page_type.is_leaf() {
            return Err(FrankenError::internal(
                "remove_cell_from_leaf called on interior page",
            ));
        }

        let delete_idx = usize::from(top.cell_idx);
        if delete_idx >= top.cell_pointers.len() {
            return Err(FrankenError::internal("cursor position out of bounds"));
        }

        // Free overflow chain if present.
        let cell_ref = self.parse_cell_at(&top, top.cell_idx)?;
        if let Some(first) = cell_ref.overflow_page {
            self.free_overflow_chain(cx, first)?;
        }

        let leaf_page_no = top.page_no;
        let mut page_data = self.pager.read_page(cx, leaf_page_no)?;
        let header_offset = cell::header_offset_for_page(leaf_page_no);
        let mut header = BtreePageHeader::parse(&page_data, header_offset)?;
        let mut ptrs = cell::read_cell_pointers(&page_data, &header, header_offset)?;
        ptrs.remove(delete_idx);

        #[allow(clippy::cast_possible_truncation)]
        {
            header.cell_count = ptrs.len() as u16;
        }
        header.cell_content_offset = if let Some(min_ptr) = ptrs.iter().copied().min() {
            u32::from(min_ptr)
        } else {
            self.usable_size
        };
        header.write(&mut page_data, header_offset);
        cell::write_cell_pointers(&mut page_data, header_offset, &header, &ptrs);
        self.pager.write_page(cx, leaf_page_no, &page_data)?;

        // Refresh the stack entry.
        let mut refreshed = self.load_page(cx, leaf_page_no)?;
        let new_count = refreshed.header.cell_count;
        if new_count == 0 {
            refreshed.cell_idx = 0;
            self.at_eof = true;
        } else if delete_idx >= usize::from(new_count) {
            refreshed.cell_idx = new_count;
            self.at_eof = true;
        } else {
            #[allow(clippy::cast_possible_truncation)]
            {
                refreshed.cell_idx = delete_idx as u16;
            }
            self.at_eof = false;
        }
        self.stack[depth - 1] = refreshed;

        Ok((leaf_page_no, new_count))
    }
}

// ---------------------------------------------------------------------------
// BtreeCursorOps implementation
// ---------------------------------------------------------------------------

impl<P: PageWriter> sealed::Sealed for BtCursor<P> {}

#[allow(clippy::missing_errors_doc)]
impl<P: PageWriter> BtreeCursorOps for BtCursor<P> {
    fn index_move_to(&mut self, cx: &Cx, key: &[u8]) -> Result<SeekResult> {
        self.index_seek(cx, key)
    }

    fn table_move_to(&mut self, cx: &Cx, rowid: i64) -> Result<SeekResult> {
        self.table_seek(cx, rowid)
    }

    fn first(&mut self, cx: &Cx) -> Result<bool> {
        self.stack.clear();
        self.at_eof = true;
        self.move_to_leftmost_leaf(cx, self.root_page, true)
    }

    fn last(&mut self, cx: &Cx) -> Result<bool> {
        self.stack.clear();
        self.at_eof = true;
        self.move_to_rightmost_leaf(cx, self.root_page, true)
    }

    fn next(&mut self, cx: &Cx) -> Result<bool> {
        self.advance_next(cx)
    }

    fn prev(&mut self, cx: &Cx) -> Result<bool> {
        self.advance_prev(cx)
    }

    fn index_insert(&mut self, cx: &Cx, key: &[u8]) -> Result<()> {
        let seek = self.index_seek(cx, key)?;
        let mut insert_idx = self
            .stack
            .last()
            .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?
            .cell_idx;
        if seek.is_found() {
            // Index allows duplicate keys; place after the existing one.
            insert_idx = insert_idx.saturating_add(1);
        }

        let (cell_data, overflow_head) = self.encode_index_leaf_cell(cx, key)?;

        match self.try_insert_on_leaf(cx, insert_idx, &cell_data) {
            Ok(true) => Ok(()),
            Ok(false) => {
                // Page full — balance and redistribute.
                self.balance_for_insert(cx, &cell_data, insert_idx)
            }
            Err(error) => {
                if let Some(first) = overflow_head {
                    let _ = self.free_overflow_chain(cx, first);
                }
                Err(error)
            }
        }
    }

    fn table_insert(&mut self, cx: &Cx, rowid: i64, data: &[u8]) -> Result<()> {
        let seek = self.table_seek(cx, rowid)?;
        if seek.is_found() {
            return Err(FrankenError::PrimaryKeyViolation);
        }

        let insert_idx = self
            .stack
            .last()
            .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?
            .cell_idx;

        let (cell_data, overflow_head) = self.encode_table_leaf_cell(cx, rowid, data)?;

        match self.try_insert_on_leaf(cx, insert_idx, &cell_data) {
            Ok(true) => Ok(()),
            Ok(false) => {
                // Page full — balance and redistribute.
                self.balance_for_insert(cx, &cell_data, insert_idx)
            }
            Err(error) => {
                if let Some(first) = overflow_head {
                    let _ = self.free_overflow_chain(cx, first);
                }
                Err(error)
            }
        }
    }

    fn delete(&mut self, cx: &Cx) -> Result<()> {
        if self.at_eof || self.stack.is_empty() {
            return Err(FrankenError::internal("cursor at EOF"));
        }

        let top = self.stack.last().unwrap();
        if !top.header.page_type.is_leaf() {
            return Err(FrankenError::internal("cursor must be on a leaf to delete"));
        }

        // Remove the cell from the leaf. This handles overflow chain
        // cleanup and refreshes the stack entry.
        let (_leaf_page_no, _new_count) = self.remove_cell_from_leaf(cx)?;

        // Rebalancing after delete is not yet implemented for multi-level
        // trees. For single-leaf root trees, no rebalance is needed since
        // the root is allowed to be underfull. For multi-level trees, we
        // skip rebalancing for now — the tree remains valid, just
        // potentially unbalanced. A future pass will add merge support.
        Ok(())
    }

    fn payload(&self, cx: &Cx) -> Result<Vec<u8>> {
        if self.at_eof || self.stack.is_empty() {
            return Err(FrankenError::internal("cursor at EOF"));
        }
        let top = self.stack.last().unwrap();
        let cell = self.parse_cell_at(top, top.cell_idx)?;
        self.read_cell_payload(cx, top, &cell)
    }

    fn rowid(&self, cx: &Cx) -> Result<i64> {
        if self.at_eof || self.stack.is_empty() {
            return Err(FrankenError::internal("cursor at EOF"));
        }
        let top = self.stack.last().unwrap();
        let cell = self.parse_cell_at(top, top.cell_idx)?;
        cell.rowid.ok_or_else(|| {
            // For index cursors, rowid is extracted from the payload.
            // For now, return error if no rowid in cell.
            let _ = cx; // suppress unused warning
            FrankenError::internal("cell has no rowid (index cursor?)")
        })
    }

    fn eof(&self) -> bool {
        self.at_eof
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;
    use fsqlite_types::serial_type::write_varint;
    use std::collections::HashMap;

    /// Simple in-memory page store for testing.
    #[derive(Debug, Clone, Default)]
    struct MemPageStore {
        pages: HashMap<u32, Vec<u8>>,
    }

    impl PageReader for MemPageStore {
        fn read_page(&self, _cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
            self.pages
                .get(&page_no.get())
                .cloned()
                .ok_or_else(|| FrankenError::internal("page not found"))
        }
    }

    impl PageWriter for MemPageStore {
        fn write_page(&mut self, _cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
            self.pages.insert(page_no.get(), data.to_vec());
            Ok(())
        }

        fn allocate_page(&mut self, _cx: &Cx) -> Result<PageNumber> {
            let next = self
                .pages
                .keys()
                .copied()
                .max()
                .unwrap_or(1)
                .saturating_add(1);
            let pgno = PageNumber::new(next).ok_or(FrankenError::DatabaseFull)?;
            self.pages.insert(next, vec![0u8; USABLE as usize]);
            Ok(pgno)
        }

        fn free_page(&mut self, _cx: &Cx, page_no: PageNumber) -> Result<()> {
            self.pages.remove(&page_no.get());
            Ok(())
        }
    }

    const USABLE: u32 = 4096;

    /// Helper: build a leaf table page with sorted (rowid, payload) entries.
    fn build_leaf_table(entries: &[(i64, &[u8])]) -> Vec<u8> {
        let mut page = vec![0u8; USABLE as usize];
        let header_size = 8usize; // leaf

        // Build cells from the end of the page.
        let mut cell_end = USABLE as usize;
        let mut cell_offsets: Vec<u16> = Vec::new();

        for &(rowid, payload) in entries {
            // Cell: [payload_size varint] [rowid varint] [payload]
            let mut cell = Vec::new();
            let mut vbuf = [0u8; 9];
            let n = write_varint(&mut vbuf, payload.len() as u64);
            cell.extend_from_slice(&vbuf[..n]);
            #[allow(clippy::cast_sign_loss)]
            let n = write_varint(&mut vbuf, rowid as u64);
            cell.extend_from_slice(&vbuf[..n]);
            cell.extend_from_slice(payload);

            cell_end -= cell.len();
            page[cell_end..cell_end + cell.len()].copy_from_slice(&cell);
            cell_offsets.push(cell_end as u16);
        }

        // Write header.
        page[0] = 0x0D; // LeafTable
        page[1..3].copy_from_slice(&0u16.to_be_bytes()); // no freeblock
        #[allow(clippy::cast_possible_truncation)]
        let cell_count = entries.len() as u16;
        page[3..5].copy_from_slice(&cell_count.to_be_bytes());
        #[allow(clippy::cast_possible_truncation)]
        let content_offset = cell_end as u16;
        page[5..7].copy_from_slice(&content_offset.to_be_bytes());
        page[7] = 0; // fragmented bytes

        // Write cell pointer array.
        for (i, &off) in cell_offsets.iter().enumerate() {
            let ptr_offset = header_size + i * 2;
            page[ptr_offset..ptr_offset + 2].copy_from_slice(&off.to_be_bytes());
        }

        page
    }

    /// Helper: build an interior table page.
    ///
    /// `children` is a list of `(left_child, rowid)` pairs plus a final right_child.
    fn build_interior_table(children: &[(PageNumber, i64)], right_child: PageNumber) -> Vec<u8> {
        let mut page = vec![0u8; USABLE as usize];
        let header_size = 12usize; // interior

        let mut cell_end = USABLE as usize;
        let mut cell_offsets: Vec<u16> = Vec::new();

        for &(left_child, rowid) in children {
            // Interior table cell: [left_child: u32 BE] [rowid: varint]
            let mut cell = Vec::new();
            cell.extend_from_slice(&left_child.get().to_be_bytes());
            let mut vbuf = [0u8; 9];
            #[allow(clippy::cast_sign_loss)]
            let n = write_varint(&mut vbuf, rowid as u64);
            cell.extend_from_slice(&vbuf[..n]);

            cell_end -= cell.len();
            page[cell_end..cell_end + cell.len()].copy_from_slice(&cell);
            cell_offsets.push(cell_end as u16);
        }

        // Write header.
        page[0] = 0x05; // InteriorTable
        page[1..3].copy_from_slice(&0u16.to_be_bytes());
        #[allow(clippy::cast_possible_truncation)]
        let cell_count = children.len() as u16;
        page[3..5].copy_from_slice(&cell_count.to_be_bytes());
        #[allow(clippy::cast_possible_truncation)]
        let content_offset = cell_end as u16;
        page[5..7].copy_from_slice(&content_offset.to_be_bytes());
        page[7] = 0;
        page[8..12].copy_from_slice(&right_child.get().to_be_bytes());

        // Write cell pointer array.
        for (i, &off) in cell_offsets.iter().enumerate() {
            let ptr_offset = header_size + i * 2;
            page[ptr_offset..ptr_offset + 2].copy_from_slice(&off.to_be_bytes());
        }

        page
    }

    /// Helper: build a leaf index page with sorted key payloads.
    fn build_leaf_index(entries: &[&[u8]]) -> Vec<u8> {
        let mut page = vec![0u8; USABLE as usize];
        let header_size = 8usize; // leaf

        let mut cell_end = USABLE as usize;
        let mut cell_offsets: Vec<u16> = Vec::new();

        for &key in entries {
            let mut cell = Vec::new();
            let mut vbuf = [0u8; 9];
            let n = write_varint(&mut vbuf, key.len() as u64);
            cell.extend_from_slice(&vbuf[..n]);
            cell.extend_from_slice(key);

            cell_end -= cell.len();
            page[cell_end..cell_end + cell.len()].copy_from_slice(&cell);
            cell_offsets.push(cell_end as u16);
        }

        page[0] = 0x0A; // LeafIndex
        page[1..3].copy_from_slice(&0u16.to_be_bytes());
        #[allow(clippy::cast_possible_truncation)]
        let cell_count = entries.len() as u16;
        page[3..5].copy_from_slice(&cell_count.to_be_bytes());
        #[allow(clippy::cast_possible_truncation)]
        let content_offset = cell_end as u16;
        page[5..7].copy_from_slice(&content_offset.to_be_bytes());
        page[7] = 0;

        for (i, &off) in cell_offsets.iter().enumerate() {
            let ptr_offset = header_size + i * 2;
            page[ptr_offset..ptr_offset + 2].copy_from_slice(&off.to_be_bytes());
        }

        page
    }

    fn pn(n: u32) -> PageNumber {
        PageNumber::new(n).unwrap()
    }

    // -- Single leaf page tests --

    #[test]
    fn test_cursor_first_last_single_leaf() {
        let mut store = MemPageStore::default();
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"alice"), (5, b"bob"), (10, b"charlie")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert_eq!(cursor.payload(&cx).unwrap(), b"alice");

        assert!(cursor.last(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 10);
        assert_eq!(cursor.payload(&cx).unwrap(), b"charlie");
    }

    #[test]
    fn test_cursor_seek_exact() {
        let mut store = MemPageStore::default();
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (5, b"five"), (10, b"ten"), (15, b"fifteen")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        assert!(cursor.table_move_to(&cx, 5).unwrap().is_found());
        assert_eq!(cursor.rowid(&cx).unwrap(), 5);
        assert_eq!(cursor.payload(&cx).unwrap(), b"five");
    }

    #[test]
    fn test_cursor_seek_not_found() {
        let mut store = MemPageStore::default();
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (5, b"five"), (10, b"ten")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        // Seek for 3 — should land on 5 (next in sort order).
        let result = cursor.table_move_to(&cx, 3).unwrap();
        assert!(!result.is_found());
        assert!(!cursor.eof());
        assert_eq!(cursor.rowid(&cx).unwrap(), 5);

        // Seek for 20 — past the end.
        let result = cursor.table_move_to(&cx, 20).unwrap();
        assert!(!result.is_found());
        assert!(cursor.eof());
    }

    #[test]
    fn test_cursor_table_insert_single_leaf() {
        let mut store = MemPageStore::default();
        store
            .pages
            .insert(2, build_leaf_table(&[(1, b"one"), (3, b"three")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        cursor.table_insert(&cx, 2, b"two").unwrap();

        assert!(cursor.table_move_to(&cx, 2).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"two");

        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 3);
        assert!(!cursor.next(&cx).unwrap());
    }

    #[test]
    fn test_cursor_table_insert_duplicate_rowid() {
        let mut store = MemPageStore::default();
        store.pages.insert(2, build_leaf_table(&[(7, b"seven")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        let err = cursor.table_insert(&cx, 7, b"dupe").unwrap_err();
        assert!(matches!(err, FrankenError::PrimaryKeyViolation));
    }

    #[test]
    fn test_cursor_index_insert_single_leaf() {
        let mut store = MemPageStore::default();
        store
            .pages
            .insert(2, build_leaf_index(&[b"apple", b"pear"]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);
        cursor.index_insert(&cx, b"banana").unwrap();

        assert!(cursor.index_move_to(&cx, b"banana").unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"banana");
    }

    #[test]
    fn test_cursor_table_insert_with_overflow_payload() {
        let mut store = MemPageStore::default();
        store.pages.insert(2, build_leaf_table(&[]));
        let payload: Vec<u8> = (0u8..=255).cycle().take(5000).collect();

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        cursor.table_insert(&cx, 42, &payload).unwrap();

        assert!(cursor.table_move_to(&cx, 42).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), payload);
    }

    #[test]
    fn test_cursor_table_insert_triggers_root_split() {
        let mut store = MemPageStore::default();
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        let mut rowid = 1i64;
        let split_rowid = loop {
            let payload = vec![b'Z'; 220];
            cursor.table_insert(&cx, rowid, &payload).unwrap();

            let root_page = cursor.pager.pages.get(&2).unwrap();
            let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
            if root_header.page_type == cell::BtreePageType::InteriorTable {
                break rowid;
            }

            rowid += 1;
            assert!(
                rowid < 1000,
                "table root did not split under sustained inserts"
            );
        };

        let root_page = cursor.pager.pages.get(&2).unwrap();
        let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
        assert_eq!(root_header.page_type, cell::BtreePageType::InteriorTable);

        assert!(cursor.table_move_to(&cx, split_rowid).unwrap().is_found());
    }

    #[test]
    fn test_cursor_index_insert_triggers_root_split() {
        let mut store = MemPageStore::default();
        store.pages.insert(2, build_leaf_index(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);
        let mut idx = 0usize;
        let split_key = loop {
            let key = format!("key-{idx:05}");
            cursor.index_insert(&cx, key.as_bytes()).unwrap();

            let root_page = cursor.pager.pages.get(&2).unwrap();
            let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
            if root_header.page_type == cell::BtreePageType::InteriorIndex {
                break key.into_bytes();
            }

            idx += 1;
            assert!(
                idx < 2000,
                "index root did not split under sustained inserts"
            );
        };

        let root_page = cursor.pager.pages.get(&2).unwrap();
        let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
        assert_eq!(root_header.page_type, cell::BtreePageType::InteriorIndex);

        assert!(cursor.index_move_to(&cx, &split_key).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), split_key);
    }

    #[test]
    fn test_cursor_table_insert_after_root_split() {
        let mut store = MemPageStore::default();
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        let mut rowid = 1i64;
        loop {
            let payload = vec![b'R'; 220];
            cursor.table_insert(&cx, rowid, &payload).unwrap();
            let root_page = cursor.pager.pages.get(&2).unwrap();
            let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
            if root_header.page_type == cell::BtreePageType::InteriorTable {
                break;
            }
            rowid += 1;
            assert!(
                rowid < 1000,
                "table root did not split under sustained inserts"
            );
        }

        // Insert after split to exercise multi-level insert path.
        rowid += 1;
        cursor.table_insert(&cx, rowid, b"after-split").unwrap();

        assert!(cursor.table_move_to(&cx, rowid).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"after-split");
    }

    #[test]
    fn test_cursor_delete_single_leaf() {
        let mut store = MemPageStore::default();
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (2, b"two"), (3, b"three")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        assert!(cursor.table_move_to(&cx, 2).unwrap().is_found());
        cursor.delete(&cx).unwrap();

        let result = cursor.table_move_to(&cx, 2).unwrap();
        assert!(!result.is_found());
        assert!(!cursor.eof());
        assert_eq!(cursor.rowid(&cx).unwrap(), 3);

        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 3);
        assert!(!cursor.next(&cx).unwrap());
    }

    #[test]
    fn test_point_read_uses_cell_witness() {
        let mut store = MemPageStore::default();
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (5, b"five"), (10, b"ten")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        let result = cursor.table_move_to(&cx, 5).unwrap();
        assert!(result.is_found());
        assert_eq!(cursor.witness_keys().len(), 1);
        assert!(matches!(
            cursor.witness_keys()[0],
            WitnessKey::Cell { btree_root, tag }
                if btree_root == pn(2) && tag == BtCursor::<MemPageStore>::cell_tag_from_rowid(5)
        ));
    }

    #[test]
    fn test_descent_pages_not_witnessed() {
        let mut store = MemPageStore::default();
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"a"), (5, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"c"), (15, b"d")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        let result = cursor.table_move_to(&cx, 10).unwrap();
        assert!(result.is_found());
        assert!(
            cursor
                .witness_keys()
                .iter()
                .all(|key| matches!(key, WitnessKey::Cell { .. }))
        );
    }

    #[test]
    fn test_negative_read_uses_cell_witness() {
        let mut store = MemPageStore::default();
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (5, b"five"), (10, b"ten")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        let result = cursor.table_move_to(&cx, 7).unwrap();
        assert!(!result.is_found());
        assert_eq!(cursor.witness_keys().len(), 1);
        assert!(matches!(
            cursor.witness_keys()[0],
            WitnessKey::Cell { btree_root, tag }
                if btree_root == pn(2) && tag == BtCursor::<MemPageStore>::cell_tag_from_rowid(7)
        ));
    }

    #[test]
    fn test_range_scan_uses_page_witness() {
        let mut store = MemPageStore::default();
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"a"), (5, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"c"), (15, b"d")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        assert!(cursor.first(&cx).unwrap());
        assert!(cursor.next(&cx).unwrap());
        assert!(cursor.next(&cx).unwrap());
        assert!(
            cursor
                .witness_keys()
                .iter()
                .any(|key| matches!(key, WitnessKey::Page(_)))
        );
    }

    #[test]
    fn test_page_only_witnesses_collapse_merge() {
        use std::collections::HashSet;

        let root = pn(2);
        let same_leaf = pn(4);

        let txn1_cell = HashSet::from([WitnessKey::Cell {
            btree_root: root,
            tag: BtCursor::<MemPageStore>::cell_tag_from_rowid(10),
        }]);
        let txn2_cell = HashSet::from([WitnessKey::Cell {
            btree_root: root,
            tag: BtCursor::<MemPageStore>::cell_tag_from_rowid(11),
        }]);
        assert!(
            txn1_cell.is_disjoint(&txn2_cell),
            "cell witnesses preserve independent point operations"
        );

        let txn1_page = HashSet::from([WitnessKey::Page(same_leaf)]);
        let txn2_page = HashSet::from([WitnessKey::Page(same_leaf)]);
        assert!(
            !txn1_page.is_disjoint(&txn2_page),
            "page-only witnesses over-approximate and force conflicts"
        );
    }

    #[test]
    fn test_e2e_point_ops_use_cell_witnesses() {
        let mut store = MemPageStore::default();
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 50)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(10, b"a"), (50, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(60, b"c"), (90, b"d")]));

        let cx = Cx::new();
        let mut point_cursor = BtCursor::new(store.clone(), pn(2), USABLE, true);
        let mut range_cursor = BtCursor::new(store, pn(2), USABLE, true);

        // Point workload: one hit and one miss.
        assert!(point_cursor.table_move_to(&cx, 60).unwrap().is_found());
        assert!(!point_cursor.table_move_to(&cx, 61).unwrap().is_found());
        assert!(
            point_cursor
                .witness_keys()
                .iter()
                .all(|key| matches!(key, WitnessKey::Cell { .. })),
            "point operations must not emit page-level witnesses"
        );

        // Range workload: traversal witnesses leaves at page granularity.
        assert!(range_cursor.first(&cx).unwrap());
        assert!(range_cursor.next(&cx).unwrap());
        assert!(range_cursor.next(&cx).unwrap());
        assert!(
            range_cursor
                .witness_keys()
                .iter()
                .any(|key| matches!(key, WitnessKey::Page(_))),
            "range operations may emit page-level witnesses"
        );
    }

    #[test]
    fn test_cursor_next_prev() {
        let mut store = MemPageStore::default();
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"a"), (2, b"b"), (3, b"c"), (4, b"d")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        // Forward.
        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 3);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 4);
        assert!(!cursor.next(&cx).unwrap());
        assert!(cursor.eof());

        // Backward.
        assert!(cursor.last(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 4);
        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 3);
        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);
        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(!cursor.prev(&cx).unwrap());
    }

    #[test]
    fn test_cursor_empty_tree() {
        let mut store = MemPageStore::default();
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        assert!(!cursor.first(&cx).unwrap());
        assert!(cursor.eof());
    }

    // -- Two-level tree tests --

    #[test]
    fn test_cursor_two_level_tree_seek() {
        // Build a two-level tree:
        //   Interior page 2: children=[left=3, rowid=5], right=4
        //   Leaf page 3: (1, "a"), (5, "b")     — rowids <= 5
        //   Leaf page 4: (10, "c"), (15, "d")    — rowids > 5
        //
        // In SQLite intkey trees, the interior cell key is the max rowid
        // in the left subtree.
        let mut store = MemPageStore::default();
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"a"), (5, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"c"), (15, b"d")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        // Seek exact matches.
        assert!(cursor.table_move_to(&cx, 1).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"a");

        assert!(cursor.table_move_to(&cx, 5).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"b");

        assert!(cursor.table_move_to(&cx, 10).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"c");

        assert!(cursor.table_move_to(&cx, 15).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"d");
    }

    #[test]
    fn test_cursor_two_level_tree_traverse() {
        let mut store = MemPageStore::default();
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"a"), (5, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"c"), (15, b"d")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        // Forward traversal: 1, 5, 10, 15.
        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 5);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 10);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 15);
        assert!(!cursor.next(&cx).unwrap());
        assert!(cursor.eof());

        // Backward traversal: 15, 10, 5, 1.
        assert!(cursor.last(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 15);
        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 10);
        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 5);
        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(!cursor.prev(&cx).unwrap());
    }

    // -- Three-level tree test --

    #[test]
    fn test_cursor_three_level_tree() {
        // Build a three-level tree. Interior cell keys are the max rowid
        // in their left subtree.
        //
        //   Root (page 2): interior, children=[(3, 15)], right=4
        //     → left subtree (page 3) has rowids <= 15
        //     → right subtree (page 4) has rowids > 15
        //
        //   Page 3: interior, children=[(5, 3), (6, 8)], right=7
        //     → page 5 has rowids <= 3
        //     → page 6 has rowids in (3, 8]
        //     → page 7 has rowids in (8, 15]
        //
        //   Page 4: interior, children=[(8, 25)], right=9
        //     → page 8 has rowids in (15, 25]
        //     → page 9 has rowids > 25
        //
        //   Leaf pages:
        //     5: (1, 3)  6: (5, 8)  7: (10, 15)  8: (20, 25)  9: (30, 40)
        let mut store = MemPageStore::default();

        // Root.
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 15)], pn(4)));

        // Interior pages.
        store
            .pages
            .insert(3, build_interior_table(&[(pn(5), 3), (pn(6), 8)], pn(7)));
        store
            .pages
            .insert(4, build_interior_table(&[(pn(8), 25)], pn(9)));

        // Leaves.
        store
            .pages
            .insert(5, build_leaf_table(&[(1, b"L1"), (3, b"L3")]));
        store
            .pages
            .insert(6, build_leaf_table(&[(5, b"L5"), (8, b"L8")]));
        store
            .pages
            .insert(7, build_leaf_table(&[(10, b"L10"), (15, b"L15")]));
        store
            .pages
            .insert(8, build_leaf_table(&[(20, b"L20"), (25, b"L25")]));
        store
            .pages
            .insert(9, build_leaf_table(&[(30, b"L30"), (40, b"L40")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        // Full forward scan.
        let mut rowids = Vec::new();
        assert!(cursor.first(&cx).unwrap());
        loop {
            rowids.push(cursor.rowid(&cx).unwrap());
            if !cursor.next(&cx).unwrap() {
                break;
            }
        }
        assert_eq!(rowids, vec![1, 3, 5, 8, 10, 15, 20, 25, 30, 40]);

        // Full backward scan.
        let mut rowids_rev = Vec::new();
        assert!(cursor.last(&cx).unwrap());
        loop {
            rowids_rev.push(cursor.rowid(&cx).unwrap());
            if !cursor.prev(&cx).unwrap() {
                break;
            }
        }
        assert_eq!(rowids_rev, vec![40, 30, 25, 20, 15, 10, 8, 5, 3, 1]);

        // Seek tests.
        assert!(cursor.table_move_to(&cx, 8).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"L8");

        assert!(cursor.table_move_to(&cx, 25).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"L25");

        // Seek not found: 12 → should land on 15.
        let r = cursor.table_move_to(&cx, 12).unwrap();
        assert!(!r.is_found());
        assert_eq!(cursor.rowid(&cx).unwrap(), 15);
    }

    #[test]
    fn test_cursor_seek_then_next() {
        let mut store = MemPageStore::default();
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"one"), (5, b"five")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"ten"), (20, b"twenty")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        // Seek to 5, then next should give 10.
        assert!(cursor.table_move_to(&cx, 5).unwrap().is_found());
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 10);
    }

    #[test]
    fn test_cursor_eof_at_payload() {
        let mut store = MemPageStore::default();
        store.pages.insert(2, build_leaf_table(&[(1, b"x")]));

        let cx = Cx::new();
        let cursor = BtCursor::new(store, pn(2), USABLE, true);

        // Before first/last, cursor is at EOF.
        assert!(cursor.eof());
        assert!(cursor.payload(&cx).is_err());
        assert!(cursor.rowid(&cx).is_err());
    }
}
