//! Transaction lifecycle (§5.4): Begin/Read/Write/Commit/Abort.
//!
//! This module implements the full transaction lifecycle for both
//! Serialized and Concurrent modes.  It provides:
//!
//! - [`BeginKind`]: The four modes (Deferred, Immediate, Exclusive, Concurrent).
//! - [`TransactionManager`]: Orchestrates begin/read/write/commit/abort.
//! - [`Savepoint`]: B-tree-level page state snapshots within a transaction.
//! - [`CommitResponse`]: Result type for the commit sequencer.

use std::collections::HashMap;

use fsqlite_types::{
    CommitSeq, PageData, PageNumber, PageSize, PageVersion, SchemaEpoch, Snapshot, TxnEpoch,
    TxnToken,
};

use crate::core_types::{
    CommitIndex, InProcessPageLockTable, Transaction, TransactionMode, TransactionState,
};
use crate::invariants::{SerializedWriteMutex, TxnManager, VersionStore};

// ---------------------------------------------------------------------------
// BeginKind
// ---------------------------------------------------------------------------

/// Transaction begin mode (maps to SQLite's BEGIN variants).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BeginKind {
    /// Deferred: snapshot established lazily on first read/write.
    Deferred,
    /// Immediate: acquire serialized writer exclusion at BEGIN.
    Immediate,
    /// Exclusive: acquire serialized writer exclusion at BEGIN (same as
    /// Immediate for our in-process model; cross-process differences in §5.6).
    Exclusive,
    /// Concurrent: MVCC page-level locking (no global mutex).
    Concurrent,
}

// ---------------------------------------------------------------------------
// CommitResponse
// ---------------------------------------------------------------------------

/// Result from the write coordinator / commit sequencer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitResponse {
    /// Successfully committed with the given `CommitSeq`.
    Ok(CommitSeq),
    /// Conflict detected on the given pages; provides the authoritative seq
    /// for merge-retry.
    Conflict(Vec<PageNumber>, CommitSeq),
    /// Aborted by the coordinator with an error code.
    Aborted(MvccError),
    /// I/O error during persist.
    IoError,
}

// ---------------------------------------------------------------------------
// MvccError
// ---------------------------------------------------------------------------

/// Error codes for MVCC operations (modeled after SQLite result codes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MvccError {
    /// Another transaction holds a conflicting lock (SQLITE_BUSY).
    Busy,
    /// Snapshot is stale due to concurrent commit (SQLITE_BUSY_SNAPSHOT).
    BusySnapshot,
    /// Schema changed since transaction started (SQLITE_SCHEMA).
    Schema,
    /// I/O error (SQLITE_IOERR).
    IoErr,
    /// Transaction not in expected state.
    InvalidState,
    /// TxnId space exhausted.
    TxnIdExhausted,
}

impl std::fmt::Display for MvccError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => write!(f, "SQLITE_BUSY"),
            Self::BusySnapshot => write!(f, "SQLITE_BUSY_SNAPSHOT"),
            Self::Schema => write!(f, "SQLITE_SCHEMA"),
            Self::IoErr => write!(f, "SQLITE_IOERR"),
            Self::InvalidState => write!(f, "invalid transaction state"),
            Self::TxnIdExhausted => write!(f, "TxnId space exhausted"),
        }
    }
}

impl std::error::Error for MvccError {}

// ---------------------------------------------------------------------------
// Savepoint
// ---------------------------------------------------------------------------

/// A savepoint records the state of a transaction's write set so it can be
/// partially rolled back.
///
/// Per spec §5.4: savepoints are a B-tree-level mechanism, NOT MVCC-level.
/// Page locks are NOT released on `ROLLBACK TO`. SSI witnesses are NOT
/// rolled back (safe overapproximation).
#[derive(Debug)]
pub struct Savepoint {
    /// Name of the savepoint.
    pub name: String,
    /// Snapshot of the write set at the time the savepoint was created.
    /// Maps page -> data so we can restore on ROLLBACK TO.
    pub write_set_snapshot: HashMap<PageNumber, PageData>,
    /// Number of pages in write_set when savepoint was created.
    pub write_set_len: usize,
}

// ---------------------------------------------------------------------------
// TransactionManager
// ---------------------------------------------------------------------------

/// Orchestrates the full transaction lifecycle.
///
/// Owns all shared MVCC infrastructure and provides begin/read/write/commit/abort
/// operations for both Serialized and Concurrent modes.
pub struct TransactionManager {
    txn_manager: TxnManager,
    version_store: VersionStore,
    lock_table: InProcessPageLockTable,
    write_mutex: SerializedWriteMutex,
    commit_index: CommitIndex,
    /// Current schema epoch (simplified; in full impl this lives in SHM).
    schema_epoch: SchemaEpoch,
}

impl TransactionManager {
    /// Create a new transaction manager.
    #[must_use]
    pub fn new(page_size: PageSize) -> Self {
        Self {
            txn_manager: TxnManager::default(),
            version_store: VersionStore::new(page_size),
            lock_table: InProcessPageLockTable::new(),
            write_mutex: SerializedWriteMutex::new(),
            commit_index: CommitIndex::new(),
            schema_epoch: SchemaEpoch::ZERO,
        }
    }

    /// Begin a new transaction.
    ///
    /// # Errors
    ///
    /// Returns `MvccError::TxnIdExhausted` if the TxnId space is exhausted.
    /// Returns `MvccError::Busy` if the serialized writer mutex cannot be acquired
    /// (for Immediate/Exclusive modes).
    pub fn begin(&self, kind: BeginKind) -> Result<Transaction, MvccError> {
        // TxnId allocation via CAS (never fetch_add, never 0, never > MAX).
        let txn_id = self
            .txn_manager
            .alloc_txn_id()
            .ok_or(MvccError::TxnIdExhausted)?;

        let mode = if kind == BeginKind::Concurrent {
            TransactionMode::Concurrent
        } else {
            TransactionMode::Serialized
        };

        let snapshot = self.load_consistent_snapshot();
        let snapshot_established = kind != BeginKind::Deferred;

        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snapshot, mode);
        txn.snapshot_established = snapshot_established;

        // For Immediate/Exclusive: acquire serialized writer exclusion at BEGIN.
        if kind == BeginKind::Immediate || kind == BeginKind::Exclusive {
            self.write_mutex
                .try_acquire(txn_id)
                .map_err(|_| MvccError::Busy)?;
            txn.serialized_write_lock_held = true;
        }

        tracing::info!(
            txn_id = %txn_id,
            ?kind,
            ?mode,
            snapshot_high = snapshot.high.get(),
            snapshot_established,
            "transaction begun"
        );

        Ok(txn)
    }

    /// Read a page within a transaction.
    ///
    /// Per spec §5.4:
    /// 1. Check write_set first (returns local modification).
    /// 2. For DEFERRED mode: establish snapshot on first read.
    /// 3. Resolve via version store.
    ///
    /// Returns `None` if the page has no committed version visible at the
    /// transaction's snapshot (and is not in the write_set).
    pub fn read_page(&self, txn: &mut Transaction, pgno: PageNumber) -> Option<PageData> {
        assert_eq!(
            txn.state,
            TransactionState::Active,
            "can only read in active transactions"
        );

        // Check write_set first.
        if let Some(data) = txn.write_set_data.get(&pgno) {
            return Some(data.clone());
        }

        // DEFERRED snapshot establishment on first read.
        if txn.mode == TransactionMode::Serialized && !txn.snapshot_established {
            txn.snapshot = self.load_consistent_snapshot();
            txn.snapshot_established = true;
            tracing::debug!(
                txn_id = %txn.txn_id,
                snapshot_high = txn.snapshot.high.get(),
                "deferred snapshot established on read"
            );
        }

        // Resolve from version store.
        let idx = self.version_store.resolve(pgno, &txn.snapshot)?;
        let version = self.version_store.get_version(idx)?;
        Some(version.data)
    }

    /// Write a page within a transaction.
    ///
    /// Per spec §5.4:
    /// - **Serialized**: DEFERRED upgrade acquires global mutex on first write.
    ///   Reader-turned-writer with stale snapshot gets `BusySnapshot`.
    ///   No page lock needed (mutex provides exclusion).
    /// - **Concurrent**: Check serialized writer exclusion, acquire page lock,
    ///   track in page_locks + write_set.
    ///
    /// # Errors
    ///
    /// Returns `MvccError::BusySnapshot` if a serialized deferred txn has a stale
    /// snapshot when upgrading to writer.
    /// Returns `MvccError::Schema` if schema epoch changed.
    /// Returns `MvccError::Busy` if a concurrent write conflicts on page lock or
    /// a serialized writer is active.
    pub fn write_page(
        &self,
        txn: &mut Transaction,
        pgno: PageNumber,
        data: PageData,
    ) -> Result<(), MvccError> {
        assert_eq!(
            txn.state,
            TransactionState::Active,
            "can only write in active transactions"
        );

        if txn.mode == TransactionMode::Serialized {
            self.write_page_serialized(txn, pgno, data)?;
        } else {
            self.write_page_concurrent(txn, pgno, data)?;
        }
        Ok(())
    }

    /// Commit a transaction.
    ///
    /// Per spec §5.4:
    /// - Schema epoch check.
    /// - **Serialized**: FCW freshness validation; abort on snapshot conflict.
    /// - **Concurrent**: SSI validation (simplified), FCW check, merge-retry loop.
    ///
    /// # Errors
    ///
    /// Returns `MvccError::Schema` if schema epoch changed.
    /// Returns `MvccError::BusySnapshot` on FCW conflict.
    /// Returns `MvccError::InvalidState` if not active.
    pub fn commit(&self, txn: &mut Transaction) -> Result<CommitSeq, MvccError> {
        if txn.state != TransactionState::Active {
            return Err(MvccError::InvalidState);
        }

        // Schema epoch check.
        if self.schema_epoch != txn.snapshot.schema_epoch {
            self.abort(txn);
            return Err(MvccError::Schema);
        }

        // If no writes, just commit (read-only transaction).
        if txn.write_set.is_empty() && txn.write_set_data.is_empty() {
            txn.commit();
            self.release_all_resources(txn);
            return Ok(CommitSeq::ZERO);
        }

        if txn.mode == TransactionMode::Serialized {
            self.commit_serialized(txn)
        } else {
            self.commit_concurrent(txn)
        }
    }

    /// Abort a transaction, releasing all held resources.
    ///
    /// Per spec §5.4:
    /// - Release page locks.
    /// - Discard write_set.
    /// - Serialized: release mutex if held.
    /// - Concurrent: SSI witnesses preserved (safe overapproximation).
    pub fn abort(&self, txn: &mut Transaction) {
        if txn.state != TransactionState::Active {
            return; // already finalized
        }
        txn.abort();
        self.release_all_resources(txn);

        tracing::info!(
            txn_id = %txn.txn_id,
            ?txn.mode,
            "transaction aborted"
        );
    }

    /// Create a savepoint within a transaction.
    ///
    /// Records the current write_set state so it can be restored on
    /// `ROLLBACK TO`.
    #[must_use]
    pub fn savepoint(txn: &Transaction, name: &str) -> Savepoint {
        assert_eq!(
            txn.state,
            TransactionState::Active,
            "can only create savepoints in active transactions"
        );
        Savepoint {
            name: name.to_owned(),
            write_set_snapshot: txn.write_set_data.clone(),
            write_set_len: txn.write_set.len(),
        }
    }

    /// Rollback to a savepoint.
    ///
    /// Per spec §5.4:
    /// - Restores write_set page states to the savepoint.
    /// - Page locks are NOT released.
    /// - SSI witnesses are NOT rolled back.
    pub fn rollback_to_savepoint(txn: &mut Transaction, savepoint: &Savepoint) {
        assert_eq!(
            txn.state,
            TransactionState::Active,
            "can only rollback to savepoint in active transactions"
        );

        // Restore write_set_data to the savepoint state.
        txn.write_set_data.clone_from(&savepoint.write_set_snapshot);

        // Truncate the write_set page list to savepoint length.
        txn.write_set.truncate(savepoint.write_set_len);

        tracing::debug!(
            txn_id = %txn.txn_id,
            savepoint = %savepoint.name,
            "rolled back to savepoint"
        );

        // NOTE: page_locks are NOT released (spec requirement).
        // NOTE: read_keys/write_keys (SSI witnesses) are NOT rolled back.
    }

    /// Get a reference to the version store.
    #[must_use]
    pub fn version_store(&self) -> &VersionStore {
        &self.version_store
    }

    /// Get a reference to the commit index.
    #[must_use]
    pub fn commit_index(&self) -> &CommitIndex {
        &self.commit_index
    }

    /// Get a reference to the lock table.
    #[must_use]
    pub fn lock_table(&self) -> &InProcessPageLockTable {
        &self.lock_table
    }

    /// Get a reference to the serialized write mutex.
    #[must_use]
    pub fn write_mutex(&self) -> &SerializedWriteMutex {
        &self.write_mutex
    }

    /// Advance the schema epoch (simulating a DDL commit).
    pub fn advance_schema_epoch(&mut self) {
        self.schema_epoch = SchemaEpoch::new(self.schema_epoch.get() + 1);
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Simplified consistent snapshot load (in-process; no seqlock needed).
    ///
    /// Derives the latest committed seq from the TxnManager's counter
    /// (the counter holds the *next* seq to allocate, so latest = counter - 1).
    fn load_consistent_snapshot(&self) -> Snapshot {
        let counter = self.txn_manager.current_commit_counter();
        let high = CommitSeq::new(counter.saturating_sub(1));
        Snapshot::new(high, self.schema_epoch)
    }

    /// Serialized write path.
    fn write_page_serialized(
        &self,
        txn: &mut Transaction,
        pgno: PageNumber,
        data: PageData,
    ) -> Result<(), MvccError> {
        if !txn.serialized_write_lock_held {
            // DEFERRED upgrade: acquire global mutex on first write.
            self.write_mutex
                .try_acquire(txn.txn_id)
                .map_err(|_| MvccError::Busy)?;

            // Reader-turned-writer rule: if snapshot was already established
            // (via prior reads) and the database has advanced, fail.
            let snap_now = self.load_consistent_snapshot();
            if txn.snapshot_established {
                if snap_now.schema_epoch != txn.snapshot.schema_epoch {
                    self.write_mutex.release(txn.txn_id);
                    return Err(MvccError::Schema);
                }
                if snap_now.high != txn.snapshot.high {
                    self.write_mutex.release(txn.txn_id);
                    return Err(MvccError::BusySnapshot);
                }
            }

            // Establish/refresh snapshot.
            txn.snapshot = snap_now;
            txn.snapshot_established = true;
            txn.serialized_write_lock_held = true;

            tracing::debug!(
                txn_id = %txn.txn_id,
                "serialized deferred upgrade: mutex acquired"
            );
        }

        // No page lock needed (mutex provides exclusion).
        // Track in write_set.
        if !txn.write_set.contains(&pgno) {
            txn.write_set.push(pgno);
        }
        txn.write_set_data.insert(pgno, data);
        Ok(())
    }

    /// Concurrent write path.
    fn write_page_concurrent(
        &self,
        txn: &mut Transaction,
        pgno: PageNumber,
        data: PageData,
    ) -> Result<(), MvccError> {
        // Check serialized writer exclusion first.
        if self.write_mutex.holder().is_some() {
            return Err(MvccError::Busy);
        }

        // Acquire page lock.
        self.lock_table
            .try_acquire(pgno, txn.txn_id)
            .map_err(|_| MvccError::Busy)?;

        let newly_locked = txn.page_locks.insert(pgno);
        if newly_locked {
            tracing::debug!(
                txn_id = %txn.txn_id,
                pgno = pgno.get(),
                "concurrent: page lock acquired"
            );
        }

        // Track in write_set.
        if !txn.write_set.contains(&pgno) {
            txn.write_set.push(pgno);
        }
        txn.write_set_data.insert(pgno, data);
        Ok(())
    }

    /// Serialized commit path.
    fn commit_serialized(&self, txn: &mut Transaction) -> Result<CommitSeq, MvccError> {
        // FCW freshness validation: check that no page in write_set has been
        // committed since our snapshot.
        for &pgno in &txn.write_set {
            if let Some(latest) = self.commit_index.latest(pgno) {
                if latest > txn.snapshot.high {
                    self.abort(txn);
                    return Err(MvccError::BusySnapshot);
                }
            }
        }

        // Publish: allocate commit_seq and publish versions.
        let commit_seq = self.publish_write_set(txn);
        txn.commit();
        self.release_all_resources(txn);

        tracing::info!(
            txn_id = %txn.txn_id,
            commit_seq = commit_seq.get(),
            "serialized commit succeeded"
        );

        Ok(commit_seq)
    }

    /// Concurrent commit path.
    fn commit_concurrent(&self, txn: &mut Transaction) -> Result<CommitSeq, MvccError> {
        // SSI validation (simplified): if dangerous structure, abort.
        if txn.has_dangerous_structure() {
            self.abort(txn);
            return Err(MvccError::BusySnapshot);
        }

        // FCW freshness validation: abort on first conflict.
        // Merge-retry is a separate bead (bd-zppf).
        for &pgno in &txn.write_set {
            if let Some(latest) = self.commit_index.latest(pgno) {
                if latest > txn.snapshot.high {
                    tracing::warn!(
                        txn_id = %txn.txn_id,
                        pgno = pgno.get(),
                        latest_seq = latest.get(),
                        snapshot_high = txn.snapshot.high.get(),
                        "FCW conflict detected in concurrent commit"
                    );
                    self.abort(txn);
                    return Err(MvccError::BusySnapshot);
                }
            }
        }

        // Publish.
        let commit_seq = self.publish_write_set(txn);
        txn.commit();
        self.release_all_resources(txn);

        tracing::info!(
            txn_id = %txn.txn_id,
            commit_seq = commit_seq.get(),
            "concurrent commit succeeded"
        );

        Ok(commit_seq)
    }

    /// Publish a transaction's write set into the version store and commit index.
    ///
    /// Returns the assigned `CommitSeq`.
    fn publish_write_set(&self, txn: &Transaction) -> CommitSeq {
        let commit_seq = self.txn_manager.alloc_commit_seq();

        for &pgno in &txn.write_set {
            if let Some(data) = txn.write_set_data.get(&pgno) {
                // Look up existing chain head for prev pointer.
                let prev = self
                    .version_store
                    .chain_head(pgno)
                    .map(crate::invariants::idx_to_version_pointer);

                let version = PageVersion {
                    pgno,
                    commit_seq,
                    created_by: TxnToken::new(txn.txn_id, txn.txn_epoch),
                    data: data.clone(),
                    prev,
                };
                self.version_store.publish(version);
                self.commit_index.update(pgno, commit_seq);
            }
        }

        commit_seq
    }

    /// Release all resources held by a transaction.
    fn release_all_resources(&self, txn: &mut Transaction) {
        // Release page locks.
        self.lock_table.release_all(txn.txn_id);

        // Release serialized write mutex if held.
        if txn.serialized_write_lock_held {
            self.write_mutex.release(txn.txn_id);
            txn.serialized_write_lock_held = false;
        }
    }
}

impl std::fmt::Debug for TransactionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionManager")
            .field("schema_epoch", &self.schema_epoch)
            .field(
                "current_commit_counter",
                &self.txn_manager.current_commit_counter(),
            )
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_types::TxnId;

    fn mgr() -> TransactionManager {
        TransactionManager::new(PageSize::DEFAULT)
    }

    fn test_data(byte: u8) -> PageData {
        let mut data = PageData::zeroed(PageSize::DEFAULT);
        data.as_bytes_mut()[0] = byte;
        data
    }

    // -----------------------------------------------------------------------
    // BEGIN tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_begin_allocates_txn_id_cas() {
        let m = mgr();
        let mut ids = Vec::new();

        for _ in 0..100 {
            let txn = m.begin(BeginKind::Deferred).unwrap();
            let raw = txn.txn_id.get();
            assert_ne!(raw, 0, "TxnId must never be zero");
            assert!(raw <= TxnId::MAX_RAW, "TxnId must fit in 62 bits");
            ids.push(raw);
        }

        // All unique.
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len(), "all TxnIds must be unique");

        // Monotonic.
        for window in ids.windows(2) {
            assert!(window[0] < window[1], "TxnIds must be strictly increasing");
        }
    }

    #[test]
    fn test_begin_deferred_no_snapshot_until_first_read() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Deferred).unwrap();

        assert!(
            !txn.snapshot_established,
            "deferred should not establish snapshot at BEGIN"
        );
        assert_eq!(txn.mode, TransactionMode::Serialized);

        // Read a page — this should establish the snapshot.
        let _ = m.read_page(&mut txn, PageNumber::new(1).unwrap());
        assert!(
            txn.snapshot_established,
            "snapshot should be established after first read"
        );
    }

    #[test]
    fn test_begin_immediate_acquires_exclusion() {
        let m = mgr();
        let txn1 = m.begin(BeginKind::Immediate).unwrap();
        assert!(
            txn1.serialized_write_lock_held,
            "IMMEDIATE should acquire mutex at BEGIN"
        );
        assert_eq!(m.write_mutex().holder(), Some(txn1.txn_id));

        // Second IMMEDIATE should fail (mutex held).
        let result = m.begin(BeginKind::Immediate);
        assert_eq!(result.unwrap_err(), MvccError::Busy);
    }

    #[test]
    fn test_begin_exclusive_acquires_exclusion() {
        let m = mgr();
        let txn = m.begin(BeginKind::Exclusive).unwrap();
        assert!(txn.serialized_write_lock_held);
        assert_eq!(m.write_mutex().holder(), Some(txn.txn_id));
    }

    #[test]
    fn test_begin_concurrent_no_exclusion() {
        let m = mgr();
        let txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let txn2 = m.begin(BeginKind::Concurrent).unwrap();

        assert_eq!(txn1.mode, TransactionMode::Concurrent);
        assert_eq!(txn2.mode, TransactionMode::Concurrent);
        assert!(!txn1.serialized_write_lock_held);
        assert!(!txn2.serialized_write_lock_held);
        assert!(m.write_mutex().holder().is_none());
    }

    // -----------------------------------------------------------------------
    // READ tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_checks_write_set_first() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let pgno = PageNumber::new(1).unwrap();
        let data = test_data(0xAB);

        m.write_page(&mut txn, pgno, data).unwrap();

        // Read should return write_set version, not version store.
        let read_data = m.read_page(&mut txn, pgno).unwrap();
        assert_eq!(read_data.as_bytes()[0], 0xAB);
    }

    #[test]
    fn test_read_establishes_deferred_snapshot() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Deferred).unwrap();

        assert!(!txn.snapshot_established);

        // Read any page — triggers snapshot establishment.
        let _ = m.read_page(&mut txn, PageNumber::new(1).unwrap());
        assert!(txn.snapshot_established);

        // Snapshot should stay the same on subsequent reads.
        let snap_high = txn.snapshot.high;
        let _ = m.read_page(&mut txn, PageNumber::new(2).unwrap());
        assert_eq!(
            txn.snapshot.high, snap_high,
            "snapshot must not change after establishment"
        );
    }

    #[test]
    fn test_read_visibility_correct() {
        let m = mgr();

        // Commit some data via txn1.
        let mut txn1 = m.begin(BeginKind::Immediate).unwrap();
        let pgno = PageNumber::new(1).unwrap();
        m.write_page(&mut txn1, pgno, test_data(0x01)).unwrap();
        let seq = m.commit(&mut txn1).unwrap();
        assert!(seq.get() > 0);

        // New transaction should see the committed data.
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();
        let data = m.read_page(&mut txn2, pgno);
        assert!(data.is_some(), "committed data should be visible");
        assert_eq!(data.unwrap().as_bytes()[0], 0x01);
    }

    // -----------------------------------------------------------------------
    // WRITE (Serialized) tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_serialized_deferred_upgrade() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Deferred).unwrap();

        assert!(
            !txn.serialized_write_lock_held,
            "DEFERRED: no mutex at BEGIN"
        );

        // First write triggers deferred upgrade.
        let pgno = PageNumber::new(1).unwrap();
        m.write_page(&mut txn, pgno, test_data(0x01)).unwrap();

        assert!(
            txn.serialized_write_lock_held,
            "mutex should be acquired on first write"
        );
        assert!(
            txn.snapshot_established,
            "snapshot should be established on first write"
        );
    }

    #[test]
    fn test_serialized_stale_snapshot_busy() {
        let m = mgr();

        // First, commit something to advance the database.
        let mut txn_writer = m.begin(BeginKind::Immediate).unwrap();
        let pgno = PageNumber::new(1).unwrap();
        m.write_page(&mut txn_writer, pgno, test_data(0x01))
            .unwrap();
        m.commit(&mut txn_writer).unwrap();

        // Now start a DEFERRED txn and read (establishing snapshot at current seq).
        let mut txn = m.begin(BeginKind::Deferred).unwrap();
        let _ = m.read_page(&mut txn, PageNumber::new(2).unwrap());
        assert!(txn.snapshot_established);

        // Advance the database again with another commit.
        let mut txn_writer2 = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(
            &mut txn_writer2,
            PageNumber::new(3).unwrap(),
            test_data(0x02),
        )
        .unwrap();
        m.commit(&mut txn_writer2).unwrap();

        // Now the deferred txn tries to write — snapshot is stale.
        let result = m.write_page(&mut txn, PageNumber::new(4).unwrap(), test_data(0x03));
        assert_eq!(
            result.unwrap_err(),
            MvccError::BusySnapshot,
            "stale snapshot must return BUSY_SNAPSHOT"
        );
    }

    #[test]
    fn test_serialized_no_page_lock_needed() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let pgno = PageNumber::new(1).unwrap();

        m.write_page(&mut txn, pgno, test_data(0x01)).unwrap();

        // Page lock table should have NO locks (serialized mode uses mutex instead).
        assert_eq!(
            m.lock_table().lock_count(),
            0,
            "serialized mode should not use page locks"
        );
    }

    // -----------------------------------------------------------------------
    // WRITE (Concurrent) tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_concurrent_page_lock_acquisition() {
        let m = mgr();
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();
        let pgno = PageNumber::new(1).unwrap();

        m.write_page(&mut txn1, pgno, test_data(0x01)).unwrap();

        // txn2 trying to write the same page should fail.
        let result = m.write_page(&mut txn2, pgno, test_data(0x02));
        assert_eq!(
            result.unwrap_err(),
            MvccError::Busy,
            "page lock contention should return BUSY"
        );
    }

    #[test]
    fn test_concurrent_page_lock_tracked() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();
        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();

        assert!(txn.page_locks.contains(&p1));
        assert!(txn.page_locks.contains(&p2));
        assert_eq!(txn.page_locks.len(), 2);
    }

    #[test]
    fn test_concurrent_write_set_counter() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();

        for i in 1..=5_u8 {
            let pgno = PageNumber::new(u32::from(i)).unwrap();
            m.write_page(&mut txn, pgno, test_data(i)).unwrap();
        }

        assert_eq!(txn.write_set.len(), 5);
        assert_eq!(txn.write_set_data.len(), 5);

        // Writing the same page again should not increase write_set len.
        m.write_page(&mut txn, PageNumber::new(1).unwrap(), test_data(0xFF))
            .unwrap();
        assert_eq!(
            txn.write_set.len(),
            5,
            "duplicate write should not increase write_set page count"
        );
    }

    #[test]
    fn test_concurrent_checks_serialized_exclusion() {
        let m = mgr();

        // Start a serialized IMMEDIATE txn (holds mutex).
        let txn_ser = m.begin(BeginKind::Immediate).unwrap();
        assert!(txn_ser.serialized_write_lock_held);

        // Concurrent write should be blocked.
        let mut txn_conc = m.begin(BeginKind::Concurrent).unwrap();
        let result = m.write_page(&mut txn_conc, PageNumber::new(1).unwrap(), test_data(0x01));
        assert_eq!(
            result.unwrap_err(),
            MvccError::Busy,
            "concurrent write while serialized writer active should fail"
        );
    }

    // -----------------------------------------------------------------------
    // COMMIT (Serialized) tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_commit_serialized_publishes_and_releases() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let pgno = PageNumber::new(1).unwrap();

        m.write_page(&mut txn, pgno, test_data(0x42)).unwrap();
        let seq = m.commit(&mut txn).unwrap();

        assert!(seq.get() > 0);
        assert_eq!(txn.state, TransactionState::Committed);

        // Mutex should be released.
        assert!(m.write_mutex().holder().is_none());

        // Version store should have the committed version.
        let snap = Snapshot::new(seq, SchemaEpoch::ZERO);
        assert!(m.version_store().resolve(pgno, &snap).is_some());

        // Commit index should be updated.
        assert_eq!(m.commit_index().latest(pgno), Some(seq));
    }

    #[test]
    fn test_commit_serialized_schema_epoch_check() {
        let mut m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let pgno = PageNumber::new(1).unwrap();
        m.write_page(&mut txn, pgno, test_data(0x01)).unwrap();

        // Advance schema epoch (simulate DDL).
        m.advance_schema_epoch();

        // Commit should fail with Schema error.
        let result = m.commit(&mut txn);
        assert_eq!(result.unwrap_err(), MvccError::Schema);
        assert_eq!(txn.state, TransactionState::Aborted);
    }

    #[test]
    fn test_commit_serialized_fcw_freshness() {
        let m = mgr();

        // Commit page 1 via txn1.
        let mut txn1 = m.begin(BeginKind::Immediate).unwrap();
        let pgno = PageNumber::new(1).unwrap();
        m.write_page(&mut txn1, pgno, test_data(0x01)).unwrap();
        m.commit(&mut txn1).unwrap();

        // Start txn2 with old snapshot (snapshot high = 0), write page 1.
        // But txn2 must be serialized and hold the mutex.
        let mut txn2 = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut txn2, pgno, test_data(0x02)).unwrap();

        // txn2's snapshot was captured after txn1 committed, so it should see seq=1.
        // This means FCW won't fire. Let's test with a manually stale snapshot.
        // Manipulate txn2 to have a stale snapshot.
        txn2.snapshot = Snapshot::new(CommitSeq::ZERO, SchemaEpoch::ZERO);

        let result = m.commit(&mut txn2);
        assert_eq!(
            result.unwrap_err(),
            MvccError::BusySnapshot,
            "FCW should reject stale snapshot"
        );
    }

    // -----------------------------------------------------------------------
    // COMMIT (Concurrent) tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_commit_concurrent_ssi_validation() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let pgno = PageNumber::new(1).unwrap();

        m.write_page(&mut txn, pgno, test_data(0x01)).unwrap();

        // Simulate dangerous SSI structure.
        txn.has_in_rw = true;
        txn.has_out_rw = true;

        let result = m.commit(&mut txn);
        assert_eq!(
            result.unwrap_err(),
            MvccError::BusySnapshot,
            "dangerous SSI structure should abort"
        );
        assert_eq!(txn.state, TransactionState::Aborted);
    }

    #[test]
    fn test_commit_concurrent_successful() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();
        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();

        let seq = m.commit(&mut txn).unwrap();
        assert!(seq.get() > 0);
        assert_eq!(txn.state, TransactionState::Committed);

        // Locks should be released.
        assert_eq!(m.lock_table().lock_count(), 0);
    }

    // -----------------------------------------------------------------------
    // ABORT tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_abort_releases_page_locks() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let pgno = PageNumber::new(1).unwrap();

        m.write_page(&mut txn, pgno, test_data(0x01)).unwrap();
        assert_eq!(m.lock_table().lock_count(), 1);

        m.abort(&mut txn);
        assert_eq!(
            m.lock_table().lock_count(),
            0,
            "abort must release all page locks"
        );
    }

    #[test]
    fn test_abort_discards_write_set() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let pgno = PageNumber::new(1).unwrap();

        m.write_page(&mut txn, pgno, test_data(0x01)).unwrap();
        assert!(!txn.write_set.is_empty());

        m.abort(&mut txn);
        assert_eq!(txn.state, TransactionState::Aborted);

        // Data should not be visible to a new transaction.
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();
        assert!(
            m.read_page(&mut txn2, pgno).is_none(),
            "aborted data must not be visible"
        );
    }

    #[test]
    fn test_abort_serialized_releases_mutex() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        assert!(m.write_mutex().holder().is_some());

        m.abort(&mut txn);
        assert!(
            m.write_mutex().holder().is_none(),
            "abort must release serialized write mutex"
        );
    }

    #[test]
    fn test_abort_concurrent_witnesses_preserved() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();

        // Add SSI witness keys.
        txn.read_keys
            .insert(fsqlite_types::WitnessKey::Page(PageNumber::new(1).unwrap()));
        txn.write_keys
            .insert(fsqlite_types::WitnessKey::Page(PageNumber::new(2).unwrap()));

        m.abort(&mut txn);

        // Witnesses are NOT cleared on abort (safe overapproximation per spec).
        assert!(
            !txn.read_keys.is_empty(),
            "SSI read witnesses must be preserved after abort"
        );
        assert!(
            !txn.write_keys.is_empty(),
            "SSI write witnesses must be preserved after abort"
        );
    }

    // -----------------------------------------------------------------------
    // SAVEPOINT tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_savepoint_records_state() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let p1 = PageNumber::new(1).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();

        let sp = TransactionManager::savepoint(&txn, "sp1");
        assert_eq!(sp.name, "sp1");
        assert_eq!(sp.write_set_len, 1);
        assert!(sp.write_set_snapshot.contains_key(&p1));
    }

    #[test]
    fn test_rollback_to_savepoint_restores_pages() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();

        // Write page 1.
        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();

        // Create savepoint.
        let sp = TransactionManager::savepoint(&txn, "sp1");

        // Write page 2 after savepoint.
        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();
        assert_eq!(txn.write_set.len(), 2);

        // Rollback to savepoint — should undo page 2 write.
        TransactionManager::rollback_to_savepoint(&mut txn, &sp);

        assert_eq!(
            txn.write_set.len(),
            1,
            "write_set should be truncated to savepoint state"
        );
        assert!(
            txn.write_set_data.contains_key(&p1),
            "page 1 should still be in write_set_data"
        );
        assert!(
            !txn.write_set_data.contains_key(&p2),
            "page 2 should be removed from write_set_data"
        );
    }

    #[test]
    fn test_rollback_to_savepoint_keeps_page_locks() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();
        let sp = TransactionManager::savepoint(&txn, "sp1");

        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();
        assert_eq!(txn.page_locks.len(), 2);

        TransactionManager::rollback_to_savepoint(&mut txn, &sp);

        // Page locks must NOT be released on ROLLBACK TO (spec requirement).
        assert_eq!(
            txn.page_locks.len(),
            2,
            "page locks must NOT be released on ROLLBACK TO"
        );
        assert!(txn.page_locks.contains(&p1));
        assert!(txn.page_locks.contains(&p2));
    }

    #[test]
    fn test_rollback_to_savepoint_keeps_witnesses() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let p1 = PageNumber::new(1).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();

        // Add SSI witnesses after the savepoint.
        let sp = TransactionManager::savepoint(&txn, "sp1");
        txn.read_keys
            .insert(fsqlite_types::WitnessKey::Page(PageNumber::new(2).unwrap()));
        txn.write_keys
            .insert(fsqlite_types::WitnessKey::Page(PageNumber::new(3).unwrap()));

        TransactionManager::rollback_to_savepoint(&mut txn, &sp);

        // SSI witnesses must NOT be rolled back (spec requirement).
        assert!(
            !txn.read_keys.is_empty(),
            "SSI read witnesses must NOT be rolled back"
        );
        assert!(
            !txn.write_keys.is_empty(),
            "SSI write witnesses must NOT be rolled back"
        );
    }

    #[test]
    fn test_nested_savepoints() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();
        let p3 = PageNumber::new(3).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();
        let sp1 = TransactionManager::savepoint(&txn, "sp1");

        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();
        let sp2 = TransactionManager::savepoint(&txn, "sp2");

        m.write_page(&mut txn, p3, test_data(0x03)).unwrap();
        assert_eq!(txn.write_set.len(), 3);

        // Rollback to sp2 — should undo p3 only.
        TransactionManager::rollback_to_savepoint(&mut txn, &sp2);
        assert_eq!(txn.write_set.len(), 2);
        assert!(txn.write_set_data.contains_key(&p1));
        assert!(txn.write_set_data.contains_key(&p2));
        assert!(!txn.write_set_data.contains_key(&p3));

        // Rollback to sp1 — should undo p2 as well.
        TransactionManager::rollback_to_savepoint(&mut txn, &sp1);
        assert_eq!(txn.write_set.len(), 1);
        assert!(txn.write_set_data.contains_key(&p1));
        assert!(!txn.write_set_data.contains_key(&p2));
    }

    // -----------------------------------------------------------------------
    // State machine tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_state_machine_transitions_irreversible() {
        let m = mgr();

        // Active -> Committed is terminal.
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.commit(&mut txn).unwrap();
        assert_eq!(txn.state, TransactionState::Committed);

        // Active -> Aborted is terminal.
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();
        m.abort(&mut txn2);
        assert_eq!(txn2.state, TransactionState::Aborted);
    }

    #[test]
    #[should_panic(expected = "can only commit active")]
    fn test_committed_cannot_commit() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.commit(&mut txn).unwrap();
        txn.commit(); // should panic
    }

    #[test]
    #[should_panic(expected = "can only abort active")]
    fn test_committed_cannot_abort() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.commit(&mut txn).unwrap();
        txn.abort(); // should panic
    }

    #[test]
    #[should_panic(expected = "can only commit active")]
    fn test_aborted_cannot_commit() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.abort(&mut txn);
        txn.commit(); // should panic
    }

    #[test]
    #[should_panic(expected = "can only abort active")]
    fn test_aborted_cannot_abort() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.abort(&mut txn);
        txn.abort(); // should panic
    }

    // -----------------------------------------------------------------------
    // E2E: full lifecycle
    // -----------------------------------------------------------------------

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_e2e_full_lifecycle_all_modes() {
        let m = mgr();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();
        let p3 = PageNumber::new(3).unwrap();
        let p4 = PageNumber::new(4).unwrap();

        // --- Serialized IMMEDIATE ---
        let mut txn_imm = m.begin(BeginKind::Immediate).unwrap();
        assert!(txn_imm.serialized_write_lock_held);
        m.write_page(&mut txn_imm, p1, test_data(0x11)).unwrap();
        let seq_imm = m.commit(&mut txn_imm).unwrap();
        assert!(seq_imm.get() > 0);

        // --- Serialized EXCLUSIVE ---
        let mut txn_exc = m.begin(BeginKind::Exclusive).unwrap();
        assert!(txn_exc.serialized_write_lock_held);
        m.write_page(&mut txn_exc, p2, test_data(0x22)).unwrap();
        let seq_exc = m.commit(&mut txn_exc).unwrap();
        assert!(seq_exc > seq_imm);

        // --- Serialized DEFERRED (read then write) ---
        let mut txn_def = m.begin(BeginKind::Deferred).unwrap();
        assert!(!txn_def.snapshot_established);

        // Read page 1 — establishes snapshot.
        let data_p1 = m.read_page(&mut txn_def, p1).unwrap();
        assert_eq!(data_p1.as_bytes()[0], 0x11);
        assert!(txn_def.snapshot_established);

        // Write page 3 — triggers deferred upgrade.
        m.write_page(&mut txn_def, p3, test_data(0x33)).unwrap();
        assert!(txn_def.serialized_write_lock_held);
        let seq_def = m.commit(&mut txn_def).unwrap();
        assert!(seq_def > seq_exc);

        // --- Concurrent ---
        let mut txn_conc = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn_conc, p4, test_data(0x44)).unwrap();
        let seq_conc = m.commit(&mut txn_conc).unwrap();
        assert!(seq_conc > seq_def);

        // --- Verify all data visible in a new snapshot ---
        let mut txn_read = m.begin(BeginKind::Concurrent).unwrap();

        let r1 = m.read_page(&mut txn_read, p1).unwrap();
        assert_eq!(r1.as_bytes()[0], 0x11, "page 1 from IMMEDIATE");

        let r2 = m.read_page(&mut txn_read, p2).unwrap();
        assert_eq!(r2.as_bytes()[0], 0x22, "page 2 from EXCLUSIVE");

        let r3 = m.read_page(&mut txn_read, p3).unwrap();
        assert_eq!(r3.as_bytes()[0], 0x33, "page 3 from DEFERRED");

        let r4 = m.read_page(&mut txn_read, p4).unwrap();
        assert_eq!(r4.as_bytes()[0], 0x44, "page 4 from CONCURRENT");

        // --- Verify isolation: old snapshot doesn't see new writes ---
        // (txn_read's snapshot sees everything so far, which is correct)
        // Start a txn before any new writes.
        let mut txn_old = m.begin(BeginKind::Concurrent).unwrap();

        // Write a new version of page 1 via another txn.
        let mut txn_update = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut txn_update, p1, test_data(0xFF)).unwrap();
        m.commit(&mut txn_update).unwrap();

        // txn_old should still see the old version of page 1 (0x11, not 0xFF).
        let r1_old = m.read_page(&mut txn_old, p1).unwrap();
        assert_eq!(
            r1_old.as_bytes()[0],
            0x11,
            "old snapshot should see old version"
        );
    }

    #[test]
    fn test_e2e_concurrent_different_pages_both_commit() {
        let m = mgr();
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();

        // Different pages — no conflict.
        m.write_page(&mut txn1, PageNumber::new(1).unwrap(), test_data(0x01))
            .unwrap();
        m.write_page(&mut txn2, PageNumber::new(2).unwrap(), test_data(0x02))
            .unwrap();

        let seq1 = m.commit(&mut txn1).unwrap();
        let seq2 = m.commit(&mut txn2).unwrap();

        assert!(seq1.get() > 0);
        assert!(seq2.get() > 0);
        assert_eq!(txn1.state, TransactionState::Committed);
        assert_eq!(txn2.state, TransactionState::Committed);
    }

    #[test]
    fn test_e2e_concurrent_same_page_conflict() {
        let m = mgr();
        let pgno = PageNumber::new(1).unwrap();

        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();

        // txn1 writes page 1 first (gets the lock).
        m.write_page(&mut txn1, pgno, test_data(0x01)).unwrap();

        // txn2 cannot write page 1 (lock held by txn1).
        let result = m.write_page(&mut txn2, pgno, test_data(0x02));
        assert_eq!(result.unwrap_err(), MvccError::Busy);

        // txn1 commits successfully.
        let seq = m.commit(&mut txn1).unwrap();
        assert!(seq.get() > 0);
    }
}
