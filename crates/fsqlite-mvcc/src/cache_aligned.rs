//! Cache-line-aware wrappers and shared-memory structures (§1.5, bd-22n.3).
//!
//! This module provides:
//!
//! - [`CacheAligned<T>`]: A transparent wrapper that forces cache-line alignment
//!   and padding, preventing false sharing between adjacent elements in arrays.
//!
//! - [`SharedTxnSlot`]: The 128-byte (2 cache-line) shared-memory transaction
//!   slot structure used for cross-process MVCC coordination (§5.6.2).
//!
//! # Cache-Line Size
//!
//! We assume 64-byte cache lines (standard on x86-64 and AArch64). This is
//! encoded in [`CACHE_LINE_BYTES`].

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};

/// Cache line size in bytes.
///
/// 64 bytes for x86-64 (Intel/AMD) and AArch64 (Apple M-series, Graviton).
/// Over-aligning for platforms with 128-byte lines (some ARM) is safe
/// (wastes a little memory but prevents false sharing on 64-byte platforms).
pub const CACHE_LINE_BYTES: usize = 64;

// ---------------------------------------------------------------------------
// CacheAligned<T>
// ---------------------------------------------------------------------------

/// Wraps a value to ensure it starts on a cache-line boundary.
///
/// When stored in an array, each element occupies a whole number of cache
/// lines, preventing false sharing between adjacent elements accessed by
/// different threads.
///
/// # Layout
///
/// `#[repr(C, align(64))]` guarantees:
/// - The struct starts at a 64-byte-aligned address.
/// - The struct size is rounded up to the next multiple of 64 bytes.
#[repr(C, align(64))]
pub struct CacheAligned<T> {
    value: T,
}

impl<T> CacheAligned<T> {
    /// Wrap `value` with cache-line alignment.
    #[inline]
    #[must_use]
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    /// Unwrap, returning the inner value.
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T: Default> Default for CacheAligned<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> std::ops::Deref for CacheAligned<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> std::ops::DerefMut for CacheAligned<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for CacheAligned<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

// ---------------------------------------------------------------------------
// SharedTxnSlot
// ---------------------------------------------------------------------------

/// Shared-memory transaction slot (§5.6.2).
///
/// Exactly 128 bytes (2 cache lines) with `#[repr(C, align(64))]`. Fields
/// are grouped by access frequency:
///
/// - **First cache line (bytes 0–63):** Hot-path fields read on every MVCC
///   visibility check — `txn_id`, sequence numbers, SSI flags.
///
/// - **Second cache line (bytes 64–127):** Administrative/lifecycle fields —
///   process identity, lease management, cleanup coordination.
///
/// All fields are atomic for lock-free cross-process access via shared memory.
/// A `txn_id` of 0 indicates the slot is free.
#[repr(C, align(64))]
pub struct SharedTxnSlot {
    // === First cache line (offsets 0–63) — hot read path ===
    /// Transaction ID (top 2 bits reserved for sentinel encoding). 0 = free.
    pub txn_id: AtomicU64,
    /// Begin sequence number (snapshot lower bound).
    pub begin_seq: AtomicU64,
    /// Commit sequence number (0 while transaction is active).
    pub commit_seq: AtomicU64,
    /// Snapshot high-water mark.
    pub snapshot_high: AtomicU64,
    /// Number of pages in the write set.
    pub write_set_pages: AtomicU32,
    /// Transaction state (discriminant of `TransactionState`).
    pub state: AtomicU8,
    /// Transaction mode (discriminant of `TransactionMode`).
    pub mode: AtomicU8,
    /// SSI: has incoming rw-antidependency edge.
    pub has_in_rw: AtomicBool,
    /// SSI: has outgoing rw-antidependency edge.
    pub has_out_rw: AtomicBool,
    /// SSI: marked for abort by conflict detector.
    pub marked_for_abort: AtomicBool,
    /// Padding to end of first cache line.
    _pad0: [u8; 23],

    // === Second cache line (offsets 64–127) — admin/cold path ===
    /// Process birth timestamp (epoch nanos, for stale-slot detection).
    pub pid_birth: AtomicU64,
    /// Lease expiry timestamp (epoch nanos).
    pub lease_expiry: AtomicU64,
    /// Timestamp when this slot was claimed (epoch nanos).
    pub claiming_timestamp: AtomicU64,
    /// TxnId of the transaction performing cleanup (0 = none).
    pub cleanup_txn_id: AtomicU64,
    /// Transaction epoch (monotonic, incremented on slot reuse).
    pub txn_epoch: AtomicU32,
    /// Witness-plane epoch for double-buffered SSI tracking.
    pub witness_epoch: AtomicU32,
    /// Process ID of the slot owner.
    pub pid: AtomicU32,
    /// Reserved padding to end of second cache line.
    _pad1: [u8; 20],
}

impl SharedTxnSlot {
    /// Create a new free (unoccupied) slot with all fields zeroed.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            txn_id: AtomicU64::new(0),
            begin_seq: AtomicU64::new(0),
            commit_seq: AtomicU64::new(0),
            snapshot_high: AtomicU64::new(0),
            write_set_pages: AtomicU32::new(0),
            state: AtomicU8::new(0),
            mode: AtomicU8::new(0),
            has_in_rw: AtomicBool::new(false),
            has_out_rw: AtomicBool::new(false),
            marked_for_abort: AtomicBool::new(false),
            _pad0: [0; 23],
            pid_birth: AtomicU64::new(0),
            lease_expiry: AtomicU64::new(0),
            claiming_timestamp: AtomicU64::new(0),
            cleanup_txn_id: AtomicU64::new(0),
            txn_epoch: AtomicU32::new(0),
            witness_epoch: AtomicU32::new(0),
            pid: AtomicU32::new(0),
            _pad1: [0; 20],
        }
    }

    /// Whether this slot is free (`txn_id == 0`).
    #[inline]
    #[must_use]
    pub fn is_free(&self, ordering: Ordering) -> bool {
        self.txn_id.load(ordering) == 0
    }
}

impl Default for SharedTxnSlot {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for SharedTxnSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedTxnSlot")
            .field("txn_id", &self.txn_id.load(Ordering::Relaxed))
            .field("state", &self.state.load(Ordering::Relaxed))
            .field("mode", &self.mode.load(Ordering::Relaxed))
            .field("begin_seq", &self.begin_seq.load(Ordering::Relaxed))
            .field("commit_seq", &self.commit_seq.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{align_of, size_of};
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    // -- CacheAligned tests --

    #[test]
    fn test_cache_aligned_size_is_multiple_of_64() {
        assert_eq!(size_of::<CacheAligned<u8>>(), 64);
        assert_eq!(size_of::<CacheAligned<u64>>(), 64);
        assert_eq!(size_of::<CacheAligned<[u8; 64]>>(), 64);
        assert_eq!(size_of::<CacheAligned<[u8; 65]>>(), 128);
        assert_eq!(size_of::<CacheAligned<[u8; 128]>>(), 128);
        assert_eq!(size_of::<CacheAligned<[u8; 129]>>(), 192);
    }

    #[test]
    fn test_cache_aligned_alignment() {
        assert_eq!(align_of::<CacheAligned<u8>>(), CACHE_LINE_BYTES);
        assert_eq!(align_of::<CacheAligned<AtomicU64>>(), CACHE_LINE_BYTES);
    }

    #[test]
    fn test_cache_aligned_deref() {
        let aligned = CacheAligned::new(42_u64);
        assert_eq!(*aligned, 42);
    }

    #[test]
    fn test_cache_aligned_deref_mut() {
        let mut aligned = CacheAligned::new(0_u64);
        *aligned = 99;
        assert_eq!(*aligned, 99);
    }

    #[test]
    fn test_cache_aligned_into_inner() {
        let aligned = CacheAligned::new(String::from("hello"));
        let s = aligned.into_inner();
        assert_eq!(s, "hello");
    }

    #[test]
    fn test_cache_aligned_default() {
        let aligned: CacheAligned<u64> = CacheAligned::default();
        assert_eq!(*aligned, 0);
    }

    #[test]
    fn test_cache_aligned_debug() {
        let aligned = CacheAligned::new(42_u32);
        let debug = format!("{aligned:?}");
        assert!(debug.contains("42"));
    }

    #[test]
    fn test_cache_aligned_array_no_false_sharing() {
        let arr: [CacheAligned<AtomicU64>; 4] =
            std::array::from_fn(|_| CacheAligned::new(AtomicU64::new(0)));

        for i in 0..3 {
            let a = (&raw const arr[i]) as usize;
            let b = (&raw const arr[i + 1]) as usize;
            assert_eq!(
                b - a,
                CACHE_LINE_BYTES,
                "adjacent CacheAligned elements must be {CACHE_LINE_BYTES} bytes apart"
            );
        }
    }

    // -- SharedTxnSlot tests --

    #[test]
    fn test_txn_slot_128_bytes() {
        assert_eq!(
            size_of::<SharedTxnSlot>(),
            128,
            "SharedTxnSlot must be exactly 128 bytes (2 cache lines)"
        );
    }

    #[test]
    fn test_txn_slot_alignment() {
        assert_eq!(
            align_of::<SharedTxnSlot>(),
            CACHE_LINE_BYTES,
            "SharedTxnSlot must be cache-line aligned"
        );
    }

    #[test]
    fn test_txn_slot_new_is_free() {
        let slot = SharedTxnSlot::new();
        assert!(slot.is_free(Ordering::Relaxed));
        assert_eq!(slot.txn_id.load(Ordering::Relaxed), 0);
        assert_eq!(slot.begin_seq.load(Ordering::Relaxed), 0);
        assert_eq!(slot.commit_seq.load(Ordering::Relaxed), 0);
        assert_eq!(slot.snapshot_high.load(Ordering::Relaxed), 0);
        assert_eq!(slot.write_set_pages.load(Ordering::Relaxed), 0);
        assert_eq!(slot.state.load(Ordering::Relaxed), 0);
        assert_eq!(slot.mode.load(Ordering::Relaxed), 0);
        assert!(!slot.has_in_rw.load(Ordering::Relaxed));
        assert!(!slot.has_out_rw.load(Ordering::Relaxed));
        assert!(!slot.marked_for_abort.load(Ordering::Relaxed));
        assert_eq!(slot.pid_birth.load(Ordering::Relaxed), 0);
        assert_eq!(slot.lease_expiry.load(Ordering::Relaxed), 0);
        assert_eq!(slot.claiming_timestamp.load(Ordering::Relaxed), 0);
        assert_eq!(slot.cleanup_txn_id.load(Ordering::Relaxed), 0);
        assert_eq!(slot.txn_epoch.load(Ordering::Relaxed), 0);
        assert_eq!(slot.witness_epoch.load(Ordering::Relaxed), 0);
        assert_eq!(slot.pid.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_txn_slot_array_no_false_sharing() {
        let slots: [SharedTxnSlot; 4] = std::array::from_fn(|_| SharedTxnSlot::new());

        for i in 0..3 {
            let a = (&raw const slots[i]) as usize;
            let b = (&raw const slots[i + 1]) as usize;
            assert_eq!(
                b - a,
                128,
                "adjacent SharedTxnSlots must be 128 bytes apart"
            );
            assert_eq!(
                a % CACHE_LINE_BYTES,
                0,
                "each slot must be cache-line aligned"
            );
        }
    }

    #[test]
    fn test_txn_slot_field_offsets_cache_line_split() {
        // Verify hot-path fields in first cache line, admin fields in second.
        let slot = SharedTxnSlot::new();
        let base = (&raw const slot) as usize;

        // First cache line: txn_id through marked_for_abort (offsets 0..41)
        let txn_id_off = (&raw const slot.txn_id) as usize - base;
        let begin_seq_off = (&raw const slot.begin_seq) as usize - base;
        let commit_seq_off = (&raw const slot.commit_seq) as usize - base;
        let snapshot_high_off = (&raw const slot.snapshot_high) as usize - base;
        let write_set_pages_off = (&raw const slot.write_set_pages) as usize - base;
        let state_off = (&raw const slot.state) as usize - base;
        let mode_off = (&raw const slot.mode) as usize - base;
        let has_in_rw_off = (&raw const slot.has_in_rw) as usize - base;
        let has_out_rw_off = (&raw const slot.has_out_rw) as usize - base;
        let marked_for_abort_off = (&raw const slot.marked_for_abort) as usize - base;

        assert_eq!(txn_id_off, 0, "txn_id at offset 0");
        assert_eq!(begin_seq_off, 8);
        assert_eq!(commit_seq_off, 16);
        assert_eq!(snapshot_high_off, 24);
        assert_eq!(write_set_pages_off, 32);
        assert_eq!(state_off, 36);
        assert_eq!(mode_off, 37);
        assert_eq!(has_in_rw_off, 38);
        assert_eq!(has_out_rw_off, 39);
        assert_eq!(marked_for_abort_off, 40);

        // All hot-path fields in first 64 bytes
        assert!(
            marked_for_abort_off < 64,
            "marked_for_abort in first cache line"
        );

        // Second cache line: pid_birth starts at offset 64
        let pid_birth_off = (&raw const slot.pid_birth) as usize - base;
        let lease_expiry_off = (&raw const slot.lease_expiry) as usize - base;
        let txn_epoch_off = (&raw const slot.txn_epoch) as usize - base;
        let pid_off = (&raw const slot.pid) as usize - base;

        assert_eq!(pid_birth_off, 64, "pid_birth starts second cache line");
        assert_eq!(lease_expiry_off, 72);
        assert!(txn_epoch_off >= 64, "txn_epoch in second cache line");
        assert!(pid_off >= 64, "pid in second cache line");
    }

    #[test]
    fn test_txn_slot_basic_atomic_ops() {
        let slot = SharedTxnSlot::new();

        // Claim the slot.
        slot.txn_id.store(42, Ordering::Release);
        assert!(!slot.is_free(Ordering::Acquire));
        assert_eq!(slot.txn_id.load(Ordering::Acquire), 42);

        // Set SSI flags.
        slot.has_in_rw.store(true, Ordering::Release);
        slot.has_out_rw.store(true, Ordering::Release);
        assert!(slot.has_in_rw.load(Ordering::Acquire));
        assert!(slot.has_out_rw.load(Ordering::Acquire));

        // Set sequence numbers.
        slot.begin_seq.store(100, Ordering::Release);
        slot.commit_seq.store(105, Ordering::Release);
        slot.snapshot_high.store(99, Ordering::Release);
        assert_eq!(slot.begin_seq.load(Ordering::Acquire), 100);
        assert_eq!(slot.commit_seq.load(Ordering::Acquire), 105);
        assert_eq!(slot.snapshot_high.load(Ordering::Acquire), 99);

        // Release the slot.
        slot.txn_id.store(0, Ordering::Release);
        assert!(slot.is_free(Ordering::Acquire));
    }

    #[test]
    fn test_txn_slot_debug_output() {
        let slot = SharedTxnSlot::new();
        slot.txn_id.store(7, Ordering::Relaxed);
        slot.state.store(1, Ordering::Relaxed);
        let debug = format!("{slot:?}");
        assert!(
            debug.contains("SharedTxnSlot"),
            "debug must contain type name"
        );
        assert!(debug.contains("txn_id: 7"), "debug must show txn_id");
    }

    #[test]
    fn test_txn_slot_default() {
        let slot = SharedTxnSlot::default();
        assert!(slot.is_free(Ordering::Relaxed));
    }

    // -- Hot witness bucket alignment (§5.6.4.5) --

    #[test]
    fn test_hot_witness_buckets_cache_aligned() {
        use crate::witness_hierarchy::HotWitnessIndexSizingV1;

        assert_eq!(
            HotWitnessIndexSizingV1::ENTRY_ALIGNMENT_BYTES as usize,
            CACHE_LINE_BYTES,
            "hot witness bucket entries must be cache-line aligned"
        );
    }

    // -- Lock table / commit index integration --

    #[test]
    fn test_shared_page_lock_table_cache_aligned() {
        use crate::InProcessPageLockTable;

        // Verify the lock table works correctly with CacheAligned shards.
        let table = InProcessPageLockTable::new();
        let page = fsqlite_types::PageNumber::new(1).unwrap();
        let txn = fsqlite_types::TxnId::new(1).unwrap();

        assert!(table.try_acquire(page, txn).is_ok());
        assert_eq!(table.holder(page), Some(txn));
        assert_eq!(table.lock_count(), 1);

        table.release_all(txn);
        assert_eq!(table.lock_count(), 0);
    }

    #[test]
    fn test_commit_index_cache_aligned() {
        use crate::CommitIndex;

        let index = CommitIndex::new();
        let page = fsqlite_types::PageNumber::new(42).unwrap();
        let seq = fsqlite_types::CommitSeq::new(5);

        index.update(page, seq);
        assert_eq!(index.latest(page), Some(seq));
    }

    // -- E2E false sharing regression --

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn test_e2e_shared_memory_false_sharing_regression() {
        const N_THREADS: usize = 4;
        const N_ITERS: u64 = 200_000;

        // --- Padded (cache-line aligned) counters ---
        let padded: Arc<[CacheAligned<AtomicU64>; N_THREADS]> =
            Arc::new(std::array::from_fn(|_| {
                CacheAligned::new(AtomicU64::new(0))
            }));

        let start = Instant::now();
        let handles: Vec<_> = (0..N_THREADS)
            .map(|t| {
                let counters = Arc::clone(&padded);
                thread::spawn(move || {
                    for _ in 0..N_ITERS {
                        counters[t].fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let padded_us = start.elapsed().as_micros();

        // Verify correctness.
        for counter in padded.iter() {
            assert_eq!(counter.load(Ordering::Relaxed), N_ITERS);
        }

        // --- Unpadded (potential false sharing) counters ---
        let unpadded: Arc<[AtomicU64; N_THREADS]> =
            Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));

        let start = Instant::now();
        let handles: Vec<_> = (0..N_THREADS)
            .map(|t| {
                let counters = Arc::clone(&unpadded);
                thread::spawn(move || {
                    for _ in 0..N_ITERS {
                        counters[t].fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let unpadded_us = start.elapsed().as_micros();

        // Verify correctness.
        for counter in unpadded.iter() {
            assert_eq!(counter.load(Ordering::Relaxed), N_ITERS);
        }

        // Log for regression analysis. On typical hardware, the padded
        // variant avoids false sharing and should be equal or faster.
        // A large padded/unpadded ratio (>> 1) would indicate a regression.
        let ratio = if unpadded_us > 0 {
            padded_us as f64 / unpadded_us as f64
        } else {
            1.0
        };
        eprintln!(
            "bead_id=bd-22n.3 case=false_sharing_regression \
             padded_us={padded_us} unpadded_us={unpadded_us} ratio={ratio:.2}"
        );
    }
}
