//! MVCC conflict analytics and observability infrastructure.
//!
//! Provides shared types and utilities for conflict tracing, metrics
//! aggregation, and diagnostic logging across the FrankenSQLite MVCC layer.
//!
//! # Design Principles
//!
//! - **Zero-cost when unused:** All observation is opt-in via the
//!   [`ConflictObserver`] trait. When no observer is registered, conflict
//!   emission compiles to nothing (the default [`NoOpObserver`] is inlined).
//! - **Non-blocking:** Observers MUST NOT acquire page locks or block writers.
//!   Conflict tracing is purely diagnostic.
//! - **Shared foundation:** Types defined here are reused by downstream
//!   observability beads (bd-t6sv2.2, .3, .5, .6, .8, .12).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use fsqlite_types::{CommitSeq, PageNumber, TxnId, TxnToken};
use parking_lot::Mutex;
use serde::Serialize;

// ---------------------------------------------------------------------------
// ConflictEvent — the core event type
// ---------------------------------------------------------------------------

/// A single conflict event emitted by the MVCC layer.
///
/// Each variant carries enough context to reconstruct what happened
/// without access to internal MVCC state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ConflictEvent {
    /// A page lock acquisition was denied because another txn holds it.
    PageLockContention {
        /// The page that was contended.
        page: PageNumber,
        /// The transaction that tried to acquire the lock.
        requester: TxnId,
        /// The transaction currently holding the lock.
        holder: TxnId,
        /// Monotonic event timestamp (nanoseconds since observer creation).
        timestamp_ns: u64,
    },

    /// First-Committer-Wins (FCW) detected base drift on a page.
    FcwBaseDrift {
        /// The page where drift was detected.
        page: PageNumber,
        /// The transaction that lost the FCW race.
        loser: TxnId,
        /// The transaction that committed first (winner).
        winner_commit_seq: CommitSeq,
        /// Whether merge was attempted.
        merge_attempted: bool,
        /// Whether merge succeeded (if attempted).
        merge_succeeded: bool,
        /// Monotonic event timestamp.
        timestamp_ns: u64,
    },

    /// SSI validation detected a dangerous structure (write skew).
    SsiAbort {
        /// The transaction that was aborted.
        txn: TxnToken,
        /// The reason for the abort.
        reason: SsiAbortCategory,
        /// Number of incoming rw-antidependency edges.
        in_edge_count: usize,
        /// Number of outgoing rw-antidependency edges.
        out_edge_count: usize,
        /// Monotonic event timestamp.
        timestamp_ns: u64,
    },

    /// A transaction committed successfully after resolving conflicts.
    ConflictResolved {
        /// The transaction that committed.
        txn: TxnId,
        /// Number of page conflicts resolved via merge.
        pages_merged: usize,
        /// Commit sequence assigned.
        commit_seq: CommitSeq,
        /// Monotonic event timestamp.
        timestamp_ns: u64,
    },
}

/// Categorized SSI abort reason (serialization-friendly).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum SsiAbortCategory {
    /// Transaction is the pivot (has both in + out rw edges).
    Pivot,
    /// A committed reader has an incoming rw edge.
    CommittedPivot,
    /// Transaction was eagerly marked for abort.
    MarkedForAbort,
}

impl ConflictEvent {
    /// Extract the monotonic timestamp from any event variant.
    #[must_use]
    pub fn timestamp_ns(&self) -> u64 {
        match self {
            Self::PageLockContention { timestamp_ns, .. }
            | Self::FcwBaseDrift { timestamp_ns, .. }
            | Self::SsiAbort { timestamp_ns, .. }
            | Self::ConflictResolved { timestamp_ns, .. } => *timestamp_ns,
        }
    }

    /// Whether this event represents a conflict (contention/drift/abort).
    #[must_use]
    pub fn is_conflict(&self) -> bool {
        !matches!(self, Self::ConflictResolved { .. })
    }
}

// ---------------------------------------------------------------------------
// ConflictObserver — trait for zero-cost opt-in observation
// ---------------------------------------------------------------------------

/// Observer trait for conflict events.
///
/// Implementations MUST be non-blocking and MUST NOT acquire page locks.
/// The observer is called on the hot path during lock acquisition and
/// commit validation; expensive work should be deferred.
pub trait ConflictObserver: Send + Sync {
    /// Called when a conflict event occurs.
    fn on_event(&self, event: &ConflictEvent);
}

/// No-op observer that compiles to nothing. Default when observability is
/// not configured.
#[derive(Debug, Clone, Copy)]
pub struct NoOpObserver;

impl ConflictObserver for NoOpObserver {
    #[inline(always)]
    fn on_event(&self, _event: &ConflictEvent) {}
}

// ---------------------------------------------------------------------------
// RingBuffer — bounded event storage
// ---------------------------------------------------------------------------

/// Fixed-capacity ring buffer for storing recent conflict events.
///
/// When the buffer is full, the oldest event is overwritten. Thread-safe
/// via internal `Mutex` (not on the hot path — only accessed via PRAGMA).
pub struct ConflictRingBuffer {
    events: Mutex<RingBuf>,
}

struct RingBuf {
    buf: Vec<ConflictEvent>,
    capacity: usize,
    head: usize,
    len: usize,
}

impl RingBuf {
    fn new(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
            capacity,
            head: 0,
            len: 0,
        }
    }

    fn push(&mut self, event: ConflictEvent) {
        if self.capacity == 0 {
            return;
        }
        let idx = (self.head + self.len) % self.capacity;
        if self.buf.len() < self.capacity {
            self.buf.push(event);
        } else {
            self.buf[idx] = event;
        }
        if self.len == self.capacity {
            self.head = (self.head + 1) % self.capacity;
        } else {
            self.len += 1;
        }
    }

    fn drain_ordered(&self) -> Vec<ConflictEvent> {
        let mut result = Vec::with_capacity(self.len);
        for i in 0..self.len {
            let idx = (self.head + i) % self.capacity;
            result.push(self.buf[idx].clone());
        }
        result
    }

    fn clear(&mut self) {
        self.buf.clear();
        self.head = 0;
        self.len = 0;
    }
}

impl ConflictRingBuffer {
    /// Create a new ring buffer with the given capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            events: Mutex::new(RingBuf::new(capacity)),
        }
    }

    /// Push an event into the ring buffer.
    pub fn push(&self, event: ConflictEvent) {
        self.events.lock().push(event);
    }

    /// Return all events in chronological order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<ConflictEvent> {
        self.events.lock().drain_ordered()
    }

    /// Clear all stored events.
    pub fn clear(&self) {
        self.events.lock().clear();
    }

    /// Current number of stored events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.lock().len
    }

    /// Whether the buffer is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Configured capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.events.lock().capacity
    }
}

// ---------------------------------------------------------------------------
// ConflictMetrics — aggregated statistics
// ---------------------------------------------------------------------------

/// Aggregated conflict statistics exposed via PRAGMA.
///
/// All counters are atomic for lock-free updates from the hot path.
/// Statistics are per-connection (not global).
pub struct ConflictMetrics {
    /// Total conflict events (contention + drift + abort).
    pub conflicts_total: AtomicU64,
    /// Page lock contention events.
    pub page_contentions: AtomicU64,
    /// FCW base drift events.
    pub fcw_drifts: AtomicU64,
    /// FCW merge attempts.
    pub fcw_merge_attempts: AtomicU64,
    /// FCW merge successes.
    pub fcw_merge_successes: AtomicU64,
    /// SSI abort events.
    pub ssi_aborts: AtomicU64,
    /// Successful conflict resolutions via merge.
    pub conflicts_resolved: AtomicU64,
    /// Per-page contention counts (behind mutex, not hot path).
    page_hotspots: Mutex<HashMap<PageNumber, u64>>,
    /// Creation time for rate calculations.
    created_at: Instant,
}

impl ConflictMetrics {
    /// Create a new metrics instance with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            conflicts_total: AtomicU64::new(0),
            page_contentions: AtomicU64::new(0),
            fcw_drifts: AtomicU64::new(0),
            fcw_merge_attempts: AtomicU64::new(0),
            fcw_merge_successes: AtomicU64::new(0),
            ssi_aborts: AtomicU64::new(0),
            conflicts_resolved: AtomicU64::new(0),
            page_hotspots: Mutex::new(HashMap::new()),
            created_at: Instant::now(),
        }
    }

    /// Record a conflict event, updating all relevant counters.
    pub fn record(&self, event: &ConflictEvent) {
        match event {
            ConflictEvent::PageLockContention { page, .. } => {
                self.conflicts_total.fetch_add(1, Ordering::Relaxed);
                self.page_contentions.fetch_add(1, Ordering::Relaxed);
                *self.page_hotspots.lock().entry(*page).or_insert(0) += 1;
            }
            ConflictEvent::FcwBaseDrift {
                page,
                merge_attempted,
                merge_succeeded,
                ..
            } => {
                self.conflicts_total.fetch_add(1, Ordering::Relaxed);
                self.fcw_drifts.fetch_add(1, Ordering::Relaxed);
                if *merge_attempted {
                    self.fcw_merge_attempts.fetch_add(1, Ordering::Relaxed);
                    if *merge_succeeded {
                        self.fcw_merge_successes.fetch_add(1, Ordering::Relaxed);
                    }
                }
                *self.page_hotspots.lock().entry(*page).or_insert(0) += 1;
            }
            ConflictEvent::SsiAbort { .. } => {
                self.conflicts_total.fetch_add(1, Ordering::Relaxed);
                self.ssi_aborts.fetch_add(1, Ordering::Relaxed);
            }
            ConflictEvent::ConflictResolved { .. } => {
                self.conflicts_resolved.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.conflicts_total.store(0, Ordering::Relaxed);
        self.page_contentions.store(0, Ordering::Relaxed);
        self.fcw_drifts.store(0, Ordering::Relaxed);
        self.fcw_merge_attempts.store(0, Ordering::Relaxed);
        self.fcw_merge_successes.store(0, Ordering::Relaxed);
        self.ssi_aborts.store(0, Ordering::Relaxed);
        self.conflicts_resolved.store(0, Ordering::Relaxed);
        self.page_hotspots.lock().clear();
    }

    /// Elapsed time since metrics creation.
    #[must_use]
    pub fn elapsed(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    /// Conflicts per second since creation.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn conflicts_per_second(&self) -> f64 {
        let elapsed_secs = self.created_at.elapsed().as_secs_f64();
        if elapsed_secs < f64::EPSILON {
            return 0.0;
        }
        self.conflicts_total.load(Ordering::Relaxed) as f64 / elapsed_secs
    }

    /// Top N pages by contention count.
    #[must_use]
    pub fn top_hotspots(&self, n: usize) -> Vec<(PageNumber, u64)> {
        let mut entries: Vec<(PageNumber, u64)> = {
            let map = self.page_hotspots.lock();
            map.iter().map(|(&k, &v)| (k, v)).collect()
        };
        entries.sort_by_key(|e| std::cmp::Reverse(e.1));
        entries.truncate(n);
        entries
    }

    /// Snapshot all metrics as a serializable summary.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn snapshot(&self) -> ConflictMetricsSnapshot {
        ConflictMetricsSnapshot {
            conflicts_total: self.conflicts_total.load(Ordering::Relaxed),
            page_contentions: self.page_contentions.load(Ordering::Relaxed),
            fcw_drifts: self.fcw_drifts.load(Ordering::Relaxed),
            fcw_merge_attempts: self.fcw_merge_attempts.load(Ordering::Relaxed),
            fcw_merge_successes: self.fcw_merge_successes.load(Ordering::Relaxed),
            ssi_aborts: self.ssi_aborts.load(Ordering::Relaxed),
            conflicts_resolved: self.conflicts_resolved.load(Ordering::Relaxed),
            conflicts_per_second: self.conflicts_per_second(),
            elapsed_secs: self.created_at.elapsed().as_secs_f64(),
            top_hotspots: self.top_hotspots(10),
        }
    }
}

impl Default for ConflictMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Serializable snapshot of conflict metrics.
#[derive(Debug, Clone, Serialize)]
pub struct ConflictMetricsSnapshot {
    pub conflicts_total: u64,
    pub page_contentions: u64,
    pub fcw_drifts: u64,
    pub fcw_merge_attempts: u64,
    pub fcw_merge_successes: u64,
    pub ssi_aborts: u64,
    pub conflicts_resolved: u64,
    pub conflicts_per_second: f64,
    pub elapsed_secs: f64,
    pub top_hotspots: Vec<(PageNumber, u64)>,
}

// ---------------------------------------------------------------------------
// MetricsObserver — observer that records to both metrics and ring buffer
// ---------------------------------------------------------------------------

/// Combined observer that records events to both a [`ConflictMetrics`]
/// aggregator and a [`ConflictRingBuffer`] for detailed logging.
pub struct MetricsObserver {
    metrics: ConflictMetrics,
    log: ConflictRingBuffer,
    epoch: Instant,
}

impl MetricsObserver {
    /// Create a new metrics observer with the given ring buffer capacity.
    #[must_use]
    pub fn new(log_capacity: usize) -> Self {
        Self {
            metrics: ConflictMetrics::new(),
            log: ConflictRingBuffer::new(log_capacity),
            epoch: Instant::now(),
        }
    }

    /// Access the aggregated metrics.
    #[must_use]
    pub fn metrics(&self) -> &ConflictMetrics {
        &self.metrics
    }

    /// Access the conflict log ring buffer.
    #[must_use]
    pub fn log(&self) -> &ConflictRingBuffer {
        &self.log
    }

    /// Elapsed nanoseconds since observer creation (for timestamps).
    #[must_use]
    pub fn elapsed_ns(&self) -> u64 {
        #[allow(clippy::cast_possible_truncation)] // clamped to u64::MAX
        {
            self.epoch.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
        }
    }

    /// Reset both metrics and log.
    pub fn reset(&self) {
        self.metrics.reset();
        self.log.clear();
    }
}

impl ConflictObserver for MetricsObserver {
    fn on_event(&self, event: &ConflictEvent) {
        self.metrics.record(event);
        self.log.push(event.clone());
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn page(n: u32) -> PageNumber {
        PageNumber::new(n).unwrap()
    }

    fn txn(n: u64) -> TxnId {
        TxnId::new(n).unwrap()
    }

    fn make_contention_event(pg: u32, req: u64, hold: u64) -> ConflictEvent {
        ConflictEvent::PageLockContention {
            page: page(pg),
            requester: txn(req),
            holder: txn(hold),
            timestamp_ns: 1000,
        }
    }

    #[test]
    fn noop_observer_compiles_away() {
        let obs = NoOpObserver;
        let event = make_contention_event(1, 2, 3);
        obs.on_event(&event);
        // If this compiles and runs, it proves the no-op path works.
    }

    #[test]
    fn ring_buffer_basic_push_and_snapshot() {
        let rb = ConflictRingBuffer::new(3);
        assert!(rb.is_empty());

        rb.push(make_contention_event(1, 10, 20));
        rb.push(make_contention_event(2, 11, 21));
        assert_eq!(rb.len(), 2);

        let snap = rb.snapshot();
        assert_eq!(snap.len(), 2);
        assert!(
            matches!(&snap[0], ConflictEvent::PageLockContention { page, .. } if page.get() == 1)
        );
        assert!(
            matches!(&snap[1], ConflictEvent::PageLockContention { page, .. } if page.get() == 2)
        );
    }

    #[test]
    fn ring_buffer_wraps_on_overflow() {
        let rb = ConflictRingBuffer::new(2);

        rb.push(make_contention_event(1, 10, 20));
        rb.push(make_contention_event(2, 11, 21));
        rb.push(make_contention_event(3, 12, 22)); // overwrites first

        assert_eq!(rb.len(), 2);
        let snap = rb.snapshot();
        // Should contain events for pages 2 and 3 (oldest evicted)
        assert!(
            matches!(&snap[0], ConflictEvent::PageLockContention { page, .. } if page.get() == 2)
        );
        assert!(
            matches!(&snap[1], ConflictEvent::PageLockContention { page, .. } if page.get() == 3)
        );
    }

    #[test]
    fn ring_buffer_clear() {
        let rb = ConflictRingBuffer::new(10);
        rb.push(make_contention_event(1, 10, 20));
        rb.push(make_contention_event(2, 11, 21));
        assert_eq!(rb.len(), 2);

        rb.clear();
        assert!(rb.is_empty());
        assert!(rb.snapshot().is_empty());
    }

    #[test]
    fn ring_buffer_zero_capacity() {
        let rb = ConflictRingBuffer::new(0);
        rb.push(make_contention_event(1, 10, 20));
        assert!(rb.is_empty());
    }

    #[test]
    fn conflict_metrics_basic_recording() {
        let m = ConflictMetrics::new();

        m.record(&make_contention_event(1, 10, 20));
        m.record(&make_contention_event(1, 11, 20)); // same page
        m.record(&make_contention_event(2, 12, 20));

        assert_eq!(m.conflicts_total.load(Ordering::Relaxed), 3);
        assert_eq!(m.page_contentions.load(Ordering::Relaxed), 3);

        let hotspots = m.top_hotspots(5);
        assert_eq!(hotspots.len(), 2);
        assert_eq!(hotspots[0].0, page(1));
        assert_eq!(hotspots[0].1, 2);
    }

    #[test]
    fn conflict_metrics_fcw_recording() {
        let m = ConflictMetrics::new();

        m.record(&ConflictEvent::FcwBaseDrift {
            page: page(5),
            loser: txn(10),
            winner_commit_seq: CommitSeq::new(100),
            merge_attempted: true,
            merge_succeeded: true,
            timestamp_ns: 2000,
        });

        assert_eq!(m.fcw_drifts.load(Ordering::Relaxed), 1);
        assert_eq!(m.fcw_merge_attempts.load(Ordering::Relaxed), 1);
        assert_eq!(m.fcw_merge_successes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn conflict_metrics_ssi_recording() {
        let m = ConflictMetrics::new();

        m.record(&ConflictEvent::SsiAbort {
            txn: TxnToken::new(txn(10), fsqlite_types::TxnEpoch::new(1)),
            reason: SsiAbortCategory::Pivot,
            in_edge_count: 1,
            out_edge_count: 1,
            timestamp_ns: 3000,
        });

        assert_eq!(m.ssi_aborts.load(Ordering::Relaxed), 1);
        assert_eq!(m.conflicts_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn conflict_metrics_reset() {
        let m = ConflictMetrics::new();
        m.record(&make_contention_event(1, 10, 20));
        assert_eq!(m.conflicts_total.load(Ordering::Relaxed), 1);

        m.reset();
        assert_eq!(m.conflicts_total.load(Ordering::Relaxed), 0);
        assert_eq!(m.page_contentions.load(Ordering::Relaxed), 0);
        assert!(m.top_hotspots(5).is_empty());
    }

    #[test]
    fn metrics_observer_records_both() {
        let obs = MetricsObserver::new(100);
        let event = make_contention_event(1, 10, 20);
        obs.on_event(&event);

        assert_eq!(obs.metrics().conflicts_total.load(Ordering::Relaxed), 1);
        assert_eq!(obs.log().len(), 1);
    }

    #[test]
    fn conflict_event_timestamp() {
        let event = make_contention_event(1, 10, 20);
        assert_eq!(event.timestamp_ns(), 1000);
    }

    #[test]
    fn conflict_event_is_conflict() {
        assert!(make_contention_event(1, 10, 20).is_conflict());
        assert!(
            !ConflictEvent::ConflictResolved {
                txn: txn(1),
                pages_merged: 0,
                commit_seq: CommitSeq::new(1),
                timestamp_ns: 0,
            }
            .is_conflict()
        );
    }

    #[test]
    fn metrics_snapshot_serializable() {
        let m = ConflictMetrics::new();
        m.record(&make_contention_event(1, 10, 20));
        let snap = m.snapshot();
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("\"conflicts_total\":1"));
    }
}
