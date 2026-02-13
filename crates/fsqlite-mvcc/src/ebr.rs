//! Epoch-based reclamation scaffolding for MVCC version-chain GC.
//!
//! This module provides a safe wrapper around `crossbeam-epoch` pin/unpin
//! semantics so transaction lifecycle hooks can carry a `VersionGuard` without
//! exposing raw epoch internals.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crossbeam_epoch::{self as epoch, Guard};
use parking_lot::Mutex;

/// Configuration for stale-reader detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaleReaderConfig {
    /// Reader pins older than this duration are considered stale.
    pub warn_after: Duration,
    /// Minimum interval between repeated warnings for the same guard.
    pub warn_every: Duration,
}

impl Default for StaleReaderConfig {
    fn default() -> Self {
        Self {
            warn_after: Duration::from_secs(30),
            warn_every: Duration::from_secs(5),
        }
    }
}

/// Snapshot of an active stale reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderPinSnapshot {
    /// Stable ID assigned to the pinned guard.
    pub guard_id: u64,
    /// Elapsed pin duration.
    pub pinned_for: Duration,
}

#[derive(Debug, Clone, Copy)]
struct ReaderPinState {
    pinned_at: Instant,
    last_warned_at: Option<Instant>,
}

/// Registry for active epoch pins (`VersionGuard`s).
///
/// The registry is intentionally lock-based and simple for the initial
/// integration slice; cardinality is bounded by active transactions.
#[derive(Debug)]
pub struct VersionGuardRegistry {
    stale_reader: StaleReaderConfig,
    next_guard_id: AtomicU64,
    active: Mutex<HashMap<u64, ReaderPinState>>,
}

impl VersionGuardRegistry {
    /// Construct a registry with the provided stale-reader policy.
    #[must_use]
    pub fn new(stale_reader: StaleReaderConfig) -> Self {
        Self {
            stale_reader,
            next_guard_id: AtomicU64::new(1),
            active: Mutex::new(HashMap::new()),
        }
    }

    /// Stale-reader policy currently in use.
    #[must_use]
    pub const fn stale_reader_config(&self) -> StaleReaderConfig {
        self.stale_reader
    }

    /// Number of currently pinned guards.
    #[must_use]
    pub fn active_guard_count(&self) -> usize {
        self.active.lock().len()
    }

    /// Snapshot all stale readers as of `now`.
    #[must_use]
    pub fn stale_reader_snapshots(&self, now: Instant) -> Vec<ReaderPinSnapshot> {
        self.active
            .lock()
            .iter()
            .filter_map(|(&guard_id, state)| {
                let pinned_for = now.saturating_duration_since(state.pinned_at);
                if pinned_for >= self.stale_reader.warn_after {
                    Some(ReaderPinSnapshot {
                        guard_id,
                        pinned_for,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    /// Emit stale-reader warnings as of `now`.
    ///
    /// Returns the number of warnings emitted.
    pub fn warn_on_stale_readers(&self, now: Instant) -> usize {
        let mut warned = 0_usize;
        let mut active = self.active.lock();
        for (&guard_id, state) in active.iter_mut() {
            let pinned_for = now.saturating_duration_since(state.pinned_at);
            if pinned_for < self.stale_reader.warn_after {
                continue;
            }

            let should_warn = state.last_warned_at.is_none_or(|last| {
                now.saturating_duration_since(last) >= self.stale_reader.warn_every
            });
            if should_warn {
                tracing::warn!(
                    guard_id,
                    pinned_for_ms = pinned_for.as_millis(),
                    stale_warn_after_ms = self.stale_reader.warn_after.as_millis(),
                    "stale MVCC reader pin is blocking epoch advancement"
                );
                state.last_warned_at = Some(now);
                warned += 1;
            }
        }
        drop(active);
        warned
    }

    fn register_guard(&self, pinned_at: Instant) -> u64 {
        let guard_id = self.next_guard_id.fetch_add(1, Ordering::Relaxed);
        self.active.lock().insert(
            guard_id,
            ReaderPinState {
                pinned_at,
                last_warned_at: None,
            },
        );
        guard_id
    }

    fn unregister_guard(&self, guard_id: u64) -> Option<Duration> {
        self.active
            .lock()
            .remove(&guard_id)
            .map(|state| state.pinned_at.elapsed())
    }
}

impl Default for VersionGuardRegistry {
    fn default() -> Self {
        Self::new(StaleReaderConfig::default())
    }
}

/// Transaction-scoped epoch pin.
///
/// Construct at transaction begin and drop at transaction end (commit or
/// abort). Retirements deferred through this guard are only reclaimed after all
/// currently pinned readers have unpinned.
#[derive(Debug)]
pub struct VersionGuard {
    registry: Arc<VersionGuardRegistry>,
    guard_id: u64,
    pinned_at: Instant,
    guard: Guard,
}

impl VersionGuard {
    /// Pin the current thread into the epoch domain.
    #[must_use]
    pub fn pin(registry: Arc<VersionGuardRegistry>) -> Self {
        let pinned_at = Instant::now();
        let guard_id = registry.register_guard(pinned_at);
        let guard = epoch::pin();
        Self {
            registry,
            guard_id,
            pinned_at,
            guard,
        }
    }

    /// Stable ID for diagnostics and stale-reader reporting.
    #[must_use]
    pub const fn guard_id(&self) -> u64 {
        self.guard_id
    }

    /// Elapsed pin duration.
    #[must_use]
    pub fn pinned_for(&self) -> Duration {
        self.pinned_at.elapsed()
    }

    /// Defer retirement of an owned value until it is safe to reclaim.
    pub fn defer_retire<T>(&self, retired: T)
    where
        T: Send + 'static,
    {
        self.guard.defer(move || drop(retired));
    }

    /// Defer an arbitrary retirement closure.
    pub fn defer_retire_with<F, R>(&self, retire: F)
    where
        F: FnOnce() -> R + Send + 'static,
    {
        self.guard.defer(retire);
    }

    /// Flush local deferred-retirement queue toward execution.
    ///
    /// Actual execution still depends on epoch advancement and active readers.
    pub fn flush(&self) {
        self.guard.flush();
    }
}

impl Drop for VersionGuard {
    fn drop(&mut self) {
        let pinned_for = self
            .registry
            .unregister_guard(self.guard_id)
            .unwrap_or_else(|| self.pinned_at.elapsed());
        if pinned_for >= self.registry.stale_reader_config().warn_after {
            tracing::warn!(
                guard_id = self.guard_id,
                pinned_for_ms = pinned_for.as_millis(),
                stale_warn_after_ms = self.registry.stale_reader_config().warn_after.as_millis(),
                "MVCC reader pin ended after stale threshold"
            );
        }
    }
}

/// Send-safe transaction-scoped epoch registration.
///
/// Unlike [`VersionGuard`], a ticket does not hold a thread-local
/// `crossbeam-epoch::Guard`.  This makes it `Send + Sync` so it can live
/// inside a [`Transaction`] that may be moved between threads (async
/// workloads, thread pools, etc.).
///
/// Stale-reader detection and epoch-advancement tracking still work because the
/// ticket is registered in the [`VersionGuardRegistry`] for its entire
/// lifetime.  Actual epoch pinning for deferred retirement happens via
/// short-lived [`VersionGuard`]s at the point where version chains are
/// traversed or old versions are freed.
#[derive(Debug)]
pub struct VersionGuardTicket {
    registry: Arc<VersionGuardRegistry>,
    guard_id: u64,
    pinned_at: Instant,
}

impl VersionGuardTicket {
    /// Register a transaction-scoped ticket.
    #[must_use]
    pub fn register(registry: Arc<VersionGuardRegistry>) -> Self {
        let pinned_at = Instant::now();
        let guard_id = registry.register_guard(pinned_at);
        Self {
            registry,
            guard_id,
            pinned_at,
        }
    }

    /// Stable ID for diagnostics and stale-reader reporting.
    #[must_use]
    pub const fn guard_id(&self) -> u64 {
        self.guard_id
    }

    /// Elapsed registration duration.
    #[must_use]
    pub fn registered_for(&self) -> Duration {
        self.pinned_at.elapsed()
    }

    /// Reference to the owning registry.
    #[must_use]
    pub fn registry(&self) -> &Arc<VersionGuardRegistry> {
        &self.registry
    }

    /// Pin the current thread's epoch and defer retirement of a value.
    ///
    /// The short-lived epoch pin ensures correctness: the deferred value is
    /// only reclaimed after all concurrently pinned readers have advanced
    /// past the current epoch.
    pub fn defer_retire<T: Send + 'static>(&self, retired: T) {
        let guard = epoch::pin();
        guard.defer(move || drop(retired));
        guard.flush();
    }

    /// Pin the current thread's epoch and defer an arbitrary closure.
    pub fn defer_retire_with<F, R>(&self, retire: F)
    where
        F: FnOnce() -> R + Send + 'static,
    {
        let guard = epoch::pin();
        guard.defer(retire);
        guard.flush();
    }
}

impl Drop for VersionGuardTicket {
    fn drop(&mut self) {
        let pinned_for = self
            .registry
            .unregister_guard(self.guard_id)
            .unwrap_or_else(|| self.pinned_at.elapsed());
        if pinned_for >= self.registry.stale_reader_config().warn_after {
            tracing::warn!(
                guard_id = self.guard_id,
                pinned_for_ms = pinned_for.as_millis(),
                stale_warn_after_ms = self.registry.stale_reader_config().warn_after.as_millis(),
                "MVCC reader registration ended after stale threshold"
            );
        }
    }
}

// SAFETY: VersionGuardTicket contains only an Arc and trivial integers.
// It does not hold a thread-local crossbeam Guard.
unsafe impl Send for VersionGuardTicket {}
// SAFETY: All fields are either atomic or behind a Mutex.
unsafe impl Sync for VersionGuardTicket {}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

    use crossbeam_epoch as epoch;

    use super::{StaleReaderConfig, VersionGuard, VersionGuardRegistry};

    #[test]
    fn version_guard_registers_and_unregisters() {
        let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig {
            warn_after: Duration::from_secs(60),
            warn_every: Duration::from_secs(10),
        }));
        assert_eq!(registry.active_guard_count(), 0);

        {
            let guard = VersionGuard::pin(Arc::clone(&registry));
            assert_eq!(registry.active_guard_count(), 1);
            assert!(guard.pinned_for() < Duration::from_secs(1));
        }

        assert_eq!(registry.active_guard_count(), 0);
    }

    #[test]
    fn stale_reader_snapshots_report_long_pins() {
        let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig {
            warn_after: Duration::from_millis(5),
            warn_every: Duration::from_millis(5),
        }));

        let _guard = VersionGuard::pin(Arc::clone(&registry));
        thread::sleep(Duration::from_millis(10));

        let stale = registry.stale_reader_snapshots(Instant::now());
        assert_eq!(stale.len(), 1);
        assert!(stale[0].pinned_for >= Duration::from_millis(5));
    }

    #[test]
    fn stale_reader_warning_is_rate_limited() {
        let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig {
            warn_after: Duration::ZERO,
            warn_every: Duration::from_millis(5),
        }));
        let _guard = VersionGuard::pin(Arc::clone(&registry));

        let base = Instant::now();
        assert_eq!(registry.warn_on_stale_readers(base), 1);
        assert_eq!(
            registry.warn_on_stale_readers(base + Duration::from_millis(1)),
            0
        );
        assert_eq!(
            registry.warn_on_stale_readers(base + Duration::from_millis(6)),
            1
        );
    }

    #[derive(Clone)]
    struct DropCounter(Arc<AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn deferred_retirement_executes_after_unpin() {
        let registry = Arc::new(VersionGuardRegistry::default());
        let dropped = Arc::new(AtomicUsize::new(0));

        {
            let guard = VersionGuard::pin(Arc::clone(&registry));
            guard.defer_retire(DropCounter(Arc::clone(&dropped)));
            guard.flush();
            assert_eq!(dropped.load(Ordering::SeqCst), 0);
        }

        for _ in 0..64 {
            let flush_guard = epoch::pin();
            flush_guard.flush();
            if dropped.load(Ordering::SeqCst) > 0 {
                break;
            }
            thread::yield_now();
        }

        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
}
