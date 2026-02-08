//! Core bounded-parallelism primitives (§1.5, bd-22n.4).
//!
//! This module provides a small bulkhead framework for internal background work.
//! It is intentionally non-blocking: overflow is rejected with `SQLITE_BUSY`
//! (`FrankenError::Busy`) instead of queue-and-wait semantics.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::Region;

const MAX_BALANCED_BG_CPU: usize = 16;

/// Policy used when the bulkhead admission budget is exhausted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Reject overflow immediately with `SQLITE_BUSY`.
    DropBusy,
}

/// Runtime profile for conservative parallelism defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParallelismProfile {
    /// Conservative profile used by default.
    Balanced,
}

/// Bounded parallelism configuration for a work class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BulkheadConfig {
    /// Number of tasks allowed to execute concurrently.
    pub max_concurrent: usize,
    /// Additional bounded admission slots (not unbounded queueing).
    pub queue_depth: usize,
    /// Overflow behavior when capacity is exhausted.
    pub overflow_policy: OverflowPolicy,
}

impl BulkheadConfig {
    /// Create an explicit configuration.
    ///
    /// Returns `None` when `max_concurrent` is zero.
    #[must_use]
    pub const fn new(
        max_concurrent: usize,
        queue_depth: usize,
        overflow_policy: OverflowPolicy,
    ) -> Option<Self> {
        if max_concurrent == 0 {
            None
        } else {
            Some(Self {
                max_concurrent,
                queue_depth,
                overflow_policy,
            })
        }
    }

    /// Conservative default derived from available CPU parallelism.
    ///
    /// Uses the "balanced profile" formula from bd-22n.4:
    /// `clamp(P / 8, 1, 16)` where `P = available_parallelism`.
    #[must_use]
    pub fn for_profile(profile: ParallelismProfile) -> Self {
        let p = available_parallelism_or_one();
        match profile {
            ParallelismProfile::Balanced => Self {
                max_concurrent: conservative_bg_cpu_max(p),
                queue_depth: 0,
                overflow_policy: OverflowPolicy::DropBusy,
            },
        }
    }

    /// Maximum admitted work units at once.
    #[must_use]
    pub const fn admission_limit(self) -> usize {
        self.max_concurrent.saturating_add(self.queue_depth)
    }
}

impl Default for BulkheadConfig {
    fn default() -> Self {
        Self::for_profile(ParallelismProfile::Balanced)
    }
}

/// Compute conservative default background CPU parallelism from `P`.
#[must_use]
pub const fn conservative_bg_cpu_max(p: usize) -> usize {
    let base = p / 8;
    if base == 0 {
        1
    } else if base > MAX_BALANCED_BG_CPU {
        MAX_BALANCED_BG_CPU
    } else {
        base
    }
}

/// Return `std::thread::available_parallelism()` with a safe floor of 1.
#[must_use]
pub fn available_parallelism_or_one() -> usize {
    std::thread::available_parallelism().map_or(1, NonZeroUsize::get)
}

/// Non-blocking bulkhead admission gate.
#[derive(Debug)]
pub struct Bulkhead {
    config: BulkheadConfig,
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
    busy_rejections: AtomicUsize,
}

impl Bulkhead {
    #[must_use]
    pub fn new(config: BulkheadConfig) -> Self {
        Self {
            config,
            in_flight: AtomicUsize::new(0),
            peak_in_flight: AtomicUsize::new(0),
            busy_rejections: AtomicUsize::new(0),
        }
    }

    #[must_use]
    pub const fn config(&self) -> BulkheadConfig {
        self.config
    }

    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn peak_in_flight(&self) -> usize {
        self.peak_in_flight.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn busy_rejections(&self) -> usize {
        self.busy_rejections.load(Ordering::Acquire)
    }

    /// Try to admit one work item.
    ///
    /// Never blocks. If the admission budget is exhausted, this returns
    /// `FrankenError::Busy`.
    pub fn try_acquire(&self) -> Result<BulkheadPermit<'_>> {
        let limit = self.config.admission_limit();
        loop {
            let current = self.in_flight.load(Ordering::Acquire);
            if current >= limit {
                self.busy_rejections.fetch_add(1, Ordering::AcqRel);
                return Err(match self.config.overflow_policy {
                    OverflowPolicy::DropBusy => FrankenError::Busy,
                });
            }

            let next = current.saturating_add(1);
            if self
                .in_flight
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.peak_in_flight.fetch_max(next, Ordering::AcqRel);
                return Ok(BulkheadPermit {
                    bulkhead: self,
                    released: false,
                });
            }
        }
    }

    /// Run work within a bulkhead permit.
    pub fn run<T>(&self, work: impl FnOnce() -> T) -> Result<T> {
        let _permit = self.try_acquire()?;
        Ok(work())
    }
}

/// RAII permit for a single admitted work item.
#[derive(Debug)]
pub struct BulkheadPermit<'a> {
    bulkhead: &'a Bulkhead,
    released: bool,
}

impl BulkheadPermit<'_> {
    /// Explicitly release the permit.
    pub fn release(mut self) {
        if !self.released {
            self.bulkhead.in_flight.fetch_sub(1, Ordering::AcqRel);
            self.released = true;
        }
    }
}

impl Drop for BulkheadPermit<'_> {
    fn drop(&mut self) {
        if !self.released {
            self.bulkhead.in_flight.fetch_sub(1, Ordering::AcqRel);
            self.released = true;
        }
    }
}

/// Region-owned wrapper used for structured-concurrency integration.
#[derive(Debug)]
pub struct RegionBulkhead {
    region: Region,
    bulkhead: Bulkhead,
    closing: AtomicBool,
}

impl RegionBulkhead {
    #[must_use]
    pub fn new(region: Region, config: BulkheadConfig) -> Self {
        Self {
            region,
            bulkhead: Bulkhead::new(config),
            closing: AtomicBool::new(false),
        }
    }

    #[must_use]
    pub const fn region(&self) -> Region {
        self.region
    }

    #[must_use]
    pub fn bulkhead(&self) -> &Bulkhead {
        &self.bulkhead
    }

    pub fn try_acquire(&self) -> Result<BulkheadPermit<'_>> {
        if self.closing.load(Ordering::Acquire) {
            return Err(FrankenError::Busy);
        }
        self.bulkhead.try_acquire()
    }

    /// Begin region close: no new admissions are allowed after this point.
    pub fn begin_close(&self) {
        self.closing.store(true, Ordering::Release);
    }

    /// Whether all region-owned work has quiesced.
    #[must_use]
    pub fn is_quiescent(&self) -> bool {
        self.bulkhead.in_flight() == 0
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use super::*;

    const BEAD_ID: &str = "bd-22n.4";

    #[test]
    fn test_parallelism_defaults_conservative() {
        assert_eq!(
            conservative_bg_cpu_max(16),
            2,
            "bead_id={BEAD_ID} case=balanced_profile_formula_p16"
        );
        assert_eq!(
            conservative_bg_cpu_max(1),
            1,
            "bead_id={BEAD_ID} case=balanced_profile_min_floor"
        );
        assert_eq!(
            conservative_bg_cpu_max(512),
            16,
            "bead_id={BEAD_ID} case=balanced_profile_max_cap"
        );
    }

    #[test]
    fn test_parallelism_bounded_by_available() {
        let cfg = BulkheadConfig::default();
        let p = available_parallelism_or_one();
        assert!(
            cfg.max_concurrent <= p,
            "bead_id={BEAD_ID} case=default_exceeds_available_parallelism cfg={cfg:?} p={p}"
        );

        let bulkhead = Bulkhead::new(cfg);
        let mut permits = Vec::new();
        for _ in 0..cfg.admission_limit() {
            permits.push(
                bulkhead
                    .try_acquire()
                    .expect("admission under configured limit should succeed"),
            );
        }

        let overflow = bulkhead.try_acquire();
        assert!(
            matches!(overflow, Err(FrankenError::Busy)),
            "bead_id={BEAD_ID} case=bounded_admission_overflow_must_be_busy overflow={overflow:?}"
        );

        drop(permits);
        assert_eq!(
            bulkhead.in_flight(),
            0,
            "bead_id={BEAD_ID} case=permits_drop_to_zero"
        );
    }

    #[test]
    fn test_bulkhead_config_max_concurrent() {
        let cfg = BulkheadConfig::new(3, 0, OverflowPolicy::DropBusy)
            .expect("non-zero max_concurrent must be valid");
        let bulkhead = Bulkhead::new(cfg);

        let p1 = bulkhead.try_acquire().expect("slot 1");
        let p2 = bulkhead.try_acquire().expect("slot 2");
        let p3 = bulkhead.try_acquire().expect("slot 3");
        let overflow = bulkhead.try_acquire();

        assert!(
            matches!(overflow, Err(FrankenError::Busy)),
            "bead_id={BEAD_ID} case=max_concurrent_enforced overflow={overflow:?}"
        );
        drop((p1, p2, p3));
    }

    #[test]
    fn test_overflow_policy_drop_with_busy() {
        let cfg = BulkheadConfig::new(1, 0, OverflowPolicy::DropBusy)
            .expect("non-zero max_concurrent must be valid");
        let bulkhead = Bulkhead::new(cfg);
        let _permit = bulkhead.try_acquire().expect("first permit must succeed");

        let overflow = bulkhead.try_acquire();
        assert!(
            matches!(overflow, Err(FrankenError::Busy)),
            "bead_id={BEAD_ID} case=overflow_policy_drop_busy overflow={overflow:?}"
        );
    }

    #[test]
    fn test_background_work_degrades_gracefully() {
        let cfg = BulkheadConfig::new(2, 0, OverflowPolicy::DropBusy)
            .expect("non-zero max_concurrent must be valid");
        let bulkhead = Bulkhead::new(cfg);

        let _a = bulkhead.try_acquire().expect("permit a");
        let _b = bulkhead.try_acquire().expect("permit b");

        for _ in 0..8 {
            let result = bulkhead.try_acquire();
            assert!(
                matches!(result, Err(FrankenError::Busy)),
                "bead_id={BEAD_ID} case=overflow_must_reject_not_wait result={result:?}"
            );
        }

        assert_eq!(
            bulkhead.busy_rejections(),
            8,
            "bead_id={BEAD_ID} case=busy_rejection_counter"
        );
    }

    #[test]
    fn test_region_integration() {
        let cfg = BulkheadConfig::new(1, 0, OverflowPolicy::DropBusy)
            .expect("non-zero max_concurrent must be valid");
        let region_bulkhead = RegionBulkhead::new(Region::new(7), cfg);
        assert_eq!(
            region_bulkhead.region().get(),
            7,
            "bead_id={BEAD_ID} case=region_id_plumbed"
        );

        let permit = region_bulkhead.try_acquire().expect("first permit");
        assert!(
            !region_bulkhead.is_quiescent(),
            "bead_id={BEAD_ID} case=region_non_quiescent_with_active_work"
        );

        region_bulkhead.begin_close();
        let after_close = region_bulkhead.try_acquire();
        assert!(
            matches!(after_close, Err(FrankenError::Busy)),
            "bead_id={BEAD_ID} case=region_close_blocks_new_work result={after_close:?}"
        );

        drop(permit);
        assert!(
            region_bulkhead.is_quiescent(),
            "bead_id={BEAD_ID} case=region_quiescent_after_permit_drop"
        );
    }

    #[test]
    fn test_e2e_bounded_parallelism_under_background_load() {
        let cfg = BulkheadConfig::new(4, 0, OverflowPolicy::DropBusy)
            .expect("non-zero max_concurrent must be valid");
        let bulkhead = Arc::new(Bulkhead::new(cfg));

        let handles: Vec<_> = (0..48)
            .map(|_| {
                let bulkhead = Arc::clone(&bulkhead);
                thread::spawn(move || {
                    bulkhead.run(|| {
                        thread::sleep(Duration::from_millis(10));
                    })
                })
            })
            .collect();

        let mut busy = 0_usize;
        for handle in handles {
            match handle.join().expect("worker thread should not panic") {
                Ok(()) => {}
                Err(FrankenError::Busy) => busy = busy.saturating_add(1),
                Err(err) => {
                    assert_eq!(
                        err.error_code(),
                        fsqlite_error::ErrorCode::Busy,
                        "bead_id={BEAD_ID} case=e2e_unexpected_bulkhead_error err={err}"
                    );
                    busy = busy.saturating_add(1);
                }
            }
        }

        assert!(
            busy > 0,
            "bead_id={BEAD_ID} case=e2e_should_observe_overflow_rejections"
        );
        assert!(
            bulkhead.peak_in_flight() <= cfg.admission_limit(),
            "bead_id={BEAD_ID} case=e2e_peak_parallelism_exceeded peak={} limit={}",
            bulkhead.peak_in_flight(),
            cfg.admission_limit()
        );
    }
}
