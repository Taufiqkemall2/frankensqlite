//! S3-FIFO cache-queue state machine for pager eviction experiments (`bd-1xfri.1`).
//!
//! This module implements a three-queue FIFO policy:
//! - `SMALL`: admission queue
//! - `MAIN`: long-lived queue with bounded reinsertion
//! - `GHOST`: metadata-only queue for pages evicted from `SMALL`
//!
//! The structure tracks queue membership in a hash map for O(1) queue-location
//! lookup and performs deterministic transitions suitable for unit testing.

use std::collections::{HashMap, VecDeque};

use fsqlite_types::PageNumber;

const DEFAULT_SMALL_RATIO_NUM: usize = 1;
const DEFAULT_SMALL_RATIO_DEN: usize = 10;
const DEFAULT_MAX_REINSERT: u8 = 1;

/// Queue kind for S3-FIFO membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueKind {
    Small,
    Main,
    Ghost,
}

/// O(1) lookup result describing which queue currently owns a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueLocation {
    pub kind: QueueKind,
}

/// Configuration for [`S3Fifo`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S3FifoConfig {
    capacity: usize,
    small_capacity: usize,
    main_capacity: usize,
    ghost_capacity: usize,
    max_reinsert: u8,
}

impl S3FifoConfig {
    /// Build default capacities from total resident capacity.
    ///
    /// Default split:
    /// - `small`: ceil(10% of capacity), min 1
    /// - `main`: remainder
    /// - `ghost`: same as `small`
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");

        let scaled_capacity = capacity.saturating_mul(DEFAULT_SMALL_RATIO_NUM);
        let mut small_capacity = scaled_capacity / DEFAULT_SMALL_RATIO_DEN;
        if scaled_capacity % DEFAULT_SMALL_RATIO_DEN != 0 {
            small_capacity = small_capacity.saturating_add(1);
        }
        if small_capacity == 0 {
            small_capacity = 1;
        }

        let main_capacity = capacity.saturating_sub(small_capacity);
        let ghost_capacity = small_capacity;

        Self {
            capacity,
            small_capacity,
            main_capacity,
            ghost_capacity,
            max_reinsert: DEFAULT_MAX_REINSERT,
        }
    }

    /// Build an explicit-capacity configuration.
    #[must_use]
    pub fn with_limits(
        capacity: usize,
        small_capacity: usize,
        ghost_capacity: usize,
        max_reinsert: u8,
    ) -> Self {
        assert!(capacity > 0, "capacity must be > 0");
        assert!(
            small_capacity <= capacity,
            "small_capacity must be <= capacity"
        );

        Self {
            capacity,
            small_capacity,
            main_capacity: capacity.saturating_sub(small_capacity),
            ghost_capacity,
            max_reinsert,
        }
    }

    #[inline]
    #[must_use]
    pub const fn capacity(self) -> usize {
        self.capacity
    }

    #[inline]
    #[must_use]
    pub const fn small_capacity(self) -> usize {
        self.small_capacity
    }

    #[inline]
    #[must_use]
    pub const fn main_capacity(self) -> usize {
        self.main_capacity
    }

    #[inline]
    #[must_use]
    pub const fn ghost_capacity(self) -> usize {
        self.ghost_capacity
    }

    #[inline]
    #[must_use]
    pub const fn max_reinsert(self) -> u8 {
        self.max_reinsert
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResidentState {
    queue: QueueKind,
    accessed: bool,
    reinsert_count: u8,
}

impl ResidentState {
    #[must_use]
    const fn small() -> Self {
        Self {
            queue: QueueKind::Small,
            accessed: false,
            reinsert_count: 0,
        }
    }

    #[must_use]
    const fn main() -> Self {
        Self {
            queue: QueueKind::Main,
            accessed: false,
            reinsert_count: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryState {
    Resident(ResidentState),
    Ghost,
}

/// Deterministic transition events emitted by [`S3Fifo::insert`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3FifoEvent {
    Inserted(PageNumber),
    AlreadyResident {
        page_id: PageNumber,
        queue: QueueKind,
    },
    GhostReadmission(PageNumber),
    PromotedToMain(PageNumber),
    EvictedFromSmallToGhost(PageNumber),
    ReinsertedInMain {
        page_id: PageNumber,
        reinsert_count: u8,
    },
    EvictedFromMain(PageNumber),
    GhostTrimmed(PageNumber),
}

/// Three-queue S3-FIFO state machine.
#[derive(Debug)]
pub struct S3Fifo {
    config: S3FifoConfig,
    small: VecDeque<PageNumber>,
    main: VecDeque<PageNumber>,
    ghost: VecDeque<PageNumber>,
    index: HashMap<PageNumber, EntryState>,
}

impl S3Fifo {
    /// Create with default split from `capacity`.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self::with_config(S3FifoConfig::new(capacity))
    }

    /// Create with explicit configuration.
    #[must_use]
    pub fn with_config(config: S3FifoConfig) -> Self {
        Self {
            config,
            small: VecDeque::new(),
            main: VecDeque::new(),
            ghost: VecDeque::new(),
            index: HashMap::new(),
        }
    }

    #[inline]
    #[must_use]
    pub const fn config(&self) -> S3FifoConfig {
        self.config
    }

    #[inline]
    #[must_use]
    pub fn resident_len(&self) -> usize {
        self.small.len().saturating_add(self.main.len())
    }

    #[inline]
    #[must_use]
    pub fn ghost_len(&self) -> usize {
        self.ghost.len()
    }

    #[inline]
    #[must_use]
    pub fn small_pages(&self) -> Vec<PageNumber> {
        self.small.iter().copied().collect()
    }

    #[inline]
    #[must_use]
    pub fn main_pages(&self) -> Vec<PageNumber> {
        self.main.iter().copied().collect()
    }

    #[inline]
    #[must_use]
    pub fn ghost_pages(&self) -> Vec<PageNumber> {
        self.ghost.iter().copied().collect()
    }

    /// O(1) queue-location lookup.
    #[inline]
    #[must_use]
    pub fn lookup(&self, page_id: PageNumber) -> Option<QueueLocation> {
        match self.index.get(&page_id).copied() {
            Some(EntryState::Resident(state)) => Some(QueueLocation { kind: state.queue }),
            Some(EntryState::Ghost) => Some(QueueLocation {
                kind: QueueKind::Ghost,
            }),
            None => None,
        }
    }

    /// Set access bit for a resident page without reordering any queue.
    pub fn access(&mut self, page_id: PageNumber) -> bool {
        match self.index.get_mut(&page_id) {
            Some(EntryState::Resident(state)) => {
                state.accessed = true;
                true
            }
            Some(EntryState::Ghost) | None => false,
        }
    }

    /// Admit a page to `SMALL`, then rebalance.
    ///
    /// Returns the ordered transition events performed for this call.
    #[must_use]
    pub fn insert(&mut self, page_id: PageNumber) -> Vec<S3FifoEvent> {
        let mut events = Vec::new();

        match self.index.get(&page_id).copied() {
            Some(EntryState::Resident(state)) => {
                if let Some(EntryState::Resident(meta)) = self.index.get_mut(&page_id) {
                    meta.accessed = true;
                }
                events.push(S3FifoEvent::AlreadyResident {
                    page_id,
                    queue: state.queue,
                });
                return events;
            }
            Some(EntryState::Ghost) => {
                self.remove_ghost(page_id);
                events.push(S3FifoEvent::GhostReadmission(page_id));
            }
            None => {}
        }

        self.small.push_back(page_id);
        self.index
            .insert(page_id, EntryState::Resident(ResidentState::small()));
        events.push(S3FifoEvent::Inserted(page_id));

        self.rebalance(&mut events);
        events
    }

    fn rebalance(&mut self, events: &mut Vec<S3FifoEvent>) {
        while self.small.len() > self.config.small_capacity {
            self.evict_from_small(events);
        }

        while self.main.len() > self.config.main_capacity {
            if !self.evict_from_main(events) {
                break;
            }
        }

        while self.resident_len() > self.config.capacity {
            if !self.small.is_empty() {
                self.evict_from_small(events);
                continue;
            }

            if !self.evict_from_main(events) {
                break;
            }
        }

        self.trim_ghosts(events);
    }

    fn evict_from_small(&mut self, events: &mut Vec<S3FifoEvent>) {
        let Some(page_id) = self.small.pop_front() else {
            return;
        };

        let Some(EntryState::Resident(state)) = self.index.get(&page_id).copied() else {
            return;
        };

        if state.accessed {
            self.main.push_back(page_id);
            self.index
                .insert(page_id, EntryState::Resident(ResidentState::main()));
            events.push(S3FifoEvent::PromotedToMain(page_id));
            return;
        }

        self.ghost.push_back(page_id);
        self.index.insert(page_id, EntryState::Ghost);
        events.push(S3FifoEvent::EvictedFromSmallToGhost(page_id));
    }

    fn evict_from_main(&mut self, events: &mut Vec<S3FifoEvent>) -> bool {
        let Some(page_id) = self.main.pop_front() else {
            return false;
        };

        let Some(EntryState::Resident(mut state)) = self.index.get(&page_id).copied() else {
            return true;
        };

        if state.accessed && state.reinsert_count < self.config.max_reinsert {
            state.accessed = false;
            state.reinsert_count = state.reinsert_count.saturating_add(1);
            self.main.push_back(page_id);
            self.index.insert(page_id, EntryState::Resident(state));
            events.push(S3FifoEvent::ReinsertedInMain {
                page_id,
                reinsert_count: state.reinsert_count,
            });
            return true;
        }

        self.index.remove(&page_id);
        events.push(S3FifoEvent::EvictedFromMain(page_id));
        true
    }

    fn trim_ghosts(&mut self, events: &mut Vec<S3FifoEvent>) {
        while self.ghost.len() > self.config.ghost_capacity {
            let Some(page_id) = self.ghost.pop_front() else {
                break;
            };
            if matches!(self.index.get(&page_id), Some(EntryState::Ghost)) {
                self.index.remove(&page_id);
            }
            events.push(S3FifoEvent::GhostTrimmed(page_id));
        }
    }

    fn remove_ghost(&mut self, page_id: PageNumber) {
        self.ghost.retain(|candidate| *candidate != page_id);
        if matches!(self.index.get(&page_id), Some(EntryState::Ghost)) {
            self.index.remove(&page_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pg(n: u32) -> PageNumber {
        match PageNumber::new(n) {
            Some(page) => page,
            None => panic!("page number must be non-zero"),
        }
    }

    #[test]
    fn default_split_matches_spec() {
        let config = S3FifoConfig::new(10);
        assert_eq!(config.small_capacity(), 1);
        assert_eq!(config.main_capacity(), 9);
        assert_eq!(config.ghost_capacity(), 1);
        assert_eq!(config.max_reinsert(), 1);
    }

    #[test]
    fn access_sets_flag_without_reordering_small() {
        let mut fifo = S3Fifo::with_config(S3FifoConfig::with_limits(4, 2, 2, 1));
        let _ = fifo.insert(pg(1));
        let _ = fifo.insert(pg(2));
        assert_eq!(fifo.small_pages(), vec![pg(1), pg(2)]);

        assert!(fifo.access(pg(1)));
        assert_eq!(fifo.small_pages(), vec![pg(1), pg(2)]);

        let location = fifo.lookup(pg(1));
        assert_eq!(
            location,
            Some(QueueLocation {
                kind: QueueKind::Small
            })
        );
    }

    #[test]
    fn accessed_small_page_promotes_to_main() {
        let mut fifo = S3Fifo::with_config(S3FifoConfig::with_limits(3, 2, 2, 1));
        let _ = fifo.insert(pg(1));
        let _ = fifo.insert(pg(2));
        assert!(fifo.access(pg(1)));

        let events = fifo.insert(pg(3));
        assert!(events.contains(&S3FifoEvent::PromotedToMain(pg(1))));
        assert_eq!(fifo.main_pages(), vec![pg(1)]);
        assert_eq!(fifo.small_pages(), vec![pg(2), pg(3)]);
        assert_eq!(
            fifo.lookup(pg(1)),
            Some(QueueLocation {
                kind: QueueKind::Main
            })
        );
    }

    #[test]
    fn main_reinsertion_is_bounded_by_max_reinsert() {
        let mut fifo = S3Fifo::with_config(S3FifoConfig::with_limits(2, 1, 1, 1));

        let _ = fifo.insert(pg(1));
        assert!(fifo.access(pg(1)));
        let _ = fifo.insert(pg(2)); // 1 promoted to MAIN

        assert!(fifo.access(pg(1)));
        let _ = fifo.insert(pg(3)); // 2 -> GHOST
        assert!(fifo.access(pg(3)));
        let events_reinsert = fifo.insert(pg(4)); // 3 -> MAIN, then MAIN overflow

        assert!(events_reinsert.contains(&S3FifoEvent::ReinsertedInMain {
            page_id: pg(1),
            reinsert_count: 1
        }));

        assert!(fifo.access(pg(1)));
        assert!(fifo.access(pg(4)));
        let events_evict = fifo.insert(pg(5)); // second overflow; 1 should now evict

        assert!(events_evict.contains(&S3FifoEvent::EvictedFromMain(pg(1))));
        assert_eq!(fifo.lookup(pg(1)), None);
    }

    #[test]
    fn ghost_queue_is_bounded_and_trimmed() {
        let mut fifo = S3Fifo::with_config(S3FifoConfig::with_limits(2, 1, 1, 1));
        let _ = fifo.insert(pg(1));
        let _ = fifo.insert(pg(2)); // 1 -> GHOST

        assert_eq!(fifo.ghost_pages(), vec![pg(1)]);
        assert_eq!(
            fifo.lookup(pg(1)),
            Some(QueueLocation {
                kind: QueueKind::Ghost
            })
        );

        let events = fifo.insert(pg(3)); // 2 -> GHOST, then trim 1
        assert!(events.contains(&S3FifoEvent::GhostTrimmed(pg(1))));
        assert_eq!(fifo.ghost_pages(), vec![pg(2)]);
        assert_eq!(fifo.lookup(pg(1)), None);
    }

    #[test]
    fn queue_lookup_tracks_small_main_and_ghost() {
        let mut fifo = S3Fifo::with_config(S3FifoConfig::with_limits(3, 2, 2, 1));
        let _ = fifo.insert(pg(10));
        let _ = fifo.insert(pg(11));
        assert_eq!(
            fifo.lookup(pg(10)),
            Some(QueueLocation {
                kind: QueueKind::Small
            })
        );

        assert!(fifo.access(pg(10)));
        let _ = fifo.insert(pg(12)); // promote 10 -> MAIN
        assert_eq!(
            fifo.lookup(pg(10)),
            Some(QueueLocation {
                kind: QueueKind::Main
            })
        );

        let _ = fifo.insert(pg(13)); // likely pushes 11 to ghost path
        assert_eq!(
            fifo.lookup(pg(11)),
            Some(QueueLocation {
                kind: QueueKind::Ghost
            })
        );
    }
}
