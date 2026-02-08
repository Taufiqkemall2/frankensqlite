//! Commit durability and asynchronous repair orchestration (§1.6, bd-22n.11).
//!
//! The critical path only appends+syncs systematic symbols. Repair symbols are
//! generated/append-synced asynchronously after commit acknowledgment.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use fsqlite_error::{FrankenError, Result};
use tracing::{debug, error, info, warn};

const BEAD_ID: &str = "bd-22n.11";

/// Commit/repair lifecycle events used for timing and invariant validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommitRepairEventKind {
    CommitDurable,
    DurableButNotRepairable,
    CommitAcked,
    RepairStarted,
    RepairCompleted,
    RepairFailed,
}

/// Timestamped lifecycle event for one commit sequence.
#[derive(Debug, Clone, Copy)]
pub struct CommitRepairEvent {
    pub commit_seq: u64,
    pub at: Instant,
    pub kind: CommitRepairEventKind,
}

/// Repair state for a commit sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairState {
    NotScheduled,
    Pending,
    Completed,
    Failed,
}

/// Commit result produced by the critical path.
#[derive(Debug, Clone, Copy)]
pub struct CommitReceipt {
    pub commit_seq: u64,
    pub durable: bool,
    pub repair_pending: bool,
    pub latency: Duration,
}

/// Runtime behavior toggle for async repair generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitRepairConfig {
    pub repair_enabled: bool,
}

impl Default for CommitRepairConfig {
    fn default() -> Self {
        Self {
            repair_enabled: true,
        }
    }
}

/// Storage sink for systematic/repair symbol append+sync operations.
pub trait CommitRepairIo: Send + Sync {
    fn append_systematic_symbols(&self, commit_seq: u64, systematic_symbols: &[u8]) -> Result<()>;
    fn sync_systematic_symbols(&self, commit_seq: u64) -> Result<()>;
    fn append_repair_symbols(&self, commit_seq: u64, repair_symbols: &[u8]) -> Result<()>;
    fn sync_repair_symbols(&self, commit_seq: u64) -> Result<()>;
}

/// Generator for repair symbols from committed systematic symbols.
pub trait RepairSymbolGenerator: Send + Sync {
    fn generate_repair_symbols(
        &self,
        commit_seq: u64,
        systematic_symbols: &[u8],
    ) -> Result<Vec<u8>>;
}

/// In-memory IO sink useful for deterministic testing/instrumentation.
#[derive(Debug, Default)]
pub struct InMemoryCommitRepairIo {
    systematic_by_commit: Mutex<HashMap<u64, Vec<u8>>>,
    repair_by_commit: Mutex<HashMap<u64, Vec<u8>>>,
    total_systematic_bytes: AtomicU64,
    total_repair_bytes: AtomicU64,
    systematic_syncs: AtomicU64,
    repair_syncs: AtomicU64,
}

impl InMemoryCommitRepairIo {
    #[must_use]
    pub fn total_repair_bytes(&self) -> u64 {
        self.total_repair_bytes.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn repair_sync_count(&self) -> u64 {
        self.repair_syncs.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn systematic_sync_count(&self) -> u64 {
        self.systematic_syncs.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn repair_symbols_for(&self, commit_seq: u64) -> Option<Vec<u8>> {
        lock_with_recovery(&self.repair_by_commit, "repair_by_commit")
            .get(&commit_seq)
            .cloned()
    }
}

impl CommitRepairIo for InMemoryCommitRepairIo {
    fn append_systematic_symbols(&self, commit_seq: u64, systematic_symbols: &[u8]) -> Result<()> {
        lock_with_recovery(&self.systematic_by_commit, "systematic_by_commit")
            .insert(commit_seq, systematic_symbols.to_vec());
        self.total_systematic_bytes.fetch_add(
            u64::try_from(systematic_symbols.len()).map_err(|_| FrankenError::OutOfRange {
                what: "systematic_symbol_len".to_owned(),
                value: systematic_symbols.len().to_string(),
            })?,
            Ordering::Release,
        );
        Ok(())
    }

    fn sync_systematic_symbols(&self, _commit_seq: u64) -> Result<()> {
        self.systematic_syncs.fetch_add(1, Ordering::Release);
        Ok(())
    }

    fn append_repair_symbols(&self, commit_seq: u64, repair_symbols: &[u8]) -> Result<()> {
        lock_with_recovery(&self.repair_by_commit, "repair_by_commit")
            .insert(commit_seq, repair_symbols.to_vec());
        self.total_repair_bytes.fetch_add(
            u64::try_from(repair_symbols.len()).map_err(|_| FrankenError::OutOfRange {
                what: "repair_symbol_len".to_owned(),
                value: repair_symbols.len().to_string(),
            })?,
            Ordering::Release,
        );
        Ok(())
    }

    fn sync_repair_symbols(&self, _commit_seq: u64) -> Result<()> {
        self.repair_syncs.fetch_add(1, Ordering::Release);
        Ok(())
    }
}

/// Deterministic repair generator with configurable delay/failure injection.
#[derive(Debug)]
pub struct DeterministicRepairGenerator {
    delay: Duration,
    output_len: usize,
    fail_repair: Arc<AtomicBool>,
}

impl DeterministicRepairGenerator {
    #[must_use]
    pub fn new(delay: Duration, output_len: usize) -> Self {
        Self {
            delay,
            output_len: output_len.max(1),
            fail_repair: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn set_fail_repair(&self, fail: bool) {
        self.fail_repair.store(fail, Ordering::Release);
    }
}

impl RepairSymbolGenerator for DeterministicRepairGenerator {
    fn generate_repair_symbols(
        &self,
        commit_seq: u64,
        systematic_symbols: &[u8],
    ) -> Result<Vec<u8>> {
        if self.delay != Duration::ZERO {
            thread::sleep(self.delay);
        }
        if self.fail_repair.load(Ordering::Acquire) {
            return Err(FrankenError::Internal(format!(
                "repair generation failed for commit_seq={commit_seq}"
            )));
        }

        let source = if systematic_symbols.is_empty() {
            &[0_u8][..]
        } else {
            systematic_symbols
        };
        let mut state = commit_seq
            ^ u64::try_from(source.len()).map_err(|_| FrankenError::OutOfRange {
                what: "systematic_symbol_len".to_owned(),
                value: source.len().to_string(),
            })?;
        let mut out = Vec::with_capacity(self.output_len);
        for idx in 0..self.output_len {
            let src = source[idx % source.len()];
            let idx_mod = u64::try_from(idx % 251).map_err(|_| FrankenError::OutOfRange {
                what: "repair_symbol_index".to_owned(),
                value: idx.to_string(),
            })?;
            state = state.rotate_left(7) ^ u64::from(src) ^ idx_mod;
            out.push((state & 0xFF) as u8);
        }
        Ok(out)
    }
}

/// Two-phase commit durability coordinator.
pub struct CommitRepairCoordinator<
    IO: CommitRepairIo + Send + Sync + 'static,
    GEN: RepairSymbolGenerator + Send + Sync + 'static,
> {
    config: CommitRepairConfig,
    io: Arc<IO>,
    generator: Arc<GEN>,
    next_commit_seq: AtomicU64,
    next_async_task_id: AtomicU64,
    repair_states: Arc<Mutex<HashMap<u64, RepairState>>>,
    events: Arc<Mutex<Vec<CommitRepairEvent>>>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl<IO, GEN> CommitRepairCoordinator<IO, GEN>
where
    IO: CommitRepairIo + Send + Sync + 'static,
    GEN: RepairSymbolGenerator + Send + Sync + 'static,
{
    #[must_use]
    pub fn new(config: CommitRepairConfig, io: IO, generator: GEN) -> Self {
        Self::with_shared(config, Arc::new(io), Arc::new(generator))
    }

    #[must_use]
    pub fn with_shared(config: CommitRepairConfig, io: Arc<IO>, generator: Arc<GEN>) -> Self {
        Self {
            config,
            io,
            generator,
            next_commit_seq: AtomicU64::new(1),
            next_async_task_id: AtomicU64::new(1),
            repair_states: Arc::new(Mutex::new(HashMap::new())),
            events: Arc::new(Mutex::new(Vec::new())),
            handles: Mutex::new(Vec::new()),
        }
    }

    /// Execute critical-path durability and schedule async repair work.
    pub fn commit(&self, systematic_symbols: &[u8]) -> Result<CommitReceipt> {
        let started = Instant::now();
        let commit_seq = self.next_commit_seq.fetch_add(1, Ordering::Relaxed);

        self.io
            .append_systematic_symbols(commit_seq, systematic_symbols)?;
        self.io.sync_systematic_symbols(commit_seq)?;
        self.record(commit_seq, CommitRepairEventKind::CommitDurable);

        if !self.config.repair_enabled {
            self.record(commit_seq, CommitRepairEventKind::CommitAcked);
            return Ok(CommitReceipt {
                commit_seq,
                durable: true,
                repair_pending: false,
                latency: started.elapsed(),
            });
        }

        lock_with_recovery(&self.repair_states, "repair_states")
            .insert(commit_seq, RepairState::Pending);
        self.record(commit_seq, CommitRepairEventKind::DurableButNotRepairable);
        debug!(
            bead_id = BEAD_ID,
            commit_seq, "commit is durable but not repairable while async repair is pending"
        );
        self.record(commit_seq, CommitRepairEventKind::CommitAcked);

        let async_task_id = self.next_async_task_id.fetch_add(1, Ordering::Relaxed);
        let io = Arc::clone(&self.io);
        let generator = Arc::clone(&self.generator);
        let repair_states = Arc::clone(&self.repair_states);
        let events = Arc::clone(&self.events);
        let systematic_snapshot = systematic_symbols.to_vec();
        let handle = thread::spawn(move || {
            info!(
                bead_id = BEAD_ID,
                commit_seq, async_task_id, "repair symbols generation started"
            );
            record_event_into(&events, commit_seq, CommitRepairEventKind::RepairStarted);

            let repair_outcome =
                generator.generate_repair_symbols(commit_seq, &systematic_snapshot);
            match repair_outcome {
                Ok(repair_symbols) => {
                    let append_sync = io
                        .append_repair_symbols(commit_seq, &repair_symbols)
                        .and_then(|()| io.sync_repair_symbols(commit_seq));
                    match append_sync {
                        Ok(()) => {
                            set_repair_state(&repair_states, commit_seq, RepairState::Completed);
                            record_event_into(
                                &events,
                                commit_seq,
                                CommitRepairEventKind::RepairCompleted,
                            );
                            info!(
                                bead_id = BEAD_ID,
                                commit_seq,
                                async_task_id,
                                repair_symbol_bytes = repair_symbols.len(),
                                "repair symbols append+sync completed"
                            );
                        }
                        Err(err) => {
                            set_repair_state(&repair_states, commit_seq, RepairState::Failed);
                            record_event_into(
                                &events,
                                commit_seq,
                                CommitRepairEventKind::RepairFailed,
                            );
                            error!(
                                bead_id = BEAD_ID,
                                commit_seq,
                                async_task_id,
                                error = %err,
                                "repair symbol append/sync failed"
                            );
                        }
                    }
                }
                Err(err) => {
                    set_repair_state(&repair_states, commit_seq, RepairState::Failed);
                    record_event_into(&events, commit_seq, CommitRepairEventKind::RepairFailed);
                    error!(
                        bead_id = BEAD_ID,
                        commit_seq,
                        async_task_id,
                        error = %err,
                        "repair symbol generation failed"
                    );
                }
            }
        });
        lock_with_recovery(&self.handles, "repair_handles").push(handle);

        Ok(CommitReceipt {
            commit_seq,
            durable: true,
            repair_pending: true,
            latency: started.elapsed(),
        })
    }

    /// Join all currently scheduled background repair workers.
    pub fn wait_for_background_repair(&self) -> Result<()> {
        let mut handles = lock_with_recovery(&self.handles, "repair_handles");
        while let Some(handle) = handles.pop() {
            if handle.join().is_err() {
                return Err(FrankenError::Internal(
                    "background repair worker panicked".to_owned(),
                ));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn repair_state_for(&self, commit_seq: u64) -> RepairState {
        lock_with_recovery(&self.repair_states, "repair_states")
            .get(&commit_seq)
            .copied()
            .unwrap_or(RepairState::NotScheduled)
    }

    #[must_use]
    pub fn events_for_commit(&self, commit_seq: u64) -> Vec<CommitRepairEvent> {
        lock_with_recovery(&self.events, "repair_events")
            .iter()
            .copied()
            .filter(|event| event.commit_seq == commit_seq)
            .collect()
    }

    #[must_use]
    pub fn durable_not_repairable_window(&self, commit_seq: u64) -> Option<Duration> {
        let events = self.events_for_commit(commit_seq);
        let ack = events
            .iter()
            .find(|event| event.kind == CommitRepairEventKind::CommitAcked)?;
        let repair_done = events
            .iter()
            .find(|event| event.kind == CommitRepairEventKind::RepairCompleted)?;
        Some(repair_done.at.saturating_duration_since(ack.at))
    }

    #[must_use]
    pub fn io_handle(&self) -> Arc<IO> {
        Arc::clone(&self.io)
    }

    #[must_use]
    pub fn generator_handle(&self) -> Arc<GEN> {
        Arc::clone(&self.generator)
    }

    fn record(&self, commit_seq: u64, kind: CommitRepairEventKind) {
        record_event_into(&self.events, commit_seq, kind);
    }
}

impl<IO, GEN> Drop for CommitRepairCoordinator<IO, GEN>
where
    IO: CommitRepairIo + Send + Sync + 'static,
    GEN: RepairSymbolGenerator + Send + Sync + 'static,
{
    fn drop(&mut self) {
        let mut handles = lock_with_recovery(&self.handles, "repair_handles");
        while let Some(handle) = handles.pop() {
            if handle.join().is_err() {
                error!(
                    bead_id = BEAD_ID,
                    "background repair worker panicked during drop"
                );
            }
        }
    }
}

fn lock_with_recovery<'a, T>(mutex: &'a Mutex<T>, lock_name: &'static str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!(
                bead_id = BEAD_ID,
                lock = lock_name,
                "mutex poisoned; recovering inner state"
            );
            poisoned.into_inner()
        }
    }
}

fn set_repair_state(
    repair_states: &Arc<Mutex<HashMap<u64, RepairState>>>,
    commit_seq: u64,
    state: RepairState,
) {
    lock_with_recovery(repair_states, "repair_states").insert(commit_seq, state);
}

fn record_event_into(
    events: &Arc<Mutex<Vec<CommitRepairEvent>>>,
    commit_seq: u64,
    kind: CommitRepairEventKind,
) {
    lock_with_recovery(events, "repair_events").push(CommitRepairEvent {
        commit_seq,
        at: Instant::now(),
        kind,
    });
}
