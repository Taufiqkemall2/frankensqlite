//! Commit durability and asynchronous repair orchestration (§1.6, bd-22n.11).
//!
//! The critical path only appends+syncs systematic symbols. Repair symbols are
//! generated/append-synced asynchronously after commit acknowledgment.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use fsqlite_error::{FrankenError, Result};
use tracing::{debug, error, info, warn};

const BEAD_ID: &str = "bd-22n.11";

/// Default bounded capacity for commit-channel backpressure.
pub const DEFAULT_COMMIT_CHANNEL_CAPACITY: usize = 16;

/// Request sent from writers to the write coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRequest {
    pub txn_id: u64,
    pub write_set_pages: Vec<u32>,
    pub payload: Vec<u8>,
}

impl CommitRequest {
    #[must_use]
    pub fn new(txn_id: u64, write_set_pages: Vec<u32>, payload: Vec<u8>) -> Self {
        Self {
            txn_id,
            write_set_pages,
            payload,
        }
    }
}

/// Capacity/config knobs for the two-phase commit pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitPipelineConfig {
    pub channel_capacity: usize,
}

impl Default for CommitPipelineConfig {
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_COMMIT_CHANNEL_CAPACITY,
        }
    }
}

impl CommitPipelineConfig {
    /// Clamp PRAGMA capacity to a valid non-zero bounded channel size.
    #[must_use]
    pub fn from_pragma_capacity(raw_capacity: i64) -> Self {
        let clamped_i64 = raw_capacity.clamp(1, i64::from(u16::MAX));
        let clamped = usize::try_from(clamped_i64).expect("clamped to positive u16 range");
        Self {
            channel_capacity: clamped,
        }
    }
}

#[derive(Debug)]
struct TwoPhaseQueueState {
    capacity: usize,
    next_wait_ticket: u64,
    wait_queue: VecDeque<u64>,
    next_reservation_seq: u64,
    next_receive_seq: u64,
    open_reservations: BTreeSet<u64>,
    aborted_reservations: BTreeSet<u64>,
    pending_commits: BTreeMap<u64, CommitRequest>,
    ready_commits: VecDeque<CommitRequest>,
}

impl TwoPhaseQueueState {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            next_wait_ticket: 1,
            wait_queue: VecDeque::new(),
            next_reservation_seq: 1,
            next_receive_seq: 1,
            open_reservations: BTreeSet::new(),
            aborted_reservations: BTreeSet::new(),
            pending_commits: BTreeMap::new(),
            ready_commits: VecDeque::new(),
        }
    }

    fn occupancy(&self) -> usize {
        self.open_reservations.len() + self.pending_commits.len() + self.ready_commits.len()
    }

    fn can_admit(&self, ticket: u64) -> bool {
        self.wait_queue.front().copied() == Some(ticket) && self.occupancy() < self.capacity
    }

    fn reserve_slot(&mut self) -> u64 {
        let seq = self.next_reservation_seq;
        self.next_reservation_seq = self.next_reservation_seq.saturating_add(1);
        self.open_reservations.insert(seq);
        seq
    }

    fn close_reservation_with_send(&mut self, reservation_seq: u64, request: CommitRequest) {
        if self.open_reservations.remove(&reservation_seq) {
            self.pending_commits.insert(reservation_seq, request);
        }
        self.promote_ready();
    }

    fn close_reservation_with_abort(&mut self, reservation_seq: u64) {
        if self.open_reservations.remove(&reservation_seq) {
            self.aborted_reservations.insert(reservation_seq);
        }
        self.promote_ready();
    }

    fn remove_wait_ticket(&mut self, ticket: u64) {
        if let Some(pos) = self.wait_queue.iter().position(|queued| *queued == ticket) {
            let _ = self.wait_queue.remove(pos);
        }
    }

    fn promote_ready(&mut self) {
        loop {
            if self.aborted_reservations.remove(&self.next_receive_seq) {
                self.next_receive_seq = self.next_receive_seq.saturating_add(1);
                continue;
            }
            let Some(request) = self.pending_commits.remove(&self.next_receive_seq) else {
                break;
            };
            self.ready_commits.push_back(request);
            self.next_receive_seq = self.next_receive_seq.saturating_add(1);
        }
    }
}

#[derive(Debug)]
struct TwoPhaseQueueShared {
    state: Mutex<TwoPhaseQueueState>,
    cv: Condvar,
}

impl TwoPhaseQueueShared {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            state: Mutex::new(TwoPhaseQueueState::new(capacity)),
            cv: Condvar::new(),
        }
    }
}

/// Sender side of the two-phase bounded MPSC commit channel.
#[derive(Debug, Clone)]
pub struct TwoPhaseCommitSender {
    shared: Arc<TwoPhaseQueueShared>,
}

impl TwoPhaseCommitSender {
    /// Reserve a slot (phase 1). Blocks when channel is saturated.
    pub fn reserve(&self) -> SendPermit {
        loop {
            if let Some(permit) = self.try_reserve_for(Duration::from_secs(3600)) {
                return permit;
            }
        }
    }

    /// Reserve with timeout; `None` means caller gave up (cancel during reserve).
    #[must_use]
    pub fn try_reserve_for(&self, timeout: Duration) -> Option<SendPermit> {
        let start = Instant::now();
        let mut guard = lock_with_recovery(&self.shared.state, "two_phase_state");
        let ticket = guard.next_wait_ticket;
        guard.next_wait_ticket = guard.next_wait_ticket.saturating_add(1);
        guard.wait_queue.push_back(ticket);

        loop {
            if guard.can_admit(ticket) {
                let _ = guard.wait_queue.pop_front();
                let reservation_seq = guard.reserve_slot();
                self.shared.cv.notify_all();
                return Some(SendPermit {
                    shared: Arc::clone(&self.shared),
                    reservation_seq: Some(reservation_seq),
                });
            }

            let elapsed = start.elapsed();
            let Some(remaining) = timeout.checked_sub(elapsed) else {
                guard.remove_wait_ticket(ticket);
                self.shared.cv.notify_all();
                return None;
            };

            let waited = self.shared.cv.wait_timeout(guard, remaining);
            guard = match waited {
                Ok((next, result)) => {
                    if result.timed_out() {
                        let mut timed_out_guard = next;
                        timed_out_guard.remove_wait_ticket(ticket);
                        self.shared.cv.notify_all();
                        return None;
                    }
                    next
                }
                Err(poisoned) => poisoned.into_inner().0,
            };
        }
    }

    /// Current buffered + reserved occupancy.
    #[must_use]
    pub fn occupancy(&self) -> usize {
        lock_with_recovery(&self.shared.state, "two_phase_state").occupancy()
    }

    /// Bounded channel capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        lock_with_recovery(&self.shared.state, "two_phase_state").capacity
    }
}

/// Receiver side of the two-phase bounded MPSC commit channel.
#[derive(Debug, Clone)]
pub struct TwoPhaseCommitReceiver {
    shared: Arc<TwoPhaseQueueShared>,
}

impl TwoPhaseCommitReceiver {
    /// Receive the next coordinator request (FIFO by reservation order).
    pub fn recv(&self) -> CommitRequest {
        loop {
            if let Some(request) = self.try_recv_for(Duration::from_secs(3600)) {
                return request;
            }
        }
    }

    /// Timed receive used by tests and bounded coordinator loops.
    #[must_use]
    pub fn try_recv_for(&self, timeout: Duration) -> Option<CommitRequest> {
        let start = Instant::now();
        let mut guard = lock_with_recovery(&self.shared.state, "two_phase_state");
        loop {
            if let Some(request) = guard.ready_commits.pop_front() {
                self.shared.cv.notify_all();
                return Some(request);
            }

            let elapsed = start.elapsed();
            let remaining = timeout.checked_sub(elapsed)?;

            let waited = self.shared.cv.wait_timeout(guard, remaining);
            guard = match waited {
                Ok((next, result)) => {
                    if result.timed_out() {
                        return None;
                    }
                    next
                }
                Err(poisoned) => poisoned.into_inner().0,
            };
        }
    }
}

/// Two-phase permit returned by `reserve()`.
///
/// Dropping without `send()`/`abort()` automatically releases the reserved slot.
#[derive(Debug)]
pub struct SendPermit {
    shared: Arc<TwoPhaseQueueShared>,
    reservation_seq: Option<u64>,
}

impl SendPermit {
    /// Stable reservation sequence used to verify FIFO behavior in tests.
    #[must_use]
    pub fn reservation_seq(&self) -> u64 {
        self.reservation_seq.unwrap_or(0)
    }

    /// Phase 2 commit. Synchronous and infallible for slot ownership.
    pub fn send(mut self, request: CommitRequest) {
        if let Some(reservation_seq) = self.reservation_seq.take() {
            {
                let mut guard = lock_with_recovery(&self.shared.state, "two_phase_state");
                guard.close_reservation_with_send(reservation_seq, request);
            }
            self.shared.cv.notify_all();
        }
    }

    /// Explicitly release reserved slot without sending.
    pub fn abort(mut self) {
        if let Some(reservation_seq) = self.reservation_seq.take() {
            {
                let mut guard = lock_with_recovery(&self.shared.state, "two_phase_state");
                guard.close_reservation_with_abort(reservation_seq);
            }
            self.shared.cv.notify_all();
        }
    }
}

impl Drop for SendPermit {
    fn drop(&mut self) {
        if let Some(reservation_seq) = self.reservation_seq.take() {
            {
                let mut guard = lock_with_recovery(&self.shared.state, "two_phase_state");
                guard.close_reservation_with_abort(reservation_seq);
            }
            self.shared.cv.notify_all();
        }
    }
}

/// Tracked sender variant that counts leaked permits (dropped without send/abort).
#[derive(Debug, Clone)]
pub struct TrackedSender {
    sender: TwoPhaseCommitSender,
    leaked_permits: Arc<AtomicU64>,
}

impl TrackedSender {
    #[must_use]
    pub fn new(sender: TwoPhaseCommitSender) -> Self {
        Self {
            sender,
            leaked_permits: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn reserve(&self) -> TrackedSendPermit {
        TrackedSendPermit {
            leaked_permits: Arc::clone(&self.leaked_permits),
            permit: Some(self.sender.reserve()),
        }
    }

    #[must_use]
    pub fn leaked_permit_count(&self) -> u64 {
        self.leaked_permits.load(Ordering::Acquire)
    }
}

/// Tracked permit wrapper for safety-critical channels.
#[derive(Debug)]
pub struct TrackedSendPermit {
    leaked_permits: Arc<AtomicU64>,
    permit: Option<SendPermit>,
}

impl TrackedSendPermit {
    /// Commit and clear obligation.
    pub fn send(mut self, request: CommitRequest) {
        if let Some(permit) = self.permit.take() {
            permit.send(request);
        }
    }

    /// Abort and clear obligation.
    pub fn abort(mut self) {
        if let Some(permit) = self.permit.take() {
            permit.abort();
        }
    }
}

impl Drop for TrackedSendPermit {
    fn drop(&mut self) {
        if self.permit.is_some() {
            self.leaked_permits.fetch_add(1, Ordering::AcqRel);
        }
    }
}

/// Build a bounded two-phase commit channel.
#[must_use]
pub fn two_phase_commit_channel(capacity: usize) -> (TwoPhaseCommitSender, TwoPhaseCommitReceiver) {
    let shared = Arc::new(TwoPhaseQueueShared::with_capacity(capacity));
    (
        TwoPhaseCommitSender {
            shared: Arc::clone(&shared),
        },
        TwoPhaseCommitReceiver { shared },
    )
}

/// Little's-law-based capacity estimate.
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
pub fn little_law_capacity(
    lambda_per_second: f64,
    t_commit: Duration,
    burst_multiplier: f64,
    jitter_multiplier: f64,
) -> usize {
    let effective = lambda_per_second
        * t_commit.as_secs_f64()
        * burst_multiplier.max(1.0)
        * jitter_multiplier.max(1.0);
    effective.ceil().max(1.0) as usize
}

/// Classical optimal group-commit batch size: `sqrt(t_fsync / t_validate)`.
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
pub fn optimal_batch_size(t_fsync: Duration, t_validate: Duration, capacity: usize) -> usize {
    let denom = t_validate.as_secs_f64().max(f64::EPSILON);
    let raw = (t_fsync.as_secs_f64() / denom).sqrt().round();
    raw.clamp(1.0, capacity.max(1) as f64) as usize
}

/// Conformal batch-size controller using upper quantiles.
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
pub fn conformal_batch_size(
    fsync_samples: &[Duration],
    validate_samples: &[Duration],
    capacity: usize,
) -> usize {
    if fsync_samples.is_empty() || validate_samples.is_empty() {
        return 1;
    }
    let q_fsync = quantile_seconds(fsync_samples, 0.9);
    let q_validate = quantile_seconds(validate_samples, 0.9).max(f64::EPSILON);
    let raw = (q_fsync / q_validate).sqrt().round();
    raw.clamp(1.0, capacity.max(1) as f64) as usize
}

fn quantile_seconds(samples: &[Duration], quantile: f64) -> f64 {
    let mut values: Vec<f64> = samples.iter().map(Duration::as_secs_f64).collect();
    values.sort_by(f64::total_cmp);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let idx = ((values.len() as f64 - 1.0) * quantile.clamp(0.0, 1.0)).round() as usize;
    values[idx]
}

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

#[cfg(test)]
mod two_phase_pipeline_tests {
    use super::*;
    use std::sync::mpsc as std_mpsc;
    use std::thread;

    fn request(txn_id: u64) -> CommitRequest {
        CommitRequest::new(
            txn_id,
            vec![u32::try_from(txn_id % 97).expect("txn id modulo fits in u32")],
            vec![u8::try_from(txn_id & 0xFF).expect("masked to u8")],
        )
    }

    #[test]
    fn test_two_phase_reserve_then_send() {
        let (sender, receiver) = two_phase_commit_channel(4);
        let permit = sender.reserve();
        let seq = permit.reservation_seq();
        permit.send(request(seq));
        let observed_request = receiver.try_recv_for(Duration::from_millis(50));
        assert_eq!(observed_request, Some(request(seq)));
    }

    #[test]
    fn test_two_phase_cancel_during_reserve() {
        let (sender, _receiver) = two_phase_commit_channel(1);
        let blocker = sender.reserve();
        let attempt = sender.try_reserve_for(Duration::from_millis(5));
        assert!(
            attempt.is_none(),
            "reserve timeout acts as cancellation during reserve"
        );
        assert_eq!(sender.occupancy(), 1, "no extra slot consumed");
        drop(blocker);
        let permit = sender.try_reserve_for(Duration::from_millis(50));
        assert!(permit.is_some(), "slot released after blocker drop");
    }

    #[test]
    fn test_two_phase_drop_permit_releases_slot() {
        let (sender, _receiver) = two_phase_commit_channel(1);
        let permit = sender.reserve();
        assert_eq!(sender.occupancy(), 1);
        drop(permit);
        assert_eq!(sender.occupancy(), 0);
        let retry = sender.try_reserve_for(Duration::from_millis(50));
        assert!(retry.is_some(), "dropped permit must release capacity");
    }

    #[test]
    fn test_backpressure_blocks_at_capacity() {
        let (sender, _receiver) = two_phase_commit_channel(2);
        let permit_a = sender.reserve();
        let permit_b = sender.reserve();

        let (tx, rx) = std_mpsc::channel();
        let join = thread::spawn(move || {
            let started = Instant::now();
            let permit = sender.reserve();
            let elapsed = started.elapsed();
            tx.send(elapsed)
                .expect("elapsed send should succeed for backpressure test");
            permit
        });

        thread::sleep(Duration::from_millis(30));
        drop(permit_a);
        drop(permit_b);

        let elapsed = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocked reserve should eventually unblock");
        assert!(
            elapsed >= Duration::from_millis(20),
            "reserve should block until capacity frees"
        );
        let _permit = join.join().expect("thread join must succeed");
    }

    #[test]
    fn test_fifo_ordering_under_contention() {
        let total = 100_u64;
        let (sender, receiver) = two_phase_commit_channel(32);
        let mut joins = Vec::new();
        for _ in 0..10 {
            let sender_clone = sender.clone();
            joins.push(thread::spawn(move || {
                let mut local = Vec::new();
                for _ in 0..10 {
                    let permit = sender_clone.reserve();
                    let seq = permit.reservation_seq();
                    permit.send(request(seq));
                    local.push(seq);
                }
                local
            }));
        }

        let mut observed_order = Vec::new();
        for _ in 0..total {
            let req = receiver
                .try_recv_for(Duration::from_secs(1))
                .expect("coordinator should receive queued request");
            observed_order.push(req.txn_id);
        }
        for join in joins {
            let _ = join.join().expect("producer join");
        }

        let expected: Vec<u64> = (1..=total).collect();
        assert_eq!(observed_order, expected, "must preserve FIFO reserve order");
    }

    #[test]
    fn test_tracked_sender_detects_leaked_permit() {
        let (sender, _receiver) = two_phase_commit_channel(4);
        let tracked = TrackedSender::new(sender.clone());

        {
            let _leaked = tracked.reserve();
        }

        assert_eq!(tracked.leaked_permit_count(), 1);
        let permit = sender.try_reserve_for(Duration::from_millis(50));
        assert!(
            permit.is_some(),
            "leaked tracked permit still releases slot via underlying drop"
        );
    }

    #[test]
    fn test_group_commit_batch_size_near_optimal() {
        let capacity = DEFAULT_COMMIT_CHANNEL_CAPACITY;
        let n_opt =
            optimal_batch_size(Duration::from_millis(2), Duration::from_micros(5), capacity);
        assert_eq!(n_opt, capacity, "20 theoretical optimum clamps to C=16");

        let (sender, receiver) = two_phase_commit_channel(capacity);
        for txn_id in 0_u64..u64::try_from(capacity).expect("capacity fits u64") {
            let permit = sender.reserve();
            permit.send(request(txn_id));
        }
        let mut drained = 0_usize;
        while drained < capacity {
            if receiver.try_recv_for(Duration::from_millis(20)).is_some() {
                drained += 1;
            }
        }
        assert_eq!(drained, capacity, "coordinator drains full batch at C");
    }

    #[test]
    fn test_conformal_batch_size_adapts_to_regime() {
        let cap = 64;
        let low_fsync: Vec<Duration> = (0..32).map(|_| Duration::from_millis(2)).collect();
        let high_fsync: Vec<Duration> = (0..32).map(|_| Duration::from_millis(10)).collect();
        let validate: Vec<Duration> = (0..32).map(|_| Duration::from_micros(5)).collect();

        let low = conformal_batch_size(&low_fsync, &validate, cap);
        let high = conformal_batch_size(&high_fsync, &validate, cap);

        assert!(
            high > low,
            "regime shift to slower fsync must increase batch"
        );
        assert!(high <= cap);
        assert!(low >= 1);
    }

    #[test]
    fn test_channel_capacity_16_default() {
        assert_eq!(CommitPipelineConfig::default().channel_capacity, 16);
    }

    #[test]
    fn test_capacity_configurable_via_pragma() {
        assert_eq!(
            CommitPipelineConfig::from_pragma_capacity(32).channel_capacity,
            32
        );
        assert_eq!(
            CommitPipelineConfig::from_pragma_capacity(0).channel_capacity,
            1
        );
    }

    #[test]
    fn test_little_law_derivation() {
        let burst_capacity = little_law_capacity(37_000.0, Duration::from_micros(40), 4.0, 2.5);
        assert_eq!(burst_capacity, 15);
        assert_eq!(DEFAULT_COMMIT_CHANNEL_CAPACITY, 16);
    }
}
