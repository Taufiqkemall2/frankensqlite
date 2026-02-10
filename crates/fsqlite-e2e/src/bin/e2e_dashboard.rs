//! E2E dashboard binary — TUI for running and visualizing E2E test results.
//!
//! Implements three rich visualization panels (bd-mmhw, bd-s4qy, bd-1nqt):
//! - **Benchmark** panel: real-time throughput sparkline, speedup ratio, progress bar
//! - **Recovery** panel: hex diff of corrupted/recovered bytes, decode progress
//! - **Correctness** panel: SHA-256 comparison table, per-workload pass/fail
//! - **Summary** panel: scrollable event log
//!
//! Architecture:
//! - `ftui` (FrankenTUI) runtime with Model/View/Update pattern
//! - mpsc channel feeds background progress into the UI
//! - `--headless` mode for CI / non-terminal environments

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use ftui::core::geometry::Rect;
use ftui::widgets::Widget;
use ftui::widgets::panel::Panel;
use ftui::widgets::paragraph::Paragraph;
use ftui::widgets::progress::ProgressBar;
use ftui::widgets::sparkline::Sparkline;
use ftui::{App, Cmd, Event, KeyCode, KeyEventKind, Model, PackedRgba, ScreenMode, Style};

// ── Dashboard events (contract between background worker and TUI) ────────

/// Events sent from background threads to the dashboard UI via mpsc channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum DashboardEvent {
    // ── Benchmark events ─────────────────────────────────────────────
    /// Periodic throughput update for FrankenSQLite.
    BenchmarkProgress {
        name: String,
        ops_per_sec: f64,
        elapsed_ms: u64,
    },
    /// Periodic throughput update for C SQLite baseline.
    BenchmarkCsqliteProgress {
        name: String,
        ops_per_sec: f64,
        elapsed_ms: u64,
    },
    /// A single benchmark run completed.
    BenchmarkComplete {
        name: String,
        wall_time_ms: u64,
        ops_per_sec: f64,
    },
    /// Overall benchmark suite progress.
    BenchmarkSuiteProgress { completed: usize, total: usize },

    // ── Corruption recovery events ───────────────────────────────────
    /// Corruption was injected into a page.
    CorruptionInjected { page: u32, pattern: String },
    /// Hex dump of original bytes before corruption (first N bytes).
    CorruptionHexData {
        original_bytes: Vec<u8>,
        corrupted_bytes: Vec<u8>,
        page_offset: u64,
    },
    /// RaptorQ recovery phase update.
    RecoveryAttempt {
        group: u32,
        symbols_available: u32,
        needed: u32,
    },
    /// Recovery decode phase progress.
    RecoveryPhaseUpdate {
        phase: String,
        symbols_resolved: u32,
    },
    /// Recovery succeeded with hex proof.
    RecoverySuccess { page: u32, decode_proof: String },
    /// Recovered bytes for hex comparison.
    RecoveryHexData { recovered_bytes: Vec<u8> },
    /// Recovery failed.
    RecoveryFailure { page: u32, reason: String },
    /// C SQLite integrity check result after corruption.
    CsqliteIntegrityResult { passed: bool, message: String },

    // ── Correctness verification events ──────────────────────────────
    /// A new correctness workload is starting.
    CorrectnessWorkloadStart { workload: String, total_ops: usize },
    /// Progress within a correctness workload.
    CorrectnessOpProgress {
        workload: String,
        ops_done: usize,
        total_ops: usize,
        current_sql: String,
    },
    /// A correctness workload completed with hash comparison.
    CorrectnessCheck {
        workload: String,
        frank_hash: String,
        csqlite_hash: String,
        matched: bool,
    },

    // ── General ──────────────────────────────────────────────────────
    /// Freeform status message for the log.
    StatusMessage { message: String },
}

// ── Panel navigation ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PanelId {
    Benchmark,
    Recovery,
    Correctness,
    Summary,
}

impl PanelId {
    const fn title(self) -> &'static str {
        match self {
            Self::Benchmark => "Benchmark",
            Self::Recovery => "Recovery",
            Self::Correctness => "Correctness",
            Self::Summary => "Summary",
        }
    }

    const fn next(self) -> Self {
        match self {
            Self::Benchmark => Self::Recovery,
            Self::Recovery => Self::Correctness,
            Self::Correctness => Self::Summary,
            Self::Summary => Self::Benchmark,
        }
    }

    const fn prev(self) -> Self {
        match self {
            Self::Benchmark => Self::Summary,
            Self::Recovery => Self::Benchmark,
            Self::Correctness => Self::Recovery,
            Self::Summary => Self::Correctness,
        }
    }
}

// ── Messages ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Msg {
    Tick,
    Quit,
    NextPanel,
    PrevPanel,
    Restart,
}

impl From<Event> for Msg {
    fn from(e: Event) -> Self {
        match e {
            Event::Key(k) if k.kind == KeyEventKind::Press && k.is_char('q') => Self::Quit,
            Event::Key(k) if k.kind == KeyEventKind::Press && k.is_char('r') => Self::Restart,
            Event::Key(k)
                if k.kind == KeyEventKind::Press && k.code == KeyCode::Tab && !k.shift() =>
            {
                Self::NextPanel
            }
            Event::Key(k)
                if k.kind == KeyEventKind::Press
                    && (k.code == KeyCode::BackTab || (k.code == KeyCode::Tab && k.shift())) =>
            {
                Self::PrevPanel
            }
            _ => Self::Tick,
        }
    }
}

// ── State types ───────────────────────────────────────────────────────────

/// Maximum throughput history samples for the sparkline.
const MAX_SPARKLINE_SAMPLES: usize = 60;

/// Maximum hex dump display bytes per panel.
const HEX_DISPLAY_BYTES: usize = 64;

/// Maximum recent SQL lines in correctness log.
const MAX_RECENT_SQL: usize = 5;

/// Benchmark panel state: throughput history, comparison, suite progress.
#[derive(Debug, Clone)]
struct BenchState {
    name: String,
    ops_per_sec: f64,
    elapsed_ms: u64,
    done: bool,
    /// FrankenSQLite throughput history for sparkline.
    frank_history: VecDeque<f64>,
    /// C SQLite throughput history for sparkline.
    csqlite_history: VecDeque<f64>,
    /// Latest C SQLite throughput for speedup calculation.
    csqlite_ops_per_sec: Option<f64>,
    /// Suite-level progress.
    suite_completed: usize,
    suite_total: usize,
}

impl BenchState {
    fn new(name: String, ops_per_sec: f64, elapsed_ms: u64) -> Self {
        let mut frank_history = VecDeque::with_capacity(MAX_SPARKLINE_SAMPLES);
        frank_history.push_back(ops_per_sec);
        Self {
            name,
            ops_per_sec,
            elapsed_ms,
            done: false,
            frank_history,
            csqlite_history: VecDeque::with_capacity(MAX_SPARKLINE_SAMPLES),
            csqlite_ops_per_sec: None,
            suite_completed: 0,
            suite_total: 0,
        }
    }

    fn push_frank_sample(&mut self, ops: f64) {
        if self.frank_history.len() >= MAX_SPARKLINE_SAMPLES {
            self.frank_history.pop_front();
        }
        self.frank_history.push_back(ops);
    }

    fn push_csqlite_sample(&mut self, ops: f64) {
        if self.csqlite_history.len() >= MAX_SPARKLINE_SAMPLES {
            self.csqlite_history.pop_front();
        }
        self.csqlite_history.push_back(ops);
    }
}

/// Recovery panel state: corruption hex data, recovery progress, outcome.
#[derive(Debug, Clone)]
struct RecoveryState {
    /// Corrupted page number.
    page: u32,
    /// Corruption pattern description.
    pattern: String,
    /// Original bytes before corruption (first N bytes).
    original_bytes: Vec<u8>,
    /// Corrupted bytes (first N bytes).
    corrupted_bytes: Vec<u8>,
    /// Recovered bytes after RaptorQ decode (first N bytes).
    recovered_bytes: Vec<u8>,
    /// Page file offset.
    page_offset: u64,
    /// RaptorQ group being decoded.
    recovery_group: Option<u32>,
    /// Available symbols for decode.
    symbols_available: u32,
    /// Required symbols for decode.
    symbols_needed: u32,
    /// Current decode phase description.
    current_phase: String,
    /// Symbols resolved so far in current phase.
    phase_symbols_resolved: u32,
    /// Whether recovery succeeded.
    succeeded: Option<bool>,
    /// Decode proof / reason string.
    verdict: String,
    /// C SQLite integrity check outcome.
    csqlite_integrity_passed: Option<bool>,
    csqlite_integrity_message: String,
    /// Step-by-step log.
    steps: Vec<String>,
}

impl RecoveryState {
    fn new(page: u32, pattern: &str) -> Self {
        let steps = vec![format!("Corruption injected: page {page} ({pattern})")];
        Self {
            page,
            pattern: pattern.to_owned(),
            original_bytes: Vec::new(),
            corrupted_bytes: Vec::new(),
            recovered_bytes: Vec::new(),
            page_offset: u64::from(page.saturating_sub(1)) * 4096,
            recovery_group: None,
            symbols_available: 0,
            symbols_needed: 0,
            current_phase: String::new(),
            phase_symbols_resolved: 0,
            succeeded: None,
            verdict: String::new(),
            csqlite_integrity_passed: None,
            csqlite_integrity_message: String::new(),
            steps,
        }
    }
}

/// A single correctness workload result.
#[derive(Debug, Clone)]
struct WorkloadResult {
    workload: String,
    frank_hash: String,
    csqlite_hash: String,
    matched: bool,
}

/// Correctness panel state: multiple workload results, progress, recent SQL.
#[derive(Debug, Clone)]
struct CorrectnessCheckState {
    /// Completed workload results.
    results: Vec<WorkloadResult>,
    /// Currently running workload name.
    current_workload: Option<String>,
    /// Operations completed in current workload.
    ops_done: usize,
    /// Total operations in current workload.
    ops_total: usize,
    /// Recent SQL statements executed.
    recent_sql: VecDeque<String>,
}

impl Default for CorrectnessCheckState {
    fn default() -> Self {
        Self {
            results: Vec::new(),
            current_workload: None,
            ops_done: 0,
            ops_total: 0,
            recent_sql: VecDeque::with_capacity(MAX_RECENT_SQL),
        }
    }
}

impl CorrectnessCheckState {
    fn push_sql(&mut self, sql: String) {
        if self.recent_sql.len() >= MAX_RECENT_SQL {
            self.recent_sql.pop_front();
        }
        self.recent_sql.push_back(sql);
    }
}

// ── Dashboard model ───────────────────────────────────────────────────────

struct DashboardModel {
    active: PanelId,
    rx: mpsc::Receiver<DashboardEvent>,
    stop: Arc<AtomicBool>,
    log: VecDeque<String>,
    bench: Option<BenchState>,
    recovery: Option<RecoveryState>,
    correctness: CorrectnessCheckState,
}

impl DashboardModel {
    fn new(rx: mpsc::Receiver<DashboardEvent>, stop: Arc<AtomicBool>) -> Self {
        Self {
            active: PanelId::Benchmark,
            rx,
            stop,
            log: VecDeque::new(),
            bench: None,
            recovery: None,
            correctness: CorrectnessCheckState::default(),
        }
    }

    fn push_log(&mut self, line: impl Into<String>) {
        const MAX: usize = 50;
        if self.log.len() >= MAX {
            self.log.pop_front();
        }
        self.log.push_back(line.into());
    }

    fn clear(&mut self) {
        self.log.clear();
        self.bench = None;
        self.recovery = None;
        self.correctness = CorrectnessCheckState::default();
        self.push_log("cleared state");
    }

    #[allow(clippy::too_many_lines)]
    fn drain_events(&mut self) {
        while let Ok(ev) = self.rx.try_recv() {
            match ev {
                // ── Benchmark events ─────────────────────────────────
                DashboardEvent::BenchmarkProgress {
                    name,
                    ops_per_sec,
                    elapsed_ms,
                } => {
                    if let Some(ref mut b) = self.bench {
                        b.name.clone_from(&name);
                        b.ops_per_sec = ops_per_sec;
                        b.elapsed_ms = elapsed_ms;
                        b.done = false;
                        b.push_frank_sample(ops_per_sec);
                    } else {
                        self.bench = Some(BenchState::new(name.clone(), ops_per_sec, elapsed_ms));
                    }
                    self.push_log(format!(
                        "bench {name}: {ops_per_sec:.0} ops/s @ {elapsed_ms}ms"
                    ));
                }
                DashboardEvent::BenchmarkCsqliteProgress {
                    name,
                    ops_per_sec,
                    elapsed_ms,
                } => {
                    if let Some(ref mut b) = self.bench {
                        b.csqlite_ops_per_sec = Some(ops_per_sec);
                        b.push_csqlite_sample(ops_per_sec);
                    } else {
                        let mut state = BenchState::new(name.clone(), 0.0, elapsed_ms);
                        state.csqlite_ops_per_sec = Some(ops_per_sec);
                        state.push_csqlite_sample(ops_per_sec);
                        self.bench = Some(state);
                    }
                    self.push_log(format!(
                        "bench {name} (csqlite): {ops_per_sec:.0} ops/s @ {elapsed_ms}ms"
                    ));
                }
                DashboardEvent::BenchmarkComplete {
                    name,
                    wall_time_ms,
                    ops_per_sec,
                } => {
                    if let Some(ref mut b) = self.bench {
                        b.name.clone_from(&name);
                        b.ops_per_sec = ops_per_sec;
                        b.elapsed_ms = wall_time_ms;
                        b.done = true;
                        b.push_frank_sample(ops_per_sec);
                    } else {
                        let mut state = BenchState::new(name.clone(), ops_per_sec, wall_time_ms);
                        state.done = true;
                        self.bench = Some(state);
                    }
                    self.push_log(format!(
                        "bench {name}: DONE {ops_per_sec:.0} ops/s ({wall_time_ms}ms)"
                    ));
                }
                DashboardEvent::BenchmarkSuiteProgress { completed, total } => {
                    if let Some(ref mut b) = self.bench {
                        b.suite_completed = completed;
                        b.suite_total = total;
                    }
                    self.push_log(format!("suite: {completed}/{total}"));
                }

                // ── Recovery events ──────────────────────────────────
                DashboardEvent::CorruptionInjected { page, pattern } => {
                    self.recovery = Some(RecoveryState::new(page, &pattern));
                    self.push_log(format!("corrupt: page={page} ({pattern})"));
                }
                DashboardEvent::CorruptionHexData {
                    original_bytes,
                    corrupted_bytes,
                    page_offset,
                } => {
                    if let Some(ref mut r) = self.recovery {
                        r.original_bytes = original_bytes;
                        r.corrupted_bytes = corrupted_bytes;
                        r.page_offset = page_offset;
                        r.steps.push("Hex data captured".to_owned());
                    }
                }
                DashboardEvent::RecoveryAttempt {
                    group,
                    symbols_available,
                    needed,
                } => {
                    if let Some(ref mut r) = self.recovery {
                        r.recovery_group = Some(group);
                        r.symbols_available = symbols_available;
                        r.symbols_needed = needed;
                        r.steps.push(format!(
                            "Recovery: group={group} symbols={symbols_available}/{needed}"
                        ));
                    }
                    self.push_log(format!(
                        "recover: group={group} symbols={symbols_available}/{needed}"
                    ));
                }
                DashboardEvent::RecoveryPhaseUpdate {
                    phase,
                    symbols_resolved,
                } => {
                    if let Some(ref mut r) = self.recovery {
                        r.current_phase.clone_from(&phase);
                        r.phase_symbols_resolved = symbols_resolved;
                        r.steps.push(format!(
                            "Phase {phase}: {symbols_resolved} symbols resolved"
                        ));
                    }
                }
                DashboardEvent::RecoverySuccess { page, decode_proof } => {
                    if let Some(ref mut r) = self.recovery {
                        r.succeeded = Some(true);
                        r.verdict.clone_from(&decode_proof);
                        r.steps.push(format!("Page {page} RECOVERED"));
                    }
                    self.push_log(format!("recover: OK page={page}"));
                }
                DashboardEvent::RecoveryHexData { recovered_bytes } => {
                    if let Some(ref mut r) = self.recovery {
                        r.recovered_bytes = recovered_bytes;
                    }
                }
                DashboardEvent::RecoveryFailure { page, reason } => {
                    if let Some(ref mut r) = self.recovery {
                        r.succeeded = Some(false);
                        r.verdict.clone_from(&reason);
                        r.steps.push(format!("FAILED: page={page} ({reason})"));
                    }
                    self.push_log(format!("recover: FAIL page={page} ({reason})"));
                }
                DashboardEvent::CsqliteIntegrityResult { passed, message } => {
                    if let Some(ref mut r) = self.recovery {
                        r.csqlite_integrity_passed = Some(passed);
                        r.csqlite_integrity_message.clone_from(&message);
                        r.steps.push(format!(
                            "C SQLite: {}",
                            if passed {
                                "integrity OK"
                            } else {
                                "INTEGRITY FAILED"
                            }
                        ));
                    }
                }

                // ── Correctness events ───────────────────────────────
                DashboardEvent::CorrectnessWorkloadStart {
                    workload,
                    total_ops,
                } => {
                    self.correctness.current_workload = Some(workload.clone());
                    self.correctness.ops_done = 0;
                    self.correctness.ops_total = total_ops;
                    self.correctness.recent_sql.clear();
                    self.push_log(format!("correctness: start {workload} ({total_ops} ops)"));
                }
                DashboardEvent::CorrectnessOpProgress {
                    workload: _,
                    ops_done,
                    total_ops,
                    current_sql,
                } => {
                    self.correctness.ops_done = ops_done;
                    self.correctness.ops_total = total_ops;
                    self.correctness.push_sql(current_sql);
                }
                DashboardEvent::CorrectnessCheck {
                    workload,
                    frank_hash,
                    csqlite_hash,
                    matched,
                } => {
                    self.correctness.results.push(WorkloadResult {
                        workload: workload.clone(),
                        frank_hash: frank_hash.clone(),
                        csqlite_hash: csqlite_hash.clone(),
                        matched,
                    });
                    self.correctness.current_workload = None;
                    self.correctness.ops_done = 0;
                    self.correctness.ops_total = 0;
                    self.push_log(format!(
                        "check {workload}: {}",
                        if matched { "MATCH" } else { "MISMATCH" }
                    ));
                }

                // ── General ──────────────────────────────────────────
                DashboardEvent::StatusMessage { message } => {
                    self.push_log(format!("status: {message}"));
                }
            }
        }
    }
}

// ── Model implementation ──────────────────────────────────────────────────

impl Model for DashboardModel {
    type Message = Msg;

    fn init(&mut self) -> Cmd<Self::Message> {
        Cmd::tick(Duration::from_millis(50))
    }

    fn update(&mut self, msg: Self::Message) -> Cmd<Self::Message> {
        match msg {
            Msg::Tick => {
                self.drain_events();
                Cmd::none()
            }
            Msg::Quit => {
                self.stop.store(true, Ordering::Relaxed);
                Cmd::quit()
            }
            Msg::NextPanel => {
                self.active = self.active.next();
                Cmd::none()
            }
            Msg::PrevPanel => {
                self.active = self.active.prev();
                Cmd::none()
            }
            Msg::Restart => {
                self.clear();
                Cmd::none()
            }
        }
    }

    fn view(&self, frame: &mut ftui::Frame) {
        let (a, b, c, d) = split_quadrants(frame.width(), frame.height());

        self.render_benchmark_panel(frame, a);
        self.render_recovery_panel(frame, b);
        self.render_correctness_panel(frame, c);
        self.render_summary_panel(frame, d);
    }
}

// ── Benchmark panel rendering (bd-mmhw) ──────────────────────────────────

impl DashboardModel {
    #[allow(clippy::too_many_lines)]
    fn render_benchmark_panel(&self, frame: &mut ftui::Frame, area: Rect) {
        let border_style = panel_border_style(PanelId::Benchmark, self.active);
        let title = panel_title(PanelId::Benchmark, self.active);

        let Some(ref b) = self.bench else {
            Panel::new(Paragraph::new(
                "Waiting for benchmark events...\n\n\
                 Keys: Tab/Shift-Tab switch panel | r reset | q quit"
                    .to_owned(),
            ))
            .title(&title)
            .border_style(border_style)
            .render(area, frame);
            return;
        };

        // Compute inner area for custom layout.
        let panel = Panel::new(Paragraph::new(String::new()))
            .title(&title)
            .border_style(border_style);
        let inner = panel.inner(area);
        panel.render(area, frame);

        if inner.height < 3 || inner.width < 10 {
            return;
        }

        let mut y = inner.y;

        // Status line.
        let status = if b.done { "DONE" } else { "RUNNING" };
        let status_line = format!("  {}: {} [{status}]", b.name, format_ops(b.ops_per_sec));
        Paragraph::new(status_line)
            .style(Style::new().fg(if b.done {
                PackedRgba::rgb(100, 220, 100)
            } else {
                PackedRgba::rgb(100, 180, 255)
            }))
            .render(Rect::new(inner.x, y, inner.width, 1), frame);
        y += 1;

        // Speedup ratio.
        if let Some(csqlite_ops) = b.csqlite_ops_per_sec {
            let speedup = if csqlite_ops > 0.0 {
                b.ops_per_sec / csqlite_ops
            } else {
                0.0
            };
            let speedup_line = format!(
                "  FrankenSQLite: {}  |  C SQLite: {}  |  Speedup: {speedup:.2}x",
                format_ops(b.ops_per_sec),
                format_ops(csqlite_ops)
            );
            let color = if speedup >= 2.0 {
                PackedRgba::rgb(80, 220, 80)
            } else if speedup >= 1.0 {
                PackedRgba::rgb(220, 220, 80)
            } else {
                PackedRgba::rgb(220, 80, 80)
            };
            Paragraph::new(speedup_line)
                .style(Style::new().fg(color))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }

        // Sparkline: FrankenSQLite throughput.
        if y < inner.y + inner.height && !b.frank_history.is_empty() {
            y += 1; // blank line
            let label = "  Throughput (FrankenSQLite):";
            Paragraph::new(label.to_owned())
                .style(Style::new().fg(PackedRgba::rgb(100, 220, 100)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            if y < inner.y + inner.height {
                let data: Vec<f64> = b.frank_history.iter().copied().collect();
                let spark_width = inner
                    .width
                    .saturating_sub(2)
                    .min(u16::try_from(data.len()).unwrap_or(u16::MAX));
                let visible = &data[data.len().saturating_sub(spark_width as usize)..];
                Sparkline::new(visible)
                    .style(Style::new().fg(PackedRgba::rgb(80, 220, 80)))
                    .render(Rect::new(inner.x + 2, y, spark_width, 1), frame);
                y += 1;
            }
        }

        // Sparkline: C SQLite throughput (if available).
        if y < inner.y + inner.height && !b.csqlite_history.is_empty() {
            let label = "  Throughput (C SQLite):";
            Paragraph::new(label.to_owned())
                .style(Style::new().fg(PackedRgba::rgb(220, 180, 60)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            if y < inner.y + inner.height {
                let data: Vec<f64> = b.csqlite_history.iter().copied().collect();
                let spark_width = inner
                    .width
                    .saturating_sub(2)
                    .min(u16::try_from(data.len()).unwrap_or(u16::MAX));
                let visible = &data[data.len().saturating_sub(spark_width as usize)..];
                Sparkline::new(visible)
                    .style(Style::new().fg(PackedRgba::rgb(220, 180, 60)))
                    .render(Rect::new(inner.x + 2, y, spark_width, 1), frame);
                y += 1;
            }
        }

        // Suite progress bar.
        if y + 1 < inner.y + inner.height && b.suite_total > 0 {
            y += 1;
            let ratio = if b.suite_total > 0 {
                b.suite_completed as f64 / b.suite_total as f64
            } else {
                0.0
            };
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let pct = (ratio * 100.0) as u32;
            let label = format!("  Suite: {}/{} ({pct}%)", b.suite_completed, b.suite_total);
            Paragraph::new(label)
                .style(Style::new().fg(PackedRgba::WHITE))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            if y < inner.y + inner.height {
                ProgressBar::new()
                    .ratio(ratio)
                    .gauge_style(Style::new().bg(PackedRgba::rgb(60, 120, 200)))
                    .render(
                        Rect::new(inner.x + 2, y, inner.width.saturating_sub(4), 1),
                        frame,
                    );
            }
        }
    }
}

// ── Recovery panel rendering (bd-s4qy) ───────────────────────────────────

impl DashboardModel {
    #[allow(clippy::too_many_lines)]
    fn render_recovery_panel(&self, frame: &mut ftui::Frame, area: Rect) {
        let border_style = panel_border_style(PanelId::Recovery, self.active);
        let title = panel_title(PanelId::Recovery, self.active);

        let Some(ref r) = self.recovery else {
            Panel::new(Paragraph::new(
                "Waiting for recovery events...\n\n\
                 Keys: Tab/Shift-Tab switch panel | r reset | q quit"
                    .to_owned(),
            ))
            .title(&title)
            .border_style(border_style)
            .render(area, frame);
            return;
        };

        let panel = Panel::new(Paragraph::new(String::new()))
            .title(&title)
            .border_style(border_style);
        let inner = panel.inner(area);
        panel.render(area, frame);

        if inner.height < 3 || inner.width < 10 {
            return;
        }

        let mut y = inner.y;

        // Header: page info.
        let header = format!(
            "  Page {} (offset {:#X}): {}",
            r.page, r.page_offset, r.pattern
        );
        Paragraph::new(header)
            .style(Style::new().fg(PackedRgba::rgb(255, 180, 80)))
            .render(Rect::new(inner.x, y, inner.width, 1), frame);
        y += 1;

        // Hex diff: original vs corrupted.
        if !r.original_bytes.is_empty() && !r.corrupted_bytes.is_empty() {
            y += 1;
            let hex_lines_available =
                ((inner.y + inner.height).saturating_sub(y).saturating_sub(8)) / 2;
            let bytes_per_line: usize = 8;
            let max_lines = (hex_lines_available as usize).min(HEX_DISPLAY_BYTES / bytes_per_line);

            // Original bytes.
            Paragraph::new("  Original:".to_owned())
                .style(Style::new().fg(PackedRgba::rgb(160, 160, 160)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            for line_idx in 0..max_lines {
                if y >= inner.y + inner.height {
                    break;
                }
                let start = line_idx * bytes_per_line;
                let hex = format_hex_line(&r.original_bytes, start, bytes_per_line);
                Paragraph::new(format!("  {hex}"))
                    .style(Style::new().fg(PackedRgba::rgb(100, 200, 100)))
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
                y += 1;
            }

            // Corrupted bytes.
            if y < inner.y + inner.height {
                Paragraph::new("  Corrupted:".to_owned())
                    .style(Style::new().fg(PackedRgba::rgb(160, 160, 160)))
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
                y += 1;

                for line_idx in 0..max_lines {
                    if y >= inner.y + inner.height {
                        break;
                    }
                    let start = line_idx * bytes_per_line;
                    let hex = format_hex_line_diff(
                        &r.corrupted_bytes,
                        &r.original_bytes,
                        start,
                        bytes_per_line,
                    );
                    Paragraph::new(format!("  {hex}"))
                        .style(Style::new().fg(PackedRgba::rgb(255, 80, 80)))
                        .render(Rect::new(inner.x, y, inner.width, 1), frame);
                    y += 1;
                }
            }
        }

        // Recovery status.
        if y < inner.y + inner.height {
            y += 1;
            Paragraph::new("  Recovery Status:".to_owned())
                .style(Style::new().fg(PackedRgba::WHITE).bold())
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }

        // Symbol availability progress.
        if r.symbols_needed > 0 && y < inner.y + inner.height {
            let ratio = if r.symbols_needed > 0 {
                f64::from(r.symbols_available) / f64::from(r.symbols_needed)
            } else {
                0.0
            };
            let decodable = if r.symbols_available >= r.symbols_needed {
                "DECODABLE"
            } else {
                "INSUFFICIENT"
            };
            let sym_line = format!(
                "  Symbols: {}/{} ({decodable})",
                r.symbols_available, r.symbols_needed
            );
            Paragraph::new(sym_line)
                .style(Style::new().fg(if r.symbols_available >= r.symbols_needed {
                    PackedRgba::rgb(80, 220, 80)
                } else {
                    PackedRgba::rgb(255, 180, 80)
                }))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            if y < inner.y + inner.height {
                ProgressBar::new()
                    .ratio(ratio.min(1.0))
                    .gauge_style(Style::new().bg(PackedRgba::rgb(60, 180, 120)))
                    .render(
                        Rect::new(inner.x + 2, y, inner.width.saturating_sub(4), 1),
                        frame,
                    );
                y += 1;
            }
        }

        // Phase progress.
        if !r.current_phase.is_empty() && y < inner.y + inner.height {
            let phase_line = format!(
                "  Phase: {} ({} resolved)",
                r.current_phase, r.phase_symbols_resolved
            );
            Paragraph::new(phase_line)
                .style(Style::new().fg(PackedRgba::rgb(180, 180, 255)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }

        // Verdict.
        if let Some(succeeded) = r.succeeded {
            if y < inner.y + inner.height {
                let (icon, color) = if succeeded {
                    ("RECOVERED", PackedRgba::rgb(80, 255, 80))
                } else {
                    ("FAILED", PackedRgba::rgb(255, 80, 80))
                };
                let verdict_line = format!("  FrankenSQLite: {icon}");
                Paragraph::new(verdict_line)
                    .style(Style::new().fg(color).bold())
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
                y += 1;
            }
        }

        // C SQLite integrity result.
        if let Some(passed) = r.csqlite_integrity_passed {
            if y < inner.y + inner.height {
                let (icon, color) = if passed {
                    ("integrity OK", PackedRgba::rgb(160, 160, 160))
                } else {
                    ("INTEGRITY FAILED", PackedRgba::rgb(255, 80, 80))
                };
                let line = format!("  C SQLite: {icon}");
                Paragraph::new(line)
                    .style(Style::new().fg(color))
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
            }
        }
    }
}

// ── Correctness panel rendering (bd-1nqt) ────────────────────────────────

impl DashboardModel {
    #[allow(clippy::too_many_lines)]
    fn render_correctness_panel(&self, frame: &mut ftui::Frame, area: Rect) {
        let border_style = panel_border_style(PanelId::Correctness, self.active);
        let title = panel_title(PanelId::Correctness, self.active);
        let c = &self.correctness;

        if c.results.is_empty() && c.current_workload.is_none() {
            Panel::new(Paragraph::new(
                "Waiting for correctness events...\n\n\
                 Keys: Tab/Shift-Tab switch panel | r reset | q quit"
                    .to_owned(),
            ))
            .title(&title)
            .border_style(border_style)
            .render(area, frame);
            return;
        }

        let panel = Panel::new(Paragraph::new(String::new()))
            .title(&title)
            .border_style(border_style);
        let inner = panel.inner(area);
        panel.render(area, frame);

        if inner.height < 3 || inner.width < 10 {
            return;
        }

        let mut y = inner.y;

        // Current workload progress.
        if let Some(ref wl) = c.current_workload {
            let progress_line = format!("  Workload: {wl}");
            Paragraph::new(progress_line)
                .style(Style::new().fg(PackedRgba::rgb(100, 180, 255)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            if c.ops_total > 0 && y < inner.y + inner.height {
                let ratio = c.ops_done as f64 / c.ops_total as f64;
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let pct = (ratio * 100.0) as u32;
                let ops_line = format!("  Progress: {}/{} ({pct}%)", c.ops_done, c.ops_total);
                Paragraph::new(ops_line)
                    .style(Style::new().fg(PackedRgba::WHITE))
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
                y += 1;

                if y < inner.y + inner.height {
                    ProgressBar::new()
                        .ratio(ratio)
                        .gauge_style(Style::new().bg(PackedRgba::rgb(60, 120, 200)))
                        .render(
                            Rect::new(inner.x + 2, y, inner.width.saturating_sub(4), 1),
                            frame,
                        );
                    y += 1;
                }
            }
            y += 1;
        }

        // Results table header.
        if y < inner.y + inner.height {
            let hdr = format!(
                "  {:<20} {:<10} {:<10} {}",
                "Workload", "Frank", "CSQLite", "Result"
            );
            Paragraph::new(hdr)
                .style(Style::new().fg(PackedRgba::WHITE).bold())
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }

        // Separator.
        if y < inner.y + inner.height {
            let sep: String = "-".repeat(inner.width.saturating_sub(2) as usize);
            Paragraph::new(format!("  {sep}"))
                .style(Style::new().fg(PackedRgba::rgb(80, 80, 80)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }

        // Result rows.
        for result in &c.results {
            if y >= inner.y + inner.height {
                break;
            }
            let frank_short = truncate_hash(&result.frank_hash, 8);
            let csqlite_short = truncate_hash(&result.csqlite_hash, 8);
            let (icon, color) = if result.matched {
                ("MATCH", PackedRgba::rgb(80, 220, 80))
            } else {
                ("MISMATCH", PackedRgba::rgb(255, 80, 80))
            };
            let wl_name = truncate_str(&result.workload, 18);
            let row = format!("  {wl_name:<20} {frank_short:<10} {csqlite_short:<10} {icon}");
            Paragraph::new(row)
                .style(Style::new().fg(color))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }

        // Currently running workload placeholder row.
        if c.current_workload.is_some() && y < inner.y + inner.height {
            let wl_name = c
                .current_workload
                .as_deref()
                .map_or("...", |w| truncate_str_static(w, 18));
            let row = format!("  {wl_name:<20} {:<10} {:<10} ...", "running", "running");
            Paragraph::new(row)
                .style(Style::new().fg(PackedRgba::rgb(180, 180, 180)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }

        // Recent SQL log.
        if !c.recent_sql.is_empty() && y + 1 < inner.y + inner.height {
            y += 1;
            Paragraph::new("  Recent SQL:".to_owned())
                .style(Style::new().fg(PackedRgba::rgb(160, 160, 160)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            for sql in &c.recent_sql {
                if y >= inner.y + inner.height {
                    break;
                }
                let truncated = truncate_str(sql, (inner.width.saturating_sub(4)) as usize);
                Paragraph::new(format!("  {truncated}"))
                    .style(Style::new().fg(PackedRgba::rgb(120, 120, 120)))
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
                y += 1;
            }
        }

        // Overall summary.
        if y < inner.y + inner.height && !c.results.is_empty() {
            y += 1;
            let passed = c.results.iter().filter(|r| r.matched).count();
            let running = i32::from(c.current_workload.is_some());
            let summary = format!(
                "  Overall: {passed}/{} passed, {running} running",
                c.results.len()
            );
            Paragraph::new(summary)
                .style(Style::new().fg(PackedRgba::WHITE))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
        }
    }
}

// ── Summary panel rendering ──────────────────────────────────────────────

impl DashboardModel {
    fn render_summary_panel(&self, frame: &mut ftui::Frame, area: Rect) {
        let border_style = panel_border_style(PanelId::Summary, self.active);
        let title = panel_title(PanelId::Summary, self.active);

        let mut body = String::new();
        for line in &self.log {
            body.push_str(line);
            body.push('\n');
        }
        if body.is_empty() {
            body.push_str("No events yet\n");
        }
        body.push_str("\nKeys: Tab/Shift-Tab switch | r reset | q quit");

        Panel::new(Paragraph::new(body))
            .title(&title)
            .border_style(border_style)
            .render(area, frame);
    }
}

// ── Layout + styling helpers ─────────────────────────────────────────────

fn split_quadrants(width: u16, height: u16) -> (Rect, Rect, Rect, Rect) {
    let mid_x = width / 2;
    let mid_y = height / 2;

    let a = Rect::new(0, 0, mid_x, mid_y);
    let b = Rect::new(mid_x, 0, width.saturating_sub(mid_x), mid_y);
    let c = Rect::new(0, mid_y, mid_x, height.saturating_sub(mid_y));
    let d = Rect::new(
        mid_x,
        mid_y,
        width.saturating_sub(mid_x),
        height.saturating_sub(mid_y),
    );

    (a, b, c, d)
}

fn panel_border_style(id: PanelId, active: PanelId) -> Style {
    if id == active {
        Style::default().fg(PackedRgba::rgb(255, 255, 0))
    } else {
        Style::default().fg(PackedRgba::rgb(80, 80, 80))
    }
}

fn panel_title(id: PanelId, active: PanelId) -> String {
    if id == active {
        format!("{} [active]", id.title())
    } else {
        id.title().to_owned()
    }
}

// ── Formatting helpers ───────────────────────────────────────────────────

/// Format operations per second with SI suffix.
fn format_ops(ops: f64) -> String {
    if ops >= 1_000_000.0 {
        format!("{:.1}M ops/s", ops / 1_000_000.0)
    } else if ops >= 1_000.0 {
        format!("{:.1}K ops/s", ops / 1_000.0)
    } else {
        format!("{ops:.0} ops/s")
    }
}

/// Format a hex line from a byte slice.
fn format_hex_line(bytes: &[u8], start: usize, count: usize) -> String {
    let end = (start + count).min(bytes.len());
    if start >= bytes.len() {
        return String::new();
    }
    let mut hex = String::with_capacity(count * 3);
    for (i, &b) in bytes[start..end].iter().enumerate() {
        if i > 0 {
            hex.push(' ');
        }
        let _ = write!(hex, "{b:02X}");
    }
    hex
}

/// Format a hex line highlighting bytes that differ from reference.
fn format_hex_line_diff(bytes: &[u8], reference: &[u8], start: usize, count: usize) -> String {
    let end = (start + count).min(bytes.len());
    if start >= bytes.len() {
        return String::new();
    }
    let mut hex = String::with_capacity(count * 3);
    for (i, &b) in bytes[start..end].iter().enumerate() {
        if i > 0 {
            hex.push(' ');
        }
        let ref_byte = reference.get(start + i).copied().unwrap_or(0);
        if b == ref_byte {
            let _ = write!(hex, "{b:02X}");
        } else {
            // Mark differing bytes with brackets.
            let _ = write!(hex, "[{b:02X}]");
        }
    }
    hex
}

/// Truncate a hash string to `max_len` characters.
fn truncate_hash(hash: &str, max_len: usize) -> String {
    if hash.len() <= max_len {
        hash.to_owned()
    } else {
        format!("{}...", &hash[..max_len.saturating_sub(3)])
    }
}

/// Truncate a string reference to `max_len` characters.
fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_owned()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

/// Truncate returning a static-like reference (returns owned for simplicity).
fn truncate_str_static(s: &str, max_len: usize) -> &str {
    if s.len() <= max_len { s } else { &s[..max_len] }
}

// ── Headless mode ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct HeadlessOutput {
    generated_at_unix_ms: u64,
    events: Vec<DashboardEvent>,
}

// ── Main ──────────────────────────────────────────────────────────────────

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        print_help();
        return Ok(());
    }

    let headless = args.iter().any(|a| a == "--headless");
    let output_path = parse_output_path(&args);

    if headless {
        let out = HeadlessOutput {
            generated_at_unix_ms: unix_ms_now(),
            events: sample_events(),
        };
        write_headless(&out, output_path.as_deref())?;
        return Ok(());
    }

    let (tx, rx) = mpsc::channel::<DashboardEvent>();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_bg = stop.clone();

    let bg = std::thread::spawn(move || demo_event_producer(&tx, &stop_bg));
    let model = DashboardModel::new(rx, stop.clone());

    let res = App::new(model).screen_mode(ScreenMode::AltScreen).run();

    stop.store(true, Ordering::Relaxed);
    let _ = bg.join();

    res
}

fn unix_ms_now() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0));
    now.as_millis().try_into().unwrap_or(u64::MAX)
}

fn parse_output_path(args: &[String]) -> Option<PathBuf> {
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--output" && i + 1 < args.len() {
            return Some(PathBuf::from(&args[i + 1]));
        }
        i += 1;
    }
    None
}

fn write_headless(out: &HeadlessOutput, path: Option<&std::path::Path>) -> std::io::Result<()> {
    let json =
        serde_json::to_string_pretty(out).map_err(|e| std::io::Error::other(e.to_string()))?;

    if let Some(p) = path {
        std::fs::write(p, json.as_bytes())?;
    } else {
        println!("{json}");
    }
    Ok(())
}

fn sample_events() -> Vec<DashboardEvent> {
    vec![
        DashboardEvent::StatusMessage {
            message: "headless mode: sample run".to_owned(),
        },
        DashboardEvent::BenchmarkComplete {
            name: "concurrent_writes_8t".to_owned(),
            wall_time_ms: 1234,
            ops_per_sec: 185_000.0,
        },
        DashboardEvent::CorrectnessCheck {
            workload: "sequential_insert".to_owned(),
            frank_hash: "a3b7c9d1e2f4a5b6".to_owned(),
            csqlite_hash: "a3b7c9d1e2f4a5b6".to_owned(),
            matched: true,
        },
        DashboardEvent::CorruptionInjected {
            page: 500,
            pattern: "PageZero".to_owned(),
        },
        DashboardEvent::RecoverySuccess {
            page: 500,
            decode_proof: "xxh3 verified".to_owned(),
        },
    ]
}

// ── Demo event producer (showcases all panels) ───────────────────────────

#[allow(clippy::too_many_lines)]
fn demo_event_producer(tx: &mpsc::Sender<DashboardEvent>, stop: &Arc<AtomicBool>) {
    let _ = tx.send(DashboardEvent::StatusMessage {
        message: "dashboard online (demo event source)".to_owned(),
    });

    let started = Instant::now();
    let mut last_emit = Instant::now();
    let mut tick: u64 = 0;

    // Phase 1: Benchmark progress (0-3s).
    // Phase 2: Correctness check (3-5s).
    // Phase 3: Corruption recovery (5-8s).
    // Phase 4: Complete (8s+).

    let _ = tx.send(DashboardEvent::BenchmarkSuiteProgress {
        completed: 0,
        total: 3,
    });

    // Start correctness workload.
    let _ = tx.send(DashboardEvent::CorrectnessWorkloadStart {
        workload: "Sequential INSERT".to_owned(),
        total_ops: 10000,
    });

    while !stop.load(Ordering::Relaxed) {
        if last_emit.elapsed() < Duration::from_millis(200) {
            std::thread::sleep(Duration::from_millis(10));
            continue;
        }

        last_emit = Instant::now();
        tick += 1;
        let elapsed_ms: u64 = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);

        // Phase 1: Streaming benchmark throughput.
        if (0..4_000).contains(&elapsed_ms) {
            let tick_f = tick as f64;
            let frank_ops = tick_f.mul_add(8_000.0, 50_000.0);
            let csqlite_ops = tick_f.mul_add(1_200.0, 12_000.0);

            let _ = tx.send(DashboardEvent::BenchmarkProgress {
                name: "concurrent_writes_8t".to_owned(),
                ops_per_sec: frank_ops,
                elapsed_ms,
            });
            let _ = tx.send(DashboardEvent::BenchmarkCsqliteProgress {
                name: "concurrent_writes_8t".to_owned(),
                ops_per_sec: csqlite_ops,
                elapsed_ms,
            });

            // Correctness op progress.
            #[allow(clippy::cast_possible_truncation)]
            let ops_done = ((tick * 600) as usize).min(10000);
            let sql_samples = [
                "INSERT INTO test VALUES (421, 'data-421')",
                "UPDATE test SET value=82.3 WHERE id=421",
                "DELETE FROM test WHERE id < 10",
                "SELECT COUNT(*) FROM test",
                "INSERT INTO test VALUES (999, 'final')",
            ];
            let _ = tx.send(DashboardEvent::CorrectnessOpProgress {
                workload: "Sequential INSERT".to_owned(),
                ops_done,
                total_ops: 10000,
                #[allow(clippy::cast_possible_truncation)]
                current_sql: sql_samples[(tick as usize) % sql_samples.len()].to_owned(),
            });
        }

        // Phase 2: Benchmark done + correctness results.
        if (4_000..4_500).contains(&elapsed_ms) && tick % 5 == 0 {
            let _ = tx.send(DashboardEvent::BenchmarkComplete {
                name: "concurrent_writes_8t".to_owned(),
                wall_time_ms: elapsed_ms,
                ops_per_sec: 185_000.0,
            });
            let _ = tx.send(DashboardEvent::BenchmarkSuiteProgress {
                completed: 1,
                total: 3,
            });

            let _ = tx.send(DashboardEvent::CorrectnessCheck {
                workload: "Sequential INSERT".to_owned(),
                frank_hash: "a3b7c9d1e2f4a5b6c7d8e9f0a1b2c3d4".to_owned(),
                csqlite_hash: "a3b7c9d1e2f4a5b6c7d8e9f0a1b2c3d4".to_owned(),
                matched: true,
            });

            let _ = tx.send(DashboardEvent::CorrectnessWorkloadStart {
                workload: "Mixed DML".to_owned(),
                total_ops: 20000,
            });
        }

        // Phase 3: Corruption + recovery demo.
        if (5_000..5_500).contains(&elapsed_ms) && tick % 5 == 0 {
            let _ = tx.send(DashboardEvent::CorruptionInjected {
                page: 500,
                pattern: "PageZero".to_owned(),
            });
            // Simulated hex data.
            let original: Vec<u8> = (0..64).map(|i| 0x0D_u8.wrapping_add(i)).collect();
            let corrupted: Vec<u8> = vec![0u8; 64];
            let _ = tx.send(DashboardEvent::CorruptionHexData {
                original_bytes: original,
                corrupted_bytes: corrupted,
                page_offset: 0x1F_4000,
            });
        }

        if (6_000..6_500).contains(&elapsed_ms) && tick % 5 == 0 {
            let _ = tx.send(DashboardEvent::RecoveryAttempt {
                group: 7,
                symbols_available: 67,
                needed: 64,
            });
            let _ = tx.send(DashboardEvent::RecoveryPhaseUpdate {
                phase: "peeling".to_owned(),
                symbols_resolved: 61,
            });
        }

        if (7_000..7_500).contains(&elapsed_ms) && tick % 5 == 0 {
            let _ = tx.send(DashboardEvent::RecoveryPhaseUpdate {
                phase: "Gaussian".to_owned(),
                symbols_resolved: 64,
            });

            let recovered: Vec<u8> = (0..64).map(|i| 0x0D_u8.wrapping_add(i)).collect();
            let _ = tx.send(DashboardEvent::RecoveryHexData {
                recovered_bytes: recovered,
            });

            let _ = tx.send(DashboardEvent::RecoverySuccess {
                page: 500,
                decode_proof: "xxh3 verified — page matches original".to_owned(),
            });

            let _ = tx.send(DashboardEvent::CsqliteIntegrityResult {
                passed: false,
                message: "PRAGMA integrity_check failed — data lost".to_owned(),
            });

            // Second correctness result.
            let _ = tx.send(DashboardEvent::CorrectnessCheck {
                workload: "Mixed DML".to_owned(),
                frank_hash: "e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0".to_owned(),
                csqlite_hash: "e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0".to_owned(),
                matched: true,
            });
        }

        if elapsed_ms >= 9_000 {
            let _ = tx.send(DashboardEvent::BenchmarkSuiteProgress {
                completed: 3,
                total: 3,
            });
            let _ = tx.send(DashboardEvent::StatusMessage {
                message: "demo complete".to_owned(),
            });
            break;
        }
    }
}

fn print_help() {
    let text = "\
e2e-dashboard — FrankenTUI dashboard for FrankenSQLite E2E runs

USAGE:
    e2e-dashboard [--headless] [--output <FILE>]

OPTIONS:
    --headless          Skip TUI; emit JSON to stdout (or --output)
    --output <FILE>     Write headless JSON output to a file
    -h, --help          Show this help

PANELS:
    Benchmark       Real-time throughput sparkline with speedup ratio
    Recovery        Hex diff of corrupted/recovered bytes + decode status
    Correctness     SHA-256 comparison table with per-workload pass/fail
    Summary         Scrollable event log

KEYS:
    Tab / Shift-Tab     Switch active panel
    r                   Reset all state
    q                   Quit
";
    print!("{text}");
}
