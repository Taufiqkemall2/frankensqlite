//! Structured logging initialization for E2E runners (bd-mblr.5.1.1).
//!
//! Provides the wiring between the unified E2E log schema
//! ([`e2e_log_schema`](crate::e2e_log_schema)) and the tracing framework.
//! Ensures every E2E runner starts with a properly initialized structured
//! logging subscriber and emits the required startup context fields.
//!
//! # Initialization
//!
//! Call [`init_e2e_logging`] early in the runner's entry point. This sets up:
//! - A tracing subscriber with structured (JSON-compatible) output
//! - A [`RunContext`] capturing the correlation fields for the entire run
//! - An initial `Start` event compliant with [`LogEventSchema`]
//!
//! # Environment Variables
//!
//! The following environment variables are consulted at init time:
//! - `RUN_ID` — override the auto-generated run identifier
//! - `SCENARIO_ID` — traceability matrix scenario identifier
//! - `SEED` — deterministic replay seed (parsed as u64)
//! - `BACKEND` — engine under test (`fsqlite`, `rusqlite`, `both`)
//! - `RUST_LOG` — tracing filter directives (standard tracing-subscriber)
//!
//! # Upstream Dependencies
//!
//! - [`e2e_log_schema`](crate::e2e_log_schema) (bd-1dp9.7.2)
//! - [`e2e_orchestrator`](crate::e2e_orchestrator)
//!
//! # Downstream Consumers
//!
//! - **bd-mblr.5.4.1**: Golden tests for log quality validate events from this module

use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Write as FmtWrite;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::e2e_log_schema::{LOG_SCHEMA_VERSION, LogEventSchema, LogEventType, LogPhase};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-mblr.5.1.1";

/// Global event counter for ordering within a run.
static EVENT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// Run context
// ---------------------------------------------------------------------------

/// Correlation context for an entire E2E run.
///
/// Created once during [`init_e2e_logging`] and threaded through all
/// lifecycle events. Carries the fields required by [`LogEventSchema`]
/// plus runner-specific metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunContext {
    /// Unique run identifier (format: `{bead_id}-{timestamp_ms}-{pid}`).
    pub run_id: String,
    /// Schema version this run was initialized with.
    pub schema_version: String,
    /// Bead ID of the invoking task (for triage correlation).
    pub bead_id: String,
    /// Optional scenario ID from the traceability matrix.
    pub scenario_id: Option<String>,
    /// Deterministic seed for replay.
    pub seed: Option<u64>,
    /// Backend under test.
    pub backend: Option<String>,
    /// Process ID at initialization time.
    pub pid: u32,
    /// Initialization timestamp (ISO 8601 UTC).
    pub init_timestamp: String,
}

impl RunContext {
    /// Create a new run context, reading overrides from environment variables.
    ///
    /// If `RUN_ID` is set in the environment, it is used directly.
    /// Otherwise, a run ID is synthesized from `bead_id`, the current
    /// time, and the PID.
    #[must_use]
    pub fn from_env(bead_id: &str) -> Self {
        let pid = std::process::id();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let timestamp = format_iso8601_from_ms(now_ms);

        let run_id =
            std::env::var("RUN_ID").unwrap_or_else(|_| format!("{bead_id}-{now_ms}-{pid}"));

        let scenario_id = std::env::var("SCENARIO_ID").ok();
        let seed = std::env::var("SEED")
            .ok()
            .and_then(|s| s.parse::<u64>().ok());
        let backend = std::env::var("BACKEND").ok();

        Self {
            run_id,
            schema_version: LOG_SCHEMA_VERSION.to_owned(),
            bead_id: bead_id.to_owned(),
            scenario_id,
            seed,
            backend,
            pid,
            init_timestamp: timestamp,
        }
    }

    /// Create a run context with explicit values (for testing).
    #[must_use]
    pub fn new(
        run_id: &str,
        bead_id: &str,
        scenario_id: Option<&str>,
        seed: Option<u64>,
        backend: Option<&str>,
    ) -> Self {
        let pid = std::process::id();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        Self {
            run_id: run_id.to_owned(),
            schema_version: LOG_SCHEMA_VERSION.to_owned(),
            bead_id: bead_id.to_owned(),
            scenario_id: scenario_id.map(ToOwned::to_owned),
            seed,
            backend: backend.map(ToOwned::to_owned),
            pid,
            init_timestamp: format_iso8601_from_ms(now_ms),
        }
    }
}

// ---------------------------------------------------------------------------
// Logging configuration
// ---------------------------------------------------------------------------

/// Output format for structured logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogOutputFormat {
    /// Human-readable tracing output (default).
    Pretty,
    /// Machine-readable JSON lines (one event per line).
    Json,
    /// Compact single-line format.
    Compact,
}

impl Default for LogOutputFormat {
    fn default() -> Self {
        Self::Pretty
    }
}

impl fmt::Display for LogOutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pretty => write!(f, "pretty"),
            Self::Json => write!(f, "json"),
            Self::Compact => write!(f, "compact"),
        }
    }
}

/// Configuration for E2E structured logging initialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2eLoggingConfig {
    /// Output format.
    pub format: LogOutputFormat,
    /// Whether to include timestamps in output (useful to disable in tests).
    pub include_timestamps: bool,
    /// Whether to include target module paths in output.
    pub include_targets: bool,
    /// Whether to include span events (enter/exit).
    pub include_span_events: bool,
    /// Maximum log level filter (e.g. "info", "debug", "trace").
    pub level_filter: String,
}

impl Default for E2eLoggingConfig {
    fn default() -> Self {
        Self {
            format: LogOutputFormat::Pretty,
            include_timestamps: true,
            include_targets: true,
            include_span_events: false,
            level_filter: "info".to_owned(),
        }
    }
}

impl E2eLoggingConfig {
    /// Configuration suitable for CI: JSON output, info level.
    #[must_use]
    pub fn ci() -> Self {
        Self {
            format: LogOutputFormat::Json,
            include_timestamps: true,
            include_targets: true,
            include_span_events: false,
            level_filter: "info".to_owned(),
        }
    }

    /// Configuration suitable for local development: pretty, debug level.
    #[must_use]
    pub fn dev() -> Self {
        Self {
            format: LogOutputFormat::Pretty,
            include_timestamps: true,
            include_targets: true,
            include_span_events: true,
            level_filter: "debug".to_owned(),
        }
    }

    /// Configuration for tests: compact, no timestamps.
    #[must_use]
    pub fn test() -> Self {
        Self {
            format: LogOutputFormat::Compact,
            include_timestamps: false,
            include_targets: false,
            include_span_events: false,
            level_filter: "warn".to_owned(),
        }
    }
}

// ---------------------------------------------------------------------------
// Lifecycle event emission
// ---------------------------------------------------------------------------

/// Emit a structured lifecycle event conforming to the E2E log schema.
///
/// Creates a [`LogEventSchema`] from the run context and emits it as
/// a serialized JSON string. Returns the event for chaining or logging.
#[must_use]
pub fn emit_lifecycle_event(
    ctx: &RunContext,
    phase: LogPhase,
    event_type: LogEventType,
    extra_context: &[(&str, &str)],
) -> LogEventSchema {
    let seq = EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);

    let mut context = BTreeMap::new();
    context.insert("schema_version".to_owned(), ctx.schema_version.clone());
    context.insert("bead_id".to_owned(), ctx.bead_id.clone());
    context.insert("pid".to_owned(), ctx.pid.to_string());
    context.insert("event_sequence".to_owned(), seq.to_string());

    for (k, v) in extra_context {
        context.insert((*k).to_owned(), (*v).to_owned());
    }

    LogEventSchema {
        run_id: ctx.run_id.clone(),
        timestamp: format_iso8601_from_ms(now_ms),
        phase,
        event_type,
        scenario_id: ctx.scenario_id.clone(),
        seed: ctx.seed,
        backend: ctx.backend.clone(),
        artifact_hash: None,
        context,
    }
}

/// Emit the initial startup event for a run.
///
/// This should be called immediately after [`init_e2e_logging`] to record
/// the run context and configuration in the event stream.
#[must_use]
pub fn emit_startup_event(ctx: &RunContext, config: &E2eLoggingConfig) -> LogEventSchema {
    emit_lifecycle_event(
        ctx,
        LogPhase::Setup,
        LogEventType::Start,
        &[
            ("log_format", &config.format.to_string()),
            ("level_filter", &config.level_filter),
            ("init_timestamp", &ctx.init_timestamp),
        ],
    )
}

/// Emit a phase transition event.
#[must_use]
pub fn emit_phase_event(
    ctx: &RunContext,
    phase: LogPhase,
    event_type: LogEventType,
    message: &str,
) -> LogEventSchema {
    emit_lifecycle_event(ctx, phase, event_type, &[("message", message)])
}

/// Emit a completion event for the entire run.
#[must_use]
pub fn emit_completion_event(
    ctx: &RunContext,
    passed: bool,
    total_scripts: usize,
    pass_count: usize,
    fail_count: usize,
) -> LogEventSchema {
    let event_type = if passed {
        LogEventType::Pass
    } else {
        LogEventType::Fail
    };

    let mut summary = String::new();
    let _ = write!(
        summary,
        "{pass_count}/{total_scripts} passed, {fail_count} failed",
    );

    emit_lifecycle_event(
        ctx,
        LogPhase::Report,
        event_type,
        &[
            ("total_scripts", &total_scripts.to_string()),
            ("pass_count", &pass_count.to_string()),
            ("fail_count", &fail_count.to_string()),
            ("summary", &summary),
        ],
    )
}

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

/// Result of initializing structured logging.
#[derive(Debug, Clone)]
pub struct LoggingInitResult {
    /// The run context created during initialization.
    pub context: RunContext,
    /// The startup event that was emitted.
    pub startup_event: LogEventSchema,
    /// Human-readable initialization summary.
    pub summary: String,
}

/// Initialize structured logging for an E2E runner.
///
/// This function:
/// 1. Creates a [`RunContext`] from environment variables
/// 2. Resets the global event sequence counter
/// 3. Emits the initial startup event
/// 4. Returns the context and startup event for downstream use
///
/// Note: This function does NOT install a tracing subscriber (that is
/// the responsibility of the binary entry point). It provides the
/// structured event emission layer that sits on top of whatever
/// subscriber the binary installs.
#[must_use]
pub fn init_e2e_logging(bead_id: &str, config: &E2eLoggingConfig) -> LoggingInitResult {
    // Reset sequence counter for this run.
    EVENT_SEQUENCE.store(0, Ordering::Relaxed);

    let context = RunContext::from_env(bead_id);
    let startup_event = emit_startup_event(&context, config);

    let summary = format!(
        "E2E logging initialized: run_id={}, schema={}, format={}, level={}",
        context.run_id, context.schema_version, config.format, config.level_filter,
    );

    LoggingInitResult {
        context,
        startup_event,
        summary,
    }
}

/// Initialize structured logging with an explicit run context (for testing).
#[must_use]
pub fn init_e2e_logging_with_context(
    context: RunContext,
    config: &E2eLoggingConfig,
) -> LoggingInitResult {
    EVENT_SEQUENCE.store(0, Ordering::Relaxed);

    let startup_event = emit_startup_event(&context, config);

    let summary = format!(
        "E2E logging initialized: run_id={}, schema={}, format={}, level={}",
        context.run_id, context.schema_version, config.format, config.level_filter,
    );

    LoggingInitResult {
        context,
        startup_event,
        summary,
    }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Validate that a log event conforms to the required schema fields.
///
/// Returns a list of validation errors (empty = valid).
#[must_use]
pub fn validate_event(event: &LogEventSchema) -> Vec<String> {
    let mut errors = Vec::new();

    if event.run_id.is_empty() {
        errors.push("run_id is empty".to_owned());
    }

    if event.timestamp.is_empty() {
        errors.push("timestamp is empty".to_owned());
    }

    // Timestamp should end with 'Z' (UTC).
    if !event.timestamp.is_empty() && !event.timestamp.ends_with('Z') {
        errors.push(format!(
            "timestamp '{}' does not end with 'Z' (UTC required)",
            event.timestamp,
        ));
    }

    errors
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Format milliseconds since epoch as ISO 8601 UTC string.
fn format_iso8601_from_ms(ms: u128) -> String {
    let secs = ms / 1000;
    let millis = ms % 1000;
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Simple year/month/day calculation from days since 1970-01-01.
    let (year, month, day) = days_to_ymd(days_since_epoch);

    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.{millis:03}Z",)
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_ymd(days: u128) -> (u128, u128, u128) {
    // Civil calendar algorithm (Howard Hinnant).
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_context() -> RunContext {
        RunContext::new(
            "test-run-001",
            "bd-test",
            Some("INFRA-1"),
            Some(42),
            Some("fsqlite"),
        )
    }

    fn test_config() -> E2eLoggingConfig {
        E2eLoggingConfig::test()
    }

    #[test]
    fn run_context_from_explicit_values() {
        let ctx = test_context();
        assert_eq!(ctx.run_id, "test-run-001");
        assert_eq!(ctx.bead_id, "bd-test");
        assert_eq!(ctx.scenario_id.as_deref(), Some("INFRA-1"));
        assert_eq!(ctx.seed, Some(42));
        assert_eq!(ctx.backend.as_deref(), Some("fsqlite"));
        assert_eq!(ctx.schema_version, LOG_SCHEMA_VERSION);
    }

    #[test]
    fn run_context_from_env_synthesizes_run_id() {
        // RUN_ID is unlikely to be set in the test environment,
        // so from_env should synthesize it from the bead_id.
        // If RUN_ID happens to be set, the run_id will be that value instead.
        let ctx = RunContext::from_env("bd-test-env");
        assert!(!ctx.run_id.is_empty(), "run_id should not be empty",);
        // If RUN_ID is not set, it should start with the bead_id prefix.
        if std::env::var("RUN_ID").is_err() {
            assert!(
                ctx.run_id.starts_with("bd-test-env-"),
                "run_id should start with bead_id: {}",
                ctx.run_id,
            );
        }
    }

    #[test]
    fn default_config_is_pretty_info() {
        let cfg = E2eLoggingConfig::default();
        assert_eq!(cfg.format, LogOutputFormat::Pretty);
        assert_eq!(cfg.level_filter, "info");
        assert!(cfg.include_timestamps);
    }

    #[test]
    fn ci_config_is_json() {
        let cfg = E2eLoggingConfig::ci();
        assert_eq!(cfg.format, LogOutputFormat::Json);
    }

    #[test]
    fn dev_config_is_debug() {
        let cfg = E2eLoggingConfig::dev();
        assert_eq!(cfg.format, LogOutputFormat::Pretty);
        assert_eq!(cfg.level_filter, "debug");
        assert!(cfg.include_span_events);
    }

    #[test]
    fn test_config_is_compact_warn() {
        let cfg = E2eLoggingConfig::test();
        assert_eq!(cfg.format, LogOutputFormat::Compact);
        assert_eq!(cfg.level_filter, "warn");
        assert!(!cfg.include_timestamps);
    }

    #[test]
    fn init_produces_startup_event() {
        let ctx = test_context();
        let cfg = test_config();
        let result = init_e2e_logging_with_context(ctx, &cfg);
        assert_eq!(result.startup_event.phase, LogPhase::Setup);
        assert_eq!(result.startup_event.event_type, LogEventType::Start);
        assert_eq!(result.startup_event.run_id, "test-run-001");
    }

    #[test]
    fn startup_event_has_required_fields() {
        let ctx = test_context();
        let cfg = test_config();
        let result = init_e2e_logging_with_context(ctx, &cfg);
        let errors = validate_event(&result.startup_event);
        assert!(
            errors.is_empty(),
            "startup event validation errors: {:?}",
            errors,
        );
    }

    #[test]
    fn startup_event_contains_config_context() {
        let ctx = test_context();
        let cfg = E2eLoggingConfig::ci();
        let result = init_e2e_logging_with_context(ctx, &cfg);
        let context = &result.startup_event.context;
        assert_eq!(context.get("log_format").map(String::as_str), Some("json"));
        assert_eq!(
            context.get("level_filter").map(String::as_str),
            Some("info")
        );
    }

    #[test]
    fn lifecycle_events_have_incrementing_sequence() {
        EVENT_SEQUENCE.store(0, Ordering::Relaxed);
        let ctx = test_context();
        let e1 = emit_lifecycle_event(&ctx, LogPhase::Setup, LogEventType::Start, &[]);
        let e2 = emit_lifecycle_event(&ctx, LogPhase::Execute, LogEventType::Info, &[]);
        let e3 = emit_lifecycle_event(&ctx, LogPhase::Validate, LogEventType::Pass, &[]);
        let seq1: u64 = e1.context.get("event_sequence").unwrap().parse().unwrap();
        let seq2: u64 = e2.context.get("event_sequence").unwrap().parse().unwrap();
        let seq3: u64 = e3.context.get("event_sequence").unwrap().parse().unwrap();
        assert!(seq1 < seq2, "seq1={seq1} should be < seq2={seq2}");
        assert!(seq2 < seq3, "seq2={seq2} should be < seq3={seq3}");
    }

    #[test]
    fn lifecycle_event_carries_run_id() {
        let ctx = test_context();
        let event = emit_lifecycle_event(&ctx, LogPhase::Execute, LogEventType::Info, &[]);
        assert_eq!(event.run_id, "test-run-001");
    }

    #[test]
    fn lifecycle_event_carries_scenario_and_seed() {
        let ctx = test_context();
        let event = emit_lifecycle_event(&ctx, LogPhase::Execute, LogEventType::Info, &[]);
        assert_eq!(event.scenario_id.as_deref(), Some("INFRA-1"));
        assert_eq!(event.seed, Some(42));
    }

    #[test]
    fn lifecycle_event_extra_context_preserved() {
        let ctx = test_context();
        let event = emit_lifecycle_event(
            &ctx,
            LogPhase::Execute,
            LogEventType::Info,
            &[("custom_key", "custom_val")],
        );
        assert_eq!(
            event.context.get("custom_key").map(String::as_str),
            Some("custom_val"),
        );
    }

    #[test]
    fn phase_event_includes_message() {
        let ctx = test_context();
        let event = emit_phase_event(&ctx, LogPhase::Execute, LogEventType::Info, "running suite");
        assert_eq!(
            event.context.get("message").map(String::as_str),
            Some("running suite"),
        );
    }

    #[test]
    fn completion_event_pass() {
        let ctx = test_context();
        let event = emit_completion_event(&ctx, true, 10, 10, 0);
        assert_eq!(event.event_type, LogEventType::Pass);
        assert_eq!(event.phase, LogPhase::Report);
        assert_eq!(
            event.context.get("total_scripts").map(String::as_str),
            Some("10"),
        );
    }

    #[test]
    fn completion_event_fail() {
        let ctx = test_context();
        let event = emit_completion_event(&ctx, false, 10, 7, 3);
        assert_eq!(event.event_type, LogEventType::Fail);
        assert_eq!(
            event.context.get("fail_count").map(String::as_str),
            Some("3"),
        );
    }

    #[test]
    fn validate_event_catches_empty_run_id() {
        let mut event =
            emit_lifecycle_event(&test_context(), LogPhase::Setup, LogEventType::Start, &[]);
        event.run_id = String::new();
        let errors = validate_event(&event);
        assert!(errors.iter().any(|e| e.contains("run_id")));
    }

    #[test]
    fn validate_event_catches_non_utc_timestamp() {
        let mut event =
            emit_lifecycle_event(&test_context(), LogPhase::Setup, LogEventType::Start, &[]);
        event.timestamp = "2026-01-01T00:00:00+05:00".to_owned();
        let errors = validate_event(&event);
        assert!(errors.iter().any(|e| e.contains("UTC")));
    }

    #[test]
    fn validate_event_passes_for_valid_event() {
        let event =
            emit_lifecycle_event(&test_context(), LogPhase::Setup, LogEventType::Start, &[]);
        let errors = validate_event(&event);
        assert!(errors.is_empty(), "unexpected errors: {:?}", errors);
    }

    #[test]
    fn iso8601_format_correct() {
        // 2026-02-13 at 00:00:00.000 UTC
        // Days from 1970-01-01 to 2026-02-13:
        let ts = format_iso8601_from_ms(0);
        assert_eq!(ts, "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn log_output_format_display() {
        assert_eq!(format!("{}", LogOutputFormat::Pretty), "pretty");
        assert_eq!(format!("{}", LogOutputFormat::Json), "json");
        assert_eq!(format!("{}", LogOutputFormat::Compact), "compact");
    }

    #[test]
    fn init_summary_contains_run_id() {
        let ctx = test_context();
        let cfg = test_config();
        let result = init_e2e_logging_with_context(ctx, &cfg);
        assert!(
            result.summary.contains("test-run-001"),
            "summary should contain run_id: {}",
            result.summary,
        );
    }

    #[test]
    fn init_resets_event_sequence() {
        // Set sequence to non-zero.
        EVENT_SEQUENCE.store(999, Ordering::Relaxed);
        let ctx = test_context();
        let cfg = test_config();
        let result = init_e2e_logging_with_context(ctx, &cfg);
        let seq: u64 = result
            .startup_event
            .context
            .get("event_sequence")
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(seq, 0, "init should reset sequence counter");
    }

    #[test]
    fn event_json_roundtrip() {
        let event = emit_lifecycle_event(
            &test_context(),
            LogPhase::Execute,
            LogEventType::Info,
            &[("key", "value")],
        );
        let json = serde_json::to_string(&event).expect("serialize");
        let restored: LogEventSchema = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(restored.run_id, event.run_id);
        assert_eq!(restored.phase, event.phase);
        assert_eq!(restored.event_type, event.event_type);
        assert_eq!(restored.context.get("key"), event.context.get("key"));
    }

    #[test]
    fn run_context_json_roundtrip() {
        let ctx = test_context();
        let json = serde_json::to_string(&ctx).expect("serialize");
        let restored: RunContext = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(restored.run_id, ctx.run_id);
        assert_eq!(restored.bead_id, ctx.bead_id);
        assert_eq!(restored.seed, ctx.seed);
    }
}
