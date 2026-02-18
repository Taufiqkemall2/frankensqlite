//! Structured logging initialization for realdb-e2e commands (bd-mblr.5.2.1).
//!
//! Adapts the generic [`e2e_logging_init`](crate::e2e_logging_init) infrastructure
//! to the realdb-e2e command structure. Each command (run, bench, corrupt, compare)
//! gets consistent structured logging with command-specific metadata.
//!
//! # Architecture
//!
//! The realdb-e2e tool supports four commands:
//! - **Run**: Execute E2E scenarios against the real database backend
//! - **Bench**: Run performance benchmarks with statistical collection
//! - **Corrupt**: Inject controlled corruption for recovery testing
//! - **Compare**: Differential comparison between fsqlite and reference SQLite
//!
//! Each command initializes logging through [`init_realdb_logging`], which:
//! 1. Creates a [`RunContext`] from environment and CLI flags
//! 2. Injects command-specific metadata (command name, config flags)
//! 3. Emits a compliant startup event
//!
//! # Upstream Dependencies
//!
//! - [`e2e_logging_init`](crate::e2e_logging_init) (bd-mblr.5.1.1)
//! - [`e2e_log_schema`](crate::e2e_log_schema) (bd-1dp9.7.2)
//!
//! # Downstream Consumers
//!
//! - **bd-mblr.5.4.1**: Golden tests for log quality validate events

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::e2e_log_schema::{LogEventSchema, LogEventType, LogPhase};
use crate::e2e_logging_init::{
    E2eLoggingConfig, LoggingInitResult, RunContext, emit_lifecycle_event,
    init_e2e_logging_with_context,
};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-mblr.5.2.1";

// ---------------------------------------------------------------------------
// Command classification
// ---------------------------------------------------------------------------

/// realdb-e2e command types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RealdbCommand {
    /// Execute E2E scenarios against real storage.
    Run,
    /// Performance benchmarking with statistical collection.
    Bench,
    /// Controlled corruption injection for recovery testing.
    Corrupt,
    /// Differential comparison between backends.
    Compare,
}

impl RealdbCommand {
    /// All defined commands.
    pub const ALL: [Self; 4] = [Self::Run, Self::Bench, Self::Corrupt, Self::Compare];

    /// Stable string identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Bench => "bench",
            Self::Corrupt => "corrupt",
            Self::Compare => "compare",
        }
    }

    /// Default log phase for initial events of this command.
    #[must_use]
    pub const fn default_phase(self) -> LogPhase {
        match self {
            Self::Run | Self::Corrupt | Self::Bench => LogPhase::Execute,
            Self::Compare => LogPhase::Validate,
        }
    }

    /// Whether this command typically produces comparison artifacts.
    #[must_use]
    pub const fn produces_comparison_artifacts(self) -> bool {
        matches!(self, Self::Compare)
    }

    /// Whether this command involves fault injection.
    #[must_use]
    pub const fn involves_fault_injection(self) -> bool {
        matches!(self, Self::Corrupt)
    }
}

impl fmt::Display for RealdbCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Command-specific config
// ---------------------------------------------------------------------------

/// Additional metadata attached to a realdb-e2e logging session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealdbCommandMeta {
    /// Which command is being executed.
    pub command: RealdbCommand,
    /// Database path or identifier under test.
    pub db_path: Option<String>,
    /// Page size configuration (if applicable).
    pub page_size: Option<u32>,
    /// Whether WAL mode is enabled.
    pub wal_mode: bool,
    /// Whether concurrent mode is active (always true for MVCC).
    pub concurrent_mode: bool,
}

impl Default for RealdbCommandMeta {
    fn default() -> Self {
        Self {
            command: RealdbCommand::Run,
            db_path: None,
            page_size: None,
            wal_mode: true,
            concurrent_mode: true, // MVCC always on
        }
    }
}

impl RealdbCommandMeta {
    /// Create metadata for a specific command.
    #[must_use]
    pub fn for_command(command: RealdbCommand) -> Self {
        Self {
            command,
            ..Default::default()
        }
    }

    /// Builder: set database path.
    #[must_use]
    pub fn with_db_path(mut self, path: &str) -> Self {
        self.db_path = Some(path.to_owned());
        self
    }

    /// Builder: set page size.
    #[must_use]
    pub fn with_page_size(mut self, size: u32) -> Self {
        self.page_size = Some(size);
        self
    }
}

// ---------------------------------------------------------------------------
// Initialization result
// ---------------------------------------------------------------------------

/// Result of initializing structured logging for a realdb-e2e command.
#[derive(Debug, Clone)]
pub struct RealdbLoggingResult {
    /// Core logging init result.
    pub core: LoggingInitResult,
    /// Command metadata.
    pub meta: RealdbCommandMeta,
    /// Startup event with command context.
    pub command_startup_event: LogEventSchema,
}

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

/// Initialize structured logging for a realdb-e2e command.
///
/// Extends the generic [`init_e2e_logging_with_context`] with command-specific
/// metadata in the event context fields.
#[must_use]
pub fn init_realdb_logging(
    context: RunContext,
    config: &E2eLoggingConfig,
    meta: &RealdbCommandMeta,
) -> RealdbLoggingResult {
    let core = init_e2e_logging_with_context(context, config);

    // Emit a command-specific startup event with additional metadata.
    let mut extra = vec![("realdb_command", meta.command.as_str())];
    let db_path_owned;
    if let Some(ref path) = meta.db_path {
        db_path_owned = path.clone();
        extra.push(("db_path", &db_path_owned));
    }
    let page_str;
    if let Some(size) = meta.page_size {
        page_str = size.to_string();
        extra.push(("page_size", &page_str));
    }
    let wal_str = if meta.wal_mode { "true" } else { "false" };
    extra.push(("wal_mode", wal_str));
    let concurrent_str = if meta.concurrent_mode {
        "true"
    } else {
        "false"
    };
    extra.push(("concurrent_mode", concurrent_str));

    let command_startup_event =
        emit_lifecycle_event(&core.context, LogPhase::Setup, LogEventType::Start, &extra);

    RealdbLoggingResult {
        core,
        meta: meta.clone(),
        command_startup_event,
    }
}

/// Emit a command-phase transition event.
#[must_use]
pub fn emit_command_event(
    ctx: &RunContext,
    command: RealdbCommand,
    event_type: LogEventType,
    message: &str,
) -> LogEventSchema {
    emit_lifecycle_event(
        ctx,
        command.default_phase(),
        event_type,
        &[("realdb_command", command.as_str()), ("message", message)],
    )
}

/// Emit a command completion event with summary statistics.
#[must_use]
pub fn emit_command_completion(
    ctx: &RunContext,
    command: RealdbCommand,
    passed: bool,
    details: &[(&str, &str)],
) -> LogEventSchema {
    let event_type = if passed {
        LogEventType::Pass
    } else {
        LogEventType::Fail
    };

    let mut extra = vec![("realdb_command", command.as_str())];
    extra.extend_from_slice(details);

    emit_lifecycle_event(ctx, LogPhase::Report, event_type, &extra)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_context() -> RunContext {
        RunContext::new(
            "realdb-test-001",
            "bd-test",
            Some("STOR-1"),
            Some(99),
            Some("fsqlite"),
        )
    }

    fn test_config() -> E2eLoggingConfig {
        E2eLoggingConfig::test()
    }

    #[test]
    fn command_as_str() {
        assert_eq!(RealdbCommand::Run.as_str(), "run");
        assert_eq!(RealdbCommand::Bench.as_str(), "bench");
        assert_eq!(RealdbCommand::Corrupt.as_str(), "corrupt");
        assert_eq!(RealdbCommand::Compare.as_str(), "compare");
    }

    #[test]
    fn command_display() {
        assert_eq!(format!("{}", RealdbCommand::Run), "run");
        assert_eq!(format!("{}", RealdbCommand::Compare), "compare");
    }

    #[test]
    fn command_all_has_four() {
        assert_eq!(RealdbCommand::ALL.len(), 4);
    }

    #[test]
    fn default_phase_mapping() {
        assert_eq!(RealdbCommand::Run.default_phase(), LogPhase::Execute);
        assert_eq!(RealdbCommand::Bench.default_phase(), LogPhase::Execute);
        assert_eq!(RealdbCommand::Corrupt.default_phase(), LogPhase::Execute);
        assert_eq!(RealdbCommand::Compare.default_phase(), LogPhase::Validate);
    }

    #[test]
    fn corrupt_involves_fault_injection() {
        assert!(RealdbCommand::Corrupt.involves_fault_injection());
        assert!(!RealdbCommand::Run.involves_fault_injection());
    }

    #[test]
    fn compare_produces_comparison_artifacts() {
        assert!(RealdbCommand::Compare.produces_comparison_artifacts());
        assert!(!RealdbCommand::Run.produces_comparison_artifacts());
    }

    #[test]
    fn default_meta_has_concurrent_mode() {
        let meta = RealdbCommandMeta::default();
        assert!(
            meta.concurrent_mode,
            "MVCC concurrent_mode must always be true"
        );
    }

    #[test]
    fn meta_builder() {
        let meta = RealdbCommandMeta::for_command(RealdbCommand::Bench)
            .with_db_path("/tmp/test.db")
            .with_page_size(4096);
        assert_eq!(meta.command, RealdbCommand::Bench);
        assert_eq!(meta.db_path.as_deref(), Some("/tmp/test.db"));
        assert_eq!(meta.page_size, Some(4096));
    }

    #[test]
    fn init_realdb_logging_produces_events() {
        let ctx = test_context();
        let cfg = test_config();
        let meta = RealdbCommandMeta::for_command(RealdbCommand::Run);
        let result = init_realdb_logging(ctx, &cfg, &meta);

        assert_eq!(result.core.startup_event.phase, LogPhase::Setup);
        assert_eq!(result.command_startup_event.phase, LogPhase::Setup);
    }

    #[test]
    fn command_startup_carries_realdb_command() {
        let ctx = test_context();
        let cfg = test_config();
        let meta = RealdbCommandMeta::for_command(RealdbCommand::Compare);
        let result = init_realdb_logging(ctx, &cfg, &meta);

        assert_eq!(
            result
                .command_startup_event
                .context
                .get("realdb_command")
                .map(String::as_str),
            Some("compare"),
        );
    }

    #[test]
    fn command_startup_carries_db_path() {
        let ctx = test_context();
        let cfg = test_config();
        let meta = RealdbCommandMeta::for_command(RealdbCommand::Run).with_db_path("/data/test.db");
        let result = init_realdb_logging(ctx, &cfg, &meta);

        assert_eq!(
            result
                .command_startup_event
                .context
                .get("db_path")
                .map(String::as_str),
            Some("/data/test.db"),
        );
    }

    #[test]
    fn command_startup_carries_concurrent_mode() {
        let ctx = test_context();
        let cfg = test_config();
        let meta = RealdbCommandMeta::for_command(RealdbCommand::Run);
        let result = init_realdb_logging(ctx, &cfg, &meta);

        assert_eq!(
            result
                .command_startup_event
                .context
                .get("concurrent_mode")
                .map(String::as_str),
            Some("true"),
        );
    }

    #[test]
    fn consistent_init_across_all_commands() {
        for cmd in RealdbCommand::ALL {
            let ctx = RunContext::new(
                &format!("test-{cmd}"),
                "bd-test",
                None,
                Some(42),
                Some("fsqlite"),
            );
            let cfg = test_config();
            let meta = RealdbCommandMeta::for_command(cmd);
            let result = init_realdb_logging(ctx, &cfg, &meta);

            // All commands should produce a Setup/Start startup event.
            assert_eq!(
                result.core.startup_event.phase,
                LogPhase::Setup,
                "command {cmd}: wrong startup phase",
            );
            assert_eq!(
                result.core.startup_event.event_type,
                LogEventType::Start,
                "command {cmd}: wrong startup event type",
            );
            // All should have the command in context.
            assert_eq!(
                result
                    .command_startup_event
                    .context
                    .get("realdb_command")
                    .map(String::as_str),
                Some(cmd.as_str()),
                "command {cmd}: missing realdb_command in context",
            );
        }
    }

    #[test]
    fn emit_command_event_carries_command_and_message() {
        let ctx = test_context();
        let event = emit_command_event(
            &ctx,
            RealdbCommand::Bench,
            LogEventType::Info,
            "starting bench",
        );
        assert_eq!(
            event.context.get("realdb_command").map(String::as_str),
            Some("bench"),
        );
        assert_eq!(
            event.context.get("message").map(String::as_str),
            Some("starting bench"),
        );
    }

    #[test]
    fn emit_command_completion_pass() {
        let ctx = test_context();
        let event =
            emit_command_completion(&ctx, RealdbCommand::Run, true, &[("scenarios_run", "15")]);
        assert_eq!(event.event_type, LogEventType::Pass);
        assert_eq!(event.phase, LogPhase::Report);
        assert_eq!(
            event.context.get("scenarios_run").map(String::as_str),
            Some("15"),
        );
    }

    #[test]
    fn emit_command_completion_fail() {
        let ctx = test_context();
        let event = emit_command_completion(
            &ctx,
            RealdbCommand::Corrupt,
            false,
            &[("recovery_failures", "3")],
        );
        assert_eq!(event.event_type, LogEventType::Fail);
    }

    #[test]
    fn command_json_roundtrip() {
        let cmd = RealdbCommand::Compare;
        let json = serde_json::to_string(&cmd).expect("serialize");
        let restored: RealdbCommand = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(restored, cmd);
    }

    #[test]
    fn meta_json_roundtrip() {
        let meta = RealdbCommandMeta::for_command(RealdbCommand::Bench)
            .with_db_path("/tmp/test.db")
            .with_page_size(8192);
        let json = serde_json::to_string(&meta).expect("serialize");
        let restored: RealdbCommandMeta = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(restored.command, RealdbCommand::Bench);
        assert_eq!(restored.db_path.as_deref(), Some("/tmp/test.db"));
        assert_eq!(restored.page_size, Some(8192));
    }

    #[test]
    fn run_id_propagated_through_all_events() {
        let ctx = test_context();
        let cfg = test_config();
        let meta = RealdbCommandMeta::for_command(RealdbCommand::Run);
        let result = init_realdb_logging(ctx, &cfg, &meta);

        assert_eq!(result.core.startup_event.run_id, "realdb-test-001");
        assert_eq!(result.command_startup_event.run_id, "realdb-test-001");

        let event = emit_command_event(
            &result.core.context,
            RealdbCommand::Run,
            LogEventType::Info,
            "test",
        );
        assert_eq!(event.run_id, "realdb-test-001");
    }
}
