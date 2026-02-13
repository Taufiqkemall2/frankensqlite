//! Integration tests for bd-mblr.5.2.1 — Wire Structured Logging into realdb-e2e.
//!
//! Validates that realdb-e2e commands initialize structured logging consistently,
//! emit command-specific metadata, and produce schema-compliant events.

use fsqlite_harness::e2e_log_schema::{LogEventType, LogPhase};
use fsqlite_harness::e2e_logging_init::{E2eLoggingConfig, RunContext};
use fsqlite_harness::realdb_e2e_logging::{
    RealdbCommand, RealdbCommandMeta, emit_command_completion, emit_command_event,
    init_realdb_logging,
};

const BEAD_ID: &str = "bd-mblr.5.2.1";

fn make_context(run_id: &str) -> RunContext {
    RunContext::new(
        run_id,
        "bd-mblr.5.2.1",
        Some("REALDB-E2E"),
        Some(42),
        Some("fsqlite"),
    )
}

fn default_config() -> E2eLoggingConfig {
    E2eLoggingConfig::test()
}

// ---------------------------------------------------------------------------
// Initialization across all commands
// ---------------------------------------------------------------------------

#[test]
fn all_commands_produce_setup_startup_event() {
    for cmd in RealdbCommand::ALL {
        let ctx = make_context(&format!("integ-{cmd}"));
        let meta = RealdbCommandMeta::for_command(cmd);
        let result = init_realdb_logging(ctx, &default_config(), &meta);

        assert_eq!(
            result.core.startup_event.phase,
            LogPhase::Setup,
            "bead_id={BEAD_ID} case=startup_phase cmd={cmd}",
        );
        assert_eq!(
            result.core.startup_event.event_type,
            LogEventType::Start,
            "bead_id={BEAD_ID} case=startup_event_type cmd={cmd}",
        );
    }
}

#[test]
fn all_commands_embed_realdb_command_in_context() {
    for cmd in RealdbCommand::ALL {
        let ctx = make_context(&format!("integ-ctx-{cmd}"));
        let meta = RealdbCommandMeta::for_command(cmd);
        let result = init_realdb_logging(ctx, &default_config(), &meta);

        assert_eq!(
            result
                .command_startup_event
                .context
                .get("realdb_command")
                .map(String::as_str),
            Some(cmd.as_str()),
            "bead_id={BEAD_ID} case=cmd_context cmd={cmd}",
        );
    }
}

#[test]
fn all_commands_propagate_run_id() {
    for cmd in RealdbCommand::ALL {
        let run_id = format!("runid-{cmd}");
        let ctx = make_context(&run_id);
        let meta = RealdbCommandMeta::for_command(cmd);
        let result = init_realdb_logging(ctx, &default_config(), &meta);

        assert_eq!(
            result.core.startup_event.run_id, run_id,
            "bead_id={BEAD_ID} case=run_id_core cmd={cmd}",
        );
        assert_eq!(
            result.command_startup_event.run_id, run_id,
            "bead_id={BEAD_ID} case=run_id_cmd cmd={cmd}",
        );
    }
}

// ---------------------------------------------------------------------------
// Command metadata propagation
// ---------------------------------------------------------------------------

#[test]
fn db_path_propagated_in_startup_event() {
    let ctx = make_context("integ-dbpath");
    let meta = RealdbCommandMeta::for_command(RealdbCommand::Run).with_db_path("/data/test.db");
    let result = init_realdb_logging(ctx, &default_config(), &meta);

    assert_eq!(
        result
            .command_startup_event
            .context
            .get("db_path")
            .map(String::as_str),
        Some("/data/test.db"),
        "bead_id={BEAD_ID} case=db_path_prop",
    );
}

#[test]
fn page_size_propagated_in_startup_event() {
    let ctx = make_context("integ-pgsz");
    let meta = RealdbCommandMeta::for_command(RealdbCommand::Bench).with_page_size(8192);
    let result = init_realdb_logging(ctx, &default_config(), &meta);

    assert_eq!(
        result
            .command_startup_event
            .context
            .get("page_size")
            .map(String::as_str),
        Some("8192"),
        "bead_id={BEAD_ID} case=page_size_prop",
    );
}

#[test]
fn wal_and_concurrent_mode_in_startup_event() {
    let ctx = make_context("integ-modes");
    let meta = RealdbCommandMeta::for_command(RealdbCommand::Run);
    let result = init_realdb_logging(ctx, &default_config(), &meta);

    assert_eq!(
        result
            .command_startup_event
            .context
            .get("wal_mode")
            .map(String::as_str),
        Some("true"),
        "bead_id={BEAD_ID} case=wal_mode_prop",
    );
    assert_eq!(
        result
            .command_startup_event
            .context
            .get("concurrent_mode")
            .map(String::as_str),
        Some("true"),
        "bead_id={BEAD_ID} case=concurrent_mode_prop",
    );
}

// ---------------------------------------------------------------------------
// Mid-command events
// ---------------------------------------------------------------------------

#[test]
fn emit_command_event_carries_correct_phase() {
    let ctx = make_context("integ-phase");
    let event = emit_command_event(
        &ctx,
        RealdbCommand::Compare,
        LogEventType::Info,
        "comparing results",
    );

    assert_eq!(
        event.phase,
        RealdbCommand::Compare.default_phase(),
        "bead_id={BEAD_ID} case=compare_phase",
    );
    assert_eq!(
        event.context.get("realdb_command").map(String::as_str),
        Some("compare"),
        "bead_id={BEAD_ID} case=compare_cmd_ctx",
    );
    assert_eq!(
        event.context.get("message").map(String::as_str),
        Some("comparing results"),
        "bead_id={BEAD_ID} case=compare_msg",
    );
}

#[test]
fn emit_command_event_preserves_run_id() {
    let ctx = make_context("integ-evt-rid");
    let event = emit_command_event(
        &ctx,
        RealdbCommand::Corrupt,
        LogEventType::Info,
        "injecting fault",
    );

    assert_eq!(
        event.run_id, "integ-evt-rid",
        "bead_id={BEAD_ID} case=event_run_id",
    );
}

// ---------------------------------------------------------------------------
// Completion events
// ---------------------------------------------------------------------------

#[test]
fn completion_pass_emits_report_phase_with_pass_type() {
    let ctx = make_context("integ-comp-pass");
    let event = emit_command_completion(
        &ctx,
        RealdbCommand::Run,
        true,
        &[("scenarios", "25"), ("failures", "0")],
    );

    assert_eq!(
        event.phase,
        LogPhase::Report,
        "bead_id={BEAD_ID} case=completion_phase",
    );
    assert_eq!(
        event.event_type,
        LogEventType::Pass,
        "bead_id={BEAD_ID} case=completion_pass",
    );
    assert_eq!(
        event.context.get("scenarios").map(String::as_str),
        Some("25"),
        "bead_id={BEAD_ID} case=completion_detail",
    );
}

#[test]
fn completion_fail_emits_fail_type() {
    let ctx = make_context("integ-comp-fail");
    let event = emit_command_completion(
        &ctx,
        RealdbCommand::Corrupt,
        false,
        &[("recovery_failures", "2")],
    );

    assert_eq!(
        event.event_type,
        LogEventType::Fail,
        "bead_id={BEAD_ID} case=completion_fail",
    );
}

// ---------------------------------------------------------------------------
// Command classification
// ---------------------------------------------------------------------------

#[test]
fn command_enum_has_four_variants() {
    assert_eq!(
        RealdbCommand::ALL.len(),
        4,
        "bead_id={BEAD_ID} case=cmd_count",
    );
}

#[test]
fn command_as_str_stable_values() {
    assert_eq!(RealdbCommand::Run.as_str(), "run");
    assert_eq!(RealdbCommand::Bench.as_str(), "bench");
    assert_eq!(RealdbCommand::Corrupt.as_str(), "corrupt");
    assert_eq!(RealdbCommand::Compare.as_str(), "compare");
}

#[test]
fn command_trait_properties() {
    assert!(
        RealdbCommand::Corrupt.involves_fault_injection(),
        "bead_id={BEAD_ID} case=corrupt_fault",
    );
    assert!(
        !RealdbCommand::Run.involves_fault_injection(),
        "bead_id={BEAD_ID} case=run_no_fault",
    );
    assert!(
        RealdbCommand::Compare.produces_comparison_artifacts(),
        "bead_id={BEAD_ID} case=compare_artifacts",
    );
    assert!(
        !RealdbCommand::Bench.produces_comparison_artifacts(),
        "bead_id={BEAD_ID} case=bench_no_artifacts",
    );
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

#[test]
fn command_json_roundtrip() {
    for cmd in RealdbCommand::ALL {
        let json = serde_json::to_string(&cmd).expect("serialize");
        let restored: RealdbCommand = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(
            restored, cmd,
            "bead_id={BEAD_ID} case=cmd_roundtrip cmd={cmd}",
        );
    }
}

#[test]
fn meta_json_roundtrip() {
    let meta = RealdbCommandMeta::for_command(RealdbCommand::Compare)
        .with_db_path("/tmp/compare.db")
        .with_page_size(16384);
    let json = serde_json::to_string(&meta).expect("serialize");
    let restored: RealdbCommandMeta = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(
        restored.command,
        RealdbCommand::Compare,
        "bead_id={BEAD_ID} case=meta_cmd",
    );
    assert_eq!(
        restored.db_path.as_deref(),
        Some("/tmp/compare.db"),
        "bead_id={BEAD_ID} case=meta_path",
    );
    assert_eq!(
        restored.page_size,
        Some(16384),
        "bead_id={BEAD_ID} case=meta_pgsz",
    );
}

// ---------------------------------------------------------------------------
// Default meta invariants
// ---------------------------------------------------------------------------

#[test]
fn default_meta_always_has_concurrent_mode() {
    let meta = RealdbCommandMeta::default();
    assert!(
        meta.concurrent_mode,
        "bead_id={BEAD_ID} case=default_concurrent",
    );
    assert!(meta.wal_mode, "bead_id={BEAD_ID} case=default_wal",);
}

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

#[test]
fn init_is_deterministic_across_same_context() {
    let ctx1 = make_context("det-run");
    let ctx2 = make_context("det-run");
    let cfg = default_config();
    let meta = RealdbCommandMeta::for_command(RealdbCommand::Run);

    let r1 = init_realdb_logging(ctx1, &cfg, &meta);
    let r2 = init_realdb_logging(ctx2, &cfg, &meta);

    assert_eq!(
        r1.command_startup_event.run_id, r2.command_startup_event.run_id,
        "bead_id={BEAD_ID} case=det_run_id",
    );
    assert_eq!(
        r1.command_startup_event.phase, r2.command_startup_event.phase,
        "bead_id={BEAD_ID} case=det_phase",
    );
    assert_eq!(
        r1.command_startup_event.event_type, r2.command_startup_event.event_type,
        "bead_id={BEAD_ID} case=det_event_type",
    );
}
