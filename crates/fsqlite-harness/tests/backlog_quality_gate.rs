use std::fs;
use std::path::Path;

use serde_json::Value;
use tempfile::tempdir;

use fsqlite_harness::backlog_quality_gate::{
    BACKLOG_QUALITY_GATE_SCHEMA_VERSION, BacklogQualityBaseline, BacklogQualityBaselineEntry,
    BacklogQualityGateConfig, RequirementKind, run_backlog_quality_gate, write_report_json,
};

const BEAD_ID: &str = "bd-1dp9.9.6";

fn issue(
    id: &str,
    status: &str,
    priority: i64,
    acceptance_criteria: &str,
    labels: &[&str],
) -> Value {
    serde_json::json!({
        "id": id,
        "title": format!("Issue {id}"),
        "status": status,
        "issue_type": "task",
        "priority": priority,
        "acceptance_criteria": acceptance_criteria,
        "labels": labels,
    })
}

fn write_jsonl(path: &Path, entries: &[Value]) {
    let body = entries
        .iter()
        .map(|entry| serde_json::to_string(entry).expect("serialize entry"))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(path, format!("{body}\n")).expect("write jsonl");
}

fn canonical_acceptance() -> &'static str {
    "Comprehensive unit/property tests are required. Deterministic e2e scenarios \
     with fixed seeds and replay instructions are mandatory. Structured logging/metrics \
     must include trace_id, run_id, scenario_id, and actionable failure context."
}

#[test]
fn report_json_schema_and_roundtrip_are_stable() {
    let temp = tempdir().expect("tempdir");
    let beads = temp.path().join("issues.jsonl");
    let report_path = temp.path().join("report.json");
    write_jsonl(
        &beads,
        &[issue(
            "bd-pass",
            "open",
            1,
            canonical_acceptance(),
            &["ev-gated"],
        )],
    );

    let report = run_backlog_quality_gate(&BacklogQualityGateConfig {
        beads_path: beads,
        baseline_path: None,
        critical_priority_max: 1,
        generated_unix_ms: Some(1_700_000_000_000),
    })
    .expect("gate should run");
    write_report_json(&report_path, &report).expect("write report");

    let restored: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(report_path).expect("read report"))
            .expect("parse report");
    assert_eq!(
        restored["schema_version"].as_str(),
        Some(BACKLOG_QUALITY_GATE_SCHEMA_VERSION),
        "bead_id={BEAD_ID} case=schema_version_mismatch"
    );
    assert_eq!(
        restored["overall_pass"].as_bool(),
        Some(true),
        "bead_id={BEAD_ID} case=overall_pass_expected_true"
    );
}

#[test]
fn baseline_mode_flags_only_new_critical_regressions() {
    let temp = tempdir().expect("tempdir");
    let beads = temp.path().join("issues.jsonl");
    let baseline = temp.path().join("baseline.json");

    let known_failure_acceptance =
        "Unit tests only. Missing deterministic e2e seed/replay and trace_id logging.";
    write_jsonl(
        &beads,
        &[
            issue(
                "bd-known",
                "open",
                1,
                known_failure_acceptance,
                &["ev-gated"],
            ),
            issue("bd-good", "open", 1, canonical_acceptance(), &["ev-gated"]),
        ],
    );
    let baseline_doc = BacklogQualityBaseline {
        schema_version: BACKLOG_QUALITY_GATE_SCHEMA_VERSION.to_owned(),
        entries: vec![BacklogQualityBaselineEntry {
            issue_id: "bd-known".to_owned(),
            missing_requirements: vec![
                RequirementKind::DeterministicE2e,
                RequirementKind::StructuredLogging,
            ],
        }],
    };
    fs::write(
        &baseline,
        serde_json::to_string_pretty(&baseline_doc).expect("serialize baseline"),
    )
    .expect("write baseline");

    let report_no_regression = run_backlog_quality_gate(&BacklogQualityGateConfig {
        beads_path: beads.clone(),
        baseline_path: Some(baseline.clone()),
        critical_priority_max: 1,
        generated_unix_ms: Some(1_700_000_000_123),
    })
    .expect("gate should run");
    assert!(
        report_no_regression.overall_pass,
        "bead_id={BEAD_ID} case=known_failure_should_not_regress"
    );
    assert_eq!(
        report_no_regression.regression_failures.len(),
        0,
        "bead_id={BEAD_ID} case=known_failure_regression_count"
    );

    write_jsonl(
        &beads,
        &[
            issue(
                "bd-known",
                "open",
                1,
                known_failure_acceptance,
                &["ev-gated"],
            ),
            issue("bd-new", "open", 1, "Unit tests only.", &["ev-gated"]),
        ],
    );
    let report_regression = run_backlog_quality_gate(&BacklogQualityGateConfig {
        beads_path: beads,
        baseline_path: Some(baseline),
        critical_priority_max: 1,
        generated_unix_ms: Some(1_700_000_000_123),
    })
    .expect("gate should run");
    assert!(
        !report_regression.overall_pass,
        "bead_id={BEAD_ID} case=new_failure_must_fail"
    );
    assert_eq!(
        report_regression.regression_failures.len(),
        1,
        "bead_id={BEAD_ID} case=new_failure_regression_count"
    );
    assert_eq!(
        report_regression.regression_failures[0].issue_id, "bd-new",
        "bead_id={BEAD_ID} case=new_failure_issue_id"
    );
}

#[test]
fn fixed_timestamp_produces_deterministic_report_bytes() {
    let temp = tempdir().expect("tempdir");
    let beads = temp.path().join("issues.jsonl");
    write_jsonl(
        &beads,
        &[
            issue("bd-z", "open", 1, canonical_acceptance(), &["ev-gated"]),
            issue("bd-a", "open", 1, "Unit tests only.", &["ev-gated"]),
        ],
    );

    let report_a = run_backlog_quality_gate(&BacklogQualityGateConfig {
        beads_path: beads.clone(),
        baseline_path: None,
        critical_priority_max: 1,
        generated_unix_ms: Some(424_242_424_242),
    })
    .expect("gate should run");
    let report_b = run_backlog_quality_gate(&BacklogQualityGateConfig {
        beads_path: beads,
        baseline_path: None,
        critical_priority_max: 1,
        generated_unix_ms: Some(424_242_424_242),
    })
    .expect("gate should run");

    let bytes_a = serde_json::to_vec(&report_a).expect("serialize report");
    let bytes_b = serde_json::to_vec(&report_b).expect("serialize report");
    assert_eq!(
        bytes_a, bytes_b,
        "bead_id={BEAD_ID} case=deterministic_bytes_with_fixed_timestamp"
    );
}
