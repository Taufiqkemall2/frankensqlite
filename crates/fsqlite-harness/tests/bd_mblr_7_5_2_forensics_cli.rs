use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use fsqlite_harness::evidence_index::{
    ArtifactKind, ArtifactRecord, EVIDENCE_INDEX_SCHEMA_VERSION, InvariantCheck, InvariantVerdict,
    LogReference, RunId, RunRecord, ScenarioOutcome, ScenarioVerdict, run_to_jsonl,
};
use fsqlite_harness::forensics_navigator::{
    QueryFilters, Severity, load_index_from_jsonl, query_index, render_text_report,
};
use tempfile::TempDir;

const BEAD_ID: &str = "bd-mblr.7.5.2";

fn sample_run(
    run_id: &str,
    started_at: &str,
    success: bool,
    seed: u64,
    scenario_verdict: ScenarioVerdict,
    code_area: &str,
    invariant_verdict: InvariantVerdict,
    artifacts: Vec<ArtifactRecord>,
) -> RunRecord {
    RunRecord {
        schema_version: EVIDENCE_INDEX_SCHEMA_VERSION,
        run_id: RunId(run_id.to_owned()),
        started_at: started_at.to_owned(),
        completed_at: Some("2026-02-13T00:10:00Z".to_owned()),
        seed,
        profile: "forensics".to_owned(),
        git_sha: "abc1234".to_owned(),
        toolchain: "nightly-2026-02-10".to_owned(),
        platform: "x86_64-unknown-linux-gnu".to_owned(),
        success,
        scenarios: vec![ScenarioOutcome {
            scenario_id: "SC-001".to_owned(),
            scenario_name: "smoke".to_owned(),
            verdict: scenario_verdict,
            duration_ms: 12,
            first_divergence: None,
            error_message: None,
            code_areas: vec![code_area.to_owned()],
        }],
        invariants: vec![InvariantCheck {
            invariant_id: "INV-1".to_owned(),
            invariant_name: "invariant".to_owned(),
            verdict: invariant_verdict,
            violation_detail: None,
            violation_timestamp: None,
        }],
        artifacts,
        logs: vec![LogReference {
            path: format!("artifacts/{run_id}/events.json"),
            schema_version: "1.0.0".to_owned(),
            event_count: 2,
            phases: vec!["execute".to_owned()],
            has_divergence_markers: false,
        }],
        bead_ids: vec![BEAD_ID.to_owned()],
        feature_flags: vec!["concurrent_mode=true".to_owned()],
        fault_profile: None,
        metadata: BTreeMap::new(),
    }
}

fn artifact(kind: ArtifactKind, path: &str) -> ArtifactRecord {
    ArtifactRecord {
        kind,
        path: path.to_owned(),
        content_hash: "deadbeef".to_owned(),
        size_bytes: 42,
        generated_at: "2026-02-13T00:00:00Z".to_owned(),
        description: None,
    }
}

fn write_jsonl(dir: &TempDir, file_name: &str, runs: &[RunRecord]) -> PathBuf {
    let path = dir.path().join(file_name);
    let payload = runs
        .iter()
        .map(|run| run_to_jsonl(run).expect("serialize run to JSONL"))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&path, payload).expect("write JSONL index");
    path
}

#[test]
fn test_query_filters_sorts_and_limits_timeline() {
    let mut index = fsqlite_harness::evidence_index::EvidenceIndex::new();

    let run_b = sample_run(
        "run-b",
        "2026-02-13T00:00:02Z",
        false,
        2,
        ScenarioVerdict::Fail,
        "planner",
        InvariantVerdict::Held,
        vec![artifact(ArtifactKind::Log, "artifacts/run-b/log.json")],
    );
    let run_a = sample_run(
        "run-a",
        "2026-02-13T00:00:01Z",
        false,
        1,
        ScenarioVerdict::Fail,
        "planner",
        InvariantVerdict::Held,
        vec![artifact(ArtifactKind::Log, "artifacts/run-a/log.json")],
    );
    let other = sample_run(
        "run-c",
        "2026-02-13T00:00:00Z",
        true,
        3,
        ScenarioVerdict::Pass,
        "pager",
        InvariantVerdict::Held,
        vec![artifact(ArtifactKind::Log, "artifacts/run-c/log.json")],
    );

    index.insert(run_b);
    index.insert(run_a);
    index.insert(other);

    let filters = QueryFilters {
        issue_id: Some(BEAD_ID.to_owned()),
        commit: None,
        seed: None,
        component: Some("planner".to_owned()),
        severity: Some(Severity::High),
        limit: Some(1),
    };
    let result = query_index(&index, &filters);

    assert_eq!(result.scanned_run_count, 3);
    assert_eq!(result.matched_run_count, 1);
    assert_eq!(result.timeline[0].run_id, "run-a");
}

#[test]
fn test_query_builds_correlations_and_replay_command() {
    let mut index = fsqlite_harness::evidence_index::EvidenceIndex::new();
    index.insert(sample_run(
        "run-1",
        "2026-02-13T00:00:00Z",
        false,
        7,
        ScenarioVerdict::Fail,
        "planner",
        InvariantVerdict::Violated,
        vec![
            artifact(ArtifactKind::ReplayManifest, "artifacts/run-1/replay-02.json"),
            artifact(ArtifactKind::ReplayManifest, "artifacts/run-1/replay-01.json"),
            artifact(ArtifactKind::FailureBundle, "artifacts/run-1/failure.json"),
        ],
    ));
    index.insert(sample_run(
        "run-2",
        "2026-02-13T00:01:00Z",
        false,
        8,
        ScenarioVerdict::Fail,
        "planner",
        InvariantVerdict::Held,
        vec![artifact(ArtifactKind::Log, "artifacts/run-2/log.json")],
    ));

    let result = query_index(&index, &QueryFilters::default());
    assert_eq!(result.matched_run_count, 2);

    let run_1 = result
        .timeline
        .iter()
        .find(|event| event.run_id == "run-1")
        .expect("run-1 in timeline");
    assert_eq!(
        run_1.replay_command.as_deref(),
        Some(
            "cargo run -p fsqlite-harness --bin replay_harness -- --manifest artifacts/run-1/replay-01.json"
        )
    );

    let run_2 = result
        .timeline
        .iter()
        .find(|event| event.run_id == "run-2")
        .expect("run-2 in timeline");
    assert_eq!(run_2.replay_command, None);

    let planner = result
        .correlations
        .iter()
        .find(|row| row.key == "component:planner")
        .expect("planner correlation");
    assert_eq!(planner.run_count, 2);

    let invariant = result
        .correlations
        .iter()
        .find(|row| row.key == "invariant:INV-1")
        .expect("invariant correlation");
    assert_eq!(invariant.run_count, 1);

    let report = render_text_report(&result);
    assert!(
        report.contains("forensics report"),
        "expected deterministic text report header"
    );
}

#[test]
fn test_load_index_from_jsonl_reports_line_number_on_parse_failure() {
    let temp_dir = tempfile::tempdir().expect("create tempdir");
    let path = temp_dir.path().join("evidence.jsonl");
    let valid_line = run_to_jsonl(&sample_run(
        "run-good",
        "2026-02-13T00:00:00Z",
        true,
        9,
        ScenarioVerdict::Pass,
        "planner",
        InvariantVerdict::Held,
        vec![artifact(ArtifactKind::Log, "artifacts/run-good/log.json")],
    ))
    .expect("serialize valid run");
    fs::write(&path, format!("{valid_line}\n{{not-json}}\n")).expect("write malformed JSONL");

    let error = load_index_from_jsonl(&path).expect_err("expected parse failure");
    assert!(
        error.contains("line 2"),
        "expected line number in parse error, got: {error}"
    );
}

fn forensics_binary_path() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_forensics_navigator"))
}

#[test]
fn test_cli_emits_json_and_applies_seed_filter() {
    let temp_dir = tempfile::tempdir().expect("create tempdir");
    let evidence_path = write_jsonl(
        &temp_dir,
        "evidence.jsonl",
        &[
            sample_run(
                "run-keep",
                "2026-02-13T00:00:00Z",
                false,
                11,
                ScenarioVerdict::Fail,
                "pager",
                InvariantVerdict::Held,
                vec![artifact(ArtifactKind::Log, "artifacts/run-keep/log.json")],
            ),
            sample_run(
                "run-drop",
                "2026-02-13T00:01:00Z",
                true,
                22,
                ScenarioVerdict::Pass,
                "planner",
                InvariantVerdict::Held,
                vec![artifact(ArtifactKind::Log, "artifacts/run-drop/log.json")],
            ),
        ],
    );

    let output = Command::new(forensics_binary_path())
        .arg("--index-jsonl")
        .arg(&evidence_path)
        .arg("--seed")
        .arg("11")
        .arg("--json")
        .output()
        .expect("run forensics_navigator binary");

    assert!(
        output.status.success(),
        "expected success, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let payload: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse CLI JSON payload");
    assert_eq!(payload["bead_id"], BEAD_ID);
    assert_eq!(payload["matched_run_count"], 1);
    assert_eq!(payload["timeline"][0]["run_id"], "run-keep");
}
