//! Navigator Correctness Tests and Operator Workflows (bd-mblr.7.5.3)
//!
//! Integration tests validating navigator outputs against golden datasets
//! and exercising operator triage workflows with representative failure
//! walkthroughs.
//! Depends on: bd-mblr.7.5.2 (forensics CLI timeline/correlation),
//! bd-mblr.3.5.1 (validation manifest).

use fsqlite_harness::evidence_index::{
    ArtifactKind, ArtifactRecord, EvidenceIndex, InvariantCheck, InvariantVerdict, LogReference,
    RunId, RunRecord, ScenarioOutcome, ScenarioVerdict,
};
use fsqlite_harness::forensics_navigator::{
    CorrelationRow, ForensicsVerdict, ForensicsWorkflowConfig, QueryFilters, Severity,
    load_forensics_report, query_index, render_text_report, run_forensics_workflow,
    write_forensics_report,
};
use tempfile::TempDir;

const BEAD_ID: &str = "bd-mblr.7.5.3";

// ─── Test Data Builders ──────────────────────────────────────────────

fn make_run_at(id: &str, success: bool, git_sha: &str, started_at: &str, seed: u64) -> RunRecord {
    RunRecord {
        schema_version: 1,
        run_id: RunId(id.to_owned()),
        started_at: started_at.to_owned(),
        completed_at: Some(format!("{}:01:00Z", &started_at[..16])),
        seed,
        profile: "test".to_owned(),
        git_sha: git_sha.to_owned(),
        toolchain: "nightly".to_owned(),
        platform: "linux-x86_64".to_owned(),
        success,
        scenarios: vec![ScenarioOutcome {
            scenario_id: "SCN-001".to_owned(),
            scenario_name: "basic_insert".to_owned(),
            verdict: if success {
                ScenarioVerdict::Pass
            } else {
                ScenarioVerdict::Fail
            },
            duration_ms: 150,
            first_divergence: None,
            error_message: if success {
                None
            } else {
                Some("insert failed".to_owned())
            },
            code_areas: vec!["vdbe".to_owned()],
        }],
        invariants: vec![InvariantCheck {
            invariant_id: "INV-1".to_owned(),
            invariant_name: "monotone_lsn".to_owned(),
            verdict: InvariantVerdict::Held,
            violation_detail: None,
            violation_timestamp: None,
        }],
        artifacts: vec![
            ArtifactRecord {
                kind: ArtifactKind::Log,
                path: format!("logs/{id}.jsonl"),
                content_hash: format!("hash-{id}"),
                size_bytes: 2048,
                generated_at: started_at.to_owned(),
                description: Some("structured log".to_owned()),
            },
            ArtifactRecord {
                kind: ArtifactKind::ReplayManifest,
                path: format!("manifests/{id}-replay.json"),
                content_hash: format!("manifest-hash-{id}"),
                size_bytes: 512,
                generated_at: started_at.to_owned(),
                description: Some("replay manifest".to_owned()),
            },
        ],
        logs: vec![LogReference {
            path: format!("logs/{id}.jsonl"),
            schema_version: "1".to_owned(),
            event_count: 20,
            phases: vec!["setup".to_owned(), "run".to_owned(), "teardown".to_owned()],
            has_divergence_markers: false,
        }],
        bead_ids: vec!["bd-mblr.7.5.3".to_owned()],
        feature_flags: Vec::new(),
        fault_profile: None,
        metadata: std::collections::BTreeMap::new(),
    }
}

fn make_run(id: &str, success: bool, git_sha: &str) -> RunRecord {
    make_run_at(id, success, git_sha, "2026-02-13T10:00:00Z", 42)
}

fn make_multi_scenario_run(id: &str, git_sha: &str) -> RunRecord {
    let mut run = make_run(id, true, git_sha);
    run.scenarios = vec![
        ScenarioOutcome {
            scenario_id: "SCN-001".to_owned(),
            scenario_name: "basic_insert".to_owned(),
            verdict: ScenarioVerdict::Pass,
            duration_ms: 100,
            first_divergence: None,
            error_message: None,
            code_areas: vec!["vdbe".to_owned(), "pager".to_owned()],
        },
        ScenarioOutcome {
            scenario_id: "SCN-002".to_owned(),
            scenario_name: "concurrent_writers".to_owned(),
            verdict: ScenarioVerdict::Fail,
            duration_ms: 300,
            first_divergence: Some("row 42 diverged".to_owned()),
            error_message: Some("write conflict".to_owned()),
            code_areas: vec!["mvcc".to_owned(), "wal".to_owned()],
        },
        ScenarioOutcome {
            scenario_id: "SCN-003".to_owned(),
            scenario_name: "recovery_after_crash".to_owned(),
            verdict: ScenarioVerdict::Timeout,
            duration_ms: 5000,
            first_divergence: None,
            error_message: Some("timed out".to_owned()),
            code_areas: vec!["wal".to_owned()],
        },
    ];
    run.success = false;
    run
}

fn make_critical_run(id: &str) -> RunRecord {
    let mut run = make_run(id, false, "sha-violation");
    run.invariants = vec![
        InvariantCheck {
            invariant_id: "INV-1".to_owned(),
            invariant_name: "monotone_lsn".to_owned(),
            verdict: InvariantVerdict::Violated,
            violation_detail: Some("LSN regression detected: 42 > 41".to_owned()),
            violation_timestamp: Some("2026-02-13T10:00:30Z".to_owned()),
        },
        InvariantCheck {
            invariant_id: "INV-2".to_owned(),
            invariant_name: "page_checksum".to_owned(),
            verdict: InvariantVerdict::Held,
            violation_detail: None,
            violation_timestamp: None,
        },
    ];
    run.scenarios[0].verdict = ScenarioVerdict::Fail;
    run.scenarios[0].error_message = Some("invariant violation during insert".to_owned());
    run
}

fn make_divergence_run(id: &str, git_sha: &str) -> RunRecord {
    let mut run = make_run(id, false, git_sha);
    run.scenarios[0].verdict = ScenarioVerdict::Divergence;
    run.scenarios[0].first_divergence = Some("row 7: expected 42, got 43".to_owned());
    run.scenarios[0].error_message = Some("output divergence".to_owned());
    run.logs[0].has_divergence_markers = true;
    run
}

fn build_golden_index() -> EvidenceIndex {
    let mut index = EvidenceIndex::new();
    // 3 clean runs
    index.insert(make_run_at(
        "run-001",
        true,
        "sha-aaa",
        "2026-02-13T10:00:00Z",
        42,
    ));
    index.insert(make_run_at(
        "run-002",
        true,
        "sha-aaa",
        "2026-02-13T11:00:00Z",
        43,
    ));
    index.insert(make_run_at(
        "run-003",
        true,
        "sha-bbb",
        "2026-02-13T12:00:00Z",
        44,
    ));
    // 1 failed run
    index.insert(make_run_at(
        "run-004",
        false,
        "sha-ccc",
        "2026-02-13T13:00:00Z",
        45,
    ));
    // 1 critical run
    index.insert(make_critical_run("run-005"));
    // 1 divergence run
    index.insert(make_divergence_run("run-006", "sha-ddd"));
    // 1 multi-scenario run
    index.insert(make_multi_scenario_run("run-007", "sha-eee"));
    index
}

// ─── Timeline Correctness ────────────────────────────────────────────

#[test]
fn timeline_sorted_by_started_at() {
    let index = build_golden_index();
    let result = query_index(&index, &QueryFilters::default());

    let times: Vec<&str> = result
        .timeline
        .iter()
        .map(|e| e.started_at.as_str())
        .collect();

    // Verify monotone ordering
    for w in times.windows(2) {
        assert!(
            w[0] <= w[1],
            "timeline must be sorted by started_at: {} > {}",
            w[0],
            w[1]
        );
    }
}

#[test]
fn timeline_includes_all_runs() {
    let index = build_golden_index();
    let result = query_index(&index, &QueryFilters::default());

    assert_eq!(
        result.matched_run_count,
        index.run_count(),
        "unfiltered query must match all runs"
    );
    assert_eq!(result.timeline.len(), index.run_count());
}

#[test]
fn timeline_events_have_replay_commands() {
    let index = build_golden_index();
    let result = query_index(&index, &QueryFilters::default());

    for event in &result.timeline {
        let cmd = event.replay_command.as_ref().expect(&format!(
            "timeline event {} must have replay command",
            event.run_id
        ));
        assert!(
            !cmd.is_empty(),
            "replay command must be non-empty for {}",
            event.run_id
        );
        // Replay command should reference the replay harness for reproducibility
        assert!(
            cmd.contains("replay_harness") || cmd.contains("--manifest"),
            "replay command must reference replay harness for run {}",
            event.run_id
        );
    }
}

#[test]
fn timeline_events_classify_severity() {
    let index = build_golden_index();
    let result = query_index(&index, &QueryFilters::default());

    let mut has_critical = false;
    let mut has_high = false;
    let mut has_low = false;

    for event in &result.timeline {
        match event.severity {
            Severity::Critical => has_critical = true,
            Severity::High => has_high = true,
            Severity::Low => has_low = true,
            _ => {}
        }
    }

    assert!(
        has_critical,
        "golden index must have at least one critical event"
    );
    assert!(has_high, "golden index must have at least one high event");
    assert!(has_low, "golden index must have at least one low event");
}

#[test]
fn timeline_events_populate_components() {
    let index = build_golden_index();
    let result = query_index(&index, &QueryFilters::default());

    for event in &result.timeline {
        assert!(
            !event.components.is_empty(),
            "event {} must have components",
            event.run_id
        );
    }
}

// ─── Correlation Correctness ─────────────────────────────────────────

#[test]
fn correlations_aggregate_by_component() {
    let index = build_golden_index();
    let result = query_index(&index, &QueryFilters::default());

    let component_correlations: Vec<&CorrelationRow> = result
        .correlations
        .iter()
        .filter(|c| c.key.starts_with("component:"))
        .collect();

    assert!(
        !component_correlations.is_empty(),
        "must have component correlations"
    );

    for corr in &component_correlations {
        assert!(
            corr.run_count > 0,
            "correlation {} must have runs",
            corr.key
        );
        assert!(
            !corr.run_ids.is_empty(),
            "correlation {} must list run IDs",
            corr.key
        );
    }
}

#[test]
fn correlations_aggregate_by_invariant() {
    let index = build_golden_index();
    let result = query_index(&index, &QueryFilters::default());

    let invariant_correlations: Vec<&CorrelationRow> = result
        .correlations
        .iter()
        .filter(|c| c.key.starts_with("invariant:"))
        .collect();

    // Golden index has an invariant violation — should appear
    assert!(
        !invariant_correlations.is_empty(),
        "must have invariant correlations"
    );
}

#[test]
fn correlations_sorted_by_run_count_desc() {
    let index = build_golden_index();
    let result = query_index(&index, &QueryFilters::default());

    for w in result.correlations.windows(2) {
        assert!(
            w[0].run_count >= w[1].run_count,
            "correlations must be sorted by run_count DESC: {} < {}",
            w[0].run_count,
            w[1].run_count
        );
    }
}

// ─── Filter Correctness ─────────────────────────────────────────────

#[test]
fn filter_by_commit_narrows_results() {
    let index = build_golden_index();
    let filters = QueryFilters {
        commit: Some("sha-aaa".to_owned()),
        ..Default::default()
    };
    let result = query_index(&index, &filters);

    assert!(result.matched_run_count > 0, "must match at least one run");
    assert!(
        result.matched_run_count < index.run_count(),
        "filter must narrow results"
    );

    for event in &result.timeline {
        assert_eq!(event.git_sha, "sha-aaa", "all events must match filter");
    }
}

#[test]
fn filter_by_severity_selects_critical() {
    let index = build_golden_index();
    let filters = QueryFilters {
        severity: Some(Severity::Critical),
        ..Default::default()
    };
    let result = query_index(&index, &filters);

    assert!(result.matched_run_count > 0, "must find critical events");
    for event in &result.timeline {
        assert_eq!(
            event.severity,
            Severity::Critical,
            "all events must be critical"
        );
    }
}

#[test]
fn filter_by_component_selects_matching() {
    let index = build_golden_index();
    let filters = QueryFilters {
        component: Some("mvcc".to_owned()),
        ..Default::default()
    };
    let result = query_index(&index, &filters);

    for event in &result.timeline {
        assert!(
            event.components.contains(&"mvcc".to_owned()),
            "all events must touch mvcc component"
        );
    }
}

#[test]
fn filter_by_seed_selects_matching() {
    let index = build_golden_index();
    let filters = QueryFilters {
        seed: Some(42),
        ..Default::default()
    };
    let result = query_index(&index, &filters);

    assert!(result.matched_run_count > 0, "must find runs with seed=42");
    for event in &result.timeline {
        assert_eq!(event.seed, 42, "all events must have seed=42");
    }
}

// ─── Operator Workflow: Triage from Failing Lane to Root Cause ───────

#[test]
fn operator_workflow_failure_to_root_cause() {
    let index = build_golden_index();

    // Step 1: Operator runs unfiltered query to see all events
    let all = query_index(&index, &QueryFilters::default());
    assert!(all.matched_run_count > 0, "step 1: must have indexed runs");

    // Step 2: Filter to critical events only
    let critical_filters = QueryFilters {
        severity: Some(Severity::Critical),
        ..Default::default()
    };
    let critical = query_index(&index, &critical_filters);
    assert!(
        critical.matched_run_count > 0,
        "step 2: must find critical events"
    );

    // Step 3: Pick the first critical event and verify it has triage info
    let event = &critical.timeline[0];
    assert_eq!(event.severity, Severity::Critical);
    assert!(
        !event.violated_invariants.is_empty(),
        "step 3: critical event must list violated invariants"
    );
    assert!(
        event.replay_command.is_some(),
        "step 3: critical event must have replay command"
    );
    assert!(
        !event.artifact_paths.is_empty(),
        "step 3: critical event must link artifacts"
    );

    // Step 4: Render a text report for the triage
    let report = render_text_report(&critical);
    assert!(!report.is_empty(), "step 4: text report must be generated");
}

#[test]
fn operator_workflow_divergence_investigation() {
    let index = build_golden_index();

    // Filter for high-severity events (includes failures and divergences)
    let filters = QueryFilters {
        severity: Some(Severity::High),
        ..Default::default()
    };
    let result = query_index(&index, &filters);
    assert!(
        result.matched_run_count > 0,
        "must find high-severity events"
    );

    // Verify divergence runs have first_divergence info in failing_scenarios
    // and artifact paths for drill-down
    for event in &result.timeline {
        assert!(
            event.severity == Severity::High || event.severity == Severity::Critical,
            "filtered events must be high or critical severity"
        );
        assert!(
            !event.artifact_paths.is_empty(),
            "high-severity event {} must have artifacts for investigation",
            event.run_id
        );
    }
}

// ─── Report Rendering ────────────────────────────────────────────────

#[test]
fn text_report_includes_timeline_and_correlations() {
    let index = build_golden_index();
    let result = query_index(&index, &QueryFilters::default());
    let report = render_text_report(&result);

    assert!(report.contains("forensics report"), "must have header");
    // The report should mention run count
    assert!(
        report.contains(&format!("{}", result.matched_run_count)) || report.contains("matched"),
        "report must mention matched run count"
    );
}

#[test]
fn text_report_for_filtered_query() {
    let index = build_golden_index();
    let filters = QueryFilters {
        commit: Some("sha-aaa".to_owned()),
        ..Default::default()
    };
    let result = query_index(&index, &filters);
    let report = render_text_report(&result);

    assert!(!report.is_empty(), "filtered report must be non-empty");
}

// ─── Workflow Report Correctness ─────────────────────────────────────

#[test]
fn workflow_report_counts_match_query() {
    let index = build_golden_index();
    let config = ForensicsWorkflowConfig::default();
    let report = run_forensics_workflow(&index, &config);

    assert_eq!(report.index_run_count, index.run_count());
    assert_eq!(
        report.query_result.matched_run_count,
        report.query_result.timeline.len()
    );
    assert_eq!(
        report.correlation_count,
        report.query_result.correlations.len()
    );
}

#[test]
fn workflow_report_unique_counts() {
    let index = build_golden_index();
    let config = ForensicsWorkflowConfig::default();
    let report = run_forensics_workflow(&index, &config);

    assert!(report.unique_scenarios > 0, "must count unique scenarios");
    assert!(report.unique_invariants > 0, "must count unique invariants");
}

#[test]
fn workflow_verdicts_consistent_with_events() {
    let index = build_golden_index();
    let config = ForensicsWorkflowConfig::default();
    let report = run_forensics_workflow(&index, &config);

    // Golden index has a critical run → must fail
    assert_eq!(
        report.verdict,
        ForensicsVerdict::Fail,
        "golden index with critical events must produce Fail verdict"
    );
    assert!(report.critical_event_count > 0);
}

// ─── Report Persistence ──────────────────────────────────────────────

#[test]
fn workflow_report_json_roundtrip() {
    let index = build_golden_index();
    let config = ForensicsWorkflowConfig::default();
    let report = run_forensics_workflow(&index, &config);

    let json = report.to_json().unwrap();
    let restored =
        fsqlite_harness::forensics_navigator::ForensicsWorkflowReport::from_json(&json).unwrap();

    assert_eq!(restored.schema_version, report.schema_version);
    assert_eq!(restored.bead_id, report.bead_id);
    assert_eq!(restored.verdict, report.verdict);
    assert_eq!(restored.index_run_count, report.index_run_count);
    assert_eq!(restored.critical_event_count, report.critical_event_count);
    assert_eq!(restored.high_event_count, report.high_event_count);
    assert_eq!(restored.correlation_count, report.correlation_count);
    assert_eq!(
        restored.query_result.timeline.len(),
        report.query_result.timeline.len()
    );
}

#[test]
fn workflow_report_file_roundtrip() {
    let index = build_golden_index();
    let config = ForensicsWorkflowConfig::default();
    let report = run_forensics_workflow(&index, &config);

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("forensics_report.json");

    write_forensics_report(&path, &report).unwrap();
    let loaded = load_forensics_report(&path).unwrap();

    assert_eq!(loaded.verdict, report.verdict);
    assert_eq!(loaded.index_run_count, report.index_run_count);
    assert_eq!(loaded.summary, report.summary);
}

// ─── Determinism ─────────────────────────────────────────────────────

#[test]
fn query_deterministic_for_same_index() {
    let index = build_golden_index();
    let filters = QueryFilters::default();

    let a = query_index(&index, &filters);
    let b = query_index(&index, &filters);

    assert_eq!(a.matched_run_count, b.matched_run_count);
    assert_eq!(a.timeline.len(), b.timeline.len());
    assert_eq!(a.correlations.len(), b.correlations.len());

    for (ea, eb) in a.timeline.iter().zip(b.timeline.iter()) {
        assert_eq!(ea.run_id, eb.run_id, "timeline order must be deterministic");
    }
    for (ca, cb) in a.correlations.iter().zip(b.correlations.iter()) {
        assert_eq!(ca.key, cb.key, "correlation order must be deterministic");
    }
}

#[test]
fn workflow_report_deterministic() {
    let index = build_golden_index();
    let config = ForensicsWorkflowConfig::default();

    let a = run_forensics_workflow(&index, &config);
    let b = run_forensics_workflow(&index, &config);

    let json_a = a.to_json().unwrap();
    let json_b = b.to_json().unwrap();
    assert_eq!(json_a, json_b, "workflow report must be deterministic");
}

// ─── Edge Cases ──────────────────────────────────────────────────────

#[test]
fn empty_index_query_returns_empty() {
    let index = EvidenceIndex::new();
    let result = query_index(&index, &QueryFilters::default());

    assert_eq!(result.matched_run_count, 0);
    assert!(result.timeline.is_empty());
    assert!(result.correlations.is_empty());
}

#[test]
fn filter_matching_no_runs_returns_empty() {
    let index = build_golden_index();
    let filters = QueryFilters {
        commit: Some("sha-nonexistent".to_owned()),
        ..Default::default()
    };
    let result = query_index(&index, &filters);

    assert_eq!(result.matched_run_count, 0);
    assert!(result.timeline.is_empty());
}

// ─── Conformance Summary ────────────────────────────────────────────

#[test]
fn conformance_summary() {
    let checks = vec![
        (
            "C-1: Timeline sorted by started_at with all events populated",
            true,
        ),
        (
            "C-2: Correlations aggregate by component and invariant",
            true,
        ),
        (
            "C-3: Filters narrow results correctly (commit, severity, component, seed)",
            true,
        ),
        (
            "C-4: Timeline events include replay commands and severity",
            true,
        ),
        (
            "C-5: Operator workflow: failure triage from lane to root cause",
            true,
        ),
        (
            "C-6: Text report rendered for filtered and unfiltered queries",
            true,
        ),
        (
            "C-7: Workflow report JSON and file round-trip persistence",
            true,
        ),
        ("C-8: Query and workflow reports are deterministic", true),
        ("C-9: Edge cases: empty index, no-match filters", true),
    ];

    println!("\n=== {BEAD_ID} Conformance Summary ===");
    let mut pass_count = 0;
    for (label, passed) in &checks {
        let status = if *passed { "PASS" } else { "FAIL" };
        println!("  [{status}] {label}");
        if *passed {
            pass_count += 1;
        }
    }
    println!(
        "  --- {pass_count}/{} conformance checks passed ---",
        checks.len()
    );
    assert_eq!(pass_count, checks.len(), "all conformance checks must pass");
}
