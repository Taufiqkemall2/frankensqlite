use std::path::PathBuf;

use fsqlite_harness::tcl_conformance::{
    BEAD_ID, TclConformanceCategory, TclExecutionMode, TclExecutionOptions,
    TclFailureClassification, TclHarnessOutcome, TclHarnessScenarioResult,
    build_tcl_conformance_matrix, build_validated_tcl_harness_suite, classify_failed_test_name,
    execute_tcl_harness_suite, parse_failed_test_names, parse_testrunner_counts,
};

#[test]
fn canonical_suite_validates() {
    let suite =
        build_validated_tcl_harness_suite().expect("canonical TCL harness suite should validate");
    assert_eq!(suite.bead_id, BEAD_ID);
    assert!(
        suite.scenarios.len() >= 5,
        "bead_id={BEAD_ID} expected five deterministic scenarios for core/tx/error/ext/wal"
    );
}

#[test]
fn testrunner_summary_parser_extracts_counts_and_skips() {
    let sample = "\
1 failures:
FAILED: quick.test
3 jobs skipped due to prior failures
0 errors out of 42 tests in 00:00:07 linux
";

    let parsed = parse_testrunner_counts(sample).expect("summary line should parse");
    assert_eq!(parsed.errors, 0);
    assert_eq!(parsed.tests, 42);
    assert_eq!(parsed.skipped_jobs, 3);
}

#[test]
fn failed_test_parser_extracts_unique_test_names() {
    let sample = "\
FAILED: quick.test
FAILED: wal.test
FAILED: quick.test
";
    let parsed = parse_failed_test_names(sample);
    assert_eq!(parsed, vec!["quick.test".to_owned(), "wal.test".to_owned()]);
}

#[test]
fn failed_test_classifier_marks_known_divergence_patterns() {
    let wal = classify_failed_test_name("wal2.test");
    assert_eq!(
        wal.classification,
        TclFailureClassification::DeliberateDivergence
    );
    let ext = classify_failed_test_name("fts3expr.test");
    assert_eq!(
        ext.classification,
        TclFailureClassification::DeliberateDivergence
    );
    let core = classify_failed_test_name("select1.test");
    assert_eq!(core.classification, TclFailureClassification::Bug);
}

#[test]
fn conformance_matrix_computes_targets_and_roadmap() {
    let core_failure = classify_failed_test_name("select1.test");
    let results = vec![
        TclHarnessScenarioResult {
            scenario_id: "release_quick".to_owned(),
            category: TclConformanceCategory::CoreSql,
            command: "tclsh ... quick.test".to_owned(),
            outcome: TclHarnessOutcome::Fail,
            reason: Some("testrunner_reported_errors errors=1 tests=100".to_owned()),
            exit_code: Some(1),
            tests: 100,
            errors: 1,
            skipped_jobs: 0,
            elapsed_ms: 10,
            log_path: "/tmp/quick.log".to_owned(),
            failures: vec![core_failure],
        },
        TclHarnessScenarioResult {
            scenario_id: "release_wal".to_owned(),
            category: TclConformanceCategory::Wal,
            command: "tclsh ... wal*.test".to_owned(),
            outcome: TclHarnessOutcome::Fail,
            reason: Some("testrunner_reported_errors errors=2 tests=20".to_owned()),
            exit_code: Some(1),
            tests: 20,
            errors: 2,
            skipped_jobs: 0,
            elapsed_ms: 10,
            log_path: "/tmp/wal.log".to_owned(),
            failures: vec![classify_failed_test_name("wal2.test")],
        },
    ];

    let matrix = build_tcl_conformance_matrix(&results);
    assert_eq!(matrix.overall_tests, 120);
    assert_eq!(matrix.overall_errors, 3);
    assert_eq!(matrix.failures.len(), 2);
    assert!(
        matrix
            .roadmap
            .iter()
            .any(|item| item.contains("triage_and_fix_bug_bucket_failures")),
        "bead_id={BEAD_ID} expected bug-triage roadmap item"
    );
}

#[test]
fn dry_run_execution_reports_skipped_without_side_effects() {
    let suite =
        build_validated_tcl_harness_suite().expect("canonical TCL harness suite should validate");
    let summary = execute_tcl_harness_suite(
        &suite,
        TclExecutionOptions {
            mode: TclExecutionMode::DryRun,
            timeout_secs: 60,
            max_scenarios: Some(1),
            runner_override: None,
            run_id_override: Some("bd-3plop-7-test-dry-run".to_owned()),
        },
    )
    .expect("dry-run summary should succeed");

    assert_eq!(summary.mode, TclExecutionMode::DryRun);
    assert_eq!(summary.total_scenarios, 1);
    assert_eq!(summary.skipped_scenarios, 1);
    assert_eq!(summary.failed_scenarios, 0);
    assert_eq!(summary.error_scenarios, 0);
    assert_eq!(summary.timeout_scenarios, 0);
    assert_eq!(summary.results.len(), 1);
    assert_eq!(summary.results[0].outcome, TclHarnessOutcome::Skipped);
    assert_eq!(summary.results[0].reason.as_deref(), Some("dry_run_mode"));
    assert_eq!(summary.conformance_matrix.overall_tests, 0);
    assert!(
        summary
            .conformance_matrix
            .roadmap
            .iter()
            .any(|item| item.contains("build_and_wire_sqlite_c_api_surface")),
        "bead_id={BEAD_ID} expected C-API wiring roadmap item when no executed tests exist"
    );
}

#[test]
fn execute_mode_with_missing_runner_is_graceful_skip() {
    let suite =
        build_validated_tcl_harness_suite().expect("canonical TCL harness suite should validate");
    let summary = execute_tcl_harness_suite(
        &suite,
        TclExecutionOptions {
            mode: TclExecutionMode::Execute,
            timeout_secs: 1,
            max_scenarios: Some(1),
            runner_override: Some(PathBuf::from("/tmp/fsqlite-does-not-exist/testrunner.tcl")),
            run_id_override: Some("bd-3plop-7-test-missing-runner".to_owned()),
        },
    )
    .expect("execute summary should succeed when runner missing");

    assert_eq!(summary.total_scenarios, 1);
    assert_eq!(summary.skipped_scenarios, 1);
    assert_eq!(summary.results[0].outcome, TclHarnessOutcome::Skipped);
    assert!(
        summary.results[0]
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("runner_not_found")),
        "bead_id={BEAD_ID} expected runner_not_found skip reason"
    );
}
