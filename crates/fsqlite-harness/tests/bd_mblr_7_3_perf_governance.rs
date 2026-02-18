//! Integration tests for Performance Regression Governance (bd-mblr.7.3).
//!
//! Exercises the full governance pipeline: corpus → synthetic samples →
//! baseline → regression detection → SLO evaluation → waiver application →
//! governance report generation and persistence.

use std::collections::BTreeMap;

use fsqlite_harness::benchmark_corpus::{DEFAULT_ROOT_SEED, build_validated_benchmark_corpus};
use fsqlite_harness::performance_regression_detector::{
    BenchmarkSample, GOVERNANCE_BEAD_ID, HostContext, PerformanceSlo, RegressionSeverity,
    RegressionTolerance, SCHEMA_VERSION, SloPolicy, SloVerdict, SloWaiver, WaiverRegistry,
    evaluate_governance, load_governance_report, validate_slo_policy, validate_waiver_registry,
    write_governance_report,
};
use tempfile::TempDir;

const TEST_RUN_ID: &str = "gov-test-20260213T170000Z-001";
const TEST_GIT_SHA: &str = "abc1234def5678";
const TEST_DATE: &str = "2026-02-13";

fn host() -> HostContext {
    HostContext {
        os: "linux".to_owned(),
        arch: "x86_64".to_owned(),
        cpu_model: "test-cpu".to_owned(),
        rustc_version: "nightly-2026-02-10".to_owned(),
    }
}

fn params() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("connections".to_owned(), "32".to_owned()),
        ("dataset_rows".to_owned(), "25000".to_owned()),
    ])
}

fn sample(scenario: &str, run_id: &str, p95: f64, throughput: f64) -> BenchmarkSample {
    BenchmarkSample {
        scenario_id: scenario.to_owned(),
        run_id: run_id.to_owned(),
        git_sha: TEST_GIT_SHA.to_owned(),
        seed: 42,
        p50_micros: p95 * 0.70,
        p95_micros: p95,
        p99_micros: p95 * 1.20,
        throughput_ops_per_sec: throughput,
        host: host(),
        benchmark_params: params(),
    }
}

fn baseline_samples(scenario: &str) -> Vec<BenchmarkSample> {
    vec![
        sample(scenario, "base-1", 1_000.0, 5_000.0),
        sample(scenario, "base-2", 1_020.0, 4_980.0),
        sample(scenario, "base-3", 995.0, 5_050.0),
        sample(scenario, "base-4", 1_010.0, 4_970.0),
        sample(scenario, "base-5", 1_005.0, 5_010.0),
    ]
}

fn default_slo(scenario: &str) -> PerformanceSlo {
    PerformanceSlo {
        scenario_id: scenario.to_owned(),
        max_p95_micros: 1_500.0,
        min_throughput_ops_per_sec: 3_500.0,
        tolerance: RegressionTolerance::default(),
    }
}

fn default_policy() -> SloPolicy {
    SloPolicy {
        schema_version: SCHEMA_VERSION,
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        slos: vec![
            default_slo("bm-write-contention-macro"),
            default_slo("bm-recovery-micro"),
        ],
    }
}

fn empty_waivers() -> WaiverRegistry {
    WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![],
    }
}

// ---------------------------------------------------------------------------
// SLO Policy Validation
// ---------------------------------------------------------------------------

#[test]
fn test_slo_policy_validates_correctly() {
    let policy = default_policy();
    let errors = validate_slo_policy(&policy);
    assert!(errors.is_empty(), "expected no errors, got: {errors:?}");
}

#[test]
fn test_slo_policy_rejects_empty_slos() {
    let policy = SloPolicy {
        schema_version: SCHEMA_VERSION,
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        slos: vec![],
    };
    let errors = validate_slo_policy(&policy);
    assert!(
        errors.iter().any(|e| e.contains("at least one")),
        "expected empty slos error, got: {errors:?}"
    );
}

#[test]
fn test_slo_policy_rejects_duplicate_scenarios() {
    let policy = SloPolicy {
        schema_version: SCHEMA_VERSION,
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        slos: vec![
            default_slo("bm-write-contention-macro"),
            default_slo("bm-write-contention-macro"),
        ],
    };
    let errors = validate_slo_policy(&policy);
    assert!(
        errors.iter().any(|e| e.contains("duplicate")),
        "expected duplicate error, got: {errors:?}"
    );
}

#[test]
fn test_slo_policy_rejects_invalid_thresholds() {
    let mut slo = default_slo("bm-test");
    slo.max_p95_micros = -1.0;
    let policy = SloPolicy {
        schema_version: SCHEMA_VERSION,
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        slos: vec![slo],
    };
    let errors = validate_slo_policy(&policy);
    assert!(
        errors.iter().any(|e| e.contains("max_p95_micros")),
        "expected threshold error, got: {errors:?}"
    );
}

// ---------------------------------------------------------------------------
// Waiver Registry Validation
// ---------------------------------------------------------------------------

#[test]
fn test_waiver_registry_validates_correctly() {
    let registry = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![SloWaiver {
            scenario_id: "bm-write-contention-macro".to_owned(),
            reason: "Known regression from MVCC refactor, fix in progress".to_owned(),
            approved_by: "tech-lead".to_owned(),
            granted_date: "2026-02-10".to_owned(),
            expiry_date: "2026-02-20".to_owned(),
            max_tolerated_severity: RegressionSeverity::Warning,
        }],
    };
    let errors = validate_waiver_registry(&registry);
    assert!(errors.is_empty(), "expected no errors, got: {errors:?}");
}

#[test]
fn test_waiver_registry_rejects_empty_reason() {
    let registry = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![SloWaiver {
            scenario_id: "bm-test".to_owned(),
            reason: String::new(),
            approved_by: "tech-lead".to_owned(),
            granted_date: "2026-02-10".to_owned(),
            expiry_date: "2026-02-20".to_owned(),
            max_tolerated_severity: RegressionSeverity::Warning,
        }],
    };
    let errors = validate_waiver_registry(&registry);
    assert!(
        errors.iter().any(|e| e.contains("reason")),
        "expected reason error, got: {errors:?}"
    );
}

#[test]
fn test_waiver_registry_rejects_inverted_dates() {
    let registry = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![SloWaiver {
            scenario_id: "bm-test".to_owned(),
            reason: "test".to_owned(),
            approved_by: "tech-lead".to_owned(),
            granted_date: "2026-02-20".to_owned(),
            expiry_date: "2026-02-10".to_owned(),
            max_tolerated_severity: RegressionSeverity::Warning,
        }],
    };
    let errors = validate_waiver_registry(&registry);
    assert!(
        errors.iter().any(|e| e.contains("after expiry")),
        "expected date inversion error, got: {errors:?}"
    );
}

#[test]
fn test_waiver_lookup_active() {
    let registry = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![SloWaiver {
            scenario_id: "bm-test".to_owned(),
            reason: "known issue".to_owned(),
            approved_by: "tech-lead".to_owned(),
            granted_date: "2026-02-10".to_owned(),
            expiry_date: "2026-02-20".to_owned(),
            max_tolerated_severity: RegressionSeverity::Warning,
        }],
    };
    assert!(
        registry
            .find_active_waiver("bm-test", "2026-02-13")
            .is_some(),
        "waiver should be active on 2026-02-13"
    );
    assert!(
        registry
            .find_active_waiver("bm-test", "2026-02-21")
            .is_none(),
        "waiver should be expired on 2026-02-21"
    );
    assert!(
        registry
            .find_active_waiver("bm-other", "2026-02-13")
            .is_none(),
        "no waiver for bm-other"
    );
}

#[test]
fn test_waiver_expired_detection() {
    let registry = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![
            SloWaiver {
                scenario_id: "bm-old".to_owned(),
                reason: "old issue".to_owned(),
                approved_by: "tech-lead".to_owned(),
                granted_date: "2026-01-01".to_owned(),
                expiry_date: "2026-01-15".to_owned(),
                max_tolerated_severity: RegressionSeverity::Warning,
            },
            SloWaiver {
                scenario_id: "bm-current".to_owned(),
                reason: "current issue".to_owned(),
                approved_by: "tech-lead".to_owned(),
                granted_date: "2026-02-10".to_owned(),
                expiry_date: "2026-02-20".to_owned(),
                max_tolerated_severity: RegressionSeverity::Warning,
            },
        ],
    };
    let expired = registry.expired_waivers("2026-02-13");
    assert_eq!(expired.len(), 1, "expected 1 expired waiver");
    assert_eq!(expired[0].scenario_id, "bm-old");
}

// ---------------------------------------------------------------------------
// Governance Pipeline: All Pass
// ---------------------------------------------------------------------------

#[test]
fn test_governance_all_scenarios_pass() {
    let scenario = "bm-write-contention-macro";
    let baselines = baseline_samples(scenario);
    let candidates = vec![sample(scenario, "candidate-1", 1_005.0, 5_000.0)];

    let report = evaluate_governance(
        &baselines,
        &candidates,
        &default_policy(),
        &empty_waivers(),
        TEST_RUN_ID,
        TEST_GIT_SHA,
        TEST_DATE,
    )
    .expect("governance evaluation");

    assert_eq!(report.bead_id, GOVERNANCE_BEAD_ID);
    assert_eq!(report.overall_verdict, SloVerdict::Pass);
    assert_eq!(report.scenarios_evaluated, 1);
    assert_eq!(report.scenarios_passed, 1);
    assert_eq!(report.scenarios_failed, 0);
    assert_eq!(report.scenarios_waived, 0);
}

// ---------------------------------------------------------------------------
// Governance Pipeline: Regression Detected
// ---------------------------------------------------------------------------

#[test]
fn test_governance_regression_causes_failure() {
    let scenario = "bm-write-contention-macro";
    let baselines = baseline_samples(scenario);
    // p95 at 2000us exceeds SLO max of 1500us
    let candidates = vec![sample(scenario, "candidate-bad", 2_000.0, 3_000.0)];

    let report = evaluate_governance(
        &baselines,
        &candidates,
        &default_policy(),
        &empty_waivers(),
        TEST_RUN_ID,
        TEST_GIT_SHA,
        TEST_DATE,
    )
    .expect("governance evaluation");

    assert_eq!(report.overall_verdict, SloVerdict::Fail);
    assert_eq!(report.scenarios_failed, 1);
    assert_eq!(report.results[0].slo_verdict, SloVerdict::Fail);
    assert!(
        report.results[0].reasons.iter().any(|r| r.contains("SLO")),
        "should mention SLO violation"
    );
}

// ---------------------------------------------------------------------------
// Governance Pipeline: Warning Severity
// ---------------------------------------------------------------------------

#[test]
fn test_governance_warning_regression() {
    let scenario = "bm-write-contention-macro";
    let baselines = baseline_samples(scenario);
    // p95 at 1120us: within SLO (max 1500) but 12% regression (warning threshold 10%)
    let candidates = vec![sample(scenario, "candidate-warn", 1_120.0, 4_500.0)];

    let report = evaluate_governance(
        &baselines,
        &candidates,
        &default_policy(),
        &empty_waivers(),
        TEST_RUN_ID,
        TEST_GIT_SHA,
        TEST_DATE,
    )
    .expect("governance evaluation");

    assert_eq!(report.overall_verdict, SloVerdict::Warning);
    assert_eq!(report.scenarios_warned, 1);
}

// ---------------------------------------------------------------------------
// Governance Pipeline: Waiver Applied
// ---------------------------------------------------------------------------

#[test]
fn test_governance_waiver_downgrades_verdict() {
    let scenario = "bm-write-contention-macro";
    let baselines = baseline_samples(scenario);
    // p95 at 1120us triggers a Warning-level regression
    let candidates = vec![sample(scenario, "candidate-waived", 1_120.0, 4_500.0)];

    let waivers = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![SloWaiver {
            scenario_id: scenario.to_owned(),
            reason: "Known MVCC refactor regression".to_owned(),
            approved_by: "tech-lead".to_owned(),
            granted_date: "2026-02-10".to_owned(),
            expiry_date: "2026-02-20".to_owned(),
            max_tolerated_severity: RegressionSeverity::Warning,
        }],
    };

    let report = evaluate_governance(
        &baselines,
        &candidates,
        &default_policy(),
        &waivers,
        TEST_RUN_ID,
        TEST_GIT_SHA,
        TEST_DATE,
    )
    .expect("governance evaluation");

    assert_eq!(report.overall_verdict, SloVerdict::Waived);
    assert_eq!(report.scenarios_waived, 1);
    assert!(report.results[0].waiver_active);
}

// ---------------------------------------------------------------------------
// Governance Pipeline: Waiver Does Not Cover Critical
// ---------------------------------------------------------------------------

#[test]
fn test_governance_waiver_insufficient_for_critical() {
    let scenario = "bm-write-contention-macro";
    let baselines = baseline_samples(scenario);
    // p95 at 2000us: exceeds SLO max, critical regression
    let candidates = vec![sample(scenario, "candidate-critical", 2_000.0, 3_000.0)];

    let waivers = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![SloWaiver {
            scenario_id: scenario.to_owned(),
            reason: "Only covers warnings".to_owned(),
            approved_by: "tech-lead".to_owned(),
            granted_date: "2026-02-10".to_owned(),
            expiry_date: "2026-02-20".to_owned(),
            max_tolerated_severity: RegressionSeverity::Warning,
        }],
    };

    let report = evaluate_governance(
        &baselines,
        &candidates,
        &default_policy(),
        &waivers,
        TEST_RUN_ID,
        TEST_GIT_SHA,
        TEST_DATE,
    )
    .expect("governance evaluation");

    // Critical regression exceeds waiver's max_tolerated_severity
    assert_eq!(report.overall_verdict, SloVerdict::Fail);
}

// ---------------------------------------------------------------------------
// Governance Pipeline: Multi-Scenario
// ---------------------------------------------------------------------------

#[test]
fn test_governance_multi_scenario_mixed_verdicts() {
    let scenario_a = "bm-write-contention-macro";
    let scenario_b = "bm-recovery-micro";

    let mut baselines = baseline_samples(scenario_a);
    baselines.extend(baseline_samples(scenario_b));

    let candidates = vec![
        sample(scenario_a, "cand-a", 1_005.0, 5_000.0), // pass
        sample(scenario_b, "cand-b", 1_120.0, 4_500.0), // warning
    ];

    let report = evaluate_governance(
        &baselines,
        &candidates,
        &default_policy(),
        &empty_waivers(),
        TEST_RUN_ID,
        TEST_GIT_SHA,
        TEST_DATE,
    )
    .expect("governance evaluation");

    assert_eq!(report.scenarios_evaluated, 2);
    assert_eq!(report.scenarios_passed, 1);
    assert_eq!(report.scenarios_warned, 1);
    // Overall is worst-case
    assert_eq!(report.overall_verdict, SloVerdict::Warning);
}

// ---------------------------------------------------------------------------
// Governance Pipeline: Ungoverned Scenario Ignored
// ---------------------------------------------------------------------------

#[test]
fn test_governance_ungoverned_scenario_skipped() {
    let scenario = "bm-write-contention-macro";
    let baselines = baseline_samples(scenario);
    let candidates = vec![
        sample(scenario, "cand-gov", 1_005.0, 5_000.0),
        sample("bm-unknown-scenario", "cand-skip", 9_000.0, 100.0),
    ];

    let report = evaluate_governance(
        &baselines,
        &candidates,
        &default_policy(),
        &empty_waivers(),
        TEST_RUN_ID,
        TEST_GIT_SHA,
        TEST_DATE,
    )
    .expect("governance evaluation");

    // Only 1 scenario evaluated (the governed one)
    assert_eq!(report.scenarios_evaluated, 1);
    assert_eq!(report.overall_verdict, SloVerdict::Pass);
}

// ---------------------------------------------------------------------------
// Governance Report JSON Round-Trip
// ---------------------------------------------------------------------------

#[test]
fn test_governance_report_json_roundtrip() {
    let scenario = "bm-write-contention-macro";
    let baselines = baseline_samples(scenario);
    let candidates = vec![sample(scenario, "cand-rt", 1_005.0, 5_000.0)];

    let report = evaluate_governance(
        &baselines,
        &candidates,
        &default_policy(),
        &empty_waivers(),
        TEST_RUN_ID,
        TEST_GIT_SHA,
        TEST_DATE,
    )
    .expect("governance evaluation");

    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("governance-report.json");

    write_governance_report(&path, &report).expect("write report");
    let loaded = load_governance_report(&path).expect("load report");

    assert_eq!(report, loaded, "JSON round-trip should preserve report");
}

// ---------------------------------------------------------------------------
// Expired Waiver Tracking
// ---------------------------------------------------------------------------

#[test]
fn test_governance_reports_expired_waivers() {
    let scenario = "bm-write-contention-macro";
    let baselines = baseline_samples(scenario);
    let candidates = vec![sample(scenario, "cand-exp", 1_005.0, 5_000.0)];

    let waivers = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![SloWaiver {
            scenario_id: "bm-old-scenario".to_owned(),
            reason: "Stale waiver".to_owned(),
            approved_by: "tech-lead".to_owned(),
            granted_date: "2026-01-01".to_owned(),
            expiry_date: "2026-01-15".to_owned(),
            max_tolerated_severity: RegressionSeverity::Warning,
        }],
    };

    let report = evaluate_governance(
        &baselines,
        &candidates,
        &default_policy(),
        &waivers,
        TEST_RUN_ID,
        TEST_GIT_SHA,
        TEST_DATE,
    )
    .expect("governance evaluation");

    assert_eq!(report.expired_waivers.len(), 1);
    assert!(report.expired_waivers[0].contains("bm-old-scenario"));
}

// ---------------------------------------------------------------------------
// Corpus Integration: Build Corpus and Verify It
// ---------------------------------------------------------------------------

#[test]
fn test_benchmark_corpus_builds_and_validates() {
    let corpus =
        build_validated_benchmark_corpus(DEFAULT_ROOT_SEED).expect("corpus build and validate");
    assert!(!corpus.entries.is_empty(), "corpus must have entries");
    assert_eq!(corpus.bead_id, "bd-mblr.7.3.1");
}

// ---------------------------------------------------------------------------
// Error Cases
// ---------------------------------------------------------------------------

#[test]
fn test_governance_rejects_empty_run_id() {
    let result = evaluate_governance(
        &[],
        &[],
        &default_policy(),
        &empty_waivers(),
        "",
        TEST_GIT_SHA,
        TEST_DATE,
    );
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("run_id"));
}

#[test]
fn test_governance_rejects_empty_git_sha() {
    let result = evaluate_governance(
        &[],
        &[],
        &default_policy(),
        &empty_waivers(),
        TEST_RUN_ID,
        "",
        TEST_DATE,
    );
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("git_sha"));
}
