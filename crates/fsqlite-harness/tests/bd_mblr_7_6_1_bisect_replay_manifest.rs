//! Integration tests for bd-mblr.7.6.1: Bisect-ready replay manifest contract.
//!
//! Validates that the `BisectReplayManifest` type is sufficient to rerun a
//! failing workload exactly across candidate commits during automated bisection.
//!
//! Evidence layers:
//!   1. Unit-level: schema validation, roundtrip, strict parsing, evaluation.
//!   2. Integration: end-to-end construction from BisectRequest, builder API,
//!      bisect-readiness checks, artifact dependency tracking.
//!   3. Structured logging: JSON evidence artifact emission.

use fsqlite_harness::ci_gate_matrix::BisectRequest;
use fsqlite_harness::replay_harness::{
    ArtifactDependency, BisectRange, BisectReplayManifest, BisectStrategy, BuildRequirements,
    DriftDetectorConfig, FailureBundleRef, ReplayConfig, ReplayPassCriteria, ReplaySession,
    ReplayVerdict, BISECT_REPLAY_MANIFEST_SCHEMA_VERSION,
};

const BEAD_ID: &str = "bd-mblr.7.6.1";

// ── Helpers ──────────────────────────────────────────────────────────────

fn make_test_summary() -> fsqlite_harness::replay_harness::ReplaySummary {
    use fsqlite_harness::differential_v2::Outcome;
    use fsqlite_harness::mismatch_minimizer::Subsystem;
    use fsqlite_harness::replay_harness::ReplayEntryResult;

    let config = ReplayConfig {
        drift_config: DriftDetectorConfig {
            window_size: 2,
            warmup_windows: 1,
            sensitivity_threshold: 1.0,
            ..DriftDetectorConfig::default()
        },
        ..ReplayConfig::default()
    };
    let mut session = ReplaySession::new(config);
    session.record_entry(ReplayEntryResult {
        entry_id: "entry-1".to_owned(),
        outcome: Outcome::Pass,
        statements_total: 10,
        statements_mismatched: 0,
        mismatch_rate: 0.0,
        subsystem: None,
        artifact_id: "art-1".to_owned(),
    });
    session.record_entry(ReplayEntryResult {
        entry_id: "entry-2".to_owned(),
        outcome: Outcome::Divergence,
        statements_total: 10,
        statements_mismatched: 1,
        mismatch_rate: 0.1,
        subsystem: Some(Subsystem::Vdbe),
        artifact_id: "art-2".to_owned(),
    });
    session.finalize()
}

fn make_bisect_request() -> BisectRequest {
    use fsqlite_harness::ci_gate_matrix::BisectTrigger;

    BisectRequest {
        request_id: "req-bisect-001".to_owned(),
        trigger: BisectTrigger::GateFailure,
        lane: "correctness".to_owned(),
        failing_gate: "phase_4_6_gate".to_owned(),
        good_commit: "abc1234".to_owned(),
        bad_commit: "def5678".to_owned(),
        replay_seed: 424_242,
        replay_command: "cargo test -p fsqlite-harness -- bisect_replay".to_owned(),
        expected_exit_code: 0,
        requested_at: "2026-02-13T10:00:00Z".to_owned(),
        description: "VDBE divergence in phase_4_6 gate".to_owned(),
    }
}

fn make_pass_criteria(
    summary: &fsqlite_harness::replay_harness::ReplaySummary,
) -> ReplayPassCriteria {
    ReplayPassCriteria {
        max_divergent_entries: summary.total_divergent,
        max_error_entries: summary.total_errors,
        max_shift_alerts: summary.shift_count(),
    }
}

// ── Schema version contract ─────────────────────────────────────────────

#[test]
fn schema_version_is_semver() {
    let parts: Vec<&str> = BISECT_REPLAY_MANIFEST_SCHEMA_VERSION.split('.').collect();
    assert_eq!(parts.len(), 3, "schema_version must be semver X.Y.Z");
    for part in parts {
        part.parse::<u32>()
            .expect("each semver component must be a u32");
    }
}

// ── Construction from BisectRequest ─────────────────────────────────────

#[test]
fn from_bisect_request_populates_range_and_seed() {
    let summary = make_test_summary();
    let request = make_bisect_request();
    let criteria = make_pass_criteria(&summary);

    let manifest = BisectReplayManifest::from_summary_and_bisect_request(
        &summary,
        &request,
        BEAD_ID,
        "2026-02-13T10:05:00Z",
        criteria,
    );

    assert_eq!(manifest.base_seed, request.replay_seed);
    assert_eq!(manifest.run_id, request.request_id);
    assert_eq!(manifest.replay_command, request.replay_command);

    let range = manifest.bisect_range.as_ref().expect("bisect_range set");
    assert_eq!(range.good_commit, "abc1234");
    assert_eq!(range.bad_commit, "def5678");

    assert!(
        manifest.bisect_strategy.is_some(),
        "bisect_strategy should be defaulted"
    );
}

// ── Builder API ─────────────────────────────────────────────────────────

#[test]
fn builder_api_chains_correctly() {
    let summary = make_test_summary();
    let criteria = make_pass_criteria(&summary);

    let manifest = BisectReplayManifest::from_summary(
        &summary,
        BEAD_ID,
        "run-builder-1",
        "2026-02-13T10:10:00Z",
        "cargo test -p fsqlite-harness -- replay",
        criteria,
    )
    .with_bisect_range(BisectRange {
        good_commit: "aaa1111".to_owned(),
        bad_commit: "bbb2222".to_owned(),
        branch: Some("main".to_owned()),
    })
    .with_build_requirements(BuildRequirements {
        profile: Some("release-perf".to_owned()),
        features: vec!["wal-fec".to_owned()],
        extra_cargo_args: vec![],
        min_toolchain: Some("nightly-2026-02-10".to_owned()),
    })
    .with_failure_bundle_ref(FailureBundleRef {
        bundle_id: "bundle-alpha".to_owned(),
        bundle_path: Some("test-results/bundle-alpha.json".to_owned()),
        bundle_hash: Some("sha256:deadbeef".to_owned()),
    })
    .with_artifact_dependency(ArtifactDependency {
        name: "corpus".to_owned(),
        path: "conformance/corpus.jsonl".to_owned(),
        hash: Some("sha256:cafebabe".to_owned()),
    })
    .with_bisect_strategy(BisectStrategy {
        max_steps: 15,
        step_timeout_secs: 600,
        retries_per_step: 2,
    });

    assert!(manifest.bisect_range.is_some());
    assert!(manifest.build_requirements.is_some());
    assert!(manifest.failure_bundle_ref.is_some());
    assert_eq!(manifest.artifact_dependencies.len(), 1);
    assert!(manifest.bisect_strategy.is_some());

    let strategy = manifest.bisect_strategy.as_ref().unwrap();
    assert_eq!(strategy.max_steps, 15);
    assert_eq!(strategy.retries_per_step, 2);
}

// ── Bisect-readiness check ──────────────────────────────────────────────

#[test]
fn is_bisect_ready_requires_range_and_strategy() {
    let summary = make_test_summary();
    let criteria = make_pass_criteria(&summary);

    // Without range: not bisect-ready.
    let manifest = BisectReplayManifest::from_summary(
        &summary,
        BEAD_ID,
        "run-ready-1",
        "2026-02-13T10:15:00Z",
        "cargo test",
        criteria.clone(),
    );
    assert!(
        !manifest.is_bisect_ready(),
        "manifest without bisect_range should not be bisect-ready"
    );

    // With range but no strategy: not bisect-ready.
    let manifest = manifest.with_bisect_range(BisectRange {
        good_commit: "aaa".to_owned(),
        bad_commit: "bbb".to_owned(),
        branch: None,
    });
    assert!(
        !manifest.is_bisect_ready(),
        "manifest without strategy should not be bisect-ready"
    );

    // With range and strategy: bisect-ready.
    let manifest = manifest.with_bisect_strategy(BisectStrategy::default());
    assert!(
        manifest.is_bisect_ready(),
        "manifest with range + strategy should be bisect-ready"
    );
}

#[test]
fn from_bisect_request_is_bisect_ready() {
    let summary = make_test_summary();
    let request = make_bisect_request();
    let criteria = make_pass_criteria(&summary);

    let manifest = BisectReplayManifest::from_summary_and_bisect_request(
        &summary,
        &request,
        BEAD_ID,
        "2026-02-13T10:20:00Z",
        criteria,
    );
    assert!(
        manifest.is_bisect_ready(),
        "manifest from BisectRequest should be bisect-ready"
    );
}

// ── Validation with new fields ──────────────────────────────────────────

#[test]
fn validate_catches_empty_bisect_range_fields() {
    let summary = make_test_summary();
    let criteria = make_pass_criteria(&summary);

    let manifest = BisectReplayManifest::from_summary(
        &summary,
        BEAD_ID,
        "run-val-1",
        "2026-02-13T10:25:00Z",
        "cargo test",
        criteria,
    )
    .with_bisect_range(BisectRange {
        good_commit: String::new(),
        bad_commit: "bbb".to_owned(),
        branch: None,
    });

    let errors = manifest.validate();
    assert!(
        errors.iter().any(|e| e.contains("good_commit is empty")),
        "should catch empty good_commit: {errors:?}"
    );
}

#[test]
fn validate_catches_identical_commits() {
    let summary = make_test_summary();
    let criteria = make_pass_criteria(&summary);

    let manifest = BisectReplayManifest::from_summary(
        &summary,
        BEAD_ID,
        "run-val-2",
        "2026-02-13T10:30:00Z",
        "cargo test",
        criteria,
    )
    .with_bisect_range(BisectRange {
        good_commit: "same_sha".to_owned(),
        bad_commit: "same_sha".to_owned(),
        branch: None,
    });

    let errors = manifest.validate();
    assert!(
        errors
            .iter()
            .any(|e| e.contains("good_commit == bad_commit")),
        "should catch identical commits: {errors:?}"
    );
}

#[test]
fn validate_catches_empty_failure_bundle_id() {
    let summary = make_test_summary();
    let criteria = make_pass_criteria(&summary);

    let manifest = BisectReplayManifest::from_summary(
        &summary,
        BEAD_ID,
        "run-val-3",
        "2026-02-13T10:35:00Z",
        "cargo test",
        criteria,
    )
    .with_failure_bundle_ref(FailureBundleRef {
        bundle_id: String::new(),
        bundle_path: None,
        bundle_hash: None,
    });

    let errors = manifest.validate();
    assert!(
        errors
            .iter()
            .any(|e| e.contains("failure_bundle_ref.bundle_id is empty")),
        "should catch empty bundle_id: {errors:?}"
    );
}

#[test]
fn validate_catches_empty_artifact_dependency_path() {
    let summary = make_test_summary();
    let criteria = make_pass_criteria(&summary);

    let manifest = BisectReplayManifest::from_summary(
        &summary,
        BEAD_ID,
        "run-val-4",
        "2026-02-13T10:40:00Z",
        "cargo test",
        criteria,
    )
    .with_artifact_dependency(ArtifactDependency {
        name: "missing-path".to_owned(),
        path: String::new(),
        hash: None,
    });

    let errors = manifest.validate();
    assert!(
        errors
            .iter()
            .any(|e| e.contains("artifact_dependencies[0].path is empty")),
        "should catch empty artifact path: {errors:?}"
    );
}

// ── JSON roundtrip with new fields ──────────────────────────────────────

#[test]
fn full_manifest_json_roundtrip() {
    let summary = make_test_summary();
    let request = make_bisect_request();
    let criteria = make_pass_criteria(&summary);

    let manifest = BisectReplayManifest::from_summary_and_bisect_request(
        &summary,
        &request,
        BEAD_ID,
        "2026-02-13T10:45:00Z",
        criteria,
    )
    .with_build_requirements(BuildRequirements {
        profile: Some("debug".to_owned()),
        features: vec!["test-feature".to_owned()],
        extra_cargo_args: vec!["--no-default-features".to_owned()],
        min_toolchain: Some("nightly-2026-02-10".to_owned()),
    })
    .with_failure_bundle_ref(FailureBundleRef {
        bundle_id: "bundle-rt-1".to_owned(),
        bundle_path: Some("results/bundle.json".to_owned()),
        bundle_hash: Some("sha256:aabbcc".to_owned()),
    })
    .with_artifact_dependency(ArtifactDependency {
        name: "fixture-db".to_owned(),
        path: "fixtures/test.db".to_owned(),
        hash: Some("sha256:112233".to_owned()),
    })
    .with_artifact_dependency(ArtifactDependency {
        name: "corpus".to_owned(),
        path: "conformance/corpus.jsonl".to_owned(),
        hash: None,
    });

    let json = manifest.to_json().expect("serialize");
    let restored = BisectReplayManifest::from_json(&json).expect("deserialize");

    assert_eq!(restored.bisect_range, manifest.bisect_range);
    assert_eq!(restored.build_requirements, manifest.build_requirements);
    assert_eq!(restored.failure_bundle_ref, manifest.failure_bundle_ref);
    assert_eq!(restored.artifact_dependencies, manifest.artifact_dependencies);
    assert_eq!(restored.bisect_strategy, manifest.bisect_strategy);
    assert_eq!(restored, manifest);
}

#[test]
fn strict_parsing_accepts_full_manifest() {
    let summary = make_test_summary();
    let request = make_bisect_request();
    let criteria = make_pass_criteria(&summary);

    let manifest = BisectReplayManifest::from_summary_and_bisect_request(
        &summary,
        &request,
        BEAD_ID,
        "2026-02-13T10:50:00Z",
        criteria,
    );

    let json = manifest.to_json().expect("serialize");
    let strict = BisectReplayManifest::from_json_strict(&json).expect("strict parse");
    assert_eq!(strict, manifest);
}

// ── Backward compatibility: old JSON without new fields ─────────────────

#[test]
fn old_json_without_new_fields_deserializes() {
    // Simulate a V1 manifest without the new optional fields.
    let json = serde_json::json!({
        "schema_version": BISECT_REPLAY_MANIFEST_SCHEMA_VERSION,
        "manifest_id": "rmf-test123",
        "bead_id": BEAD_ID,
        "run_id": "run-compat",
        "created_at": "2026-02-13T10:55:00Z",
        "source_summary_hash": "hash123",
        "base_seed": 42,
        "expected_entry_count": 5,
        "expected_window_count": 1,
        "replay_command": "cargo test",
        "pass_criteria": {
            "max_divergent_entries": 0,
            "max_error_entries": 0,
            "max_shift_alerts": 0
        },
        "environment": {},
        "notes": []
    });

    let manifest: BisectReplayManifest =
        serde_json::from_value(json).expect("deserialize old format");
    assert!(manifest.bisect_range.is_none());
    assert!(manifest.build_requirements.is_none());
    assert!(manifest.failure_bundle_ref.is_none());
    assert!(manifest.artifact_dependencies.is_empty());
    assert!(manifest.bisect_strategy.is_none());
    assert!(!manifest.is_bisect_ready());
}

// ── Evaluation still works correctly ────────────────────────────────────

#[test]
fn evaluate_summary_passes_with_matching_criteria() {
    let summary = make_test_summary();
    let request = make_bisect_request();
    let criteria = make_pass_criteria(&summary);

    let manifest = BisectReplayManifest::from_summary_and_bisect_request(
        &summary,
        &request,
        BEAD_ID,
        "2026-02-13T11:00:00Z",
        criteria,
    );

    let eval = manifest.evaluate_summary(&summary);
    assert_eq!(eval.verdict, ReplayVerdict::Pass);
    assert!(eval.reasons.is_empty());
}

#[test]
fn evaluate_summary_fails_with_stricter_criteria() {
    let summary = make_test_summary();
    let request = make_bisect_request();

    // Set criteria stricter than the summary.
    let criteria = ReplayPassCriteria {
        max_divergent_entries: 0,
        max_error_entries: 0,
        max_shift_alerts: 0,
    };

    let manifest = BisectReplayManifest::from_summary_and_bisect_request(
        &summary,
        &request,
        BEAD_ID,
        "2026-02-13T11:05:00Z",
        criteria,
    );

    let eval = manifest.evaluate_summary(&summary);
    assert_eq!(eval.verdict, ReplayVerdict::Fail);
    assert!(!eval.reasons.is_empty());
}

// ── BisectStrategy defaults ─────────────────────────────────────────────

#[test]
fn bisect_strategy_defaults() {
    let strategy = BisectStrategy::default();
    assert_eq!(strategy.max_steps, 20);
    assert_eq!(strategy.step_timeout_secs, 300);
    assert_eq!(strategy.retries_per_step, 1);
}

// ── Evidence artifact emission ──────────────────────────────────────────

#[test]
fn evidence_artifact_json_emission() {
    let summary = make_test_summary();
    let request = make_bisect_request();
    let criteria = make_pass_criteria(&summary);

    let manifest = BisectReplayManifest::from_summary_and_bisect_request(
        &summary,
        &request,
        BEAD_ID,
        "2026-02-13T11:10:00Z",
        criteria,
    )
    .with_build_requirements(BuildRequirements::default())
    .with_failure_bundle_ref(FailureBundleRef {
        bundle_id: "bundle-evidence".to_owned(),
        bundle_path: None,
        bundle_hash: None,
    });

    let json = manifest.to_json().expect("serialize evidence artifact");
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("parse JSON");

    // Verify top-level required keys.
    assert!(parsed["schema_version"].is_string());
    assert!(parsed["manifest_id"].is_string());
    assert!(parsed["bead_id"].is_string());
    assert!(parsed["run_id"].is_string());
    assert!(parsed["base_seed"].is_number());
    assert!(parsed["replay_command"].is_string());
    assert!(parsed["pass_criteria"].is_object());

    // Verify new optional keys.
    assert!(parsed["bisect_range"].is_object());
    assert_eq!(
        parsed["bisect_range"]["good_commit"].as_str(),
        Some("abc1234")
    );
    assert_eq!(
        parsed["bisect_range"]["bad_commit"].as_str(),
        Some("def5678")
    );
    assert!(parsed["bisect_strategy"].is_object());
    assert!(parsed["failure_bundle_ref"].is_object());
}
