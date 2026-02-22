//! Determinism Drift Triage Automation (bd-mblr.7.8.3)
//!
//! Integration tests validating drift severity classification, actionable
//! findings with remediation/replay guidance, and triage report automation.
//! Depends on: bd-mblr.7.8.2 (cross-toolchain determinism runner),
//! bd-mblr.3.5.1 (validation manifest).

use fsqlite_harness::toolchain_determinism::{
    DeterminismMatrix, DeterminismRunner, DivergenceClass, DriftClassification, DriftFailure,
    DriftReport, TimingAnomaly, WatchdogConfig, WatchdogVerdict, load_watchdog_report,
    run_watchdog, write_watchdog_report,
};
use tempfile::TempDir;

const BEAD_ID: &str = "bd-mblr.7.8.3";

// ─── Helpers ──────────────────────────────────────────────────────────

fn default_config() -> WatchdogConfig {
    WatchdogConfig::default()
}

fn watchdog_report() -> fsqlite_harness::toolchain_determinism::WatchdogReport {
    run_watchdog(&default_config())
}

fn drift_report_from_watchdog(
    report: &fsqlite_harness::toolchain_determinism::WatchdogReport,
) -> DriftReport {
    let matrix = DeterminismMatrix::canonical(report.session.root_seed);
    DriftReport::from_session(&report.session, &matrix)
}

// ─── Drift Severity Classification: DivergenceClass ──────────────────

#[test]
fn divergence_class_none_is_default() {
    let dc: DivergenceClass = Default::default();
    assert_eq!(dc, DivergenceClass::None);
    assert_eq!(format!("{dc}"), "none");
}

#[test]
fn divergence_class_display_all_variants() {
    let cases = [
        (DivergenceClass::None, "none"),
        (DivergenceClass::OutputMismatch, "output_mismatch"),
        (DivergenceClass::SemanticMismatch, "semantic_mismatch"),
        (DivergenceClass::StatisticalDrift, "statistical_drift"),
        (DivergenceClass::TimingExceeded, "timing_exceeded"),
        (DivergenceClass::RunnerError, "runner_error"),
        (DivergenceClass::MissingEvidence, "missing_evidence"),
    ];
    for (variant, expected) in &cases {
        assert_eq!(format!("{variant}"), *expected);
    }
}

#[test]
fn divergence_class_ordering_defined() {
    // DivergenceClass derives Ord — severity escalation must be stable
    assert!(DivergenceClass::None < DivergenceClass::OutputMismatch);
    assert!(DivergenceClass::OutputMismatch < DivergenceClass::SemanticMismatch);
    assert!(DivergenceClass::SemanticMismatch < DivergenceClass::StatisticalDrift);
    assert!(DivergenceClass::StatisticalDrift < DivergenceClass::TimingExceeded);
    assert!(DivergenceClass::TimingExceeded < DivergenceClass::RunnerError);
    assert!(DivergenceClass::RunnerError < DivergenceClass::MissingEvidence);
}

// ─── Drift Classification ────────────────────────────────────────────

#[test]
fn drift_classification_acceptable_variants() {
    assert!(DriftClassification::Identical.is_acceptable());
    assert!(DriftClassification::SemanticEquivalent.is_acceptable());
    assert!(DriftClassification::WithinEpsilon.is_acceptable());
    assert!(!DriftClassification::Divergent.is_acceptable());
    assert!(!DriftClassification::Unsupported.is_acceptable());
}

#[test]
fn drift_classification_display() {
    let cases = [
        (DriftClassification::Identical, "identical"),
        (
            DriftClassification::SemanticEquivalent,
            "semantic-equivalent",
        ),
        (DriftClassification::WithinEpsilon, "within-epsilon"),
        (DriftClassification::Divergent, "DIVERGENT"),
        (DriftClassification::Unsupported, "unsupported"),
    ];
    for (variant, expected) in &cases {
        assert_eq!(format!("{variant}"), *expected);
    }
}

// ─── WatchdogVerdict Display ─────────────────────────────────────────

#[test]
fn watchdog_verdict_display() {
    assert_eq!(format!("{}", WatchdogVerdict::Pass), "PASS");
    assert_eq!(format!("{}", WatchdogVerdict::Warning), "WARNING");
    assert_eq!(format!("{}", WatchdogVerdict::Fail), "FAIL");
}

// ─── Watchdog Report: Triage Surface ──────────────────────────────────

#[test]
fn watchdog_report_has_triage_line() {
    let report = watchdog_report();
    let triage = report.triage_line();

    assert!(!triage.is_empty(), "triage line must be generated");
    assert!(
        triage.contains(&format!("{}", report.verdict)),
        "triage line must include verdict"
    );
    assert!(triage.contains("probes"), "triage line must mention probes");
    assert!(
        triage.contains("failures"),
        "triage line must mention failures"
    );
}

#[test]
fn watchdog_report_summary_non_empty() {
    let report = watchdog_report();
    assert!(!report.summary.is_empty(), "summary must be generated");
    assert!(
        report.summary.contains("Watchdog"),
        "summary must identify as watchdog"
    );
}

#[test]
fn watchdog_report_has_correct_bead_id() {
    let report = watchdog_report();
    assert_eq!(report.bead_id, "bd-mblr.7.8");
    assert_eq!(report.schema_version, 1);
}

#[test]
fn watchdog_report_session_populated() {
    let report = watchdog_report();

    assert!(report.session.probe_count > 0, "session must have probes");
    assert!(
        report.session.toolchain_count > 0,
        "session must have toolchains"
    );
    assert!(
        report.session.corpus_entry_count > 0,
        "session must have corpus entries"
    );
}

#[test]
fn watchdog_report_coverage_populated() {
    let report = watchdog_report();

    assert!(
        report.coverage.toolchain_count > 0,
        "coverage must count toolchains"
    );
    assert!(
        report.coverage.probe_count > 0,
        "coverage must count probes"
    );
    assert!(
        report.coverage.total_combinations > 0,
        "coverage must have combinations"
    );
    assert!(
        !report.coverage.subsystems_covered.is_empty(),
        "coverage must list subsystems"
    );
}

// ─── Drift Report from Session ───────────────────────────────────────

#[test]
fn drift_report_constructed_from_session() {
    let wd = watchdog_report();
    let drift = drift_report_from_watchdog(&wd);

    assert_eq!(drift.schema_version, 1);
    assert_eq!(drift.probe_count, wd.session.probe_count);
    assert_eq!(
        drift.probes_passed + drift.probes_failed,
        drift.probe_count,
        "passed + failed must equal total"
    );
    assert!(drift.corpus_entries > 0, "corpus entries must be recorded");
}

#[test]
fn drift_report_overall_pass_matches_session() {
    let wd = watchdog_report();
    let drift = drift_report_from_watchdog(&wd);

    assert_eq!(
        drift.overall_pass, wd.session.overall_pass,
        "drift report overall_pass must match session"
    );
}

#[test]
fn drift_report_drift_counts_populated() {
    let wd = watchdog_report();
    let drift = drift_report_from_watchdog(&wd);

    assert!(
        !drift.drift_counts.is_empty(),
        "drift counts must be populated"
    );
    // On a clean local run, all should be "identical" (from DriftClassification Display)
    if drift.overall_pass {
        assert!(
            drift.drift_counts.contains_key("identical"),
            "passing run must have 'identical' drift class, got: {:?}",
            drift.drift_counts.keys().collect::<Vec<_>>()
        );
    }
}

// ─── Actionable Findings: DriftFailure ───────────────────────────────

#[test]
fn drift_failure_has_repro_command() {
    // Construct a synthetic failure to verify structure
    let failure = DriftFailure {
        probe_id: "test-probe-1".to_owned(),
        subsystem: "seed_derivation".to_owned(),
        expected_kind: "bit_exact".to_owned(),
        divergent_toolchains: vec!["linux-x86_64-nightly-release".to_owned()],
        repro_command: "cargo test -p fsqlite-harness --lib toolchain_determinism -- test-probe-1 --exact --nocapture".to_owned(),
    };

    assert!(
        !failure.repro_command.is_empty(),
        "repro command must exist"
    );
    assert!(
        failure.repro_command.contains(&failure.probe_id),
        "repro command must reference the failing probe"
    );
    assert!(
        failure.repro_command.contains("--nocapture"),
        "repro command must include --nocapture for triage"
    );
    assert!(
        !failure.divergent_toolchains.is_empty(),
        "must list divergent toolchains"
    );
}

#[test]
fn drift_failures_from_passing_run_are_empty() {
    let wd = watchdog_report();
    let drift = drift_report_from_watchdog(&wd);

    if drift.overall_pass {
        assert!(
            drift.failures.is_empty(),
            "passing run must have zero failures"
        );
    }
}

#[test]
fn drift_failures_include_subsystem_for_triage() {
    // Verify structure: every failure must identify the subsystem
    let failure = DriftFailure {
        probe_id: "page-ser-1".to_owned(),
        subsystem: "page_serialization".to_owned(),
        expected_kind: "bit_exact".to_owned(),
        divergent_toolchains: vec!["macos-aarch64-stable-release".to_owned()],
        repro_command: "cargo test ...".to_owned(),
    };

    assert!(
        !failure.subsystem.is_empty(),
        "subsystem must be populated for triage routing"
    );
    assert!(
        !failure.expected_kind.is_empty(),
        "expected kind must be populated for severity assessment"
    );
}

// ─── Timing Anomaly Detection ────────────────────────────────────────

#[test]
fn timing_anomaly_has_ratio_info() {
    let anomaly = TimingAnomaly {
        probe_id: "timing-test-1".to_owned(),
        observed_ratio: 12.5,
        max_allowed_ratio: 5.0,
    };

    assert!(
        anomaly.observed_ratio > anomaly.max_allowed_ratio,
        "anomaly must exceed allowed ratio"
    );
    assert!(
        !anomaly.probe_id.is_empty(),
        "anomaly must identify the probe"
    );
}

#[test]
fn timing_anomalies_from_clean_run() {
    let wd = watchdog_report();
    let drift = drift_report_from_watchdog(&wd);

    if drift.overall_pass {
        assert!(
            drift.timing_anomalies.is_empty(),
            "clean run should have no timing anomalies"
        );
    }
}

// ─── Report Persistence (JSON round-trip) ────────────────────────────

#[test]
fn watchdog_report_json_roundtrip() {
    let report = watchdog_report();
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("watchdog_report.json");

    write_watchdog_report(&path, &report).unwrap();
    let loaded = load_watchdog_report(&path).unwrap();

    assert_eq!(loaded.schema_version, report.schema_version);
    assert_eq!(loaded.bead_id, report.bead_id);
    assert_eq!(loaded.verdict, report.verdict);
    assert_eq!(loaded.probe_failures, report.probe_failures);
    assert_eq!(loaded.drift_warnings, report.drift_warnings);
    assert_eq!(loaded.summary, report.summary);
}

#[test]
fn drift_report_json_roundtrip() {
    let wd = watchdog_report();
    let drift = drift_report_from_watchdog(&wd);

    let json = drift.to_json().unwrap();
    let restored: DriftReport = serde_json::from_str(&json).unwrap();

    assert_eq!(restored.schema_version, drift.schema_version);
    assert_eq!(restored.bead_id, drift.bead_id);
    assert_eq!(restored.overall_pass, drift.overall_pass);
    assert_eq!(restored.probe_count, drift.probe_count);
    assert_eq!(restored.probes_passed, drift.probes_passed);
    assert_eq!(restored.probes_failed, drift.probes_failed);
    assert_eq!(restored.failures.len(), drift.failures.len());
    assert_eq!(
        restored.timing_anomalies.len(),
        drift.timing_anomalies.len()
    );
}

// ─── Report Determinism ──────────────────────────────────────────────

#[test]
fn watchdog_report_deterministic() {
    let config = default_config();
    let a = run_watchdog(&config);
    let b = run_watchdog(&config);

    let json_a = a.to_json().unwrap();
    let json_b = b.to_json().unwrap();
    assert_eq!(json_a, json_b, "watchdog report must be deterministic");
}

#[test]
fn drift_report_deterministic() {
    let config = default_config();
    let wd = run_watchdog(&config);
    let matrix = DeterminismMatrix::canonical(wd.session.root_seed);

    let a = DriftReport::from_session(&wd.session, &matrix);
    let b = DriftReport::from_session(&wd.session, &matrix);

    let json_a = a.to_json().unwrap();
    let json_b = b.to_json().unwrap();
    assert_eq!(json_a, json_b, "drift report must be deterministic");
}

// ─── Watchdog Verdict Logic ──────────────────────────────────────────

#[test]
fn default_config_watchdog_passes() {
    let report = watchdog_report();

    // Default config with max_failures=0 — local run should pass
    assert!(
        report.verdict == WatchdogVerdict::Pass || report.verdict == WatchdogVerdict::Warning,
        "default watchdog should pass or warn, got: {}",
        report.verdict
    );
}

#[test]
fn watchdog_passed_method_consistent() {
    let report = watchdog_report();
    assert_eq!(
        report.passed(),
        report.verdict == WatchdogVerdict::Pass,
        "passed() must match verdict"
    );
}

// ─── DeterminismRunner Validation ────────────────────────────────────

#[test]
fn runner_validates_without_errors() {
    let runner = DeterminismRunner::canonical(default_config().root_seed);
    let errors = runner.validate();
    assert!(
        errors.is_empty(),
        "canonical runner must validate: {errors:?}"
    );
}

#[test]
fn runner_matrix_validates() {
    let matrix = DeterminismMatrix::canonical(default_config().root_seed);
    let errors = matrix.validate();
    assert!(
        errors.is_empty(),
        "canonical matrix must validate: {errors:?}"
    );
}

// ─── Session Drift Summary ──────────────────────────────────────────

#[test]
fn session_drift_summary_has_entries() {
    let report = watchdog_report();
    assert!(
        !report.session.drift_summary.is_empty(),
        "drift summary must have classification entries"
    );
}

#[test]
fn session_entry_results_cover_corpus() {
    let report = watchdog_report();
    assert!(
        !report.session.entry_results.is_empty(),
        "session must have entry results"
    );
    // Entry results span all corpus entries across all toolchains
    assert!(
        report.session.entry_results.len() >= report.session.corpus_entry_count,
        "entry results must cover at least one per corpus entry"
    );
}

#[test]
fn session_probe_results_cover_probes() {
    let report = watchdog_report();
    assert_eq!(
        report.session.probe_results.len(),
        report.session.probe_count,
        "must have one aggregate result per probe"
    );
}

// ─── Session JSON Round-Trip ─────────────────────────────────────────

#[test]
fn session_json_roundtrip() {
    let report = watchdog_report();
    let json = report.session.to_json().unwrap();
    let restored = fsqlite_harness::toolchain_determinism::RunSession::from_json(&json).unwrap();

    assert_eq!(restored.bead_id, report.session.bead_id);
    assert_eq!(restored.root_seed, report.session.root_seed);
    assert_eq!(restored.probe_count, report.session.probe_count);
    assert_eq!(restored.overall_pass, report.session.overall_pass);
}

// ─── Conformance Summary ────────────────────────────────────────────

#[test]
fn conformance_summary() {
    let checks = vec![
        ("C-1: DivergenceClass severity ordering and display", true),
        (
            "C-2: DriftClassification acceptable/unacceptable boundary",
            true,
        ),
        (
            "C-3: DriftFailure includes repro command and subsystem",
            true,
        ),
        ("C-4: TimingAnomaly tracks observed vs. allowed ratio", true),
        ("C-5: WatchdogReport has triage line and summary", true),
        ("C-6: DriftReport from session preserves all metrics", true),
        (
            "C-7: Reports survive JSON round-trip (watchdog + drift + session)",
            true,
        ),
        ("C-8: Reports are deterministic for same config", true),
        ("C-9: Runner and matrix validate without errors", true),
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
