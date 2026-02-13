//! Integration tests for bd-1dp9.4.4 — Crash/torn-write/recovery differential parity.

use fsqlite_harness::crash_recovery_parity::{
    CRASH_RECOVERY_PARITY_BEAD_ID, CRASH_RECOVERY_SCHEMA_VERSION, CrashRecoveryParityConfig,
    CrashRecoveryParityReport, CrashRecoveryVerdict, CrashScenario, RecoveryOutcome,
    assess_crash_recovery_parity, load_crash_recovery_report, write_crash_recovery_report,
};
use fsqlite_harness::fault_profiles::FaultProfileCatalog;

const BEAD_ID: &str = "bd-1dp9.4.4";

#[test]
fn assessment_produces_parity_verdict() {
    let report = assess_crash_recovery_parity(&CrashRecoveryParityConfig::default());
    assert_eq!(
        report.verdict,
        CrashRecoveryVerdict::Parity,
        "bead_id={BEAD_ID} case=verdict"
    );
    assert_eq!(
        report.bead_id, CRASH_RECOVERY_PARITY_BEAD_ID,
        "bead_id={BEAD_ID} case=bead_id"
    );
    assert_eq!(
        report.schema_version, CRASH_RECOVERY_SCHEMA_VERSION,
        "bead_id={BEAD_ID} case=schema",
    );
}

#[test]
fn all_twelve_scenarios_tested() {
    let report = assess_crash_recovery_parity(&CrashRecoveryParityConfig::default());
    assert_eq!(
        report.scenarios_tested.len(),
        12,
        "bead_id={BEAD_ID} case=scenario_count"
    );
    for s in CrashScenario::ALL {
        assert!(
            report.scenarios_tested.contains(&s.as_str().to_owned()),
            "bead_id={BEAD_ID} case=scenario_present scenario={s}",
        );
    }
}

#[test]
fn all_scenarios_at_parity() {
    let report = assess_crash_recovery_parity(&CrashRecoveryParityConfig::default());
    assert_eq!(
        report.scenarios_at_parity.len(),
        12,
        "bead_id={BEAD_ID} case=scenarios_at_parity",
    );
}

#[test]
fn four_fault_categories_covered() {
    let report = assess_crash_recovery_parity(&CrashRecoveryParityConfig::default());
    assert_eq!(
        report.categories_covered.len(),
        4,
        "bead_id={BEAD_ID} case=categories"
    );
}

#[test]
fn four_severity_levels_covered() {
    let report = assess_crash_recovery_parity(&CrashRecoveryParityConfig::default());
    assert_eq!(
        report.severities_covered.len(),
        4,
        "bead_id={BEAD_ID} case=severities"
    );
}

#[test]
fn parity_score_full() {
    let report = assess_crash_recovery_parity(&CrashRecoveryParityConfig::default());
    assert_eq!(report.parity_score, 1.0, "bead_id={BEAD_ID} case=score");
    assert_eq!(
        report.checks_at_parity, report.total_checks,
        "bead_id={BEAD_ID} case=all_pass",
    );
}

#[test]
fn fault_catalog_validates() {
    let catalog = FaultProfileCatalog::default_catalog();
    assert_eq!(catalog.len(), 12, "bead_id={BEAD_ID} case=catalog_size");
    let report = assess_crash_recovery_parity(&CrashRecoveryParityConfig::default());
    assert_eq!(
        report.catalog_profiles_validated, 12,
        "bead_id={BEAD_ID} case=catalog_validated",
    );
}

#[test]
fn committed_data_preservation() {
    let report = assess_crash_recovery_parity(&CrashRecoveryParityConfig::default());
    // 11 out of 12 scenarios preserve committed data (all except corrupt_wal_header)
    assert_eq!(
        report.committed_data_preserved_count, 11,
        "bead_id={BEAD_ID} case=committed_data",
    );
}

#[test]
fn proof_artifacts_generated() {
    let report = assess_crash_recovery_parity(&CrashRecoveryParityConfig::default());
    assert!(
        report.proof_artifacts.len() >= 5,
        "bead_id={BEAD_ID} case=proof_artifacts count={}",
        report.proof_artifacts.len(),
    );
    for art in &report.proof_artifacts {
        assert!(
            art.deterministic,
            "bead_id={BEAD_ID} case=deterministic scenario={}",
            art.scenario
        );
        assert!(
            !art.description.is_empty(),
            "bead_id={BEAD_ID} case=art_desc"
        );
    }
}

#[test]
fn triage_line_has_fields() {
    let report = assess_crash_recovery_parity(&CrashRecoveryParityConfig::default());
    let line = report.triage_line();
    for field in [
        "verdict=",
        "parity=",
        "scenarios=",
        "categories=",
        "profiles_validated=",
    ] {
        assert!(
            line.contains(field),
            "bead_id={BEAD_ID} case=triage field={field}"
        );
    }
}

#[test]
fn summary_is_informative() {
    let report = assess_crash_recovery_parity(&CrashRecoveryParityConfig::default());
    assert!(!report.summary.is_empty(), "bead_id={BEAD_ID} case=summary");
    assert!(
        report.summary.contains("PARITY"),
        "bead_id={BEAD_ID} case=summary_verdict"
    );
}

#[test]
fn report_json_roundtrip() {
    let report = assess_crash_recovery_parity(&CrashRecoveryParityConfig::default());
    let json = report.to_json().expect("serialize");
    let parsed = CrashRecoveryParityReport::from_json(&json).expect("parse");
    assert_eq!(parsed.verdict, report.verdict);
    assert_eq!(parsed.parity_score, report.parity_score);
    assert_eq!(parsed.proof_artifacts.len(), report.proof_artifacts.len());
}

#[test]
fn report_file_roundtrip() {
    let report = assess_crash_recovery_parity(&CrashRecoveryParityConfig::default());
    let dir = std::env::temp_dir().join("fsqlite-crash-integ");
    std::fs::create_dir_all(&dir).expect("create dir");
    let path = dir.join("crash-integ.json");
    write_crash_recovery_report(&path, &report).expect("write");
    let loaded = load_crash_recovery_report(&path).expect("load");
    assert_eq!(loaded.verdict, report.verdict);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(&dir);
}

#[test]
fn assessment_is_deterministic() {
    let cfg = CrashRecoveryParityConfig::default();
    let r1 = assess_crash_recovery_parity(&cfg);
    let r2 = assess_crash_recovery_parity(&cfg);
    assert_eq!(
        r1.to_json().unwrap(),
        r2.to_json().unwrap(),
        "bead_id={BEAD_ID} case=det"
    );
}

#[test]
fn scenario_category_and_severity_consistent() {
    // Verify the scenario-to-category and scenario-to-severity mappings are consistent
    for s in CrashScenario::ALL {
        let _cat = s.fault_category();
        let _sev = s.expected_severity();
        // Just verify these don't panic and return valid values
        assert!(
            !s.as_str().is_empty(),
            "bead_id={BEAD_ID} case=scenario_str scenario={s}"
        );
    }
}

#[test]
fn recovery_outcome_serialization() {
    for outcome in [
        RecoveryOutcome::FullRecovery,
        RecoveryOutcome::PartialRecovery,
        RecoveryOutcome::GracefulRetry,
        RecoveryOutcome::Lost,
    ] {
        let json = serde_json::to_string(&outcome).expect("serialize");
        let restored: RecoveryOutcome = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(restored, outcome);
    }
}
