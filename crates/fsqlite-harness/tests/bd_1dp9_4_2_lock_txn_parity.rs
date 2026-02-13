//! Integration tests for bd-1dp9.4.2 — Lock/busy/savepoint/autocommit parity.

use fsqlite_harness::lock_txn_parity::{
    LOCK_TXN_PARITY_BEAD_ID, LOCK_TXN_SCHEMA_VERSION, LockTxnParityConfig, LockTxnParityReport,
    TransactionMode, TxnFeatureArea, TxnParityVerdict, assess_lock_txn_parity,
    load_lock_txn_report, write_lock_txn_report,
};

const BEAD_ID: &str = "bd-1dp9.4.2";

#[test]
fn assessment_produces_parity_verdict() {
    let report = assess_lock_txn_parity(&LockTxnParityConfig::default());
    assert_eq!(report.verdict, TxnParityVerdict::Parity, "bead_id={BEAD_ID} case=verdict");
    assert_eq!(report.bead_id, LOCK_TXN_PARITY_BEAD_ID, "bead_id={BEAD_ID} case=bead_id");
    assert_eq!(report.schema_version, LOCK_TXN_SCHEMA_VERSION, "bead_id={BEAD_ID} case=schema");
}

#[test]
fn all_transaction_modes_tested() {
    let report = assess_lock_txn_parity(&LockTxnParityConfig::default());
    assert_eq!(report.txn_modes_tested.len(), 4, "bead_id={BEAD_ID} case=modes_count");
    for mode in TransactionMode::ALL {
        assert!(
            report.txn_modes_tested.contains(&mode.as_str().to_owned()),
            "bead_id={BEAD_ID} case=mode_present mode={mode}",
        );
    }
}

#[test]
fn all_feature_areas_at_parity() {
    let report = assess_lock_txn_parity(&LockTxnParityConfig::default());
    assert_eq!(report.areas_tested.len(), 5, "bead_id={BEAD_ID} case=areas_count");
    for area in TxnFeatureArea::ALL {
        assert!(
            report.areas_at_parity.contains(&area.as_str().to_owned()),
            "bead_id={BEAD_ID} case=area_parity area={area}",
        );
    }
}

#[test]
fn concurrent_and_savepoint_verified() {
    let report = assess_lock_txn_parity(&LockTxnParityConfig::default());
    assert!(report.concurrent_default_verified, "bead_id={BEAD_ID} case=concurrent");
    assert!(report.savepoint_nesting_verified, "bead_id={BEAD_ID} case=savepoints");
}

#[test]
fn parity_score_is_full() {
    let report = assess_lock_txn_parity(&LockTxnParityConfig::default());
    assert_eq!(report.parity_score, 1.0, "bead_id={BEAD_ID} case=score");
    assert_eq!(report.checks_at_parity, report.total_checks, "bead_id={BEAD_ID} case=all_pass");
}

#[test]
fn checks_cover_all_areas() {
    let report = assess_lock_txn_parity(&LockTxnParityConfig::default());
    let areas: Vec<&str> = report.checks.iter().map(|c| c.area.as_str()).collect();
    for expected in ["busy_timeout", "savepoint", "autocommit", "lock_transition", "concurrent_mode"] {
        assert!(areas.contains(&expected), "bead_id={BEAD_ID} case=check_area area={expected}");
    }
}

#[test]
fn triage_line_has_fields() {
    let report = assess_lock_txn_parity(&LockTxnParityConfig::default());
    let line = report.triage_line();
    for field in ["verdict=", "modes=", "areas=", "concurrent=", "savepoints=", "gaps="] {
        assert!(line.contains(field), "bead_id={BEAD_ID} case=triage field={field}");
    }
}

#[test]
fn summary_is_informative() {
    let report = assess_lock_txn_parity(&LockTxnParityConfig::default());
    assert!(!report.summary.is_empty(), "bead_id={BEAD_ID} case=summary_nonempty");
    assert!(report.summary.contains("PARITY"), "bead_id={BEAD_ID} case=summary_verdict");
}

#[test]
fn report_json_roundtrip() {
    let report = assess_lock_txn_parity(&LockTxnParityConfig::default());
    let json = report.to_json().expect("serialize");
    let parsed = LockTxnParityReport::from_json(&json).expect("parse");
    assert_eq!(parsed.verdict, report.verdict);
    assert_eq!(parsed.parity_score, report.parity_score);
    assert_eq!(parsed.total_checks, report.total_checks);
}

#[test]
fn report_file_roundtrip() {
    let report = assess_lock_txn_parity(&LockTxnParityConfig::default());
    let dir = std::env::temp_dir().join("fsqlite-lock-txn-integ");
    std::fs::create_dir_all(&dir).expect("create dir");
    let path = dir.join("lock-txn-integ.json");
    write_lock_txn_report(&path, &report).expect("write");
    let loaded = load_lock_txn_report(&path).expect("load");
    assert_eq!(loaded.verdict, report.verdict);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(&dir);
}

#[test]
fn assessment_is_deterministic() {
    let cfg = LockTxnParityConfig::default();
    let r1 = assess_lock_txn_parity(&cfg);
    let r2 = assess_lock_txn_parity(&cfg);
    assert_eq!(r1.to_json().unwrap(), r2.to_json().unwrap(), "bead_id={BEAD_ID} case=det");
}
