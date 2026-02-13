//! Integration tests for bd-1dp9.4.5 — Concurrent-writer-default invariants.

use fsqlite_harness::concurrent_writer_parity::{
    CONCURRENT_WRITER_PARITY_BEAD_ID, CONCURRENT_WRITER_SCHEMA_VERSION,
    ConcurrentInvariantArea, ConcurrentWriterParityConfig, ConcurrentWriterParityReport,
    ConcurrentWriterVerdict, assess_concurrent_writer_parity, load_concurrent_writer_report,
    write_concurrent_writer_report,
};

const BEAD_ID: &str = "bd-1dp9.4.5";

#[test]
fn assessment_produces_parity_verdict() {
    let report = assess_concurrent_writer_parity(&ConcurrentWriterParityConfig::default());
    assert_eq!(
        report.verdict,
        ConcurrentWriterVerdict::Parity,
        "bead_id={BEAD_ID} case=verdict",
    );
    assert_eq!(report.bead_id, CONCURRENT_WRITER_PARITY_BEAD_ID, "bead_id={BEAD_ID} case=bead_id");
    assert_eq!(
        report.schema_version, CONCURRENT_WRITER_SCHEMA_VERSION,
        "bead_id={BEAD_ID} case=schema",
    );
}

#[test]
fn all_ten_areas_tested() {
    let report = assess_concurrent_writer_parity(&ConcurrentWriterParityConfig::default());
    assert_eq!(report.areas_tested.len(), 10, "bead_id={BEAD_ID} case=area_count");
    for a in ConcurrentInvariantArea::ALL {
        assert!(
            report.areas_tested.contains(&a.as_str().to_owned()),
            "bead_id={BEAD_ID} case=area_present area={a}",
        );
    }
}

#[test]
fn all_areas_at_parity() {
    let report = assess_concurrent_writer_parity(&ConcurrentWriterParityConfig::default());
    assert_eq!(report.areas_at_parity.len(), 10, "bead_id={BEAD_ID} case=areas_at_parity");
}

#[test]
fn all_critical_areas_pass() {
    let report = assess_concurrent_writer_parity(&ConcurrentWriterParityConfig::default());
    assert_eq!(report.critical_areas_total, 5, "bead_id={BEAD_ID} case=critical_total");
    assert_eq!(
        report.critical_areas_at_parity, 5,
        "bead_id={BEAD_ID} case=critical_parity",
    );
}

#[test]
fn parity_score_full() {
    let report = assess_concurrent_writer_parity(&ConcurrentWriterParityConfig::default());
    assert_eq!(report.parity_score, 1.0, "bead_id={BEAD_ID} case=score");
    assert_eq!(
        report.checks_at_parity, report.total_checks,
        "bead_id={BEAD_ID} case=all_pass",
    );
}

#[test]
fn max_concurrency_tested() {
    let report = assess_concurrent_writer_parity(&ConcurrentWriterParityConfig::default());
    assert!(
        report.max_writer_concurrency_tested >= 100,
        "bead_id={BEAD_ID} case=max_writers val={}",
        report.max_writer_concurrency_tested,
    );
}

#[test]
fn triage_line_has_fields() {
    let report = assess_concurrent_writer_parity(&ConcurrentWriterParityConfig::default());
    let line = report.triage_line();
    for field in ["verdict=", "parity=", "areas=", "critical=", "max_writers="] {
        assert!(line.contains(field), "bead_id={BEAD_ID} case=triage field={field}");
    }
}

#[test]
fn summary_is_informative() {
    let report = assess_concurrent_writer_parity(&ConcurrentWriterParityConfig::default());
    assert!(!report.summary.is_empty(), "bead_id={BEAD_ID} case=summary");
    assert!(report.summary.contains("PARITY"), "bead_id={BEAD_ID} case=summary_verdict");
}

#[test]
fn report_json_roundtrip() {
    let report = assess_concurrent_writer_parity(&ConcurrentWriterParityConfig::default());
    let json = report.to_json().expect("serialize");
    let parsed = ConcurrentWriterParityReport::from_json(&json).expect("parse");
    assert_eq!(parsed.verdict, report.verdict);
    assert_eq!(parsed.parity_score, report.parity_score);
}

#[test]
fn report_file_roundtrip() {
    let report = assess_concurrent_writer_parity(&ConcurrentWriterParityConfig::default());
    let dir = std::env::temp_dir().join("fsqlite-concurrent-integ");
    std::fs::create_dir_all(&dir).expect("create dir");
    let path = dir.join("concurrent-integ.json");
    write_concurrent_writer_report(&path, &report).expect("write");
    let loaded = load_concurrent_writer_report(&path).expect("load");
    assert_eq!(loaded.verdict, report.verdict);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(&dir);
}

#[test]
fn assessment_is_deterministic() {
    let cfg = ConcurrentWriterParityConfig::default();
    let r1 = assess_concurrent_writer_parity(&cfg);
    let r2 = assess_concurrent_writer_parity(&cfg);
    assert_eq!(r1.to_json().unwrap(), r2.to_json().unwrap(), "bead_id={BEAD_ID} case=det");
}

#[test]
fn critical_checks_flag_consistency() {
    let report = assess_concurrent_writer_parity(&ConcurrentWriterParityConfig::default());
    // Critical checks should cover: default_mode, first_committer_wins, ssi_validation,
    // page_level_locking, deadlock_freedom
    let critical_areas: Vec<&str> = report
        .checks
        .iter()
        .filter(|c| c.critical)
        .map(|c| c.area.as_str())
        .collect();
    for area in ["default_mode", "first_committer_wins", "ssi_validation", "page_level_locking", "deadlock_freedom"] {
        assert!(
            critical_areas.contains(&area),
            "bead_id={BEAD_ID} case=critical_area area={area}",
        );
    }
}
