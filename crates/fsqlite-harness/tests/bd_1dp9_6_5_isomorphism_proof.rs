//! Integration tests for bd-1dp9.6.5 — Isomorphism-proof harness.

use fsqlite_harness::isomorphism_proof::{
    ISOMORPHISM_PROOF_BEAD_ID, ISOMORPHISM_PROOF_SCHEMA_VERSION, IsomorphismProofConfig,
    IsomorphismProofReport, IsomorphismVerdict, ProofInvariantClass, assess_isomorphism_proof,
    load_isomorphism_report, write_isomorphism_report,
};

const BEAD_ID: &str = "bd-1dp9.6.5";

#[test]
fn assessment_produces_parity_verdict() {
    let report = assess_isomorphism_proof(&IsomorphismProofConfig::default());
    assert_eq!(
        report.verdict,
        IsomorphismVerdict::Parity,
        "bead_id={BEAD_ID} case=verdict"
    );
    assert_eq!(
        report.bead_id, ISOMORPHISM_PROOF_BEAD_ID,
        "bead_id={BEAD_ID} case=bead_id"
    );
    assert_eq!(
        report.schema_version, ISOMORPHISM_PROOF_SCHEMA_VERSION,
        "bead_id={BEAD_ID} case=schema",
    );
}

#[test]
fn all_ten_invariants_tested() {
    let report = assess_isomorphism_proof(&IsomorphismProofConfig::default());
    assert_eq!(
        report.invariants_tested.len(),
        10,
        "bead_id={BEAD_ID} case=invariant_count"
    );
    for i in ProofInvariantClass::ALL {
        assert!(
            report.invariants_tested.contains(&i.as_str().to_owned()),
            "bead_id={BEAD_ID} case=invariant_present invariant={i}",
        );
    }
}

#[test]
fn all_invariants_preserved() {
    let report = assess_isomorphism_proof(&IsomorphismProofConfig::default());
    assert_eq!(
        report.invariants_preserved.len(),
        10,
        "bead_id={BEAD_ID} case=invariants_preserved"
    );
}

#[test]
fn all_mandatory_preserved() {
    let report = assess_isomorphism_proof(&IsomorphismProofConfig::default());
    assert_eq!(
        report.mandatory_total, 4,
        "bead_id={BEAD_ID} case=mandatory_total"
    );
    assert_eq!(
        report.mandatory_preserved, 4,
        "bead_id={BEAD_ID} case=mandatory_preserved"
    );
}

#[test]
#[allow(clippy::float_cmp)]
fn parity_score_full() {
    let report = assess_isomorphism_proof(&IsomorphismProofConfig::default());
    assert_eq!(report.parity_score, 1.0, "bead_id={BEAD_ID} case=score");
    assert_eq!(
        report.checks_at_parity, report.total_checks,
        "bead_id={BEAD_ID} case=all_pass"
    );
}

#[test]
fn proof_records_generated() {
    let report = assess_isomorphism_proof(&IsomorphismProofConfig::default());
    assert!(
        report.proof_records.len() >= 2,
        "bead_id={BEAD_ID} case=proof_records count={}",
        report.proof_records.len(),
    );
}

#[test]
fn triage_line_has_fields() {
    let report = assess_isomorphism_proof(&IsomorphismProofConfig::default());
    let line = report.triage_line();
    for field in [
        "verdict=",
        "parity=",
        "invariants=",
        "mandatory=",
        "checksum=",
    ] {
        assert!(
            line.contains(field),
            "bead_id={BEAD_ID} case=triage field={field}"
        );
    }
}

#[test]
fn summary_is_informative() {
    let report = assess_isomorphism_proof(&IsomorphismProofConfig::default());
    assert!(!report.summary.is_empty(), "bead_id={BEAD_ID} case=summary");
    assert!(
        report.summary.contains("PARITY"),
        "bead_id={BEAD_ID} case=summary_verdict"
    );
}

#[test]
#[allow(clippy::float_cmp)]
fn report_json_roundtrip() {
    let report = assess_isomorphism_proof(&IsomorphismProofConfig::default());
    let json = report.to_json().expect("serialize");
    let parsed = IsomorphismProofReport::from_json(&json).expect("parse");
    assert_eq!(parsed.verdict, report.verdict);
    assert_eq!(parsed.parity_score, report.parity_score);
}

#[test]
fn report_file_roundtrip() {
    let report = assess_isomorphism_proof(&IsomorphismProofConfig::default());
    let dir = std::env::temp_dir().join("fsqlite-iso-integ");
    std::fs::create_dir_all(&dir).expect("create dir");
    let path = dir.join("iso-integ.json");
    write_isomorphism_report(&path, &report).expect("write");
    let loaded = load_isomorphism_report(&path).expect("load");
    assert_eq!(loaded.verdict, report.verdict);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(&dir);
}

#[test]
fn assessment_is_deterministic() {
    let cfg = IsomorphismProofConfig::default();
    let r1 = assess_isomorphism_proof(&cfg);
    let r2 = assess_isomorphism_proof(&cfg);
    assert_eq!(
        r1.to_json().unwrap(),
        r2.to_json().unwrap(),
        "bead_id={BEAD_ID} case=det"
    );
}
