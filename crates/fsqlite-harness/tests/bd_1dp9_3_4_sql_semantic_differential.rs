//! Integration tests for bd-1dp9.3.4 — SQL semantic differential unit+e2e suites.

use fsqlite_harness::sql_semantic_differential::{
    SQL_SEMANTIC_DIFF_BEAD_ID, SQL_SEMANTIC_DIFF_SCHEMA_VERSION, SemanticTestLayer,
    SqlSemanticDiffConfig, SqlSemanticDiffReport, SqlSemanticVerdict,
    assess_sql_semantic_differential, load_sql_semantic_report, write_sql_semantic_report,
};

const BEAD_ID: &str = "bd-1dp9.3.4";

#[test]
fn assessment_produces_parity_verdict() {
    let report = assess_sql_semantic_differential(&SqlSemanticDiffConfig::default());
    assert_eq!(
        report.verdict,
        SqlSemanticVerdict::Parity,
        "bead_id={BEAD_ID} case=verdict"
    );
    assert_eq!(
        report.bead_id, SQL_SEMANTIC_DIFF_BEAD_ID,
        "bead_id={BEAD_ID} case=bead_id"
    );
    assert_eq!(
        report.schema_version, SQL_SEMANTIC_DIFF_SCHEMA_VERSION,
        "bead_id={BEAD_ID} case=schema",
    );
}

#[test]
fn all_eight_layers_tested() {
    let report = assess_sql_semantic_differential(&SqlSemanticDiffConfig::default());
    assert_eq!(
        report.layers_tested.len(),
        8,
        "bead_id={BEAD_ID} case=layer_count"
    );
    for l in SemanticTestLayer::ALL {
        assert!(
            report.layers_tested.contains(&l.as_str().to_owned()),
            "bead_id={BEAD_ID} case=layer_present layer={l}",
        );
    }
}

#[test]
fn all_layers_at_parity() {
    let report = assess_sql_semantic_differential(&SqlSemanticDiffConfig::default());
    assert_eq!(
        report.layers_at_parity.len(),
        8,
        "bead_id={BEAD_ID} case=layers_at_parity"
    );
}

#[test]
fn pipeline_stages_comprehensive() {
    let report = assess_sql_semantic_differential(&SqlSemanticDiffConfig::default());
    assert!(
        report.pipeline_stages_covered.len() >= 6,
        "bead_id={BEAD_ID} case=pipeline_stages count={}",
        report.pipeline_stages_covered.len(),
    );
    for stage in ["parse", "resolve", "plan", "execute"] {
        assert!(
            report.pipeline_stages_covered.contains(&stage.to_owned()),
            "bead_id={BEAD_ID} case=stage_present stage={stage}",
        );
    }
}

#[test]
fn parity_score_full() {
    let report = assess_sql_semantic_differential(&SqlSemanticDiffConfig::default());
    assert_eq!(report.parity_score, 1.0, "bead_id={BEAD_ID} case=score");
    assert_eq!(
        report.checks_at_parity, report.total_checks,
        "bead_id={BEAD_ID} case=all_pass",
    );
}

#[test]
fn triage_line_has_fields() {
    let report = assess_sql_semantic_differential(&SqlSemanticDiffConfig::default());
    let line = report.triage_line();
    for field in ["verdict=", "parity=", "layers=", "pipeline_stages="] {
        assert!(
            line.contains(field),
            "bead_id={BEAD_ID} case=triage field={field}"
        );
    }
}

#[test]
fn summary_is_informative() {
    let report = assess_sql_semantic_differential(&SqlSemanticDiffConfig::default());
    assert!(!report.summary.is_empty(), "bead_id={BEAD_ID} case=summary");
    assert!(
        report.summary.contains("PARITY"),
        "bead_id={BEAD_ID} case=summary_verdict"
    );
}

#[test]
fn report_json_roundtrip() {
    let report = assess_sql_semantic_differential(&SqlSemanticDiffConfig::default());
    let json = report.to_json().expect("serialize");
    let parsed = SqlSemanticDiffReport::from_json(&json).expect("parse");
    assert_eq!(parsed.verdict, report.verdict);
    assert_eq!(parsed.parity_score, report.parity_score);
}

#[test]
fn report_file_roundtrip() {
    let report = assess_sql_semantic_differential(&SqlSemanticDiffConfig::default());
    let dir = std::env::temp_dir().join("fsqlite-sql-sem-integ");
    std::fs::create_dir_all(&dir).expect("create dir");
    let path = dir.join("sql-sem-integ.json");
    write_sql_semantic_report(&path, &report).expect("write");
    let loaded = load_sql_semantic_report(&path).expect("load");
    assert_eq!(loaded.verdict, report.verdict);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(&dir);
}

#[test]
fn assessment_is_deterministic() {
    let cfg = SqlSemanticDiffConfig::default();
    let r1 = assess_sql_semantic_differential(&cfg);
    let r2 = assess_sql_semantic_differential(&cfg);
    assert_eq!(
        r1.to_json().unwrap(),
        r2.to_json().unwrap(),
        "bead_id={BEAD_ID} case=det"
    );
}
