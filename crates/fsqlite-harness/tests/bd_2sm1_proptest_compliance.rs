use std::fs;
use std::path::{Path, PathBuf};

use proptest::prelude::*;
use serde_json::Value;

const BEAD_ID: &str = "bd-2sm1";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_2sm1_unit_compliance_gate",
    "prop_bd_2sm1_structure_compliance",
];
const E2E_TEST_ID: &str = "test_e2e_bd_2sm1_compliance";
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_STANDARD_REF: &str = "bd-1fpm";
const REQUIRED_TOKENS: [&str; 8] = [
    "test_bd_2sm1_unit_compliance_gate",
    "prop_bd_2sm1_structure_compliance",
    "test_e2e_bd_2sm1_compliance",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "bd-1fpm",
];

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
struct ComplianceEvaluation {
    missing_unit_ids: Vec<&'static str>,
    missing_e2e_id: bool,
    missing_log_levels: Vec<&'static str>,
    missing_log_standard_ref: bool,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && !self.missing_e2e_id
            && self.missing_log_levels.is_empty()
            && !self.missing_log_standard_ref
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root should be canonicalizable")
}

fn load_issue_description(issue_id: &str) -> String {
    let issues_path = workspace_root().join(ISSUES_JSONL);
    let raw = fs::read_to_string(&issues_path).expect("issues.jsonl should be readable");

    for line in raw.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line).expect("issues line should be valid JSON");
        if value.get("id").and_then(Value::as_str) == Some(issue_id) {
            let mut canonical = value
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();

            if let Some(comments) = value.get("comments").and_then(Value::as_array) {
                for comment in comments {
                    if let Some(text) = comment.get("text").and_then(Value::as_str) {
                        canonical.push_str("\n\n");
                        canonical.push_str(text);
                    }
                }
            }

            return canonical;
        }
    }

    panic!("bead_id={issue_id} not found in {ISSUES_JSONL}");
}

fn contains_identifier(text: &str, expected_marker: &str) -> bool {
    text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .any(|token| token == expected_marker)
}

fn evaluate_description(description: &str) -> ComplianceEvaluation {
    let missing_unit_ids = UNIT_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();

    let missing_log_levels = LOG_LEVEL_MARKERS
        .into_iter()
        .filter(|level| !description.contains(level))
        .collect::<Vec<_>>();

    ComplianceEvaluation {
        missing_unit_ids,
        missing_e2e_id: !contains_identifier(description, E2E_TEST_ID),
        missing_log_levels,
        missing_log_standard_ref: !description.contains(LOG_STANDARD_REF),
    }
}

#[test]
fn test_bd_2sm1_unit_compliance_gate() {
    let description = load_issue_description(BEAD_ID);
    let evaluation = evaluate_description(&description);

    assert!(
        evaluation.missing_unit_ids.is_empty(),
        "bead_id={BEAD_ID} case=unit_ids_missing missing={:?}",
        evaluation.missing_unit_ids
    );
    assert!(
        !evaluation.missing_e2e_id,
        "bead_id={BEAD_ID} case=e2e_identifier_missing expected={E2E_TEST_ID}"
    );
    assert!(
        evaluation.missing_log_levels.is_empty(),
        "bead_id={BEAD_ID} case=logging_levels_missing missing={:?}",
        evaluation.missing_log_levels
    );
    assert!(
        !evaluation.missing_log_standard_ref,
        "bead_id={BEAD_ID} case=logging_standard_missing expected_ref={LOG_STANDARD_REF}"
    );
}

proptest! {
    #[test]
    fn prop_bd_2sm1_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Test Requirements\n- {}\n- {}\n\n## E2E Test\n- {}\n\n## Logging Requirements\n- DEBUG: stage progress\n- INFO: summary\n- WARN: degraded mode\n- ERROR: terminal failure\n- Reference: {}\n",
            UNIT_TEST_IDS[0],
            UNIT_TEST_IDS[1],
            E2E_TEST_ID,
            LOG_STANDARD_REF,
        );

        synthetic = synthetic.replacen(REQUIRED_TOKENS[missing_index], "", 1);
        let evaluation = evaluate_description(&synthetic);
        prop_assert!(
            !evaluation.is_compliant(),
            "bead_id={} case=structure_compliance expected_non_compliant missing_index={} token={}",
            BEAD_ID,
            missing_index,
            REQUIRED_TOKENS[missing_index]
        );
    }
}

#[test]
fn test_e2e_bd_2sm1_compliance() {
    let description = load_issue_description(BEAD_ID);
    let evaluation = evaluate_description(&description);

    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_e2e_id={} missing_log_levels={} missing_log_standard_ref={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_e2e_id,
        evaluation.missing_log_levels.len(),
        evaluation.missing_log_standard_ref
    );

    for id in &evaluation.missing_unit_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_unit_id id={id}");
    }
    if evaluation.missing_e2e_id {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_e2e_id id={E2E_TEST_ID}");
    }
    for level in &evaluation.missing_log_levels {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_log_level level={level}");
    }
    if evaluation.missing_log_standard_ref {
        eprintln!(
            "ERROR bead_id={BEAD_ID} case=missing_log_standard_ref expected={LOG_STANDARD_REF}"
        );
    }

    assert!(
        evaluation.is_compliant(),
        "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?}"
    );
}
