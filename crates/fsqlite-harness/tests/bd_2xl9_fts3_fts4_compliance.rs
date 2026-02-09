use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-2xl9";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const LOG_STANDARD_REF: &str = "bd-1fpm";
const E2E_SCRIPT: &str = "e2e/fts3_compat_report.sh";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_2xl9_unit_compliance_gate",
    "prop_bd_2xl9_structure_compliance",
];
const E2E_TEST_IDS: [&str; 2] = ["test_e2e_bd_2xl9", "test_e2e_bd_2xl9_compliance"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const SEMANTIC_MARKERS: [&str; 5] = [
    "column-level MATCH",
    "explicit AND",
    "unary NOT",
    "matchinfo",
    "offsets",
];
const REQUIRED_TOKENS: [&str; 14] = [
    "test_bd_2xl9_unit_compliance_gate",
    "prop_bd_2xl9_structure_compliance",
    "test_e2e_bd_2xl9",
    "test_e2e_bd_2xl9_compliance",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "bd-1fpm",
    "column-level MATCH",
    "explicit AND",
    "unary NOT",
    "matchinfo",
    "offsets",
];

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
struct ComplianceEvaluation {
    missing_unit_ids: Vec<&'static str>,
    missing_e2e_ids: Vec<&'static str>,
    missing_log_levels: Vec<&'static str>,
    missing_semantic_markers: Vec<&'static str>,
    missing_log_standard_ref: bool,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && self.missing_e2e_ids.is_empty()
            && self.missing_log_levels.is_empty()
            && self.missing_semantic_markers.is_empty()
            && !self.missing_log_standard_ref
    }
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn load_issue_description(issue_id: &str) -> Result<String, String> {
    let issues_path = workspace_root()?.join(ISSUES_JSONL);
    let raw = fs::read_to_string(&issues_path).map_err(|error| {
        format!(
            "issues_jsonl_read_failed path={} error={error}",
            issues_path.display()
        )
    })?;

    for line in raw.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line)
            .map_err(|error| format!("issues_jsonl_parse_failed error={error} line={line}"))?;
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

            return Ok(canonical);
        }
    }

    Err(format!("bead_id={issue_id} not_found_in={ISSUES_JSONL}"))
}

fn contains_identifier(text: &str, marker: &str) -> bool {
    text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.'))
        .any(|part| part == marker)
}

fn evaluate_description(description: &str) -> ComplianceEvaluation {
    let lower = description.to_ascii_lowercase();

    let missing_unit_ids = UNIT_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();

    let missing_e2e_ids = E2E_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();

    let missing_log_levels = LOG_LEVEL_MARKERS
        .into_iter()
        .filter(|level| !description.contains(level))
        .collect::<Vec<_>>();

    let missing_semantic_markers = SEMANTIC_MARKERS
        .into_iter()
        .filter(|marker| !lower.contains(&marker.to_ascii_lowercase()))
        .collect::<Vec<_>>();

    ComplianceEvaluation {
        missing_unit_ids,
        missing_e2e_ids,
        missing_log_levels,
        missing_semantic_markers,
        missing_log_standard_ref: !description.contains(LOG_STANDARD_REF),
    }
}

#[test]
fn test_bd_2xl9_unit_compliance_gate() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    if !evaluation.missing_unit_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=missing_unit_ids ids={:?}",
            evaluation.missing_unit_ids
        ));
    }
    if !evaluation.missing_e2e_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=missing_e2e_ids ids={:?}",
            evaluation.missing_e2e_ids
        ));
    }
    if !evaluation.missing_log_levels.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=missing_log_levels levels={:?}",
            evaluation.missing_log_levels
        ));
    }
    if !evaluation.missing_semantic_markers.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=missing_semantic_markers markers={:?}",
            evaluation.missing_semantic_markers
        ));
    }
    if evaluation.missing_log_standard_ref {
        return Err(format!(
            "bead_id={BEAD_ID} case=missing_log_standard expected_ref={LOG_STANDARD_REF}"
        ));
    }

    Ok(())
}

proptest! {
    #[test]
    fn prop_bd_2xl9_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Test Requirements\n- {}\n- {}\n\n## E2E Test\n- {}\n- {}\n\n## FTS3/FTS4 Semantics\n- column-level MATCH\n- explicit AND\n- unary NOT\n- matchinfo\n- offsets\n\n## Logging Requirements\n- DEBUG: stage-level progress\n- INFO: summary counters\n- WARN: degraded conditions\n- ERROR: terminal diagnostics\n- Reference: {}\n",
            UNIT_TEST_IDS[0],
            UNIT_TEST_IDS[1],
            E2E_TEST_IDS[0],
            E2E_TEST_IDS[1],
            LOG_STANDARD_REF
        );

        synthetic = synthetic.replacen(REQUIRED_TOKENS[missing_index], "", 1);
        let evaluation = evaluate_description(&synthetic);
        if evaluation.is_compliant() {
            return Err(TestCaseError::fail(format!(
                "bead_id={} case=structure_compliance expected_non_compliant missing_index={} marker={}",
                BEAD_ID,
                missing_index,
                REQUIRED_TOKENS[missing_index]
            )));
        }
    }
}

#[test]
fn test_e2e_bd_2xl9_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_e2e_ids={} missing_log_levels={} missing_semantics={} missing_log_standard_ref={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_semantic_markers.len(),
        evaluation.missing_log_standard_ref
    );

    for id in &evaluation.missing_unit_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_unit_id id={id}");
    }
    for id in &evaluation.missing_e2e_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_e2e_id id={id}");
    }
    for level in &evaluation.missing_log_levels {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_log_level level={level}");
    }
    for marker in &evaluation.missing_semantic_markers {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_semantic_marker marker={marker}");
    }
    if evaluation.missing_log_standard_ref {
        eprintln!(
            "ERROR bead_id={BEAD_ID} case=missing_log_standard_ref expected={LOG_STANDARD_REF}"
        );
    }

    let workspace_root = workspace_root()?;
    let status = Command::new("bash")
        .arg(E2E_SCRIPT)
        .current_dir(&workspace_root)
        .status()
        .map_err(|error| format!("bead_id={BEAD_ID} case=e2e_script_spawn_failed error={error}"))?;

    if !status.success() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_script_failed script={E2E_SCRIPT} status={status}"
        ));
    }

    if !evaluation.is_compliant() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_bd_2xl9() -> Result<(), String> {
    test_e2e_bd_2xl9_compliance()
}
