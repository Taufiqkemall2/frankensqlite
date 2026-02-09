use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-2zoa";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_2zoa_unit_compliance_gate",
    "prop_bd_2zoa_structure_compliance",
];
const E2E_TEST_IDS: [&str; 2] = ["test_e2e_bd_2zoa", "test_e2e_bd_2zoa_compliance"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_STANDARD_REF: &str = "bd-1fpm";
const REPORT_SCRIPT: &str = "e2e/arc_warmup_report.sh";
const REPORT_JSONL: &str = "test-results/bd_2zoa_arc_warmup_report.jsonl";
const REQUIRED_TOKENS: [&str; 20] = [
    "test_bd_2zoa_unit_compliance_gate",
    "prop_bd_2zoa_structure_compliance",
    "test_e2e_bd_2zoa",
    "test_e2e_bd_2zoa_compliance",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "bd-1fpm",
    "OLTP point queries",
    "Mixed OLTP + scan",
    "Full table scan",
    "Zipfian",
    "MVCC 8 writers",
    "Cold start",
    "Learning",
    "Steady state",
    "PRAGMA cache_warm",
    "WAL index",
    "sqlite_master root pages",
];

#[derive(Debug, Clone, Copy)]
struct Requirement {
    key: &'static str,
    alternatives: &'static [&'static str],
}

const WORKLOAD_REQUIREMENTS: [Requirement; 5] = [
    Requirement {
        key: "oltp_point_queries",
        alternatives: &["OLTP point queries", "OLTP point query workload"],
    },
    Requirement {
        key: "mixed_oltp_scan",
        alternatives: &["Mixed OLTP + scan", "Mixed OLTP+scan"],
    },
    Requirement {
        key: "full_scan",
        alternatives: &["Full table scan"],
    },
    Requirement {
        key: "zipf",
        alternatives: &["Zipfian", "Zipf"],
    },
    Requirement {
        key: "mvcc_eight_writers",
        alternatives: &["MVCC 8 writers", "8-writer MVCC"],
    },
];

const WARMUP_REQUIREMENTS: [Requirement; 4] = [
    Requirement {
        key: "cold_start",
        alternatives: &["Cold start"],
    },
    Requirement {
        key: "learning_phase",
        alternatives: &["Learning"],
    },
    Requirement {
        key: "steady_state",
        alternatives: &["Steady state"],
    },
    Requirement {
        key: "three_x_capacity",
        alternatives: &["3x capacity"],
    },
];

const PREWARM_REQUIREMENTS: [Requirement; 4] = [
    Requirement {
        key: "pragma_cache_warm",
        alternatives: &["PRAGMA cache_warm", "cache_warm=ON"],
    },
    Requirement {
        key: "wal_index",
        alternatives: &["WAL index"],
    },
    Requirement {
        key: "half_capacity_limit",
        alternatives: &["half capacity", "half-capacity"],
    },
    Requirement {
        key: "sqlite_master_root_pages",
        alternatives: &[
            "sqlite_master root pages",
            "root pages of all tables/indexes",
        ],
    },
];

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
struct ComplianceEvaluation {
    missing_unit_ids: Vec<&'static str>,
    missing_e2e_ids: Vec<&'static str>,
    missing_log_levels: Vec<&'static str>,
    missing_log_standard_ref: bool,
    missing_workload_markers: Vec<&'static str>,
    missing_warmup_markers: Vec<&'static str>,
    missing_prewarm_markers: Vec<&'static str>,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && self.missing_e2e_ids.is_empty()
            && self.missing_log_levels.is_empty()
            && !self.missing_log_standard_ref
            && self.missing_workload_markers.is_empty()
            && self.missing_warmup_markers.is_empty()
            && self.missing_prewarm_markers.is_empty()
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

fn contains_identifier(text: &str, expected_marker: &str) -> bool {
    text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .any(|token| token == expected_marker)
}

fn missing_requirements<'a>(description: &str, requirements: &'a [Requirement]) -> Vec<&'a str> {
    requirements
        .iter()
        .filter_map(|requirement| {
            let present = requirement
                .alternatives
                .iter()
                .any(|marker| description.contains(marker));
            (!present).then_some(requirement.key)
        })
        .collect::<Vec<_>>()
}

fn evaluate_description(description: &str) -> ComplianceEvaluation {
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

    ComplianceEvaluation {
        missing_unit_ids,
        missing_e2e_ids,
        missing_log_levels,
        missing_log_standard_ref: !description.contains(LOG_STANDARD_REF),
        missing_workload_markers: missing_requirements(description, &WORKLOAD_REQUIREMENTS),
        missing_warmup_markers: missing_requirements(description, &WARMUP_REQUIREMENTS),
        missing_prewarm_markers: missing_requirements(description, &PREWARM_REQUIREMENTS),
    }
}

#[test]
fn test_bd_2zoa_unit_compliance_gate() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    if !evaluation.missing_unit_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=unit_ids_missing missing={:?}",
            evaluation.missing_unit_ids
        ));
    }
    if !evaluation.missing_e2e_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_ids_missing missing={:?}",
            evaluation.missing_e2e_ids
        ));
    }
    if !evaluation.missing_log_levels.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=logging_levels_missing missing={:?}",
            evaluation.missing_log_levels
        ));
    }
    if evaluation.missing_log_standard_ref {
        return Err(format!(
            "bead_id={BEAD_ID} case=logging_standard_missing expected_ref={LOG_STANDARD_REF}"
        ));
    }
    if !evaluation.missing_workload_markers.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=workload_markers_missing missing={:?}",
            evaluation.missing_workload_markers
        ));
    }
    if !evaluation.missing_warmup_markers.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=warmup_markers_missing missing={:?}",
            evaluation.missing_warmup_markers
        ));
    }
    if !evaluation.missing_prewarm_markers.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=prewarm_markers_missing missing={:?}",
            evaluation.missing_prewarm_markers
        ));
    }

    Ok(())
}

proptest! {
    #[test]
    fn prop_bd_2zoa_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Test Requirements\n- {}\n- {}\n\n## E2E Test\n- {}\n- {}\n\n## Workloads\n- OLTP point queries\n- Mixed OLTP + scan\n- Full table scan\n- Zipfian\n- MVCC 8 writers\n\n## Warm-Up\n- Cold start\n- Learning\n- Steady state\n- 3x capacity\n\n## Prewarm\n- PRAGMA cache_warm=ON\n- WAL index\n- half capacity\n- sqlite_master root pages\n\n## Logging Requirements\n- DEBUG: stage progress\n- INFO: summary\n- WARN: degraded mode\n- ERROR: terminal failure\n- Reference: {}\n",
            UNIT_TEST_IDS[0],
            UNIT_TEST_IDS[1],
            E2E_TEST_IDS[0],
            E2E_TEST_IDS[1],
            LOG_STANDARD_REF,
        );

        synthetic = synthetic.replacen(REQUIRED_TOKENS[missing_index], "", 1);
        let evaluation = evaluate_description(&synthetic);
        if evaluation.is_compliant() {
            return Err(TestCaseError::fail(format!(
                "bead_id={} case=structure_compliance expected_non_compliant missing_index={} missing_marker={}",
                BEAD_ID,
                missing_index,
                REQUIRED_TOKENS[missing_index]
            )));
        }
    }
}

#[test]
fn test_workload_markers_present() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);
    if evaluation.missing_workload_markers.is_empty() {
        return Ok(());
    }

    Err(format!(
        "bead_id={BEAD_ID} case=workload_markers_missing missing={:?}",
        evaluation.missing_workload_markers
    ))
}

#[test]
fn test_warmup_markers_present() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);
    if evaluation.missing_warmup_markers.is_empty() {
        return Ok(());
    }

    Err(format!(
        "bead_id={BEAD_ID} case=warmup_markers_missing missing={:?}",
        evaluation.missing_warmup_markers
    ))
}

#[test]
fn test_prewarm_markers_present() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);
    if evaluation.missing_prewarm_markers.is_empty() {
        return Ok(());
    }

    Err(format!(
        "bead_id={BEAD_ID} case=prewarm_markers_missing missing={:?}",
        evaluation.missing_prewarm_markers
    ))
}

#[test]
fn test_e2e_bd_2zoa_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_e2e_ids={} missing_log_levels={} missing_workload_markers={} missing_warmup_markers={} missing_prewarm_markers={} missing_log_standard_ref={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_workload_markers.len(),
        evaluation.missing_warmup_markers.len(),
        evaluation.missing_prewarm_markers.len(),
        evaluation.missing_log_standard_ref,
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
    for marker in &evaluation.missing_workload_markers {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_workload_marker marker={marker}");
    }
    for marker in &evaluation.missing_warmup_markers {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_warmup_marker marker={marker}");
    }
    for marker in &evaluation.missing_prewarm_markers {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_prewarm_marker marker={marker}");
    }
    if evaluation.missing_log_standard_ref {
        eprintln!(
            "ERROR bead_id={BEAD_ID} case=missing_log_standard_ref expected={LOG_STANDARD_REF}"
        );
    }

    if !evaluation.is_compliant() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?}"
        ));
    }

    let root = workspace_root()?;
    let script_path = root.join(REPORT_SCRIPT);
    let output = Command::new(&script_path)
        .current_dir(&root)
        .output()
        .map_err(|error| {
            format!(
                "bead_id={BEAD_ID} case=e2e_script_exec_failed script={} error={error}",
                script_path.display()
            )
        })?;

    if !output.status.success() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_script_failed status={} stdout={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        ));
    }

    let report_path = root.join(REPORT_JSONL);
    let report = fs::read_to_string(&report_path).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=missing_report path={} error={error}",
            report_path.display()
        )
    })?;

    let json_row_count = report.lines().filter(|line| line.starts_with('{')).count();
    if json_row_count == 0 {
        return Err(format!(
            "bead_id={BEAD_ID} case=empty_report path={} expected_non_zero_json_rows",
            report_path.display()
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_bd_2zoa() -> Result<(), String> {
    test_e2e_bd_2zoa_compliance()
}
