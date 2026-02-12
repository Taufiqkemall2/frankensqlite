use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Deserialize;

const BEAD_ID: &str = "bd-16e7";
const LOG_PREFIX: &str = "[REGR_GUARD]";
const REGRESSION_BASELINE_PATH: &str = "tests/regression_baseline.json";
const CHILD_ENV: &str = "FSQLITE_REGRESSION_GUARD_CHILD";

#[derive(Debug, Clone, Deserialize)]
struct RegressionBaseline {
    as_of_phase: String,
    total_tests: u64,
    passed: u64,
    failed: u64,
    ignored: u64,
    baseline_commit: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RegressionCounts {
    total_tests: u64,
    passed: u64,
    failed: u64,
    ignored: u64,
}

impl RegressionCounts {
    const fn zero() -> Self {
        Self {
            total_tests: 0,
            passed: 0,
            failed: 0,
            ignored: 0,
        }
    }

    fn add(&mut self, rhs: Self) {
        self.total_tests = self.total_tests.saturating_add(rhs.total_tests);
        self.passed = self.passed.saturating_add(rhs.passed);
        self.failed = self.failed.saturating_add(rhs.failed);
        self.ignored = self.ignored.saturating_add(rhs.ignored);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RegressionDelta {
    delta_total: i64,
    delta_passed: i64,
    delta_failed: i64,
    delta_ignored: i64,
    new_tests: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RegressionReport {
    pass: bool,
    delta: RegressionDelta,
    reason: Option<String>,
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("harness crate should be nested under workspace root")
}

fn baseline_path(root: &Path) -> PathBuf {
    root.join(REGRESSION_BASELINE_PATH)
}

fn load_regression_baseline(path: &Path) -> Result<RegressionBaseline, String> {
    let bytes = fs::read(path)
        .map_err(|error| format!("unable to read baseline at {}: {error}", path.display()))?;
    serde_json::from_slice::<RegressionBaseline>(&bytes).map_err(|error| {
        format!(
            "unable to parse baseline JSON at {}: {error}",
            path.display()
        )
    })
}

fn parse_count_segment(segment: &str, label: &str) -> Option<u64> {
    let suffix = format!(" {label}");
    let value_prefix = segment.trim().strip_suffix(&suffix)?;
    let count_text = value_prefix.split_whitespace().last()?;
    count_text.parse::<u64>().ok()
}

fn parse_summary_line(line: &str) -> Option<RegressionCounts> {
    if !line.contains("test result:") {
        return None;
    }

    let mut passed = None;
    let mut failed = None;
    let mut ignored = None;

    for segment in line.split(';') {
        if passed.is_none() {
            passed = parse_count_segment(segment, "passed");
        }
        if failed.is_none() {
            failed = parse_count_segment(segment, "failed");
        }
        if ignored.is_none() {
            ignored = parse_count_segment(segment, "ignored");
        }
    }

    let passed = passed?;
    let failed = failed?;
    let ignored = ignored?;
    let total_tests = passed.saturating_add(failed).saturating_add(ignored);

    Some(RegressionCounts {
        total_tests,
        passed,
        failed,
        ignored,
    })
}

fn parse_workspace_test_counts(output: &str) -> Result<RegressionCounts, String> {
    let mut totals = RegressionCounts::zero();
    let mut summary_lines = 0_u64;

    for line in output.lines() {
        if let Some(parsed) = parse_summary_line(line) {
            summary_lines = summary_lines.saturating_add(1);
            totals.add(parsed);
        }
    }

    if summary_lines == 0 {
        return Err("no cargo test summary lines were found in output".to_owned());
    }

    Ok(totals)
}

fn as_i64(value: i128) -> i64 {
    match i64::try_from(value) {
        Ok(v) => v,
        Err(_) => {
            if value.is_negative() {
                i64::MIN
            } else {
                i64::MAX
            }
        }
    }
}

fn compare_against_baseline(
    baseline: &RegressionBaseline,
    actual: &RegressionCounts,
) -> RegressionReport {
    let delta_total = i128::from(actual.total_tests) - i128::from(baseline.total_tests);
    let delta_passed = i128::from(actual.passed) - i128::from(baseline.passed);
    let delta_failed = i128::from(actual.failed) - i128::from(baseline.failed);
    let delta_ignored = i128::from(actual.ignored) - i128::from(baseline.ignored);

    let delta = RegressionDelta {
        delta_total: as_i64(delta_total),
        delta_passed: as_i64(delta_passed),
        delta_failed: as_i64(delta_failed),
        delta_ignored: as_i64(delta_ignored),
        new_tests: as_i64(delta_total),
    };

    let mut reasons = Vec::new();
    if actual.failed > baseline.failed {
        reasons.push(format!(
            "failed increased from {} to {}",
            baseline.failed, actual.failed
        ));
    }
    if actual.passed < baseline.passed {
        reasons.push(format!(
            "passed decreased from {} to {}",
            baseline.passed, actual.passed
        ));
    }
    if actual.ignored > baseline.ignored {
        reasons.push(format!(
            "ignored increased from {} to {}",
            baseline.ignored, actual.ignored
        ));
    }
    if actual.total_tests < baseline.total_tests {
        reasons.push(format!(
            "total tests decreased from {} to {}",
            baseline.total_tests, actual.total_tests
        ));
    }

    let pass = reasons.is_empty();
    let reason = if pass { None } else { Some(reasons.join("; ")) };

    RegressionReport {
        pass,
        delta,
        reason,
    }
}

fn extract_failed_tests(output: &str) -> Vec<String> {
    let mut failed = Vec::new();
    for line in output.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("test ") && trimmed.ends_with(" ... FAILED") {
            failed.push(trimmed.to_owned());
        }
    }
    failed
}

#[test]
fn test_regression_guard_parses_cargo_output() {
    let sample = r"
test result: ok. 4 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
";

    let counts = parse_workspace_test_counts(sample)
        .expect("sample output should parse into aggregate regression counts");
    assert_eq!(
        counts.total_tests, 7,
        "bead_id={BEAD_ID} case=parse_output_total"
    );
    assert_eq!(
        counts.passed, 6,
        "bead_id={BEAD_ID} case=parse_output_passed"
    );
    assert_eq!(
        counts.failed, 0,
        "bead_id={BEAD_ID} case=parse_output_failed"
    );
    assert_eq!(
        counts.ignored, 1,
        "bead_id={BEAD_ID} case=parse_output_ignored"
    );
}

#[test]
fn test_regression_guard_detects_failure() {
    let baseline = RegressionBaseline {
        as_of_phase: "checkpoint_1".to_owned(),
        total_tests: 5_319,
        passed: 5_319,
        failed: 0,
        ignored: 0,
        baseline_commit: "deadbeef".to_owned(),
    };
    let actual = RegressionCounts {
        total_tests: 5_319,
        passed: 5_317,
        failed: 2,
        ignored: 0,
    };

    let report = compare_against_baseline(&baseline, &actual);
    assert!(
        !report.pass,
        "bead_id={BEAD_ID} case=detect_failure_report_must_fail"
    );
    let reason = report.reason.unwrap_or_default();
    assert!(
        reason.contains("failed increased"),
        "bead_id={BEAD_ID} case=detect_failure_reason reason={reason}"
    );
}

#[test]
fn test_regression_guard_baseline_comparison() {
    let baseline = RegressionBaseline {
        as_of_phase: "checkpoint_1".to_owned(),
        total_tests: 5_319,
        passed: 5_319,
        failed: 0,
        ignored: 0,
        baseline_commit: "deadbeef".to_owned(),
    };
    let actual = RegressionCounts {
        total_tests: 5_322,
        passed: 5_322,
        failed: 0,
        ignored: 0,
    };

    let report = compare_against_baseline(&baseline, &actual);
    assert!(
        report.pass,
        "bead_id={BEAD_ID} case=baseline_compare_should_pass report={report:?}"
    );
    assert_eq!(
        report.delta.new_tests, 3,
        "bead_id={BEAD_ID} case=baseline_compare_new_tests"
    );
    assert_eq!(
        report.delta.delta_failed, 0,
        "bead_id={BEAD_ID} case=baseline_compare_failed_delta"
    );
}

#[test]
#[ignore = "Runs full cargo test --workspace and compares against regression baseline JSON"]
fn phase5_regression_guard_full_workspace_against_baseline() -> Result<(), String> {
    if std::env::var_os(CHILD_ENV).is_some() {
        return Ok(());
    }

    let root = repo_root();
    let baseline_file = baseline_path(&root);
    let baseline = load_regression_baseline(&baseline_file)
        .map_err(|error| format!("bead_id={BEAD_ID} case=load_baseline_failed error={error}"))?;

    eprintln!(
        "{LOG_PREFIX}[phase={}][step=run_tests] Starting full workspace test suite",
        baseline.as_of_phase
    );

    let output = Command::new("cargo")
        .arg("test")
        .arg("--workspace")
        .arg("--")
        .arg("--nocapture")
        .current_dir(&root)
        .env(CHILD_ENV, "1")
        .output()
        .map_err(|error| format!("bead_id={BEAD_ID} case=spawn_cargo_test_failed error={error}"))?;

    let combined_output = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let counts = parse_workspace_test_counts(&combined_output).map_err(|error| {
        format!("bead_id={BEAD_ID} case=parse_workspace_output_failed error={error}")
    })?;

    eprintln!(
        "{LOG_PREFIX}[phase={}][step=parse_results] total={} passed={} failed={} ignored={}",
        baseline.as_of_phase, counts.total_tests, counts.passed, counts.failed, counts.ignored
    );

    let report = compare_against_baseline(&baseline, &counts);
    eprintln!(
        "{LOG_PREFIX}[phase={}][step=compare_baseline] delta_passed={} delta_failed={} new_tests={}",
        baseline.as_of_phase,
        report.delta.delta_passed,
        report.delta.delta_failed,
        report.delta.new_tests
    );

    if report.pass {
        eprintln!(
            "{LOG_PREFIX}[phase={}][result=PASS] All baseline tests preserved against commit {}",
            baseline.as_of_phase, baseline.baseline_commit
        );
        return Ok(());
    }

    for failed in extract_failed_tests(&combined_output) {
        eprintln!(
            "{LOG_PREFIX}[phase={}][step=failures] test_name=\"{}\"",
            baseline.as_of_phase, failed
        );
    }

    let reason = report
        .reason
        .unwrap_or_else(|| "unknown regression detected".to_owned());
    Err(format!(
        "{LOG_PREFIX}[phase={}][result=FAIL] {reason}; baseline_commit={} cargo_status={}",
        baseline.as_of_phase, baseline.baseline_commit, output.status
    ))
}
