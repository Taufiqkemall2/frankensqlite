//! Backlog acceptance-completeness quality gate (bd-1dp9.9.6).
//!
//! Scans active beads and checks that acceptance criteria explicitly include:
//! - unit/property test requirements
//! - deterministic E2E scenario requirements
//! - structured logging/metrics requirements with traceability identifiers
//!
//! CI can run this gate in "regression" mode using a baseline. In that mode the
//! gate fails only when new critical-path completeness failures are introduced.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use fsqlite_error::{FrankenError, Result};

pub const BACKLOG_QUALITY_GATE_SCHEMA_VERSION: &str = "1.0.0";
const BEAD_ID: &str = "bd-1dp9.9.6";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequirementKind {
    UnitPropertyTests,
    DeterministicE2e,
    StructuredLogging,
}

impl RequirementKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnitPropertyTests => "unit_property_tests",
            Self::DeterministicE2e => "deterministic_e2e",
            Self::StructuredLogging => "structured_logging",
        }
    }

    #[must_use]
    pub const fn remediation(self) -> &'static str {
        match self {
            Self::UnitPropertyTests => {
                "Add explicit acceptance text for unit/property coverage (normal, boundary, adversarial) with deterministic seeds."
            }
            Self::DeterministicE2e => {
                "Add deterministic end-to-end scenario requirement with replay instructions and artifact capture."
            }
            Self::StructuredLogging => {
                "Add structured logging/metrics requirement including trace/run/scenario identifiers and actionable failure context."
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct BacklogQualityGateConfig {
    pub beads_path: PathBuf,
    pub baseline_path: Option<PathBuf>,
    pub critical_priority_max: i64,
    pub generated_unix_ms: Option<u128>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacklogQualityGateSummary {
    pub scanned_active_beads: usize,
    pub scanned_critical_beads: usize,
    pub total_failures: usize,
    pub critical_failures: usize,
    pub regression_failures: usize,
    pub missing_unit_property_count: usize,
    pub missing_deterministic_e2e_count: usize,
    pub missing_structured_logging_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacklogQualityFailure {
    pub issue_id: String,
    pub title: String,
    pub status: String,
    pub issue_type: String,
    pub priority: i64,
    pub is_critical: bool,
    pub missing_requirements: Vec<RequirementKind>,
    pub remediation: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacklogQualityGateReport {
    pub schema_version: String,
    pub bead_id: String,
    pub generated_unix_ms: u128,
    pub beads_path: String,
    pub baseline_path: Option<String>,
    pub critical_priority_max: i64,
    pub overall_pass: bool,
    pub summary: BacklogQualityGateSummary,
    pub failures: Vec<BacklogQualityFailure>,
    pub regression_failures: Vec<BacklogQualityFailure>,
}

impl BacklogQualityGateReport {
    #[must_use]
    pub fn render_markdown(&self) -> String {
        let mut out = String::new();
        out.push_str("# Backlog Quality Gate Report\n\n");
        out.push_str(&format!("- overall_pass: `{}`\n", self.overall_pass));
        out.push_str(&format!(
            "- scanned_active_beads: `{}`\n",
            self.summary.scanned_active_beads
        ));
        out.push_str(&format!(
            "- scanned_critical_beads: `{}`\n",
            self.summary.scanned_critical_beads
        ));
        out.push_str(&format!(
            "- total_failures: `{}`\n",
            self.summary.total_failures
        ));
        out.push_str(&format!(
            "- critical_failures: `{}`\n",
            self.summary.critical_failures
        ));
        out.push_str(&format!(
            "- regression_failures: `{}`\n",
            self.summary.regression_failures
        ));
        out.push_str(&format!(
            "- missing_unit_property_count: `{}`\n",
            self.summary.missing_unit_property_count
        ));
        out.push_str(&format!(
            "- missing_deterministic_e2e_count: `{}`\n",
            self.summary.missing_deterministic_e2e_count
        ));
        out.push_str(&format!(
            "- missing_structured_logging_count: `{}`\n",
            self.summary.missing_structured_logging_count
        ));
        out.push('\n');

        if self.regression_failures.is_empty() {
            out.push_str("## Regression Failures\n\nNone.\n");
        } else {
            out.push_str("## Regression Failures\n\n");
            for failure in &self.regression_failures {
                out.push_str(&format!(
                    "- `{}` (P{} {}) missing: {}\n",
                    failure.issue_id,
                    failure.priority,
                    if failure.is_critical {
                        "critical"
                    } else {
                        "non-critical"
                    },
                    join_requirements(&failure.missing_requirements)
                ));
            }
        }

        if !self.failures.is_empty() {
            out.push_str("\n## All Failures\n\n");
            for failure in &self.failures {
                out.push_str(&format!(
                    "- `{}` ({}) missing: {}\n",
                    failure.issue_id,
                    failure.title,
                    join_requirements(&failure.missing_requirements)
                ));
                for item in &failure.remediation {
                    out.push_str(&format!("  - remediation: {item}\n"));
                }
            }
        }

        out
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacklogQualityBaseline {
    pub schema_version: String,
    pub entries: Vec<BacklogQualityBaselineEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacklogQualityBaselineEntry {
    pub issue_id: String,
    pub missing_requirements: Vec<RequirementKind>,
}

#[derive(Debug, Deserialize)]
struct RawIssue {
    #[serde(default)]
    id: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    status: String,
    #[serde(default, rename = "issue_type")]
    issue_type: String,
    #[serde(default)]
    priority: Option<i64>,
    #[serde(default)]
    acceptance_criteria: String,
    #[serde(default)]
    labels: Vec<String>,
}

#[derive(Debug, Clone)]
struct IssueRecord {
    id: String,
    title: String,
    status: String,
    issue_type: String,
    priority: i64,
    acceptance_criteria: String,
    labels: Vec<String>,
}

#[must_use]
pub fn default_generated_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

/// Evaluate the backlog quality gate for acceptance-completeness regressions.
///
/// # Errors
///
/// Returns an error if the beads file or baseline cannot be read/parsed.
pub fn run_backlog_quality_gate(
    config: &BacklogQualityGateConfig,
) -> Result<BacklogQualityGateReport> {
    let generated_unix_ms = config
        .generated_unix_ms
        .unwrap_or_else(default_generated_unix_ms);
    let issues = load_issues(&config.beads_path)?;
    let baseline = load_baseline(config.baseline_path.as_deref())?;
    let allowed_failure_signatures = baseline_signature_set(baseline.as_ref());

    let mut failures = Vec::new();
    let mut scanned_active_beads = 0_usize;
    let mut scanned_critical_beads = 0_usize;
    let mut missing_unit_property_count = 0_usize;
    let mut missing_deterministic_e2e_count = 0_usize;
    let mut missing_structured_logging_count = 0_usize;

    for issue in issues {
        if !is_active_status(&issue.status) || !is_bead_kind(&issue.issue_type) {
            continue;
        }
        scanned_active_beads = scanned_active_beads.saturating_add(1);
        let is_critical = is_critical_issue(&issue, config.critical_priority_max);
        if is_critical {
            scanned_critical_beads = scanned_critical_beads.saturating_add(1);
        }

        let missing = missing_requirements(&issue.acceptance_criteria);
        if missing.is_empty() {
            continue;
        }

        for requirement in &missing {
            match requirement {
                RequirementKind::UnitPropertyTests => {
                    missing_unit_property_count = missing_unit_property_count.saturating_add(1);
                }
                RequirementKind::DeterministicE2e => {
                    missing_deterministic_e2e_count =
                        missing_deterministic_e2e_count.saturating_add(1);
                }
                RequirementKind::StructuredLogging => {
                    missing_structured_logging_count =
                        missing_structured_logging_count.saturating_add(1);
                }
            }
        }

        let remediation = missing
            .iter()
            .map(|item| item.remediation().to_owned())
            .collect::<Vec<_>>();
        failures.push(BacklogQualityFailure {
            issue_id: issue.id,
            title: issue.title,
            status: issue.status,
            issue_type: issue.issue_type,
            priority: issue.priority,
            is_critical,
            missing_requirements: missing,
            remediation,
        });
    }

    failures.sort_by(|left, right| left.issue_id.cmp(&right.issue_id));
    let regression_failures = failures
        .iter()
        .filter(|failure| failure.is_critical)
        .filter(|failure| !allowed_failure_signatures.contains(&failure_signature(failure)))
        .cloned()
        .collect::<Vec<_>>();
    let critical_failures = failures
        .iter()
        .filter(|failure| failure.is_critical)
        .count();
    let overall_pass = regression_failures.is_empty();

    Ok(BacklogQualityGateReport {
        schema_version: BACKLOG_QUALITY_GATE_SCHEMA_VERSION.to_owned(),
        bead_id: BEAD_ID.to_owned(),
        generated_unix_ms,
        beads_path: config.beads_path.display().to_string(),
        baseline_path: config
            .baseline_path
            .as_ref()
            .map(|path| path.display().to_string()),
        critical_priority_max: config.critical_priority_max,
        overall_pass,
        summary: BacklogQualityGateSummary {
            scanned_active_beads,
            scanned_critical_beads,
            total_failures: failures.len(),
            critical_failures,
            regression_failures: regression_failures.len(),
            missing_unit_property_count,
            missing_deterministic_e2e_count,
            missing_structured_logging_count,
        },
        failures,
        regression_failures,
    })
}

/// Write a deterministic JSON report.
///
/// # Errors
///
/// Returns an error if serialization or write fails.
pub fn write_report_json(path: &Path, report: &BacklogQualityGateReport) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(report)
        .map_err(|error| internal_error(format!("report_serialize_failed: {error}")))?;
    fs::write(path, json)?;
    Ok(())
}

fn load_issues(path: &Path) -> Result<Vec<IssueRecord>> {
    let raw = fs::read_to_string(path)?;
    let mut issues = Vec::new();
    for (line_idx, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed = serde_json::from_str::<RawIssue>(line).map_err(|error| {
            internal_error(format!(
                "issues_json_parse_failed path={} line={} error={error}",
                path.display(),
                line_idx + 1
            ))
        })?;
        if parsed.id.trim().is_empty() {
            continue;
        }
        issues.push(IssueRecord {
            id: parsed.id.trim().to_owned(),
            title: parsed.title.trim().to_owned(),
            status: parsed.status.trim().to_lowercase(),
            issue_type: parsed.issue_type.trim().to_lowercase(),
            priority: parsed.priority.unwrap_or(99),
            acceptance_criteria: parsed.acceptance_criteria.trim().to_owned(),
            labels: parsed
                .labels
                .into_iter()
                .map(|label| label.trim().to_lowercase())
                .filter(|label| !label.is_empty())
                .collect(),
        });
    }
    Ok(issues)
}

fn load_baseline(path: Option<&Path>) -> Result<Option<BacklogQualityBaseline>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let raw = fs::read_to_string(path)?;
    let mut baseline: BacklogQualityBaseline = serde_json::from_str(&raw).map_err(|error| {
        internal_error(format!(
            "baseline_json_parse_failed path={} error={error}",
            path.display()
        ))
    })?;
    for entry in &mut baseline.entries {
        entry.missing_requirements.sort();
        entry.missing_requirements.dedup();
    }
    baseline.entries.sort_by(|left, right| {
        left.issue_id
            .cmp(&right.issue_id)
            .then_with(|| left.missing_requirements.cmp(&right.missing_requirements))
    });
    Ok(Some(baseline))
}

fn baseline_signature_set(baseline: Option<&BacklogQualityBaseline>) -> BTreeSet<String> {
    let mut signatures = BTreeSet::new();
    let Some(baseline) = baseline else {
        return signatures;
    };
    for entry in &baseline.entries {
        let mut requirements = entry.missing_requirements.clone();
        requirements.sort();
        requirements.dedup();
        signatures.insert(signature_parts(&entry.issue_id, &requirements));
    }
    signatures
}

fn failure_signature(failure: &BacklogQualityFailure) -> String {
    signature_parts(&failure.issue_id, &failure.missing_requirements)
}

fn signature_parts(issue_id: &str, requirements: &[RequirementKind]) -> String {
    let requirement_suffix = requirements
        .iter()
        .map(|item| item.as_str())
        .collect::<Vec<_>>()
        .join("+");
    format!("{issue_id}|{requirement_suffix}")
}

fn is_active_status(status: &str) -> bool {
    matches!(status, "open" | "in_progress")
}

fn is_bead_kind(kind: &str) -> bool {
    matches!(kind, "task" | "epic")
}

fn is_critical_issue(issue: &IssueRecord, critical_priority_max: i64) -> bool {
    issue.priority <= critical_priority_max
        || issue
            .labels
            .iter()
            .any(|label| label == "ev-gated" || label == "critical-path")
}

fn missing_requirements(acceptance_criteria: &str) -> Vec<RequirementKind> {
    let mut missing = Vec::new();
    let normalized = normalize_text(acceptance_criteria);
    let words = token_words(&normalized);

    if !has_unit_property_tests(&normalized, &words) {
        missing.push(RequirementKind::UnitPropertyTests);
    }
    if !has_deterministic_e2e(&normalized, &words) {
        missing.push(RequirementKind::DeterministicE2e);
    }
    if !has_structured_logging(&normalized, &words) {
        missing.push(RequirementKind::StructuredLogging);
    }
    missing
}

fn has_unit_property_tests(normalized: &str, words: &BTreeSet<String>) -> bool {
    let mentions_test = words.contains("test") || words.contains("tests");
    let mentions_unit = words.contains("unit");
    let mentions_property = words.contains("property") || normalized.contains("proptest");
    mentions_test && (mentions_unit || mentions_property)
}

fn has_deterministic_e2e(normalized: &str, words: &BTreeSet<String>) -> bool {
    let mentions_e2e = words.contains("e2e")
        || normalized.contains("end-to-end")
        || normalized.contains("end to end");
    let mentions_determinism = words.contains("deterministic")
        || words.contains("seed")
        || words.contains("seeds")
        || words.contains("replay")
        || normalized.contains("replay instructions");
    mentions_e2e && mentions_determinism
}

fn has_structured_logging(normalized: &str, words: &BTreeSet<String>) -> bool {
    let mentions_logging = words.contains("logging")
        || words.contains("metrics")
        || normalized.contains("logging/metrics");
    let mentions_traceability = normalized.contains("trace_id")
        || normalized.contains("run_id")
        || normalized.contains("scenario_id")
        || normalized.contains("trace/run/scenario")
        || (words.contains("trace") && words.contains("scenario"));
    mentions_logging && mentions_traceability
}

fn normalize_text(input: &str) -> String {
    input.to_ascii_lowercase()
}

fn token_words(input: &str) -> BTreeSet<String> {
    let mut words = BTreeSet::new();
    let mut current = String::new();
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            current.push(ch);
        } else if !current.is_empty() {
            words.insert(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        words.insert(current);
    }
    words
}

fn join_requirements(requirements: &[RequirementKind]) -> String {
    requirements
        .iter()
        .map(|item| item.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn internal_error(message: String) -> FrankenError {
    FrankenError::Internal(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn write_issues(path: &Path, entries: &[serde_json::Value]) {
        let body = entries
            .iter()
            .map(|entry| serde_json::to_string(entry).expect("serialize entry"))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(path, format!("{body}\n")).expect("write issues");
    }

    fn base_issue(id: &str, status: &str, priority: i64, acceptance: &str) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "title": format!("Issue {id}"),
            "status": status,
            "issue_type": "task",
            "priority": priority,
            "acceptance_criteria": acceptance,
            "labels": ["ev-gated"]
        })
    }

    fn full_acceptance() -> &'static str {
        "Comprehensive unit/property tests are required. Deterministic e2e scenarios \
         with fixed seeds and replay instructions are mandatory. Structured logging/metrics \
         must include trace_id, run_id, scenario_id and actionable failure context."
    }

    #[test]
    fn detects_missing_requirements_for_active_issue() {
        let temp = tempdir().expect("tempdir");
        let beads_path = temp.path().join("issues.jsonl");
        write_issues(
            &beads_path,
            &[base_issue(
                "bd-a",
                "open",
                1,
                "Unit tests exist, but no deterministic e2e or trace_id logging requirements.",
            )],
        );

        let report = run_backlog_quality_gate(&BacklogQualityGateConfig {
            beads_path,
            baseline_path: None,
            critical_priority_max: 1,
            generated_unix_ms: Some(42),
        })
        .expect("gate should run");

        assert!(
            !report.overall_pass,
            "expected missing requirements to fail without baseline"
        );
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.regression_failures.len(), 1);
        assert_eq!(
            report.failures[0].missing_requirements,
            vec![RequirementKind::DeterministicE2e]
        );
    }

    #[test]
    fn baseline_allows_known_failures_but_blocks_new_regression() {
        let temp = tempdir().expect("tempdir");
        let beads_path = temp.path().join("issues.jsonl");
        let baseline_path = temp.path().join("baseline.json");

        write_issues(
            &beads_path,
            &[
                base_issue(
                    "bd-known",
                    "open",
                    1,
                    "Unit tests only. Missing deterministic e2e and structured logging trace_id details.",
                ),
                base_issue("bd-good", "open", 1, full_acceptance()),
            ],
        );

        let baseline = BacklogQualityBaseline {
            schema_version: BACKLOG_QUALITY_GATE_SCHEMA_VERSION.to_owned(),
            entries: vec![BacklogQualityBaselineEntry {
                issue_id: "bd-known".to_owned(),
                missing_requirements: vec![RequirementKind::DeterministicE2e],
            }],
        };
        fs::write(
            &baseline_path,
            serde_json::to_string_pretty(&baseline).expect("serialize baseline"),
        )
        .expect("write baseline");

        let report = run_backlog_quality_gate(&BacklogQualityGateConfig {
            beads_path: beads_path.clone(),
            baseline_path: Some(baseline_path.clone()),
            critical_priority_max: 1,
            generated_unix_ms: Some(42),
        })
        .expect("gate should run");
        assert!(
            report.overall_pass,
            "known baseline failure should not fail regression gate"
        );
        assert_eq!(report.regression_failures.len(), 0);

        write_issues(
            &beads_path,
            &[
                base_issue(
                    "bd-known",
                    "open",
                    1,
                    "Unit tests only. Missing deterministic e2e and structured logging trace_id details.",
                ),
                base_issue("bd-new", "open", 1, "Unit tests only."),
            ],
        );

        let report_with_regression = run_backlog_quality_gate(&BacklogQualityGateConfig {
            beads_path,
            baseline_path: Some(baseline_path),
            critical_priority_max: 1,
            generated_unix_ms: Some(42),
        })
        .expect("gate should run");
        assert!(
            !report_with_regression.overall_pass,
            "new critical-path regression should fail gate"
        );
        assert_eq!(report_with_regression.regression_failures.len(), 1);
        assert_eq!(
            report_with_regression.regression_failures[0].issue_id,
            "bd-new"
        );
    }

    #[test]
    fn text_matching_uses_word_boundaries() {
        let normalized = normalize_text(
            "Community testing and telemetry mention neither end-to-end nor trace_id logging.",
        );
        let words = token_words(&normalized);
        assert!(
            !has_unit_property_tests(&normalized, &words),
            "community should not satisfy unit test requirement"
        );
        assert!(
            !has_deterministic_e2e(&normalized, &words),
            "must require deterministic e2e wording"
        );
    }

    #[test]
    fn deterministic_output_with_fixed_timestamp() {
        let temp = tempdir().expect("tempdir");
        let beads_path = temp.path().join("issues.jsonl");
        write_issues(
            &beads_path,
            &[
                base_issue("bd-b", "open", 1, full_acceptance()),
                base_issue("bd-a", "open", 1, "Unit tests only."),
            ],
        );

        let report_a = run_backlog_quality_gate(&BacklogQualityGateConfig {
            beads_path: beads_path.clone(),
            baseline_path: None,
            critical_priority_max: 1,
            generated_unix_ms: Some(777),
        })
        .expect("gate should run");
        let report_b = run_backlog_quality_gate(&BacklogQualityGateConfig {
            beads_path,
            baseline_path: None,
            critical_priority_max: 1,
            generated_unix_ms: Some(777),
        })
        .expect("gate should run");

        let json_a = serde_json::to_string(&report_a).expect("serialize report");
        let json_b = serde_json::to_string(&report_b).expect("serialize report");
        assert_eq!(
            json_a, json_b,
            "fixed timestamp should yield deterministic JSON"
        );
    }
}
