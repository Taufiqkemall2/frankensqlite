use std::collections::BTreeMap;

use fsqlite_harness::toolchain_determinism::{
    DeterminismMatrix, DeterminismProbe, DeterminismProbeExecutor, DeterminismRunReport,
    DeterminismRunnerConfig, DivergenceClass, ProbeExecutionArtifact, ToolchainEntry,
    run_determinism_matrix_with_executor,
};

#[derive(Debug, Clone, Default)]
struct FixtureExecutor {
    artifacts: BTreeMap<(String, String), ProbeExecutionArtifact>,
    failures: BTreeMap<(String, String), String>,
}

impl FixtureExecutor {
    fn set_artifact(
        &mut self,
        toolchain_id: &str,
        probe_id: &str,
        artifact: ProbeExecutionArtifact,
    ) {
        self.artifacts
            .insert((toolchain_id.to_owned(), probe_id.to_owned()), artifact);
    }

    fn set_failure(&mut self, toolchain_id: &str, probe_id: &str, error: &str) {
        self.failures.insert(
            (toolchain_id.to_owned(), probe_id.to_owned()),
            error.to_owned(),
        );
    }
}

impl DeterminismProbeExecutor for FixtureExecutor {
    fn execute_probe(
        &self,
        toolchain: &ToolchainEntry,
        probe: &DeterminismProbe,
    ) -> Result<ProbeExecutionArtifact, String> {
        let key = (toolchain.id.clone(), probe.id.clone());
        if let Some(error) = self.failures.get(&key) {
            return Err(error.clone());
        }
        self.artifacts
            .get(&key)
            .cloned()
            .ok_or_else(|| format!("missing fixture artifact for {}:{}", toolchain.id, probe.id))
    }
}

fn find_cell<'a>(
    report: &'a DeterminismRunReport,
    probe_id: &str,
    toolchain_id: &str,
) -> Option<&'a fsqlite_harness::toolchain_determinism::DeterminismCellReport> {
    report
        .results
        .iter()
        .find(|result| result.probe_id == probe_id && result.toolchain_id == toolchain_id)
}

#[test]
fn test_runner_report_stability_for_same_inputs() {
    let matrix = DeterminismMatrix::canonical(42);
    let config = DeterminismRunnerConfig {
        selected_suites: vec!["DPROBE-001".to_owned(), "DPROBE-004".to_owned()],
        require_evidence: true,
    };

    let mut executor = FixtureExecutor::default();
    for toolchain in &matrix.toolchains {
        executor.set_artifact(
            &toolchain.id,
            "DPROBE-001",
            ProbeExecutionArtifact {
                canonical_output: "bit-exact-output".to_owned(),
                semantic_output: "bit-exact-output".to_owned(),
                duration_us: 100,
                evidence_paths: vec![format!("artifacts/{}/DPROBE-001.json", toolchain.id)],
            },
        );
        executor.set_artifact(
            &toolchain.id,
            "DPROBE-004",
            ProbeExecutionArtifact {
                canonical_output: "unordered rows: b,a".to_owned(),
                semantic_output: "a\nb".to_owned(),
                duration_us: 200,
                evidence_paths: vec![format!("artifacts/{}/DPROBE-004.json", toolchain.id)],
            },
        );
    }

    let report_a =
        run_determinism_matrix_with_executor(&matrix, &config, &executor).expect("first run");
    let report_b =
        run_determinism_matrix_with_executor(&matrix, &config, &executor).expect("second run");

    let json_a = serde_json::to_string_pretty(&report_a).expect("serialize report_a");
    let json_b = serde_json::to_string_pretty(&report_b).expect("serialize report_b");
    assert_eq!(json_a, json_b, "runner output should be deterministic");
    assert!(report_a.overall_pass, "all fixtures are equivalent");
}

#[test]
fn test_runner_detects_output_mismatch_and_links_evidence() {
    let matrix = DeterminismMatrix::canonical(17);
    let reference_id = matrix.reference_toolchain.clone();
    let divergent_toolchain = matrix
        .toolchains
        .iter()
        .find(|toolchain| toolchain.id != reference_id)
        .expect("need non-reference toolchain")
        .id
        .clone();

    let config = DeterminismRunnerConfig {
        selected_suites: vec!["DPROBE-001".to_owned()],
        require_evidence: true,
    };

    let mut executor = FixtureExecutor::default();
    for toolchain in &matrix.toolchains {
        let output = if toolchain.id == divergent_toolchain {
            "different-output"
        } else {
            "reference-output"
        };
        executor.set_artifact(
            &toolchain.id,
            "DPROBE-001",
            ProbeExecutionArtifact {
                canonical_output: output.to_owned(),
                semantic_output: output.to_owned(),
                duration_us: 150,
                evidence_paths: vec![format!("artifacts/{}/DPROBE-001.log", toolchain.id)],
            },
        );
    }

    let report =
        run_determinism_matrix_with_executor(&matrix, &config, &executor).expect("run report");
    assert!(
        !report.overall_pass,
        "one toolchain diverges from reference"
    );

    let divergent_cell =
        find_cell(&report, "DPROBE-001", &divergent_toolchain).expect("divergent cell present");
    assert_eq!(
        divergent_cell.divergence_class,
        DivergenceClass::OutputMismatch
    );
    assert!(
        !divergent_cell.evidence_paths.is_empty(),
        "evidence paths must be linked in report"
    );
    assert!(
        report.summary.failed_cells >= 1,
        "summary should count at least one failing cell"
    );
}

#[test]
fn test_runner_marks_missing_evidence_and_runner_error() {
    let matrix = DeterminismMatrix::canonical(99);
    let reference_id = matrix.reference_toolchain.clone();
    let non_reference_id = matrix
        .toolchains
        .iter()
        .find(|toolchain| toolchain.id != reference_id)
        .expect("need non-reference toolchain")
        .id
        .clone();

    let config = DeterminismRunnerConfig {
        selected_suites: vec!["DPROBE-004".to_owned()],
        require_evidence: true,
    };

    let mut executor = FixtureExecutor::default();
    for toolchain in &matrix.toolchains {
        executor.set_artifact(
            &toolchain.id,
            "DPROBE-004",
            ProbeExecutionArtifact {
                canonical_output: "a,b".to_owned(),
                semantic_output: "a\nb".to_owned(),
                duration_us: 120,
                evidence_paths: if toolchain.id == reference_id {
                    vec!["artifacts/reference/DPROBE-004.log".to_owned()]
                } else {
                    Vec::new()
                },
            },
        );
    }
    executor.set_failure(
        &non_reference_id,
        "DPROBE-004",
        "fixture-injected-runner-error",
    );

    let report =
        run_determinism_matrix_with_executor(&matrix, &config, &executor).expect("run report");

    let reference_cell = find_cell(&report, "DPROBE-004", &reference_id).expect("reference cell");
    assert_eq!(reference_cell.divergence_class, DivergenceClass::None);

    let failing_cell =
        find_cell(&report, "DPROBE-004", &non_reference_id).expect("non-reference cell");
    assert_eq!(failing_cell.divergence_class, DivergenceClass::RunnerError);
    assert!(
        failing_cell
            .divergence_notes
            .as_deref()
            .is_some_and(|notes| notes.contains("fixture-injected-runner-error"))
    );

    let has_missing_evidence = report
        .results
        .iter()
        .any(|result| result.divergence_class == DivergenceClass::MissingEvidence);
    assert!(
        has_missing_evidence,
        "at least one cell should fail for missing evidence"
    );
}
