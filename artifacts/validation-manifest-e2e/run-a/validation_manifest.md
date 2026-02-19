# Validation Manifest

- bead_id: `bd-mblr.3.5.1`
- schema_version: `1.0.0`
- run_id: `bd-mblr.3.5.1-seed-424242`
- trace_id: `trace-dde11f1184376ebf`
- scenario_id: `QUALITY-351`
- commit_sha: `b289f41d5395f88c7bdac348582201cc8fb7e656`
- generated_unix_ms: `1700000000000`
- overall_outcome: `FAIL`
- overall_pass: `false`

## Gates
- `bd-mblr.3.1.1` [coverage] outcome=`PASS_WITH_WARNINGS` artifacts=artifacts/validation-manifest-e2e/shared/coverage_gate_report.json summary="Coverage gate PASS_WITH_WARNINGS: 41 tests, 124 invariants, global fill 61.5%, evidence 10000.0%, 0 blocking, 1 warnings"
- `bd-mblr.3.1.2` [invariant_drift] outcome=`PASS` artifacts=artifacts/validation-manifest-e2e/shared/invariant_drift_report.json summary="invariant drift: required_gap_count=0 critical_real=57/57"
- `bd-mblr.3.2.2` [scenario_drift] outcome=`FAIL` artifacts=artifacts/validation-manifest-e2e/shared/scenario_coverage_drift_report.json summary="scenario drift: required_gap_count=3 manifest_missing=3"
- `bd-mblr.3.4.1` [no_mock_critical_path] outcome=`PASS` artifacts=artifacts/validation-manifest-e2e/shared/no_mock_critical_path_report.json summary="No-mock critical-path gate: PASS — 57 critical invariants, 57 with real evidence, 0 exceptions, 0 missing (0 blocking, 0 warnings)"
- `bd-mblr.5.5.1` [logging_conformance] outcome=`PASS` artifacts=artifacts/validation-manifest-e2e/shared/logging_conformance_report.json,artifacts/validation-manifest-e2e/shared/validation_manifest_events.jsonl summary="logging conformance: profile_errors=0 schema_errors=0 warnings=0 shell_errors=0 shell_warnings=11"

## Replay
- command: `cargo run -p fsqlite-harness --bin validation_manifest_runner -- --root-seed 424242 --generated-unix-ms 1700000000000 --commit-sha 'b289f41d5395f88c7bdac348582201cc8fb7e656' --run-id 'bd-mblr.3.5.1-seed-424242' --trace-id 'trace-dde11f1184376ebf' --scenario-id 'QUALITY-351' --artifact-uri-prefix 'artifacts/validation-manifest-e2e/shared'`
