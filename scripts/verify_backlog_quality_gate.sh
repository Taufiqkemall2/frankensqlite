#!/usr/bin/env bash
# verify_backlog_quality_gate.sh — deterministic E2E verifier for bd-1dp9.9.6
#
# What this checks:
# 1. Synthetic backlog with known baseline issue produces deterministic pass output.
# 2. Introducing a new critical-path acceptance gap causes regression failure.
# 3. Optional repository-mode run validates current .beads backlog against baseline.
#
# Env overrides:
# - BACKLOG_QUALITY_GATE_RUNNER_BIN=target/debug/backlog_quality_gate_runner
# - BACKLOG_QUALITY_GATE_USE_RCH=1

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ROOT="$WORKSPACE_ROOT/artifacts/backlog-quality-gate-e2e"
REPO_GATE=false
JSON_OUTPUT=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-gate)
      REPO_GATE=true
      shift
      ;;
    --json)
      JSON_OUTPUT=true
      shift
      ;;
    *)
      echo "ERROR: unknown argument '$1'" >&2
      exit 2
      ;;
  esac
done

mkdir -p "$RUN_ROOT"

RUNNER_BIN="${BACKLOG_QUALITY_GATE_RUNNER_BIN:-}"
if [[ -n "${RUNNER_BIN}" ]]; then
  if [[ ! -x "${RUNNER_BIN}" ]]; then
    echo "ERROR: BACKLOG_QUALITY_GATE_RUNNER_BIN is not executable: ${RUNNER_BIN}" >&2
    exit 2
  fi
  RUNNER=("${RUNNER_BIN}")
elif [[ "${BACKLOG_QUALITY_GATE_USE_RCH:-1}" == "1" ]] && command -v rch >/dev/null 2>&1; then
  RUNNER=(rch exec -- cargo run -p fsqlite-harness --bin backlog_quality_gate_runner --)
else
  RUNNER=(cargo run -p fsqlite-harness --bin backlog_quality_gate_runner --)
fi

FIXTURE_DIR="$RUN_ROOT/fixture"
mkdir -p "$FIXTURE_DIR"

FIXTURE_BEADS_PASS="$FIXTURE_DIR/issues_pass.jsonl"
FIXTURE_BEADS_FAIL="$FIXTURE_DIR/issues_fail.jsonl"
FIXTURE_BASELINE="$FIXTURE_DIR/baseline.json"
REPORT_A="$FIXTURE_DIR/report_a.json"
REPORT_B="$FIXTURE_DIR/report_b.json"
SUMMARY_A="$FIXTURE_DIR/report_a.md"
SUMMARY_B="$FIXTURE_DIR/report_b.md"
REPORT_FAIL="$FIXTURE_DIR/report_fail.json"
SUMMARY_FAIL="$FIXTURE_DIR/report_fail.md"
FIXED_TS="1700000000000"

cat > "$FIXTURE_BEADS_PASS" <<'EOF'
{"id":"bd-fixture-known","title":"Known debt","status":"open","issue_type":"task","priority":1,"acceptance_criteria":"Unit tests only. Missing deterministic e2e seed/replay and trace_id logging.","labels":["ev-gated"]}
{"id":"bd-fixture-good","title":"Compliant","status":"open","issue_type":"task","priority":1,"acceptance_criteria":"Comprehensive unit/property tests are required. Deterministic e2e scenarios with fixed seeds and replay instructions are mandatory. Structured logging/metrics must include trace_id, run_id, scenario_id and actionable failure context.","labels":["ev-gated"]}
EOF

cat > "$FIXTURE_BEADS_FAIL" <<'EOF'
{"id":"bd-fixture-known","title":"Known debt","status":"open","issue_type":"task","priority":1,"acceptance_criteria":"Unit tests only. Missing deterministic e2e seed/replay and trace_id logging.","labels":["ev-gated"]}
{"id":"bd-fixture-new","title":"New regression","status":"open","issue_type":"task","priority":1,"acceptance_criteria":"Unit tests only.","labels":["ev-gated"]}
EOF

cat > "$FIXTURE_BASELINE" <<'EOF'
{
  "schema_version": "1.0.0",
  "entries": [
    {
      "issue_id": "bd-fixture-known",
      "missing_requirements": [
        "deterministic_e2e",
        "structured_logging"
      ]
    }
  ]
}
EOF

"${RUNNER[@]}" \
  --beads "$FIXTURE_BEADS_PASS" \
  --baseline "$FIXTURE_BASELINE" \
  --output-json "$REPORT_A" \
  --output-human "$SUMMARY_A" \
  --critical-priority-max 1 \
  --generated-unix-ms "$FIXED_TS" \
  1>&2

"${RUNNER[@]}" \
  --beads "$FIXTURE_BEADS_PASS" \
  --baseline "$FIXTURE_BASELINE" \
  --output-json "$REPORT_B" \
  --output-human "$SUMMARY_B" \
  --critical-priority-max 1 \
  --generated-unix-ms "$FIXED_TS" \
  1>&2

if ! diff -u "$REPORT_A" "$REPORT_B" >/dev/null; then
  echo "ERROR: deterministic replay failed for pass fixture reports" >&2
  diff -u "$REPORT_A" "$REPORT_B" >&2 || true
  exit 1
fi

set +e
"${RUNNER[@]}" \
  --beads "$FIXTURE_BEADS_FAIL" \
  --baseline "$FIXTURE_BASELINE" \
  --output-json "$REPORT_FAIL" \
  --output-human "$SUMMARY_FAIL" \
  --critical-priority-max 1 \
  --generated-unix-ms "$FIXED_TS" \
  1>&2
FAIL_EXIT=$?
set -e

if [[ $FAIL_EXIT -eq 0 ]]; then
  echo "ERROR: expected regression fixture to fail gate" >&2
  exit 1
fi

jq -e '.overall_pass == false' "$REPORT_FAIL" >/dev/null
jq -e '.summary.regression_failures == 1' "$REPORT_FAIL" >/dev/null
jq -e '.regression_failures[] | select(.issue_id == "bd-fixture-new")' "$REPORT_FAIL" >/dev/null

REPO_STATUS=0
REPO_REPORT="$RUN_ROOT/repo/report.json"
REPO_SUMMARY="$RUN_ROOT/repo/report.md"

if $REPO_GATE; then
  mkdir -p "$RUN_ROOT/repo"
  set +e
  "${RUNNER[@]}" \
    --beads "$WORKSPACE_ROOT/.beads/issues.jsonl" \
    --baseline "$WORKSPACE_ROOT/conformance/backlog_quality_gate_baseline.json" \
    --output-json "$REPO_REPORT" \
    --output-human "$REPO_SUMMARY" \
    --critical-priority-max 1 \
    1>&2
  REPO_STATUS=$?
  set -e
fi

if $JSON_OUTPUT; then
  cat <<ENDJSON
{
  "bead_id": "bd-1dp9.9.6",
  "deterministic_fixture_pass": true,
  "fixture_regression_detected": true,
  "fixture_pass_report": "${REPORT_A#$WORKSPACE_ROOT/}",
  "fixture_regression_report": "${REPORT_FAIL#$WORKSPACE_ROOT/}",
  "repo_gate_enabled": $REPO_GATE,
  "repo_gate_exit_code": $REPO_STATUS,
  "repo_report": "${REPO_REPORT#$WORKSPACE_ROOT/}"
}
ENDJSON
else
  echo "=== Backlog Quality Gate E2E ==="
  echo "Bead ID:                     bd-1dp9.9.6"
  echo "Deterministic pass fixture:  PASS"
  echo "Regression fixture:          PASS (failure correctly detected)"
  echo "Fixture pass report:         ${REPORT_A#$WORKSPACE_ROOT/}"
  echo "Fixture fail report:         ${REPORT_FAIL#$WORKSPACE_ROOT/}"
  if $REPO_GATE; then
    echo "Repo gate enabled:           yes"
    echo "Repo gate exit code:         $REPO_STATUS"
    echo "Repo report:                 ${REPO_REPORT#$WORKSPACE_ROOT/}"
  else
    echo "Repo gate enabled:           no"
  fi
fi

if $REPO_GATE && [[ $REPO_STATUS -ne 0 ]]; then
  exit "$REPO_STATUS"
fi
