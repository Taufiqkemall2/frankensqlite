#!/usr/bin/env bash
# bd_1fbkm_trigger_old_new_e2e.sh — deterministic trigger OLD/NEW replay verifier

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

JSON_OUTPUT=false
NO_RCH=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --json)
      JSON_OUTPUT=true
      shift
      ;;
    --no-rch)
      NO_RCH=true
      shift
      ;;
    *)
      echo "ERROR: unknown argument '$1'" >&2
      exit 2
      ;;
  esac
done

if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq is required" >&2
  exit 1
fi

BEAD_ID="bd-1fbkm"
SCENARIO_ID="${SCENARIO_ID:-TRIGGER-OLD-NEW-1}"
SEED="${SEED:-20260219}"
TRACE_ID="${TRACE_ID:-$SEED}"
RUN_ID="${RUN_ID:-${BEAD_ID}-seed-${SEED}}"

ARTIFACT_DIR="$WORKSPACE_ROOT/artifacts/$BEAD_ID"
ARTIFACT_PATH="${FSQLITE_TRIGGER_E2E_ARTIFACT:-$ARTIFACT_DIR/trigger_old_new_e2e_artifact.json}"
JSONL_PATH="$ARTIFACT_DIR/trigger_old_new_assertions.jsonl"
SUITE_LOG_PATH="$ARTIFACT_DIR/trigger_suite.log"
mkdir -p "$ARTIFACT_DIR"
: > "$JSONL_PATH"

if ! $NO_RCH && command -v rch >/dev/null 2>&1; then
  RUNNER=(rch exec --)
else
  RUNNER=()
fi

# case_id|title|test_name|assertions
CASES=(
  "1|Basic INSERT trigger with NEW access|test_insert_trigger_row_images_via_execute_statement|3"
  "2|Basic DELETE trigger with OLD access|test_delete_trigger_row_images_via_execute_statement|3"
  "3|Basic UPDATE trigger with OLD+NEW access|test_update_trigger_row_images_via_execute_statement|3"
  "4|WHEN clause filtering|test_update_trigger_when_clause_uses_old_new_during_execute_statement|3"
  "5|Multi-column trigger correctness|test_trigger_body_binds_old_new_columns_from_frame|3"
  "6|Nested trigger (2 levels)|test_nested_triggers_two_levels_bind_new_values|2"
  "7|UPDATE OF column filtering|test_update_of_trigger_skips_when_listed_column_unchanged|2"
  "8|RAISE(ABORT) behavior|test_trigger_raise_abort_returns_function_error|3"
  "9|RAISE(ROLLBACK) behavior|test_trigger_raise_rollback_aborts_transaction|3"
  "10|RAISE(IGNORE) behavior|test_trigger_raise_ignore_skips_before_insert_dml|3"
  "11|Large table (100 columns) trigger performance|test_trigger_large_table_column_binding_100_columns|4"
  "12|Trigger behavior under BEGIN CONCURRENT|test_trigger_begin_concurrent_mode_with_old_new_bindings|3"
)

TOTAL_ASSERTIONS_EXPECTED=35
passed_assertions=0
failed_assertions=0
failed_cases=0

suite_start_ms="$(date +%s%3N)"
if "${RUNNER[@]}" cargo test -p fsqlite-core trigger -- --nocapture >"$SUITE_LOG_PATH" 2>&1; then
  suite_status="pass"
else
  suite_status="fail"
fi
suite_end_ms="$(date +%s%3N)"
suite_elapsed_ms="$((suite_end_ms - suite_start_ms))"

for case_entry in "${CASES[@]}"; do
  IFS='|' read -r case_id case_title case_test_name case_assertions <<<"$case_entry"

  case_status="fail"
  if [[ "$suite_status" == "pass" ]] && rg -q "test .*${case_test_name} .* ok" "$SUITE_LOG_PATH"; then
    case_status="pass"
  fi

  if [[ "$case_status" == "pass" ]]; then
    passed_assertions="$((passed_assertions + case_assertions))"
  else
    failed_assertions="$((failed_assertions + case_assertions))"
    failed_cases="$((failed_cases + 1))"
  fi

  jq -cn \
    --argjson case "$case_id" \
    --arg assertion "$case_title" \
    --arg status "${case_status^^}" \
    --arg expected "pass" \
    --arg actual "$case_status" \
    --argjson elapsed_ms "$suite_elapsed_ms" \
    '{case:$case, assertion:$assertion, status:$status, expected:$expected, actual:$actual, elapsed_ms:$elapsed_ms}' \
    >> "$JSONL_PATH"
done

overall_status="pass"
if [[ "$suite_status" != "pass" || "$failed_cases" -gt 0 || "$passed_assertions" -ne "$TOTAL_ASSERTIONS_EXPECTED" ]]; then
  overall_status="fail"
fi

cases_json="$(jq -s '.' "$JSONL_PATH")"
REPLAY_CMD="scripts/bd_1fbkm_trigger_old_new_e2e.sh --json"

jq -n \
  --arg bead_id "$BEAD_ID" \
  --arg run_id "$RUN_ID" \
  --argjson trace_id "$TRACE_ID" \
  --arg scenario_id "$SCENARIO_ID" \
  --argjson seed "$SEED" \
  --arg overall_status "$overall_status" \
  --arg suite_status "$suite_status" \
  --arg suite_log_path "${SUITE_LOG_PATH#$WORKSPACE_ROOT/}" \
  --arg assertions_jsonl "${JSONL_PATH#$WORKSPACE_ROOT/}" \
  --arg replay_command "$REPLAY_CMD" \
  --argjson total_assertions "$TOTAL_ASSERTIONS_EXPECTED" \
  --argjson passed_assertions "$passed_assertions" \
  --argjson failed_assertions "$failed_assertions" \
  --argjson failed_cases "$failed_cases" \
  --argjson suite_elapsed_ms "$suite_elapsed_ms" \
  --argjson cases "$cases_json" \
  '{
    bead_id: $bead_id,
    run_id: $run_id,
    trace_id: $trace_id,
    scenario_id: $scenario_id,
    seed: $seed,
    overall_status: $overall_status,
    suite_status: $suite_status,
    suite_elapsed_ms: $suite_elapsed_ms,
    total_assertions: $total_assertions,
    passed_assertions: $passed_assertions,
    failed_assertions: $failed_assertions,
    failed_cases: $failed_cases,
    suite_log_path: $suite_log_path,
    assertions_jsonl: $assertions_jsonl,
    cases: $cases,
    replay_command: $replay_command
  }' > "$ARTIFACT_PATH"

if $JSON_OUTPUT; then
  jq -n \
    --arg bead_id "$BEAD_ID" \
    --arg run_id "$RUN_ID" \
    --argjson trace_id "$TRACE_ID" \
    --arg scenario_id "$SCENARIO_ID" \
    --argjson seed "$SEED" \
    --arg overall_status "$overall_status" \
    --arg suite_status "$suite_status" \
    --arg artifact_path "${ARTIFACT_PATH#$WORKSPACE_ROOT/}" \
    --arg suite_log_path "${SUITE_LOG_PATH#$WORKSPACE_ROOT/}" \
    --arg assertions_jsonl "${JSONL_PATH#$WORKSPACE_ROOT/}" \
    --arg replay_command "$REPLAY_CMD" \
    --argjson total_assertions "$TOTAL_ASSERTIONS_EXPECTED" \
    --argjson passed_assertions "$passed_assertions" \
    --argjson failed_assertions "$failed_assertions" \
    --argjson failed_cases "$failed_cases" \
    '{
      bead_id: $bead_id,
      run_id: $run_id,
      trace_id: $trace_id,
      scenario_id: $scenario_id,
      seed: $seed,
      overall_status: $overall_status,
      suite_status: $suite_status,
      total_assertions: $total_assertions,
      passed_assertions: $passed_assertions,
      failed_assertions: $failed_assertions,
      failed_cases: $failed_cases,
      artifact_path: $artifact_path,
      suite_log_path: $suite_log_path,
      assertions_jsonl: $assertions_jsonl,
      replay_command: $replay_command
    }'
else
  echo "=== bd-1fbkm OLD/NEW Trigger E2E ==="
  echo "Run ID:      $RUN_ID"
  echo "Trace ID:    $TRACE_ID"
  echo "Scenario ID: $SCENARIO_ID"
  echo "Seed:        $SEED"
  echo "Suite:       $suite_status (${suite_elapsed_ms}ms)"
  echo "Assertions:  $passed_assertions/$TOTAL_ASSERTIONS_EXPECTED"
  echo "Status:      $overall_status"
  echo "Artifact:    ${ARTIFACT_PATH#$WORKSPACE_ROOT/}"
  echo "Replay:      $REPLAY_CMD"
fi

if [[ "$overall_status" != "pass" ]]; then
  exit 1
fi
