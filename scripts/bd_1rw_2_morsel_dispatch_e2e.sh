#!/usr/bin/env bash
# bd_1rw_2_morsel_dispatch_e2e.sh — deterministic verifier for bd-1rw.2
#
# Verifies morsel-dispatch deterministic behavior and captures an artifact with:
# - run_id / trace_id / scenario_id
# - per-worker-count measurements
# - replay command for deterministic reruns

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

BEAD_ID="bd-1rw.2"
SCENARIO_ID="${SCENARIO_ID:-VDBE-1}"
SEED="${SEED:-424242}"
TRACE_ID="${TRACE_ID:-$SEED}"
RUN_ID="${RUN_ID:-${BEAD_ID}-seed-${SEED}}"
MIN_SPEEDUP="${MIN_SPEEDUP:-2.00}"

ARTIFACT_DIR="$WORKSPACE_ROOT/target/test-results/bd-1rw.2"
ARTIFACT_PATH="${FSQLITE_MORSEL_E2E_ARTIFACT:-$ARTIFACT_DIR/morsel_dispatch_e2e_artifact_${RUN_ID}.json}"
mkdir -p "$(dirname "$ARTIFACT_PATH")"

export RUN_ID TRACE_ID SCENARIO_ID SEED
export FSQLITE_MORSEL_E2E_ARTIFACT="$ARTIFACT_PATH"

TEST_NAME="vectorized_dispatch::tests::morsel_dispatch_e2e_replay_emits_artifact"
TEST_OUTPUT=""
if ! $NO_RCH && command -v rch >/dev/null 2>&1; then
  if ! TEST_OUTPUT="$(
    rch exec -- env \
      RUN_ID="$RUN_ID" \
      TRACE_ID="$TRACE_ID" \
      SCENARIO_ID="$SCENARIO_ID" \
      SEED="$SEED" \
      FSQLITE_MORSEL_E2E_ARTIFACT="$ARTIFACT_PATH" \
      cargo test -p fsqlite-vdbe "$TEST_NAME" -- --exact --nocapture 2>&1
  )"; then
    printf '%s\n' "$TEST_OUTPUT" >&2
    exit 1
  fi
else
  if ! TEST_OUTPUT="$(
    env \
      RUN_ID="$RUN_ID" \
      TRACE_ID="$TRACE_ID" \
      SCENARIO_ID="$SCENARIO_ID" \
      SEED="$SEED" \
      FSQLITE_MORSEL_E2E_ARTIFACT="$ARTIFACT_PATH" \
      cargo test -p fsqlite-vdbe "$TEST_NAME" -- --exact --nocapture 2>&1
  )"; then
    printf '%s\n' "$TEST_OUTPUT" >&2
    exit 1
  fi
fi
printf '%s\n' "$TEST_OUTPUT"

INLINE_ARTIFACT_JSON="$(printf '%s\n' "$TEST_OUTPUT" | sed -n 's/^MORSEL_E2E_ARTIFACT_JSON://p' | tail -n 1)"
if [[ -n "$INLINE_ARTIFACT_JSON" ]]; then
  printf '%s\n' "$INLINE_ARTIFACT_JSON" > "$ARTIFACT_PATH"
fi

if [[ ! -f "$ARTIFACT_PATH" ]]; then
  echo "ERROR: expected artifact at '$ARTIFACT_PATH'" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq is required for artifact validation" >&2
  exit 1
fi

jq -e --arg bead "$BEAD_ID" '.bead_id == $bead' "$ARTIFACT_PATH" >/dev/null
jq -e --arg run_id "$RUN_ID" '.run_id == $run_id' "$ARTIFACT_PATH" >/dev/null
jq -e --arg scenario_id "$SCENARIO_ID" '.scenario_id == $scenario_id' "$ARTIFACT_PATH" >/dev/null
jq -e --argjson trace_id "$TRACE_ID" '.trace_id == $trace_id' "$ARTIFACT_PATH" >/dev/null
jq -e '.deterministic_checksum == true' "$ARTIFACT_PATH" >/dev/null
jq -e '.measurements | length == 3' "$ARTIFACT_PATH" >/dev/null

BASE_TPS="$(jq -r '.measurements[] | select(.worker_threads == 1) | .throughput_tasks_per_sec' "$ARTIFACT_PATH")"
FOUR_TPS="$(jq -r '.measurements[] | select(.worker_threads == 4) | .throughput_tasks_per_sec' "$ARTIFACT_PATH")"

if [[ -z "$BASE_TPS" || -z "$FOUR_TPS" ]]; then
  echo "ERROR: missing worker_threads=1 or worker_threads=4 measurements" >&2
  exit 1
fi

SPEEDUP="$(awk -v base="$BASE_TPS" -v four="$FOUR_TPS" 'BEGIN { if (base <= 0) { print "0.000"; } else { printf "%.3f", four / base; } }')"
if ! awk -v speedup="$SPEEDUP" -v floor="$MIN_SPEEDUP" 'BEGIN { exit !(speedup + 0 >= floor + 0) }'; then
  echo "ERROR: speedup ${SPEEDUP}x is below threshold ${MIN_SPEEDUP}x" >&2
  exit 1
fi

REPLAY_CMD="$(jq -r '.replay_command' "$ARTIFACT_PATH")"

if $JSON_OUTPUT; then
  cat <<ENDJSON
{
  "bead_id": "$BEAD_ID",
  "run_id": "$RUN_ID",
  "trace_id": $TRACE_ID,
  "scenario_id": "$SCENARIO_ID",
  "seed": $SEED,
  "artifact_path": "${ARTIFACT_PATH#$WORKSPACE_ROOT/}",
  "base_throughput_tasks_per_sec": $BASE_TPS,
  "four_worker_throughput_tasks_per_sec": $FOUR_TPS,
  "speedup_x": $SPEEDUP,
  "min_speedup_x": $MIN_SPEEDUP,
  "replay_command": "$REPLAY_CMD"
}
ENDJSON
else
  echo "=== Morsel Dispatch E2E Verification ==="
  echo "Bead ID:                    $BEAD_ID"
  echo "Run ID:                     $RUN_ID"
  echo "Trace ID:                   $TRACE_ID"
  echo "Scenario ID:                $SCENARIO_ID"
  echo "Seed:                       $SEED"
  echo "Artifact:                   ${ARTIFACT_PATH#$WORKSPACE_ROOT/}"
  echo "Throughput (1 worker):      $BASE_TPS tasks/sec"
  echo "Throughput (4 workers):     $FOUR_TPS tasks/sec"
  echo "Speedup (4/1):              ${SPEEDUP}x (threshold ${MIN_SPEEDUP}x)"
  echo "Replay command:"
  echo "  $REPLAY_CMD"
fi
