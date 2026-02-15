#!/usr/bin/env bash
# verify_validation_manifest.sh — deterministic E2E check for bd-mblr.3.5.1
#
# Validates machine-readable manifest generation from real harness gate outputs:
# 1. Generates manifest artifacts twice with fixed deterministic inputs
# 2. Verifies required schema/field contract with jq
# 3. Confirms byte-for-byte deterministic reproducibility
# 4. Emits replay command and artifact paths for operator handoff
#
# Usage:
#   ./scripts/verify_validation_manifest.sh [--json] [--seed <u64>] [--generated-unix-ms <u128>]

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JSON_OUTPUT=false
ROOT_SEED="${VALIDATION_MANIFEST_SEED:-424242}"
GENERATED_UNIX_MS="${VALIDATION_MANIFEST_GENERATED_UNIX_MS:-1700000000000}"
SCENARIO_ID="QUALITY-VALIDATION-MANIFEST"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --json)
            JSON_OUTPUT=true
            shift
            ;;
        --seed)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --seed requires a value" >&2
                exit 2
            fi
            ROOT_SEED="$2"
            shift 2
            ;;
        --generated-unix-ms)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --generated-unix-ms requires a value" >&2
                exit 2
            fi
            GENERATED_UNIX_MS="$2"
            shift 2
            ;;
        *)
            echo "ERROR: unknown argument '$1'" >&2
            exit 2
            ;;
    esac
done

RUN_ROOT="$WORKSPACE_ROOT/artifacts/validation-manifest-e2e"
RUN_A="$RUN_ROOT/run-a"
RUN_B="$RUN_ROOT/run-b"
MANIFEST_A="$RUN_A/validation_manifest.json"
MANIFEST_B="$RUN_B/validation_manifest.json"
SUMMARY_A="$RUN_A/validation_manifest.md"
SUMMARY_B="$RUN_B/validation_manifest.md"

mkdir -p "$RUN_A" "$RUN_B"

COMMIT_SHA="$(git -C "$WORKSPACE_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
RUN_ID="bd-mblr.3.5.1-seed-${ROOT_SEED}"
TRACE_ID="trace-$(printf '%s' "$RUN_ID" | sha256sum | awk '{print $1}' | cut -c1-16)"

if command -v rch >/dev/null 2>&1; then
    RUNNER=(rch exec -- cargo run -p fsqlite-harness --bin validation_manifest_runner --)
else
    RUNNER=(cargo run -p fsqlite-harness --bin validation_manifest_runner --)
fi

COMMON_ARGS=(
    --workspace-root "$WORKSPACE_ROOT"
    --commit-sha "$COMMIT_SHA"
    --run-id "$RUN_ID"
    --trace-id "$TRACE_ID"
    --scenario-id "$SCENARIO_ID"
    --root-seed "$ROOT_SEED"
    --generated-unix-ms "$GENERATED_UNIX_MS"
)

"${RUNNER[@]}" \
    "${COMMON_ARGS[@]}" \
    --output-dir "$RUN_A" \
    --output-json "$MANIFEST_A" \
    --output-human "$SUMMARY_A" \
    --artifact-uri-prefix "artifacts/validation-manifest-e2e/run-a"

"${RUNNER[@]}" \
    "${COMMON_ARGS[@]}" \
    --output-dir "$RUN_B" \
    --output-json "$MANIFEST_B" \
    --output-human "$SUMMARY_B" \
    --artifact-uri-prefix "artifacts/validation-manifest-e2e/run-b"

if [[ ! -f "$MANIFEST_A" || ! -f "$MANIFEST_B" ]]; then
    echo "ERROR: manifest output missing" >&2
    exit 1
fi

jq -e '
    .schema_version == "1.0.0" and
    .bead_id == "bd-mblr.3.5.1" and
    (.commit_sha | length) > 0 and
    (.run_id | length) > 0 and
    (.trace_id | length) > 0 and
    (.scenario_id == "QUALITY-VALIDATION-MANIFEST") and
    (.gates | length) >= 5 and
    (.artifact_uris | length) >= 6 and
    (.replay.command | length) > 0 and
    (.logging_conformance.log_validation.passed == true)
' "$MANIFEST_A" >/dev/null

if ! diff -u "$MANIFEST_A" "$MANIFEST_B" >/dev/null; then
    echo "ERROR: deterministic replay check failed; manifests differ" >&2
    diff -u "$MANIFEST_A" "$MANIFEST_B" >&2 || true
    exit 1
fi

REPLAY_COMMAND="$(jq -r '.replay.command' "$MANIFEST_A")"
GATE_COUNT="$(jq -r '.gates | length' "$MANIFEST_A")"
ARTIFACT_COUNT="$(jq -r '.artifact_uris | length' "$MANIFEST_A")"
OVERALL_OUTCOME="$(jq -r '.overall_outcome' "$MANIFEST_A")"

if $JSON_OUTPUT; then
    cat <<ENDJSON
{
  "bead_id": "bd-mblr.3.5.1",
  "run_id": "$RUN_ID",
  "trace_id": "$TRACE_ID",
  "scenario_id": "$SCENARIO_ID",
  "commit_sha": "$COMMIT_SHA",
  "root_seed": "$ROOT_SEED",
  "generated_unix_ms": "$GENERATED_UNIX_MS",
  "deterministic_match": true,
  "manifest_a": "${MANIFEST_A#$WORKSPACE_ROOT/}",
  "manifest_b": "${MANIFEST_B#$WORKSPACE_ROOT/}",
  "summary_a": "${SUMMARY_A#$WORKSPACE_ROOT/}",
  "summary_b": "${SUMMARY_B#$WORKSPACE_ROOT/}",
  "gate_count": $GATE_COUNT,
  "artifact_count": $ARTIFACT_COUNT,
  "overall_outcome": "$OVERALL_OUTCOME",
  "replay_command": "$REPLAY_COMMAND"
}
ENDJSON
else
    echo "=== Validation Manifest E2E Check ==="
    echo "Bead ID:            bd-mblr.3.5.1"
    echo "Run ID:             $RUN_ID"
    echo "Trace ID:           $TRACE_ID"
    echo "Scenario ID:        $SCENARIO_ID"
    echo "Commit SHA:         $COMMIT_SHA"
    echo "Root seed:          $ROOT_SEED"
    echo "Generated unix ms:  $GENERATED_UNIX_MS"
    echo "Gate count:         $GATE_COUNT"
    echo "Artifact count:     $ARTIFACT_COUNT"
    echo "Overall outcome:    $OVERALL_OUTCOME"
    echo "Manifest A:         ${MANIFEST_A#$WORKSPACE_ROOT/}"
    echo "Manifest B:         ${MANIFEST_B#$WORKSPACE_ROOT/}"
    echo "Deterministic:      PASS"
    echo "Replay command:"
    echo "  $REPLAY_COMMAND"
fi
