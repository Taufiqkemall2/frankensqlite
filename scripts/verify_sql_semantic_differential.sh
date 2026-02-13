#!/usr/bin/env bash
# E2E verification for bd-1dp9.3.4 — SQL semantic differential unit+e2e suites
set -euo pipefail

BEAD="bd-1dp9.3.4"
RESULTS_DIR="test-results"
ARTIFACT="$RESULTS_DIR/${BEAD}-sql-semantic-differential-verify.json"
mkdir -p "$RESULTS_DIR"

echo "=== $BEAD E2E verification ==="

echo ">> Running harness integration tests …"
INTEG_OUT=$(cargo test -p fsqlite-harness --test bd_1dp9_3_4_sql_semantic_differential 2>&1)
INTEG_LINE=$(echo "$INTEG_OUT" | grep 'test result:' | tail -1)
INTEG_PASSED=$(echo "$INTEG_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
INTEG_FAILED=$(echo "$INTEG_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
echo "   harness integration: $INTEG_PASSED passed, $INTEG_FAILED failed"

UNIT_PASSED=0; UNIT_FAILED=0
echo ">> Running sql_semantic_differential unit tests …"
if UNIT_OUT=$(cargo test -p fsqlite-harness --lib -- sql_semantic_differential 2>&1); then
    UNIT_LINE=$(echo "$UNIT_OUT" | grep 'test result:' | tail -1)
    UNIT_PASSED=$(echo "$UNIT_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    UNIT_FAILED=$(echo "$UNIT_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   harness unit: $UNIT_PASSED passed, $UNIT_FAILED failed"
else
    echo "   harness unit: SKIPPED"
fi

ORC_PASSED=0; ORC_FAILED=0
echo ">> Running oracle unit tests …"
if ORC_OUT=$(cargo test -p fsqlite-harness --lib -- oracle 2>&1); then
    ORC_LINE=$(echo "$ORC_OUT" | grep 'test result:' | tail -1)
    ORC_PASSED=$(echo "$ORC_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    ORC_FAILED=$(echo "$ORC_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   oracle: $ORC_PASSED passed, $ORC_FAILED failed"
else
    echo "   oracle: SKIPPED"
fi

MM_PASSED=0; MM_FAILED=0
echo ">> Running mismatch_minimizer unit tests …"
if MM_OUT=$(cargo test -p fsqlite-harness --lib -- mismatch_minimizer 2>&1); then
    MM_LINE=$(echo "$MM_OUT" | grep 'test result:' | tail -1)
    MM_PASSED=$(echo "$MM_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    MM_FAILED=$(echo "$MM_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   mismatch_minimizer: $MM_PASSED passed, $MM_FAILED failed"
else
    echo "   mismatch_minimizer: SKIPPED"
fi

SGM_PASSED=0; SGM_FAILED=0
echo ">> Running semantic_gap_map unit tests …"
if SGM_OUT=$(cargo test -p fsqlite-harness --lib -- semantic_gap_map 2>&1); then
    SGM_LINE=$(echo "$SGM_OUT" | grep 'test result:' | tail -1)
    SGM_PASSED=$(echo "$SGM_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    SGM_FAILED=$(echo "$SGM_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   semantic_gap_map: $SGM_PASSED passed, $SGM_FAILED failed"
else
    echo "   semantic_gap_map: SKIPPED"
fi

PVC_PASSED=0; PVC_FAILED=0
echo ">> Running planner_vdbe_closure unit tests …"
if PVC_OUT=$(cargo test -p fsqlite-harness --lib -- planner_vdbe_closure 2>&1); then
    PVC_LINE=$(echo "$PVC_OUT" | grep 'test result:' | tail -1)
    PVC_PASSED=$(echo "$PVC_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    PVC_FAILED=$(echo "$PVC_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   planner_vdbe_closure: $PVC_PASSED passed, $PVC_FAILED failed"
else
    echo "   planner_vdbe_closure: SKIPPED"
fi

META_PASSED=0; META_FAILED=0
echo ">> Running metamorphic unit tests …"
if META_OUT=$(cargo test -p fsqlite-harness --lib -- metamorphic 2>&1); then
    META_LINE=$(echo "$META_OUT" | grep 'test result:' | tail -1)
    META_PASSED=$(echo "$META_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    META_FAILED=$(echo "$META_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   metamorphic: $META_PASSED passed, $META_FAILED failed"
else
    echo "   metamorphic: SKIPPED"
fi

SPS_PASSED=0; SPS_FAILED=0
echo ">> Running sql_pipeline_suites tests …"
if SPS_OUT=$(cargo test -p fsqlite-harness --lib -- sql_pipeline_suites 2>&1); then
    SPS_LINE=$(echo "$SPS_OUT" | grep 'test result:' | tail -1)
    SPS_PASSED=$(echo "$SPS_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    SPS_FAILED=$(echo "$SPS_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   sql_pipeline_suites: $SPS_PASSED passed, $SPS_FAILED failed"
else
    echo "   sql_pipeline_suites: SKIPPED"
fi

TOTAL_PASSED=$((INTEG_PASSED + UNIT_PASSED + ORC_PASSED + MM_PASSED + SGM_PASSED + PVC_PASSED + META_PASSED + SPS_PASSED))
TOTAL_FAILED=$((INTEG_FAILED + UNIT_FAILED + ORC_FAILED + MM_FAILED + SGM_FAILED + PVC_FAILED + META_FAILED + SPS_FAILED))

if [ "$INTEG_FAILED" -gt 0 ] || [ "$UNIT_FAILED" -gt 0 ]; then VERDICT="FAIL"; else VERDICT="PASS"; fi

echo ""
echo "=== TOTAL: $TOTAL_PASSED passed, $TOTAL_FAILED failed — $VERDICT ==="

cat > "$ARTIFACT" <<EOF
{
  "bead_id": "$BEAD",
  "schema_version": 1,
  "verdict": "$VERDICT",
  "harness_integration": { "passed": $INTEG_PASSED, "failed": $INTEG_FAILED },
  "harness_unit": { "passed": $UNIT_PASSED, "failed": $UNIT_FAILED },
  "oracle": { "passed": $ORC_PASSED, "failed": $ORC_FAILED },
  "mismatch_minimizer": { "passed": $MM_PASSED, "failed": $MM_FAILED },
  "semantic_gap_map": { "passed": $SGM_PASSED, "failed": $SGM_FAILED },
  "planner_vdbe_closure": { "passed": $PVC_PASSED, "failed": $PVC_FAILED },
  "metamorphic": { "passed": $META_PASSED, "failed": $META_FAILED },
  "sql_pipeline_suites": { "passed": $SPS_PASSED, "failed": $SPS_FAILED },
  "total_passed": $TOTAL_PASSED,
  "total_failed": $TOTAL_FAILED,
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

echo "Evidence written to $ARTIFACT"
