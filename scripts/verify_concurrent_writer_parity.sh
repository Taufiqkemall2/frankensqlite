#!/usr/bin/env bash
# E2E verification for bd-1dp9.4.5 — Concurrent-writer-default invariants
set -euo pipefail

BEAD="bd-1dp9.4.5"
RESULTS_DIR="test-results"
ARTIFACT="$RESULTS_DIR/${BEAD}-concurrent-writer-parity-verify.json"
mkdir -p "$RESULTS_DIR"

echo "=== $BEAD E2E verification ==="

echo ">> Running harness integration tests …"
INTEG_OUT=$(cargo test -p fsqlite-harness --test bd_1dp9_4_5_concurrent_writer_parity 2>&1)
INTEG_LINE=$(echo "$INTEG_OUT" | grep 'test result:' | tail -1)
INTEG_PASSED=$(echo "$INTEG_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
INTEG_FAILED=$(echo "$INTEG_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
echo "   harness integration: $INTEG_PASSED passed, $INTEG_FAILED failed"

UNIT_PASSED=0; UNIT_FAILED=0
echo ">> Running concurrent_writer_parity unit tests …"
if UNIT_OUT=$(cargo test -p fsqlite-harness --lib -- concurrent_writer_parity 2>&1); then
    UNIT_LINE=$(echo "$UNIT_OUT" | grep 'test result:' | tail -1)
    UNIT_PASSED=$(echo "$UNIT_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    UNIT_FAILED=$(echo "$UNIT_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   harness unit: $UNIT_PASSED passed, $UNIT_FAILED failed"
else
    echo "   harness unit: SKIPPED"
fi

MVCC_PASSED=0; MVCC_FAILED=0
echo ">> Running MVCC concurrent writer tests …"
if MVCC_OUT=$(cargo test -p fsqlite-mvcc -- concurrent begin_concurrent 2>&1); then
    MVCC_LINE=$(echo "$MVCC_OUT" | grep 'test result:' | head -1)
    MVCC_PASSED=$(echo "$MVCC_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    MVCC_FAILED=$(echo "$MVCC_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   mvcc concurrent: $MVCC_PASSED passed, $MVCC_FAILED failed"
else
    echo "   mvcc concurrent: SKIPPED"
fi

STRESS_PASSED=0; STRESS_FAILED=0
echo ">> Running concurrent writer stress tests …"
if STRESS_OUT=$(cargo test -p fsqlite-harness --test bd_2npr_mvcc_concurrent_writer_stress 2>&1); then
    STRESS_LINE=$(echo "$STRESS_OUT" | grep 'test result:' | tail -1)
    STRESS_PASSED=$(echo "$STRESS_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    STRESS_FAILED=$(echo "$STRESS_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   stress: $STRESS_PASSED passed, $STRESS_FAILED failed"
else
    echo "   stress: SKIPPED"
fi

SSI_PASSED=0; SSI_FAILED=0
echo ">> Running E2E SSI/concurrent tests …"
if SSI_OUT=$(cargo test -p fsqlite-e2e -- concurrent ssi 2>&1); then
    SSI_LINE=$(echo "$SSI_OUT" | grep 'test result:' | head -1)
    SSI_PASSED=$(echo "$SSI_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    SSI_FAILED=$(echo "$SSI_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   e2e ssi/concurrent: $SSI_PASSED passed, $SSI_FAILED failed"
else
    echo "   e2e ssi/concurrent: SKIPPED"
fi

TOTAL_PASSED=$((INTEG_PASSED + UNIT_PASSED + MVCC_PASSED + STRESS_PASSED + SSI_PASSED))
TOTAL_FAILED=$((INTEG_FAILED + UNIT_FAILED + MVCC_FAILED + STRESS_FAILED + SSI_FAILED))

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
  "mvcc_concurrent": { "passed": $MVCC_PASSED, "failed": $MVCC_FAILED },
  "stress": { "passed": $STRESS_PASSED, "failed": $STRESS_FAILED },
  "e2e_ssi_concurrent": { "passed": $SSI_PASSED, "failed": $SSI_FAILED },
  "total_passed": $TOTAL_PASSED,
  "total_failed": $TOTAL_FAILED,
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

echo "Evidence written to $ARTIFACT"
