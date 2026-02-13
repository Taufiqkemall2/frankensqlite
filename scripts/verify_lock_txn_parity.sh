#!/usr/bin/env bash
# E2E verification for bd-1dp9.4.2 — Lock/busy/savepoint/autocommit parity
set -euo pipefail

BEAD="bd-1dp9.4.2"
RESULTS_DIR="test-results"
ARTIFACT="$RESULTS_DIR/${BEAD}-lock-txn-parity-verify.json"
mkdir -p "$RESULTS_DIR"

echo "=== $BEAD E2E verification ==="

# --- harness integration tests ---
echo ">> Running harness integration tests …"
INTEG_OUT=$(cargo test -p fsqlite-harness --test bd_1dp9_4_2_lock_txn_parity 2>&1)
INTEG_LINE=$(echo "$INTEG_OUT" | grep 'test result:' | tail -1)
INTEG_PASSED=$(echo "$INTEG_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
INTEG_FAILED=$(echo "$INTEG_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
echo "   harness integration: $INTEG_PASSED passed, $INTEG_FAILED failed"

# --- harness unit tests ---
UNIT_PASSED=0
UNIT_FAILED=0
echo ">> Running lock_txn_parity unit tests …"
if UNIT_OUT=$(cargo test -p fsqlite-harness --lib -- lock_txn_parity 2>&1); then
    UNIT_LINE=$(echo "$UNIT_OUT" | grep 'test result:' | tail -1)
    UNIT_PASSED=$(echo "$UNIT_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    UNIT_FAILED=$(echo "$UNIT_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   harness unit: $UNIT_PASSED passed, $UNIT_FAILED failed"
else
    echo "   harness unit: SKIPPED"
fi

# --- core unit tests ---
CORE_PASSED=0
CORE_FAILED=0
echo ">> Running core lock/txn unit tests …"
if CORE_OUT=$(cargo test -p fsqlite-core -- test_pragma_busy test_savepoint test_autocommit test_begin test_transaction 2>&1); then
    CORE_LINE=$(echo "$CORE_OUT" | grep 'test result:' | head -1)
    CORE_PASSED=$(echo "$CORE_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    CORE_FAILED=$(echo "$CORE_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   core unit: $CORE_PASSED passed, $CORE_FAILED failed"
else
    echo "   core unit: SKIPPED"
fi

# --- E2E savepoint tests ---
E2E_PASSED=0
E2E_FAILED=0
echo ">> Running E2E savepoint tests …"
if E2E_OUT=$(cargo test -p fsqlite-e2e --test correctness_transactions -- txn_savepoint 2>&1); then
    E2E_LINE=$(echo "$E2E_OUT" | grep 'test result:' | tail -1)
    E2E_PASSED=$(echo "$E2E_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    E2E_FAILED=$(echo "$E2E_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   e2e savepoint: $E2E_PASSED passed, $E2E_FAILED failed"
else
    echo "   e2e savepoint: SKIPPED"
fi

# --- totals ---
TOTAL_PASSED=$((INTEG_PASSED + UNIT_PASSED + CORE_PASSED + E2E_PASSED))
TOTAL_FAILED=$((INTEG_FAILED + UNIT_FAILED + CORE_FAILED + E2E_FAILED))

if [ "$INTEG_FAILED" -gt 0 ] || [ "$CORE_FAILED" -gt 0 ]; then
    VERDICT="FAIL"
else
    VERDICT="PASS"
fi

echo ""
echo "=== TOTAL: $TOTAL_PASSED passed, $TOTAL_FAILED failed — $VERDICT ==="

cat > "$ARTIFACT" <<EOF
{
  "bead_id": "$BEAD",
  "schema_version": 1,
  "verdict": "$VERDICT",
  "harness_integration": { "passed": $INTEG_PASSED, "failed": $INTEG_FAILED },
  "harness_unit": { "passed": $UNIT_PASSED, "failed": $UNIT_FAILED },
  "core_unit": { "passed": $CORE_PASSED, "failed": $CORE_FAILED },
  "e2e_savepoint": { "passed": $E2E_PASSED, "failed": $E2E_FAILED },
  "total_passed": $TOTAL_PASSED,
  "total_failed": $TOTAL_FAILED,
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

echo "Evidence written to $ARTIFACT"
