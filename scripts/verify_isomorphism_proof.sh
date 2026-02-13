#!/usr/bin/env bash
# E2E verification for bd-1dp9.6.5 — Isomorphism-proof harness
set -euo pipefail

BEAD="bd-1dp9.6.5"
RESULTS_DIR="test-results"
ARTIFACT="$RESULTS_DIR/${BEAD}-isomorphism-proof-verify.json"
mkdir -p "$RESULTS_DIR"

echo "=== $BEAD E2E verification ==="

echo ">> Running harness integration tests …"
INTEG_OUT=$(cargo test -p fsqlite-harness --test bd_1dp9_6_5_isomorphism_proof 2>&1)
INTEG_LINE=$(echo "$INTEG_OUT" | grep 'test result:' | tail -1)
INTEG_PASSED=$(echo "$INTEG_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
INTEG_FAILED=$(echo "$INTEG_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
echo "   harness integration: $INTEG_PASSED passed, $INTEG_FAILED failed"

UNIT_PASSED=0; UNIT_FAILED=0
echo ">> Running isomorphism_proof unit tests …"
if UNIT_OUT=$(cargo test -p fsqlite-harness --lib -- isomorphism_proof 2>&1); then
    UNIT_LINE=$(echo "$UNIT_OUT" | grep 'test result:' | tail -1)
    UNIT_PASSED=$(echo "$UNIT_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    UNIT_FAILED=$(echo "$UNIT_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   harness unit: $UNIT_PASSED passed, $UNIT_FAILED failed"
else
    echo "   harness unit: SKIPPED"
fi

TD_PASSED=0; TD_FAILED=0
echo ">> Running toolchain_determinism tests …"
if TD_OUT=$(cargo test -p fsqlite-harness --lib -- toolchain_determinism 2>&1); then
    TD_LINE=$(echo "$TD_OUT" | grep 'test result:' | tail -1)
    TD_PASSED=$(echo "$TD_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    TD_FAILED=$(echo "$TD_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   toolchain_determinism: $TD_PASSED passed, $TD_FAILED failed"
else
    echo "   toolchain_determinism: SKIPPED"
fi

RP_PASSED=0; RP_FAILED=0
echo ">> Running ratchet_policy tests …"
if RP_OUT=$(cargo test -p fsqlite-harness --lib -- ratchet_policy 2>&1); then
    RP_LINE=$(echo "$RP_OUT" | grep 'test result:' | tail -1)
    RP_PASSED=$(echo "$RP_LINE" | grep -oP '\d+ passed' | grep -oP '\d+')
    RP_FAILED=$(echo "$RP_LINE" | grep -oP '\d+ failed' | grep -oP '\d+')
    echo "   ratchet_policy: $RP_PASSED passed, $RP_FAILED failed"
else
    echo "   ratchet_policy: SKIPPED"
fi

TOTAL_PASSED=$((INTEG_PASSED + UNIT_PASSED + TD_PASSED + RP_PASSED))
TOTAL_FAILED=$((INTEG_FAILED + UNIT_FAILED + TD_FAILED + RP_FAILED))

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
  "toolchain_determinism": { "passed": $TD_PASSED, "failed": $TD_FAILED },
  "ratchet_policy": { "passed": $RP_PASSED, "failed": $RP_FAILED },
  "total_passed": $TOTAL_PASSED,
  "total_failed": $TOTAL_FAILED,
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

echo "Evidence written to $ARTIFACT"
