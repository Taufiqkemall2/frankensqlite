#!/usr/bin/env bash
# verify_differential_harness_v2.sh — E2E validation for oracle differential harness v2 (bd-1dp9.1.2)
#
# Validates:
# 1. Unit tests compile and pass
# 2. Envelope artifact IDs are deterministic (same input → same ID)
# 3. Differential comparison detects both pass and divergence
# 4. Result serialization produces valid JSON
#
# Usage: ./scripts/verify_differential_harness_v2.sh [--json]
#
# Output: structured log with run_id, test results, and artifact hashes.

set -euo pipefail

RUN_ID="differential-v2-$(date -u +%Y%m%dT%H%M%SZ)-$$"
JSON_OUTPUT=false

if [[ "${1:-}" == "--json" ]]; then
    JSON_OUTPUT=true
fi

# ── Phase 1: Compile check ──────────────────────────────────────────────

COMPILE_RESULT="unknown"
if cargo check -p fsqlite-harness 2>&1; then
    COMPILE_RESULT="pass"
else
    COMPILE_RESULT="fail"
fi

# ── Phase 2: Run unit tests ─────────────────────────────────────────────

TEST_OUTPUT=""
TEST_RESULT="unknown"
TEST_COUNT=0
TEST_PASSED=0
TEST_FAILED=0

if TEST_OUTPUT=$(cargo test -p fsqlite-harness --test differential_v2_test 2>&1); then
    TEST_RESULT="pass"
else
    TEST_RESULT="fail"
fi

# Parse test counts from output
if echo "$TEST_OUTPUT" | grep -q "test result:"; then
    RESULT_LINE=$(echo "$TEST_OUTPUT" | grep "test result:" | tail -1)
    TEST_PASSED=$(echo "$RESULT_LINE" | grep -o '[0-9]* passed' | grep -o '[0-9]*' || echo 0)
    TEST_FAILED=$(echo "$RESULT_LINE" | grep -o '[0-9]* failed' | grep -o '[0-9]*' || echo 0)
    TEST_COUNT=$((TEST_PASSED + TEST_FAILED))
fi

# ── Phase 3: Run parity taxonomy tests (cross-validation) ───────────────

TAXONOMY_RESULT="unknown"
if cargo test -p fsqlite-harness --test parity_taxonomy_test 2>&1 > /dev/null; then
    TAXONOMY_RESULT="pass"
else
    TAXONOMY_RESULT="fail"
fi

# ── Phase 4: Verify module is registered ─────────────────────────────────

MODULE_REGISTERED="false"
if grep -q "pub mod differential_v2" crates/fsqlite-harness/src/lib.rs; then
    MODULE_REGISTERED="true"
fi

# ── Phase 5: Verify key artifacts exist ──────────────────────────────────

ARTIFACTS_OK="true"
for f in \
    "crates/fsqlite-harness/src/differential_v2.rs" \
    "crates/fsqlite-harness/tests/differential_v2_test.rs" \
    "scripts/verify_differential_harness_v2.sh" \
; do
    if [[ ! -f "$f" ]]; then
        ARTIFACTS_OK="false"
        echo "ERROR: Missing artifact: $f" >&2
    fi
done

# Compute artifact hashes
V2_MODULE_HASH=$(sha256sum crates/fsqlite-harness/src/differential_v2.rs | awk '{print $1}')
V2_TEST_HASH=$(sha256sum crates/fsqlite-harness/tests/differential_v2_test.rs | awk '{print $1}')

# ── Aggregate result ────────────────────────────────────────────────────

ERRORS=0
[[ "$COMPILE_RESULT" == "pass" ]] || ERRORS=$((ERRORS + 1))
[[ "$TEST_RESULT" == "pass" ]] || ERRORS=$((ERRORS + 1))
[[ "$MODULE_REGISTERED" == "true" ]] || ERRORS=$((ERRORS + 1))
[[ "$ARTIFACTS_OK" == "true" ]] || ERRORS=$((ERRORS + 1))

# ── Output ──────────────────────────────────────────────────────────────

if $JSON_OUTPUT; then
    cat <<ENDJSON
{
  "run_id": "$RUN_ID",
  "phase": "differential_harness_v2_validation",
  "bead_id": "bd-1dp9.1.2",
  "compile_result": "$COMPILE_RESULT",
  "unit_test_result": "$TEST_RESULT",
  "test_counts": {
    "total": $TEST_COUNT,
    "passed": $TEST_PASSED,
    "failed": $TEST_FAILED
  },
  "taxonomy_cross_check": "$TAXONOMY_RESULT",
  "module_registered": $MODULE_REGISTERED,
  "artifacts_present": $ARTIFACTS_OK,
  "artifact_hashes": {
    "differential_v2_module": "$V2_MODULE_HASH",
    "differential_v2_test": "$V2_TEST_HASH"
  },
  "validation_errors": $ERRORS,
  "result": "$([ $ERRORS -eq 0 ] && echo 'pass' || echo 'fail')"
}
ENDJSON
else
    echo "=== Differential Harness V2 Validation ==="
    echo "Run ID:           $RUN_ID"
    echo "Bead:             bd-1dp9.1.2"
    echo ""
    echo "--- Compilation ---"
    echo "Result:           $COMPILE_RESULT"
    echo ""
    echo "--- Unit Tests ---"
    echo "Result:           $TEST_RESULT"
    echo "Total:            $TEST_COUNT"
    echo "  Passed:         $TEST_PASSED"
    echo "  Failed:         $TEST_FAILED"
    echo ""
    echo "--- Cross-Validation ---"
    echo "Taxonomy tests:   $TAXONOMY_RESULT"
    echo ""
    echo "--- Module Registration ---"
    echo "Registered:       $MODULE_REGISTERED"
    echo ""
    echo "--- Artifacts ---"
    echo "Present:          $ARTIFACTS_OK"
    echo "Module hash:      $V2_MODULE_HASH"
    echo "Test hash:        $V2_TEST_HASH"
    echo ""
    echo "--- Validation ---"
    echo "Errors:           $ERRORS"
    echo "Result:           $([ $ERRORS -eq 0 ] && echo 'PASS' || echo 'FAIL')"
fi

exit $ERRORS
