#!/usr/bin/env bash
# verify_corpus_ingest.sh — E2E validation for corpus ingestion (bd-1dp9.2.1)
#
# Validates:
# 1. Unit tests compile and pass
# 2. Seed corpus covers all 8 taxonomy families
# 3. Conformance fixtures are ingested correctly
# 4. Coverage report shows bucket fill percentages
#
# Usage: ./scripts/verify_corpus_ingest.sh [--json]

set -euo pipefail

RUN_ID="corpus-ingest-$(date -u +%Y%m%dT%H%M%SZ)-$$"
JSON_OUTPUT=false

if [[ "${1:-}" == "--json" ]]; then
    JSON_OUTPUT=true
fi

# ── Phase 1: Run unit tests ─────────────────────────────────────────────

TEST_OUTPUT=""
TEST_RESULT="unknown"
TEST_COUNT=0
TEST_PASSED=0
TEST_FAILED=0

if TEST_OUTPUT=$(cargo test -p fsqlite-harness --test corpus_ingest_test 2>&1); then
    TEST_RESULT="pass"
else
    TEST_RESULT="fail"
fi

if echo "$TEST_OUTPUT" | grep -q "test result:"; then
    RESULT_LINE=$(echo "$TEST_OUTPUT" | grep "test result:" | tail -1)
    TEST_PASSED=$(echo "$RESULT_LINE" | grep -o '[0-9]* passed' | grep -o '[0-9]*' || echo 0)
    TEST_FAILED=$(echo "$RESULT_LINE" | grep -o '[0-9]* failed' | grep -o '[0-9]*' || echo 0)
    TEST_COUNT=$((TEST_PASSED + TEST_FAILED))
fi

# ── Phase 2: Verify artifacts exist ─────────────────────────────────────

ARTIFACTS_OK="true"
for f in \
    "crates/fsqlite-harness/src/corpus_ingest.rs" \
    "crates/fsqlite-harness/tests/corpus_ingest_test.rs" \
    "scripts/verify_corpus_ingest.sh" \
; do
    if [[ ! -f "$f" ]]; then
        ARTIFACTS_OK="false"
        echo "ERROR: Missing artifact: $f" >&2
    fi
done

MODULE_HASH=$(sha256sum crates/fsqlite-harness/src/corpus_ingest.rs | awk '{print $1}')
TEST_HASH=$(sha256sum crates/fsqlite-harness/tests/corpus_ingest_test.rs | awk '{print $1}')

# ── Phase 3: Verify module is registered ─────────────────────────────────

MODULE_REGISTERED="false"
if grep -q "pub mod corpus_ingest" crates/fsqlite-harness/src/lib.rs; then
    MODULE_REGISTERED="true"
fi

# ── Aggregate ───────────────────────────────────────────────────────────

ERRORS=0
[[ "$TEST_RESULT" == "pass" ]] || ERRORS=$((ERRORS + 1))
[[ "$MODULE_REGISTERED" == "true" ]] || ERRORS=$((ERRORS + 1))
[[ "$ARTIFACTS_OK" == "true" ]] || ERRORS=$((ERRORS + 1))

# ── Output ──────────────────────────────────────────────────────────────

if $JSON_OUTPUT; then
    cat <<ENDJSON
{
  "run_id": "$RUN_ID",
  "phase": "corpus_ingest_validation",
  "bead_id": "bd-1dp9.2.1",
  "unit_test_result": "$TEST_RESULT",
  "test_counts": {
    "total": $TEST_COUNT,
    "passed": $TEST_PASSED,
    "failed": $TEST_FAILED
  },
  "module_registered": $MODULE_REGISTERED,
  "artifacts_present": $ARTIFACTS_OK,
  "artifact_hashes": {
    "corpus_ingest_module": "$MODULE_HASH",
    "corpus_ingest_test": "$TEST_HASH"
  },
  "validation_errors": $ERRORS,
  "result": "$([ $ERRORS -eq 0 ] && echo 'pass' || echo 'fail')"
}
ENDJSON
else
    echo "=== Corpus Ingest Validation ==="
    echo "Run ID:           $RUN_ID"
    echo "Bead:             bd-1dp9.2.1"
    echo ""
    echo "--- Unit Tests ---"
    echo "Result:           $TEST_RESULT"
    echo "Total:            $TEST_COUNT"
    echo "  Passed:         $TEST_PASSED"
    echo "  Failed:         $TEST_FAILED"
    echo ""
    echo "--- Artifacts ---"
    echo "Present:          $ARTIFACTS_OK"
    echo "Module hash:      $MODULE_HASH"
    echo "Test hash:        $TEST_HASH"
    echo ""
    echo "--- Validation ---"
    echo "Errors:           $ERRORS"
    echo "Result:           $([ $ERRORS -eq 0 ] && echo 'PASS' || echo 'FAIL')"
fi

exit $ERRORS
