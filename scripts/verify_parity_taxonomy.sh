#!/usr/bin/env bash
# verify_parity_taxonomy.sh — E2E validation for parity taxonomy (bd-1dp9.1.1)
#
# Validates the parity taxonomy file and computes the current parity score.
# Exits 0 on success, 1 on failure.
#
# Usage: ./scripts/verify_parity_taxonomy.sh [--json]
#
# Output: structured log with run_id, score, feature counts, and artifact hashes.

set -euo pipefail

TAXONOMY_FILE="parity_taxonomy.toml"
RUN_ID="parity-taxonomy-$(date -u +%Y%m%dT%H%M%SZ)-$$"
JSON_OUTPUT=false

if [[ "${1:-}" == "--json" ]]; then
    JSON_OUTPUT=true
fi

# Verify taxonomy file exists
if [[ ! -f "$TAXONOMY_FILE" ]]; then
    echo "ERROR: $TAXONOMY_FILE not found" >&2
    exit 1
fi

# Compute artifact hash
TAXONOMY_HASH=$(sha256sum "$TAXONOMY_FILE" | awk '{print $1}')

# Count features by status
TOTAL=$(grep -c '^\[\[features\]\]' "$TAXONOMY_FILE" || echo 0)
PASS=$(grep -c 'status = "pass"' "$TAXONOMY_FILE" || echo 0)
FAIL=$(grep -c 'status = "fail"' "$TAXONOMY_FILE" || echo 0)
PARTIAL=$(grep -c 'status = "partial"' "$TAXONOMY_FILE" || echo 0)
EXCLUDED_COUNT=$(grep -c '^\[\[exclusions\]\]' "$TAXONOMY_FILE" || echo 0)

# Count features by family
SQL_COUNT=$(grep 'family = "SQL"' "$TAXONOMY_FILE" | wc -l || echo 0)
TXN_COUNT=$(grep 'family = "TXN"' "$TAXONOMY_FILE" | wc -l || echo 0)
FUN_COUNT=$(grep 'family = "FUN"' "$TAXONOMY_FILE" | wc -l || echo 0)
VDB_COUNT=$(grep 'family = "VDB"' "$TAXONOMY_FILE" | wc -l || echo 0)
PLN_COUNT=$(grep 'family = "PLN"' "$TAXONOMY_FILE" | wc -l || echo 0)
PGM_COUNT=$(grep 'family = "PGM"' "$TAXONOMY_FILE" | wc -l || echo 0)
EXT_COUNT=$(grep 'family = "EXT"' "$TAXONOMY_FILE" | wc -l || echo 0)
CLI_COUNT=$(grep 'family = "CLI"' "$TAXONOMY_FILE" | wc -l || echo 0)

# Compute weighted parity score using awk
SCORE_DATA=$(awk '
/^\[\[features\]\]/ { in_feature=1; weight=0; status="" }
in_feature && /^weight = / { gsub(/[^0-9]/, "", $3); weight=$3+0 }
in_feature && /^status = / { gsub(/"/, "", $3); status=$3 }
in_feature && /^$/ {
    total_weight += weight
    if (status == "pass") passing_weight += weight
    in_feature=0
}
END {
    # Handle last feature (no trailing blank line)
    if (in_feature) {
        total_weight += weight
        if (status == "pass") passing_weight += weight
    }
    if (total_weight > 0) {
        score = passing_weight / total_weight
    } else {
        score = 0
    }
    printf "%.6f %d %d", score, passing_weight, total_weight
}
' "$TAXONOMY_FILE")

SCORE=$(echo "$SCORE_DATA" | awk '{print $1}')
PASSING_WEIGHT=$(echo "$SCORE_DATA" | awk '{print $2}')
TOTAL_WEIGHT=$(echo "$SCORE_DATA" | awk '{print $3}')
SCORE_PCT=$(echo "$SCORE" | awk '{printf "%.2f", $1 * 100}')

# Run taxonomy unit tests
TEST_RESULT="unknown"
TEST_OUTPUT=""
if cargo test -p fsqlite-harness --test parity_taxonomy_test 2>&1; then
    TEST_RESULT="pass"
else
    TEST_RESULT="fail"
fi

# Validate structural invariants
ERRORS=0

# Check unique IDs
DUP_IDS=$(grep '^id = ' "$TAXONOMY_FILE" | sort | uniq -d)
if [[ -n "$DUP_IDS" ]]; then
    echo "ERROR: Duplicate feature/exclusion IDs found: $DUP_IDS" >&2
    ERRORS=$((ERRORS + 1))
fi

# Check declared total_weight matches
DECLARED_TOTAL=$(grep 'total_weight' "$TAXONOMY_FILE" | head -1 | grep -o '[0-9]*')
if [[ "$TOTAL_WEIGHT" -ne "$DECLARED_TOTAL" ]]; then
    echo "ERROR: Declared total_weight ($DECLARED_TOTAL) != actual sum ($TOTAL_WEIGHT)" >&2
    ERRORS=$((ERRORS + 1))
fi

# Output results
if $JSON_OUTPUT; then
    cat <<ENDJSON
{
  "run_id": "$RUN_ID",
  "phase": "parity_taxonomy_validation",
  "bead_id": "bd-1dp9.1.1",
  "taxonomy_hash": "$TAXONOMY_HASH",
  "score": $SCORE,
  "score_pct": $SCORE_PCT,
  "passing_weight": $PASSING_WEIGHT,
  "total_weight": $TOTAL_WEIGHT,
  "feature_counts": {
    "total": $TOTAL,
    "pass": $PASS,
    "fail": $FAIL,
    "partial": $PARTIAL
  },
  "family_counts": {
    "SQL": $SQL_COUNT,
    "TXN": $TXN_COUNT,
    "FUN": $FUN_COUNT,
    "VDB": $VDB_COUNT,
    "PLN": $PLN_COUNT,
    "PGM": $PGM_COUNT,
    "EXT": $EXT_COUNT,
    "CLI": $CLI_COUNT
  },
  "exclusion_count": $EXCLUDED_COUNT,
  "unit_test_result": "$TEST_RESULT",
  "validation_errors": $ERRORS,
  "result": "$([ $ERRORS -eq 0 ] && echo 'pass' || echo 'fail')"
}
ENDJSON
else
    echo "=== Parity Taxonomy Validation ==="
    echo "Run ID:         $RUN_ID"
    echo "Taxonomy hash:  $TAXONOMY_HASH"
    echo ""
    echo "--- Parity Score ---"
    echo "Score:          $SCORE_PCT% ($PASSING_WEIGHT / $TOTAL_WEIGHT weight points)"
    echo ""
    echo "--- Feature Counts ---"
    echo "Total:          $TOTAL"
    echo "  Pass:         $PASS"
    echo "  Fail:         $FAIL"
    echo "  Partial:      $PARTIAL"
    echo "Exclusions:     $EXCLUDED_COUNT"
    echo ""
    echo "--- Family Distribution ---"
    echo "  SQL:  $SQL_COUNT    TXN: $TXN_COUNT    FUN: $FUN_COUNT"
    echo "  VDB:  $VDB_COUNT    PLN: $PLN_COUNT    PGM: $PGM_COUNT"
    echo "  EXT:  $EXT_COUNT    CLI: $CLI_COUNT"
    echo ""
    echo "--- Unit Tests ---"
    echo "Result:         $TEST_RESULT"
    echo ""
    echo "--- Validation ---"
    echo "Errors:         $ERRORS"
    echo "Result:         $([ $ERRORS -eq 0 ] && echo 'PASS' || echo 'FAIL')"
fi

exit $ERRORS
