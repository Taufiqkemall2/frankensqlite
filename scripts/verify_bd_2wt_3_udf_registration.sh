#!/usr/bin/env bash
# bd-2wt.3: Verify UDF registration API
# Runs the comprehensive UDF registration test suite.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== bd-2wt.3: UDF Registration API Verification ==="
echo "repo_root=$REPO_ROOT"
echo ""

cd "$REPO_ROOT"

echo "--- Running UDF registration tests ---"
cargo test -p fsqlite-harness \
    --test bd_2wt_3_udf_registration \
    -- --nocapture 2>&1 | tee /tmp/bd_2wt_3_udf.log

echo ""
echo "--- Running fsqlite crate unit tests (--skip concurrent) ---"
cargo test -p fsqlite --lib \
    -- --skip concurrent --nocapture 2>&1 | tee /tmp/bd_2wt_3_fsqlite_unit.log

echo ""
echo "=== bd-2wt.3: All UDF registration verification gates passed ==="
