#!/usr/bin/env bash
# bd-2wt.4: Verify function output compatibility with SQLite reference
# Runs the function compatibility test suite against rusqlite.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== bd-2wt.4: Function Compatibility Verification ==="
echo "repo_root=$REPO_ROOT"
echo ""

cd "$REPO_ROOT"

echo "--- Running function compatibility tests ---"
cargo test -p fsqlite-harness \
    --test bd_2wt_4_function_compat \
    -- --nocapture 2>&1 | tee /tmp/bd_2wt_4_compat.log

echo ""
echo "=== bd-2wt.4: All function compatibility verification gates passed ==="
