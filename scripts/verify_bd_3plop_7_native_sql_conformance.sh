#!/usr/bin/env bash
# bd-3plop.7: Verify native SQL conformance suite
# Runs the native conformance tests and checks pass-rate targets.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== bd-3plop.7: Native SQL Conformance Verification ==="
echo "repo_root=$REPO_ROOT"
echo ""

cd "$REPO_ROOT"

echo "--- Running native SQL conformance tests ---"
cargo test -p fsqlite-harness \
    --test bd_3plop_7_native_sql_conformance \
    -- --nocapture 2>&1 | tee /tmp/bd_3plop_7_native.log

echo ""
echo "--- Running TCL conformance harness tests ---"
cargo test -p fsqlite-harness \
    --test bd_3plop_7_tcl_conformance \
    -- --nocapture 2>&1 | tee /tmp/bd_3plop_7_tcl.log

echo ""
echo "--- Running C API unit tests ---"
cargo test -p fsqlite-c-api \
    -- --nocapture 2>&1 | tee /tmp/bd_3plop_7_capi.log

echo ""
echo "=== bd-3plop.7: All conformance gates passed ==="
