#!/usr/bin/env bash
# bd-5ew.4: Verify parser conformance & fuzz suite
# Runs the comprehensive parser test suite and checks pass-rate targets.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== bd-5ew.4: Parser Conformance & Fuzz Verification ==="
echo "repo_root=$REPO_ROOT"
echo ""

cd "$REPO_ROOT"

echo "--- Running parser conformance + round-trip + fuzz tests ---"
cargo test -p fsqlite-harness \
    --test bd_5ew_4_parser_conformance \
    -- --nocapture 2>&1 | tee /tmp/bd_5ew_4_parser.log

echo ""
echo "--- Running parser crate unit tests ---"
cargo test -p fsqlite-parser --lib \
    -- --nocapture 2>&1 | tee /tmp/bd_5ew_4_parser_unit.log

echo ""
echo "=== bd-5ew.4: All parser verification gates passed ==="
