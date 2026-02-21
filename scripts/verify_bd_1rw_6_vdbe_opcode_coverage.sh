#!/usr/bin/env bash
# bd-1rw.6: Verify VDBE opcode coverage, sqllogictest, parallel determinism
# Runs the comprehensive VDBE test suite and checks pass-rate targets.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== bd-1rw.6: VDBE Opcode Coverage Verification ==="
echo "repo_root=$REPO_ROOT"
echo ""

cd "$REPO_ROOT"

echo "--- Running VDBE opcode coverage + sqllogictest + determinism tests ---"
cargo test -p fsqlite-harness \
    --test bd_1rw_6_vdbe_opcode_coverage \
    -- --nocapture 2>&1 | tee /tmp/bd_1rw_6_vdbe.log

echo ""
echo "--- Running VDBE crate unit tests ---"
cargo test -p fsqlite-vdbe --lib \
    -- --nocapture 2>&1 | tee /tmp/bd_1rw_6_vdbe_unit.log

echo ""
echo "=== bd-1rw.6: All VDBE verification gates passed ==="
