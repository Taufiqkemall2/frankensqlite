#!/usr/bin/env bash
# bd-1as.5: Verify planner correctness, cardinality accuracy & join optimality
# Runs the comprehensive planner test suite with synthetic schemas.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== bd-1as.5: Planner Correctness Verification ==="
echo "repo_root=$REPO_ROOT"
echo ""

cd "$REPO_ROOT"

echo "--- Running planner correctness tests ---"
cargo test -p fsqlite-harness \
    --test bd_1as_5_planner_correctness \
    -- --nocapture 2>&1 | tee /tmp/bd_1as_5_planner.log

echo ""
echo "=== bd-1as.5: All planner correctness verification gates passed ==="
