#!/usr/bin/env bash
# bd-3plop.4: Verify lock contention storm resilience under page-level MVCC
# Runs 6 tests covering hot-row, hot-page, convoy, write-skew, livelock, and conformance.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== bd-3plop.4: Lock Contention Storm Verification ==="
echo "repo_root=$REPO_ROOT"
echo ""

cd "$REPO_ROOT"

echo "--- Running lock contention storm tests ---"
cargo test -p fsqlite-harness \
    --test bd_3plop_4_lock_contention_storms \
    -- --nocapture 2>&1 | tee /tmp/bd_3plop_4_lock_contention.log

echo ""
echo "=== bd-3plop.4: All lock contention storm verification gates passed ==="
