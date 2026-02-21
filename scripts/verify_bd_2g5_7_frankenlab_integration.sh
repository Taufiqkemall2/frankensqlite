#!/usr/bin/env bash
# CI verification gate for bd-2g5.7: FrankenLab deterministic testing integration
# Validates deterministic scheduling with concurrency primitives.
set -euo pipefail

echo "=== bd-2g5.7: FrankenLab Integration Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_2g5_7_frankenlab_integration -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-2g5.7 FrankenLab integration — all tests passed"
