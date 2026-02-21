#!/usr/bin/env bash
# CI verification gate for bd-2g5.8: Flat Combining correctness tests
# Validates sequential batching under contention with no lost updates.
set -euo pipefail

echo "=== bd-2g5.8: Flat Combining Correctness Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_2g5_8_flat_combining_correctness -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-2g5.8 flat combining correctness — all tests passed"
