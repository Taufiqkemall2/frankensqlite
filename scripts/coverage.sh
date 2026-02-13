#!/usr/bin/env bash
# FrankenSQLite Coverage Script
# Reproducible coverage commands for workspace and per-crate runs
#
# Prerequisites:
#   cargo install cargo-llvm-cov
#
# Usage:
#   ./scripts/coverage.sh              # Full workspace coverage with HTML report
#   ./scripts/coverage.sh --summary    # Quick summary (text)
#   ./scripts/coverage.sh --json       # JSON output for CI
#   ./scripts/coverage.sh --lcov       # LCOV format for external tools
#   ./scripts/coverage.sh --crate fsqlite-mvcc  # Single crate coverage
#   ./scripts/coverage.sh --branch     # Include branch coverage (nightly only)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="$PROJECT_ROOT/target/coverage"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Default options
FORMAT="html"
CRATE=""
BRANCH_COV=false
FAIL_UNDER_LINES=""
FAIL_UNDER_FUNCTIONS=""

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --summary           Generate text summary only"
    echo "  --json              Generate JSON output"
    echo "  --lcov              Generate LCOV output"
    echo "  --html              Generate HTML report (default)"
    echo "  --crate CRATE       Run coverage for a specific crate"
    echo "  --branch            Enable branch coverage (requires nightly)"
    echo "  --fail-under-lines N    Fail if line coverage < N%"
    echo "  --fail-under-functions N    Fail if function coverage < N%"
    echo "  -h, --help          Show this help"
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --summary)
            FORMAT="summary"
            shift
            ;;
        --json)
            FORMAT="json"
            shift
            ;;
        --lcov)
            FORMAT="lcov"
            shift
            ;;
        --html)
            FORMAT="html"
            shift
            ;;
        --crate)
            CRATE="$2"
            shift 2
            ;;
        --branch)
            BRANCH_COV=true
            shift
            ;;
        --fail-under-lines)
            FAIL_UNDER_LINES="$2"
            shift 2
            ;;
        --fail-under-functions)
            FAIL_UNDER_FUNCTIONS="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Build command
CMD="cargo llvm-cov"

# Scope: workspace or single crate
if [[ -n "$CRATE" ]]; then
    CMD="$CMD -p $CRATE"
else
    CMD="$CMD --workspace"
fi

# Branch coverage (nightly feature)
if [[ "$BRANCH_COV" == "true" ]]; then
    CMD="$CMD --branch"
fi

# Failure thresholds
if [[ -n "$FAIL_UNDER_LINES" ]]; then
    CMD="$CMD --fail-under-lines $FAIL_UNDER_LINES"
fi
if [[ -n "$FAIL_UNDER_FUNCTIONS" ]]; then
    CMD="$CMD --fail-under-functions $FAIL_UNDER_FUNCTIONS"
fi

# Output format
mkdir -p "$OUTPUT_DIR"
case $FORMAT in
    summary)
        echo "Running coverage (summary)..."
        $CMD --summary-only
        ;;
    json)
        OUTPUT_FILE="$OUTPUT_DIR/coverage_${TIMESTAMP}.json"
        echo "Running coverage (JSON -> $OUTPUT_FILE)..."
        $CMD --json --output-path "$OUTPUT_FILE"
        echo "Coverage data written to: $OUTPUT_FILE"
        # Also create latest symlink
        ln -sf "coverage_${TIMESTAMP}.json" "$OUTPUT_DIR/coverage_latest.json"
        ;;
    lcov)
        OUTPUT_FILE="$OUTPUT_DIR/coverage_${TIMESTAMP}.lcov"
        echo "Running coverage (LCOV -> $OUTPUT_FILE)..."
        $CMD --lcov --output-path "$OUTPUT_FILE"
        echo "Coverage data written to: $OUTPUT_FILE"
        ln -sf "coverage_${TIMESTAMP}.lcov" "$OUTPUT_DIR/coverage_latest.lcov"
        ;;
    html)
        HTML_DIR="$OUTPUT_DIR/html_${TIMESTAMP}"
        echo "Running coverage (HTML -> $HTML_DIR)..."
        $CMD --html --output-dir "$HTML_DIR"
        echo "Coverage report generated at: $HTML_DIR/index.html"
        ln -sfn "html_${TIMESTAMP}" "$OUTPUT_DIR/html_latest"
        ;;
esac

echo "Done."
