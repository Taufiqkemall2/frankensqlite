#!/usr/bin/env bash
set -u -o pipefail

BEAD_ID="bd-2v8x"
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-${WORKSPACE_ROOT}/target/bd-2v8x-build-matrix}"
LOG_DIR="${WORKSPACE_ROOT}/target/bd-2v8x-build-matrix/logs"

mkdir -p "${TARGET_DIR}" "${LOG_DIR}"

declare -a failures=()

log_result() {
    local level="$1"
    local variant="$2"
    local cmd="$3"
    local exit_code="$4"
    local duration_ms="$5"
    local log_path="$6"

    printf \
        'bead_id=%s level=%s variant=%s cmd="%s" exit_code=%s duration_ms=%s log=%s\n' \
        "${BEAD_ID}" "${level}" "${variant}" "${cmd}" "${exit_code}" "${duration_ms}" "${log_path}"
}

run_variant() {
    local variant="$1"
    shift

    local start_ms end_ms duration_ms exit_code
    local log_path="${LOG_DIR}/${variant}.log"
    local cmd=(cargo build -p fsqlite "$@")

    start_ms="$(date +%s%3N)"
    (
        cd "${WORKSPACE_ROOT}" || exit 1
        CARGO_TARGET_DIR="${TARGET_DIR}" "${cmd[@]}"
    ) >"${log_path}" 2>&1
    exit_code=$?
    end_ms="$(date +%s%3N)"
    duration_ms=$((end_ms - start_ms))

    if [[ ${exit_code} -eq 0 ]]; then
        log_result "INFO" "${variant}" "${cmd[*]}" "${exit_code}" "${duration_ms}" "${log_path}"
        return
    fi

    log_result "ERROR" "${variant}" "${cmd[*]}" "${exit_code}" "${duration_ms}" "${log_path}"
    printf 'bead_id=%s level=ERROR variant=%s preview_start\n' "${BEAD_ID}" "${variant}"
    sed -n '1,50p' "${log_path}"
    printf 'bead_id=%s level=ERROR variant=%s preview_end log=%s\n' "${BEAD_ID}" "${variant}" "${log_path}"
    failures+=("${variant}")
}

run_variant "default"
run_variant "no-default-features" "--no-default-features"
run_variant "json" "--features" "json"
run_variant "fts5" "--features" "fts5"
run_variant "fts3" "--features" "fts3"
run_variant "rtree" "--features" "rtree"
run_variant "session" "--features" "session"
run_variant "icu" "--features" "icu"
run_variant "misc" "--features" "misc"

if [[ ${#failures[@]} -gt 0 ]]; then
    printf 'bead_id=%s level=ERROR summary=build_matrix_failed failing_variants=%s\n' \
        "${BEAD_ID}" "${failures[*]}"
    exit 1
fi

printf 'bead_id=%s level=INFO summary=build_matrix_passed variants=%s\n' "${BEAD_ID}" "9"
