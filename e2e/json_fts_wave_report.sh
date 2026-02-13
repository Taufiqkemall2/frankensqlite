#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-1dp9.5.2"
LOG_STANDARD_REF="bd-1fpm"
SEED="1095200001"
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="bd-1dp9.5.2-$(date -u +%Y%m%dT%H%M%SZ)-$$"
TARGET_DIR="${CARGO_TARGET_DIR:-target_bd_1dp9_5_2}"
REPORT_DIR="${WORKSPACE_ROOT}/test-results/bd_1dp9_5_2"
LOG_DIR="${REPORT_DIR}/logs/${RUN_ID}"
REPORT_JSONL="${REPORT_DIR}/${RUN_ID}.jsonl"
JSON_OUTPUT=false

if [[ "${1:-}" == "--json" ]]; then
    JSON_OUTPUT=true
fi

mkdir -p "${LOG_DIR}"

printf 'bead_id=%s level=DEBUG run_id=%s seed=%s phase=start report=%s reference=%s\n' \
    "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${REPORT_JSONL}" "${LOG_STANDARD_REF}"

emit_event() {
    local phase="$1"
    local marker="$2"
    local status="$3"
    local exit_code="$4"
    local log_path="$5"
    local log_sha256="$6"
    local first_divergence="$7"
    local timestamp
    timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '{"bead_id":"%s","run_id":"%s","seed":"%s","timestamp":"%s","phase":"%s","marker":"%s","status":"%s","exit_code":%s,"log_path":"%s","log_sha256":"%s","first_divergence":%s,"log_standard_ref":"%s"}\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${timestamp}" "${phase}" "${marker}" \
        "${status}" "${exit_code}" "${log_path}" "${log_sha256}" "${first_divergence}" \
        "${LOG_STANDARD_REF}" >>"${REPORT_JSONL}"
}

run_phase() {
    local phase="$1"
    shift
    local log_file="${LOG_DIR}/${phase}.log"

    printf 'bead_id=%s level=DEBUG run_id=%s seed=%s phase=%s marker=start reference=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${phase}" "${LOG_STANDARD_REF}"
    emit_event "${phase}" "start" "running" 0 "${log_file#${WORKSPACE_ROOT}/}" "" "false"

    local status="pass"
    local exit_code=0
    set +e
    CARGO_TARGET_DIR="${TARGET_DIR}" "$@" >"${log_file}" 2>&1
    exit_code=$?
    set -e
    if [[ "${exit_code}" -ne 0 ]]; then
        status="fail"
    fi

    local log_sha256
    log_sha256="$(sha256sum "${log_file}" | awk '{print $1}')"
    local first_divergence="false"
    if grep -qi "first_divergence" "${log_file}"; then
        first_divergence="true"
    fi

    if [[ "${status}" == "pass" ]]; then
        printf 'bead_id=%s level=INFO run_id=%s seed=%s phase=%s status=pass log=%s sha256=%s first_divergence=%s\n' \
            "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${phase}" "${log_file}" "${log_sha256}" "${first_divergence}"
    else
        printf 'bead_id=%s level=ERROR run_id=%s seed=%s phase=%s status=fail exit_code=%s log=%s sha256=%s first_divergence=%s\n' \
            "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${phase}" "${exit_code}" "${log_file}" "${log_sha256}" "${first_divergence}"
    fi

    emit_event \
        "${phase}" \
        "complete" \
        "${status}" \
        "${exit_code}" \
        "${log_file#${WORKSPACE_ROOT}/}" \
        "${log_sha256}" \
        "${first_divergence}"
    return "${exit_code}"
}

failures=0

run_phase \
    "differential_wave" \
    cargo test -p fsqlite-harness --test bd_1dp9_5_2_json_fts_wave -- --nocapture \
    || failures=$((failures + 1))

run_phase \
    "unit_json_extension" \
    cargo test -p fsqlite-ext-json --lib -- --nocapture \
    || failures=$((failures + 1))

run_phase \
    "unit_fts5_extension" \
    cargo test -p fsqlite-ext-fts5 --lib -- --nocapture \
    || failures=$((failures + 1))

run_phase \
    "e2e_json_storage" \
    cargo test -p fsqlite-harness --test ext_real_storage_test json_text_storage_round_trip -- --nocapture \
    || failures=$((failures + 1))

run_phase \
    "e2e_fts_tokenizer" \
    cargo test -p fsqlite-harness --test ext_real_storage_test fts5_tokenizer_on_stored_text -- --nocapture \
    || failures=$((failures + 1))

summary_sha256="$(sha256sum "${REPORT_JSONL}" | awk '{print $1}')"
overall_status="pass"
if [[ "${failures}" -gt 0 ]]; then
    overall_status="fail"
fi

printf '{"bead_id":"%s","run_id":"%s","seed":"%s","phase":"summary","status":"%s","failures":%s,"report":"%s","report_sha256":"%s","log_standard_ref":"%s"}\n' \
    "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${overall_status}" "${failures}" \
    "${REPORT_JSONL#${WORKSPACE_ROOT}/}" "${summary_sha256}" "${LOG_STANDARD_REF}" \
    >>"${REPORT_JSONL}"

if [[ "${overall_status}" == "pass" ]]; then
    printf 'bead_id=%s level=WARN run_id=%s seed=%s phase=summary degraded_mode_count=0 reference=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${LOG_STANDARD_REF}"
    printf 'bead_id=%s level=INFO run_id=%s seed=%s phase=summary status=pass report=%s report_sha256=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${REPORT_JSONL}" "${summary_sha256}"
else
    printf 'bead_id=%s level=WARN run_id=%s seed=%s phase=summary degraded_mode_count=%s reference=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${failures}" "${LOG_STANDARD_REF}"
    printf 'bead_id=%s level=ERROR run_id=%s seed=%s phase=summary status=fail failures=%s report=%s report_sha256=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${failures}" "${REPORT_JSONL}" "${summary_sha256}"
fi

if ${JSON_OUTPUT}; then
    cat <<ENDJSON
{
  "bead_id": "${BEAD_ID}",
  "run_id": "${RUN_ID}",
  "seed": "${SEED}",
  "status": "${overall_status}",
  "failures": ${failures},
  "report_jsonl": "${REPORT_JSONL}",
  "report_sha256": "${summary_sha256}"
}
ENDJSON
fi

if [[ "${failures}" -gt 0 ]]; then
    exit 1
fi

