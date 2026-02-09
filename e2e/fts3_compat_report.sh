#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-2xl9"
LOG_STANDARD_REF="bd-1fpm"
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ISSUES_PATH="${WORKSPACE_ROOT}/.beads/issues.jsonl"
REPORT_DIR="${WORKSPACE_ROOT}/test-results"
REPORT_JSONL="${REPORT_DIR}/bd_2xl9_fts3_compat_report.jsonl"

declare -a UNIT_IDS=(
    "test_bd_2xl9_unit_compliance_gate"
    "prop_bd_2xl9_structure_compliance"
)
declare -a E2E_IDS=(
    "test_e2e_bd_2xl9"
    "test_e2e_bd_2xl9_compliance"
)
declare -a LOG_LEVELS=(
    "DEBUG"
    "INFO"
    "WARN"
    "ERROR"
)
declare -a SEMANTIC_MARKERS=(
    "column-level MATCH"
    "explicit AND"
    "unary NOT"
    "matchinfo"
    "offsets"
)

printf 'bead_id=%s level=DEBUG case=start workspace=%s report=%s reference=%s\n' \
    "${BEAD_ID}" "${WORKSPACE_ROOT}" "${REPORT_JSONL}" "${LOG_STANDARD_REF}"

if [[ ! -f "${ISSUES_PATH}" ]]; then
    printf 'bead_id=%s level=ERROR case=missing_issues_jsonl path=%s reference=%s\n' \
        "${BEAD_ID}" "${ISSUES_PATH}" "${LOG_STANDARD_REF}"
    exit 1
fi

mkdir -p "${REPORT_DIR}"

description="$(
    jq -r '
        select(.id == "bd-2xl9")
        | .description,
          (.comments[]?.text // empty)
    ' "${ISSUES_PATH}"
)"

if [[ -z "${description//[[:space:]]/}" ]]; then
    printf 'bead_id=%s level=ERROR case=missing_bead_description path=%s reference=%s\n' \
        "${BEAD_ID}" "${ISSUES_PATH}" "${LOG_STANDARD_REF}"
    exit 1
fi

trace_id="$(printf '%s' "${description}" | sha256sum | awk '{print substr($1, 1, 16)}')"
printf 'bead_id=%s level=DEBUG case=trace trace_id=%s reference=%s\n' \
    "${BEAD_ID}" "${trace_id}" "${LOG_STANDARD_REF}"

printf '# bead_id=%s fts3/fts4 compatibility report\n' "${BEAD_ID}" >"${REPORT_JSONL}"
printf '# kind\tmarker\tpresent\n' >>"${REPORT_JSONL}"

declare -a missing_markers=()

check_marker() {
    local kind="$1"
    local marker="$2"
    local present=0

    if grep -Fqi -- "${marker}" <<<"${description}"; then
        present=1
    else
        missing_markers+=("${kind}:${marker}")
    fi

    printf '{"bead_id":"%s","trace_id":"%s","kind":"%s","marker":"%s","present":%s}\n' \
        "${BEAD_ID}" "${trace_id}" "${kind}" "${marker}" "${present}" >>"${REPORT_JSONL}"
}

for marker in "${UNIT_IDS[@]}"; do
    check_marker "unit_id" "${marker}"
done
for marker in "${E2E_IDS[@]}"; do
    check_marker "e2e_id" "${marker}"
done
for marker in "${LOG_LEVELS[@]}"; do
    check_marker "log_level" "${marker}"
done
check_marker "log_standard" "${LOG_STANDARD_REF}"
for marker in "${SEMANTIC_MARKERS[@]}"; do
    check_marker "semantic" "${marker}"
done

printf 'bead_id=%s level=INFO case=summary total_checks=%s missing=%s trace_id=%s report=%s\n' \
    "${BEAD_ID}" \
    "$(( ${#UNIT_IDS[@]} + ${#E2E_IDS[@]} + ${#LOG_LEVELS[@]} + ${#SEMANTIC_MARKERS[@]} + 1 ))" \
    "${#missing_markers[@]}" \
    "${trace_id}" \
    "${REPORT_JSONL}"

for marker in "${missing_markers[@]}"; do
    printf 'bead_id=%s level=WARN case=missing_marker marker=%s trace_id=%s reference=%s\n' \
        "${BEAD_ID}" "${marker}" "${trace_id}" "${LOG_STANDARD_REF}"
done

if [[ "${#missing_markers[@]}" -gt 0 ]]; then
    printf 'bead_id=%s level=ERROR case=terminal_failure missing=%s trace_id=%s report=%s reference=%s\n' \
        "${BEAD_ID}" "${missing_markers[*]}" "${trace_id}" "${REPORT_JSONL}" "${LOG_STANDARD_REF}"
    exit 1
fi

printf 'bead_id=%s level=WARN case=degraded_mode_count=0 trace_id=%s reference=%s\n' \
    "${BEAD_ID}" "${trace_id}" "${LOG_STANDARD_REF}"
printf 'bead_id=%s level=ERROR case=terminal_failure_count=0 trace_id=%s reference=%s\n' \
    "${BEAD_ID}" "${trace_id}" "${LOG_STANDARD_REF}"
printf 'bead_id=%s level=INFO case=pass trace_id=%s report=%s\n' \
    "${BEAD_ID}" "${trace_id}" "${REPORT_JSONL}"
