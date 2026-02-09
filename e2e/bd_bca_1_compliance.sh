#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-bca.1"
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ISSUES_PATH="${WORKSPACE_ROOT}/.beads/issues.jsonl"
TEST_TARGET="bd_bca_1_phase5_compliance"

printf 'bead_id=%s level=DEBUG case=start workspace=%s target=%s\n' \
    "${BEAD_ID}" "${WORKSPACE_ROOT}" "${TEST_TARGET}"

if [[ ! -f "${ISSUES_PATH}" ]]; then
    printf 'bead_id=%s level=ERROR case=missing_issues_jsonl path=%s\n' "${BEAD_ID}" "${ISSUES_PATH}"
    exit 1
fi

description="$(
    jq -r '
        select(.id == "bd-bca.1")
        | .description,
          (.comments[]?.text // empty)
    ' "${ISSUES_PATH}" | tr '\n' ' '
)"

if [[ -z "${description// }" ]]; then
    printf 'bead_id=%s level=ERROR case=missing_bead_description path=%s\n' "${BEAD_ID}" "${ISSUES_PATH}"
    exit 1
fi

required_tokens=(
    "test_bd_bca_1_unit_compliance_gate"
    "prop_bd_bca_1_structure_compliance"
    "test_e2e_bd_bca_1"
    "test_e2e_bd_bca_1_compliance"
    "DEBUG"
    "INFO"
    "WARN"
    "ERROR"
    "bd-1fpm"
)

declare -a missing_tokens=()
for token in "${required_tokens[@]}"; do
    if ! rg -Fq "${token}" <<<"${description}"; then
        missing_tokens+=("${token}")
    fi
done

printf \
    'bead_id=%s level=INFO case=description_scan required=%s missing=%s\n' \
    "${BEAD_ID}" "${#required_tokens[@]}" "${#missing_tokens[@]}"

if [[ "${#missing_tokens[@]}" -gt 0 ]]; then
    printf 'bead_id=%s level=WARN case=degraded_mode_count=%s reference=bd-1fpm\n' \
        "${BEAD_ID}" "${#missing_tokens[@]}"
    printf 'bead_id=%s level=ERROR case=missing_tokens items=%s\n' \
        "${BEAD_ID}" "${missing_tokens[*]}"
    exit 1
fi

if (cd "${WORKSPACE_ROOT}" && cargo test -p fsqlite-harness --test "${TEST_TARGET}" -- --nocapture); then
    printf 'bead_id=%s level=WARN case=degraded_mode_count=0 reference=bd-1fpm\n' "${BEAD_ID}"
    printf 'bead_id=%s level=ERROR case=terminal_failure_count=0 reference=bd-1fpm\n' "${BEAD_ID}"
    printf 'bead_id=%s level=INFO case=pass\n' "${BEAD_ID}"
    exit 0
fi

printf 'bead_id=%s level=WARN case=degraded_mode_count=1 reference=bd-1fpm\n' "${BEAD_ID}"
printf 'bead_id=%s level=ERROR case=harness_test_failed target=%s\n' "${BEAD_ID}" "${TEST_TARGET}"
exit 1
