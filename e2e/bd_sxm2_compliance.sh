#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-sxm2"
SPEC_PATH="COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md"

if [[ ! -f "${SPEC_PATH}" ]]; then
    printf 'bead_id=%s level=ERROR case=missing_spec path=%s\n' "${BEAD_ID}" "${SPEC_PATH}"
    exit 1
fi

mapfile -t EXPECTED_CRATES <<'EOF'
fsqlite-ast
fsqlite-btree
fsqlite-cli
fsqlite-core
fsqlite-error
fsqlite-ext-fts3
fsqlite-ext-fts5
fsqlite-ext-icu
fsqlite-ext-json
fsqlite-ext-misc
fsqlite-ext-rtree
fsqlite-ext-session
fsqlite-func
fsqlite-harness
fsqlite-mvcc
fsqlite-pager
fsqlite-parser
fsqlite-planner
fsqlite-types
fsqlite-vdbe
fsqlite-vfs
fsqlite-wal
fsqlite
EOF

is_concise_allowed() {
    local crate_name="$1"
    [[ "${crate_name}" == fsqlite-ext-* ]] \
        || [[ "${crate_name}" == "fsqlite" ]] \
        || [[ "${crate_name}" == "fsqlite-cli" ]] \
        || [[ "${crate_name}" == "fsqlite-harness" ]] \
        || [[ "${crate_name}" == "fsqlite-error" ]]
}

extract_section_8_3() {
    awk '
        /^### 8\.3 Per-Crate Detailed Descriptions/ { in_section = 1; next }
        /^### 8\.4 Dependency Edges with Rationale/ { in_section = 0 }
        in_section { print }
    ' "${SPEC_PATH}"
}

extract_section_8_4() {
    awk '
        /^### 8\.4 Dependency Edges with Rationale/ { in_section = 1; next }
        /^### 8\.5 Feature Flags/ { in_section = 0 }
        in_section { print }
    ' "${SPEC_PATH}"
}

extract_crate_block() {
    local section="$1"
    local crate_name="$2"
    local marker="**\`${crate_name}\`**"
    printf '%s\n' "${section}" | awk -v marker="${marker}" '
        index($0, marker) > 0 { in_block = 1; next }
        in_block && $0 ~ /^\*\*`/ { exit }
        in_block { print }
    '
}

section_8_3="$(extract_section_8_3)"
section_8_4="$(extract_section_8_4)"

if [[ -z "${section_8_3}" || -z "${section_8_4}" ]]; then
    printf 'bead_id=%s level=ERROR case=missing_sections spec=%s\n' "${BEAD_ID}" "${SPEC_PATH}"
    exit 1
fi

printf 'bead_id=%s level=DEBUG case=start expected_crates=%s spec=%s\n' \
    "${BEAD_ID}" "${#EXPECTED_CRATES[@]}" "${SPEC_PATH}"

declare -a violations=()
described_count=0
module_listed_count=0

for crate_name in "${EXPECTED_CRATES[@]}"; do
    marker="**\`${crate_name}\`**"
    marker_count="$(printf '%s\n' "${section_8_3}" | rg -F "${marker}" -c || true)"

    if [[ "${marker_count}" -ne 1 ]]; then
        violations+=("description_marker_count:${crate_name}:${marker_count}")
        continue
    fi

    described_count=$((described_count + 1))
    block="$(extract_crate_block "${section_8_3}" "${crate_name}")"
    block_len="$(printf '%s' "${block}" | wc -c | awk '{print $1}')"

    if [[ "${block_len}" -lt 70 ]]; then
        violations+=("description_too_short:${crate_name}:${block_len}")
    fi

    first_nonempty_line="$(printf '%s\n' "${block}" | awk 'NF { print; exit }')"
    if [[ -z "${first_nonempty_line}" ]]; then
        violations+=("summary_line_missing:${crate_name}")
    fi

    has_modules=0
    if printf '%s\n' "${block}" | rg -qi 'modules:'; then
        has_modules=1
        module_listed_count=$((module_listed_count + 1))
        module_line_count="$(printf '%s\n' "${block}" | rg -c '^- `[^`]+\.rs`' || true)"
        if [[ "${module_line_count}" -lt 3 || "${module_line_count}" -gt 12 ]]; then
            violations+=("module_count_out_of_range:${crate_name}:${module_line_count}")
        fi
    fi

    has_dependency_signal=0
    if printf '%s\n' "${block}" | rg -q 'Dependency rationale:|depends on'; then
        has_dependency_signal=1
    elif printf '%s\n' "${section_8_4}" | rg -q "${crate_name}"; then
        has_dependency_signal=1
    elif is_concise_allowed "${crate_name}"; then
        has_dependency_signal=1
    fi

    if [[ "${has_dependency_signal}" -eq 0 ]]; then
        violations+=("dependency_direction_missing:${crate_name}")
    fi

    printf \
        'bead_id=%s level=DEBUG case=crate_scan crate=%s described=1 modules=%s block_len=%s\n' \
        "${BEAD_ID}" "${crate_name}" "${has_modules}" "${block_len}"
done

printf \
    'bead_id=%s level=INFO case=summary described_count=%s module_listed_count=%s expected=%s\n' \
    "${BEAD_ID}" "${described_count}" "${module_listed_count}" "${#EXPECTED_CRATES[@]}"

if [[ "${#violations[@]}" -gt 0 ]]; then
    printf 'bead_id=%s level=WARN case=degraded_mode_count=%s reference=bd-1fpm\n' \
        "${BEAD_ID}" "${#violations[@]}"
    printf 'bead_id=%s level=ERROR case=violations items=%s\n' "${BEAD_ID}" "${violations[*]}"
    exit 1
fi

printf 'bead_id=%s level=WARN case=degraded_mode_count=0 reference=bd-1fpm\n' "${BEAD_ID}"
printf 'bead_id=%s level=ERROR case=terminal_failure_count=0 reference=bd-1fpm\n' "${BEAD_ID}"
printf 'bead_id=%s level=INFO case=pass\n' "${BEAD_ID}"
