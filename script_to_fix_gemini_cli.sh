#!/usr/bin/env bash
set -euo pipefail

# Fix Gemini CLI EBADF crash in node-pty resize
# Bug: ioctl(2) on a closed PTY fd throws Error with message "EBADF"
#       but the catch blocks only check err.code (which is undefined for
#       native addon errors) — so it falls through and crashes.
# Fix:  Add err.message?.includes('EBADF') checks to both catch sites.

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
RESET='\033[0m'

info()  { printf "${CYAN}[info]${RESET}  %s\n" "$*"; }
ok()    { printf "${GREEN}[ok]${RESET}    %s\n" "$*"; }
warn()  { printf "${YELLOW}[warn]${RESET}  %s\n" "$*"; }
fail()  { printf "${RED}[fail]${RESET}  %s\n" "$*"; exit 1; }

# --- Locate the Gemini CLI installation ---

find_gemini_root() {
    local candidates=()

    # bun global
    candidates+=("$HOME/.bun/install/global/node_modules/@google/gemini-cli")
    # npm global (Linux/macOS)
    local npm_root
    if npm_root="$(npm root -g 2>/dev/null)"; then
        candidates+=("$npm_root/@google/gemini-cli")
    fi
    # yarn global
    local yarn_root
    if yarn_root="$(yarn global dir 2>/dev/null)"; then
        candidates+=("$yarn_root/node_modules/@google/gemini-cli")
    fi
    # pnpm global
    local pnpm_root
    if pnpm_root="$(pnpm root -g 2>/dev/null)"; then
        candidates+=("$pnpm_root/@google/gemini-cli")
    fi
    # Homebrew (macOS)
    if command -v brew &>/dev/null; then
        local brew_prefix
        brew_prefix="$(brew --prefix 2>/dev/null)"
        candidates+=("$brew_prefix/lib/node_modules/@google/gemini-cli")
    fi

    for dir in "${candidates[@]}"; do
        if [[ -d "$dir/dist" ]]; then
            echo "$dir"
            return 0
        fi
    done
    return 1
}

GEMINI_ROOT=""
if ! GEMINI_ROOT="$(find_gemini_root)"; then
    fail "Could not find @google/gemini-cli installation. Is it installed?"
fi
info "Found Gemini CLI at: $GEMINI_ROOT"

# --- Patch targets ---

SHELL_SVC="$GEMINI_ROOT/../gemini-cli-core/dist/src/services/shellExecutionService.js"
APP_CONTAINER="$GEMINI_ROOT/dist/src/ui/AppContainer.js"

# Resolve symlinks / relative paths
SHELL_SVC="$(realpath "$SHELL_SVC" 2>/dev/null || readlink -f "$SHELL_SVC" 2>/dev/null || echo "$SHELL_SVC")"
APP_CONTAINER="$(realpath "$APP_CONTAINER" 2>/dev/null || readlink -f "$APP_CONTAINER" 2>/dev/null || echo "$APP_CONTAINER")"

[[ -f "$SHELL_SVC" ]]    || fail "shellExecutionService.js not found at: $SHELL_SVC"
[[ -f "$APP_CONTAINER" ]] || fail "AppContainer.js not found at: $APP_CONTAINER"

# --- Idempotent patch helper ---
# Uses node for cross-platform string replacement (no sed portability issues)

patch_file() {
    local file="$1"
    local check_string="$2"
    local old_string="$3"
    local new_string="$4"
    local label="$5"

    # Idempotency: skip if already patched
    if grep -qF "$check_string" "$file" 2>/dev/null; then
        ok "$label — already patched, skipping"
        return 0
    fi

    # Verify the original code is present
    if ! grep -qF "$old_string" "$file" 2>/dev/null; then
        warn "$label — original pattern not found (different version?), skipping"
        return 1
    fi

    # Apply patch using node (portable across macOS/Linux/Windows-WSL)
    node -e "
        const fs = require('fs');
        const file = process.argv[1];
        const old_str = process.argv[2];
        const new_str = process.argv[3];
        let content = fs.readFileSync(file, 'utf8');
        if (!content.includes(old_str)) {
            process.exit(2);
        }
        content = content.replace(old_str, new_str);
        fs.writeFileSync(file, content, 'utf8');
    " "$file" "$old_string" "$new_string"

    local rc=$?
    if [[ $rc -eq 0 ]]; then
        ok "$label — patched successfully"
    else
        fail "$label — patch failed (exit $rc)"
    fi
}

# --- Patch 1: shellExecutionService.js ---
# The catch block checks err.code === 'ESRCH' but the native pty addon
# throws Error({ message: "ioctl(2) failed, EBADF" }) with no .code property.

info "Patching shellExecutionService.js ..."
patch_file \
    "$SHELL_SVC" \
    "err.message?.includes('EBADF')" \
    "const isEsrch = err.code === 'ESRCH';
                const isWindowsPtyError = err.message?.includes('Cannot resize a pty that has already exited');
                if (isEsrch || isWindowsPtyError) {
                    // On Unix, we get an ESRCH error.
                    // On Windows, we get a message-based error.
                    // In both cases, it's safe to ignore." \
    "const isEsrch = err.code === 'ESRCH';
                const isEbadf = err.code === 'EBADF' || err.message?.includes('EBADF');
                const isWindowsPtyError = err.message?.includes('Cannot resize a pty that has already exited');
                if (isEsrch || isEbadf || isWindowsPtyError) {
                    // On Unix, we get ESRCH (process gone) or EBADF (fd already closed).
                    // Native pty addon throws Error with 'EBADF' in message (no .code).
                    // On Windows, we get a message-based error.
                    // In all cases, it's safe to ignore." \
    "shellExecutionService.js EBADF catch"

# --- Patch 2: AppContainer.js ---
# The React useEffect catch only checks for the Windows error message string.

info "Patching AppContainer.js ..."
patch_file \
    "$APP_CONTAINER" \
    "e.message.includes('EBADF')" \
    "if (!(e instanceof Error &&
                    e.message.includes('Cannot resize a pty that has already exited'))) {
                    throw e;
                }" \
    "if (!(e instanceof Error &&
                    (e.message.includes('Cannot resize a pty that has already exited') ||
                     e.message.includes('EBADF') ||
                     e.code === 'EBADF' ||
                     e.code === 'ESRCH'))) {
                    throw e;
                }" \
    "AppContainer.js EBADF catch"

# --- Done ---

echo ""
ok "All patches applied. Gemini CLI EBADF resize crash should be fixed."
info "Note: patches live in node_modules and will be overwritten by 'bun update -g @google/gemini-cli'."
info "Re-run this script after any Gemini CLI update."
