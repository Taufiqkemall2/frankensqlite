#!/bin/bash
set -euo pipefail

# Optional: set CLOUDFLARE_ACCOUNT_ID in the environment to force a specific account.
# If unset, wrangler will use the default account associated with your login/token.

CANONICAL_DOMAIN="https://frankensqlite.com"
MIRROR_DOMAINS=("https://www.frankensqlite.com" "https://frankensqlite-spec-evolution.pages.dev")
SQLITE_FILE="spec_evolution_v1.sqlite3"
EXPECTED_DB_URL="$CANONICAL_DOMAIN/$SQLITE_FILE"
PROJECT_NAME="frankensqlite-spec-evolution"

# Ensure dist exists and is populated
mkdir -p dist
cp visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html dist/index.html
cp visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html dist/spec_evolution.html
cp spec_evolution_v1.sqlite3 dist/
cp spec_evolution_v1.sqlite3.config.json dist/
cp og-image.png dist/
cp twitter-image.png dist/
cp frankensqlite_illustration.webp dist/
cp frankensqlite_diagram.webp dist/
cp _headers dist/
cp _routes.json dist/

# Deploy to Cloudflare Pages
echo "Deploying to Cloudflare Pages..."
npx wrangler pages deploy dist --project-name "$PROJECT_NAME" --commit-dirty=true

# Post-deployment verification
echo ""
echo "Verifying deployment..."
sleep 5  # Give CDN time to propagate

assert_local_contract() {
    local page_file="$1"
    if ! rg -q 'const CANONICAL_ORIGIN = "https://frankensqlite.com";' "$page_file"; then
        echo "  ERROR: $page_file is missing CANONICAL_ORIGIN pin to $CANONICAL_DOMAIN"
        return 1
    fi
    if ! rg -q 'const DB_FILENAME = "spec_evolution_v1.sqlite3";' "$page_file"; then
        echo "  ERROR: $page_file is missing DB filename pin to $SQLITE_FILE"
        return 1
    fi
    if ! rg -q 'const DB_URL = `\$\{CANONICAL_ORIGIN\}/\$\{DB_FILENAME\}`;' "$page_file"; then
        echo "  ERROR: $page_file does not compose DB_URL from canonical constants"
        return 1
    fi
    if rg -q 'const DB_URL = ".*\\?.*";' "$page_file"; then
        echo "  ERROR: $page_file contains a query-string DB_URL, which can be routed to HTML fallback."
        return 1
    fi
    return 0
}

check_sqlite_once() {
    local url="$1"
    local content_type
    local magic

    content_type=$(curl -sSI "$url" | awk 'BEGIN{IGNORECASE=1} /^content-type:/ {print $2; exit}' | tr -d '\r')
    magic=$(curl -fsSL "$url" 2>/dev/null | head -c 15 || true)

    if [[ "$content_type" == "application/octet-stream" ]] && [[ "$magic" == "SQLite format 3" ]]; then
        return 0
    fi
    echo "  WARN: $url -> Content-Type=$content_type, Magic='$magic'"
    return 1
}

verify_sqlite() {
    local url="$1"
    local max_retries=5
    local retry=0

    while [ "$retry" -lt "$max_retries" ]; do
        echo "  Checking $url (attempt $((retry + 1))/$max_retries)..."
        if check_sqlite_once "$url"; then
            echo "  OK: SQLite bytes verified"
            return 0
        fi
        retry=$((retry + 1))
        sleep 3
    done
    return 1
}

extract_db_url_from_html() {
    sed -n 's/^[[:space:]]*const DB_URL = "\(.*\)";/\1/p' | head -n 1
}

extract_const_string() {
    local key="$1"
    sed -n "s/^[[:space:]]*const $key = \"\\([^\"]*\\)\";.*/\\1/p" | head -n 1
}

resolve_db_url() {
    local origin="$1"
    local db_url="$2"
    if [[ "$db_url" =~ ^https?:// ]]; then
        printf '%s' "$db_url"
    else
        printf '%s/%s' "$origin" "${db_url#/}"
    fi
}

verify_viewer_contract() {
    local origin="$1"
    local page_url="$origin/spec_evolution"
    local max_retries=5
    local retry=0

    while [ "$retry" -lt "$max_retries" ]; do
        echo "  Checking viewer contract at $page_url (attempt $((retry + 1))/$max_retries)..."
        local html
        local canonical_origin
        local db_filename
        local db_url
        local resolved_db_url
        html=$(curl -fsSL "$page_url" || true)
        canonical_origin=$(printf '%s' "$html" | extract_const_string "CANONICAL_ORIGIN")
        db_filename=$(printf '%s' "$html" | extract_const_string "DB_FILENAME")
        if [[ -n "$canonical_origin" ]] && [[ -n "$db_filename" ]]; then
            resolved_db_url="${canonical_origin%/}/${db_filename#/}"
        else
            db_url=$(printf '%s' "$html" | extract_db_url_from_html)
            if [[ -z "$db_url" ]]; then
                echo "  WARN: Could not extract DB URL contract from $page_url"
                retry=$((retry + 1))
                sleep 3
                continue
            fi
            resolved_db_url=$(resolve_db_url "$origin" "$db_url")
            if [[ "$db_url" == *"?"* ]]; then
                echo "  WARN: DB_URL includes query string ($db_url), which can break SQLite fetches."
                retry=$((retry + 1))
                sleep 3
                continue
            fi
        fi
        if [[ "$resolved_db_url" == *"?"* ]]; then
            echo "  WARN: Resolved DB URL includes query string ($resolved_db_url), which can break SQLite fetches."
            retry=$((retry + 1))
            sleep 3
            continue
        fi
        if [[ "$resolved_db_url" != "$EXPECTED_DB_URL" ]]; then
            echo "  WARN: DB_URL resolves to $resolved_db_url (expected $EXPECTED_DB_URL)"
            retry=$((retry + 1))
            sleep 3
            continue
        fi
        if verify_sqlite "$resolved_db_url"; then
            echo "  OK: Viewer on $origin references a working canonical DB URL"
            return 0
        fi
        retry=$((retry + 1))
        sleep 3
    done
    return 1
}

if ! assert_local_contract "dist/index.html"; then
    echo ""
    echo "DEPLOYMENT VERIFICATION FAILED!"
    exit 1
fi

if ! verify_sqlite "$EXPECTED_DB_URL"; then
    echo ""
    echo "DEPLOYMENT VERIFICATION FAILED!"
    echo "Canonical SQLite endpoint is not serving valid bytes."
    exit 1
fi

verification_failed=0
for origin in "$CANONICAL_DOMAIN" "${MIRROR_DOMAINS[@]}"; do
    if ! verify_viewer_contract "$origin"; then
        verification_failed=1
    fi
done

if [ "$verification_failed" -ne 0 ]; then
    echo ""
    echo "DEPLOYMENT VERIFICATION FAILED!"
    echo "At least one public host serves a viewer that does not resolve to a valid canonical SQLite URL."
    exit 1
fi

echo ""
echo "Deployment verified successfully!"
exit 0
