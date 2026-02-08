#!/bin/bash
export CLOUDFLARE_ACCOUNT_ID="d3680880006806000000000000000000" # Placeholder, actual ID should be in env
npx wrangler pages deploy dist --project-name frankensqlite-spec-evolution --branch main --commit-dirty=true
