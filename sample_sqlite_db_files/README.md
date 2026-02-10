# sample_sqlite_db_files

Local-only corpus of **real SQLite database files** used for end-to-end demos and benchmarking.

## Safety Rules

- NEVER run tests or demos against `/dp/...` originals.
- Always take a consistent snapshot using SQLite's backup API (`sqlite3 ... ".backup '...'"`).
- Treat `golden/` as immutable. Do work on copies in `working/`.

## What Goes Where

- `golden/`: immutable golden copies created from `/dp` sources (directory ignored by git).
- `working/`: ephemeral mutable copies created per run (directory ignored by git).
- `metadata/`: tracked JSON/markdown describing each golden DB (schema summary, stats, etc.).
- `checksums.sha256`: tracked canonical checksum file for golden DBs (populated by a later task).

## Corpus Manifest (Schema + Conventions)

This corpus is intended to be self-describing and reproducible.

Source-of-truth artifacts:
- `checksums.sha256`: sha256 for every file in `golden/` (used to ensure golden bytes never change).
- `metadata/*.json`: per-DB metadata/provenance captured read-only from the source and/or golden copy.

Optional (recommended) manifest:
- JSON Schema: `manifests/manifest.v1.schema.json`
- Manifest file (when maintained): `manifests/manifest.v1.json`

The manifest exists so the harness can select fixtures by a stable `db_id`, and so future us
doesn't have to remember where each DB came from, what sidecars existed at capture time, or
whether a DB might contain secrets/PII.

## Snapshot Copy (Recommended)

Prefer `.backup` over `cp` because some source DBs may have active WAL/SHM files.

Example:

```bash
src="/dp/asupersync/.beads/beads.db"
dst="sample_sqlite_db_files/golden/asupersync.db"
sqlite3 "$src" ".backup '$dst'"
sqlite3 "$dst" "PRAGMA integrity_check;"
```

## How The Harness Uses This Corpus

- The Rust E2E harness (`crates/fsqlite-e2e`) treats `golden/` as immutable inputs.
- Each run creates a fresh working copy under `working/` (or a per-run scratch dir) and operates only on that copy.
- Verification gates check:
  - `PRAGMA integrity_check` on golden files
  - sha256 of golden files vs `checksums.sha256`

## Git Hygiene

This repo must never commit DB bytes from `/dp`. Only metadata + checksums are tracked.
