# Fixture Ingestion and Safety Policy

This document describes how to safely ingest SQLite database files into the
FrankenSQLite E2E corpus, how the golden/working copy system works, and how
SHA-256 provenance tracking prevents silent data corruption.

## Safety Policy

**Rule 1: Never touch `/dp/` originals.**
Source databases under `/dp/` are live project databases.  Running queries,
opening WAL connections, or even `cp` during active writes can corrupt them.
Always use SQLite's `.backup` API to produce a consistent snapshot.

**Rule 2: Treat `golden/` as immutable.**
Once a golden copy is captured and its checksum recorded, the file must not be
modified.  If a database needs updating, ingest a fresh copy under a new
filename or re-run the full ingestion pipeline.

**Rule 3: Never commit database binaries to git.**
The `.gitignore` in this directory blocks `*.db`, `*.db-wal`, `*.db-shm`, and
`*.db-journal`.  Only metadata, checksums, manifests, and documentation are
tracked.  This prevents the repository from bloating with multi-megabyte binary
files.

**Rule 4: Verify integrity immediately after capture.**
Every newly ingested golden copy must pass `PRAGMA integrity_check` before being
used in any test or benchmark.

**Rule 5: Keep fixtures CI-friendly (size caps).**
Discovery/import defaults to a **512 MiB** size cap to keep scans and CI
reasonable. Override with `--max-file-size-mib <N>` only when you have a
compelling reason (use `0` to disable the cap; not recommended).

**Rule 6: Use stable tags.**
`realdb-e2e corpus import --tag <TAG>` only accepts a small stable tag set
(`asupersync`, `frankentui`, `flywheel`, `frankensqlite`, `agent-mail`, `beads`,
`misc`) so fixture selection and reporting stays predictable.

## Directory Layout

```
sample_sqlite_db_files/
  golden/           # Immutable golden copies (git-ignored *.db files)
  working/          # Ephemeral per-run copies (git-ignored, recreated each run)
  metadata/         # Per-DB JSON metadata files (git-tracked)
  manifests/        # Corpus manifest + JSON Schema (git-tracked)
  checksums.sha256  # SHA-256 checksums for all golden files (git-tracked)
  README.md         # Quick overview (git-tracked)
  FIXTURES.md       # This file (git-tracked)
```

### Golden vs Working Copies

| Aspect | `golden/` | `working/` |
|--------|-----------|------------|
| Purpose | Immutable reference snapshots | Mutable scratch copies for test runs |
| Lifetime | Permanent (until re-ingested) | Ephemeral (deleted after each run) |
| Modified by tests? | Never | Yes |
| Git-tracked? | No (only checksums) | No |
| Created by | Manual ingestion (see below) | E2E harness automatically |

The E2E harness copies a golden file into `working/` (or a per-run temp dir)
before each test.  This guarantees that golden files remain untouched even if a
test crashes mid-write.

## Ingesting a New Fixture

### Step 1: Identify the Source

Source databases live under `/dp/`.  Common locations:

```
/dp/asupersync/.beads/beads.db
/dp/frankentui/.beads/beads.db
/dp/brenner_bot/brenner_bot.db
```

### Step 2: Create a Consistent Snapshot

Use SQLite's backup API to capture a consistent snapshot.  This safely
checkpoints any WAL data into the main file:

```bash
SRC="/dp/asupersync/.beads/beads.db"
DST="sample_sqlite_db_files/golden/asupersync.db"

sqlite3 "$SRC" ".backup '$DST'"
```

Do **not** use `cp` or `rsync` -- if the source has an active WAL or journal,
a raw file copy may produce a corrupt database.

**Preferred (one command):** use `realdb-e2e corpus import`, which performs the
backup, runs `PRAGMA integrity_check`, updates `checksums.sha256`, and writes
metadata:

```bash
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus import \
  --db /dp/asupersync/.beads/beads.db --tag asupersync
```

### Step 3: Verify Integrity

```bash
sqlite3 "$DST" "PRAGMA integrity_check;"
# Expected output: ok
```

If the check fails, the source may have been corrupted or the backup was
interrupted.  Discard the file and retry.

### Step 4: Record the SHA-256 Checksum

If you used `realdb-e2e corpus import`, this is done automatically and the tool
will refuse to overwrite an existing checksum entry (golden files are
immutable).

```bash
sha256sum "$DST" | awk '{print $1 "  " FILENAME}' FILENAME="$(basename "$DST")" \
  >> sample_sqlite_db_files/checksums.sha256
```

Or regenerate the entire checksum file:

```bash
cd sample_sqlite_db_files/golden
sha256sum *.db | sort -k2 > ../checksums.sha256
```

### Step 5: Capture Metadata

If you used `realdb-e2e corpus import`, this is done automatically (and the
metadata is intentionally schema/PRAGMA-focused: no row contents).

Metadata lives under `sample_sqlite_db_files/metadata/<db_id>.json` and follows
the stable schema `FixtureMetadataV1` (see `crates/fsqlite-e2e/src/fixture_metadata.rs`).

To (re)generate metadata for all golden DBs (deterministic JSON, best-effort
schema summaries, derived feature flags):

```bash
cargo run -p fsqlite-e2e --bin profile-db -- --pretty
```

At minimum, each metadata JSON should include:
- `schema_version` (metadata schema version)
- `db_id`
- `golden_filename`
- `sha256_golden` + `size_bytes`
- `sidecars_present` (e.g. `-wal`, `-shm`, `-journal`) when observed
- `sqlite_meta.page_size` (plus other PRAGMAs as best-effort)

See `metadata/README.md` for the full recommended field set and safety rules.

### Step 6: Update the Manifest (Optional)

If maintained, `manifests/manifest.v1.json` should stay consistent with:
- `checksums.sha256` (filenames + `sha256_golden`)
- `metadata/*.json` (at minimum `size_bytes` + `sqlite_meta.page_size`)

Preferred: generate it deterministically from `checksums.sha256` + `metadata/*.json`:

```bash
cargo run -p fsqlite-e2e --bin profile-db -- --manifest-only
```

If you need to edit it by hand, follow the schema in `manifests/manifest.v1.schema.json`.
Required fields:

```json
{
  "db_id": "asupersync",
  "golden_filename": "asupersync.db",
  "sha256_golden": "<64-char hex>",
  "size_bytes": 12345678
}
```

## Removing a Fixture

1. Delete the golden file: `rm sample_sqlite_db_files/golden/<name>.db`
2. Remove its line from `checksums.sha256`
3. Delete its metadata: `rm sample_sqlite_db_files/metadata/<name>.json`
4. Remove its entry from `manifests/manifest.v1.json` (if maintained)
5. Commit the metadata/checksum changes

The provenance chain (checksum + metadata JSON) is preserved in git history even
after removal, so the fixture can be re-ingested later if needed.

## SHA-256 Provenance Chain

The provenance chain provides three guarantees:

1. **Immutability**: `checksums.sha256` (git-tracked) records the expected hash
   of every golden file.  The harness verifies these before each run.

2. **Reproducibility**: `metadata/<db_id>.json` records the source path, page
   size, schema, and other metadata needed to re-create the golden copy from
   the same source.

3. **Auditability**: Git history preserves the complete timeline of when
   fixtures were added, updated, or removed.

### How the Harness Verifies Provenance

The Rust E2E harness (`crates/fsqlite-e2e`) performs these checks before any
test or benchmark run:

```
golden/*.db  →  sha256sum  →  compare with checksums.sha256
                           →  PRAGMA integrity_check
                           →  load metadata from metadata/<db_id>.json
```

If any check fails, the run aborts with a clear diagnostic message.  This
prevents tests from silently running against corrupted or stale fixtures.

### Re-verifying All Checksums

```bash
cd sample_sqlite_db_files
while IFS='  ' read -r expected name; do
  actual=$(sha256sum "golden/$name" | awk '{print $1}')
  if [ "$actual" != "$expected" ]; then
    echo "MISMATCH: $name (expected $expected, got $actual)"
  else
    echo "OK: $name"
  fi
done < checksums.sha256
```

## Quick Start: Ingest and Smoke Test

```bash
# 1. Ingest a fixture (backup + integrity_check + checksums + metadata)
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus import \
  --db /dp/frankensqlite/.beads/beads.db --tag frankensqlite

# 2. Verify golden checksums
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus verify

# 3. Run a simple workload against C SQLite (working copy)
cargo run -p fsqlite-e2e --bin realdb-e2e -- run \
  --engine sqlite3 --db frankensqlite --workload commutative_inserts --concurrency 1
```

A new contributor who follows these four steps will have a working fixture corpus
and can run the full E2E suite.

## Inclusion Policy

### Allowed Roots

The default discovery root is `/dp/`.  Override with `--root` on the CLI.
Additional roots may be added but each source directory must be explicitly
opted-in; recursive scans never escape the configured root.

### File Extensions

Discovery considers files with these extensions: `.db`, `.sqlite`, `.sqlite3`.
Other extensions are silently ignored unless the file passes a SQLite magic
header check and `--allow-bad-header` is used during import.

### Size Thresholds

| Threshold | Value | Behavior |
|-----------|-------|----------|
| Soft cap (discovery) | 512 MiB | Files larger than this are skipped during `corpus scan`. |
| Hard cap (import) | 512 MiB | `corpus import` refuses files larger than this unless overridden. |
| CI-friendly subset | < 20 MiB | Tests that need fast turnaround should filter on the `small` or `medium` tags. |

To override the cap: pass `--max-file-size-mib <N>` (use `0` to disable; not recommended).

### WAL/SHM/Journal Sidecars

- **At capture time**: The backup API checkpoints WAL data into the main file.
  The golden `.db` is self-contained.
- **Sidecar copies**: `-wal`, `-shm`, and `-journal` sidecars from the source
  are copied alongside the golden file for reference only (these are git-ignored).
  They are not required for tests; the harness uses the main `.db` file.
- **Active writers**: If the source is actively written during backup, the
  backup API serializes the snapshot.  No manual locking is needed.

### Exclusion Rules

Skip a database if any of these apply:

- `PRAGMA integrity_check` fails on the source (even after retry)
- The file contains known PII (see sensitivity rules below)
- The file is a temporary/cache database with no stable schema
- The file is a WAL-only database with no useful data (empty tables)

## Tagging Taxonomy

Fixture selection and reporting use the `tags` array stored in `metadata/<db_id>.json`
(see `FixtureMetadataV1.tags` in `crates/fsqlite-e2e/src/fixture_metadata.rs`).

- `realdb-e2e corpus import --tag <TAG>` adds one stable classification tag (small allowlist).
- The ingestion/profiling pipeline may also add derived tags (e.g. size bucket `small|medium|large`,
  `wal`, or other feature tags) for convenience.
- `realdb-e2e corpus scan` emits heuristic tags in its discovery JSON output; treat these as hints
  unless your pipeline persists them into `metadata/*.json`.

### Stable Classification Tags

| Tag | When to use |
|-----|-------------|
| `asupersync` | Fixtures sourced from `/dp/asupersync/...` |
| `frankentui` | Fixtures sourced from `/dp/frankentui/...` |
| `flywheel` | Fixtures sourced from `/dp/flywheel_*/*` (connectors/gateway/etc.) |
| `frankensqlite` | Fixtures sourced from `/dp/frankensqlite/...` |
| `agent-mail` | Fixtures sourced from Agent Mail tooling databases |
| `beads` | Beads issue tracker DBs (`.beads/beads.db`) when a project tag is not needed |
| `misc` | Anything that doesn’t fit cleanly above (prefer adding a stable tag over time) |

### Discovery Tags (Auto-assigned)

Discovery tags are best-effort hints. Current heuristics include:

- Project name tags (e.g., `asupersync`, `flywheel`, `frankensqlite`, `frankentui`)
- `beads`, `cache`, `sample`, `test`
- Size buckets: `small` (< 64 KiB), `medium` (64 KiB–4 MiB), `large` (> 4 MiB)

### Notes

- `journal_mode` is recorded in metadata (`sqlite_meta.journal_mode`). We also commonly include
  `wal` as a derived tag for selection convenience.
- Tag-based selection for `run/bench/corrupt` can be built on top of metadata/manifest
  (future work); today the CLI prints tags during scan/import.

## Sensitivity and PII Policy

### Metadata Rules

Metadata JSON (`metadata/*.json`) is git-tracked and therefore **public within
the repo**.  The following rules apply:

**Allowed in metadata:**
- Schema structure: table names, column names, column types, constraints
- Aggregate statistics: row counts, page counts, file size
- PRAGMA values: page_size, journal_mode, user_version, application_id
- Index and trigger names
- Source path (optional; may be omitted/null for redaction safety)

**Forbidden in metadata:**
- Row contents or sample data
- Column value distributions or histograms
- Query results or query logs
- Any field that could leak secrets, tokens, or credentials

### PII Assessment

Before ingesting any fixture, assess PII risk:

| Risk Level | Meaning | Action |
|------------|---------|--------|
| `unlikely` | Internal dev tool, no user data (beads, flywheel) | Ingest freely |
| `possible` | Contains project names or author fields | Review schema, ingest if safe |
| `likely` | Contains emails, tokens, user content | **Do not ingest** |
| `unknown` | Not yet assessed | Treat as `possible` until reviewed |

Set `safety.pii_risk` (and `safety.secrets_risk` / `safety.allowed_for_ci`) in fixture metadata
(via `realdb-e2e corpus import` flags) to document the assessment.

### Existing Corpus Assessment

The current corpus consists of internal development tool databases
(beads, flywheel, session search, agent tools).  These contain only project
metadata (issue titles, timestamps, schema DDL) and are assessed as `unlikely`
PII risk.  No databases containing user-facing data, credentials, or personal
information have been ingested.

## Concrete Examples

### Example 1: Beads Database (asupersync)

```
Source:  /dp/asupersync/.beads/beads.db
db_id:   asupersync
Tags:    (manual) asupersync; (discovery) beads, large
Tables:  issues, comments, dependencies, events, labels, ...
PII:     unlikely (issue tracker metadata only)
```

Import command:
```bash
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus import \
  --db /dp/asupersync/.beads/beads.db --tag asupersync
```

### Example 2: WAL-Mode DB With Sidecars (flywheel_gateway)

```
Source:  /dp/flywheel_gateway/data/gateway.db
db_id:   flywheel_gateway
Tags:    (manual) flywheel; (discovery) flywheel, large
Sidecars present at capture: gateway.db-wal, gateway.db-shm
PII:     unlikely (internal tooling metadata)
```

Import command:
```bash
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus import \
  --db /dp/flywheel_gateway/data/gateway.db --tag flywheel
```

### Example 3: Larger Beads DB (flywheel_connectors)

```
Source:  /dp/flywheel_connectors/.beads/beads.db
db_id:   flywheel_connectors
Tags:    (manual) flywheel; (discovery) flywheel, beads, large
PII:     unlikely (internal project metadata)
```

Import command:
```bash
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus import \
  --db /dp/flywheel_connectors/.beads/beads.db --tag flywheel
```
