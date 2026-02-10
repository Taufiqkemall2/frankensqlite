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

### Step 3: Verify Integrity

```bash
sqlite3 "$DST" "PRAGMA integrity_check;"
# Expected output: ok
```

If the check fails, the source may have been corrupted or the backup was
interrupted.  Discard the file and retry.

### Step 4: Record the SHA-256 Checksum

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

Create a JSON metadata file under `metadata/`:

```bash
DB="sample_sqlite_db_files/golden/asupersync.db"
ID="asupersync"

# Quick metadata capture (adapt as needed):
sqlite3 "$DB" <<'SQL'
.mode json
SELECT
  'asupersync' AS name,
  (SELECT page_size FROM pragma_page_size()) AS page_size,
  (SELECT page_count FROM pragma_page_count()) AS page_count,
  (SELECT freelist_count FROM pragma_freelist_count()) AS freelist_count,
  (SELECT schema_version FROM pragma_schema_version()) AS schema_version,
  (SELECT journal_mode FROM pragma_journal_mode()) AS journal_mode;
SQL
```

At minimum, the metadata JSON should include:
- `name` (matches the db_id slug)
- `file_size_bytes`
- `page_size`, `page_count`, `freelist_count`
- `journal_mode`, `schema_version`
- Table list with row counts

See `metadata/README.md` for the full recommended field set.

### Step 6: Update the Manifest (Optional)

If maintained, add an entry to `manifests/manifest.v1.json` following the schema
in `manifests/manifest.v1.schema.json`.  Required fields:

```json
{
  "db_id": "asupersync",
  "golden_filename": "asupersync.db",
  "sha256_golden": "<64-char hex>",
  "size_bytes": 12345678,
  "source_path": "/dp/asupersync/.beads/beads.db"
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
# 1. Ingest a fixture
sqlite3 /dp/frankensqlite/.beads/beads.db \
  ".backup 'sample_sqlite_db_files/golden/frankensqlite.db'"

# 2. Verify integrity
sqlite3 sample_sqlite_db_files/golden/frankensqlite.db \
  "PRAGMA integrity_check;"

# 3. Update checksums
cd sample_sqlite_db_files/golden && sha256sum *.db | sort -k2 > ../checksums.sha256

# 4. Run the E2E smoke test
cargo run -p fsqlite-e2e --bin realdb-e2e -- smoke
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
| Hard cap (golden) | No fixed limit | Large files are allowed in `golden/` but tagged `large`. |
| CI-friendly subset | < 20 MiB | Tests that need fast turnaround should filter on the `small` or `medium` tags. |

To override the soft cap during scan: increase `DiscoveryConfig::max_file_size`.

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

Tags provide a stable vocabulary for fixture selection and reporting.  Each
golden file should have at least one project tag and one size tag.

### Project Tags

Derived from the source path or assigned manually at import via `--tag`:

| Tag | Description |
|-----|-------------|
| `beads` | Beads issue tracker databases (`.beads/beads.db`) |
| `flywheel` | Flywheel ecosystem databases (connectors, gateway, private) |
| `agent-tools` | Agent tooling databases (session search, skills, DCG, meta-skill) |
| `app-data` | Application data databases (brenner-bot, idea-analyzer, prompts) |
| `infra` | Infrastructure databases (asupersync, NTM, RCH, dp-level) |
| `frankensqlite` | FrankenSQLite's own beads database |
| `frankentui` | FrankenTUI's own beads database |

### Size Tags (Auto-assigned)

| Tag | Threshold |
|-----|-----------|
| `small` | < 64 KiB |
| `medium` | 64 KiB - 4 MiB |
| `large` | > 4 MiB |

### Feature Tags (Optional)

| Tag | When to use |
|-----|-------------|
| `wal` | Journal mode is WAL |
| `many-indexes` | > 20 indexes |
| `many-tables` | > 20 user tables |
| `fts` | Contains FTS3/FTS5 virtual tables |
| `custom-collation` | Uses non-default collation sequences |

### Tag Usage

- `realdb-e2e run --tag beads` selects only beads fixtures
- Reports group results by tag for cross-fixture comparison
- CI smoke tests filter on `small` + `medium` tags for speed

## Sensitivity and PII Policy

### Metadata Rules

Metadata JSON (`metadata/*.json`) is git-tracked and therefore **public within
the repo**.  The following rules apply:

**Allowed in metadata:**
- Schema structure: table names, column names, column types, constraints
- Aggregate statistics: row counts, page counts, file size
- PRAGMA values: page_size, journal_mode, user_version, application_id
- Index and trigger names
- Source path (shows project structure, not data)

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

Set `safety.pii_risk` in the manifest entry to document the assessment.

### Existing Corpus Assessment

The current corpus consists of internal development tool databases
(beads, flywheel, session search, agent tools).  These contain only project
metadata (issue titles, timestamps, schema DDL) and are assessed as `unlikely`
PII risk.  No databases containing user-facing data, credentials, or personal
information have been ingested.

## Concrete Examples

### Example 1: Beads Database (WAL mode, medium size)

```
Source:  /dp/frankensqlite/.beads/beads.db
db_id:   frankensqlite
Tags:    beads, frankensqlite, large, wal
Size:    ~10 MiB
Journal: WAL
Tables:  issues, comments, dependencies, events, labels, ...
PII:     unlikely (issue tracker metadata only)
```

Import command:
```bash
realdb-e2e corpus import --db /dp/frankensqlite/.beads/beads.db \
  --id frankensqlite --tag beads
```

### Example 2: Agent Tool Database (WAL mode, large)

```
Source:  /dp/flywheel_connectors/.beads/beads.db
db_id:   flywheel_connectors
Tags:    flywheel, large, wal, many-indexes
Size:    ~17 MiB
Journal: WAL
Tables:  issues, comments, dependencies, events, labels, ...
PII:     unlikely (flywheel project metadata)
```

### Example 3: Small Infrastructure Database

```
Source:  /dp/dp_level/dp_level.db
db_id:   dp_level
Tags:    infra, medium, wal
Size:    ~3 MiB
Journal: WAL
Tables:  (project-specific infrastructure tables)
PII:     unlikely (infrastructure metadata)
```

This fixture is useful for fast CI smoke tests due to its small size.
