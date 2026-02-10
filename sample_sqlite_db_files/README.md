# sample_sqlite_db_files

Local-only corpus of **real SQLite database files** used for end-to-end demos and benchmarking.

## Safety Rules

- NEVER run tests or demos against `/dp/...` originals.
- Always take a consistent snapshot using SQLite's backup API (`sqlite3 ... ".backup '...'"`).
- Treat `golden/` as immutable. Do work on copies in `working/`.

## Corpus Inclusion Policy (bd-1n0i)

This corpus is curated. The goal is realistic fixtures without accidentally
ingesting secrets/PII, huge blobs that make CI unusable, or inconsistent
snapshots from live WAL-mode databases.

### Inclusion Rules (Default Behavior)

- Allowed roots: `/dp` by default (override with `realdb-e2e corpus scan --root ...`).
- Allowed extensions: `.db`, `.sqlite`, `.sqlite3` (plus common Beads DB names like `beads.db`).
- Size cap: `realdb-e2e` discovery/import refuses files over **512 MiB** by default.
  - Override with `--max-file-size-mib <N>` (use `0` to disable the cap; not recommended).
- WAL/SHM sidecars: import uses SQLite's backup API to capture a consistent snapshot.
  - Sidecar presence (`-wal`, `-shm`, `-journal`) is recorded in metadata.

### Sensitivity / Redaction Rules

- Golden DB bytes are **never** committed to git (only checksums + metadata + manifests).
- Metadata must never include row contents.
- If a DB looks suspicious (tokens, emails, customer data), exclude it from the corpus rather
  than trying to partially redact it.

### Tagging Taxonomy

`realdb-e2e corpus scan` emits heuristic tags (project name, `beads`, size buckets) in its
JSON discovery output.

`realdb-e2e corpus import --tag <TAG>` sets a stable *classification* tag (from a small
allowlist) and stores it in the fixture metadata.

- `asupersync`, `frankentui`, `flywheel`, `frankensqlite`, `agent-mail`, `beads`, `misc`

Additional derived tags (e.g. `small|medium|large`, `wal`) may also be present in the
metadata `tags` array; these are normalized (lowercase), sorted, and de-duplicated.

### Safety / CI Eligibility

Fixture metadata is git-tracked, so it must not leak secrets/PII.

`realdb-e2e corpus import` records a conservative safety classification:

- `--pii-risk unknown|unlikely|possible|likely`
- `--secrets-risk unknown|unlikely|possible|likely`
- `--allow-for-ci` (or leave false)

These fields are stored in `metadata/*.json` under `safety.{pii_risk,secrets_risk,allowed_for_ci}`.
For redaction safety, `source_path` may be omitted (`null`) in committed metadata.

### Concrete Examples

Beads DB (safe internal metadata, stable schema):

```bash
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus import \
  --db /dp/asupersync/.beads/beads.db --tag asupersync
```

WAL-mode DB with sidecars present at capture time:

```bash
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus import \
  --db /dp/flywheel_gateway/data/gateway.db --tag flywheel
```

Larger Beads DB (tens of MiB; fine locally, but consider CI impact):

```bash
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus import \
  --db /dp/flywheel_connectors/.beads/beads.db --tag flywheel
```

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

In most cases you should prefer the single command:

```bash
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus import --db "$src" --tag asupersync
```

## How The Harness Uses This Corpus

- The Rust E2E harness (`crates/fsqlite-e2e`) treats `golden/` as immutable inputs.
- Each run creates a fresh working copy under `working/` (or a per-run scratch dir) and operates only on that copy.
- Verification gates check:
  - `PRAGMA integrity_check` on golden files
  - sha256 of golden files vs `checksums.sha256`

## Git Hygiene

This repo must never commit DB bytes from `/dp`. Only metadata + checksums are tracked.
