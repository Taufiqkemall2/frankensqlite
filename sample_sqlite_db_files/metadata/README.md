# metadata/

Tracked metadata describing each DB in `../golden/`.

## File Naming

Prefer one JSON file per database:
- `<db_id>.json`

Where `db_id` is the stable slug used in the corpus manifest schema:
`../manifests/manifest.v1.schema.json`.

## Recommended Fields (v1)

Metadata JSON is emitted using `fsqlite_e2e::fixture_metadata::FixtureMetadataV1`
(`crates/fsqlite-e2e/src/fixture_metadata.rs`).

This folder is intentionally flexible, but metadata should generally include:
- `schema_version` (fixture metadata schema version, currently `1`)
- `db_id`
- `source_path` (optional; original `/dp/...` path used to seed the golden copy)
- `golden_filename` (file under `../golden/`)
- `sha256_golden` + `size_bytes`
- `sidecars_present` at capture time (`-wal`, `-shm`, `-journal`), if known
- SQLite PRAGMAs (best-effort):
  - `sqlite_meta.page_size`, `sqlite_meta.encoding`, `sqlite_meta.user_version`,
    `sqlite_meta.application_id`, `sqlite_meta.journal_mode`, `sqlite_meta.auto_vacuum`
- Schema summaries (best-effort):
  - list of tables/indexes/views/triggers
  - per-table row counts (optional; can be expensive on large DBs)
  - freelist/page stats (`sqlite_meta.page_count`, `sqlite_meta.freelist_count`) for storage-shape diversity

Note: `schema_version` above is the *metadata schema* version. The SQLite PRAGMA
`schema_version` is stored as `sqlite_meta.schema_version`.

### Tags

Fixture metadata stores a `tags: [..]` array used for selection and reporting. Tags must be:
- lowercase
- sorted
- de-duplicated

`realdb-e2e corpus import --tag <TAG>` records a stable classification tag (stored in `tags`).
Stable tags are intentionally small:

- `asupersync`, `frankentui`, `flywheel`, `frankensqlite`, `agent-mail`, `beads`, `misc`

Additional derived tags (e.g. `small|medium|large`, `wal`) may also be present in `tags`.

`realdb-e2e corpus scan` emits heuristic tags in its discovery JSON output; those are hints and may
or may not be persisted into `metadata/*.json` depending on the ingestion pipeline.

## Safety / Redaction

Do not commit anything that looks like secrets, tokens, API keys, or PII. If a DB is suspicious,
exclude it from the corpus rather than trying to partially redact it.

The stable metadata schema includes a `safety` object used for CI gating:
- `safety.pii_risk` and `safety.secrets_risk` (`unknown|unlikely|possible|likely`)
- `safety.allowed_for_ci` (bool)

`realdb-e2e corpus import` sets these via `--pii-risk`, `--secrets-risk`, and `--allow-for-ci`.
