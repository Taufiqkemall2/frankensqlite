# metadata/

Tracked metadata describing each DB in `../golden/`.

## File Naming

Prefer one JSON file per database:
- `<db_id>.json`

Where `db_id` is the stable slug used in the corpus manifest schema:
`../manifests/manifest.v1.schema.json`.

## Recommended Fields (v1)

This folder is intentionally flexible, but metadata should generally include:
- `db_id`
- `source_path` (original `/dp/...` path used to seed the golden copy)
- `golden_filename` (file under `../golden/`)
- `sha256_golden` + `size_bytes`
- `sidecars` present at capture time (`-wal`, `-shm`, `-journal`), if known
- SQLite PRAGMAs (best-effort):
  - `page_size`, `encoding`, `user_version`, `application_id`, `journal_mode`, `auto_vacuum`
- Schema summaries (best-effort):
  - list of tables/indexes/views/triggers
  - per-table row counts (optional; can be expensive on large DBs)
  - freelist/page stats (`page_count`, `freelist_count`) for storage-shape diversity

## Safety / Redaction

Do not commit anything that looks like secrets, tokens, API keys, or PII. If a DB is suspicious,
exclude it from the corpus rather than trying to partially redact it.
