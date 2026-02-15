# Unified E2E Log Schema Contract

This document defines the canonical E2E structured log schema for FrankenSQLite harness tooling.

- Schema version: `1.0.0`
- Minimum supported version: `1.0.0`
- Required fields: `run_id`, `timestamp`, `phase`, `event_type`
- Replayability keys: `scenario_id`, `seed`, `phase`, `context.invariant_ids`, `context.artifact_paths`
- Shell-script profile artifact: `docs/e2e_shell_script_log_profile.json` (version `1.0.0`)

## Field Contract

| Field | Requirement | Type | Description | Allowed Values | Allowed Range | Semantics |
| --- | --- | --- | --- | --- | --- | --- |
| `run_id` | Required | String | Unique run identifier for log correlation | - | non-empty; `{bead_id}-{timestamp}-{pid}` | Correlation key across a run |
| `timestamp` | Required | String | Event timestamp | - | RFC3339 UTC ending in `Z` | Timeline reconstruction |
| `phase` | Required | Enum | Execution phase | `setup`, `execute`, `validate`, `teardown`, `report` | - | Lifecycle marker for orchestrators |
| `event_type` | Required | Enum | Event classification | `start`, `pass`, `fail`, `skip`, `info`, `warn`, `error`, `first_divergence`, `artifact_generated` | - | Analytics and gating semantics |
| `scenario_id` | Recommended | String | Traceability scenario ID | - | `[A-Z]+-[0-9]+` | Links to parity traceability matrix |
| `seed` | Recommended | UnsignedInteger | Deterministic random seed | - | `0..=u64::MAX` | Reproducibility key |
| `backend` | Recommended | Enum | Backend under test | `fsqlite`, `rusqlite`, `both` | - | Differential run disambiguation |
| `artifact_hash` | Optional | String | Artifact integrity hash | - | 64 lowercase hex chars | Evidence integrity and dedupe |
| `context` | Optional | Object | Additional key/value context | - | String map with replay keys when relevant | Extensible deterministic metadata |

## Shell-Script Profile (Machine-Readable)

Shell-script emitters and CI conformance checks consume `docs/e2e_shell_script_log_profile.json`.
The profile is versioned, deterministic, and includes:

- Required shell fields: `run_id`, `timestamp`, `phase`, `event_type`
- Optional shell fields: `scenario_id`, `seed`, `backend`, `artifact_hash`, `context`, and context-namespaced keys for `trace_id`, `level`, `outcome`, `duration_ms`, `retry_attempt`, `artifact_paths`, `invariant_ids`
- Normative success/failure examples encoded as canonical event payloads
- Replay instructions for deterministic local/CI verification

### Legacy Ad-Hoc Log Migration Guidance

| Legacy token | Canonical field | Migration guidance |
| --- | --- | --- |
| `level` | `context.level` | Keep INFO/WARN/ERROR severity in context and map state changes to `event_type`. |
| `status` | `context.outcome` | Preserve script-level status in context; keep schema enum values in `event_type`. |
| `log` | `context.artifact_paths` | Normalize to deterministic comma-separated artifact paths. |
| `duration_ms` | `context.duration_ms` | Keep duration as stringified integer milliseconds. |
| `retry_count` | `context.retry_attempt` | Normalize retries to zero-based attempt count. |
| `scenario` | `scenario_id` | Promote to `CATEGORY-NUMBER` scenario ID format. |
| `seed_value` | `seed` | Emit deterministic seed as unsigned integer. |

## Versioning Policy

- Additive changes must bump **MINOR** and preserve all previously required fields.
- Breaking changes must bump **MAJOR** and include explicit migration guidance for tooling.
- Downgrades are unsupported; emitters must never decrease schema version.

## Tooling Compatibility Policy

- `tool.major == event.major && tool.minor >= event.minor`:
  - Compatibility: `ReadWrite`
  - Behavior: parse and emit events normally.
- `tool.major == event.major && tool.minor < event.minor`:
  - Compatibility: `ReadOnlyForwardCompatible`
  - Behavior: parse by ignoring unknown additive fields and avoid re-emitting transformed events.
- `tool.major != event.major`:
  - Compatibility: `Incompatible`
  - Behavior: fail fast and require explicit major-version upgrade.

## Canonical Event Examples

```json
{"run_id":"bd-mblr.5.3.1-20260213T090000Z-1001","timestamp":"2026-02-13T09:00:00.000Z","phase":"setup","event_type":"start","scenario_id":"INFRA-6","seed":1001,"backend":"both","context":{"invariant_ids":"INV-1,INV-9","artifact_paths":"artifacts/events.jsonl,artifacts/diff.json"}}
{"run_id":"bd-mblr.5.3.1-20260213T090000Z-1001","timestamp":"2026-02-13T09:00:03.100Z","phase":"validate","event_type":"pass","scenario_id":"MVCC-3","seed":1001,"backend":"fsqlite","artifact_hash":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","context":{"invariant_ids":"INV-1,INV-9","artifact_paths":"artifacts/events.jsonl,artifacts/diff.json"}}
{"run_id":"bd-mblr.5.3.1-20260213T090000Z-1001","timestamp":"2026-02-13T09:00:04.250Z","phase":"validate","event_type":"first_divergence","scenario_id":"COR-2","seed":1001,"backend":"both","artifact_hash":"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","context":{"invariant_ids":"INV-1,INV-9","artifact_paths":"artifacts/events.jsonl,artifacts/diff.json"}}
```

Shell-script normative success/failure examples are maintained in:

- `docs/e2e_shell_script_log_profile.json` (`normative_examples`)
