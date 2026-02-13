# Canonical E2E Scenario Matrix

## Overview
Complete E2E scenario catalog with pass criteria and test entrypoints.

## Scenario Categories

### 1. Schema Evolution (SCH-*)
| ID | Scenario | Prerequisites | Command | Pass Criteria |
|----|----------|---------------|---------|---------------|
| SCH-1 | CREATE TABLE | None | `cargo test -p fsqlite-e2e schema_create` | Table exists in sqlite_master |
| SCH-2 | ALTER TABLE ADD | Existing table | `cargo test -p fsqlite-e2e schema_alter` | Column added, data preserved |
| SCH-3 | DROP TABLE | Existing table | `cargo test -p fsqlite-e2e schema_drop` | Table removed from sqlite_master |

### 2. Transactional Semantics (TXN-*)
| ID | Scenario | Prerequisites | Command | Pass Criteria |
|----|----------|---------------|---------|---------------|
| TXN-1 | COMMIT durability | WAL mode | `cargo run -p fsqlite-e2e --bin realdb_e2e -- txn-commit` | fsync + data visible after restart |
| TXN-2 | ROLLBACK correctness | Active txn | `cargo test -p fsqlite-e2e txn_rollback` | No changes persisted |
| TXN-3 | Savepoint nesting | Active txn | `cargo test -p fsqlite-e2e savepoint` | Partial rollback works |
| TXN-4 | Crash recovery | Committed data | `cargo run -p fsqlite-e2e --bin realdb_e2e -- crash-recovery` | All committed data recovered |

### 3. Concurrent Writers (CON-*)
| ID | Scenario | Prerequisites | Command | Pass Criteria |
|----|----------|---------------|---------|---------------|
| CON-1 | Multi-writer contention | concurrent_mode=true | `cargo test -p fsqlite-e2e concurrent_write` | No SQLITE_BUSY deadlock |
| CON-2 | MVCC snapshot isolation | concurrent_mode=true | `cargo test -p fsqlite-e2e mvcc_snapshot` | Reads see consistent snapshot |
| CON-3 | SSI write-skew prevention | concurrent_mode=true | `cargo test -p fsqlite-e2e ssi_validation` | Write skew aborted |
| CON-4 | Page-level conflict | Same page access | `cargo test -p fsqlite-e2e page_conflict` | First-committer-wins |

### 4. Corruption/Recovery (COR-*)
| ID | Scenario | Prerequisites | Command | Pass Criteria |
|----|----------|---------------|---------|---------------|
| COR-1 | WAL byte corruption | WAL with data | `cargo run -p fsqlite-e2e --bin corruption_demo -- wal-byte` | RaptorQ repairs or detects |
| COR-2 | WAL frame truncation | WAL with frames | `cargo run -p fsqlite-e2e --bin corruption_demo -- wal-truncate` | Recovery to last good frame |
| COR-3 | Database header | Valid .db file | `cargo run -p fsqlite-e2e --bin corruption_demo -- header` | Open fails with clear error |
| COR-4 | RaptorQ FEC recovery | WAL-FEC enabled | `cargo run -p fsqlite-e2e --bin realdb_e2e -- fec-recover` | Full data recovery |

### 5. Compatibility (CMP-*)
| ID | Scenario | Prerequisites | Command | Pass Criteria |
|----|----------|---------------|---------|---------------|
| CMP-1 | SQLite roundtrip | .db file | `cargo run -p fsqlite-e2e --bin e2e_runner -- compat` | Same data in both engines |
| CMP-2 | Differential | Test workload | `cargo run -p fsqlite-e2e --bin e2e_dashboard` | No divergence in results |

## Deterministic Seed Policy

All scenarios use deterministic RNG seeding:
- Default seed: `0xFRANKEN` (0x4652414E4B454E)
- Override: `--seed <u64>`
- Seed logged in JSON artifacts for reproducibility

## Failure Artifacts

On failure, scenarios produce:
- `{scenario_id}_failure.json` - Error details + stack
- `{scenario_id}_db_snapshot.sqlite` - Database state at failure
- `{scenario_id}_wal_snapshot.wal` - WAL state if applicable
- `{scenario_id}_integrity_check.txt` - PRAGMA integrity_check output

## Run All Scenarios

```bash
# Full matrix (CI)
cargo test -p fsqlite-e2e --all-features

# Quick smoke
cargo test -p fsqlite-e2e smoke

# Specific category
cargo test -p fsqlite-e2e txn_
cargo test -p fsqlite-e2e concurrent_
cargo test -p fsqlite-e2e corruption_
```

*SwiftOwl 2026-02-13*
