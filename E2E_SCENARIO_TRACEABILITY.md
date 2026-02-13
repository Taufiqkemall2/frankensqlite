# E2E Scenario Traceability Map

## Scenario Categories

### Corruption/Recovery (fsqlite-e2e/src/corruption*.rs)
| ID | Scenario | Runner | Artifacts |
|----|----------|--------|-----------|
| COR-1 | WAL byte corruption | realdb_e2e | integrity_check.json |
| COR-2 | WAL frame truncation | corruption_demo | recovered_rows.log |
| COR-3 | Database header corruption | corruption_demo | open_error.log |
| COR-4 | RaptorQ FEC recovery | realdb_e2e | fec_repair.log |

### Concurrency (fsqlite-e2e/src/concurrency_showcase.rs, tests/ssi_write_skew.rs)
| ID | Scenario | Runner | Artifacts |
|----|----------|--------|-----------|
| CON-1 | Multi-writer contention | e2e_runner | write_conflict.json |
| CON-2 | MVCC snapshot isolation | e2e_runner | snapshot_check.json |
| CON-3 | SSI write-skew prevention | ssi_write_skew | ssi_abort.log |
| CON-4 | Deadlock freedom | e2e_runner | no_deadlock.json |

### Transaction/Recovery (fsqlite-e2e/src/recovery*.rs)
| ID | Scenario | Runner | Artifacts |
|----|----------|--------|-----------|
| TXN-1 | COMMIT durability | realdb_e2e | commit_verify.json |
| TXN-2 | ROLLBACK correctness | e2e_runner | rollback_verify.json |
| TXN-3 | Savepoint nesting | e2e_runner | savepoint_stack.log |
| TXN-4 | Crash recovery | realdb_e2e | recovery_replay.json |

### Compatibility (fsqlite-e2e/src/comparison.rs)
| ID | Scenario | Runner | Artifacts |
|----|----------|--------|-----------|
| CMP-1 | SQLite format roundtrip | e2e_runner | format_check.json |
| CMP-2 | Differential comparison | e2e_dashboard | diff_report.html |

## Test Runners

| Binary | Path | Description |
|--------|------|-------------|
| e2e_runner | bin/e2e_runner.rs | Core E2E test orchestrator |
| realdb_e2e | bin/realdb_e2e.rs | Real file-backed DB tests |
| corruption_demo | bin/corruption_demo.rs | Corruption showcase |
| e2e_dashboard | bin/e2e_dashboard.rs | Report dashboard |
| bench_report | bin/bench_report.rs | Performance reports |

## Modules Mapping

| Module | Scenarios Covered |
|--------|-------------------|
| corruption.rs | COR-1, COR-2, COR-4 |
| corruption_scenarios.rs | COR-1..4 definitions |
| concurrency_showcase.rs | CON-1, CON-2, CON-4 |
| tests/ssi_write_skew.rs | CON-3 (SSI write-skew prevention) |
| recovery_runner.rs | TXN-1..4 |
| comparison.rs | CMP-1, CMP-2 |
| golden.rs | All (fixture management) |
| workload.rs | All (deterministic generation) |

*SwiftOwl 2026-02-13*
