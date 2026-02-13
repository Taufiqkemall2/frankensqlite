# E2E Scenario Catalog

This document defines the canonical catalog of end-to-end test scenarios for FrankenSQLite. Each scenario has a stable ID, pass/fail criteria, and links to relevant invariants.

## Scenario ID Format

`E2E-{CATEGORY}-{NUMBER}`

Categories:
- **DDL** - Data Definition Language (CREATE, ALTER, DROP)
- **DML** - Data Manipulation Language (INSERT, UPDATE, DELETE, SELECT)
- **TXN** - Transaction lifecycle
- **CNC** - Concurrency and MVCC
- **REC** - Recovery and durability
- **COR** - Corruption detection and repair
- **CMP** - Compatibility mode
- **EXT** - Extensions

---

## DDL Scenarios

### E2E-DDL-001: CREATE TABLE with all column types
- **Description**: Create table with INTEGER, REAL, TEXT, BLOB, NULL-capable columns
- **Pass criteria**: Table created, schema persisted, columns queryable
- **Invariants**: INV-S1 (Schema Epoch)
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::create_table_and_insert_select`

### E2E-DDL-002: CREATE INDEX single column
- **Description**: Create index on single column, verify query uses it
- **Pass criteria**: Index created, EXPLAIN shows index usage
- **Invariants**: INV-S1
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::index_insert_single_column`

### E2E-DDL-003: CREATE INDEX multi-column
- **Description**: Create composite index, verify prefix usage
- **Pass criteria**: Index works for leading column queries
- **Invariants**: INV-S1
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::index_insert_multi_column`

### E2E-DDL-004: DROP TABLE with active cursors
- **Description**: Drop table while cursor is open
- **Pass criteria**: Error or deferred drop, no crash
- **Invariants**: INV-S1
- **Evidence**: TBD - gap identified

### E2E-DDL-005: ALTER TABLE ADD COLUMN
- **Description**: Add column to existing table with data
- **Pass criteria**: New column added, existing data preserved
- **Invariants**: INV-S1
- **Evidence**: TBD - gap identified

---

## DML Scenarios

### E2E-DML-001: INSERT single row
- **Description**: Insert single row with explicit values
- **Pass criteria**: Row inserted, rowid returned
- **Invariants**: INV-3 (Version Chain)
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::create_table_and_insert_select`

### E2E-DML-002: INSERT multi-row VALUES
- **Description**: Insert multiple rows in single statement
- **Pass criteria**: All rows inserted atomically
- **Invariants**: INV-6 (Commit Atomicity)
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::insert_multi_row_values`

### E2E-DML-003: INSERT ... SELECT
- **Description**: Insert rows from subquery
- **Pass criteria**: Rows copied correctly
- **Invariants**: INV-6
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::probe_insert_select`

### E2E-DML-004: UPDATE with WHERE
- **Description**: Update subset of rows matching predicate
- **Pass criteria**: Matching rows updated, others unchanged
- **Invariants**: INV-3, INV-6
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::update_modifies_rows`

### E2E-DML-005: DELETE with WHERE
- **Description**: Delete subset of rows matching predicate
- **Pass criteria**: Matching rows deleted, others preserved
- **Invariants**: INV-3, INV-6
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::delete_removes_rows`

### E2E-DML-006: SELECT with JOIN
- **Description**: Inner/outer joins across tables
- **Pass criteria**: Correct result set
- **Invariants**: None (query correctness)
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::full_outer_join_null_extension`

### E2E-DML-007: SELECT with subquery
- **Description**: Scalar and table subqueries in WHERE
- **Pass criteria**: Correct filtering
- **Invariants**: None
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::scalar_subquery_expression`

---

## Transaction Scenarios

### E2E-TXN-001: BEGIN/COMMIT basic
- **Description**: Start transaction, make changes, commit
- **Pass criteria**: Changes visible after commit
- **Invariants**: INV-6 (Commit Atomicity)
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::begin_commit_persists_changes`

### E2E-TXN-002: BEGIN/ROLLBACK basic
- **Description**: Start transaction, make changes, rollback
- **Pass criteria**: Changes reverted
- **Invariants**: INV-6
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::rollback_reverts_changes`

### E2E-TXN-003: SAVEPOINT create/release
- **Description**: Nested savepoints within transaction
- **Pass criteria**: Savepoints work correctly
- **Invariants**: INV-5 (Snapshot Stability)
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::savepoint_and_rollback_to`

### E2E-TXN-004: SAVEPOINT rollback
- **Description**: Rollback to savepoint
- **Pass criteria**: Changes since savepoint reverted
- **Invariants**: INV-5
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::savepoint_release_commits_changes`

### E2E-TXN-005: Nested transaction error
- **Description**: BEGIN inside active transaction
- **Pass criteria**: Error returned
- **Invariants**: None (constraint)
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::nested_begin_errors`

---

## Concurrency Scenarios

### E2E-CNC-001: Concurrent readers
- **Description**: Multiple readers see consistent snapshot
- **Pass criteria**: No dirty reads
- **Invariants**: INV-5 (Snapshot Stability)
- **Evidence**: `crates/fsqlite/src/lib.rs::tests::concurrent_readers_consistency`

### E2E-CNC-002: Concurrent writers different pages
- **Description**: Two writers touching different leaf pages
- **Pass criteria**: Both commit successfully
- **Invariants**: INV-2 (Page Lock Exclusivity), INV-C2 (No Deadlock)
- **Evidence**: `crates/fsqlite-harness/tests/bd_2npr_mvcc_concurrent_writer_stress.rs`

### E2E-CNC-003: Concurrent writers same page
- **Description**: Two writers touching same leaf page
- **Pass criteria**: FCW or safe merge, one commits
- **Invariants**: INV-2, INV-4 (Write Set)
- **Evidence**: `crates/fsqlite-mvcc/src/deterministic_rebase.rs` tests

### E2E-CNC-004: Writer vs reader isolation
- **Description**: Reader doesn't see uncommitted writer changes
- **Pass criteria**: Snapshot isolation holds
- **Invariants**: INV-5, INV-6
- **Evidence**: TBD - gap identified

### E2E-CNC-005: SSI write skew prevention
- **Description**: Detect and abort write skew anomaly
- **Pass criteria**: One transaction aborts with SQLITE_BUSY_SNAPSHOT
- **Invariants**: INV-6 (SSI)
- **Evidence**: `crates/fsqlite-harness/tests/bd_2d3i_1_ssi_witness_plane_deterministic_scenarios_compliance.rs`

---

## Recovery Scenarios

### E2E-REC-001: Clean shutdown and restart
- **Description**: Close connection, reopen, verify data
- **Pass criteria**: All committed data present
- **Invariants**: INV-D1 (WAL Integrity)
- **Evidence**: `crates/fsqlite-core/src/connection.rs` tests

### E2E-REC-002: Crash after commit, before checkpoint
- **Description**: Kill process after WAL commit, recover
- **Pass criteria**: Committed data recovered
- **Invariants**: INV-D2 (Crash Recovery)
- **Evidence**: `crates/fsqlite-wal/tests/wal_fec_recovery.rs::test_recovery_intact_wal`

### E2E-REC-003: Crash during write
- **Description**: Kill process mid-write, recover
- **Pass criteria**: No partial commits visible
- **Invariants**: INV-D2
- **Evidence**: TBD - gap identified

### E2E-REC-004: WAL replay after checkpoint
- **Description**: Verify checkpoint followed by WAL replay
- **Pass criteria**: Correct final state
- **Invariants**: INV-D2
- **Evidence**: `crates/fsqlite-wal/src/checkpoint_executor.rs` tests

---

## Corruption Scenarios

### E2E-COR-001: WAL checksum validation
- **Description**: Detect corrupted WAL frame
- **Pass criteria**: Corruption detected, error returned
- **Invariants**: INV-D1 (WAL Integrity)
- **Evidence**: `crates/fsqlite-wal/tests/wal_fec_recovery.rs::test_raptorq_bitflip_detected`

### E2E-COR-002: RaptorQ repair within budget
- **Description**: Corrupt symbols, recover via repair
- **Pass criteria**: Data recovered successfully
- **Invariants**: INV-D3 (Self-Healing)
- **Evidence**: `crates/fsqlite-wal/tests/wal_fec_recovery.rs::test_raptorq_bitflip_repair`

### E2E-COR-003: RaptorQ repair exceeded
- **Description**: Corruption beyond repair budget
- **Pass criteria**: Error returned, no silent corruption
- **Invariants**: INV-D3
- **Evidence**: `crates/fsqlite-wal/tests/wal_fec_recovery.rs::test_raptorq_symbol_loss_beyond_R`

### E2E-COR-004: Database file corruption detection
- **Description**: Corrupt database page, detect on read
- **Pass criteria**: Corruption detected
- **Invariants**: INV-D1
- **Evidence**: TBD - gap identified

---

## Compatibility Scenarios

### E2E-CMP-001: Open SQLite database
- **Description**: Open database created by C SQLite
- **Pass criteria**: Data readable, schema correct
- **Invariants**: None (compatibility)
- **Evidence**: `crates/fsqlite-core/src/compat_persist.rs` tests

### E2E-CMP-002: Export to SQLite format
- **Description**: Write database readable by C SQLite
- **Pass criteria**: sqlite3 can read the file
- **Invariants**: None (compatibility)
- **Evidence**: TBD - gap identified

### E2E-CMP-003: WAL mode interop
- **Description**: WAL files compatible with C SQLite
- **Pass criteria**: Recovery works in both directions
- **Invariants**: INV-D1
- **Evidence**: TBD - gap identified

---

## Extension Scenarios

### E2E-EXT-001: JSON1 functions
- **Description**: json_extract, json_set, json_array
- **Pass criteria**: Correct JSON manipulation
- **Invariants**: None
- **Evidence**: `crates/fsqlite-ext-json` tests

### E2E-EXT-002: FTS5 full-text search
- **Description**: Create FTS table, query with MATCH
- **Pass criteria**: Correct search results
- **Invariants**: None
- **Evidence**: `crates/fsqlite-ext-fts5` tests

### E2E-EXT-003: R-tree spatial index
- **Description**: Create R-tree, spatial queries
- **Pass criteria**: Correct spatial results
- **Invariants**: None
- **Evidence**: `crates/fsqlite-ext-rtree` tests

---

## Gap Summary

Scenarios marked "TBD - gap identified" require test implementation:

| ID | Scenario | Priority |
|----|----------|----------|
| E2E-DDL-004 | DROP TABLE with active cursors | P1 |
| E2E-DDL-005 | ALTER TABLE ADD COLUMN | P1 |
| E2E-CNC-004 | Writer vs reader isolation | P0 |
| E2E-REC-003 | Crash during write | P0 |
| E2E-COR-004 | Database file corruption | P1 |
| E2E-CMP-002 | Export to SQLite format | P1 |
| E2E-CMP-003 | WAL mode interop | P1 |

---

## Related Documents

- [Critical Invariants Catalog](critical-invariants.md)
- [Coverage Gap Report](coverage-gap-report.md)
- [Test Realism Inventory](test-realism/README.md)
