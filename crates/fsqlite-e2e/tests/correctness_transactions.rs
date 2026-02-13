//! Correctness test: transaction patterns (BEGIN/COMMIT/ROLLBACK/SAVEPOINT).
//!
//! Bead: bd-24uu
//!
//! Verifies that FrankenSQLite's transaction handling produces identical
//! results to C SQLite across all major transaction patterns:
//! - Simple commit
//! - Transaction rollback
//! - Savepoint with partial rollback
//! - Nested savepoints
//! - Implicit autocommit
//! - Large transactional batches
//!
//! Transaction control statements (BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE)
//! may report different `changes()` counts across engines (e.g., rusqlite
//! returns 1 for COMMIT while FrankenSQLite returns 0). This is cosmetic.
//! These tests verify **state correctness**: the data visible after each
//! transaction pattern must be identical on both engines.

use fsqlite_e2e::comparison::{ComparisonRunner, SqlBackend, SqlValue};
use tempfile::tempdir;

// ─── Helpers ───────────────────────────────────────────────────────────

/// Execute all statements on both backends (ignoring per-statement row-count
/// differences for transaction control), then verify the listed queries
/// produce identical results on both engines.
fn run_scenario(setup: &[&str], verify: &[(&str, &[SqlValue])]) {
    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");

    // Execute all setup statements on both engines.
    for sql in setup {
        let c_res = runner.csqlite().execute(sql);
        let f_res = runner.frank().execute(sql);

        // Both must succeed (but we don't compare affected row counts for
        // transaction control statements).
        let is_txn_ctrl = {
            let upper = sql.trim().to_uppercase();
            upper.starts_with("BEGIN")
                || upper.starts_with("COMMIT")
                || upper.starts_with("ROLLBACK")
                || upper.starts_with("SAVEPOINT")
                || upper.starts_with("RELEASE")
                || upper.starts_with("END")
        };

        if is_txn_ctrl {
            // Both must succeed (or both fail).
            assert!(
                c_res.is_ok() == f_res.is_ok(),
                "txn control outcome diverged for '{sql}':\n  csqlite={c_res:?}\n  fsqlite={f_res:?}"
            );
        } else {
            // For DML/DDL, both must succeed.
            assert!(c_res.is_ok(), "csqlite failed on '{sql}': {c_res:?}");
            assert!(f_res.is_ok(), "fsqlite failed on '{sql}': {f_res:?}");
        }
    }

    // Verify final state matches.
    for (sql, expected_first_row) in verify {
        let c_rows = runner.csqlite().query(sql).expect("csqlite verify");
        let f_rows = runner.frank().query(sql).expect("fsqlite verify");
        assert_eq!(c_rows, f_rows, "verify query differs: {sql}");
        if !expected_first_row.is_empty() {
            assert!(!c_rows.is_empty(), "expected rows for verify query: {sql}");
            assert_eq!(
                &c_rows[0][..expected_first_row.len()],
                *expected_first_row,
                "first row mismatch for: {sql}"
            );
        }
    }
}

fn csqlite_query_values(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<SqlValue>> {
    let mut stmt = conn.prepare(sql).expect("csqlite prepare");
    let col_count = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            let mut values = Vec::with_capacity(col_count);
            for i in 0..col_count {
                let value: rusqlite::types::Value =
                    row.get(i).unwrap_or(rusqlite::types::Value::Null);
                values.push(match value {
                    rusqlite::types::Value::Null => SqlValue::Null,
                    rusqlite::types::Value::Integer(v) => SqlValue::Integer(v),
                    rusqlite::types::Value::Real(v) => SqlValue::Real(v),
                    rusqlite::types::Value::Text(v) => SqlValue::Text(v),
                    rusqlite::types::Value::Blob(v) => SqlValue::Blob(v),
                });
            }
            Ok(values)
        })
        .expect("csqlite query_map");
    rows.collect::<Result<Vec<_>, _>>()
        .expect("csqlite collect rows")
}

fn fsqlite_query_values(conn: &fsqlite::Connection, sql: &str) -> Vec<Vec<SqlValue>> {
    conn.query(sql)
        .expect("fsqlite query")
        .into_iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    fsqlite_types::SqliteValue::Null => SqlValue::Null,
                    fsqlite_types::SqliteValue::Integer(v) => SqlValue::Integer(*v),
                    fsqlite_types::SqliteValue::Float(v) => SqlValue::Real(*v),
                    fsqlite_types::SqliteValue::Text(v) => SqlValue::Text(v.clone()),
                    fsqlite_types::SqliteValue::Blob(v) => SqlValue::Blob(v.clone()),
                })
                .collect()
        })
        .collect()
}

fn checkpoint_triplet(rows: &[Vec<SqlValue>], label: &str) -> (i64, i64, i64) {
    assert_eq!(rows.len(), 1, "{label}: expected one row");
    assert_eq!(rows[0].len(), 3, "{label}: expected three columns");
    let read_i64 = |idx: usize| -> i64 {
        if let SqlValue::Integer(v) = &rows[0][idx] {
            *v
        } else {
            assert!(
                matches!(rows[0][idx], SqlValue::Integer(_)),
                "{label}: expected integer at index {idx}, got {:?}",
                rows[0][idx]
            );
            0
        }
    };
    (read_i64(0), read_i64(1), read_i64(2))
}

// ─── Scenario A: Simple transaction commit ─────────────────────────────

#[test]
fn txn_simple_commit() {
    run_scenario(
        &[
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)",
            "BEGIN",
            "INSERT INTO test VALUES (1, 'a')",
            "INSERT INTO test VALUES (2, 'b')",
            "COMMIT",
        ],
        &[
            ("SELECT COUNT(*) FROM test", &[SqlValue::Integer(2)]),
            (
                "SELECT val FROM test WHERE id = 1",
                &[SqlValue::Text("a".to_owned())],
            ),
            (
                "SELECT val FROM test WHERE id = 2",
                &[SqlValue::Text("b".to_owned())],
            ),
        ],
    );
}

// ─── Scenario B: Transaction rollback ──────────────────────────────────

#[test]
fn txn_rollback_discards_all() {
    run_scenario(
        &[
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO test VALUES (99, 'pre_existing')",
            "BEGIN",
            "INSERT INTO test VALUES (1, 'a')",
            "INSERT INTO test VALUES (2, 'b')",
            "ROLLBACK",
        ],
        &[
            ("SELECT COUNT(*) FROM test", &[SqlValue::Integer(1)]),
            (
                "SELECT val FROM test WHERE id = 99",
                &[SqlValue::Text("pre_existing".to_owned())],
            ),
            (
                "SELECT COUNT(*) FROM test WHERE id IN (1, 2)",
                &[SqlValue::Integer(0)],
            ),
        ],
    );
}

// ─── Scenario C: Savepoint with partial rollback ───────────────────────

#[test]
fn txn_savepoint_partial_rollback() {
    run_scenario(
        &[
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)",
            "BEGIN",
            "INSERT INTO test VALUES (1, 'a')",
            "SAVEPOINT sp1",
            "INSERT INTO test VALUES (2, 'b')",
            "ROLLBACK TO sp1",
            "INSERT INTO test VALUES (3, 'c')",
            "COMMIT",
        ],
        &[
            ("SELECT COUNT(*) FROM test", &[SqlValue::Integer(2)]),
            (
                "SELECT val FROM test WHERE id = 1",
                &[SqlValue::Text("a".to_owned())],
            ),
            (
                "SELECT COUNT(*) FROM test WHERE id = 2",
                &[SqlValue::Integer(0)],
            ),
            (
                "SELECT val FROM test WHERE id = 3",
                &[SqlValue::Text("c".to_owned())],
            ),
        ],
    );
}

// ─── Scenario D: Nested savepoints ─────────────────────────────────────

#[test]
fn txn_nested_savepoints() {
    run_scenario(
        &[
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)",
            "BEGIN",
            "SAVEPOINT sp1",
            "INSERT INTO test VALUES (1, 'a')",
            "SAVEPOINT sp2",
            "INSERT INTO test VALUES (2, 'b')",
            "ROLLBACK TO sp2",
            "RELEASE sp1",
            "COMMIT",
        ],
        &[
            ("SELECT COUNT(*) FROM test", &[SqlValue::Integer(1)]),
            (
                "SELECT val FROM test WHERE id = 1",
                &[SqlValue::Text("a".to_owned())],
            ),
            (
                "SELECT COUNT(*) FROM test WHERE id = 2",
                &[SqlValue::Integer(0)],
            ),
        ],
    );
}

// ─── Scenario E: Implicit autocommit ───────────────────────────────────

#[test]
fn txn_implicit_autocommit() {
    run_scenario(
        &[
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO test VALUES (1, 'a')",
            "INSERT INTO test VALUES (2, 'b')",
        ],
        &[
            ("SELECT COUNT(*) FROM test", &[SqlValue::Integer(2)]),
            (
                "SELECT val FROM test WHERE id = 1",
                &[SqlValue::Text("a".to_owned())],
            ),
            (
                "SELECT val FROM test WHERE id = 2",
                &[SqlValue::Text("b".to_owned())],
            ),
        ],
    );
}

// ─── Scenario F: Large transactional batch ─────────────────────────────

#[test]
fn txn_large_batch_commit() {
    let mut stmts: Vec<&str> = Vec::new();
    let owned: Vec<String>;
    {
        let mut v = Vec::with_capacity(1125);
        v.push("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)".to_owned());
        v.push("BEGIN".to_owned());
        for i in 1..=1000 {
            v.push(format!("INSERT INTO test VALUES ({i}, {})", i * 3));
        }
        for i in (10..=1000).step_by(10) {
            v.push(format!("UPDATE test SET val = {} WHERE id = {i}", i * 100));
        }
        for i in (5..=100).step_by(5) {
            v.push(format!("DELETE FROM test WHERE id = {i}"));
        }
        v.push("COMMIT".to_owned());
        owned = v;
    }
    stmts.extend(owned.iter().map(String::as_str));

    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    for sql in &stmts {
        let c_res = runner.csqlite().execute(sql);
        assert!(
            c_res.is_ok(),
            "csqlite: {:?} on {}",
            c_res.as_ref().err(),
            sql
        );
        let _ = c_res.unwrap();

        let f_res = runner.frank().execute(sql);
        assert!(
            f_res.is_ok(),
            "fsqlite: {:?} on {}",
            f_res.as_ref().err(),
            sql
        );
        let _ = f_res.unwrap();
    }

    // Expected: 1000 inserts - 20 deletes = 980 rows.
    let c_count = runner
        .csqlite()
        .query("SELECT COUNT(*) FROM test")
        .expect("csqlite count");
    let f_count = runner
        .frank()
        .query("SELECT COUNT(*) FROM test")
        .expect("fsqlite count");
    assert_eq!(c_count, f_count, "row counts differ");
    assert_eq!(c_count[0][0], SqlValue::Integer(980));

    // Verify an updated row (id=110 is divisible by 10 but not by 5 in
    // the delete range 5..=100, so it was updated but not deleted).
    let c_val = runner
        .csqlite()
        .query("SELECT val FROM test WHERE id = 110")
        .expect("csqlite check");
    let f_val = runner
        .frank()
        .query("SELECT val FROM test WHERE id = 110")
        .expect("fsqlite check");
    assert_eq!(c_val, f_val, "updated row differs");
    assert_eq!(c_val[0][0], SqlValue::Integer(11000));
}

// ─── Scenario G: Large batch rollback ──────────────────────────────────

#[test]
fn txn_large_batch_rollback() {
    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");

    let setup = [
        "CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)",
        "INSERT INTO test VALUES (1, 100)",
        "BEGIN",
    ];
    for sql in &setup {
        runner.csqlite().execute(sql).expect("csqlite setup");
        runner.frank().execute(sql).expect("fsqlite setup");
    }

    for i in 2..=500 {
        let sql = format!("INSERT INTO test VALUES ({i}, {i})");
        runner.csqlite().execute(&sql).expect("csqlite insert");
        runner.frank().execute(&sql).expect("fsqlite insert");
    }

    runner
        .csqlite()
        .execute("ROLLBACK")
        .expect("csqlite rollback");
    runner
        .frank()
        .execute("ROLLBACK")
        .expect("fsqlite rollback");

    let c_count = runner
        .csqlite()
        .query("SELECT COUNT(*) FROM test")
        .expect("csqlite count");
    let f_count = runner
        .frank()
        .query("SELECT COUNT(*) FROM test")
        .expect("fsqlite count");
    assert_eq!(c_count, f_count, "row counts differ after rollback");
    assert_eq!(c_count[0][0], SqlValue::Integer(1));
}

// ─── Scenario H: Savepoint release collapses into parent ───────────────

#[test]
fn txn_savepoint_release_collapses() {
    run_scenario(
        &[
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)",
            "BEGIN",
            "INSERT INTO test VALUES (1, 'before_sp')",
            "SAVEPOINT sp1",
            "INSERT INTO test VALUES (2, 'in_sp')",
            "RELEASE sp1",
            "COMMIT",
        ],
        &[
            ("SELECT COUNT(*) FROM test", &[SqlValue::Integer(2)]),
            (
                "SELECT val FROM test WHERE id = 2",
                &[SqlValue::Text("in_sp".to_owned())],
            ),
        ],
    );
}

// ─── Scenario I: Multiple savepoints, rollback middle one ──────────────

#[test]
fn txn_multiple_savepoints_rollback_middle() {
    run_scenario(
        &[
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)",
            "BEGIN",
            "INSERT INTO test VALUES (1, 'base')",
            "SAVEPOINT sp1",
            "INSERT INTO test VALUES (2, 'sp1_data')",
            "SAVEPOINT sp2",
            "INSERT INTO test VALUES (3, 'sp2_data')",
            "SAVEPOINT sp3",
            "INSERT INTO test VALUES (4, 'sp3_data')",
            "ROLLBACK TO sp2",
            "INSERT INTO test VALUES (5, 'after_rollback')",
            "RELEASE sp1",
            "COMMIT",
        ],
        &[
            ("SELECT COUNT(*) FROM test", &[SqlValue::Integer(3)]),
            (
                "SELECT val FROM test WHERE id = 1",
                &[SqlValue::Text("base".to_owned())],
            ),
            (
                "SELECT val FROM test WHERE id = 2",
                &[SqlValue::Text("sp1_data".to_owned())],
            ),
            (
                "SELECT COUNT(*) FROM test WHERE id IN (3, 4)",
                &[SqlValue::Integer(0)],
            ),
            (
                "SELECT val FROM test WHERE id = 5",
                &[SqlValue::Text("after_rollback".to_owned())],
            ),
        ],
    );
}

// ─── Scenario J: Rollback to savepoint then re-use same name ───────────

#[test]
fn txn_savepoint_reuse_name() {
    run_scenario(
        &[
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)",
            "BEGIN",
            "SAVEPOINT sp1",
            "INSERT INTO test VALUES (1, 'first')",
            "ROLLBACK TO sp1",
            "SAVEPOINT sp1",
            "INSERT INTO test VALUES (2, 'second')",
            "RELEASE sp1",
            "COMMIT",
        ],
        &[
            ("SELECT COUNT(*) FROM test", &[SqlValue::Integer(1)]),
            (
                "SELECT COUNT(*) FROM test WHERE id = 1",
                &[SqlValue::Integer(0)],
            ),
            (
                "SELECT val FROM test WHERE id = 2",
                &[SqlValue::Text("second".to_owned())],
            ),
        ],
    );
}

// ─── Scenario K: WAL/checkpoint/journal-mode parity transitions ───────

#[test]
fn txn_wal_checkpoint_journal_mode_transitions_file_backed() {
    const BEAD_ID: &str = "bd-1dp9.4.1";
    const SEED: u64 = 0x1D94_0401;
    let run_id = format!("bd-1dp9.4.1-seed-{SEED}-wal-checkpoint-journal-transitions");

    let tmp = tempdir().expect("tempdir");
    let c_path = tmp.path().join("oracle_csqlite.db");
    let f_path = tmp.path().join("candidate_fsqlite.db");

    let c_conn = rusqlite::Connection::open(&c_path).expect("open csqlite db");
    let f_conn =
        fsqlite::Connection::open(f_path.to_string_lossy().as_ref()).expect("open fsqlite db");

    eprintln!(
        "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=start c_db={} f_db={}",
        c_path.display(),
        f_path.display()
    );

    let c_mode_wal = csqlite_query_values(&c_conn, "PRAGMA journal_mode=WAL;");
    let f_mode_wal = fsqlite_query_values(&f_conn, "PRAGMA journal_mode=WAL;");
    eprintln!(
        "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=mode_switch_wal c_mode={c_mode_wal:?} f_mode={f_mode_wal:?}"
    );
    assert_eq!(c_mode_wal, f_mode_wal, "journal_mode WAL response mismatch");

    let setup_sql = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);",
        "INSERT INTO t VALUES (1, 'alpha');",
        "INSERT INTO t VALUES (2, 'beta');",
    ];
    for sql in setup_sql {
        c_conn.execute(sql, []).expect("csqlite setup exec");
        f_conn.execute(sql).expect("fsqlite setup exec");
    }

    let c_ckpt_passive = csqlite_query_values(&c_conn, "PRAGMA wal_checkpoint(PASSIVE);");
    let f_ckpt_passive = fsqlite_query_values(&f_conn, "PRAGMA wal_checkpoint(PASSIVE);");
    let c_triplet = checkpoint_triplet(&c_ckpt_passive, "csqlite passive");
    let f_triplet = checkpoint_triplet(&f_ckpt_passive, "fsqlite passive");
    eprintln!(
        "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=checkpoint_passive c={c_triplet:?} f={f_triplet:?}"
    );
    assert_eq!(c_triplet.0, 0, "csqlite passive busy should be 0");
    assert_eq!(f_triplet.0, 0, "fsqlite passive busy should be 0");
    assert!(c_triplet.1 >= 0 && c_triplet.2 >= 0);
    assert!(f_triplet.1 >= 0 && f_triplet.2 >= 0);

    let c_mode_delete = csqlite_query_values(&c_conn, "PRAGMA journal_mode='delete';");
    let f_mode_delete = fsqlite_query_values(&f_conn, "PRAGMA journal_mode='delete';");
    eprintln!(
        "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=mode_switch_delete c_mode={c_mode_delete:?} f_mode={f_mode_delete:?}"
    );
    assert_eq!(
        c_mode_delete, f_mode_delete,
        "journal_mode DELETE response mismatch"
    );

    let c_ckpt_nonwal = csqlite_query_values(&c_conn, "PRAGMA wal_checkpoint(TRUNCATE);");
    let f_ckpt_nonwal = fsqlite_query_values(&f_conn, "PRAGMA wal_checkpoint(TRUNCATE);");
    eprintln!(
        "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=checkpoint_nonwal c_rows={c_ckpt_nonwal:?} f_rows={f_ckpt_nonwal:?}"
    );
    assert_eq!(
        c_ckpt_nonwal, f_ckpt_nonwal,
        "non-WAL wal_checkpoint sentinel mismatch"
    );
    assert_eq!(
        f_ckpt_nonwal,
        vec![vec![
            SqlValue::Integer(0),
            SqlValue::Integer(-1),
            SqlValue::Integer(-1)
        ]],
        "expected SQLite sentinel row (0,-1,-1) in non-WAL mode"
    );

    let c_count = csqlite_query_values(&c_conn, "SELECT COUNT(*) FROM t;");
    let f_count = fsqlite_query_values(&f_conn, "SELECT COUNT(*) FROM t;");
    eprintln!(
        "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=visibility_after_nonwal_ckpt c_count={c_count:?} f_count={f_count:?}"
    );
    assert_eq!(
        c_count, f_count,
        "row visibility mismatch after non-WAL checkpoint"
    );
}

// ─── Scenario L: Journal mode PRAGMA response parity for all modes ────
//
// SQLite supports 6 journal modes: delete, truncate, persist, memory, wal, off.
// This test verifies that both engines return the same PRAGMA response string
// for each mode transition on a file-backed database.

#[test]
fn txn_journal_mode_all_modes_response_parity() {
    const BEAD_ID: &str = "bd-1dp9.4.1";
    const SEED: u64 = 0x1D94_0402;
    let run_id = format!("bd-1dp9.4.1-seed-{SEED}-journal-mode-all-modes");

    let tmp = tempdir().expect("tempdir");
    let c_path = tmp.path().join("oracle_csqlite_L.db");
    let f_path = tmp.path().join("candidate_fsqlite_L.db");

    let c_conn = rusqlite::Connection::open(&c_path).expect("open csqlite db");
    let f_conn =
        fsqlite::Connection::open(f_path.to_string_lossy().as_ref()).expect("open fsqlite db");

    eprintln!("bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=start");

    // Test each journal_mode transition. SQLite echoes back the mode that was
    // actually set (which may differ from the requested mode on some backends).
    let modes = [
        "wal", "delete", "truncate", "persist", "memory", "off", "wal",
    ];

    for mode in modes {
        let sql = format!("PRAGMA journal_mode='{mode}';");
        let c_resp = csqlite_query_values(&c_conn, &sql);
        let f_resp = fsqlite_query_values(&f_conn, &sql);
        eprintln!(
            "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=mode_set mode={mode} c={c_resp:?} f={f_resp:?}"
        );
        assert_eq!(
            c_resp, f_resp,
            "journal_mode response mismatch for mode='{mode}'"
        );

        // Query should also match.
        let c_query = csqlite_query_values(&c_conn, "PRAGMA journal_mode;");
        let f_query = fsqlite_query_values(&f_conn, "PRAGMA journal_mode;");
        assert_eq!(
            c_query, f_query,
            "journal_mode query mismatch after setting mode='{mode}'"
        );
    }

    // Verify data integrity is maintained through mode transitions.
    c_conn
        .execute("CREATE TABLE t_L(id INTEGER PRIMARY KEY, v TEXT)", [])
        .expect("csqlite create");
    f_conn
        .execute("CREATE TABLE t_L(id INTEGER PRIMARY KEY, v TEXT)")
        .expect("fsqlite create");
    c_conn
        .execute("INSERT INTO t_L VALUES (1, 'mode_test')", [])
        .expect("csqlite insert");
    f_conn
        .execute("INSERT INTO t_L VALUES (1, 'mode_test')")
        .expect("fsqlite insert");

    let c_count = csqlite_query_values(&c_conn, "SELECT COUNT(*) FROM t_L;");
    let f_count = fsqlite_query_values(&f_conn, "SELECT COUNT(*) FROM t_L;");
    eprintln!(
        "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=data_integrity c={c_count:?} f={f_count:?}"
    );
    assert_eq!(
        c_count, f_count,
        "data integrity mismatch after mode transitions"
    );
}

// ─── Scenario M: Checkpoint modes with WAL data ──────────────────────
//
// Verifies that all 4 checkpoint modes (PASSIVE, FULL, RESTART, TRUNCATE)
// return parity results when WAL contains actual data.

#[test]
fn txn_checkpoint_all_modes_with_data() {
    const BEAD_ID: &str = "bd-1dp9.4.1";
    const SEED: u64 = 0x1D94_0403;
    let run_id = format!("bd-1dp9.4.1-seed-{SEED}-checkpoint-all-modes");

    let tmp = tempdir().expect("tempdir");
    let c_path = tmp.path().join("oracle_csqlite_M.db");
    let f_path = tmp.path().join("candidate_fsqlite_M.db");

    let c_conn = rusqlite::Connection::open(&c_path).expect("open csqlite db");
    let f_conn =
        fsqlite::Connection::open(f_path.to_string_lossy().as_ref()).expect("open fsqlite db");

    // Ensure both in WAL mode.
    csqlite_query_values(&c_conn, "PRAGMA journal_mode=WAL;");
    fsqlite_query_values(&f_conn, "PRAGMA journal_mode=WAL;");

    // Disable auto-checkpoint so we can control when it happens.
    csqlite_query_values(&c_conn, "PRAGMA wal_autocheckpoint=0;");
    fsqlite_query_values(&f_conn, "PRAGMA wal_autocheckpoint=0;");

    eprintln!("bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=start");

    // Insert data to populate the WAL.
    let setup_sql = [
        "CREATE TABLE t_M(id INTEGER PRIMARY KEY, v TEXT);",
        "INSERT INTO t_M VALUES (1, 'alpha');",
        "INSERT INTO t_M VALUES (2, 'beta');",
        "INSERT INTO t_M VALUES (3, 'gamma');",
    ];
    for sql in setup_sql {
        c_conn.execute(sql, []).expect("csqlite setup");
        f_conn.execute(sql).expect("fsqlite setup");
    }

    // Test PASSIVE checkpoint with data in WAL.
    let c_passive = csqlite_query_values(&c_conn, "PRAGMA wal_checkpoint(PASSIVE);");
    let f_passive = fsqlite_query_values(&f_conn, "PRAGMA wal_checkpoint(PASSIVE);");
    let c_trip = checkpoint_triplet(&c_passive, "csqlite passive");
    let f_trip = checkpoint_triplet(&f_passive, "fsqlite passive");
    eprintln!(
        "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=checkpoint_passive c={c_trip:?} f={f_trip:?}"
    );
    // busy=0 for both (no concurrent readers in single-connection test).
    assert_eq!(c_trip.0, 0, "csqlite passive busy");
    assert_eq!(f_trip.0, 0, "fsqlite passive busy");

    // Add more data for next checkpoint.
    for i in 4..=6 {
        let sql = format!("INSERT INTO t_M VALUES ({i}, 'row_{i}');");
        c_conn.execute(&sql, []).expect("csqlite insert");
        f_conn.execute(&sql).expect("fsqlite insert");
    }

    // Test FULL checkpoint.
    let c_full = csqlite_query_values(&c_conn, "PRAGMA wal_checkpoint(FULL);");
    let f_full = fsqlite_query_values(&f_conn, "PRAGMA wal_checkpoint(FULL);");
    let c_trip = checkpoint_triplet(&c_full, "csqlite full");
    let f_trip = checkpoint_triplet(&f_full, "fsqlite full");
    eprintln!(
        "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=checkpoint_full c={c_trip:?} f={f_trip:?}"
    );
    assert_eq!(c_trip.0, 0, "csqlite full busy");
    assert_eq!(f_trip.0, 0, "fsqlite full busy");

    // Add more data for RESTART checkpoint.
    for i in 7..=9 {
        let sql = format!("INSERT INTO t_M VALUES ({i}, 'row_{i}');");
        c_conn.execute(&sql, []).expect("csqlite insert");
        f_conn.execute(&sql).expect("fsqlite insert");
    }

    // Test RESTART checkpoint (resets WAL after checkpoint).
    let c_restart = csqlite_query_values(&c_conn, "PRAGMA wal_checkpoint(RESTART);");
    let f_restart = fsqlite_query_values(&f_conn, "PRAGMA wal_checkpoint(RESTART);");
    let c_trip = checkpoint_triplet(&c_restart, "csqlite restart");
    let f_trip = checkpoint_triplet(&f_restart, "fsqlite restart");
    eprintln!(
        "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=checkpoint_restart c={c_trip:?} f={f_trip:?}"
    );
    assert_eq!(c_trip.0, 0, "csqlite restart busy");
    assert_eq!(f_trip.0, 0, "fsqlite restart busy");

    // Add more data for TRUNCATE checkpoint.
    for i in 10..=12 {
        let sql = format!("INSERT INTO t_M VALUES ({i}, 'row_{i}');");
        c_conn.execute(&sql, []).expect("csqlite insert");
        f_conn.execute(&sql).expect("fsqlite insert");
    }

    // Test TRUNCATE checkpoint (truncates WAL to zero after checkpoint).
    let c_truncate = csqlite_query_values(&c_conn, "PRAGMA wal_checkpoint(TRUNCATE);");
    let f_truncate = fsqlite_query_values(&f_conn, "PRAGMA wal_checkpoint(TRUNCATE);");
    let c_trip = checkpoint_triplet(&c_truncate, "csqlite truncate");
    let f_trip = checkpoint_triplet(&f_truncate, "fsqlite truncate");
    eprintln!(
        "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=checkpoint_truncate c={c_trip:?} f={f_trip:?}"
    );
    assert_eq!(c_trip.0, 0, "csqlite truncate busy");
    assert_eq!(f_trip.0, 0, "fsqlite truncate busy");

    // Verify all data is accessible after all checkpoint modes.
    let c_count = csqlite_query_values(&c_conn, "SELECT COUNT(*) FROM t_M;");
    let f_count = fsqlite_query_values(&f_conn, "SELECT COUNT(*) FROM t_M;");
    eprintln!(
        "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=final_data_check c={c_count:?} f={f_count:?}"
    );
    assert_eq!(
        c_count, f_count,
        "row count mismatch after all checkpoint modes"
    );
    assert_eq!(
        f_count,
        vec![vec![SqlValue::Integer(12)]],
        "expected 12 rows after all inserts"
    );
}
