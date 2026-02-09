//! Public API facade for FrankenSQLite.
//!
//! This crate will grow a stable, ergonomic API surface over time. In early
//! phases it also re-exports selected internal crates for integration tests.

pub use fsqlite_core::connection::{Connection, PreparedStatement, Row};
pub use fsqlite_vfs;

#[cfg(test)]
mod tests {
    use super::Connection;
    use fsqlite_error::FrankenError;
    use fsqlite_types::value::SqliteValue;

    fn row_values(row: &super::Row) -> Vec<SqliteValue> {
        row.values().to_vec()
    }

    #[test]
    fn test_connection_open_and_path() {
        let conn = Connection::open(":memory:").expect("in-memory connection should open");
        assert_eq!(conn.path(), ":memory:");
    }

    #[test]
    fn test_public_api_query_expression() {
        let conn = Connection::open(":memory:").expect("in-memory connection should open");
        let rows = conn
            .query("SELECT 1 + 2, 'ab' || 'cd';")
            .expect("query should succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(3),
                SqliteValue::Text("abcd".to_owned()),
            ]
        );
    }

    #[test]
    fn test_public_api_query_with_params() {
        let conn = Connection::open(":memory:").expect("in-memory connection should open");
        let rows = conn
            .query_with_params(
                "SELECT ?1 + ?2, ?3;",
                &[
                    SqliteValue::Integer(4),
                    SqliteValue::Integer(5),
                    SqliteValue::Text("ok".to_owned()),
                ],
            )
            .expect("query_with_params should succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(9), SqliteValue::Text("ok".to_owned())]
        );
    }

    #[test]
    fn test_public_api_query_row_returns_first_row() {
        let conn = Connection::open(":memory:").expect("in-memory connection should open");
        let row = conn
            .query_row("VALUES (10), (20), (30);")
            .expect("query_row should return first row");
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(10)]);
    }

    #[test]
    fn test_public_api_query_row_empty_error() {
        let conn = Connection::open(":memory:").expect("in-memory connection should open");
        let error = conn
            .query_row("SELECT 1 WHERE 0;")
            .expect_err("query_row should fail for empty result set");
        assert!(matches!(error, FrankenError::QueryReturnedNoRows));
    }

    #[test]
    fn test_public_api_execute_returns_row_count() {
        let conn = Connection::open(":memory:").expect("in-memory connection should open");
        let count = conn
            .execute("VALUES (1), (2), (3);")
            .expect("execute should succeed");
        assert_eq!(count, 3);
    }

    // ── Connection::open error paths ────────────────────────────────────

    #[test]
    fn open_empty_path_fails() {
        let err = Connection::open("").expect_err("empty path should fail");
        assert!(matches!(err, FrankenError::CannotOpen { .. }));
    }

    // ── Row accessors ────────────────────────────────────────────────────

    #[test]
    fn row_get_valid_index() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 42, 'hello';").unwrap();
        assert_eq!(row.get(0), Some(&SqliteValue::Integer(42)));
        assert_eq!(row.get(1), Some(&SqliteValue::Text("hello".to_owned())));
    }

    #[test]
    fn row_get_out_of_bounds() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 1;").unwrap();
        assert_eq!(row.get(99), None);
    }

    #[test]
    fn row_values_returns_all_columns() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 1, 2, 3;").unwrap();
        assert_eq!(row.values().len(), 3);
    }

    // ── PreparedStatement ────────────────────────────────────────────────

    #[test]
    fn prepared_query() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("SELECT 7 * 6;").unwrap();
        let rows = stmt.query().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn prepared_query_with_params() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("SELECT ?1 + ?2;").unwrap();
        let rows = stmt
            .query_with_params(&[SqliteValue::Integer(10), SqliteValue::Integer(20)])
            .unwrap();
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(30)]);
    }

    #[test]
    fn prepared_query_row() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("SELECT 99;").unwrap();
        let row = stmt.query_row().unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(99)]);
    }

    #[test]
    fn prepared_query_row_with_params() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("SELECT ?1;").unwrap();
        let row = stmt
            .query_row_with_params(&[SqliteValue::Text("xyz".to_owned())])
            .unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Text("xyz".to_owned())]);
    }

    #[test]
    fn prepared_execute() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("VALUES (1), (2);").unwrap();
        assert_eq!(stmt.execute().unwrap(), 2);
    }

    #[test]
    fn prepared_execute_with_params() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("SELECT ?1;").unwrap();
        assert_eq!(
            stmt.execute_with_params(&[SqliteValue::Integer(1)])
                .unwrap(),
            1
        );
    }

    #[test]
    fn prepared_explain_not_empty() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("SELECT 1 + 2;").unwrap();
        let explain = stmt.explain();
        assert!(!explain.is_empty());
    }

    // ── Connection::query_row_with_params ────────────────────────────────

    #[test]
    fn query_row_with_params() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row_with_params("SELECT ?1 * 2;", &[SqliteValue::Integer(5)])
            .unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(10)]);
    }

    // ── Connection::execute_with_params ──────────────────────────────────

    #[test]
    fn execute_with_params_returns_count() {
        let conn = Connection::open(":memory:").unwrap();
        let count = conn
            .execute_with_params("SELECT ?1;", &[SqliteValue::Integer(1)])
            .unwrap();
        assert_eq!(count, 1);
    }

    // ── DDL ──────────────────────────────────────────────────────────────

    #[test]
    fn create_table_and_insert_select() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER, b TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'one');").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2, 'two');").unwrap();
        let rows = conn.query("SELECT a, b FROM t1;").unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn create_table_if_not_exists_no_error() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (x INTEGER);").unwrap();
        // Should not error with IF NOT EXISTS
        conn.execute("CREATE TABLE IF NOT EXISTS t1 (x INTEGER);")
            .unwrap();
    }

    #[test]
    fn create_duplicate_table_errors() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (x INTEGER);").unwrap();
        let err = conn
            .execute("CREATE TABLE t1 (x INTEGER);")
            .expect_err("duplicate table should fail");
        assert!(matches!(err, FrankenError::Internal(_)));
    }

    // ── DML: UPDATE / DELETE ─────────────────────────────────────────────

    #[test]
    fn update_modifies_rows() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (10);").unwrap();
        conn.execute("INSERT INTO t VALUES (20);").unwrap();
        conn.execute("UPDATE t SET v = 99 WHERE v = 10;").unwrap();
        let rows = conn.query("SELECT v FROM t;").unwrap();
        let vals: Vec<_> = rows.iter().map(row_values).collect();
        assert!(vals.contains(&vec![SqliteValue::Integer(99)]));
        assert!(vals.contains(&vec![SqliteValue::Integer(20)]));
    }

    #[test]
    fn delete_removes_rows() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        conn.execute("INSERT INTO t VALUES (3);").unwrap();
        conn.execute("DELETE FROM t WHERE v = 2;").unwrap();
        let rows = conn.query("SELECT v FROM t;").unwrap();
        assert_eq!(rows.len(), 2);
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert!(vals.contains(&SqliteValue::Integer(1)));
        assert!(vals.contains(&SqliteValue::Integer(3)));
    }

    // ── Type handling ────────────────────────────────────────────────────

    #[test]
    fn null_value_roundtrip() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT NULL;").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Null]);
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn real_value_roundtrip() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 3.14;").unwrap();
        if let SqliteValue::Float(v) = &row_values(&row)[0] {
            assert!((*v - 3.14).abs() < f64::EPSILON);
        } else {
            panic!("expected Real value");
        }
    }

    #[test]
    fn text_value_roundtrip() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 'hello world';").unwrap();
        assert_eq!(
            row_values(&row),
            vec![SqliteValue::Text("hello world".to_owned())]
        );
    }

    #[test]
    fn blob_value_via_params() {
        let conn = Connection::open(":memory:").unwrap();
        let blob = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let row = conn
            .query_row_with_params("SELECT ?1;", &[SqliteValue::Blob(blob.clone())])
            .unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Blob(blob)]);
    }

    // ── Transaction control ──────────────────────────────────────────────

    #[test]
    fn in_transaction_flag() {
        let conn = Connection::open(":memory:").unwrap();
        assert!(!conn.in_transaction());
        conn.execute("BEGIN;").unwrap();
        assert!(conn.in_transaction());
        conn.execute("COMMIT;").unwrap();
        assert!(!conn.in_transaction());
    }

    #[test]
    fn begin_commit_persists_changes() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        conn.execute("BEGIN;").unwrap();
        conn.execute("INSERT INTO t VALUES (42);").unwrap();
        conn.execute("COMMIT;").unwrap();
        let rows = conn.query("SELECT v FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn rollback_reverts_changes() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("BEGIN;").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        conn.execute("ROLLBACK;").unwrap();
        let rows = conn.query("SELECT v FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn nested_begin_errors() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("BEGIN;").unwrap();
        let err = conn
            .execute("BEGIN;")
            .expect_err("nested begin should fail");
        assert!(matches!(err, FrankenError::Internal(_)));
    }

    #[test]
    fn commit_without_transaction_errors() {
        let conn = Connection::open(":memory:").unwrap();
        let err = conn
            .execute("COMMIT;")
            .expect_err("commit without txn should fail");
        assert!(matches!(err, FrankenError::Internal(_)));
    }

    #[test]
    fn rollback_without_transaction_errors() {
        let conn = Connection::open(":memory:").unwrap();
        let err = conn
            .execute("ROLLBACK;")
            .expect_err("rollback without txn should fail");
        assert!(matches!(err, FrankenError::Internal(_)));
    }

    // ── Savepoint ────────────────────────────────────────────────────────

    #[test]
    fn savepoint_and_rollback_to() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("SAVEPOINT sp1;").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        conn.execute("ROLLBACK TO sp1;").unwrap();
        let rows = conn.query("SELECT v FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn savepoint_release_commits_changes() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        conn.execute("SAVEPOINT sp1;").unwrap();
        conn.execute("INSERT INTO t VALUES (100);").unwrap();
        conn.execute("RELEASE sp1;").unwrap();
        let rows = conn.query("SELECT v FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(100)]);
    }

    #[test]
    fn release_nonexistent_savepoint_errors() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("BEGIN;").unwrap();
        let err = conn
            .execute("RELEASE nosuch;")
            .expect_err("release nonexistent savepoint should fail");
        assert!(matches!(err, FrankenError::Internal(_)));
    }

    // ── Parse error ──────────────────────────────────────────────────────

    #[test]
    fn parse_error_on_invalid_sql() {
        let conn = Connection::open(":memory:").unwrap();
        assert!(conn.query("NOT VALID SQL;").is_err());
    }

    // ── Multiple statements ──────────────────────────────────────────────

    #[test]
    fn multiple_statements_in_query() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        // query() processes all statements, returns rows from last
        let rows = conn
            .query("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); SELECT v FROM t;")
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    // ── Expression arithmetic ────────────────────────────────────────────

    #[test]
    fn arithmetic_expressions() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 10 - 3, 4 * 5, 20 / 4;").unwrap();
        assert_eq!(
            row_values(&row),
            vec![
                SqliteValue::Integer(7),
                SqliteValue::Integer(20),
                SqliteValue::Integer(5),
            ]
        );
    }

    #[test]
    fn string_concatenation() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 'foo' || 'bar';").unwrap();
        assert_eq!(
            row_values(&row),
            vec![SqliteValue::Text("foobar".to_owned())]
        );
    }
}
