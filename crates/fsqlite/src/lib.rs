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
}
