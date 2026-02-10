//! Differential comparison engine — run identical SQL against FrankenSQLite and
//! C SQLite (via rusqlite) and compare results.

use rusqlite::Connection as CConnection;

use crate::{E2eError, E2eResult};

/// A single row result represented as a vector of stringified column values.
pub type Row = Vec<String>;

/// Outcome of executing a SQL statement against one engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StmtOutcome {
    /// Statement returned rows.
    Rows(Vec<Row>),
    /// Statement executed successfully with `n` affected rows.
    Execute(usize),
    /// Statement failed with an error message.
    Error(String),
}

/// Run a sequence of SQL statements against C SQLite (rusqlite) and collect
/// outcomes.
///
/// # Errors
///
/// Returns `E2eError::Rusqlite` if the connection itself fails to open.
pub fn run_csqlite(db_path: &str, statements: &[String]) -> E2eResult<Vec<StmtOutcome>> {
    let conn = CConnection::open(db_path)?;
    let mut outcomes = Vec::with_capacity(statements.len());

    for stmt in statements {
        let outcome = execute_csqlite_stmt(&conn, stmt);
        outcomes.push(outcome);
    }

    Ok(outcomes)
}

/// Execute a single statement against a rusqlite connection.
fn execute_csqlite_stmt(conn: &CConnection, sql: &str) -> StmtOutcome {
    let trimmed = sql.trim();
    let is_query = trimmed
        .split_whitespace()
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case("SELECT"));

    if is_query {
        match conn.prepare(trimmed) {
            Ok(mut prepared) => {
                let col_count = prepared.column_count();
                match prepared.query_map([], |row| {
                    let mut cols = Vec::with_capacity(col_count);
                    for i in 0..col_count {
                        let val: String = row
                            .get::<_, rusqlite::types::Value>(i)
                            .map_or_else(|e| format!("ERR:{e}"), |v| format!("{v:?}"));
                        cols.push(val);
                    }
                    Ok(cols)
                }) {
                    Ok(rows) => {
                        let collected: Vec<Row> = rows.filter_map(Result::ok).collect();
                        StmtOutcome::Rows(collected)
                    }
                    Err(e) => StmtOutcome::Error(e.to_string()),
                }
            }
            Err(e) => StmtOutcome::Error(e.to_string()),
        }
    } else {
        match conn.execute(trimmed, []) {
            Ok(n) => StmtOutcome::Execute(n),
            Err(e) => StmtOutcome::Error(e.to_string()),
        }
    }
}

/// Compare two sequences of outcomes and return divergences.
///
/// Returns a list of `(index, csqlite_outcome, fsqlite_outcome)` for each
/// position where the results differ.
#[must_use]
pub fn find_divergences(
    csqlite: &[StmtOutcome],
    fsqlite: &[StmtOutcome],
) -> Vec<(usize, StmtOutcome, StmtOutcome)> {
    csqlite
        .iter()
        .zip(fsqlite.iter())
        .enumerate()
        .filter(|(_, (c, f))| c != f)
        .map(|(i, (c, f))| (i, c.clone(), f.clone()))
        .collect()
}

/// Run comparison and return an error if any divergences are found.
///
/// # Errors
///
/// Returns `E2eError::Divergence` listing the first divergent statement.
pub fn assert_no_divergences(csqlite: &[StmtOutcome], fsqlite: &[StmtOutcome]) -> E2eResult<()> {
    let divs = find_divergences(csqlite, fsqlite);
    if divs.is_empty() {
        Ok(())
    } else {
        let (idx, ref c, ref f) = divs[0];
        Err(E2eError::Divergence(format!(
            "statement {idx}: csqlite={c:?}, fsqlite={f:?}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_divergences_identical() {
        let a = vec![StmtOutcome::Execute(1), StmtOutcome::Execute(0)];
        let b = a.clone();
        assert!(find_divergences(&a, &b).is_empty());
    }

    #[test]
    fn test_find_divergences_different() {
        let a = vec![StmtOutcome::Execute(1)];
        let b = vec![StmtOutcome::Execute(2)];
        let divs = find_divergences(&a, &b);
        assert_eq!(divs.len(), 1);
        assert_eq!(divs[0].0, 0);
    }

    #[test]
    fn test_csqlite_basic_roundtrip() {
        let stmts = vec![
            "CREATE TABLE x (id INTEGER PRIMARY KEY, v TEXT)".to_owned(),
            "INSERT INTO x VALUES (1, 'hello')".to_owned(),
            "SELECT * FROM x".to_owned(),
        ];
        let outcomes = run_csqlite(":memory:", &stmts).unwrap();
        assert_eq!(outcomes.len(), 3);
        // CREATE TABLE
        assert!(matches!(outcomes[0], StmtOutcome::Execute(0)));
        // INSERT
        assert!(matches!(outcomes[1], StmtOutcome::Execute(1)));
        // SELECT
        assert!(matches!(outcomes[2], StmtOutcome::Rows(ref r) if r.len() == 1));
    }
}
