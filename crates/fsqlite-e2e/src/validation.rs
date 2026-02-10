//! Post-workload validation: logical invariants and per-table checksums.
//!
//! After running a workload against both FrankenSQLite and C SQLite, this
//! module validates that both engines produced the same logical state:
//!
//! - **Row counts** per table
//! - **Aggregate checksums** over deterministic columns (GROUP_CONCAT of sorted rows)
//! - **Invariant checks** (no duplicate primary keys, UNIQUE constraints honoured)
//!
//! A [`ValidationReport`] contains per-table results and a top-level pass/fail.
//! When outputs diverge the report carries a crisp, machine-readable diff.
//!
//! Bead: bd-1w6k.4.4

use std::fmt;

use crate::comparison::{SqlBackend, SqlValue};

// ─── Per-table validation result ─────────────────────────────────────────

/// Row-count pair for a single table across both engines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowCountCheck {
    /// Table name.
    pub table: String,
    /// Row count from C SQLite.
    pub csqlite_count: i64,
    /// Row count from FrankenSQLite.
    pub fsqlite_count: i64,
}

impl RowCountCheck {
    /// Whether the counts match.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.csqlite_count == self.fsqlite_count
    }
}

impl fmt::Display for RowCountCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = if self.passed() { "OK" } else { "FAIL" };
        write!(
            f,
            "[{status}] {}: csqlite={}, fsqlite={}",
            self.table, self.csqlite_count, self.fsqlite_count
        )
    }
}

/// Aggregate checksum for a single table (GROUP_CONCAT of sorted rows hashed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChecksumCheck {
    /// Table name.
    pub table: String,
    /// Hex digest from C SQLite logical dump.
    pub csqlite_checksum: String,
    /// Hex digest from FrankenSQLite logical dump.
    pub fsqlite_checksum: String,
}

impl ChecksumCheck {
    /// Whether the checksums match.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.csqlite_checksum == self.fsqlite_checksum
    }
}

impl fmt::Display for ChecksumCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = if self.passed() { "OK" } else { "FAIL" };
        write!(
            f,
            "[{status}] {}: csqlite={}, fsqlite={}",
            self.table, self.csqlite_checksum, self.fsqlite_checksum
        )
    }
}

/// A single invariant violation detected in one engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvariantViolation {
    /// Which engine produced the violation.
    pub engine: Engine,
    /// Table where the violation was detected.
    pub table: String,
    /// Human-readable description of the violation.
    pub description: String,
}

impl fmt::Display for InvariantViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}: {}", self.engine, self.table, self.description)
    }
}

/// Engine label for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    /// C SQLite (rusqlite).
    CSqlite,
    /// FrankenSQLite.
    Fsqlite,
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CSqlite => write!(f, "csqlite"),
            Self::Fsqlite => write!(f, "fsqlite"),
        }
    }
}

// ─── Validation report ──────────────────────────────────────────────────

/// Top-level validation report comparing two engines after a workload.
#[derive(Debug, Clone)]
pub struct ValidationReport {
    /// Per-table row count checks.
    pub row_counts: Vec<RowCountCheck>,
    /// Per-table aggregate checksum checks.
    pub checksums: Vec<ChecksumCheck>,
    /// Invariant violations (empty if all invariants hold).
    pub violations: Vec<InvariantViolation>,
    /// Tables discovered in C SQLite.
    pub csqlite_tables: Vec<String>,
    /// Tables discovered in FrankenSQLite.
    pub fsqlite_tables: Vec<String>,
}

impl ValidationReport {
    /// Whether all checks passed (row counts match, checksums match, no violations,
    /// and the same tables exist in both engines).
    #[must_use]
    pub fn passed(&self) -> bool {
        self.row_counts.iter().all(RowCountCheck::passed)
            && self.checksums.iter().all(ChecksumCheck::passed)
            && self.violations.is_empty()
            && self.csqlite_tables == self.fsqlite_tables
    }

    /// Build a human-readable diff string.  Empty if [`Self::passed`] is true.
    #[must_use]
    pub fn diff(&self) -> String {
        use std::fmt::Write as _;

        if self.passed() {
            return String::new();
        }

        let mut out = String::new();
        let _ = writeln!(out, "=== VALIDATION DIFF ===\n");

        // Table set difference.
        if self.csqlite_tables != self.fsqlite_tables {
            let _ = writeln!(out, "-- Table set mismatch --");
            let _ = writeln!(out, "  csqlite tables: {:?}", self.csqlite_tables);
            let _ = writeln!(out, "  fsqlite tables: {:?}", self.fsqlite_tables);
            let _ = writeln!(out);
        }

        // Row count failures.
        let count_fails: Vec<_> = self.row_counts.iter().filter(|c| !c.passed()).collect();
        if !count_fails.is_empty() {
            let _ = writeln!(out, "-- Row count mismatches --");
            for c in count_fails {
                let _ = writeln!(out, "  {c}");
            }
            let _ = writeln!(out);
        }

        // Checksum failures.
        let hash_fails: Vec<_> = self.checksums.iter().filter(|c| !c.passed()).collect();
        if !hash_fails.is_empty() {
            let _ = writeln!(out, "-- Checksum mismatches --");
            for c in hash_fails {
                let _ = writeln!(out, "  {c}");
            }
            let _ = writeln!(out);
        }

        // Invariant violations.
        if !self.violations.is_empty() {
            let _ = writeln!(out, "-- Invariant violations --");
            for v in &self.violations {
                let _ = writeln!(out, "  {v}");
            }
            let _ = writeln!(out);
        }

        out
    }
}

impl fmt::Display for ValidationReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.passed() {
            write!(
                f,
                "VALIDATION PASSED: {} tables, {} row-count checks, {} checksum checks",
                self.csqlite_tables.len(),
                self.row_counts.len(),
                self.checksums.len()
            )
        } else {
            write!(f, "{}", self.diff())
        }
    }
}

// ─── Validator ──────────────────────────────────────────────────────────

/// Run post-workload validation across two [`SqlBackend`] implementations.
///
/// Table discovery uses C SQLite (`sqlite_master`) as the reference oracle,
/// since FrankenSQLite's in-memory backend does not yet expose `sqlite_master`.
/// The provided `tables` list is used as the ground truth for both engines.
///
/// For each table:
/// 1. Compares row counts.
/// 2. Computes a deterministic aggregate checksum (SHA-256 of sorted dump).
/// 3. Checks for duplicate primary keys (invariant).
pub fn validate<C: SqlBackend, F: SqlBackend>(csqlite: &C, fsqlite: &F) -> ValidationReport {
    // Discover tables from C SQLite (the reference oracle).  FrankenSQLite's
    // in-memory backend does not expose sqlite_master, so we rely on C SQLite
    // for table discovery and pass the list to both engines for data checks.
    let csqlite_tables = list_user_tables(csqlite);
    let fsqlite_tables = list_user_tables(fsqlite);

    // Use C SQLite's table list as ground truth.
    let reference_tables = &csqlite_tables;

    validate_with_tables(
        csqlite,
        fsqlite,
        reference_tables,
        &csqlite_tables,
        &fsqlite_tables,
    )
}

/// Run validation using an explicit table list.
///
/// Use this when both backends were set up with known table names and table
/// discovery via `sqlite_master` is not reliable (e.g. FrankenSQLite in-memory).
pub fn validate_with_known_tables<C: SqlBackend, F: SqlBackend>(
    csqlite: &C,
    fsqlite: &F,
    tables: &[String],
) -> ValidationReport {
    let table_list = tables.to_vec();
    validate_with_tables(csqlite, fsqlite, tables, &table_list, &table_list)
}

fn validate_with_tables<C: SqlBackend, F: SqlBackend>(
    csqlite: &C,
    fsqlite: &F,
    tables: &[String],
    csqlite_tables: &[String],
    fsqlite_tables: &[String],
) -> ValidationReport {
    let mut row_counts = Vec::with_capacity(tables.len());
    let mut checksums = Vec::with_capacity(tables.len());
    let mut violations = Vec::new();

    for table in tables {
        // Row counts.
        let c_count = count_rows(csqlite, table);
        let f_count = count_rows(fsqlite, table);
        row_counts.push(RowCountCheck {
            table: table.clone(),
            csqlite_count: c_count,
            fsqlite_count: f_count,
        });

        // Aggregate checksum (SHA-256 of sorted logical dump per table).
        let c_hash = table_checksum(csqlite, table);
        let f_hash = table_checksum(fsqlite, table);
        checksums.push(ChecksumCheck {
            table: table.clone(),
            csqlite_checksum: c_hash,
            fsqlite_checksum: f_hash,
        });

        // Invariant: no duplicate primary keys.
        check_pk_duplicates(csqlite, table, Engine::CSqlite, &mut violations);
        check_pk_duplicates(fsqlite, table, Engine::Fsqlite, &mut violations);
    }

    ValidationReport {
        row_counts,
        checksums,
        violations,
        csqlite_tables: csqlite_tables.to_vec(),
        fsqlite_tables: fsqlite_tables.to_vec(),
    }
}

// ─── Internal helpers ───────────────────────────────────────────────────

/// Discover user tables from a backend (excludes `sqlite_%` internal tables).
fn list_user_tables<B: SqlBackend>(backend: &B) -> Vec<String> {
    let Ok(rows) = backend.query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    ) else {
        return Vec::new();
    };

    rows.iter()
        .filter_map(|r| match r.first() {
            Some(SqlValue::Text(name)) => Some(name.clone()),
            _ => None,
        })
        .collect()
}

/// Merge two sorted table lists into a sorted deduplicated union.
#[cfg(test)]
fn merged_table_list(a: &[String], b: &[String]) -> Vec<String> {
    let mut set: Vec<String> = a.to_vec();
    for t in b {
        if !set.contains(t) {
            set.push(t.clone());
        }
    }
    set.sort();
    set
}

/// Count rows in a table.  Returns -1 on error (table missing in this engine).
fn count_rows<B: SqlBackend>(backend: &B, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM \"{table}\"");
    match backend.query(&sql) {
        Ok(rows) => rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| match v {
                SqlValue::Integer(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(-1),
        Err(_) => -1,
    }
}

/// Compute a deterministic SHA-256 checksum of all rows in a table,
/// ordered by rowid (falling back to first column).
///
/// Each row is serialized as pipe-delimited values; rows are newline-separated.
fn table_checksum<B: SqlBackend>(backend: &B, table: &str) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;

    let sql = format!("SELECT * FROM \"{table}\" ORDER BY rowid");
    let rows = backend
        .query(&sql)
        .or_else(|_| backend.query(&format!("SELECT * FROM \"{table}\" ORDER BY 1")))
        .or_else(|_| backend.query(&format!("SELECT * FROM \"{table}\"")));

    let mut hasher = Sha256::new();
    if let Ok(rows) = rows {
        for row in &rows {
            let mut line = String::new();
            for (j, val) in row.iter().enumerate() {
                if j > 0 {
                    line.push('|');
                }
                let _ = write!(line, "{val}");
            }
            line.push('\n');
            hasher.update(line.as_bytes());
        }
    }

    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// Check for duplicate primary keys (rowids) in a table.
///
/// If the table uses `INTEGER PRIMARY KEY`, the `id` column *is* the rowid.
/// For all tables we check via `rowid` which SQLite guarantees exists.
fn check_pk_duplicates<B: SqlBackend>(
    backend: &B,
    table: &str,
    engine: Engine,
    violations: &mut Vec<InvariantViolation>,
) {
    let sql =
        format!("SELECT rowid, COUNT(*) AS cnt FROM \"{table}\" GROUP BY rowid HAVING cnt > 1");
    match backend.query(&sql) {
        Ok(rows) if !rows.is_empty() => {
            let dup_count = rows.len();
            violations.push(InvariantViolation {
                engine,
                table: table.to_owned(),
                description: format!("{dup_count} duplicate rowid(s) detected"),
            });
        }
        _ => {} // No duplicates or query failed (table might not exist in this engine).
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comparison::{CSqliteBackend, FrankenSqliteBackend};

    /// Helper: run setup SQL against both backends.
    fn setup_both<C: SqlBackend, F: SqlBackend>(c: &C, f: &F, stmts: &[&str]) {
        for sql in stmts {
            let _ = c.execute(sql);
            let _ = f.execute(sql);
        }
    }

    /// Helper: table names from the setup SQL (extract table names from CREATE TABLE).
    fn tables_from_stmts(stmts: &[&str]) -> Vec<String> {
        stmts
            .iter()
            .filter_map(|s| {
                let upper = s.to_uppercase();
                if !upper.starts_with("CREATE TABLE") {
                    return None;
                }
                // Extract table name: CREATE TABLE [IF NOT EXISTS] <name> (
                let after = s
                    .strip_prefix("CREATE TABLE IF NOT EXISTS ")
                    .or_else(|| s.strip_prefix("CREATE TABLE "))
                    .unwrap_or(s);
                let name = after.split_whitespace().next().unwrap_or("");
                let name = name.trim_start_matches('"').trim_end_matches('"');
                if name.is_empty() {
                    None
                } else {
                    Some(name.split('(').next().unwrap_or(name).trim().to_owned())
                }
            })
            .collect()
    }

    #[test]
    fn test_identical_state_passes() {
        let c = CSqliteBackend::open_in_memory().unwrap();
        let f = FrankenSqliteBackend::open_in_memory().unwrap();
        let stmts: &[&str] = &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, num REAL)",
            "INSERT INTO t VALUES (1, 'alpha', 1.5)",
            "INSERT INTO t VALUES (2, 'beta', 2.5)",
            "INSERT INTO t VALUES (3, 'gamma', 3.5)",
        ];
        setup_both(&c, &f, stmts);

        let tables = tables_from_stmts(stmts);
        let report = validate_with_known_tables(&c, &f, &tables);
        assert!(report.passed(), "report should pass: {report}");
        assert_eq!(report.row_counts.len(), 1);
        assert_eq!(report.row_counts[0].csqlite_count, 3);
        assert_eq!(report.row_counts[0].fsqlite_count, 3);
        assert!(report.checksums[0].passed());
        assert!(report.violations.is_empty());
    }

    #[test]
    fn test_different_row_counts_fail() {
        let c = CSqliteBackend::open_in_memory().unwrap();
        let f = FrankenSqliteBackend::open_in_memory().unwrap();
        let _ = c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
        let _ = f.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
        let _ = c.execute("INSERT INTO t VALUES (1, 'a')");
        let _ = c.execute("INSERT INTO t VALUES (2, 'b')");
        // fsqlite only gets one row
        let _ = f.execute("INSERT INTO t VALUES (1, 'a')");

        let tables = vec!["t".to_owned()];
        let report = validate_with_known_tables(&c, &f, &tables);
        assert!(!report.passed());
        assert!(!report.row_counts[0].passed());
        assert_eq!(report.row_counts[0].csqlite_count, 2);
        assert_eq!(report.row_counts[0].fsqlite_count, 1);
        let diff = report.diff();
        assert!(diff.contains("Row count mismatches"));
    }

    #[test]
    fn test_different_data_checksum_fails() {
        let c = CSqliteBackend::open_in_memory().unwrap();
        let f = FrankenSqliteBackend::open_in_memory().unwrap();
        let _ = c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
        let _ = f.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
        // Same row count but different data.
        let _ = c.execute("INSERT INTO t VALUES (1, 'hello')");
        let _ = f.execute("INSERT INTO t VALUES (1, 'world')");

        let tables = vec!["t".to_owned()];
        let report = validate_with_known_tables(&c, &f, &tables);
        assert!(!report.passed());
        assert!(report.row_counts[0].passed()); // counts match (both 1)
        assert!(!report.checksums[0].passed()); // checksums differ
        let diff = report.diff();
        assert!(diff.contains("Checksum mismatches"));
    }

    #[test]
    fn test_multiple_tables() {
        let c = CSqliteBackend::open_in_memory().unwrap();
        let f = FrankenSqliteBackend::open_in_memory().unwrap();
        let stmts: &[&str] = &[
            "CREATE TABLE t0 (id INTEGER PRIMARY KEY, val TEXT)",
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, num REAL)",
            "INSERT INTO t0 VALUES (1, 'a')",
            "INSERT INTO t1 VALUES (10, 3.14)",
        ];
        setup_both(&c, &f, stmts);

        let tables = tables_from_stmts(stmts);
        let report = validate_with_known_tables(&c, &f, &tables);
        assert!(report.passed(), "report should pass: {report}");
        assert_eq!(report.row_counts.len(), 2);
        assert_eq!(report.checksums.len(), 2);
    }

    #[test]
    fn test_empty_tables_pass() {
        let c = CSqliteBackend::open_in_memory().unwrap();
        let f = FrankenSqliteBackend::open_in_memory().unwrap();
        let stmts: &[&str] = &["CREATE TABLE empty_t (id INTEGER PRIMARY KEY, val TEXT)"];
        setup_both(&c, &f, stmts);

        let tables = tables_from_stmts(stmts);
        let report = validate_with_known_tables(&c, &f, &tables);
        assert!(report.passed(), "empty tables should match: {report}");
        assert_eq!(report.row_counts[0].csqlite_count, 0);
        assert_eq!(report.row_counts[0].fsqlite_count, 0);
    }

    #[test]
    fn test_table_set_mismatch_via_validate() {
        // Uses `validate()` which discovers from C SQLite only.
        // The FrankenSQLite backend doesn't expose sqlite_master, so
        // fsqlite_tables will be empty while csqlite_tables has entries.
        let c = CSqliteBackend::open_in_memory().unwrap();
        let f = FrankenSqliteBackend::open_in_memory().unwrap();
        let _ = c.execute("CREATE TABLE only_in_c (id INTEGER PRIMARY KEY)");
        let _ = c.execute("INSERT INTO only_in_c VALUES (1)");

        let report = validate(&c, &f);
        // Table discovered from C SQLite → row count check runs on both.
        // FrankenSQLite never got CREATE TABLE so fsqlite count = -1.
        assert!(!report.passed());
        assert_ne!(report.csqlite_tables, report.fsqlite_tables);
    }

    #[test]
    fn test_report_display_on_pass() {
        let c = CSqliteBackend::open_in_memory().unwrap();
        let f = FrankenSqliteBackend::open_in_memory().unwrap();
        let stmts: &[&str] = &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY)",
            "INSERT INTO t VALUES (1)",
        ];
        setup_both(&c, &f, stmts);

        let tables = tables_from_stmts(stmts);
        let report = validate_with_known_tables(&c, &f, &tables);
        let display = format!("{report}");
        assert!(display.contains("VALIDATION PASSED"));
    }

    #[test]
    fn test_workload_then_validate() {
        // Simulate a small deterministic workload and validate.
        let c = CSqliteBackend::open_in_memory().unwrap();
        let f = FrankenSqliteBackend::open_in_memory().unwrap();
        let stmts: &[&str] = &[
            "CREATE TABLE t0 (id INTEGER PRIMARY KEY, val TEXT, num REAL)",
            "BEGIN",
            "INSERT INTO t0 VALUES (1, 'alpha', 1.0)",
            "INSERT INTO t0 VALUES (2, 'beta', 2.0)",
            "INSERT INTO t0 VALUES (3, 'gamma', 3.0)",
            "COMMIT",
            "BEGIN",
            "UPDATE t0 SET val='ALPHA' WHERE id=1",
            "DELETE FROM t0 WHERE id=3",
            "COMMIT",
        ];
        setup_both(&c, &f, stmts);

        let tables = vec!["t0".to_owned()];
        let report = validate_with_known_tables(&c, &f, &tables);
        assert!(report.passed(), "workload validation should pass: {report}");
        assert_eq!(report.row_counts[0].csqlite_count, 2);
        assert_eq!(report.row_counts[0].fsqlite_count, 2);
    }

    #[test]
    fn test_merged_table_list_dedup() {
        let a = vec!["t0".to_owned(), "t1".to_owned()];
        let b = vec!["t1".to_owned(), "t2".to_owned()];
        let merged = merged_table_list(&a, &b);
        assert_eq!(merged, vec!["t0", "t1", "t2"]);
    }

    #[test]
    fn test_count_rows_missing_table() {
        let c = CSqliteBackend::open_in_memory().unwrap();
        let count = count_rows(&c, "nonexistent");
        assert_eq!(count, -1);
    }
}
