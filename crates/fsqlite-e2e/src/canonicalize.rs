//! Database canonicalization pipeline for deterministic SHA-256 hashing.
//!
//! Beads: bd-1w6k.5.2, bd-1opl
//!
//! Produces a canonical database file whose SHA-256 is stable across repeated
//! runs for identical logical content.  The pipeline:
//!
//! 1. Checkpoint the WAL (`PRAGMA wal_checkpoint(TRUNCATE)`)
//! 2. Normalize PRAGMAs (`page_size`, `auto_vacuum = NONE`)
//! 3. `VACUUM INTO <canonical_path>` to produce a defragmented, single-file copy
//! 4. SHA-256 hash the canonical file
//!
//! ## Three-Tier Comparison (bd-1opl)
//!
//! For cross-engine comparison, three tiers with automatic fallback:
//!
//! - **Tier 1 (`ByteIdentical`):** VACUUM INTO both databases, compare SHA-256.
//! - **Tier 2 (`LogicalMatch`):** Row-level comparison via `SELECT * ORDER BY rowid`.
//! - **Tier 3 (`DataComplete`):** Row counts + spot checks + integrity check.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::{E2eError, E2eResult};

/// Result of canonicalizing a database file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CanonicalResult {
    /// Path to the canonical output file.
    pub canonical_path: PathBuf,
    /// SHA-256 hex digest of the canonical file.
    pub sha256: String,
    /// Size of the canonical file in bytes.
    pub size_bytes: u64,
}

/// Canonicalize a `SQLite` database file for deterministic hashing.
///
/// The source database is opened read-only (via rusqlite), its WAL is
/// checkpointed, and the result is `VACUUM INTO` a new file at `output_path`.
/// The output file's SHA-256 is then computed and returned.
///
/// Fixed PRAGMAs applied before `VACUUM INTO`:
/// - `page_size = 4096` (the `SQLite` default, ensuring layout stability)
/// - `auto_vacuum = 0` (OFF — avoids non-deterministic page relocation)
///
/// # Errors
///
/// Returns `E2eError::Rusqlite` for database errors, `E2eError::Io` for
/// filesystem errors.
///
/// # Safety / immutability
///
/// The source database is opened with `SQLITE_OPEN_READ_ONLY`.
/// WAL checkpointing is best-effort (may silently fail on a read-only handle).
/// **Never pass a golden database path directly** — always operate on a working
/// copy.
pub fn canonicalize(source: &Path, output_path: &Path) -> E2eResult<CanonicalResult> {
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = rusqlite::Connection::open_with_flags(source, flags)?;

    // Checkpoint the WAL to fold all WAL frames back into the main database.
    // TRUNCATE mode also removes the WAL file afterward.  Best-effort: may
    // fail on a read-only connection, which is acceptable.
    let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");

    // Fixed PRAGMAs for deterministic output.
    conn.execute_batch("PRAGMA page_size = 4096;")?;
    conn.execute_batch("PRAGMA auto_vacuum = 0;")?;

    // Remove dest if it exists so VACUUM INTO doesn't fail.
    if output_path.exists() {
        std::fs::remove_file(output_path)?;
    }

    // VACUUM INTO creates a fresh, defragmented database file at output_path.
    // The resulting file has:
    //   - No freelist pages
    //   - Contiguous page allocation
    //   - Deterministic page layout for the same logical content
    let output_str = output_path
        .to_str()
        .ok_or_else(|| E2eError::Io(std::io::Error::other("output path is not valid UTF-8")))?;

    conn.execute_batch(&format!("VACUUM INTO '{output_str}';"))?;
    drop(conn);

    // Compute SHA-256 of the canonical file.
    let canonical_bytes = std::fs::read(output_path)?;
    let sha256 = sha256_hex(&canonical_bytes);
    let size_bytes = u64::try_from(canonical_bytes.len()).unwrap_or(0);

    Ok(CanonicalResult {
        canonical_path: output_path.to_path_buf(),
        sha256,
        size_bytes,
    })
}

/// Canonicalize a database and return only the SHA-256 hash.
///
/// Convenience wrapper that creates a temporary canonical file, hashes it,
/// and cleans up.
///
/// # Errors
///
/// Returns errors from [`canonicalize`].
pub fn canonical_sha256(source: &Path) -> E2eResult<String> {
    let tmp_dir = tempfile::TempDir::new()?;
    let output = tmp_dir.path().join("canonical.db");
    let result = canonicalize(source, &output)?;
    Ok(result.sha256)
}

/// Compare two databases by canonicalizing both and comparing SHA-256 hashes.
///
/// Returns `(sha256_a, sha256_b, matched)`.
///
/// # Errors
///
/// Returns errors from [`canonicalize`].
pub fn compare_canonical(db_a: &Path, db_b: &Path) -> E2eResult<(String, String, bool)> {
    let tmp_dir = tempfile::TempDir::new()?;
    let out_a = tmp_dir.path().join("canonical_a.db");
    let out_b = tmp_dir.path().join("canonical_b.db");

    let result_a = canonicalize(db_a, &out_a)?;
    let result_b = canonicalize(db_b, &out_b)?;

    let matched = result_a.sha256 == result_b.sha256;
    Ok((result_a.sha256, result_b.sha256, matched))
}

/// Compute SHA-256 hex digest of arbitrary bytes.
fn sha256_hex(data: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(data);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

// ─── Three-Tier Comparison (bd-1opl) ─────────────────────────────────────

/// Which comparison tier produced the result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ComparisonTier {
    /// Tier 1: SHA-256 of VACUUM INTO output matches byte-for-byte.
    ByteIdentical,
    /// Tier 2: Row-level logical comparison matches across all tables.
    LogicalMatch,
    /// Tier 3: Row counts and spot-check rows match; minimum bar.
    DataComplete,
}

impl std::fmt::Display for ComparisonTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ByteIdentical => write!(f, "Tier 1: Byte-Identical (SHA-256)"),
            Self::LogicalMatch => write!(f, "Tier 2: Logical Match (row-level)"),
            Self::DataComplete => write!(f, "Tier 3: Data Complete (counts + spot checks)"),
        }
    }
}

/// Result of a three-tier cross-engine database comparison.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TieredComparisonResult {
    /// The highest tier that matched.
    pub tier: ComparisonTier,
    /// SHA-256 of the first database (after VACUUM INTO), if Tier 1 was attempted.
    pub sha256_a: Option<String>,
    /// SHA-256 of the second database (after VACUUM INTO), if Tier 1 was attempted.
    pub sha256_b: Option<String>,
    /// Whether Tier 1 (byte-identical) matched.
    pub byte_match: bool,
    /// Whether Tier 2 (logical row-level) matched.
    pub logical_match: bool,
    /// Whether Tier 3 (row counts) matched.
    pub row_counts_match: bool,
    /// Human-readable description of how the result was determined.
    pub detail: String,
}

/// Compare two on-disk databases using the three-tier fallback strategy.
///
/// Attempts Tier 1 (VACUUM INTO + SHA-256) first.  If that fails (e.g.
/// VACUUM INTO not supported by one engine), falls back to Tier 2 (logical
/// row-level comparison).  If Tier 2 fails, falls back to Tier 3 (row
/// counts + spot checks + integrity check).
///
/// Both paths are opened via rusqlite (read-only) so this works for any
/// SQLite-compatible database file, regardless of which engine produced it.
///
/// # Errors
///
/// Returns `E2eError` on I/O or database errors that prevent even Tier 3.
pub fn canonicalize_and_compare(db_a: &Path, db_b: &Path) -> E2eResult<TieredComparisonResult> {
    // --- Tier 1: VACUUM INTO + SHA-256 ---
    match try_tier1(db_a, db_b) {
        Ok(result) => return Ok(result),
        Err(e) => {
            tracing::info!(error = %e, "Tier 1 (VACUUM INTO) failed, falling back to Tier 2");
        }
    }

    // --- Tier 2: Logical row-level comparison ---
    match try_tier2(db_a, db_b) {
        Ok(result) => return Ok(result),
        Err(e) => {
            tracing::info!(error = %e, "Tier 2 (logical) failed, falling back to Tier 3");
        }
    }

    // --- Tier 3: Data completeness ---
    try_tier3(db_a, db_b)
}

/// Tier 1: VACUUM INTO both databases, compare SHA-256 hashes.
fn try_tier1(db_a: &Path, db_b: &Path) -> E2eResult<TieredComparisonResult> {
    let tmp_dir = tempfile::TempDir::new()?;
    let out_a = tmp_dir.path().join("canonical_a.db");
    let out_b = tmp_dir.path().join("canonical_b.db");

    let result_a = canonicalize(db_a, &out_a)?;
    let result_b = canonicalize(db_b, &out_b)?;

    let byte_match = result_a.sha256 == result_b.sha256;

    Ok(TieredComparisonResult {
        tier: ComparisonTier::ByteIdentical,
        sha256_a: Some(result_a.sha256.clone()),
        sha256_b: Some(result_b.sha256.clone()),
        byte_match,
        logical_match: byte_match,
        row_counts_match: byte_match,
        detail: if byte_match {
            format!("Tier 1 PASS: SHA-256 match ({})", &result_a.sha256[..16])
        } else {
            format!(
                "Tier 1 FAIL: SHA-256 mismatch (a={}, b={})",
                &result_a.sha256[..16],
                &result_b.sha256[..16]
            )
        },
    })
}

/// Open a database read-only via rusqlite.
fn open_readonly(path: &Path) -> E2eResult<rusqlite::Connection> {
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    Ok(rusqlite::Connection::open_with_flags(path, flags)?)
}

/// List user tables (excluding `sqlite_*` internal tables), sorted by name.
fn list_user_tables(conn: &rusqlite::Connection) -> E2eResult<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let names: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<String>, _>>()?;
    Ok(names)
}

/// Get schema SQL for all user tables, sorted by name.
fn schema_sql(conn: &rusqlite::Connection) -> E2eResult<Vec<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let rows: Vec<(String, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get::<_, String>(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Get row count for a table.
fn row_count(conn: &rusqlite::Connection, table: &str) -> E2eResult<i64> {
    let count: i64 = conn.query_row(&format!("SELECT count(*) FROM \"{table}\""), [], |r| {
        r.get(0)
    })?;
    Ok(count)
}

/// Fetch all rows from a table, sorted by rowid (or first column as fallback).
fn fetch_all_rows_sorted(conn: &rusqlite::Connection, table: &str) -> E2eResult<Vec<Vec<String>>> {
    let sql = format!("SELECT * FROM \"{table}\" ORDER BY rowid");
    let fallback_sql = format!("SELECT * FROM \"{table}\" ORDER BY 1");

    let sql_to_use = if conn.prepare(&sql).is_ok() {
        sql
    } else {
        fallback_sql
    };

    let mut stmt = conn.prepare(&sql_to_use)?;
    let col_count = stmt.column_count();
    let rows: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            let mut vals = Vec::with_capacity(col_count);
            for i in 0..col_count {
                let v: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                vals.push(format_value(&v));
            }
            Ok(vals)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Format a rusqlite value as a deterministic string for comparison.
fn format_value(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(i) => i.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => s.clone(),
        rusqlite::types::Value::Blob(b) => {
            use std::fmt::Write as _;
            let mut hex = String::with_capacity(b.len() * 2 + 2);
            hex.push_str("X'");
            for byte in b {
                let _ = write!(hex, "{byte:02X}");
            }
            hex.push('\'');
            hex
        }
    }
}

/// Tier 2: Logical row-level comparison across all tables.
fn try_tier2(db_a: &Path, db_b: &Path) -> E2eResult<TieredComparisonResult> {
    let conn_a = open_readonly(db_a)?;
    let conn_b = open_readonly(db_b)?;

    // Compare schemas first.
    let schema_a = schema_sql(&conn_a)?;
    let schema_b = schema_sql(&conn_b)?;

    if schema_a != schema_b {
        return Ok(TieredComparisonResult {
            tier: ComparisonTier::LogicalMatch,
            sha256_a: None,
            sha256_b: None,
            byte_match: false,
            logical_match: false,
            row_counts_match: false,
            detail: "Tier 2 FAIL: schema mismatch".to_owned(),
        });
    }

    let tables = list_user_tables(&conn_a)?;

    // Compare every row in every table.
    for table in &tables {
        let rows_a = fetch_all_rows_sorted(&conn_a, table)?;
        let rows_b = fetch_all_rows_sorted(&conn_b, table)?;

        if rows_a != rows_b {
            return Ok(TieredComparisonResult {
                tier: ComparisonTier::LogicalMatch,
                sha256_a: None,
                sha256_b: None,
                byte_match: false,
                logical_match: false,
                row_counts_match: rows_a.len() == rows_b.len(),
                detail: format!(
                    "Tier 2 FAIL: row mismatch in table \"{table}\" (a={} rows, b={} rows)",
                    rows_a.len(),
                    rows_b.len()
                ),
            });
        }
    }

    Ok(TieredComparisonResult {
        tier: ComparisonTier::LogicalMatch,
        sha256_a: None,
        sha256_b: None,
        byte_match: false,
        logical_match: true,
        row_counts_match: true,
        detail: format!(
            "Tier 2 PASS: all {} table(s) match row-by-row",
            tables.len()
        ),
    })
}

/// Tier 3: Data completeness — row counts, spot checks, integrity check.
fn try_tier3(db_a: &Path, db_b: &Path) -> E2eResult<TieredComparisonResult> {
    let conn_a = open_readonly(db_a)?;
    let conn_b = open_readonly(db_b)?;

    let tables_a = list_user_tables(&conn_a)?;
    let tables_b = list_user_tables(&conn_b)?;

    // Table list must match.
    if tables_a != tables_b {
        return Ok(TieredComparisonResult {
            tier: ComparisonTier::DataComplete,
            sha256_a: None,
            sha256_b: None,
            byte_match: false,
            logical_match: false,
            row_counts_match: false,
            detail: format!(
                "Tier 3 FAIL: table list mismatch (a={:?}, b={:?})",
                tables_a, tables_b
            ),
        });
    }

    // Check row counts for each table.
    let mut all_counts_match = true;
    let mut detail_parts = Vec::new();

    for table in &tables_a {
        let count_a = row_count(&conn_a, table)?;
        let count_b = row_count(&conn_b, table)?;

        if count_a != count_b {
            all_counts_match = false;
            detail_parts.push(format!(
                "\"{table}\": count mismatch (a={count_a}, b={count_b})"
            ));
        }
    }

    if !all_counts_match {
        return Ok(TieredComparisonResult {
            tier: ComparisonTier::DataComplete,
            sha256_a: None,
            sha256_b: None,
            byte_match: false,
            logical_match: false,
            row_counts_match: false,
            detail: format!("Tier 3 FAIL: {}", detail_parts.join("; ")),
        });
    }

    // Spot checks: first 10 and last 10 rows of each table.
    let mut spot_checks_pass = true;
    for table in &tables_a {
        let count = row_count(&conn_a, table)?;
        if count == 0 {
            continue;
        }

        // First 10 rows.
        let first_a = spot_check_rows(&conn_a, table, "ASC", 10)?;
        let first_b = spot_check_rows(&conn_b, table, "ASC", 10)?;
        if first_a != first_b {
            spot_checks_pass = false;
            detail_parts.push(format!("\"{table}\": first-10 spot check mismatch"));
        }

        // Last 10 rows.
        let last_a = spot_check_rows(&conn_a, table, "DESC", 10)?;
        let last_b = spot_check_rows(&conn_b, table, "DESC", 10)?;
        if last_a != last_b {
            spot_checks_pass = false;
            detail_parts.push(format!("\"{table}\": last-10 spot check mismatch"));
        }
    }

    // Integrity check on both.
    let integrity_a = run_integrity_check(&conn_a);
    let integrity_b = run_integrity_check(&conn_b);

    let integrity_ok = integrity_a && integrity_b;
    if !integrity_ok {
        detail_parts.push(format!(
            "integrity_check: a={}, b={}",
            if integrity_a { "ok" } else { "FAIL" },
            if integrity_b { "ok" } else { "FAIL" },
        ));
    }

    let row_counts_match = all_counts_match && spot_checks_pass && integrity_ok;

    Ok(TieredComparisonResult {
        tier: ComparisonTier::DataComplete,
        sha256_a: None,
        sha256_b: None,
        byte_match: false,
        logical_match: false,
        row_counts_match,
        detail: if row_counts_match {
            format!(
                "Tier 3 PASS: {} table(s), counts match, spot checks pass, integrity ok",
                tables_a.len()
            )
        } else {
            format!("Tier 3 FAIL: {}", detail_parts.join("; "))
        },
    })
}

/// Fetch a limited number of rows for spot-check comparison.
fn spot_check_rows(
    conn: &rusqlite::Connection,
    table: &str,
    order: &str,
    limit: usize,
) -> E2eResult<Vec<Vec<String>>> {
    let sql = format!("SELECT * FROM \"{table}\" ORDER BY rowid {order} LIMIT {limit}");
    let fallback = format!("SELECT * FROM \"{table}\" ORDER BY 1 {order} LIMIT {limit}");

    let sql_to_use = if conn.prepare(&sql).is_ok() {
        sql
    } else {
        fallback
    };

    let mut stmt = conn.prepare(&sql_to_use)?;
    let col_count = stmt.column_count();
    let rows: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            let mut vals = Vec::with_capacity(col_count);
            for i in 0..col_count {
                let v: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                vals.push(format_value(&v));
            }
            Ok(vals)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Run `PRAGMA integrity_check` and return whether it passes.
fn run_integrity_check(conn: &rusqlite::Connection) -> bool {
    conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
        .is_ok_and(|result| result == "ok")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalize_produces_stable_hash() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("test.db");

        // Create a database with some data.
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (1, 'hello');
             INSERT INTO t VALUES (2, 'world');",
        )
        .unwrap();
        drop(conn);

        // Canonicalize twice — hashes must be identical.
        let out1 = tmp.path().join("canon1.db");
        let out2 = tmp.path().join("canon2.db");

        let r1 = canonicalize(&db_path, &out1).unwrap();
        let r2 = canonicalize(&db_path, &out2).unwrap();

        assert_eq!(r1.sha256, r2.sha256, "canonical hashes should be stable");
        assert!(!r1.sha256.is_empty());
        assert!(r1.size_bytes > 0);
    }

    #[test]
    fn different_data_produces_different_hash() {
        let tmp = tempfile::TempDir::new().unwrap();

        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        let conn_a = rusqlite::Connection::open(&db_a).unwrap();
        conn_a
            .execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY);
                 INSERT INTO t VALUES (1);",
            )
            .unwrap();
        drop(conn_a);

        let conn_b = rusqlite::Connection::open(&db_b).unwrap();
        conn_b
            .execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY);
                 INSERT INTO t VALUES (1);
                 INSERT INTO t VALUES (2);",
            )
            .unwrap();
        drop(conn_b);

        let (sha_a, sha_b, matched) = compare_canonical(&db_a, &db_b).unwrap();
        assert!(!matched, "different data should have different hashes");
        assert_ne!(sha_a, sha_b);
    }

    #[test]
    fn same_data_different_insertion_order_produces_same_hash() {
        let tmp = tempfile::TempDir::new().unwrap();

        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        // Insert in order 1,2,3
        let conn_a = rusqlite::Connection::open(&db_a).unwrap();
        conn_a
            .execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
                 INSERT INTO t VALUES (1, 'a');
                 INSERT INTO t VALUES (2, 'b');
                 INSERT INTO t VALUES (3, 'c');",
            )
            .unwrap();
        drop(conn_a);

        // Insert in order 3,1,2
        let conn_b = rusqlite::Connection::open(&db_b).unwrap();
        conn_b
            .execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
                 INSERT INTO t VALUES (3, 'c');
                 INSERT INTO t VALUES (1, 'a');
                 INSERT INTO t VALUES (2, 'b');",
            )
            .unwrap();
        drop(conn_b);

        let (sha_a, sha_b, matched) = compare_canonical(&db_a, &db_b).unwrap();
        assert!(
            matched,
            "same logical data should produce same canonical hash\n  a={sha_a}\n  b={sha_b}"
        );
    }

    #[test]
    fn canonical_sha256_convenience_works() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("test.db");

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY);
             INSERT INTO t VALUES (1);",
        )
        .unwrap();
        drop(conn);

        let hash = canonical_sha256(&db_path).unwrap();
        assert_eq!(hash.len(), 64, "SHA-256 hex should be 64 chars");

        // Running again should give the same result.
        let hash2 = canonical_sha256(&db_path).unwrap();
        assert_eq!(hash, hash2);
    }

    #[test]
    fn canonicalize_handles_wal_mode() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("wal_test.db");

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (1, 'hello');
             INSERT INTO t VALUES (2, 'world');",
        )
        .unwrap();
        // Leave connection open so WAL is active.
        drop(conn);

        let out = tmp.path().join("canon.db");
        let result = canonicalize(&db_path, &out).unwrap();
        assert!(!result.sha256.is_empty());
        assert!(result.size_bytes > 0);

        // The WAL should have been checkpointed.
        let wal_path = db_path.with_extension("db-wal");
        if wal_path.exists() {
            let wal_size = std::fs::metadata(&wal_path).unwrap().len();
            // After TRUNCATE checkpoint, WAL should be 0 bytes or removed.
            assert_eq!(wal_size, 0, "WAL should be truncated after checkpoint");
        }
    }

    // ─── Three-Tier Comparison Tests (bd-1opl) ──────────────────────────

    /// Helper: create a database at `path` and run `sql` inside it.
    fn create_db(path: &Path, sql: &str) {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch(sql).unwrap();
        drop(conn);
    }

    #[test]
    fn test_identical_databases_tier1_match() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
                    INSERT INTO t VALUES (1, 'hello');
                    INSERT INTO t VALUES (2, 'world');
                    INSERT INTO t VALUES (3, 'test');";
        create_db(&db_a, sql);
        create_db(&db_b, sql);

        let result = canonicalize_and_compare(&db_a, &db_b).unwrap();
        assert_eq!(result.tier, ComparisonTier::ByteIdentical);
        assert!(result.byte_match);
        assert!(result.logical_match);
        assert!(result.row_counts_match);
        assert!(result.sha256_a.is_some());
        assert_eq!(result.sha256_a, result.sha256_b);
    }

    #[test]
    fn test_different_insert_order_tier1_match() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        create_db(
            &db_a,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (1, 'a');
             INSERT INTO t VALUES (2, 'b');
             INSERT INTO t VALUES (3, 'c');",
        );
        create_db(
            &db_b,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (3, 'c');
             INSERT INTO t VALUES (1, 'a');
             INSERT INTO t VALUES (2, 'b');",
        );

        let result = canonicalize_and_compare(&db_a, &db_b).unwrap();
        assert_eq!(result.tier, ComparisonTier::ByteIdentical);
        assert!(result.byte_match);
    }

    #[test]
    fn test_with_deletes_tier1_match() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        // Both insert 1..5 then delete 2,4 — same logical content.
        let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
                    INSERT INTO t VALUES (1, 'a');
                    INSERT INTO t VALUES (2, 'b');
                    INSERT INTO t VALUES (3, 'c');
                    INSERT INTO t VALUES (4, 'd');
                    INSERT INTO t VALUES (5, 'e');
                    DELETE FROM t WHERE id IN (2, 4);";
        create_db(&db_a, sql);
        create_db(&db_b, sql);

        let result = canonicalize_and_compare(&db_a, &db_b).unwrap();
        assert_eq!(result.tier, ComparisonTier::ByteIdentical);
        assert!(result.byte_match);
    }

    #[test]
    fn test_tier2_fallback() {
        // Tier 2 (logical match) is tested by directly calling try_tier2
        // on databases with identical logical content.
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        create_db(
            &db_a,
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
             INSERT INTO users VALUES (1, 'Alice', 30);
             INSERT INTO users VALUES (2, 'Bob', 25);",
        );
        create_db(
            &db_b,
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
             INSERT INTO users VALUES (2, 'Bob', 25);
             INSERT INTO users VALUES (1, 'Alice', 30);",
        );

        let result = try_tier2(&db_a, &db_b).unwrap();
        assert_eq!(result.tier, ComparisonTier::LogicalMatch);
        assert!(result.logical_match);
        assert!(result.row_counts_match);
        assert!(result.detail.contains("PASS"));
    }

    #[test]
    fn test_tier3_fallback() {
        // Tier 3 verifies row counts and spot checks even when
        // full row comparison might not be available.
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        create_db(
            &db_a,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (1, 'hello');
             INSERT INTO t VALUES (2, 'world');",
        );
        create_db(
            &db_b,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (1, 'hello');
             INSERT INTO t VALUES (2, 'world');",
        );

        let result = try_tier3(&db_a, &db_b).unwrap();
        assert_eq!(result.tier, ComparisonTier::DataComplete);
        assert!(result.row_counts_match);
        assert!(result.detail.contains("PASS"));

        // Now test a mismatch: different row counts.
        let db_c = tmp.path().join("c.db");
        create_db(
            &db_c,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (1, 'hello');",
        );

        let mismatch = try_tier3(&db_a, &db_c).unwrap();
        assert!(!mismatch.row_counts_match);
        assert!(mismatch.detail.contains("FAIL"));
    }

    #[test]
    fn test_empty_database_match() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        // Empty databases (only sqlite_master).
        create_db(&db_a, "SELECT 1;");
        create_db(&db_b, "SELECT 1;");

        let result = canonicalize_and_compare(&db_a, &db_b).unwrap();
        assert!(result.byte_match || result.logical_match || result.row_counts_match);

        // Also test with matching empty tables.
        let db_c = tmp.path().join("c.db");
        let db_d = tmp.path().join("d.db");
        create_db(&db_c, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);");
        create_db(&db_d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);");

        let result2 = canonicalize_and_compare(&db_c, &db_d).unwrap();
        assert_eq!(result2.tier, ComparisonTier::ByteIdentical);
        assert!(result2.byte_match);
    }

    #[test]
    fn test_schema_only_match() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        // Tables exist but have no rows.
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
                    CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL);";
        create_db(&db_a, sql);
        create_db(&db_b, sql);

        let result = canonicalize_and_compare(&db_a, &db_b).unwrap();
        assert_eq!(result.tier, ComparisonTier::ByteIdentical);
        assert!(result.byte_match);
    }

    #[test]
    fn test_null_handling() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        // Databases with NULL values in various positions.
        let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score REAL);
                    INSERT INTO t VALUES (1, NULL, 3.14);
                    INSERT INTO t VALUES (2, 'test', NULL);
                    INSERT INTO t VALUES (3, NULL, NULL);";
        create_db(&db_a, sql);
        create_db(&db_b, sql);

        // Tier 1: byte-identical should work.
        let result = canonicalize_and_compare(&db_a, &db_b).unwrap();
        assert_eq!(result.tier, ComparisonTier::ByteIdentical);
        assert!(result.byte_match);

        // Also verify Tier 2 handles NULLs correctly.
        let tier2 = try_tier2(&db_a, &db_b).unwrap();
        assert!(tier2.logical_match);

        // And Tier 3.
        let tier3 = try_tier3(&db_a, &db_b).unwrap();
        assert!(tier3.row_counts_match);
    }
}
