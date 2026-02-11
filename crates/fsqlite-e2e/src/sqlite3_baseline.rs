//! sqlite3 baseline behavior measurement under corruption (bd-2als.4.3).
//!
//! For each entry in the [`crate::corruption::corruption_strategy_catalog`],
//! this module:
//!
//! 1. Creates a fresh WAL-mode database with known data.
//! 2. Takes a deterministic logical dump (schema + sorted rows) as the
//!    "expected" reference.
//! 3. Applies the corruption pattern via [`crate::corruption::CatalogEntry::inject`].
//! 4. Attempts to open, integrity-check, query, and dump the corrupted copy
//!    with stock C SQLite (via `rusqlite`).
//! 5. Classifies the outcome into a four-tier system.
//!
//! Results are JSON-serializable so downstream beads (bd-2als.4.4) can combine
//! them with the FrankenSQLite recovery side for comparative reports.

use std::fmt;
use std::path::{Path, PathBuf};

use rusqlite::Connection;

use sha2::{Digest, Sha256};

use crate::E2eResult;
use crate::corruption::{
    CatalogEntry, CorruptionCategory, CorruptionReport, CorruptionSeverity,
    corruption_strategy_catalog,
};

// ── Outcome classification ──────────────────────────────────────────────

/// Four-tier outcome for sqlite3 behavior after corruption.
///
/// Listed in order of increasing "health" — `OpenFailed` is the worst,
/// `OpenedAndMatches` means the corruption had no observable effect.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum SqliteOutcomeTier {
    /// sqlite3 could not open the database file at all.
    OpenFailed,
    /// sqlite3 opened the file but `PRAGMA integrity_check` failed.
    IntegrityFailed,
    /// sqlite3 opened the file, integrity passed, but data differs from
    /// the pre-corruption dump.
    LogicallyDiverged,
    /// sqlite3 opened the file, integrity passed, and data matches the
    /// pre-corruption dump (corruption was in an unused region).
    OpenedAndMatches,
}

impl fmt::Display for SqliteOutcomeTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OpenFailed => f.write_str("open_failed"),
            Self::IntegrityFailed => f.write_str("integrity_failed"),
            Self::LogicallyDiverged => f.write_str("opened_but_logically_diverged"),
            Self::OpenedAndMatches => f.write_str("opened_and_matches"),
        }
    }
}

// ── Baseline result ─────────────────────────────────────────────────────

/// Structured evidence from running one corruption catalog entry against
/// stock C SQLite.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SqliteBaselineResult {
    /// The catalog entry's stable identifier.
    pub strategy_id: String,
    /// Human-readable strategy name.
    pub strategy_name: String,
    /// Target file category.
    pub category: CorruptionCategory,
    /// Severity level.
    pub severity: CorruptionSeverity,
    /// Classified outcome tier.
    pub outcome: SqliteOutcomeTier,
    /// Number of rows in the database before corruption.
    pub rows_before: usize,
    /// Number of rows sqlite3 could read after corruption (`None` if open failed).
    pub rows_after: Option<usize>,
    /// `PRAGMA integrity_check` output (`None` if open failed).
    pub integrity_output: Option<String>,
    /// Error message from the open attempt (`None` if open succeeded).
    pub open_error: Option<String>,
    /// Error message from the query attempt (`None` if query succeeded).
    pub query_error: Option<String>,
    /// SHA-256 hash of the expected (pre-corruption) logical dump.
    pub expected_dump_hash: String,
    /// SHA-256 hash of the actual (post-corruption) logical dump, if obtainable.
    pub actual_dump_hash: Option<String>,
    /// Injection report from the corruption framework.
    pub corruption_report: Option<CorruptionReport>,
}

// ── Setup ───────────────────────────────────────────────────────────────

/// Default row count for baseline databases.
const BASELINE_ROW_COUNT: usize = 100;

/// Page size for baseline databases.
const BASELINE_PAGE_SIZE: u32 = 4096;

/// Create a fresh WAL-mode database with known, deterministic data.
///
/// Returns the path to the `.db` file.  The caller manages the directory.
fn setup_baseline_db(dir: &Path) -> E2eResult<PathBuf> {
    let db_path = dir.join("baseline.db");
    let conn = Connection::open(&db_path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;\n\
         PRAGMA synchronous=NORMAL;\n\
         PRAGMA page_size=4096;\n\
         CREATE TABLE items(id INTEGER PRIMARY KEY, label TEXT NOT NULL, value REAL NOT NULL);\n\
         CREATE INDEX idx_items_label ON items(label);",
    )?;

    // Insert deterministic rows.
    #[allow(clippy::cast_precision_loss, clippy::items_after_statements)]
    fn row_value(i: usize) -> f64 {
        i as f64 * 1.5
    }

    for i in 0..BASELINE_ROW_COUNT {
        let id = i64::try_from(i).expect("row count fits i64");
        conn.execute(
            "INSERT INTO items(id, label, value) VALUES (?1, ?2, ?3)",
            rusqlite::params![id, format!("item-{i:04}"), row_value(i)],
        )?;
    }

    // Checkpoint to ensure the DB file has content, then add more rows
    // so unflushed WAL frames exist for WAL-targeting strategies.
    conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;

    for i in BASELINE_ROW_COUNT..(BASELINE_ROW_COUNT + 20) {
        let id = i64::try_from(i).expect("row count fits i64");
        conn.execute(
            "INSERT INTO items(id, label, value) VALUES (?1, ?2, ?3)",
            rusqlite::params![id, format!("item-{i:04}"), row_value(i)],
        )?;
    }

    drop(conn);
    Ok(db_path)
}

/// Create a dummy WAL-FEC sidecar file (sqlite3 ignores these, but the
/// corruption injector needs a file to target).
fn ensure_sidecar_file(db_path: &Path) -> E2eResult<PathBuf> {
    let mut sidecar = db_path.as_os_str().to_owned();
    sidecar.push("-wal-fec");
    let sidecar_path = PathBuf::from(sidecar);
    if !sidecar_path.exists() {
        // Write a synthetic sidecar with enough bytes for corruption patterns.
        let dummy = vec![0xFE; 2048];
        std::fs::write(&sidecar_path, dummy)?;
    }
    Ok(sidecar_path)
}

/// Get the file path for a given corruption category relative to a DB path.
fn target_path_for_category(db_path: &Path, category: CorruptionCategory) -> PathBuf {
    match category {
        CorruptionCategory::DatabaseFile => db_path.to_path_buf(),
        CorruptionCategory::Wal => {
            let mut p = db_path.as_os_str().to_owned();
            p.push("-wal");
            PathBuf::from(p)
        }
        CorruptionCategory::Sidecar => {
            let mut p = db_path.as_os_str().to_owned();
            p.push("-wal-fec");
            PathBuf::from(p)
        }
    }
}

// ── Logical dump ────────────────────────────────────────────────────────

/// Produce a deterministic logical dump: schema DDL + sorted rows per table.
///
/// Used to detect logical divergence after corruption.
fn logical_dump(conn: &Connection) -> E2eResult<String> {
    use std::fmt::Write;

    let mut dump = String::new();

    // Schema: all DDL sorted by name.
    let mut schema_stmt =
        conn.prepare("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name")?;
    let sqls: Vec<String> = schema_stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;
    for sql in &sqls {
        let _ = writeln!(dump, "{sql};");
    }
    dump.push_str("---\n");

    // Data: per table, SELECT * ORDER BY first column.
    let mut tables_stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' \
         AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let tables: Vec<String> = tables_stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    for table in &tables {
        let _ = writeln!(dump, "TABLE {table}");
        let query = format!("SELECT * FROM \"{table}\" ORDER BY 1");
        let mut stmt = conn.prepare(&query)?;
        let col_count = stmt.column_count();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let mut parts = Vec::with_capacity(col_count);
            for i in 0..col_count {
                let val: rusqlite::types::Value = row.get(i)?;
                parts.push(format!("{val:?}"));
            }
            let _ = writeln!(dump, "  {}", parts.join("|"));
        }
    }
    Ok(dump)
}

/// SHA-256 hash of a string, returned as hex.
fn hash_dump(dump: &str) -> String {
    use std::fmt::Write;
    let digest = Sha256::digest(dump.as_bytes());
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

// ── Single-strategy measurement ─────────────────────────────────────────

/// Run a single corruption catalog entry against stock C SQLite and classify
/// the outcome.
///
/// # Errors
///
/// Returns `E2eError` for filesystem failures during setup or injection.
/// sqlite3 query-level errors are captured in the result, not propagated.
pub fn measure_sqlite3_baseline(
    entry: &CatalogEntry,
    work_dir: &Path,
) -> E2eResult<SqliteBaselineResult> {
    let total_rows = BASELINE_ROW_COUNT + 20;

    // 1. Set up a fresh database.
    let db_path = setup_baseline_db(work_dir)?;

    // Ensure sidecar exists for sidecar strategies.
    if entry.category == CorruptionCategory::Sidecar {
        ensure_sidecar_file(&db_path)?;
    }

    // 2. Take the expected dump before corruption.
    let expected_dump = {
        let conn = Connection::open(&db_path)?;
        let dump = logical_dump(&conn)?;
        drop(conn);
        dump
    };
    let expected_hash = hash_dump(&expected_dump);

    // 3. Apply corruption.
    let target = target_path_for_category(&db_path, entry.category);
    let corruption_report = if target.exists() {
        Some(entry.inject(&target, BASELINE_PAGE_SIZE)?)
    } else {
        // Target file doesn't exist (e.g., no WAL frames yet).
        // Corruption cannot be applied; result is effectively "matches".
        let hash_copy = expected_hash.clone();
        return Ok(SqliteBaselineResult {
            strategy_id: entry.strategy_id.clone(),
            strategy_name: entry.name.clone(),
            category: entry.category,
            severity: entry.severity,
            outcome: SqliteOutcomeTier::OpenedAndMatches,
            rows_before: total_rows,
            rows_after: Some(total_rows),
            integrity_output: Some("ok (target file absent)".to_owned()),
            open_error: None,
            query_error: None,
            expected_dump_hash: expected_hash,
            actual_dump_hash: Some(hash_copy),
            corruption_report: None,
        });
    };

    // 4. Try to open with sqlite3.
    let open_result = Connection::open(&db_path);
    let conn = match open_result {
        Err(e) => {
            return Ok(SqliteBaselineResult {
                strategy_id: entry.strategy_id.clone(),
                strategy_name: entry.name.clone(),
                category: entry.category,
                severity: entry.severity,
                outcome: SqliteOutcomeTier::OpenFailed,
                rows_before: total_rows,
                rows_after: None,
                integrity_output: None,
                open_error: Some(e.to_string()),
                query_error: None,
                expected_dump_hash: expected_hash,
                actual_dump_hash: None,
                corruption_report,
            });
        }
        Ok(c) => c,
    };

    // 5. Run integrity check.
    let integrity = conn
        .query_row("PRAGMA integrity_check;", [], |row| row.get::<_, String>(0))
        .unwrap_or_else(|e| format!("error: {e}"));
    let integrity_ok = integrity == "ok";

    // 6. Count surviving rows.
    let (rows_after, query_error) = match conn.query_row("SELECT COUNT(*) FROM items;", [], |row| {
        row.get::<_, i64>(0)
    }) {
        Ok(n) => (usize::try_from(n).ok(), None),
        Err(e) => (None, Some(e.to_string())),
    };

    // 7. Attempt logical dump for divergence detection.
    let actual_dump = logical_dump(&conn).ok();
    let actual_hash = actual_dump.as_deref().map(hash_dump);
    drop(conn);

    // 8. Classify outcome.
    let outcome = if !integrity_ok {
        SqliteOutcomeTier::IntegrityFailed
    } else if actual_hash.as_deref() == Some(expected_hash.as_str()) {
        SqliteOutcomeTier::OpenedAndMatches
    } else {
        SqliteOutcomeTier::LogicallyDiverged
    };

    Ok(SqliteBaselineResult {
        strategy_id: entry.strategy_id.clone(),
        strategy_name: entry.name.clone(),
        category: entry.category,
        severity: entry.severity,
        outcome,
        rows_before: total_rows,
        rows_after,
        integrity_output: Some(integrity),
        open_error: None,
        query_error,
        expected_dump_hash: expected_hash,
        actual_dump_hash: actual_hash,
        corruption_report,
    })
}

// ── Full catalog sweep ──────────────────────────────────────────────────

/// Run every strategy from the corruption catalog against stock C SQLite.
///
/// Returns one [`SqliteBaselineResult`] per catalog entry, in catalog order.
///
/// # Errors
///
/// Returns `E2eError` if the temp directory cannot be created.
pub fn run_full_sqlite3_baseline() -> E2eResult<Vec<SqliteBaselineResult>> {
    let catalog = corruption_strategy_catalog();
    let base_dir = tempfile::tempdir()?;
    let mut results = Vec::with_capacity(catalog.len());

    for (i, entry) in catalog.iter().enumerate() {
        let entry_dir = base_dir
            .path()
            .join(format!("strategy_{i}_{}", entry.strategy_id));
        std::fs::create_dir_all(&entry_dir)?;
        results.push(measure_sqlite3_baseline(entry, &entry_dir)?);
    }

    Ok(results)
}

/// Run a filtered set of strategies (by category) against stock C SQLite.
///
/// # Errors
///
/// Returns `E2eError` if the temp directory cannot be created.
pub fn run_sqlite3_baseline_by_category(
    category: CorruptionCategory,
) -> E2eResult<Vec<SqliteBaselineResult>> {
    let catalog: Vec<CatalogEntry> = corruption_strategy_catalog()
        .into_iter()
        .filter(|e| e.category == category)
        .collect();
    let base_dir = tempfile::tempdir()?;
    let mut results = Vec::with_capacity(catalog.len());

    for (i, entry) in catalog.iter().enumerate() {
        let entry_dir = base_dir
            .path()
            .join(format!("strategy_{i}_{}", entry.strategy_id));
        std::fs::create_dir_all(&entry_dir)?;
        results.push(measure_sqlite3_baseline(entry, &entry_dir)?);
    }

    Ok(results)
}

// ── Summary report ──────────────────────────────────────────────────────

/// Summary statistics for a batch of baseline results.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BaselineSummary {
    /// Total number of strategies tested.
    pub total: usize,
    /// Count of `OpenFailed` outcomes.
    pub open_failed: usize,
    /// Count of `IntegrityFailed` outcomes.
    pub integrity_failed: usize,
    /// Count of `LogicallyDiverged` outcomes.
    pub logically_diverged: usize,
    /// Count of `OpenedAndMatches` outcomes.
    pub opened_and_matches: usize,
}

impl BaselineSummary {
    /// Compute summary statistics from a slice of results.
    #[must_use]
    pub fn from_results(results: &[SqliteBaselineResult]) -> Self {
        let mut summary = Self {
            total: results.len(),
            open_failed: 0,
            integrity_failed: 0,
            logically_diverged: 0,
            opened_and_matches: 0,
        };
        for r in results {
            match r.outcome {
                SqliteOutcomeTier::OpenFailed => summary.open_failed += 1,
                SqliteOutcomeTier::IntegrityFailed => summary.integrity_failed += 1,
                SqliteOutcomeTier::LogicallyDiverged => summary.logically_diverged += 1,
                SqliteOutcomeTier::OpenedAndMatches => summary.opened_and_matches += 1,
            }
        }
        summary
    }
}

impl fmt::Display for BaselineSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "sqlite3 baseline: {total} strategies — \
             {of} open_failed, {if_} integrity_failed, \
             {ld} logically_diverged, {om} opened_and_matches",
            total = self.total,
            of = self.open_failed,
            if_ = self.integrity_failed,
            ld = self.logically_diverged,
            om = self.opened_and_matches,
        )
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_setup_baseline_db_creates_valid_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_baseline_db(dir.path()).unwrap();
        assert!(db_path.exists());

        let conn = Connection::open(&db_path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM items;", [], |r| r.get(0))
            .unwrap();
        #[allow(clippy::cast_possible_wrap)]
        let expected = (BASELINE_ROW_COUNT + 20) as i64;
        assert_eq!(count, expected);
    }

    #[test]
    fn test_logical_dump_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_baseline_db(dir.path()).unwrap();

        let conn = Connection::open(&db_path).unwrap();
        let dump1 = logical_dump(&conn).unwrap();
        let dump2 = logical_dump(&conn).unwrap();
        assert_eq!(dump1, dump2, "logical dump must be deterministic");
    }

    #[test]
    fn test_logical_dump_contains_schema_and_data() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_baseline_db(dir.path()).unwrap();

        let conn = Connection::open(&db_path).unwrap();
        let dump = logical_dump(&conn).unwrap();
        assert!(dump.contains("CREATE TABLE"), "dump should contain schema");
        assert!(
            dump.contains("TABLE items"),
            "dump should contain table header"
        );
        assert!(dump.contains("item-0000"), "dump should contain first row");
    }

    #[test]
    fn test_hash_dump_consistent() {
        let h1 = hash_dump("hello world");
        let h2 = hash_dump("hello world");
        assert_eq!(h1, h2);
        assert_ne!(h1, hash_dump("different"));
    }

    #[test]
    fn test_measure_header_zero() {
        let catalog = corruption_strategy_catalog();
        let entry = catalog
            .iter()
            .find(|e| e.strategy_id == "header_zero")
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let result = measure_sqlite3_baseline(entry, dir.path()).unwrap();

        assert_eq!(result.strategy_id, "header_zero");
        assert_eq!(result.rows_before, BASELINE_ROW_COUNT + 20);
        // Header zeroing should cause open failure or integrity failure.
        assert!(
            result.outcome == SqliteOutcomeTier::OpenFailed
                || result.outcome == SqliteOutcomeTier::IntegrityFailed,
            "header_zero should cause open or integrity failure, got {}",
            result.outcome
        );
    }

    #[test]
    fn test_measure_sidecar_corrupt() {
        let catalog = corruption_strategy_catalog();
        let entry = catalog
            .iter()
            .find(|e| e.strategy_id == "sidecar_corrupt_symbols")
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let result = measure_sqlite3_baseline(entry, dir.path()).unwrap();

        assert_eq!(result.strategy_id, "sidecar_corrupt_symbols");
        // sqlite3 ignores FEC sidecars — should open and match.
        assert_eq!(
            result.outcome,
            SqliteOutcomeTier::OpenedAndMatches,
            "sidecar corruption should not affect sqlite3"
        );
    }

    #[test]
    fn test_measure_single_bitflip_db() {
        let catalog = corruption_strategy_catalog();
        let entry = catalog
            .iter()
            .find(|e| e.strategy_id == "bitflip_db_single")
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let result = measure_sqlite3_baseline(entry, dir.path()).unwrap();

        assert_eq!(result.strategy_id, "bitflip_db_single");
        assert!(result.rows_before > 0);
        // A single bit flip at byte 200 may be in unused space or cause subtle issues.
        // We just verify the measurement completed without panic.
        assert!(result.corruption_report.is_some());
    }

    #[test]
    fn test_measure_wal_truncate() {
        let catalog = corruption_strategy_catalog();
        let entry = catalog
            .iter()
            .find(|e| e.strategy_id == "wal_truncate_0")
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let result = measure_sqlite3_baseline(entry, dir.path()).unwrap();

        assert_eq!(result.strategy_id, "wal_truncate_0");
        // WAL truncation to 0 frames loses uncheckpointed data.
        // sqlite3 should still open (it falls back to the DB file).
        assert!(
            result.open_error.is_none(),
            "sqlite3 should open after WAL truncation"
        );
    }

    #[test]
    fn test_run_full_baseline() {
        let results = run_full_sqlite3_baseline().unwrap();
        let catalog = corruption_strategy_catalog();
        assert_eq!(
            results.len(),
            catalog.len(),
            "should have one result per catalog entry"
        );

        // Every result should have a valid strategy_id.
        for result in &results {
            assert!(!result.strategy_id.is_empty());
            assert!(result.rows_before > 0);
        }
    }

    #[test]
    fn test_baseline_summary() {
        let results = run_full_sqlite3_baseline().unwrap();
        let summary = BaselineSummary::from_results(&results);

        assert_eq!(summary.total, results.len());
        assert_eq!(
            summary.open_failed
                + summary.integrity_failed
                + summary.logically_diverged
                + summary.opened_and_matches,
            summary.total,
            "tier counts must sum to total"
        );

        // At least some strategies should cause damage.
        assert!(
            summary.open_failed + summary.integrity_failed + summary.logically_diverged > 0,
            "at least some strategies should cause observable damage"
        );
    }

    #[test]
    fn test_run_baseline_by_category_db() {
        let results = run_sqlite3_baseline_by_category(CorruptionCategory::DatabaseFile).unwrap();
        for r in &results {
            assert_eq!(r.category, CorruptionCategory::DatabaseFile);
        }
        assert!(!results.is_empty());
    }

    #[test]
    fn test_run_baseline_by_category_wal() {
        let results = run_sqlite3_baseline_by_category(CorruptionCategory::Wal).unwrap();
        for r in &results {
            assert_eq!(r.category, CorruptionCategory::Wal);
        }
        assert!(!results.is_empty());
    }

    #[test]
    fn test_run_baseline_by_category_sidecar() {
        let results = run_sqlite3_baseline_by_category(CorruptionCategory::Sidecar).unwrap();
        for r in &results {
            assert_eq!(r.category, CorruptionCategory::Sidecar);
        }
        // All sidecar corruption should be transparent to sqlite3.
        for r in &results {
            assert_eq!(r.outcome, SqliteOutcomeTier::OpenedAndMatches);
        }
    }

    #[test]
    fn test_results_json_serializable() {
        let results = run_full_sqlite3_baseline().unwrap();
        let json = serde_json::to_string_pretty(&results).unwrap();
        assert!(!json.is_empty());

        // Round-trip deserialization.
        let deserialized: Vec<SqliteBaselineResult> = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.len(), results.len());
    }

    #[test]
    fn test_summary_display() {
        let results = run_full_sqlite3_baseline().unwrap();
        let summary = BaselineSummary::from_results(&results);
        let display = summary.to_string();
        assert!(display.contains("sqlite3 baseline"));
        assert!(display.contains("strategies"));
    }

    #[test]
    fn test_outcome_tier_ordering() {
        assert!(SqliteOutcomeTier::OpenFailed < SqliteOutcomeTier::IntegrityFailed);
        assert!(SqliteOutcomeTier::IntegrityFailed < SqliteOutcomeTier::LogicallyDiverged);
        assert!(SqliteOutcomeTier::LogicallyDiverged < SqliteOutcomeTier::OpenedAndMatches);
    }

    #[test]
    fn test_outcome_tier_display() {
        assert_eq!(SqliteOutcomeTier::OpenFailed.to_string(), "open_failed");
        assert_eq!(
            SqliteOutcomeTier::IntegrityFailed.to_string(),
            "integrity_failed"
        );
        assert_eq!(
            SqliteOutcomeTier::LogicallyDiverged.to_string(),
            "opened_but_logically_diverged"
        );
        assert_eq!(
            SqliteOutcomeTier::OpenedAndMatches.to_string(),
            "opened_and_matches"
        );
    }
}
