//! Stable, git-tracked fixture metadata schema for the RealDB corpus.
//!
//! This schema is written to `sample_sqlite_db_files/metadata/<db_id>.json` by:
//! - `realdb-e2e corpus import` (captures provenance like `source_path`)
//! - `profile-db` (re-profiles golden DBs; should preserve curated fields when possible)

use serde::{Deserialize, Serialize};

/// JSON schema version for `metadata/*.json`.
pub const FIXTURE_METADATA_SCHEMA_VERSION_V1: u32 = 1;

/// Coarse, conservative risk assessment for fixture inclusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Unknown,
    Unlikely,
    Possible,
    Likely,
}

impl RiskLevel {
    /// Parse a CLI string like `unknown|unlikely|possible|likely`.
    pub fn parse(s: &str) -> Result<Self, String> {
        let v = s.trim().to_ascii_lowercase();
        match v.as_str() {
            "unknown" => Ok(Self::Unknown),
            "unlikely" => Ok(Self::Unlikely),
            "possible" => Ok(Self::Possible),
            "likely" => Ok(Self::Likely),
            _ => Err(format!(
                "invalid risk level `{s}` (expected unknown|unlikely|possible|likely)"
            )),
        }
    }
}

/// Safety metadata that gates whether a fixture may be used in CI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureSafetyV1 {
    pub pii_risk: RiskLevel,
    pub secrets_risk: RiskLevel,
    pub allowed_for_ci: bool,
}

/// Feature flags derived from schema inspection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureFeaturesV1 {
    pub has_wal_sidecars_observed: bool,
    pub has_fts: bool,
    pub has_rtree: bool,
    pub has_triggers: bool,
    pub has_views: bool,
    pub has_foreign_keys: bool,
}

/// SQLite metadata captured from PRAGMAs on open.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteMetaV1 {
    pub page_size: u32,
    pub page_count: u32,
    pub freelist_count: u32,
    pub schema_version: u32,
    pub encoding: String,
    pub user_version: u32,
    pub application_id: u32,
    pub journal_mode: String,
    pub auto_vacuum: u32,
}

/// Full, stable metadata record for one corpus fixture.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureMetadataV1 {
    /// Fixture metadata schema version (this struct).
    pub schema_version: u32,
    /// Stable identifier used by selectors/logging.
    pub db_id: String,
    /// Absolute source path used to seed the golden copy, when known.
    pub source_path: Option<String>,
    /// File name under `sample_sqlite_db_files/golden/`.
    pub golden_filename: String,
    /// SHA-256 of the golden bytes (lowercase hex).
    pub sha256_golden: String,
    /// Size of the golden file in bytes.
    pub size_bytes: u64,
    /// Sidecar suffixes observed at capture time (e.g. `-wal`, `-shm`, `-journal`).
    pub sidecars_present: Vec<String>,
    /// SQLite PRAGMA metadata.
    pub sqlite_meta: SqliteMetaV1,
    /// Derived feature flags.
    pub features: FixtureFeaturesV1,
    /// Tags used for selection/reporting. Must be lowercase, sorted, and de-duplicated.
    pub tags: Vec<String>,
    /// Safety classification for CI gating.
    pub safety: FixtureSafetyV1,
    /// Schema summaries (best-effort, but stable ordering).
    pub tables: Vec<TableProfileV1>,
    pub indices: Vec<String>,
    pub triggers: Vec<String>,
    pub views: Vec<String>,
}

/// Profile of a single table within a database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableProfileV1 {
    pub name: String,
    pub row_count: u64,
    pub columns: Vec<ColumnProfileV1>,
}

/// Profile of a single column within a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnProfileV1 {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
    pub primary_key: bool,
    pub not_null: bool,
    pub default_value: Option<String>,
}

/// Normalize a tag for stable storage in metadata.
///
/// Returns `None` for empty/whitespace-only tags.
#[must_use]
pub fn normalize_tag(tag: &str) -> Option<String> {
    let t = tag.trim();
    if t.is_empty() {
        return None;
    }
    Some(t.to_ascii_lowercase())
}

/// Normalize, sort, and de-duplicate tags for deterministic JSON output.
#[must_use]
pub fn normalize_tags(tags: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut out: Vec<String> = tags.into_iter().filter_map(|t| normalize_tag(&t)).collect();
    out.sort();
    out.dedup();
    out
}

/// Size bucket tag used by the corpus (see `sample_sqlite_db_files/FIXTURES.md`).
#[must_use]
pub fn size_bucket_tag(size_bytes: u64) -> &'static str {
    if size_bytes < 64 * 1024 {
        "small"
    } else if size_bytes < 4 * 1024 * 1024 {
        "medium"
    } else {
        "large"
    }
}
