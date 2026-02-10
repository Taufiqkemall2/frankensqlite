//! Fixture discovery: scan directories for candidate SQLite database files.
//!
//! This module provides safe, bounded traversal of the filesystem to locate
//! SQLite databases suitable for E2E integration testing.  It checks the
//! 16-byte magic header (`"SQLite format 3\0"`) and classifies files by
//! path heuristics (beads, cache, sample, etc.).
//!
//! # Safety Rules
//!
//! - Source files are **never** modified.
//! - Traversal depth is bounded (default: 6 levels).
//! - Configurable denylist skips irrelevant subtrees early.
//!
//! # Example
//!
//! ```no_run
//! use fsqlite_harness::fixture_discovery::{DiscoveryConfig, discover_sqlite_files};
//!
//! let config = DiscoveryConfig::default();
//! let candidates = discover_sqlite_files(&config).unwrap();
//! for c in &candidates {
//!     println!("{} ({} bytes, header_ok={}, tags={:?})", c.path.display(), c.size_bytes, c.header_ok, c.tags);
//! }
//! ```

use std::fmt;
use std::fs;
use std::io::Read as _;
use std::path::{Path, PathBuf};

// ── Configuration ────────────────────────────────────────────────────────

/// Configuration for the SQLite file discovery scan.
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// Root directories to scan.
    pub roots: Vec<PathBuf>,
    /// Maximum traversal depth (0 = root only).
    pub max_depth: usize,
    /// Directory name patterns to skip entirely.
    pub denylist: Vec<String>,
    /// Only include files matching these extensions (empty = all
    /// SQLite-header-valid files).
    pub extensions: Vec<String>,
    /// Maximum file size to consider (bytes). Files larger than this are
    /// skipped to keep discovery fast.
    pub max_file_size: u64,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            roots: vec![PathBuf::from("/dp")],
            max_depth: 6,
            denylist: vec![
                "node_modules".to_owned(),
                ".git".to_owned(),
                "target".to_owned(),
                "__pycache__".to_owned(),
                ".cache".to_owned(),
                ".venv".to_owned(),
                "venv".to_owned(),
                ".tox".to_owned(),
                "dist".to_owned(),
                "build".to_owned(),
            ],
            extensions: vec!["db".to_owned(), "sqlite".to_owned(), "sqlite3".to_owned()],
            max_file_size: 512 * 1024 * 1024, // 512 MiB
        }
    }
}

// ── Candidate ────────────────────────────────────────────────────────────

/// A discovered SQLite database file candidate.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// Absolute path to the file.
    pub path: PathBuf,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Whether the first 16 bytes match the SQLite magic header.
    pub header_ok: bool,
    /// Classification tags derived from path heuristics.
    pub tags: Vec<String>,
}

impl fmt::Display for Candidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}\t{}\theader={}\t[{}]",
            self.path.display(),
            human_size(self.size_bytes),
            if self.header_ok { "ok" } else { "BAD" },
            self.tags.join(", "),
        )
    }
}

// ── Discovery ────────────────────────────────────────────────────────────

/// The SQLite database file magic header (first 16 bytes).
const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";

/// Discover SQLite database files under the configured roots.
///
/// Returns a sorted list of candidates (sorted by path).
///
/// # Errors
///
/// Returns an error only for catastrophic I/O failures.  Individual
/// unreadable files or directories are silently skipped.
pub fn discover_sqlite_files(config: &DiscoveryConfig) -> std::io::Result<Vec<Candidate>> {
    let mut candidates = Vec::new();
    for root in &config.roots {
        if root.is_dir() {
            walk_dir(root, 0, config, &mut candidates);
        }
    }
    candidates.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(candidates)
}

fn walk_dir(dir: &Path, depth: usize, config: &DiscoveryConfig, out: &mut Vec<Candidate>) {
    if depth > config.max_depth {
        return;
    }

    let Ok(entries) = fs::read_dir(dir) else {
        return; // Permission denied, etc. — skip silently.
    };

    for entry in entries {
        let Ok(entry) = entry else { continue };
        let path = entry.path();

        // Resolve symlinks for metadata, but keep original path.
        let Ok(meta) = fs::metadata(&path) else {
            continue;
        };

        if meta.is_dir() {
            let dir_name = entry.file_name();
            let name = dir_name.to_string_lossy();
            if config.denylist.iter().any(|d| d == name.as_ref()) {
                continue;
            }
            walk_dir(&path, depth + 1, config, out);
        } else if meta.is_file() {
            if meta.len() > config.max_file_size {
                continue;
            }

            // Extension filter.
            if !config.extensions.is_empty() {
                let ext = path.extension().map(|e| e.to_string_lossy().to_lowercase());
                let ext_str = ext.as_deref().unwrap_or("");
                if !config.extensions.iter().any(|e| e == ext_str) {
                    continue;
                }
            }

            let header_ok = check_sqlite_header(&path);
            let tags = classify_path(&path);
            out.push(Candidate {
                path,
                size_bytes: meta.len(),
                header_ok,
                tags,
            });
        }
    }
}

/// Check whether a file starts with the 16-byte SQLite magic header.
fn check_sqlite_header(path: &Path) -> bool {
    let Ok(mut f) = fs::File::open(path) else {
        return false;
    };
    let mut buf = [0u8; 16];
    if f.read_exact(&mut buf).is_err() {
        return false;
    }
    buf == *SQLITE_MAGIC
}

/// Classify a file path into tags based on heuristics.
fn classify_path(path: &Path) -> Vec<String> {
    let s = path.to_string_lossy().to_lowercase();
    let mut tags = Vec::new();

    if s.contains(".beads/") || s.contains("beads.db") {
        tags.push("beads".to_owned());
    }
    if s.contains("cache") {
        tags.push("cache".to_owned());
    }
    if s.contains("sample") || s.contains("example") || s.contains("demo") {
        tags.push("sample".to_owned());
    }
    if s.contains("northwind") {
        tags.push("northwind".to_owned());
    }
    if s.contains("chinook") {
        tags.push("chinook".to_owned());
    }
    if s.contains("test") {
        tags.push("test".to_owned());
    }

    // Size classification from file name patterns.
    let size_hint = path.metadata().map_or(0, |m| m.len());
    if size_hint < 64 * 1024 {
        tags.push("small".to_owned());
    } else if size_hint < 4 * 1024 * 1024 {
        tags.push("medium".to_owned());
    } else {
        tags.push("large".to_owned());
    }

    tags
}

/// Format a byte count for human display.
#[must_use]
pub fn human_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;

    if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    #[test]
    fn test_sqlite_magic_header_detection() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Write a file with the SQLite magic header.
        let mut f = fs::File::create(&db_path).unwrap();
        f.write_all(SQLITE_MAGIC).unwrap();
        f.write_all(&[0u8; 84]).unwrap(); // Pad to 100 bytes.
        drop(f);

        assert!(check_sqlite_header(&db_path));

        // Write a file without the magic header.
        let bad_path = dir.path().join("bad.db");
        fs::write(&bad_path, b"not a sqlite database").unwrap();
        assert!(!check_sqlite_header(&bad_path));
    }

    #[test]
    fn test_classify_path_beads() {
        let tags = classify_path(Path::new("/dp/myproject/.beads/beads.db"));
        assert!(tags.contains(&"beads".to_owned()));
    }

    #[test]
    fn test_classify_path_cache() {
        let tags = classify_path(Path::new("/tmp/some_cache.db"));
        assert!(tags.contains(&"cache".to_owned()));
    }

    #[test]
    fn test_denylist_skips_node_modules() {
        let dir = tempfile::tempdir().unwrap();
        let nm = dir.path().join("node_modules");
        fs::create_dir(&nm).unwrap();
        let db = nm.join("test.db");
        let mut f = fs::File::create(&db).unwrap();
        f.write_all(SQLITE_MAGIC).unwrap();
        f.write_all(&[0u8; 84]).unwrap();
        drop(f);

        let config = DiscoveryConfig {
            roots: vec![dir.path().to_owned()],
            max_depth: 3,
            ..DiscoveryConfig::default()
        };
        let results = discover_sqlite_files(&config).unwrap();
        // Should NOT find the file inside node_modules.
        assert!(
            results.is_empty(),
            "node_modules should be denylisted: {results:?}"
        );
    }

    #[test]
    fn test_discovery_finds_valid_sqlite_files() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("found.db");
        let mut f = fs::File::create(&db_path).unwrap();
        f.write_all(SQLITE_MAGIC).unwrap();
        f.write_all(&[0u8; 84]).unwrap();
        drop(f);

        let config = DiscoveryConfig {
            roots: vec![dir.path().to_owned()],
            max_depth: 1,
            ..DiscoveryConfig::default()
        };
        let results = discover_sqlite_files(&config).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].header_ok);
        assert_eq!(results[0].size_bytes, 100);
    }

    #[test]
    fn test_max_depth_respected() {
        let dir = tempfile::tempdir().unwrap();
        let deep = dir.path().join("a").join("b").join("c");
        fs::create_dir_all(&deep).unwrap();
        let db = deep.join("deep.db");
        let mut f = fs::File::create(&db).unwrap();
        f.write_all(SQLITE_MAGIC).unwrap();
        f.write_all(&[0u8; 84]).unwrap();
        drop(f);

        // max_depth = 1 should NOT find a file 3 levels deep.
        let config = DiscoveryConfig {
            roots: vec![dir.path().to_owned()],
            max_depth: 1,
            ..DiscoveryConfig::default()
        };
        let results = discover_sqlite_files(&config).unwrap();
        assert!(results.is_empty());

        // max_depth = 4 should find it.
        let config2 = DiscoveryConfig {
            roots: vec![dir.path().to_owned()],
            max_depth: 4,
            ..DiscoveryConfig::default()
        };
        let results2 = discover_sqlite_files(&config2).unwrap();
        assert_eq!(results2.len(), 1);
    }

    #[test]
    fn test_human_size_formatting() {
        assert_eq!(human_size(0), "0 B");
        assert_eq!(human_size(512), "512 B");
        assert_eq!(human_size(1024), "1.0 KiB");
        assert_eq!(human_size(1_048_576), "1.0 MiB");
        assert_eq!(human_size(1_073_741_824), "1.0 GiB");
    }

    /// Integration test: scan /dp if it exists.
    #[test]
    fn test_discover_dp_if_present() {
        let dp = Path::new("/dp");
        if !dp.is_dir() {
            // /dp not available (CI, etc.) — skip gracefully.
            return;
        }

        let config = DiscoveryConfig::default();
        let candidates = discover_sqlite_files(&config).unwrap();

        // We expect at least some beads.db files in /dp.
        let beads_count = candidates
            .iter()
            .filter(|c| c.tags.contains(&"beads".to_owned()) && c.header_ok)
            .count();

        assert!(
            beads_count > 0,
            "expected at least one beads.db file in /dp, found 0 out of {} candidates",
            candidates.len()
        );
    }
}
