//! Integration tests: validate that all golden database copies are clean.
//!
//! These tests require the golden database files to exist under
//! `sample_sqlite_db_files/golden/`.  When the directory is empty or
//! missing (e.g. in CI where large binaries are not checked in), every
//! test in this module is skipped gracefully.

use std::path::PathBuf;

use fsqlite_e2e::golden::{self, GOLDEN_DIR_RELATIVE};

/// Resolve the golden directory relative to the workspace root.
///
/// `CARGO_MANIFEST_DIR` points at `crates/fsqlite-e2e/`, so we walk up
/// two levels to reach the workspace root.
fn golden_dir() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .join(GOLDEN_DIR_RELATIVE)
}

/// Returns `true` if the golden directory exists and contains at least one `.db` file.
fn golden_available() -> bool {
    let dir = golden_dir();
    dir.is_dir() && golden::discover_golden_files(&dir).is_ok_and(|v| !v.is_empty())
}

// ─── Tests ─────────────────────────────────────────────────────────────

#[test]
fn all_golden_pass_integrity_check() {
    if !golden_available() {
        eprintln!("SKIP: golden database files not available");
        return;
    }

    let dir = golden_dir();
    let reports = golden::validate_all_golden(&dir).expect("failed to validate golden copies");

    assert!(
        !reports.is_empty(),
        "expected at least one golden database file"
    );

    let mut failures = Vec::new();
    for report in &reports {
        if !report.integrity_ok {
            failures.push(format!(
                "{}: integrity_check returned '{}'",
                report.name, report.integrity_result
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "integrity_check failures:\n{}",
        failures.join("\n")
    );

    eprintln!(
        "OK: {} golden databases passed integrity_check",
        reports.len()
    );
}

#[test]
fn all_golden_have_nonzero_page_count() {
    if !golden_available() {
        eprintln!("SKIP: golden database files not available");
        return;
    }

    let dir = golden_dir();
    let reports = golden::validate_all_golden(&dir).expect("failed to validate golden copies");

    let mut failures = Vec::new();
    for report in &reports {
        if report.page_count == 0 {
            failures.push(format!("{}: page_count is 0", report.name));
        }
    }

    assert!(
        failures.is_empty(),
        "zero page_count failures:\n{}",
        failures.join("\n")
    );

    eprintln!(
        "OK: {} golden databases have non-zero page_count",
        reports.len()
    );
}

#[test]
fn all_golden_have_at_least_one_table() {
    if !golden_available() {
        eprintln!("SKIP: golden database files not available");
        return;
    }

    let dir = golden_dir();
    let reports = golden::validate_all_golden(&dir).expect("failed to validate golden copies");

    let mut failures = Vec::new();
    for report in &reports {
        if report.master_count == 0 {
            failures.push(format!(
                "{}: sqlite_master is empty (no tables/views/triggers)",
                report.name
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "empty sqlite_master failures:\n{}",
        failures.join("\n")
    );

    eprintln!(
        "OK: {} golden databases have at least one schema object",
        reports.len()
    );
}

#[test]
fn golden_discovery_returns_sorted_list() {
    if !golden_available() {
        eprintln!("SKIP: golden database files not available");
        return;
    }

    let dir = golden_dir();
    let files = golden::discover_golden_files(&dir).expect("discovery failed");

    assert!(!files.is_empty());

    // Verify the list is sorted.
    let names: Vec<String> = files
        .iter()
        .filter_map(|p| p.file_name().map(|f| f.to_string_lossy().into_owned()))
        .collect();
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted, "discover_golden_files must return sorted");

    eprintln!(
        "OK: discovered {} golden files in sorted order",
        files.len()
    );
}

#[test]
fn golden_checksum_file_matches_actual_hashes() {
    if !golden_available() {
        eprintln!("SKIP: golden database files not available");
        return;
    }

    let dir = golden_dir();
    let checksum_path = dir
        .parent()
        .expect("golden parent")
        .join("checksums.sha256");
    if !checksum_path.exists() {
        eprintln!("SKIP: checksums.sha256 not found");
        return;
    }

    let content = std::fs::read_to_string(&checksum_path).expect("failed to read checksums");
    let files = golden::discover_golden_files(&dir).expect("discovery failed");

    let mut checksum_map = std::collections::HashMap::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // Format: "<hash>  <filename>"
        let parts: Vec<&str> = line.splitn(2, "  ").collect();
        assert_eq!(parts.len(), 2, "malformed checksum line: {line}");
        checksum_map.insert(parts[1].to_owned(), parts[0].to_owned());
    }

    let mut failures = Vec::new();
    for path in &files {
        let fname = path
            .file_name()
            .expect("filename")
            .to_string_lossy()
            .into_owned();
        let actual = golden::GoldenCopy::hash_file(path).expect("hash failed");
        if let Some(expected) = checksum_map.get(&fname) {
            if *expected != actual {
                failures.push(format!(
                    "{fname}: checksum mismatch (expected {expected}, got {actual})"
                ));
            }
        } else {
            failures.push(format!("{fname}: not found in checksums.sha256"));
        }
    }

    assert!(
        failures.is_empty(),
        "checksum mismatches:\n{}",
        failures.join("\n")
    );

    eprintln!("OK: {} golden files match checksums.sha256", files.len());
}

// ─── Always-run guardrails (no golden files needed) ────────────────────

/// Resolve the checksums file relative to the workspace root.
fn checksums_path() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .join("sample_sqlite_db_files/checksums.sha256")
}

/// Validate that `checksums.sha256` exists, is non-empty, and every line
/// follows the `<64-hex-char-sha256>  <filename.db>` format.
///
/// This test runs unconditionally (does NOT require golden `.db` files)
/// so it works in CI where the large binaries are gitignored.
#[test]
fn checksums_sha256_is_well_formed() {
    let path = checksums_path();
    assert!(
        path.exists(),
        "checksums.sha256 must exist at {path:?} (tracked in git)"
    );

    let content = std::fs::read_to_string(&path).expect("failed to read checksums.sha256");
    let lines: Vec<&str> = content.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(
        !lines.is_empty(),
        "checksums.sha256 must contain at least one entry"
    );

    for (i, line) in lines.iter().enumerate() {
        let parts: Vec<&str> = line.splitn(2, "  ").collect();
        assert_eq!(
            parts.len(),
            2,
            "line {}: malformed checksum line (expected '<hash>  <filename>'): {line}",
            i + 1
        );

        let hash = parts[0];
        assert_eq!(
            hash.len(),
            64,
            "line {}: SHA-256 hash must be 64 hex characters, got {} chars: {hash}",
            i + 1,
            hash.len()
        );
        assert!(
            hash.chars().all(|c| c.is_ascii_hexdigit()),
            "line {}: hash contains non-hex characters: {hash}",
            i + 1
        );

        let filename = parts[1];
        assert!(
            std::path::Path::new(filename)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("db")),
            "line {}: filename must end with .db: {filename}",
            i + 1
        );
    }

    eprintln!(
        "OK: checksums.sha256 is well-formed ({} entries)",
        lines.len()
    );
}

/// Verify that no two entries in `checksums.sha256` reference the same filename.
#[test]
fn checksums_sha256_no_duplicate_filenames() {
    let path = checksums_path();
    if !path.exists() {
        eprintln!("SKIP: checksums.sha256 not found");
        return;
    }

    let content = std::fs::read_to_string(&path).expect("failed to read checksums.sha256");
    let mut seen = std::collections::HashSet::new();
    let mut duplicates = Vec::new();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some((_, filename)) = line.split_once("  ") {
            if !seen.insert(filename.to_owned()) {
                duplicates.push(filename.to_owned());
            }
        }
    }

    assert!(
        duplicates.is_empty(),
        "duplicate filenames in checksums.sha256: {duplicates:?}"
    );
}
