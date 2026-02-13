//! Unit + integration tests for corpus ingestion and normalization (bd-1dp9.2.1).
//!
//! Validates:
//! - Family classification heuristic correctness
//! - Seed derivation determinism
//! - Corpus builder API
//! - Coverage report accuracy
//! - Seed corpus completeness (all families represented)
//! - Conformance fixture ingestion

use std::collections::BTreeSet;
use std::path::Path;

use fsqlite_harness::corpus_ingest::{
    classify_family, derive_entry_seed, generate_seed_corpus, ingest_conformance_fixtures,
    CorpusBuilder, CorpusSource, Family, CORPUS_SEED_BASE,
};

// ─── Family Classification Tests ─────────────────────────────────────────

#[test]
fn classify_select_as_sql() {
    let stmts = vec!["SELECT 1".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::SQL);
}

#[test]
fn classify_create_table_as_sql() {
    let stmts = vec!["CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::SQL);
}

#[test]
fn classify_begin_commit_as_txn() {
    let stmts = vec!["BEGIN".to_owned(), "COMMIT".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::TXN);
}

#[test]
fn classify_savepoint_as_txn() {
    let stmts = vec![
        "SAVEPOINT sp1".to_owned(),
        "RELEASE sp1".to_owned(),
    ];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::TXN);
}

#[test]
fn classify_aggregate_functions_as_fun() {
    let stmts = vec!["SELECT COUNT(*), SUM(x), AVG(x) FROM t".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::FUN);
}

#[test]
fn classify_string_functions_as_fun() {
    let stmts = vec!["SELECT UPPER('hello'), LOWER('WORLD'), LENGTH('test')".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::FUN);
}

#[test]
fn classify_explain_as_vdb() {
    let stmts = vec!["EXPLAIN SELECT * FROM t".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::VDB);
}

#[test]
fn classify_pragma_as_pgm() {
    let stmts = vec!["PRAGMA page_size".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::PGM);
}

#[test]
fn classify_json_as_ext() {
    let stmts = vec!["SELECT JSON('{\"a\":1}')".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::EXT);
}

#[test]
fn classify_fts_as_ext() {
    let stmts = vec![
        "CREATE VIRTUAL TABLE docs USING FTS5(title, body)".to_owned(),
    ];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::EXT);
}

#[test]
fn classify_join_as_pln() {
    let stmts = vec![
        "SELECT a.x, b.y FROM t1 a JOIN t2 b ON a.id = b.a_id".to_owned(),
    ];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::PLN);
}

#[test]
fn classify_cte_as_pln() {
    let stmts = vec![
        "WITH cte AS (SELECT 1) SELECT * FROM cte".to_owned(),
    ];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::PLN);
}

#[test]
fn classify_detects_secondary_families() {
    // A query with both functions and JOINs should have secondary families.
    let stmts = vec![
        "SELECT COUNT(*), SUM(b.val) FROM t1 a JOIN t2 b ON a.id = b.a_id GROUP BY a.name"
            .to_owned(),
    ];
    let (primary, secondary) = classify_family(&stmts);
    // Primary should be one of FUN or PLN (both have strong signals).
    assert!(
        primary == Family::FUN || primary == Family::PLN,
        "expected FUN or PLN, got {primary}"
    );
    // Secondary should include the other.
    assert!(
        !secondary.is_empty(),
        "expected secondary families for cross-domain query"
    );
}

// ─── Seed Derivation Tests ───────────────────────────────────────────────

#[test]
fn seed_derivation_is_deterministic() {
    let s1 = derive_entry_seed(42, 0);
    let s2 = derive_entry_seed(42, 0);
    assert_eq!(s1, s2);
}

#[test]
fn seed_derivation_varies_by_index() {
    let s0 = derive_entry_seed(42, 0);
    let s1 = derive_entry_seed(42, 1);
    let s2 = derive_entry_seed(42, 2);
    assert_ne!(s0, s1);
    assert_ne!(s1, s2);
    assert_ne!(s0, s2);
}

#[test]
fn seed_derivation_varies_by_base() {
    let s1 = derive_entry_seed(42, 5);
    let s2 = derive_entry_seed(43, 5);
    assert_ne!(s1, s2);
}

#[test]
fn corpus_seed_base_is_franken_seed() {
    assert_eq!(CORPUS_SEED_BASE, 0x4652_414E_4B45_4E);
}

// ─── Corpus Builder Tests ────────────────────────────────────────────────

#[test]
fn builder_creates_empty_corpus() {
    let builder = CorpusBuilder::new(42);
    let manifest = builder.build();
    assert_eq!(manifest.entries.len(), 0);
    assert_eq!(manifest.coverage.total_entries, 0);
}

#[test]
fn builder_adds_classified_entries() {
    let mut builder = CorpusBuilder::new(42);
    builder.add_statements(
        ["SELECT 1"],
        CorpusSource::Custom {
            author: "test".to_owned(),
        },
        "test entry",
    );
    let manifest = builder.build();
    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(manifest.entries[0].family, Family::SQL);
    assert!(!manifest.entries[0].id.is_empty());
}

#[test]
fn builder_assigns_deterministic_seeds() {
    let mut builder = CorpusBuilder::new(42);
    builder.add_statements(
        ["SELECT 1"],
        CorpusSource::Custom {
            author: "test".to_owned(),
        },
        "first",
    );
    builder.add_statements(
        ["SELECT 2"],
        CorpusSource::Custom {
            author: "test".to_owned(),
        },
        "second",
    );
    let manifest = builder.build();
    assert_ne!(manifest.entries[0].seed, manifest.entries[1].seed);
    assert_ne!(manifest.entries[0].seed, 0);
    assert_ne!(manifest.entries[1].seed, 0);
}

#[test]
fn builder_skip_marks_entry() {
    let mut builder = CorpusBuilder::new(42);
    builder.add_statements(
        ["SELECT 1"],
        CorpusSource::Custom {
            author: "test".to_owned(),
        },
        "skipped entry",
    );
    builder.skip_last("not supported yet", Some("X-AMAL.1".to_owned()));

    let manifest = builder.build();
    assert!(manifest.entries[0].skip.is_some());
    assert_eq!(manifest.coverage.skipped_entries, 1);
    assert_eq!(manifest.coverage.active_entries, 0);
}

#[test]
fn builder_link_features_attaches_ids() {
    let mut builder = CorpusBuilder::new(42);
    builder.add_statements(
        ["SELECT COUNT(*) FROM t"],
        CorpusSource::Custom {
            author: "test".to_owned(),
        },
        "function test",
    );
    builder.link_features(["F-FUN.5", "F-SQL.2"]);

    let manifest = builder.build();
    assert_eq!(manifest.entries[0].taxonomy_features.len(), 2);
    assert!(manifest.entries[0].taxonomy_features.contains(&"F-FUN.5".to_owned()));
}

// ─── Coverage Report Tests ───────────────────────────────────────────────

#[test]
fn coverage_reports_missing_families() {
    let mut builder = CorpusBuilder::new(42);
    // Only add SQL entries — all other families should be missing.
    builder.add_with_family(
        Family::SQL,
        ["SELECT 1"],
        CorpusSource::Custom {
            author: "test".to_owned(),
        },
        "sql only",
    );
    let manifest = builder.build();

    assert!(manifest.coverage.missing_families.len() >= 7);
    assert!(manifest.coverage.missing_families.contains(&"TXN".to_owned()));
    assert!(manifest.coverage.missing_families.contains(&"FUN".to_owned()));
    assert!(manifest.coverage.missing_families.contains(&"VDB".to_owned()));
    assert!(!manifest.coverage.missing_families.contains(&"SQL".to_owned()));
}

#[test]
fn coverage_fill_percentage_is_correct() {
    let mut builder = CorpusBuilder::new(42);
    // SQL family minimum is 30; add 15 entries → 50%.
    for i in 0..15 {
        builder.add_with_family(
            Family::SQL,
            [format!("SELECT {i}")],
            CorpusSource::Custom {
                author: "test".to_owned(),
            },
            format!("entry {i}"),
        );
    }
    let manifest = builder.build();

    let sql_coverage = manifest.coverage.by_family.get("SQL").expect("SQL family");
    assert_eq!(sql_coverage.entry_count, 15);
    assert!(
        (sql_coverage.fill_pct - 50.0).abs() < 0.1,
        "expected ~50% fill, got {}",
        sql_coverage.fill_pct
    );
}

// ─── Seed Corpus Tests ───────────────────────────────────────────────────

#[test]
fn seed_corpus_covers_all_families() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let families_present: BTreeSet<String> = manifest
        .entries
        .iter()
        .map(|e| e.family.to_string())
        .collect();

    for fam in Family::ALL {
        assert!(
            families_present.contains(&fam.to_string()),
            "seed corpus missing family: {fam}"
        );
    }

    eprintln!(
        "bead_id=bd-1dp9.2.1 test=seed_corpus families={} entries={}",
        families_present.len(),
        manifest.entries.len()
    );
}

#[test]
fn seed_corpus_has_taxonomy_feature_links() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let linked = manifest
        .entries
        .iter()
        .filter(|e| !e.taxonomy_features.is_empty())
        .count();

    assert!(
        linked > 0,
        "seed corpus should have taxonomy feature links"
    );
    assert!(
        linked >= manifest.entries.len() / 2,
        "at least half of seed corpus entries should have feature links, got {}/{}",
        linked,
        manifest.entries.len()
    );

    eprintln!(
        "bead_id=bd-1dp9.2.1 test=feature_links linked={} total={}",
        linked,
        manifest.entries.len()
    );
}

#[test]
fn seed_corpus_entries_have_unique_ids() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let mut seen = BTreeSet::new();
    for entry in &manifest.entries {
        assert!(
            seen.insert(&entry.id),
            "duplicate corpus entry ID: {}",
            entry.id
        );
    }
}

#[test]
fn seed_corpus_entries_have_unique_seeds() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let mut seen = BTreeSet::new();
    for entry in &manifest.entries {
        assert!(
            seen.insert(entry.seed),
            "duplicate seed for entry {}: {}",
            entry.id,
            entry.seed
        );
    }
}

// ─── Conformance Fixture Ingestion Tests ─────────────────────────────────

#[test]
fn ingest_conformance_fixtures_from_directory() {
    let conformance_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("conformance");
    if !conformance_dir.exists() {
        eprintln!("bead_id=bd-1dp9.2.1 test=conformance_ingest skip=no_conformance_dir");
        return;
    }

    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    let count =
        ingest_conformance_fixtures(&conformance_dir, &mut builder).expect("ingest fixtures");

    assert!(
        count > 0,
        "expected at least 1 conformance fixture, found {count}"
    );

    let manifest = builder.build();
    assert_eq!(manifest.entries.len(), count);

    // All ingested fixtures should have a Fixture source.
    for entry in &manifest.entries {
        assert!(
            matches!(&entry.source, CorpusSource::Fixture { .. }),
            "ingested entry should have Fixture source: {}",
            entry.id
        );
    }

    eprintln!(
        "bead_id=bd-1dp9.2.1 test=conformance_ingest count={count} families={:?}",
        manifest
            .entries
            .iter()
            .map(|e| e.family.to_string())
            .collect::<BTreeSet<_>>()
    );
}

// ─── Manifest Serialization Tests ────────────────────────────────────────

#[test]
fn manifest_serializes_to_json() {
    let mut builder = CorpusBuilder::new(42);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let json = serde_json::to_string_pretty(&manifest).expect("serialize manifest");
    assert!(json.contains("\"bead_id\""));
    assert!(json.contains("\"coverage\""));
    assert!(json.contains("\"entries\""));

    // Verify round-trip.
    let m2: fsqlite_harness::corpus_ingest::CorpusManifest =
        serde_json::from_str(&json).expect("deserialize manifest");
    assert_eq!(m2.entries.len(), manifest.entries.len());
    assert_eq!(m2.coverage.total_entries, manifest.coverage.total_entries);
}

#[test]
fn coverage_report_json_has_all_families() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    for fam in Family::ALL {
        assert!(
            manifest.coverage.by_family.contains_key(&fam.to_string()),
            "coverage report missing family: {fam}"
        );
    }
}

// ─── Family Display/Parsing Tests ────────────────────────────────────────

#[test]
fn family_display_roundtrip() {
    for fam in Family::ALL {
        let s = fam.to_string();
        let parsed = Family::from_str_opt(&s).expect("parse family");
        assert_eq!(parsed, fam);
    }
}

#[test]
fn family_from_str_case_insensitive() {
    assert_eq!(Family::from_str_opt("sql"), Some(Family::SQL));
    assert_eq!(Family::from_str_opt("Txn"), Some(Family::TXN));
    assert_eq!(Family::from_str_opt("FUN"), Some(Family::FUN));
    assert_eq!(Family::from_str_opt("unknown"), None);
}
