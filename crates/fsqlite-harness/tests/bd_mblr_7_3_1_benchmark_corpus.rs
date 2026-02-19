use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;

use fsqlite_harness::benchmark_corpus::{
    BEAD_ID, BenchmarkFamily, BenchmarkTier, DEFAULT_ROOT_SEED, build_benchmark_corpus,
    validate_benchmark_corpus,
};

#[test]
fn test_corpus_is_valid_and_covers_required_axes() {
    let corpus = build_benchmark_corpus(DEFAULT_ROOT_SEED);
    let errors = validate_benchmark_corpus(&corpus);
    assert!(
        errors.is_empty(),
        "bead_id={BEAD_ID} expected valid corpus, errors={errors:?}"
    );

    let tiers: BTreeSet<BenchmarkTier> = corpus.entries.iter().map(|entry| entry.tier).collect();
    assert!(tiers.contains(&BenchmarkTier::Micro));
    assert!(tiers.contains(&BenchmarkTier::Macro));

    let families: BTreeSet<BenchmarkFamily> =
        corpus.entries.iter().map(|entry| entry.family).collect();
    assert!(families.contains(&BenchmarkFamily::WriteContention));
    assert!(families.contains(&BenchmarkFamily::Recovery));
    assert!(families.contains(&BenchmarkFamily::Checkpoint));
    assert!(families.contains(&BenchmarkFamily::SqlOperatorMix));

    let has_conflict_heavy = corpus
        .entries
        .iter()
        .any(|entry| entry.family == BenchmarkFamily::WriteContention && entry.conflict_heavy);
    assert!(
        has_conflict_heavy,
        "bead_id={BEAD_ID} requires at least one conflict-heavy write-contention benchmark"
    );
}

#[test]
fn test_dataset_generation_and_warmup_are_deterministic() {
    let corpus_a = build_benchmark_corpus(DEFAULT_ROOT_SEED);
    let corpus_b = build_benchmark_corpus(DEFAULT_ROOT_SEED);
    assert_eq!(
        corpus_a, corpus_b,
        "bead_id={BEAD_ID} same seed must produce identical corpus"
    );

    let corpus_c = build_benchmark_corpus(DEFAULT_ROOT_SEED + 1);
    assert_ne!(
        corpus_a, corpus_c,
        "bead_id={BEAD_ID} different seed must change dataset/warmup schedule"
    );
}

#[test]
fn test_sanity_checks_catch_misconfigured_entries() {
    let mut corpus = build_benchmark_corpus(DEFAULT_ROOT_SEED);
    let first = corpus
        .entries
        .first_mut()
        .expect("canonical corpus should not be empty");
    first.command.clear();
    first.dataset.warmup_iterations = 0;

    let errors = validate_benchmark_corpus(&corpus);
    assert!(
        errors.iter().any(|error| error.contains("invalid command")),
        "expected invalid command error, got {errors:?}"
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("zero warmup_iterations")),
        "expected zero warmup_iterations error, got {errors:?}"
    );
}

fn benchmark_corpus_binary() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_benchmark_corpus_manifest"))
}

#[test]
fn test_manifest_cli_emits_json() {
    let output = Command::new(benchmark_corpus_binary())
        .arg("--root-seed")
        .arg(DEFAULT_ROOT_SEED.to_string())
        .output()
        .expect("run benchmark_corpus_manifest");

    assert!(
        output.status.success(),
        "expected success, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let payload: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse manifest JSON");
    assert_eq!(payload["bead_id"], BEAD_ID);
    let entry_count = payload["entries"].as_array().map_or(0, std::vec::Vec::len);
    assert!(
        entry_count >= 6,
        "expected representative corpus entry set, got {entry_count}"
    );
}

#[test]
fn test_manifest_cli_emits_workflow() {
    let output = Command::new(benchmark_corpus_binary())
        .arg("--workflow")
        .output()
        .expect("run workflow output");

    assert!(
        output.status.success(),
        "expected success, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let workflow = String::from_utf8(output.stdout).expect("workflow output should be utf-8");
    assert!(workflow.contains("entries:"));
    assert!(workflow.contains("write-contention"));
}
