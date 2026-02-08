//! Workspace structure and dependency layer validation (bd-1wwc, §8.1–§8.2).
//!
//! These tests enforce the 23-crate workspace layout and the 10-layer
//! dependency hierarchy documented in the spec. They run `cargo metadata`
//! and verify the resolved dependency graph against the documented layering.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;

const BEAD_ID: &str = "bd-1wwc";

/// The 23 crates specified in §8.1.
const EXPECTED_CRATES: [&str; 23] = [
    "fsqlite-ast",
    "fsqlite-btree",
    "fsqlite-cli",
    "fsqlite-core",
    "fsqlite-error",
    "fsqlite-ext-fts3",
    "fsqlite-ext-fts5",
    "fsqlite-ext-icu",
    "fsqlite-ext-json",
    "fsqlite-ext-misc",
    "fsqlite-ext-rtree",
    "fsqlite-ext-session",
    "fsqlite-func",
    "fsqlite-harness",
    "fsqlite-mvcc",
    "fsqlite-pager",
    "fsqlite-parser",
    "fsqlite-planner",
    "fsqlite-types",
    "fsqlite-vdbe",
    "fsqlite-vfs",
    "fsqlite-wal",
    "fsqlite",
];

/// Supporting directories required by §8.1.
const SUPPORTING_DIRS: [&str; 5] = [
    "conformance",
    "tests",
    "benches",
    "fuzz",
    "legacy_sqlite_code",
];

/// 10-layer dependency hierarchy from §8.2.
///
/// No crate may depend on a strictly higher layer (except where explicitly
/// allowed for apps at L9).
fn layer_assignments() -> HashMap<&'static str, u8> {
    let mut m = HashMap::new();
    // Layer 0: leaves
    m.insert("fsqlite-types", 0);
    m.insert("fsqlite-error", 0);
    // Layer 1: storage + AST
    m.insert("fsqlite-vfs", 1);
    m.insert("fsqlite-ast", 1);
    // Layer 2: cache + parser + func
    m.insert("fsqlite-pager", 2);
    m.insert("fsqlite-parser", 2);
    m.insert("fsqlite-func", 2);
    // Layer 3: log + mvcc + planner
    m.insert("fsqlite-wal", 3);
    m.insert("fsqlite-mvcc", 3);
    m.insert("fsqlite-planner", 3);
    // Layer 4: btree
    m.insert("fsqlite-btree", 4);
    // Layer 5: vm
    m.insert("fsqlite-vdbe", 5);
    // Layer 6: extensions
    m.insert("fsqlite-ext-fts3", 6);
    m.insert("fsqlite-ext-fts5", 6);
    m.insert("fsqlite-ext-rtree", 6);
    m.insert("fsqlite-ext-json", 6);
    m.insert("fsqlite-ext-session", 6);
    m.insert("fsqlite-ext-icu", 6);
    m.insert("fsqlite-ext-misc", 6);
    // Layer 7: core
    m.insert("fsqlite-core", 7);
    // Layer 8: api
    m.insert("fsqlite", 8);
    // Layer 9: apps
    m.insert("fsqlite-cli", 9);
    m.insert("fsqlite-harness", 9);
    m
}

fn workspace_root() -> &'static Path {
    static ROOT: OnceLock<&Path> = OnceLock::new();
    ROOT.get_or_init(|| {
        // CARGO_MANIFEST_DIR = .../crates/fsqlite-harness
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        // Go up: crates/ -> workspace root
        manifest_dir
            .parent()
            .and_then(Path::parent)
            .expect("workspace root should be two levels up from fsqlite-harness")
    })
}

fn cargo_metadata_cached() -> &'static serde_json::Value {
    static METADATA: OnceLock<serde_json::Value> = OnceLock::new();
    METADATA.get_or_init(|| {
        let root = workspace_root();
        let output = Command::new("cargo")
            .args(["metadata", "--format-version=1"])
            .current_dir(root)
            .output()
            .expect("failed to execute cargo metadata");
        assert!(
            output.status.success(),
            "bead_id={BEAD_ID} cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).expect("cargo metadata JSON parse failed")
    })
}

/// Extract workspace member names from cargo metadata.
///
/// Handles both old format (`"name version (path)"`) and new format
/// (`"path+file:///…/crate-name#version"`).
fn workspace_member_names(metadata: &serde_json::Value) -> BTreeSet<String> {
    metadata["workspace_members"]
        .as_array()
        .expect("workspace_members should be an array")
        .iter()
        .filter_map(|m| {
            let s = m.as_str()?;
            if s.starts_with("path+file://") {
                // New format: "path+file:///abs/path/crate-name#0.1.0"
                let without_fragment = s.split('#').next()?;
                let name = without_fragment.rsplit('/').next()?;
                Some(name.to_string())
            } else {
                // Old format: "name version (path+file:///...)"
                Some(s.split_whitespace().next()?.to_string())
            }
        })
        .collect()
}

/// Extract crate name from a cargo metadata package ID.
///
/// Handles both `"name version (path)"` and `"path+file:///…/name#version"`.
fn name_from_pkg_id(id: &str) -> String {
    if id.starts_with("path+file://") {
        let without_fragment = id.split('#').next().unwrap_or(id);
        without_fragment
            .rsplit('/')
            .next()
            .unwrap_or(id)
            .to_string()
    } else {
        id.split_whitespace().next().unwrap_or(id).to_string()
    }
}

/// Build the internal (workspace-only) dependency graph from cargo metadata.
///
/// Returns a map: crate_name -> set of internal dependency names.
fn internal_dep_graph(metadata: &serde_json::Value) -> BTreeMap<String, BTreeSet<String>> {
    let members = workspace_member_names(metadata);
    let resolve = metadata["resolve"]["nodes"]
        .as_array()
        .expect("resolve.nodes should be an array");

    let mut graph: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for node in resolve {
        let id = node["id"].as_str().unwrap_or_default();
        let name = name_from_pkg_id(id);
        if !members.contains(&name) {
            continue;
        }

        let deps: BTreeSet<String> = node["deps"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .filter_map(|dep| {
                let dep_name = dep["name"].as_str()?;
                // cargo metadata uses underscores in dep names, convert back
                let normalized = dep_name.replace('_', "-");
                if members.contains(&normalized) {
                    Some(normalized)
                } else {
                    None
                }
            })
            .collect();

        graph.insert(name, deps);
    }
    graph
}

// ---------------------------------------------------------------------------
// §8.1 tests
// ---------------------------------------------------------------------------

#[test]
fn test_workspace_crate_count_is_23() {
    let metadata = cargo_metadata_cached();
    let members = workspace_member_names(metadata);

    assert_eq!(
        members.len(),
        23,
        "bead_id={BEAD_ID} case=crate_count expected=23 actual={} members={members:?}",
        members.len()
    );

    let expected: BTreeSet<String> = EXPECTED_CRATES.iter().map(|s| (*s).to_string()).collect();
    assert_eq!(
        members, expected,
        "bead_id={BEAD_ID} case=crate_names_match"
    );
}

#[test]
fn test_supporting_directories_present() {
    let root = workspace_root();
    let mut missing = Vec::new();
    for dir in &SUPPORTING_DIRS {
        let path = root.join(dir);
        if !path.is_dir() {
            missing.push(*dir);
        }
    }
    assert!(
        missing.is_empty(),
        "bead_id={BEAD_ID} case=supporting_dirs_present missing={missing:?}"
    );
}

// ---------------------------------------------------------------------------
// §8.2 tests
// ---------------------------------------------------------------------------

#[test]
fn test_layering_document_matches_cargo_metadata() {
    let layers = layer_assignments();

    // Every expected crate must have a layer assignment.
    for crate_name in &EXPECTED_CRATES {
        assert!(
            layers.contains_key(crate_name),
            "bead_id={BEAD_ID} case=layer_assignment_complete crate={crate_name} has no layer"
        );
    }

    // Every layer assignment must refer to a known crate.
    for crate_name in layers.keys() {
        assert!(
            EXPECTED_CRATES.contains(crate_name),
            "bead_id={BEAD_ID} case=layer_assignment_valid crate={crate_name} not in workspace"
        );
    }

    // Verify the documented layer counts.
    let mut by_layer: BTreeMap<u8, Vec<&str>> = BTreeMap::new();
    for (&name, &layer) in &layers {
        by_layer.entry(layer).or_default().push(name);
    }

    // 10 layers (0..=9)
    assert_eq!(
        by_layer.keys().copied().collect::<Vec<_>>(),
        (0..=9).collect::<Vec<_>>(),
        "bead_id={BEAD_ID} case=layer_range expected 0..=9"
    );

    // Total crates across all layers must be 23.
    let total: usize = by_layer.values().map(Vec::len).sum();
    assert_eq!(
        total, 23,
        "bead_id={BEAD_ID} case=layer_total expected=23 actual={total}"
    );
}

#[test]
fn test_no_cross_layer_backedges() {
    let metadata = cargo_metadata_cached();
    let graph = internal_dep_graph(metadata);
    let layers = layer_assignments();
    let mut violations = Vec::new();

    for (crate_name, deps) in &graph {
        let Some(&from_layer) = layers.get(crate_name.as_str()) else {
            continue; // skip if not in layer map (shouldn't happen)
        };

        for dep in deps {
            let Some(&to_layer) = layers.get(dep.as_str()) else {
                continue;
            };

            // A crate must NOT depend on a strictly higher layer.
            if to_layer > from_layer {
                violations.push(format!(
                    "{crate_name} (L{from_layer}) -> {dep} (L{to_layer})"
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "bead_id={BEAD_ID} case=no_cross_layer_backedges layer_violations_count={} violations:\n{}",
        violations.len(),
        violations.join("\n")
    );
}

#[test]
fn test_wal_does_not_depend_on_pager() {
    let metadata = cargo_metadata_cached();
    let graph = internal_dep_graph(metadata);

    if let Some(wal_deps) = graph.get("fsqlite-wal") {
        assert!(
            !wal_deps.contains("fsqlite-pager"),
            "bead_id={BEAD_ID} case=wal_pager_cycle_break \
             fsqlite-wal must NOT depend on fsqlite-pager (cycle breaker per §8.2)"
        );
    }
}

#[test]
fn test_mvcc_at_layer_3() {
    let layers = layer_assignments();
    assert_eq!(
        layers.get("fsqlite-mvcc"),
        Some(&3),
        "bead_id={BEAD_ID} case=mvcc_layer \
         fsqlite-mvcc must be at L3 (not L6) per §8.2 rationale"
    );
}

// ---------------------------------------------------------------------------
// E2E: combined workspace sanity check
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_bd_1wwc() {
    let metadata = cargo_metadata_cached();
    let members = workspace_member_names(metadata);
    let graph = internal_dep_graph(metadata);
    let layers = layer_assignments();

    // 1. Member count
    let member_count = members.len();
    assert_eq!(member_count, 23, "member_count={member_count}");

    // 2. Members missing from spec
    let expected: HashSet<&str> = EXPECTED_CRATES.iter().copied().collect();
    let actual: HashSet<&str> = members.iter().map(String::as_str).collect();
    let members_missing: Vec<_> = expected.difference(&actual).collect();
    assert!(
        members_missing.is_empty(),
        "members_missing={members_missing:?}"
    );

    // 3. Layer violations
    let mut layer_violations = Vec::new();
    for (crate_name, deps) in &graph {
        let Some(&from_layer) = layers.get(crate_name.as_str()) else {
            continue;
        };
        for dep in deps {
            let Some(&to_layer) = layers.get(dep.as_str()) else {
                continue;
            };
            if to_layer > from_layer {
                layer_violations.push(format!(
                    "{crate_name} (L{from_layer}) -> {dep} (L{to_layer})"
                ));
            }
        }
    }

    // Summary output (grep-friendly per bead requirement)
    eprintln!("member_count={member_count}");
    eprintln!("members_missing={}", members_missing.len());
    eprintln!("layer_violations_count={}", layer_violations.len());
    for v in &layer_violations {
        eprintln!("  {v}");
    }

    assert!(
        layer_violations.is_empty(),
        "bead_id={BEAD_ID} case=e2e_workspace_sanity layer_violations_count={}",
        layer_violations.len()
    );
}
