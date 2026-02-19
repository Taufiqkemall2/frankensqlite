//! Integration tests for bd-mblr.7.9.2 lane selection engine.
//!
//! Representative scenarios validate deterministic output, mandatory
//! safety-floor behavior, and uncertainty fallback.

use fsqlite_harness::ci_gate_matrix::CiLane;
use fsqlite_harness::impact_graph::ImpactGraph;
use fsqlite_harness::lane_selector::select_ci_lanes_for_paths;

const BEAD_ID: &str = "bd-mblr.7.9.2";
const SEED: u64 = 20_260_213;

fn assert_contains_lane(lanes: &[CiLane], lane: CiLane, case: &str) {
    assert!(
        lanes.contains(&lane),
        "bead_id={BEAD_ID} case={case} missing lane={}",
        lane.as_str(),
    );
}

#[test]
fn e2e_mvcc_change_selects_concurrency_and_recovery_with_safety_floor() {
    let graph = ImpactGraph::canonical();
    let report =
        select_ci_lanes_for_paths(&["crates/fsqlite-mvcc/src/witness_plane.rs"], &graph, None);
    let lanes = report.selected_lanes();

    assert_contains_lane(&lanes, CiLane::Unit, "mvcc_unit");
    assert_contains_lane(&lanes, CiLane::E2eCorrectness, "mvcc_correctness");
    assert_contains_lane(&lanes, CiLane::E2eDifferential, "mvcc_concurrency");
    assert_contains_lane(&lanes, CiLane::E2eRecovery, "mvcc_recovery");

    assert!(
        !report.fallback_full_suite,
        "bead_id={BEAD_ID} case=mvcc_no_fallback",
    );
    assert!(
        report
            .resolved_code_areas
            .contains(&"fsqlite-mvcc".to_owned()),
        "bead_id={BEAD_ID} case=mvcc_area_resolved",
    );

    eprintln!(
        "bead_id={BEAD_ID} phase=report event_type=pass run_id={BEAD_ID}-mvcc-{SEED} seed={SEED} lanes={:?}",
        lanes.iter().map(|lane| lane.as_str()).collect::<Vec<_>>()
    );
}

#[test]
fn e2e_parser_change_is_deterministic_across_path_order() {
    let graph = ImpactGraph::canonical();
    let report_a = select_ci_lanes_for_paths(
        &[
            "crates/fsqlite-parser/src/lib.rs",
            "crates/fsqlite-core/src/connection.rs",
        ],
        &graph,
        None,
    );
    let report_b = select_ci_lanes_for_paths(
        &[
            "crates/fsqlite-core/src/connection.rs",
            "crates/fsqlite-parser/src/lib.rs",
        ],
        &graph,
        None,
    );

    assert_eq!(
        report_a, report_b,
        "bead_id={BEAD_ID} case=deterministic_ordering",
    );

    let lanes = report_a.selected_lanes();
    assert_contains_lane(&lanes, CiLane::E2eDifferential, "parser_differential");
    assert_contains_lane(&lanes, CiLane::E2eCorrectness, "parser_correctness");
    assert_contains_lane(&lanes, CiLane::E2eRecovery, "parser_recovery_floor");
}

#[test]
fn e2e_unmapped_code_path_triggers_full_suite_fallback() {
    let graph = ImpactGraph::canonical();
    let report =
        select_ci_lanes_for_paths(&["crates/experimental-new-crate/src/lib.rs"], &graph, None);

    assert!(
        report.fallback_full_suite,
        "bead_id={BEAD_ID} case=unknown_full_suite_fallback",
    );
    assert_eq!(
        report.selected_lanes().len(),
        CiLane::ALL.len(),
        "bead_id={BEAD_ID} case=unknown_all_lanes",
    );
    assert!(
        report
            .unresolved_paths
            .contains(&"crates/experimental-new-crate/src/lib.rs".to_owned()),
        "bead_id={BEAD_ID} case=unknown_path_recorded",
    );
}
