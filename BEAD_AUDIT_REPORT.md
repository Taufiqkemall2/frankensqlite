# Bead Audit Report: Comprehensive Spec Coverage

**Date:** 2026-02-08
**Spec:** `COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md` (18,180 lines)
**Status:** Audit Complete.

## Summary
The bead coverage for the specification is exceptionally high. The initial pass created 149 beads that map very closely to the specification's structure. Major sections like §3 (RaptorQ), §11 (File Format), and §17 (Testing) are broken down into granular, actionable tasks.

However, detailed analysis revealed **overlap and density issues** in two key sections:
1.  **§5.10 (Safe Write Merging):** Multiple beads covered overlapping ranges of subsections (§5.10.1-2 vs §5.10.1-1.1), creating ambiguity in scope.
2.  **§4 (Asupersync Integration):** Several "clumped" beads covered multiple dense subsections (e.g., §4.11-4.13, §4.18-4.19) that warrant individual tracking due to their complexity.

## Coverage Analysis by Section

| Section | Status | Notes |
| :--- | :--- | :--- |
| **§0 Governance** | ✅ Covered | Glossary types and RaptorQ audit covered. |
| **§1 Identity** | ✅ Excellent | Mechanical sympathy & critical controls broken out individually. |
| **§2 MVCC Rationale** | ✅ Covered | Layers 1-3, write skew tests, and PRAGMA covered. |
| **§3 RaptorQ** | ✅ Excellent | 33 granular beads covering math, WAL, replication, ECS, indexes. |
| **§4 Asupersync** | ⚠️ **Dense** | **Action:** Split §4.11-13 and §4.18-19 for better granularity. |
| **§5 MVCC Model** | ⚠️ **Overlap** | **Action:** Refactor §5.10 beads to eliminate range overlaps. |
| **§6 Buffer Pool** | ✅ Covered | ARC algo, eviction, memory accounting covered. |
| **§7 Integrity** | ✅ Covered | Checksums, recovery, compaction covered. |
| **§8 Architecture** | ✅ Covered | Crates & dependencies covered. |
| **§9 Traits** | ✅ Covered | Storage, function, extension traits covered. |
| **§10 Query Pipeline** | ✅ Covered | Parser, planner, VDBE, precedence covered. |
| **§11 File Format** | ✅ Excellent | Detailed coverage including edge cases (varint, SHM hash). |
| **§12 SQL Coverage** | ✅ Good | DML, DDL, Txn, Time Travel covered. Deep-dives on Triggers. |
| **§13 Functions** | ✅ Good | Scalar, Agg, Window, Math, Date covered. |
| **§14 Extensions** | ✅ Good | JSON, FTS, R-Tree, etc. covered. |
| **§15 Exclusions** | ✅ Covered | Encryption and PRAGMA compatibility covered. |
| **§16 Phases** | ✅ Covered | Aligned with implementation plan. |
| **§17 Testing** | ✅ Excellent | Comprehensive coverage of all testing methodologies. |
| **§18 Conflict Model** | ✅ Excellent | Probabilistic model and estimators covered. |
| **§19-23 Meta** | ✅ Covered | Reference, risks, gates, summary covered. |

## Recommended Fixes

### 1. Refactor §5.10 (Write Merging)
The current beads overlap significantly. We will strictly partition them:
- **§5.10.1:** Intent Logs & RowId Allocation (`bd-2blq`)
- **§5.10.2:** Deterministic Rebase & Index Regen (`bd-1h3b`)
- **§5.10.3-5:** Physical Merge & Safety Proofs (`bd-3dv4`)
- **§5.10.6-8:** History & Certificates (`bd-c6tx`)
- **Duplicate:** `bd-13b7` (covers 5.10.1-2) will be closed in favor of the above.

### 2. Split §4 (Asupersync)
Complex subsystems need individual tracking:
- **Split `bd-3go.9`** (§4.11-4.13) into three beads:
    - §4.11 Structured Concurrency (Regions)
    - §4.12 Cancellation Protocol
    - §4.13 Obligations
- **Split `bd-3go.12`** (§4.18-4.19) into two beads:
    - §4.18 Epochs
    - §4.19 Remote Effects

A script `fix_beads.sh` has been generated to apply these changes using the `br` tool.
