# Critical-Path Coverage Gap Report

Generated: 2026-02-13
Status: Initial baseline

## Executive Summary

This report maps critical correctness paths to current test coverage and identifies gaps requiring attention.

**Overall Assessment**: Core MVCC and WAL crates have good coverage (70-95%). Integration paths and error handling show gaps.

---

## Critical Path Coverage Matrix

### 1. MVCC Conflict Detection (fsqlite-mvcc)

| Component | Line Coverage | Risk Level | Notes |
|-----------|--------------|------------|-------|
| `invariants.rs` | ~90% | LOW | All 7 invariants have dedicated tests |
| `core_types.rs` | ~75% | MEDIUM | Transaction lifecycle well-covered |
| `conflict_model.rs` | ~70% | MEDIUM | SSI detection tested |
| `deterministic_rebase.rs` | ~65% | HIGH | Safe merge ladder needs more edge cases |
| `gc.rs` | ~60% | MEDIUM | GC horizon logic needs stress tests |

**Gap Priority**:
1. Deterministic rebase edge cases (split/merge during rebase)
2. GC under concurrent load
3. Multi-transaction conflict chains

### 2. WAL Replay & Recovery (fsqlite-wal)

| Component | Line Coverage | Risk Level | Notes |
|-----------|--------------|------------|-------|
| `wal.rs` | 96% | LOW | Core WAL operations well-tested |
| `checkpoint.rs` | 99% | LOW | Checkpoint logic comprehensive |
| `recovery_compaction.rs` | 91% | LOW | Recovery paths tested |
| `checksum.rs` | 92% | LOW | Checksum validation covered |
| `wal_fec.rs` | 75% | MEDIUM | RaptorQ repair needs more cases |

**Gap Priority**:
1. wal_fec partial symbol loss scenarios
2. Recovery after truncated commit
3. Checkpoint under write pressure

### 3. Pager & Buffer Pool (fsqlite-pager)

| Component | Line Coverage | Risk Level | Notes |
|-----------|--------------|------------|-------|
| `pager.rs` | ~50% | HIGH | Core pager paths under-tested |
| `arc_cache.rs` | ~40% | HIGH | ARC eviction logic sparse |
| `page_buf.rs` | ~30% | HIGH | Page buffer lifecycle gaps |

**Gap Priority**:
1. ARC eviction under memory pressure
2. Dirty page write-back ordering
3. Page pinning/unpinning lifecycle

### 4. Transaction Lifecycle (fsqlite-core)

| Component | Line Coverage | Risk Level | Notes |
|-----------|--------------|------------|-------|
| `connection.rs` | ~80% | LOW | Public API well-tested |
| `commit_marker.rs` | ~75% | MEDIUM | Commit marker persistence |
| `commit_repair.rs` | ~70% | MEDIUM | Repair scenarios |
| `compat_persist.rs` | ~60% | MEDIUM | Compatibility mode |

**Gap Priority**:
1. Nested savepoint edge cases
2. Transaction abort during commit
3. Cross-connection coordination

### 5. Schema Operations (fsqlite-core)

| Component | Line Coverage | Risk Level | Notes |
|-----------|--------------|------------|-------|
| `attach.rs` | ~50% | HIGH | ATTACH/DETACH sparse |
| Schema DDL paths | ~60% | MEDIUM | CREATE/ALTER/DROP |

**Gap Priority**:
1. ALTER TABLE in concurrent context
2. DROP TABLE with active cursors
3. Schema epoch propagation

---

## Missing Test Cases Backlog (Prioritized)

### P0 - Critical (blocks release)

1. **PAGER-001**: ARC cache eviction under memory pressure
   - Path: `fsqlite-pager::arc_cache::evict_if_needed`
   - Risk: Data loss if dirty pages evicted incorrectly
   - Effort: M

2. **MVCC-001**: Deterministic rebase during B-tree split
   - Path: `fsqlite-mvcc::deterministic_rebase::replay_intent`
   - Risk: Rebase could produce invalid B-tree state
   - Effort: L

3. **WAL-001**: Recovery with partial RaptorQ symbols
   - Path: `fsqlite-wal::wal_fec::repair_frame`
   - Risk: Incorrect repair could corrupt data
   - Effort: M

### P1 - High (pre-beta)

4. **TXN-001**: Nested savepoint rollback with concurrent writers
   - Path: `fsqlite-core::connection::savepoint_rollback`
   - Risk: Visibility inconsistency
   - Effort: M

5. **GC-001**: GC horizon advancement under concurrent snapshots
   - Path: `fsqlite-mvcc::gc::advance_horizon`
   - Risk: Premature version reclamation
   - Effort: L

6. **SCHEMA-001**: ALTER TABLE with active concurrent queries
   - Path: `fsqlite-core::schema_ops`
   - Risk: Query sees inconsistent schema
   - Effort: H

### P2 - Medium (post-beta)

7. **PAGER-002**: Page buffer alignment for direct I/O
   - Path: `fsqlite-pager::page_buf`
   - Risk: Performance regression
   - Effort: S

8. **WAL-002**: Checkpoint under sustained write load
   - Path: `fsqlite-wal::checkpoint_executor`
   - Risk: Checkpoint lag grows unbounded
   - Effort: M

9. **ATTACH-001**: ATTACH with concurrent writes to both databases
   - Path: `fsqlite-core::attach`
   - Risk: Deadlock or corruption
   - Effort: H

---

## Test Realism Distribution (from inventory)

| Critical Path | Unit Tests | E2E Tests | Proptest | Gap Assessment |
|--------------|------------|-----------|----------|----------------|
| MVCC | 450 | 200 | 150 | ADEQUATE |
| WAL | 120 | 50 | 30 | ADEQUATE |
| Pager | 80 | 10 | 20 | NEEDS E2E |
| Transaction | 300 | 150 | 50 | ADEQUATE |
| Schema | 50 | 20 | 10 | NEEDS E2E |

---

## Recommended Actions

### Immediate (This Sprint)

1. Add E2E tests for pager under memory pressure
2. Add rebase edge case tests for MVCC
3. Add partial symbol recovery tests for WAL

### Near-Term (Next 2 Sprints)

4. Build schema operation E2E test suite
5. Expand GC stress testing
6. Add checkpoint lag monitoring tests

### Long-Term (Backlog)

7. Fuzz testing for parser/B-tree edge cases
8. Multi-process MVCC coordination tests
9. Cross-platform VFS compatibility tests

---

## Related Documents

- [Critical Invariants Catalog](critical-invariants.md)
- [Test Realism Inventory](test-realism/README.md)
- [ADR-0001: Coverage Toolchain](adr/0001-coverage-toolchain-selection.md)

## Updating This Report

Run coverage analysis and update:
```bash
./scripts/coverage.sh crate fsqlite-mvcc
./scripts/coverage.sh crate fsqlite-wal
./scripts/coverage.sh crate fsqlite-pager
./scripts/coverage.sh crate fsqlite-core
```
