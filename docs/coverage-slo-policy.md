# Coverage and Realism SLO Policy

This document defines Service Level Objectives (SLOs) for code coverage and test realism in FrankenSQLite.

## Coverage Thresholds

### Workspace-Wide Minimums

| Metric | Target | Hard Fail | Current |
|--------|--------|-----------|---------|
| Line coverage | >= 80% | < 70% | ~75% |
| Function coverage | >= 75% | < 65% | ~72% |
| Branch coverage | >= 70% | < 60% | ~70% |

### Per-Crate Requirements

Critical crates have elevated requirements:

| Crate | Line Coverage | Rationale |
|-------|--------------|-----------|
| `fsqlite-mvcc` | >= 85% | Core correctness |
| `fsqlite-wal` | >= 85% | Durability |
| `fsqlite-core` | >= 80% | Integration |
| `fsqlite-types` | >= 80% | Foundation |
| `fsqlite-pager` | >= 75% | Performance-critical |
| Other crates | >= 70% | Standard |

### New Code Requirements

All PRs must maintain or improve coverage:
- No coverage regression > 1% on any critical crate
- New files must have >= 80% coverage
- New functions must have >= 75% coverage

---

## Realism Tier Requirements

Based on the test realism taxonomy:

| Tier | Description | Acceptable For |
|------|-------------|----------------|
| **unit** | Pure unit, no I/O | Edge cases, algorithms |
| **mocked** | Uses mocks/fakes | Interface testing |
| **in-memory** | MemDatabase/MemoryVfs | Fast integration |
| **file-backed** | tempfile I/O | Persistence paths |
| **e2e** | Full stack | Critical invariants |

### Critical Path Realism Requirements

Critical invariants (from `docs/critical-invariants.md`) MUST have:

| Invariant Class | Minimum Tier | E2E Required |
|-----------------|--------------|--------------|
| MVCC (INV-1 to INV-7) | in-memory | YES |
| Durability (INV-D1 to INV-D3) | file-backed | YES |
| Concurrent writers (INV-C1, INV-C2) | in-memory | YES |
| Schema (INV-S1) | in-memory | PREFERRED |
| Type system (INV-T1, INV-T2) | unit | NO |

### Mock Usage Limits

| Context | Mock Allowed | Notes |
|---------|--------------|-------|
| Unit tests | YES | Standard practice |
| Integration tests | DISCOURAGED | Prefer real backends |
| Critical path tests | NO | Must use real implementation |
| E2E tests | NO | Full stack only |

**Current mock usage**: 968 tests (3.1%) - ACCEPTABLE

---

## Quality Gates

### CI Gate Enforcement

```yaml
# .github/workflows/coverage.yml (example)
coverage-gate:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - name: Run coverage check
      run: |
        ./scripts/coverage.sh ci
      env:
        COVERAGE_THRESHOLD: 70
```

### Gate Levels (Staged Rollout)

| Phase | Line Threshold | Branch Threshold | Status |
|-------|---------------|-----------------|--------|
| Phase 1 (now) | 70% | 60% | ACTIVE |
| Phase 2 (Q2 2026) | 75% | 65% | PLANNED |
| Phase 3 (Q3 2026) | 80% | 70% | PLANNED |

### Hard Fail Criteria

A PR MUST be blocked if:
1. Workspace line coverage drops below 70%
2. Critical crate coverage drops below the per-crate minimum
3. A P0 invariant loses its E2E test coverage
4. Mock usage in critical path tests is introduced

---

## Exception Protocol

### Temporary Exceptions

1. **Request**: File an issue with `exception-request` label
2. **Justification**: Document why coverage cannot be achieved
3. **Duration**: Maximum 2 sprint cycles
4. **Tracking**: Exception must reference a remediation issue

### Permanent Exceptions

Only allowed for:
- Unreachable code paths (platform-specific, error handling)
- External dependencies with untestable paths
- Explicitly marked with `#[cfg(not(coverage))]`

All permanent exceptions must be documented in `docs/coverage-exceptions.md`.

---

## Monitoring and Reporting

### Weekly Reports

Generate weekly coverage reports:
```bash
./scripts/coverage.sh breakdown
```

### Trend Tracking

Coverage trends should be tracked:
- [ ] Add coverage badge to README
- [ ] Set up Codecov or similar integration
- [ ] Archive weekly coverage CSVs in `docs/coverage/history/`

### Alert Thresholds

| Condition | Alert Level | Action |
|-----------|-------------|--------|
| Coverage drop > 5% | CRITICAL | Block merge, investigate |
| Coverage drop 2-5% | WARNING | Review before merge |
| Coverage improvement | INFO | Celebrate |
| New uncovered critical path | CRITICAL | Immediate remediation |

---

## Implementation Checklist

- [x] Define coverage thresholds (this document)
- [x] Create `scripts/coverage.sh` with CI mode
- [ ] Add GitHub Actions workflow
- [ ] Set up coverage reporting integration
- [ ] Configure pre-merge coverage gates
- [ ] Document exception protocol

---

## Related Documents

- [Critical Invariants Catalog](critical-invariants.md)
- [Coverage Gap Report](coverage-gap-report.md)
- [Test Realism Inventory](test-realism/README.md)
- [ADR-0001: Coverage Toolchain](adr/0001-coverage-toolchain-selection.md)
