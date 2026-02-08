# ANALYSIS_OF_SPEC_DOC_DIFFS.md

This is a living, incrementally-expandable audit log of how the FrankenSQLite V1 spec evolved over time, commit-by-commit, with each logical change-group categorized into the 10 buckets defined below.

**Scope (current):**
- Primary document: `COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md`
- Data source: local git history (no GitHub API usage; all diffs are from `git show`)
- As-of: `2026-02-07`

---

## Buckets (Canonical)

1. **Logic/Math Fixes**: fixing outright mistakes in logic, math, or reasoning.
2. **SQLite Legacy Corrections**: fixing inaccurate statements about the C SQLite codebase or semantics.
3. **asupersync Corrections**: fixing inaccurate statements about asupersync APIs/behavior.
4. **Architecture Fixes**: fixing conceptual errors or architectural mistakes.
5. **Scrivening**: ministerial fixes (renumbering, references, wording cleanup).
6. **Added Context**: background info to make the spec more self-contained.
7. **Standard Engineering**: improvements based on standard engineering (perf, cache, concurrency mechanics, durability mechanics).
8. **Alien Artifact Math**: esoteric math/rigor additions (e-processes, conformal, BOCPD, decision theory, proofs, bounds, sketching).
9. **Clarification**: elaboration/clarification without substantive improvements or fixes.
10. **Other**: catch-all.

**Multi-label reality:** a change-group may belong to multiple buckets. For visualization stacks that must be disjoint, assign a `primary` bucket using post-hoc judgment (“what was the real point of this change?”).

---

## Method (How To Read / How To Extend)

Each commit may contain multiple **change-groups**. The unit of analysis is the *logical change-group*, not the git commit.

For each deep-reviewed commit, this file records:
- `stats`: `+added/-deleted` for the spec doc
- `groups[]`: structured change-groups
  - `primary_bucket`: 1-10
  - `buckets`: multi-label list
  - `confidence`: how confident the categorization is (0-1)
  - `diff_notes`: small, high-signal excerpts or descriptions of specific edits
  - `why`: post-hoc rationale (“what this fixed/added and why it matters”)

When expanding this file, prefer:
- splitting large commits into multiple groups
- writing *verifiable* diff-notes (quote small snippets; cite section numbers/headings)
- capturing “why this matters” (architectural correctness, durability, perf, or future implementation constraints)

---

## Deep Review Subset A (2026-02-07 15:27–17:28 ET)

This is a focused deep-review window selected because it contains dense correctness and architecture hardening around MVCC conflict modeling, TxnSlot cleanup, SHM safety, `.db-fec` semantics, and commit sequencing.

| # | Commit | Time (ISO) | + / - | Impact | Subject |
|---:|---|---|---:|---:|---|
| 1 | `29f7ebe` | `2026-02-07T15:27:04-05:00` | `+262 / -65` | `327` | spec: harden rebase + safe SHM + skew-aware conflicts |
| 2 | `6b0c12f` | `2026-02-07T15:30:14-05:00` | `+3 / -1` | `4` | spec: fix §16 Phase 7 join ordering — beam search, not exhaustive |
| 3 | `b181b6d` | `2026-02-07T15:31:48-05:00` | `+2 / -2` | `4` | spec: fix §8.3 planner join ordering — beam search, not exhaustive |
| 4 | `d302b39` | `2026-02-07T15:44:17-05:00` | `+229 / -45` | `274` | mvcc/spec: witness hot-index sizing manifest |
| 5 | `0177456` | `2026-02-07T15:44:51-05:00` | `+12 / -2` | `14` | spec: clarify Zipf write-set skew section |
| 6 | `5dae90d` | `2026-02-07T15:45:46-05:00` | `+5 / -1` | `6` | spec: tighten Zipf s_hat guidance |
| 7 | `ca60e00` | `2026-02-07T15:47:59-05:00` | `+51 / -0` | `51` | spec: define .db-fec physical layout + crash-consistent update |
| 8 | `30203fb` | `2026-02-07T15:50:22-05:00` | `+6 / -2` | `8` | spec: reserve TxnId sentinels + guard allocation |
| 9 | `75ac25d` | `2026-02-07T16:07:05-05:00` | `+116 / -51` | `167` | spec: harden TxnId alloc + replication changeset id + ARC singleflight |
| 10 | `ec9adc1` | `2026-02-07T16:13:12-05:00` | `+16 / -8` | `24` | spec: fix TxnId monotonicity note + clarify P_eff |
| 11 | `e80fdde` | `2026-02-07T16:14:32-05:00` | `+12 / -3` | `15` | spec: deterministic RaptorQ seed for ChangesetId |
| 12 | `fa25db0` | `2026-02-07T16:21:05-05:00` | `+78 / -34` | `112` | spec: adopt NGQP beam search for V1 join ordering |
| 13 | `1d8bbfb` | `2026-02-07T16:22:50-05:00` | `+6 / -2` | `8` | spec: add TxnId CAS abort path and correct beam search complexity |
| 14 | `4432a3d` | `2026-02-07T16:25:52-05:00` | `+119 / -26` | `145` | spec: conformance mode matrix; bump asupersync |
| 15 | `aa8e816` | `2026-02-07T16:28:25-05:00` | `+45 / -20` | `65` | spec: tighten serialized FCW + schema_epoch open + rebase read footprint |
| 16 | `0a8d867` | `2026-02-07T16:38:09-05:00` | `+261 / -135` | `396` | spec: fix TxnSlot cleanup crash-safety and reconcile lock/VFS semantics |
| 17 | `3d56854` | `2026-02-07T16:39:53-05:00` | `+16 / -16` | `32` | spec: fix Vfs trait formatting and cleanup_txn_id comment |
| 18 | `4c07e10` | `2026-02-07T16:41:55-05:00` | `+78 / -57` | `135` | spec: clarify TxnSlot cleanup_txn_id + fix Vfs trait formatting |
| 19 | `df0313b` | `2026-02-07T16:42:11-05:00` | `+5 / -5` | `10` | spec: fix ARC/CAR comment indentation |
| 20 | `97df1f0` | `2026-02-07T16:42:40-05:00` | `+4 / -0` | `4` | spec: clarify zero-copy terminology |
| 21 | `bbc4a31` | `2026-02-07T16:45:48-05:00` | `+3 / -0` | `3` | spec: define canonical AAD encoding for page encryption |
| 22 | `4363f50` | `2026-02-07T16:51:53-05:00` | `+44 / -2` | `46` | spec: add critical implementation controls checklist |
| 23 | `d9021cf` | `2026-02-07T16:52:38-05:00` | `+5 / -4` | `9` | spec: clarify rebase rowid reuse + DatabaseId encoding |
| 24 | `29107df` | `2026-02-07T17:00:26-05:00` | `+109 / -166` | `275` | spec: harden TxnSlot cleanup and epoch reset semantics |
| 25 | `f708f33` | `2026-02-07T17:01:38-05:00` | `+242 / -100` | `342` | spec: clarify pipelined durability and compatibility spill semantics |
| 26 | `a71e1d9` | `2026-02-07T17:07:05-05:00` | `+178 / -105` | `283` | spec: harden ECS root update; snapshot slot tid; clarify ESCAPE parsing |
| 27 | `120eee2` | `2026-02-07T17:08:11-05:00` | `+6 / -6` | `12` | spec: strengthen WAL-FEC per-source validation hash to xxh3_128 |
| 28 | `975f65c` | `2026-02-07T17:08:59-05:00` | `+17 / -2` | `19` | spec: clarify GF(256) elimination note; bound delta reconstruction cost |
| 29 | `24b6f60` | `2026-02-07T17:15:05-05:00` | `+3 / -3` | `6` | spec: fix GC scheduling cross-reference |
| 30 | `80decf6` | `2026-02-07T17:28:31-05:00` | `+15 / -10` | `25` | spec: clarify db-fec generation digest + ESI terminology |

---

## Commit Deep Reviews

### `29f7ebe` (2026-02-07T15:27:04-05:00) — harden rebase + safe SHM + skew-aware conflicts

**stats:** `+262 / -65` (impact `327`)

#### Group 1 — Deterministic Rebase Is Not Allowed Inside the Sequencer
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 7, 1, 9
- **confidence:** 0.9
- **diff_notes:**
  - Adds a normative rule: deterministic rebase MUST run in the committing transaction context *before* entering the serialized commit section; the coordinator MUST NOT do B-tree traversal / expression evaluation / index-key regeneration inside its critical section.
  - Tightens the spec for index regeneration during rebase: partial indexes require predicate evaluation; expression indexes require expression evaluation with correct affinity/collation; UNIQUE enforcement must be against the new committed base snapshot (abort on violations).
- **why:**
  - Preserves the “tiny sequencer” invariant (critical for both Native mode and compat WAL append critical section).
  - Fixes a latent correctness hole: treating index ops as “replayable bytes” during rebase can violate SQLite semantics (partial/expr indexes, uniqueness).

#### Group 2 — SHM/VFS Safety: No `unsafe` Escape Hatch for Mmap
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 5, 9
- **confidence:** 0.75
- **diff_notes:**
  - Replaces language implying an `unsafe_code` exception for VFS SHM mapping with a stricter rule: the workspace forbids `unsafe`; VFS implementations must rely on safe wrappers that encapsulate `unsafe` outside this repo (e.g., safe SHM mapping APIs).
- **why:**
  - Aligns the spec with the repo’s lints/constraints and prevents “spec drift” where the spec implicitly blesses unsafe in-core.

#### Group 3 — Conflict Modeling: Shift From “Zipf Page Access” to Measured Write-Set Collision Mass
- **primary_bucket:** 1 (Logic/Math Fixes)
- **buckets:** 1, 7, 8, 4
- **confidence:** 0.8
- **diff_notes:**
  - Reframes skew: the conflict model depends on the distribution of pages in *write sets*, not read-hot pages.
  - Introduces `M2 := Σ q(pgno)^2` and `P_eff := 1/M2` as model-free policy inputs, and updates instrumentation to record `M2_hat`, `P_eff_hat`, and conflict breakdown by page kind.
  - Clarifies benchmark target: match `M2_hat`-based prediction within ~20% for skewed workloads; treat Zipf `s_hat` as interpretability-only.
- **why:**
  - Fixes a conceptual misalignment: “Zipf reads” is not the right sufficient statistic for write conflicts (root is read-hot but often write-cold).
  - Enables policies to reason about the real collision geometry directly (collision mass), independent of whether a Zipf fit is good.

#### Group 4 — Retry/Backoff Policy: Learn `p_succ(t | evidence)` Per Regime
- **primary_bucket:** 8 (Alien Artifact Math)
- **buckets:** 8, 7, 6
- **confidence:** 0.7
- **diff_notes:**
  - Makes `p_succ(t | evidence)` estimation normative, with a recommended discrete Beta-Bernoulli model over a finite action set `T` and optional exponential hazard smoothing.
  - Requires evidence-ledger outputs for policy decisions (inputs, posteriors, expected loss per candidate, selected action, regime context).
- **why:**
  - Prevents “hand-wavy backoff” from becoming an implementation footgun; yields explainable, workload-adaptive retry.

### `d302b39` (2026-02-07T15:44:17-05:00) — witness hot-index sizing manifest

**stats:** `+229 / -45` (impact `274`)

#### Group 1 — `.db-fec` Repair Symbols Must Bind to a Group Snapshot (ObjectId + OTI)
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 4, 1
- **confidence:** 0.85
- **diff_notes:**
  - Adds `DbFecGroupMeta.object_id` and requires repair `SymbolRecord`s to match `(object_id, oti)`; readers must ignore repair records that don’t match the active group snapshot.
- **why:**
  - Eliminates “symbol mixing” across checkpoints/generations, which would otherwise create silent decode ambiguity or, worse, “successful repair to wrong bytes”.

#### Group 2 — Make `M2_hat` Estimation Concrete: Deterministic AMS F2 Sketch + Explainable Heavy-Hitters
- **primary_bucket:** 8 (Alien Artifact Math)
- **buckets:** 8, 7, 1
- **confidence:** 0.85
- **diff_notes:**
  - Promotes second-moment sketching from “recommended” to **required** with a normative AMS-style estimator (`F2_hat := median(z_r^2)`).
  - Specifies deterministic seeding (`BLAKE3(...)`), a canonical `mix64` (SplitMix64 finalization), bounded parameters (`R=12` default), and lab-mode validation requirements.
  - Adds an optional deterministic SpaceSaving heavy-hitters table for explainability and a conservative head/tail decomposition.
- **why:**
  - Converts “measure skew” from prose to an implementable, testable, deterministic algorithm with explicit evidence ledger requirements (alien-artifact level operational rigor).

#### Group 3 — Skewed Sharding: Replace Zipf Hand-Waves With `M2_shard` and `S_eff`
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 8, 9
- **confidence:** 0.7
- **diff_notes:**
  - Rewrites PageLockTable shard collision discussion in terms of shard collision mass (`M2_shard`) and effective shard count (`S_eff := 1/M2_shard`).
- **why:**
  - Makes “hot shards” quantifiable and connects it to the same collision-mass machinery used elsewhere.

#### Group 4 — Clarify Conflict/Throughput Math and Planner File Map
- **primary_bucket:** 1 (Logic/Math Fixes)
- **buckets:** 1, 6, 2
- **confidence:** 0.55
- **diff_notes:**
  - Replaces `P_conflict` with `p_drift` (base-drift at commit) and ties `p_drift` to `M2_hat` and active writer count; clarifies heavy-tail effects via `E[W^2]`.
  - Adds missing SQLite “WHERE subsystem” file breakdown (`wherecode.c`, `whereexpr.c`, `whereInt.h`) to the legacy map table.
- **why:**
- Fixes modeling terminology so the measured thing (base drift) matches the policy decisions (retry/merge budgeting).
- Reduces “spec drift” in the SQLite reference map by acknowledging the optimizer/codegen split in upstream.

### `fa25db0` (2026-02-07T16:21:05-05:00) — NGQP-style beam search for join ordering (plus TxnSlot + ARC liveness hardening)

**stats:** `+78 / -34` (impact `112`)

#### Group 1 — Replace “N!/Greedy” Join Ordering With NGQP Beam Search (V1)
- **primary_bucket:** 2 (SQLite Legacy Corrections)
- **buckets:** 2, 7, 1
- **confidence:** 0.85
- **diff_notes:**
  - Removes the “N<=8 exhaustive N!, else greedy” join-order story; replaces with bounded best-first/beam search modeled on SQLite NGQP (`wherePathSolver()`).
  - Makes `mxChoice` a tuning knob derived from join complexity (1/5/12/18, star-heuristic), and states there is no `N!` exhaustive path in V1.
- **why:**
  - Fixes a major spec drift risk: the originally-described join-ordering strategy diverged from SQLite’s actual optimizer architecture and complexity envelope.

#### Group 2 — TxnSlot Publish Must Be CAS, Not Store (TOCTOU Defense)
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 1
- **confidence:** 0.8
- **diff_notes:**
  - Changes Phase 3 “publish” from a plain store to `CAS(txn_id, TXN_ID_CLAIMING -> real_txn_id)` and adds the failure case: if CAS fails, cleanup reclaimed the slot, so acquisition must restart.
  - Requires `claiming_timestamp` be set *after* successful Phase 1 CAS and be seeded via `CAS(0 -> now)` so no actor can extend timeouts by overwriting an earlier seed.
- **why:**
  - Prevents a stalled claimer from clobbering a slot that cleanup has already reclaimed, which would corrupt cross-process shared state.

#### Group 3 — ARC REPLACE Must Be Terminating Under Pinned/Dirty Candidates
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 4
- **confidence:** 0.65
- **diff_notes:**
  - Tightens the REPLACE pseudocode to fall back from preferred list (T1/T2) when exhausted, rather than “spin and hope”; makes termination/liveness explicit.
  - Expands the ARC “policy vs physical implementation” note: exact ARC vs CAR, and explicitly warns that CAR is a different algorithm and must be implemented/validated as such.
- **why:**
  - Removes an implicit liveness footgun: pinned/dirty pages + naive preference logic can otherwise produce non-terminating eviction attempts.

### `75ac25d` (2026-02-07T16:07:05-05:00) — harden TxnId allocation + replication changeset id + ARC singleflight/flush protocol

**stats:** `+116 / -51` (impact `167`)

#### Group 1 — TxnId Allocation: Replace `fetch_add` With CAS Loop to Prevent Sentinel Publication + Wrap Hazards
- **primary_bucket:** 1 (Logic/Math Fixes)
- **buckets:** 1, 4
- **confidence:** 0.9
- **diff_notes:**
  - Forbids `fetch_add` for `next_txn_id` because it advances even when the txn aborts and will eventually wrap to `TxnId=0`.
  - Specifies a CAS loop that refuses to publish `0`, `TXN_ID_CLEANING`, or `TXN_ID_CLAIMING` (TxnSlot sentinel values).
- **why:**
  - This is a hard correctness constraint for shared-memory coordination: once you publish a reserved TxnId, the slot protocol and lock ownership become ambiguous and unsafe.

#### Group 2 — Replication Naming + Identity: `ChangesetId` Is Not ECS `ObjectId`
- **primary_bucket:** 5 (Scrivening)
- **buckets:** 5, 4, 9
- **confidence:** 0.8
- **diff_notes:**
  - Renames `changeset_object_id` to `changeset_id` and explicitly states it is a RaptorQ stream identifier, not an ECS durable `ObjectId`.
- **why:**
  - Avoids a future implementation bug class: treating transport-level changeset IDs as durable-content addresses would break layering and auditability.

#### Group 3 — Replication Decoder State: Validate Parameters, Deduplicate Symbols, and Truncate Padding Deterministically
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 1
- **confidence:** 0.8
- **diff_notes:**
  - Decoder tracks `(k_source, symbol_size)` per `ChangesetId`, rejects inconsistent packets, enforces `1 <= K_source <= 56,403`, and deduplicates by ISI before counting “received”.
  - On decode, recovers padded bytes and truncates to `ChangesetHeader.total_len` to ignore final-symbol padding.
- **why:**
  - Makes the replication receiver robust to malformed or inconsistent traffic and removes ambiguity about how to interpret padding.

#### Group 4 — ARC Cache: Explicit `flush_inflight` + Singleflight Load Status for Waiters
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 4
- **confidence:** 0.7
- **diff_notes:**
  - Adds `flush_inflight` to prevent concurrent flushes and to block eviction while WAL write is active; requires cancellation-masking so inflight isn’t stranded.
  - Refines the “Loading placeholder” pattern: replaces `Option<Result<...>>` with `LoadStatus {Pending|Ok|Err(Arc<Error>)}` so waiters can observe loader failure deterministically.
- **why:**
  - These are classic production hazards in async + cache systems: without singleflight and explicit inflight claims, you get thundering herds, stuck pages, or latent deadlocks.

### `aa8e816` (2026-02-07T16:28:25-05:00) — serialized freshness validation + durable schema_epoch on open + rebase read-footprint clarification

**stats:** `+45 / -20` (impact `65`)

#### Group 1 — Serialized Mode Still Needs Freshness Validation (FCW for Reader→Writer)
- **primary_bucket:** 1 (Logic/Math Fixes)
- **buckets:** 1, 4
- **confidence:** 0.85
- **diff_notes:**
  - Changes the serialized-mode commit row in the mode matrix from “no validation needed” to “FCW freshness validation”.
  - Clarifies serialized begin semantics: writer exclusion can fail immediately with `SQLITE_BUSY` if concurrent writers are active (or wait under busy-timeout), otherwise may wait behind serialized mutex.
- **why:**
  - Serialization prevents write-write concurrency, but it does not magically make a stale snapshot safe to write: a read-then-write transaction must not overwrite newer commits.

#### Group 2 — Schema Epoch Must Be Initialized From Durable Reality (and Reconciled If SHM Exists)
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 2
- **confidence:** 0.75
- **diff_notes:**
  - Adds a normative open rule: set `shm.schema_epoch` from the durable schema epoch (Native: `RootManifest.schema_epoch`; Compat: durable schema cookie at WAL tip).
  - Requires reconciling existing SHM so `shm.schema_epoch` cannot remain “ahead” of durable reality.
- **why:**
  - Prevents mixed-schema snapshots and schema drift across processes, a subtle class of MVCC correctness failures.

#### Group 3 — IntentFootprint: Semantic Reads vs Re-validated Uniqueness Probes
- **primary_bucket:** 9 (Clarification)
- **buckets:** 9, 4
- **confidence:** 0.65
- **diff_notes:**
  - Clarifies that `footprint.reads` is for semantic reads that cannot be re-evaluated during rebase.
  - Explicitly excludes uniqueness checks for keys being written: those are re-validated during replay.
- **why:**
  - Prevents over-approximation of blocking reads that would unnecessarily reduce rebase/merge opportunities.

### `0a8d867` (2026-02-07T16:38:09-05:00) — TxnSlot crash cleanup retryability + shared lock-table canon + rebase probe semantics + schema cookie correction

**stats:** `+261 / -135` (impact `396`)

#### Group 1 — TxnSlot Cleanup Must Be Retryable After Cleaner Crash (`cleanup_txn_id`) + Release Locks via Shared Scan
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 1, 7
- **confidence:** 0.9
- **diff_notes:**
  - Introduces `cleanup_txn_id` recorded before `txn_id` sentinel overwrite so cleanup is crash-retryable.
  - Redefines `claiming_timestamp` as “sentinel-entry time” for both CLAIMING and CLEANING, so stuck sentinels can be detected uniformly.
  - Adds a normative `release_page_locks_for(txn_id)` that scans the shared lock table and CASes `owner_txn` to 0 without clearing the key (key-stability).
- **why:**
  - Correctness: if a cleaner crashes mid-release, locks must not leak indefinitely.
  - Cross-process reality: the crashed txn’s in-process lock set is gone, so cleanup must be possible from shared state alone.

#### Group 2 — SharedPageLockTable Is Canonical in Concurrent Mode (In-Process Lock Table Is Test-Only)
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 7, 9
- **confidence:** 0.85
- **diff_notes:**
  - Makes the shared-memory `SharedPageLockTable` the single source of truth for page writer exclusion in concurrent mode.
  - Renames the illustrative `PageLockTable` to `InProcessPageLockTable` and explicitly bans its use for cross-process attachments.
- **why:**
  - Prevents a disastrous “two lock tables” split-brain where different processes enforce different exclusion rules.

#### Group 3 — Deterministic Rebase Must Treat Branchy Conflict Policies as Blocking Reads (or Forbid Them)
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 1, 2
- **confidence:** 0.85
- **diff_notes:**
  - Refines IntentFootprint semantics: probes are only non-blocking for policies that abort/fail on violation; for `OR IGNORE`, `REPLACE`, UPSERT branches, the probe becomes an observable branch decision.
  - V1 requirement: forbid these unless the intent log encodes the chosen branch; until then record probe as blocking read.
- **why:**
  - Without this, deterministic rebase could silently change program behavior by taking a different branch at replay time.

#### Group 4 — SQLite Schema Cookie Is Mod-2^32; Merge Safety Requires Equality, Not Monotonicity
- **primary_bucket:** 2 (SQLite Legacy Corrections)
- **buckets:** 2, 1, 9
- **confidence:** 0.8
- **diff_notes:**
  - Corrects the schema cookie assumption: it’s a 32-bit counter modulo 2^32; numeric monotonicity is not reliable.
- **why:**
  - Prevents spec-driven bugs where code treats wrap/decrease as corruption; the safe invariant is “cookie changed => schema changed”, not “cookie always increases”.

#### Group 5 — ARC Spec: Abstract Physical Structures (EntryRef/RecencyStore/GhostStore) and Stop Implying LinkedHashMap Is Canonical
- **primary_bucket:** 9 (Clarification)
- **buckets:** 9, 7, 4
- **confidence:** 0.6
- **diff_notes:**
  - Introduces conceptual structs for recency/ghost stores and changes `ArcCache` fields to `RecencyStore`/`GhostStore`, separating policy from implementation.
- **why:**
  - Reduces “spec drift” where a reader might assume the reference struct layout is intended as the hot-path implementation.

### `29107df` (2026-02-07T17:00:26-05:00) — TxnSlot cleanup sentinel flow + “eviction is pure” cache semantics

**stats:** `+109 / -166` (impact `275`)

#### Group 1 — Sentinel Cleanup Flow: Always `continue` for Recent CLAIMING (Never Interpret Stale PID/Lease Fields)
- **primary_bucket:** 1 (Logic/Math Fixes)
- **buckets:** 1, 4
- **confidence:** 0.75
- **diff_notes:**
  - Adds the missing “CLAIMING recently; give the claimer time” `continue`, preventing the lease-expiry liveness path from running on a CLAIMING slot with stale PID metadata.
- **why:**
  - Without this, cleanup can accidentally treat stale PID/lease data as real, leading to incorrect cleanup behavior.

#### Group 2 — ARC Eviction Must Not Perform WAL/Durability I/O (Coordinator Owns Durability)
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 7
- **confidence:** 0.7
- **diff_notes:**
  - Rewrites eviction constraints and removes “flush inside REPLACE” pseudocode; replaces “flush-then-evict protocol” narrative with “REPLACE does no I/O; REQUEST misses drop mutex before fetch”.
- **why:**
  - Aligns the buffer pool with the coordinator-only WAL append rule: eviction should not become a backdoor WAL writer.

### `f708f33` (2026-02-07T17:01:38-05:00) — WAL-FEC pipelining semantics + write-set spill + concurrent RowId allocator + ARC cache boundary

**stats:** `+242 / -100` (impact `342`)

#### Group 1 — WAL-FEC Pipelining: Eventual Repairability, Optional Synchronous Mode
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 9
- **confidence:** 0.8
- **diff_notes:**
  - Clarifies SQLite WAL recovery behavior: corruption within durable history can truncate recovery at first invalid frame.
  - Defines pipelined `.wal-fec` generation as “eventual repairability”: commits may be durable but temporarily not FEC-protected; recovery falls back to SQLite semantics if `.wal-fec` isn’t durable.
  - Adds optional “synchronous `.wal-fec`” mode that waits for `.wal-fec` fsync before acknowledging commit.
- **why:**
  - Makes the durability contract precise: “durable” is not the same as “repairable”, and pipelining necessarily introduces a window.

#### Group 2 — Compatibility Commit Path: Coordinator-Only WAL Append + Spill Large Write Sets to Per-Txn Temp File
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 7
- **confidence:** 0.85
- **diff_notes:**
  - Replaces `CommitRequest.write_set: HashMap<...>` with `CommitWriteSet::{Inline,Spilled}` and specifies spill file semantics (last-write-wins index, per-page xxh3).
  - Adds `PRAGMA fsqlite.txn_write_set_mem_bytes` and an auto derivation rule: `clamp(4 * cache.max_bytes, 32 MiB, 512 MiB)`.
  - Establishes a critical invariant: WAL append is privileged and must be coordinator-only to preserve contiguity and correct wal-index state.
- **why:**
  - Prevents OOM on large transactions without turning eviction into an uncoordinated WAL writer.

#### Group 3 — Concurrent RowId Allocation: Snapshot-Independent Per-Table Allocator (Stable RowIds for Intent Replay)
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 2, 7
- **confidence:** 0.8
- **diff_notes:**
  - Adds §5.10.1.1: in `BEGIN CONCURRENT`, `OP_NewRowid` must allocate from a global per-table allocator shared across writers/processes; RowId must be recorded in intent at execution time and be stable (RETURNING/last_insert_rowid correctness).
  - Defines AUTOINCREMENT high-water persistence via a monotone `max` update expression on `sqlite_sequence`.
- **why:**
  - Without a global allocator, concurrent writers starting from the same snapshot collide on rowid and make deterministic rebase for insert intents fundamentally non-workable.

#### Group 4 — ARC Boundary Clarification: Transaction-Private Writes Are Not ARC Entries
- **primary_bucket:** 9 (Clarification)
- **buckets:** 9, 7, 4
- **confidence:** 0.6
- **diff_notes:**
  - Adds a normative note: uncommitted page images live in txn `write_set` (possibly spilled); `commit_seq=0` in ARC refers only to the on-disk baseline.
  - Removes `dirty`/`flush_inflight` from `CachedPage` in the ARC spec and rewords eviction constraints to be about “non-evictable” rather than “unflushable”.
- **why:**
  - Clarifies ownership boundaries so the cache spec doesn’t imply unsafe cross-cutting durability behavior.

### `a71e1d9` (2026-02-07T17:07:05-05:00) — crash-safe ECS root update + snapshot slot txn_id + ESCAPE parsing clarification

**stats:** `+178 / -105` (impact `283`)

#### Group 1 — ECS Root Update Protocol: fsync Temp + fsync Directory (Rename Alone Is Not Enough)
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 1
- **confidence:** 0.85
- **diff_notes:**
  - Defines a crash-safe update sequence for `ecs/root`: write temp, fsync temp, rename, fsync directory; explicitly calls out failure modes if you omit steps.
- **why:**
  - “Atomic rename” is not a durability barrier; without fsyncing both file and directory you can lose the update or persist garbage after power loss.

#### Group 2 — TxnSlot Cleanup: Snapshot `txn_id` Once Per Slot Iteration to Avoid Sentinel Races
- **primary_bucket:** 1 (Logic/Math Fixes)
- **buckets:** 1, 4
- **confidence:** 0.75
- **diff_notes:**
  - Introduces `tid = slot.txn_id.load(Acquire)` and branches on `tid` (including early-continue if `tid==0`), avoiding multiple unsynchronized reads.
- **why:**
  - Prevents a race where another cleaner transitions the slot into CLEANING between reads and the current cleaner incorrectly frees the slot while locks are still being released.

#### Group 3 — OTI/SymbolRecord Widths and Invariants (SQLite Page Size Corner)
- **primary_bucket:** 2 (SQLite Legacy Corrections)
- **buckets:** 2, 1, 7
- **confidence:** 0.7
- **diff_notes:**
  - Notes that SQLite allows 65,536-byte pages (encoded as `1` in header), requiring `OTI.T` be `u32` and enforcing `symbol_size == OTI.T`.
  - Clarifies FrankenSQLite OTI is not RFC 6330 Common FEC OTI wire format (internal widening is allowed/required).
- **why:**
  - Prevents a subtle “page size overflow” failure mode where valid SQLite page sizes cannot be represented in OTI metadata.

#### Group 4 — Parser Semantics: ESCAPE Is Not an Operator (Pratt Parsing Must Treat It as LIKE/GLOB Suffix)
- **primary_bucket:** 2 (SQLite Legacy Corrections)
- **buckets:** 2, 4, 9
- **confidence:** 0.65
- **diff_notes:**
  - Explains `parse.y` `%right ESCAPE` is a Lemon conflict-resolution artifact; ESCAPE is not a standalone operator. In Pratt parsing, parse ESCAPE as part of LIKE/GLOB handling.
- **why:**
  - Prevents parser architecture from mis-modeling SQLite grammar and producing incorrect parse trees for LIKE ... ESCAPE ...

### `4432a3d` (2026-02-07T16:25:52-05:00) — conformance mode matrix + I/O-fatal flush semantics

**stats:** `+119 / -26` (impact `145`)

#### Group 1 — Conformance Harness Anti-Drift: Every Fixture Declares Compat vs Native Modes
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 7
- **confidence:** 0.8
- **diff_notes:**
  - Adds a normative “mode matrix”: fixtures must run under both `compatibility` and `native` by default; single-mode tests require an explicit reason; CI must assert cross-mode parity when both run.
- **why:**
  - Prevents an inevitable drift where one commit engine silently diverges from the other (and from Oracle behavior) due to incomplete test coverage.

#### Group 2 — Flush Cancellation Masking Implies I/O-Fatal Failure Modes (Data Safety > Liveness)
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 6
- **confidence:** 0.7
- **diff_notes:**
  - Adds explicit guidance: if `wal.write_frame()` hangs, there’s no safe timeout; supervisors should apply backpressure and require operator intervention.
- **why:**
  - In a DB engine, uncertainty about durability outcome is worse than being stuck. The spec now makes that trade explicit.

### `4c07e10` (2026-02-07T16:41:55-05:00) — formatting + rowid reuse semantics + encryption AAD no-circularity

**stats:** `+78 / -57` (impact `135`)

#### Group 1 — Scrivening: Normalize Pseudocode/Doc Indentation for MVCC Structures
- **primary_bucket:** 5 (Scrivening)
- **buckets:** 5
- **confidence:** 0.8
- **diff_notes:**
  - Re-indents `VersionArena` / lock-table pseudocode and ARC comment blocks for readability and consistency.

#### Group 2 — Deterministic Rebase Semantics Under RowId Reuse (Not a “Corruption Bug”)
- **primary_bucket:** 2 (SQLite Legacy Corrections)
- **buckets:** 2, 4, 9
- **confidence:** 0.75
- **diff_notes:**
  - Clarifies rebase step 2 as “key not found” rather than “row deleted”.
  - Explicitly states: rowid reuse is allowed in SQLite unless AUTOINCREMENT; rebase operates on semantic key, so replay may update a later row that reused the same rowid.
- **why:**
  - Sets correct expectations: key-reuse means some “delete then reuse” races are not special-cased; the semantics are “as if re-executed at commit-time base”.

#### Group 3 — Reserved Space Checksums + Interop Wording Tightening
- **primary_bucket:** 2 (SQLite Legacy Corrections)
- **buckets:** 2, 9
- **confidence:** 0.55
- **diff_notes:**
  - Clarifies that C SQLite treats reserved bytes as opaque (can read dbs with reserved checksums); reframes default rationale as “interoperability”.

#### Group 4 — Encryption AAD Inputs Must Be Pre-Decrypt Known (No Circular Dependencies) + Stable DatabaseId
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 7, 1
- **confidence:** 0.8
- **diff_notes:**
  - Requires a stable random `DatabaseId` stored alongside wrapped DEK and stable across rekey.
  - AAD must include `(page_number, database_id)` and must not depend on encrypted page bytes (e.g., page type flag); optional context tags only if pre-decrypt known.
- **why:**
  - Prevents an “AAD circularity” design that cannot be implemented safely (you cannot authenticate/decrypt if your AAD requires decrypted content).

### `80decf6` (2026-02-07T17:28:31-05:00) — db-fec digest binding + RFC terminology + commit time monotonicity rule

**stats:** `+15 / -10` (impact `25`)

#### Group 1 — `.db-fec` Metadata Guardrails and RFC-Consistent Symbol Naming
- **primary_bucket:** 9 (Clarification)
- **buckets:** 9, 5, 1, 7
- **confidence:** 0.7
- **diff_notes:**
  - Specifies exact `db_gen_digest` inputs (header fields and offsets) to prevent stale-sidecar mistakes.
  - Renames repair symbol indexing from “ISI” to RFC 6330 “ESI” naming (and uses ESI from SymbolRecord).
  - Makes `commit_time_unix_ns` monotonicity enforcement explicit: `max(now, last+1)`.
- **why:**
  - Tightens “plumbing details” that are easy to get subtly wrong and later nearly impossible to debug (stale sidecars, symbol identity, ordering).

---

## Deep Review Subset B (2026-02-07 18:11–18:41 ET)

This window continues the cross-process hardening thread: SHM snapshot seqlock
(and its failure modes), coordinator IPC transport, rolling lock-table rebuild
without abort storms, and formalizing the TxnSlot acquire/publish protocol.

| # | Commit | Time (ISO) | + / - | Impact | Subject |
|---:|---|---|---:|---:|---|
| 1 | `7cc7263` | `2026-02-07T18:11:25-05:00` | `+781 / -148` | `929` | spec: harden SHM snapshot seqlock + compat db-fec freshness |
| 2 | `9ad50ae` | `2026-02-07T18:15:58-05:00` | `+160 / -23` | `183` | spec: define SHM snapshot seqlock + coordinator IPC |
| 3 | `19106d1` | `2026-02-07T18:20:51-05:00` | `+87 / -21` | `108` | spec: harden MVCC TxnSlot protocol, write_page idempotency, and SHM layout |
| 4 | `7313951` | `2026-02-07T18:21:25-05:00` | `+183 / -20` | `203` | spec: define wire payload schemas, RowId allocator state, and CommitRequest type |
| 5 | `d329df0` | `2026-02-07T18:28:22-05:00` | `+126 / -43` | `169` | spec: fix snapshot seqlock + shm invariants |
| 6 | `351c282` | `2026-02-07T18:41:22-05:00` | `+50 / -45` | `95` | spec: fix TxnSlot acquire pseudocode + add spec viz wasm |

### `7cc7263` (2026-02-07T18:11:25-05:00) — SHM seqlock + TxnSlot tagging + lock-table rolling rebuild + `.db-fec` freshness

**stats:** `+781 / -148` (impact `929`)

#### Group 1 — `.db-fec` Sidecar Freshness: Stale/Foreign Guard + Header-as-Commit-Record Discipline
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 1, 2, 4
- **confidence:** 0.85
- **diff_notes:**
  - Requires verifying `.db-fec` freshness before using any repair metadata: compute `db_gen_digest_current` from the `.db` header u32 fields at offsets 24/28/36/40 and require it matches `DbFecHeader.db_gen_digest` (after verifying `.db-fec` header checksum).
  - If page 1 is corrupted and repair is attempted, the digest must be recomputed from *repaired* bytes and still match `DbFecHeader.db_gen_digest`; otherwise fail closed (`SQLITE_CORRUPT`) rather than “repairing” to a foreign state.
  - Specifies a *global generation commit record* for `.db-fec`: checkpoint must durable-write `.db`, then write `DbFecHeader.db_gen_digest`, then write header checksum, then `fsync` `.db-fec`; WAL `RESTART/TRUNCATE` must not happen until this header fsync completes.
  - Adds a **single-writer checkpoint rule** for `.db` + `.db-fec` updates in Compatibility mode (cross-process mutual exclusion).
- **why:**
  - Prevents the highest-severity failure mode: “successful” repair to a stale/foreign database generation, which would be silent data loss/corruption masquerading as recovery.

#### Group 2 — SHM Snapshot Seqlock + Serialized Writer Indicator (Cross-Process Correctness Backbone)
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 7, 1
- **confidence:** 0.8
- **diff_notes:**
  - Adds `snapshot_seq: AtomicU64` to SHM and defines an odd/even seqlock protocol around publishing `(commit_seq, schema_epoch, ecs_epoch)`.
  - Adds `serialized_writer_token/pid/pid_birth/lease_expiry` indicator fields (Release-publish on token) and requires Concurrent writers to check the indicator before acquiring page locks.
  - Defines a `check_serialized_writer_exclusion()` algorithm that uses lease expiry + PID liveness and best-effort clearing of stale indicators.
- **why:**
  - Makes “snapshot capture” and “serialized writer exclusion” *explicit cross-process protocols* with failure handling, instead of an implicit assumption baked into prose.

#### Group 3 — TxnSlot `txn_id` Tagged Encoding: ABA-Resistant Claim/Cleaning States
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 1, 7
- **confidence:** 0.85
- **diff_notes:**
  - Redefines `TxnSlot.txn_id` as a **tagged atomic state word** (top 2 bits as tag): Free / Active / CLAIMING / CLEANING.
  - Phase 1 claim uses `claim_word = encode_claiming(real_txn_id)` (not a constant sentinel); Phase 3 publish uses CAS `claim_word -> real_txn_id`.
  - Cleanup paths branch on `decode_tag(tid)` and use `encode_cleaning(payload)` so only the correct claimer can publish and cleanup is retryable.
  - `raise_gc_horizon()` and witness-epoch advancement treat *any* sentinel-tagged slot as a horizon blocker (CLAIMING can already have pinned snapshot fields).
- **why:**
  - Fixes a real multi-process correctness bug class: constant sentinels permit “stalled claimer steals later claim” ABA races after crash cleanup.

#### Group 4 — Cross-Process “Recently Committed Readers” Ring: Fixed SHM Layout + Bloom Summary (Fail Closed)
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 4
- **confidence:** 0.78
- **diff_notes:**
  - Replaces unstable “RoaringBitmap in SHM” with a fixed-layout ring buffer at `SharedMemoryLayout.committed_readers_offset`.
  - Uses a 4096-bit Bloom filter (`k=3`) per entry to summarize read pages (false positives allowed; false negatives forbidden unless the committer aborts under overflow policy).
  - Defines a publish protocol using `commit_seq` as the entry publication word, plus a hard fail-closed rule: if insertion would evict an entry with `commit_seq > gc_horizon`, the commit aborts with `SQLITE_BUSY_SNAPSHOT`.
- **why:**
  - Makes the cross-process SSI bookkeeping implementable and ABI-stable while staying memory-bounded.

#### Group 5 — Rolling Lock-Table Rebuild (Rotate + Drain + Clear) to Avoid Abort Storms
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 7
- **confidence:** 0.8
- **diff_notes:**
  - Redesigns `SharedPageLockTable` into two instances (active + draining) with `active_table` / `draining_table` selectors.
  - `try_acquire` consults the draining table first to preserve correctness for locks held pre-rotation; `release` and crash cleanup scan both tables.
  - Rebuild protocol becomes rolling: rotate quickly, drain in background, clear drained table at quiescence; avoids stop-the-world “force everyone to abort” behavior.
- **why:**
  - “Freeze acquisitions and abort lock-holders” is a deterministic write-unavailability failure mode at scale; rotation avoids that while preserving correctness.

#### Group 6 — In-Process GC Must Be Incremental and Touched-Page Driven (No Stop-the-World Arena Scans)
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 4
- **confidence:** 0.75
- **diff_notes:**
  - Adds §5.6.5.1: per-process `GcTodo` queue; enqueue on “publish/materialize committed version”; prune only touched pages under strict work budgets (`pages_budget=64`, `versions_budget=4096`).
  - Forbids GC designs that scan the whole `VersionArena` under a write guard, to preserve the WAL property “writers do not block readers for long intervals”.
  - Defines ARC interaction + “no I/O in prune” rule.
- **why:**
  - Prevents long pauses and memory leaks while keeping MVCC reclaiming aligned with real workload touch patterns.

#### Group 7 — Compatibility WAL Reader Marks: Join Fast Path + Correct Lock Discipline (Plus `BEGIN CONCURRENT` Hard-Fail)
- **primary_bucket:** 2 (SQLite Legacy Corrections)
- **buckets:** 2, 4, 7
- **confidence:** 0.7
- **diff_notes:**
  - Clarifies SQLite WAL read-mark discipline: readers either join an existing `aReadMark[i]==m` by acquiring `WAL_READ_LOCK(i)` in SHARED (fast path), or claim+update by taking EXCLUSIVE then downgrading to SHARED for snapshot lifetime.
  - Makes the “5 read marks” limitation explicit as a bound on **distinct** snapshots, not on total readers (many readers can share a mark).
  - Adds normative rule: if `foo.db.fsqlite-shm` is unavailable, `BEGIN CONCURRENT` must error (no silent downgrade to Serialized).
- **why:**
  - Fixes a subtle but crucial interop contract: the read locks, not just `aReadMark[]` values, are what legacy checkpointers consult.

### `9ad50ae` (2026-02-07T18:15:58-05:00) — coordinator IPC transport + seqlock reader algorithm + spill-fd semantics

**stats:** `+160 / -23` (impact `183`)

#### Group 1 — Coordinator IPC Transport (Unix Domain Sockets + Framing + Two-Phase Reserve/Submit)
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 7, 6
- **confidence:** 0.82
- **diff_notes:**
  - Specifies Unix socket endpoints per DB, strict permissions, and mandatory peer UID checks.
  - Defines length-delimited framing (`len_be`, `version_be`, `kind_be`, `request_id`) and two-phase `RESERVE` → `SUBMIT_*` discipline (bounded outstanding permits).
  - Requires idempotency keyed by `(txn_id, txn_epoch)` so “disconnect after submit” yields the same terminal decision on retry.
  - Requires large Compatibility/WAL payload transfer via SCM_RIGHTS spill fd passing (no inline page bytes).
- **why:**
  - Avoids a variable-sized shared-memory queue inside a no-unsafe workspace while preserving backpressure, cancel-safety, and cross-process robustness.

#### Group 2 — `load_consistent_snapshot` Uses Seqlock Reads (No Mixed `(high, schema_epoch)` Pairs)
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 1, 7
- **confidence:** 0.78
- **diff_notes:**
  - Snapshot capture loops on `snapshot_seq`: read `s1`, retry if odd; read `commit_seq` + `schema_epoch`; read `s2`; accept only if `s1==s2` and even.
- **why:**
  - Prevents BEGIN-time mixed snapshots around DDL publication; aligns the spec with the seqlock design rather than “two loads should be enough”.

#### Group 3 — Multi-Process Spill Write-Set Semantics: Use `OwnedFd` (+ Optional Path) and Unlink-for-Robustness
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 4, 6
- **confidence:** 0.75
- **diff_notes:**
  - Adds normative notes: in multi-process mode, the Rust in-process structs are *schemas only*; heap objects and channels must not cross processes.
  - Switches spilled write-set from `spill_path` to `spill_fd: OwnedFd` plus optional diagnostics `spill_path`, and recommends unlinking after opening so cleanup is automatic.
  - Makes cross-process commits require `CommitWriteSet::Spilled` and SCM_RIGHTS fd passing.
- **why:**
  - Forces a correct, scalable transport for large payloads while making crash cleanup easier and avoiding TOCTOU/path races.

### `19106d1` (2026-02-07T18:20:51-05:00) — TxnSlot acquire/publish protocol + write_page idempotency + SHM layout safety

**stats:** `+87 / -21` (impact `108`)

#### Group 1 — `acquire_and_publish_txn_slot`: Make the 3-Phase Protocol Explicit and Snapshot-Self-Consistent
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 7
- **confidence:** 0.82
- **diff_notes:**
  - Introduces a wrapper pseudocode function: claim via CAS, initialize all slot fields (SSI flags, witness_epoch, lease, pid/pid_birth), capture snapshot via `load_consistent_snapshot()`, then publish real `txn_id` via CAS claim→real, then clear `claiming_timestamp`.
- **why:**
  - Captures the “real protocol” in one place so future implementation can’t silently omit critical steps (horizon safety + self-consistent snapshot fields).

#### Group 2 — `write_page` Cross-Process Hint Must Be Idempotent per (txn,page)
- **primary_bucket:** 1 (Logic/Math Fixes)
- **buckets:** 1, 7
- **confidence:** 0.8
- **diff_notes:**
  - Tracks `newly_locked` and guards `write_set_pages.fetch_add(1)` behind it; clarifies `write_set_pages` is a hint/metric, not the correctness source of truth (lock tables are).
- **why:**
  - Prevents inflated counts from repeated writes to the same page, which could otherwise poison coordination heuristics (and worst case, correctness checks if misused).

#### Group 3 — SHM Layout Hardening: Immutable-Metadata Checksum + “No Unsafe Reinterpret Casts” Rule
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 7, 6
- **confidence:** 0.78
- **diff_notes:**
  - Renames `checksum` to `layout_checksum` and clarifies it covers only immutable layout metadata (not dynamic atomics).
  - Adds a normative constraint: because workspace forbids `unsafe`, SHM access must use safe offset-based typed accessors (any `unsafe` lives in external abstraction), not `&[u8] -> &SharedMemoryLayout` casts.
  - Clarifies that DDL publication correctness relies on seqlock windows, not “store ordering alone”.
- **why:**
  - Makes the spec implementable within a safe-Rust-only repo and removes an easy-to-miss “layout checksum includes dynamic state” design mistake.

#### Group 4 — Coordinator Liveness During Rebuild: Don’t Block Commit Publication on Quiescence
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 4
- **confidence:** 0.72
- **diff_notes:**
  - Adds a rule forbidding tight “wait until all owner_txn==0” loops on the commit critical path; rebuilding is background maintenance.
- **why:**
  - Preserves throughput/latency under maintenance and avoids turning a housekeeping task into a global availability cliff.

### `7313951` (2026-02-07T18:21:25-05:00) — wire payload schemas + RowId allocator state + framing math fix

**stats:** `+183 / -20` (impact `203`)

#### Group 1 — Framing Fix: Payload Length Is `len_be - 12` (Not `- 16`)
- **primary_bucket:** 1 (Logic/Math Fixes)
- **buckets:** 1, 5
- **confidence:** 0.9
- **diff_notes:**
  - Corrects `Frame.payload` length to subtract only `(version_be + kind_be + request_id)` (12 bytes), not also the length field itself.
- **why:**
  - Fixes a wire-format math error that would have produced systematic framing/parsing breakage.

#### Group 2 — V1 Wire Payload Schemas + Size Caps + Exact FD-Passing Rules
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 7, 6
- **confidence:** 0.8
- **diff_notes:**
  - Defines canonical payload encodings for `RESERVE`, `SUBMIT_NATIVE_PUBLISH`, `SUBMIT_WAL_COMMIT`, and `ROWID_RESERVE` (including `SpillPageV1` fields and `xxh3_64` checksum).
  - Requires `SUBMIT_WAL_COMMIT` to carry **exactly one** SCM_RIGHTS fd for the spill file; missing/extra fds must be rejected.
  - Adds explicit caps: `write_set_summary <= 1MiB`, witness/edge counts bounded, frame cap 4MiB.
- **why:**
  - Makes cross-process coordinator IPC deterministic, bounded, and implementable without “Rust-struct-through-SHM” traps.

#### Group 3 — RowId Allocator State: Coordinator-Owned Map + `ROWID_RESERVE` IPC Semantics
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 7, 6
- **confidence:** 0.78
- **diff_notes:**
  - Defines allocator state location as coordinator-owned in-memory map keyed by `(schema_epoch, TableId)`, served cross-process by `ROWID_RESERVE`.
  - Initialization uses durable tip (`max_committed_rowid + 1`, with AUTOINCREMENT override); schema_epoch mismatch yields `SQLITE_SCHEMA`; ranges monotone/non-reusable; gaps permitted.
- **why:**
  - Resolves the “where do the counters live?” problem without requiring a dynamic shared-memory hash table.

#### Group 4 — Seqlock Writer Protocol Hardening: Don’t Flip Odd→Even Until Backbone Fields Are Reconciled
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 1, 7
- **confidence:** 0.7
- **diff_notes:**
  - Adds a normative rule: flipping `snapshot_seq` odd→even is forbidden unless the backbone fields were written as a self-consistent set derived from durable state.
  - If `snapshot_seq` is already odd (crash-stale), coordinator must treat that as an open publish window and complete reconciliation before ending publish.
- **why:**
  - Prevents readers from accepting a mixed snapshot under an even seqlock word after a crash mid-publication.

### `d329df0` (2026-02-07T18:28:22-05:00) — serialized writer acquisition pseudocode + wire response schemas + canonical write-set summary

**stats:** `+126 / -43` (impact `169`)

#### Group 1 — Define `acquire_serialized_writer_exclusion` / `release_serialized_writer_exclusion` Ordering and Drain Semantics
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 7
- **confidence:** 0.78
- **diff_notes:**
  - Adds explicit pseudocode: acquire the mode’s global exclusion, publish the shared indicator (token + pid + lease), drain concurrent writers via lock-table scan (with orphan cleanup while draining).
  - Release clears the indicator before releasing the global exclusion, preventing an interlock window.
- **why:**
  - Turns an implicit invariant into an explicit protocol with ordering guarantees.

#### Group 2 — SHM Layout Checksum Now Has a Canonical Definition + Mandatory Verification on Map
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 4, 1
- **confidence:** 0.75
- **diff_notes:**
  - Defines `layout_checksum = xxh3_64(immutable_layout_metadata_bytes)` encoded canonically in little-endian and explicitly excluding dynamic atomics; mismatch must reject SHM as incompatible/corrupt.
- **why:**
  - Prevents “layout checksum includes dynamic fields” ambiguity and provides a real compatibility guardrail.

#### Group 3 — `write_set_summary` Encoding Is Canonical Sorted `u32_le[]` (Not Roaring) + Add Response Payload Schemas
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 7, 1, 6
- **confidence:** 0.74
- **diff_notes:**
  - Replaces “canonical RoaringBitmap serialization” with a strict `u32_le[]` sorted unique encoding; length must be multiple of 4.
  - Adds normative response payload schemas for `SUBMIT_NATIVE_PUBLISH` and `SUBMIT_WAL_COMMIT` (Ok/Conflict/Err variants).
- **why:**
  - Stabilizes cross-process ABI and makes tooling/interop easier (no implicit dependency on a specific roaring serialization format).

#### Group 4 — Spill Handle + Spill Page Hash Semantics Are Made Explicit (`xxh3_64`, Not Crypto)
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 4
- **confidence:** 0.7
- **diff_notes:**
  - Introduces `SpillHandle::{Path,Fd}` and clarifies `SpillLoc.xxh3_64` as a fast corruption detector, not a cryptographic hash.
- **why:**
  - Avoids accidental misuse of checksums as authentication and makes the multi-process fd-passing story explicit.

#### Group 5 — Expression Precedence Duplication Removed; Normative Rules Centralized
- **primary_bucket:** 2 (SQLite Legacy Corrections)
- **buckets:** 2, 5, 9
- **confidence:** 0.7
- **diff_notes:**
  - Removes a duplicate precedence table and points to the Pratt precedence table as normative; reiterates key rules (`NOT x=y` parsing, `ESCAPE` not operator, unary vs COLLATE).
- **why:**
  - Prevents the spec from being self-inconsistent about one of the easiest-to-botch parser semantics.

### `351c282` (2026-02-07T18:41:22-05:00) — TxnSlot acquire pseudocode race fix + page-aligned buffers under no-unsafe constraint

**stats:** `+50 / -45` (impact `95`)

#### Group 1 — “No Unsafe” Implementation Constraint Applied to Page-Aligned Allocation (PageBuf Is Aligned)
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 4, 6
- **confidence:** 0.75
- **diff_notes:**
  - Clarifies aligned allocation must be supplied via safe abstractions because workspace forbids `unsafe`.
  - Reasserts `PageBuf` as page-sized, page-aligned buffer handle (alignment is required; allocator provides it).
- **why:**
  - Forces the spec to stay implementable inside this repo and keeps direct-I/O-friendly buffer invariants explicit.

#### Group 2 — TxnSlot Acquire: Detect Lost Claim Before Stamping `claiming_timestamp`; Fix Indentation/Clarity
- **primary_bucket:** 1 (Logic/Math Fixes)
- **buckets:** 1, 4, 5
- **confidence:** 0.7
- **diff_notes:**
  - After Phase 1 CAS, re-load `slot.txn_id` and require it still equals `claim_word` before writing `claiming_timestamp` (avoid races where cleanup reclaimed the slot).
  - Fixes indentation and clarifies omitted transaction fields “initialize empty/false”.
- **why:**
  - Makes the acquire pseudocode faithful to the intended correctness: don’t write sentinel timestamps for a claim you no longer own.

---

## Deep Review Subset C (2026-02-07 18:58–19:16 ET)

This window is a tight continuation of the coordinator-IPC + SHM-liveness thread:
canonical wire framing/response tagging, permit binding, and the correctness
critical rule “never reclaim a live TAG_CLAIMING claimer”.

| # | Commit | Time (ISO) | + / - | Impact | Subject |
|---:|---|---|---:|---:|---|
| 1 | `b1c1e72` | `2026-02-07T18:58:48-05:00` | `+56 / -11` | `67` | spec: tighten coordinator IPC framing |
| 2 | `e600497` | `2026-02-07T19:15:51-05:00` | `+81 / -25` | `106` | spec: harden claiming liveness and IPC ordering |
| 3 | `6d5d36a` | `2026-02-07T19:16:12-05:00` | `+1 / -1` | `2` | spec: update Round 16 audit notes |

### `b1c1e72` (2026-02-07T18:58:48-05:00) — coordinator IPC framing hardening + canonical response tags + TxnId alloc pseudocode fix

**stats:** `+56 / -11` (impact `67`)

#### Group 1 — Coordinator IPC: Reject Bad Frames Early + Bind Permits + Canonical Tagged Responses
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 7, 1
- **confidence:** 0.78
- **diff_notes:**
  - Adds explicit framing validity rules: `len_be` in `[12, 4 MiB]`, `version_be==1`, unknown kinds rejected; enumerates `kind_be` values (RESERVE/SUBMIT_*/RESPONSE/PING/PONG).
  - Makes `permit_id` a connection-scoped, single-use capability: SUBMIT must reference a prior RESERVE on the same connection; unknown/reused permits rejected.
  - Makes response payloads fully canonical with explicit `(tag + padding + body)` wrappers for ReserveResp/NativePublishResp/WalCommitResp/RowIdReserveResp.
- **why:**
  - Eliminates “undefined behavior” surface area in the IPC codec and prevents cross-connection permit confusion that would otherwise become a reliability/security footgun.

#### Group 2 — Fix TxnId Allocation Pseudocode: `next_txn_id` Lives in Shared Memory
- **primary_bucket:** 1 (Logic/Math Fixes)
- **buckets:** 1, 5
- **confidence:** 0.9
- **diff_notes:**
  - Corrects `begin()` pseudocode to read/modify `manager.shm.next_txn_id`, not `manager.next_txn_id`.
- **why:**
  - Avoids a spec/implementation divergence where the pseudocode implies a per-process counter (which would violate cross-process uniqueness).

#### Group 3 — Scrivening: Document Version Bump to 1.32 (Round 15 Audit Summary)
- **primary_bucket:** 5 (Scrivening)
- **buckets:** 5, 9
- **confidence:** 0.8
- **diff_notes:**
  - Updates the footer audit note to include Round 15 framing/kind/permit binding and canonical tagging changes.

### `e600497` (2026-02-07T19:15:51-05:00) — TAG_CLAIMING liveness safety + stale serialized-writer indicator retry loop + canonical set ordering

**stats:** `+81 / -25` (impact `106`)

#### Group 1 — TAG_CLAIMING Liveness: Publish PID Identity Before Any Potentially-Blocking Step (and Never Reclaim Live Claimers)
- **primary_bucket:** 4 (Architecture Fixes)
- **buckets:** 4, 1, 7
- **confidence:** 0.85
- **diff_notes:**
  - Requires writing `pid/pid_birth/lease_expiry` immediately after Phase 1 claim and **before** snapshot capture (seqlock spin is “potentially blocking”).
  - Tightens cleanup_orphaned_slots: if CLAIMING and pid/birth are published and `process_alive(pid,birth)`, it MUST NOT reclaim; introduces a more conservative timeout when pid/birth are still 0.
  - Tightens freeing discipline: clear `commit_seq` and liveness fields (`pid/pid_birth/lease_expiry`) before publishing `txn_id=0`.
- **why:**
  - This is a correctness-critical cross-process safety rule: reclaiming an alive claimer permits “resumed-claimer shared-memory scribbles” after the slot is freed and re-claimed.

#### Group 2 — `check_serialized_writer_exclusion`: Retry on CAS Failure to Avoid Returning Ok During Token Turnover
- **primary_bucket:** 1 (Logic/Math Fixes)
- **buckets:** 1, 4, 7
- **confidence:** 0.8
- **diff_notes:**
  - Wraps stale-token clearing in a loop: if CAS(tok->0) fails, retry because either another checker cleared it or a new serialized writer installed a fresh token.
- **why:**
  - Prevents a narrow but real race: a concurrent writer must not return Ok in the same window a new serialized writer becomes active.

#### Group 3 — Canonical Ordering Rules for Set-Like Fields in IPC Payloads
- **primary_bucket:** 7 (Standard Engineering)
- **buckets:** 7, 4
- **confidence:** 0.75
- **diff_notes:**
  - Requires ObjectId arrays (witness refs/edge refs/merge refs) sorted lexicographically and deduped; requires conflict page arrays sorted+deduped; requires spill_pages sorted by pgno with no duplicates.
- **why:**
  - Canonical ordering shrinks the state space for testing, improves reproducibility, and prevents “same meaning, different bytes” bugs in deterministic codecs.

#### Group 4 — Scrivening: Footer Audit Note to Round 16 + Last Updated Date to 2026-02-08
- **primary_bucket:** 5 (Scrivening)
- **buckets:** 5, 9
- **confidence:** 0.75
- **diff_notes:**
  - Updates doc version to 1.33 with Round 16 audit notes; advances *Last updated* to `2026-02-08`.

### `6d5d36a` (2026-02-07T19:16:12-05:00) — Round 16 audit note wording fix

**stats:** `+1 / -1` (impact `2`)

#### Group 1 — Scrivening: Audit Note Expanded to Include TAG_CLAIMING Liveness Rule
- **primary_bucket:** 5 (Scrivening)
- **buckets:** 5, 9
- **confidence:** 0.9
- **diff_notes:**
  - Updates the Round 16 audit note to explicitly mention early pid/birth publication + “don’t reclaim live claimers” as part of the round’s scope.

---

## TODO (Next Deep-Review Targets)

- Continue deep-review for the remaining commits in Subset A not covered above:
  - `6b0c12f`, `b181b6d`, `0177456`, `5dae90d`, `ca60e00`, `30203fb`, `ec9adc1`, `e80fdde`, `1d8bbfb`
  - `3d56854`, `df0313b`, `97df1f0`, `bbc4a31`, `4363f50`, `d9021cf`, `120eee2`, `975f65c`, `24b6f60`
- Subset B (18:11–18:41 ET hardening thread: SHM seqlock + coordinator IPC + rolling rebuild) is now covered above.
- Next: pick Subset C by impact (top commits outside Subset A/B across the full history) and repeat the same deep-review structure.
