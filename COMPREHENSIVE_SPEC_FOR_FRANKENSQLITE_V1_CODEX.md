# COMPREHENSIVE SPEC FOR FRANKENSQLITE V1 (CODEX)

**Document status:** Draft v1 (single-source-of-truth for agents)  
**Repo:** `/data/projects/frankensqlite`  
**Target oracle:** SQLite 3.52.0 (C reference in `legacy_sqlite_code/`)  
**RaptorQ bible:** `docs/rfc6330.txt` (RFC 6330)

This document is intentionally **self-contained**: it includes the goals, constraints, architecture, formal semantics, verification strategy, and execution plan. It is written to be handed to other agents without requiring them to read any other docs first.

If any other project docs disagree with this file, treat this file as authoritative until updated.

---

## Table of Contents

0. What We Are Building (One Paragraph)
0.1 Glossary + Normative Language (MUST/SHOULD/MAY)
1. Non-Negotiables (Hard Constraints)
2. Success Definition (What “Parity” Means)
3. The Big Idea: “Erasure-Coded Streams” Everywhere
4. Operating Modes (Compatibility vs Native)
5. Core Data Model (Formal-ish, Testable)
6. ECS Storage Substrate (Objects, Symbols, Physical Layout)
7. Concurrency: MVCC + SSI (Serializable by Default)
8. Algebraic Write Merging (Conflict Reduction Without Row MVCC)
9. Durability: Coded Commit Stream (Protocol + Recovery + Compaction)
10. The Radical Index: RaptorQ-Coded Index Segments (Lookup, Repair, Rebuild)
11. Caching & Acceleration (ARC, Bloom/Quotient Filters, Hot Paths)
12. Replication: Fountain-Coded, Loss-Tolerant, Late-Join Friendly
13. Asupersync Integration Contract (Cx, LabRuntime, DPOR, TLA+ Export)
14. Conformance Harness (The Oracle Is The Judge)
15. Performance Discipline (Extreme Optimization)
16. Implementation Plan (V1 Phase Gates)
17. Risk Register + Open Questions
18. Local References (Canon)

---

## 0. What We Are Building (One Paragraph)

**FrankenSQLite** is a clean-room, safe-Rust reimplementation of SQLite that keeps **SQLite’s SQL semantics and public API** (as proven by a conformance harness against C SQLite 3.52.0), while replacing SQLite’s single-writer bottleneck with **concurrent writers** and upgrading durability + replication by making **RaptorQ erasure coding** the universal substrate for persistence, recovery, and synchronization. The guiding idea is that the database is not “a file with a WAL”, it is an **information-theoretically optimal, self-healing, erasure-coded stream of commits** that can be stored locally, repaired after crashes, and replicated over lossy networks without fragile reliable-transport assumptions.

---

## 0.1 Glossary + Normative Language

### Normative Language

We use RFC-style terms:

- **MUST**: required for correctness, conformance, or project constraints.
- **SHOULD**: strong default; deviations require justification and tests.
- **MAY**: optional enhancement or later-phase refinement.

### Canonical Terms

- **Oracle**: the authoritative reference implementation (C SQLite 3.52.0 built from `legacy_sqlite_code/`).
- **Conformance harness**: the machinery that runs the same input against Oracle + FrankenSQLite and compares outputs.
- **ECS (Erasure-Coded Stream)**: our universal persistence/replication substrate: a stream of **objects**, each encoded into **RaptorQ symbols**.
- **Object**: a byte payload + canonical header, encoded as RaptorQ symbols. Examples: `CommitCapsule`, `CommitMarker`, `IndexSegment`, `SnapshotManifest`, `PageHistory`.
- **OTI**: Object Transmission Information (RFC 6330), the parameters required to decode.
- **Symbol**: an encoding symbol produced by the RaptorQ encoder (source or repair). The unit of storage and replication.
- **Symbol store**: a component that can persist and later serve symbols (local disk, remote replicas, simulated lab transport).
- **Commit capsule**: the primary commit payload (what changed + evidence).
- **Commit marker**: the atomic “this commit exists” record; if a marker is durable, the commit is considered durable.
- **Index segment**: a (coded) object that accelerates lookups (page → latest patch pointer, object locator maps, filters).
- **Compatibility view**: a materialized SQLite-compatible `.db`/`.wal` view produced from ECS state for tooling/oracle parity.
- **Native mode**: ECS is the source-of-truth; compatibility views are derived artifacts.

### What “RaptorQ Everywhere” Means (No Weasel Words)

RaptorQ is not an “optional replication feature”. It is the default substrate for:

- durability objects (commit capsules, markers, checkpoints)
- indexing objects (index segments, locator segments)
- replication traffic (symbols, not files)
- repair (recover from partial loss/corruption by decoding, not by panicking)
- compression of history (patch chains stored as coded objects, not infinite full-page copies)

If a subsystem persists or synchronizes bytes, it MUST specify how those bytes are represented as ECS objects and how they are repaired/replicated.

---

## 1. Non-Negotiables (Hard Constraints)

### 1.1 Engineering / Process Constraints

These are hard project constraints for all work:

- **User is in charge.** If the user overrides anything, follow the user.
- **No file deletion** without explicit written permission.
- **No destructive commands** unless the user explicitly provides the exact command and confirms they want the irreversible consequences. This includes but is not limited to `git reset --hard`, `git clean -fd`, and `rm -rf`.
- **Branch:** `main` only (never reference `master` in docs or code).
- **Rust toolchain:** nightly, edition 2024.
- **No `unsafe`** anywhere: workspace lints forbid unsafe code.
- **Clippy:** `pedantic` + `nursery` denied at workspace level; warnings are treated as errors.
- **No script-based code transformations.** Manual edits only.
- **No file proliferation** unless genuinely necessary (this spec file exists only because the user explicitly requested a single canonical spec).
- **After substantive code changes:** run `cargo check --all-targets`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`, and tests.
- **Use `br`** for tasks/issues and dependencies (Beads). Use `bv --robot-*` for triage. (This spec includes a plan so we don’t have to read beads to understand direction, but beads remain the execution substrate.)

### 1.2 Library / Dependency Constraints

- **All async/network/I/O patterns must use** `/dp/asupersync`.  
  - No Tokio. No “I’ll just use async-std”. No bespoke runtimes.
- **All console/terminal rendering must use** `/dp/frankentui`.
- **“RaptorQ-ness” must permeate the whole design.** RaptorQ is not a side feature; it is the organizing principle.

### 1.3 Workspace Structure (What Exists In This Repo)

FrankenSQLite is a Cargo workspace with crates under `crates/`. The intent is strict layering (SQL layer cannot reach into VFS; parser cannot grab pager internals, etc.).

Core crates (high-level roles):

- `crates/fsqlite-types`: foundational types and constants (page ids, record encoding, opcodes, flags).
- `crates/fsqlite-error`: unified error types and SQLite-ish error code mapping.
- `crates/fsqlite-vfs`: VFS traits + implementations (MemoryVfs, UnixVfs). All OS I/O must go through here.
- `crates/fsqlite-pager`: pager + cache policy plumbing + page buffer lifecycle.
- `crates/fsqlite-wal`: durability substrate (compat-WAL and/or native ECS-backed persistence hooks).
- `crates/fsqlite-mvcc`: MVCC + SSI + commit capsule formation + conflict reduction machinery.
- `crates/fsqlite-btree`: B-tree engine (spec-driven).
- `crates/fsqlite-ast`: typed AST nodes.
- `crates/fsqlite-parser`: tokenizer + parser.
- `crates/fsqlite-planner`: planner / optimizer.
- `crates/fsqlite-vdbe`: bytecode VM + opcode semantics.
- `crates/fsqlite-func`: built-in functions (scalar/aggregate/window).
- `crates/fsqlite-ext-*`: extensions (FTS, JSON1, RTree, Session, ICU, misc) as compile-time features.
- `crates/fsqlite-core`: wires everything into an engine.
- `crates/fsqlite`: the public API facade.
- `crates/fsqlite-cli`: CLI shell binary (must use frankentui).
- `crates/fsqlite-harness`: conformance runner and oracle comparison.

This spec defines the “true end-state” architecture for these crates, even if many are currently stubs.

---

## 2. Success Definition (What “Parity” Means)

We are not aiming for “close enough”. We want:

### 2.1 Behavioral Parity

- **SQL semantics parity** with C SQLite 3.52.0 for the chosen surface: query results, NULL semantics, type conversions, constraint behavior, PRAGMAs, error codes (normalized where needed).
- **Public API parity**: connection lifecycle, prepared statements, transactions/savepoints, extension surface (as scoped), pragmas, and error mapping.

### 2.2 Concurrency Semantics (Important Caveat)

SQLite’s single-writer design makes many anomaly classes impossible “by construction”. If we add concurrency, we must not silently weaken correctness.

Therefore:

- **Default isolation target:** **Serializable** behavior for concurrent writers (not just Snapshot Isolation).  
  - We achieve this by implementing **Serializable Snapshot Isolation (SSI)** with page-granularity tracking and aborts on dangerous structures.
- We may expose explicit modes for compatibility or experimentation, but **defaults must not silently introduce write skew**.

### 2.3 Durability + Crash Recovery

We must define a crash model and then meet it. Additionally, with RaptorQ we explicitly target **stronger-than-SQLite** resilience against corruption/erasures:

- SQLite-style fsync ordering still exists as a baseline guarantee.
- But we add **erasure-coded repair** so that bounded corruption/torn writes can be corrected rather than merely detected.

### 2.4 Performance

We will not “optimize later”. Performance is tracked from day one:

- Baselines first (criterion microbenches + macrobenches).
- Profile-driven changes only.
- Correctness proven by oracle conformance for every optimization change.

---

## 3. The Big Idea: “Erasure-Coded Streams” Everywhere

SQLite’s classic architecture:

```
DB file + WAL file + WAL index + checkpoints
```

FrankenSQLite’s architecture is reorganized around a single universal abstraction:

> **Erasure-Coded Stream (ECS):** an append-only stream of *objects*, where each object is encoded as RaptorQ symbols such that any `K` of the transmitted/stored symbols suffice to reconstruct the object. Objects can be stored locally, distributed across replicas, repaired after partial loss, and replayed to reconstruct state.

This is the database engine reframed as information theory:

- **Durability** becomes: “did we persist enough symbols for the commit object under the declared loss model?”
- **Replication** becomes: “can replicas collect enough symbols for each commit object?”
- **Recovery** becomes: “if some symbols are corrupt/missing, can we decode the object anyway?”
- **Indexing** becomes: “can we represent index state as encoded objects too, so indices are self-healing and replication-native?”

This is “RaptorQ in every pore”: not only for snapshot shipping, but for WAL, WAL index, page versions, and even conflict reduction via algebraic delta merging.

---

## 3.1 RaptorQ Primer (RFC 6330 Terms We Use)

RaptorQ (RFC 6330) is a systematic fountain code for object delivery.

We use the following conceptual mapping:

- **Object** (RFC): the byte string we want to deliver/durably store/replicate.
  - In FrankenSQLite: a commit capsule, an index segment, a snapshot manifest chunk, etc.
- **Source symbols**: the `K` “original” blocks (systematic encoding) derived from the object at a chosen symbol size.
- **Repair symbols**: additional encoded blocks that provide redundancy.
- **Decoding**: reconstruct the original object once enough symbols are available.
- **OTI (Object Transmission Information)**: the parameters required to interpret symbol ids and decode correctly.
  - In FrankenSQLite: this must be present in the ECS object header, and is treated as *part of the object’s canonical identity*.

We treat RFC 6330 as normative. In practice:

- The canonical reference text is vendored at `docs/rfc6330.txt`.
- `/dp/asupersync/src/raptorq/` provides an RFC 6330-grade implementation and is the only RaptorQ implementation we use.

Implementation note (practical API surface we will bind to):

- `asupersync::config::RaptorQConfig` for validated parameterization
- `asupersync::raptorq::RaptorQSenderBuilder` / `RaptorQReceiverBuilder` for pipeline construction

**Why a primer belongs here:** Every agent implementing any durable or replicated path must understand the meaning of `K`, symbol sizing, overhead, and OTI. RaptorQ is not “a compression trick”; it is the algebra of our durability and replication contracts.

---

## 4. Two Operating Modes (So We Can Prove Parity While Inventing The Future)

We need to be bold without losing the ability to verify parity. We therefore define two modes:

### 4.1 Compatibility Mode (Oracle-Friendly)

Purpose: quickly prove SQL/API correctness against C SQLite 3.52.0.

- DB file is standard SQLite format.
- WAL frames are standard SQLite WAL frames (or a minimally compatible derivative, explicitly documented).
- We may write *extra* sidecars (e.g., `.wal-fec`, `.idx-fec`) but the core `.db` stays SQLite-compatible when checkpointed.

### 4.2 Native Mode (RaptorQ-First)

Purpose: maximum concurrency + durability + replication; “the future”.

- Primary durable state is an ECS commit stream (commit capsules).
- Checkpointing can materialize a canonical `.db` for compatibility export, but the source-of-truth is the commit stream and its encoded objects.

Both modes must be supported by the **same SQL/API layer**. Conformance harness validates behavior, not internal format.

---

## 5. Core Data Model (Formal-ish, Testable)

We keep this section intentionally executable: every predicate here must map to unit tests, property tests, and lab runtime schedule exploration.

### 5.1 Identifiers and Types

- `TxnId`: monotone increasing transaction id.
- `Pgno`: page number (1-based).
- `PageSize`: configured page size.
- `ObjectId`: content-addressed id of an ECS object (e.g., hash of canonical object header + source bytes).
- `SymbolId`: (object id, encoding symbol id) per RFC 6330.

### 5.2 The ECS Object

An **ECS object** is the unit of RaptorQ encoding.

Examples:

- a commit capsule (page diffs + metadata)
- a WAL segment
- an index segment
- a snapshot manifest chunk

Each object has:

- `object_header`: versioned, canonical encoding (byte-exact), includes:
  - `object_type`
  - `object_len`
  - `encoding_params` (symbol size, K, etc; RFC 6330 derived)
  - `integrity` (hash / checksum and algorithm id)
  - `links` (parents / dependencies; see §8)
- `source_bytes`: the payload to be encoded

RaptorQ encoding produces:

- `K` **source symbols** (systematic)
- optionally `R` **repair symbols** (overhead), such that receiving any `K` symbols from the union suffices (with high probability, but we treat RFC 6330 compliance as the operational contract)

### 5.3 Commit Capsule (The Heartbeat)

We define a commit capsule object type:

```
CommitCapsule {
  commit_id: TxnId,
  snapshot_basis: SnapshotId,
  writes: Vec<PageDelta>,
  read_conflicts: Vec<ConflictEvidence>,   // for SSI validation
  schema_delta: Option<SchemaDelta>,       // when DDL involved
  checks: CommitChecks,                   // invariants / quick checks
}
```

Where `PageDelta` is not necessarily “a full page image”. It can be:

- full page image (baseline)
- sparse byte-range patch (preferred)
- algebraic delta (GF(256) vector / XOR patch), enabling merge (see §8)

The capsule is encoded into RaptorQ symbols and persisted/distributed via ECS.

---

## 6. ECS Storage Substrate (Objects, Symbols, Physical Layout)

This section is the “steel beam” of the whole system. If ECS is underspecified, everything else becomes vibes.

### 6.1 ECS Invariants (What We Rely On)

ECS is defined by these invariants:

1. **Append-only first:** the primary durable structures are append-only streams of records. Mutations occur via new objects and new pointers, not in-place rewrites.
2. **Self-description:** a symbol record MUST carry enough metadata to be routed, authenticated, and decoded without out-of-band state.
3. **Repair-first recovery:** corruption/loss is handled by *decoding from any sufficient subset*, not by assuming perfect disks/networks.
4. **Determinism in tests:** given identical inputs (seed, object bytes), encoding and scheduling MUST be reproducible under `asupersync::LabRuntime`.
5. **One tiny mutable root:** we allow a minimal mutable “root pointer” file for bootstrapping (like git refs). Everything else is append-only and/or content addressed.

### 6.2 ECS Object Params = RaptorQ Decode Params

We standardize on asupersync’s `ObjectParams` as our concrete OTI carrier:

- `object_size` (bytes)
- `symbol_size` (bytes)
- `source_blocks` (SBN count)
- `symbols_per_block` (K)

These are exactly the parameters needed to know “how many symbols are enough” and how to interpret `(SBN, ESI)` within the object.

### 6.3 Object Identity (ObjectId) and Content Addressing

We use `asupersync::types::symbol::ObjectId` as the on-the-wire object id type.

**V1 policy (native mode):**

- `ObjectId` MUST be *deterministically derived* from canonical bytes, not randomly generated.
- The derivation MUST be stable across machines and runs.
- The derivation MUST be independent of physical storage location.

Recommended construction:

```
object_id = Trunc128( BLAKE3( "fsqlite:ecs:v1" || canonical_object_header || payload_hash ) )
```

Notes:

- `payload_hash` SHOULD be BLAKE3 as well (crypto + speed), but the exact hash choice is a decision we can change behind a versioned header as long as conformance fixtures pin it.
- Even if we later add authenticated symbol transport, the object id remains a *content identity*, not a security credential.

### 6.4 ECS Symbol Record (On-Disk + On-Wire Envelope)

We store and replicate **symbol records**. Each record wraps one RaptorQ symbol payload plus the metadata required to validate it.

Symbol record fields (conceptual):

```
EcsSymbolRecordV1 {
  magic: [u8; 4] = *b"FSQ1",
  version: u16 = 1,

  // Decode params (OTI)
  object_id: ObjectId (128-bit),
  object_size: u64,
  symbol_size: u16,
  source_blocks: u8,
  symbols_per_block: u16, // K

  // Symbol identity
  sbn: u8,
  esi: u32,
  kind: u8, // 0=source, 1=repair

  // Payload
  payload_len: u16, // must equal symbol_size except possibly last source symbol padding rules
  payload_bytes: [u8; payload_len],

  // Integrity/auth
  frame_xxh3_64: u64,              // fast corruption detection for local storage
  auth_tag: Option<AuthenticationTag>, // present when security context enabled (asupersync)
}
```

**Why OTI is repeated in every record:** it makes every record independently decodable/routable and removes “index chicken-and-egg” during recovery and replication. This is deliberate redundancy in service of self-healing.

### 6.5 Local Physical Layout (Native Mode)

For a database path `foo.db`, native ECS state lives under `foo.db.fsqlite/ecs/`:

- `root` (small mutable file, updated via atomic rename):
  - points to the latest `RootManifest` object id
  - includes a tiny redundant payload (e.g., the `RootManifest`’s canonical header + hash) so startup can sanity check quickly
- `symbols/segment-000000.log` (append-only symbol records)
- `symbols/segment-000001.log` (rotated by size)
- `markers/segment-000000.log` (append-only commit marker records; also ECS objects, but we keep a fast sequential stream for scanning)

We also allow derived caches (rebuildable):

- `cache/object_locator.cache` (accelerator mapping object_id → offsets of symbol records)
- `cache/page_index.cache` (accelerator mapping pgno → latest page-update pointer)

**Rule:** Caches MUST be safely deletable and reconstructable by scanning the append-only logs plus the `root` pointer.

### 6.6 RootManifest (The Bootstrapping Anchor)

`RootManifest` is an ECS object whose payload declares:

- current configuration (page size, symbol sizing policy, hash algorithms)
- tips (latest pointers) for:
  - commit marker stream
  - index segment stream(s)
  - snapshot manifest stream
  - compaction/checkpoint generations
- compatibility view status (if enabled)

Startup:

1. Read `ecs/root`
2. Fetch/decode the referenced `RootManifest` (repairing via symbols if needed)
3. Use its tips to locate the latest index segments and commit markers
4. Rebuild any missing caches if configured

### 6.7 Object Retrieval (Decoder-Centric)

To retrieve object bytes:

1. Obtain `ObjectParams` for the object.
   - from any symbol record header (preferred)
   - or from `RootManifest` / index segments
2. Collect any `K = symbols_per_block * source_blocks` symbols.
   - from local symbol logs
   - from remote peers (replication)
3. Decode via `asupersync::raptorq::RaptorQReceiver`.
4. Validate payload hash (and optionally decode proof, see below).

### 6.8 Decode Proofs (Auditable Repair)

Asupersync includes a `DecodeProof` facility (`asupersync::raptorq::proof`). We exploit this in two ways:

- In **lab runtime**: every decode that repairs corruption MUST produce a proof artifact attached to the test trace.
- In **replication**: a replica MAY demand proof artifacts for suspicious objects (e.g., repeated decode failures), enabling explainable “why did we reject this commit?” answers.

### 6.9 Deterministic Encoding (Required For Content-Addressed ECS)

If `ObjectId` is content-derived, symbol generation must be deterministic:

- The set of source symbols is deterministic by definition (payload chunking).
- Repair symbol generation MUST be deterministic for a given object id and config.

Practical rule:

- Derive any internal “repair schedule seed” from `ObjectId` (e.g., `seed = xxh3_64(object_id_bytes)`), and wire it through `RaptorQConfig` or sender construction as needed.

This makes “the object” a platonic mathematical entity: any replica can regenerate missing repair symbols (within policy) without coordination.

---

## 7. Concurrency: MVCC + SSI (Serializable by Default)

### 7.1 Surface Semantics (What Users See)

SQLite’s concurrency is “serial by single-writer lock”. FrankenSQLite’s concurrency is “serializable by validation”. We MUST not silently downgrade correctness.

User-visible contract:

- **Default isolation:** serializable.
- **Readers never block writers** and **writers never block readers**.
- **Writers do not wait while holding locks.** If a required lock is unavailable, the operation fails fast with a retryable error (`BUSY`-class).

API / SQL surface (proposed):

- `BEGIN` (default): serializable MVCC mode (this section).
- `BEGIN IMMEDIATE`: “compat” style global writer intent (useful for legacy apps that expect single-writer semantics; still MVCC underneath, but we suppress concurrency).
- `BEGIN CONCURRENT`: synonym for default MVCC serializable mode (explicit, readable).
- `PRAGMA fsqlite.serializable = ON|OFF`:
  - `ON` (default): SSI/OCC validation enabled
  - `OFF`: Snapshot Isolation (SI) allowed as an explicit opt-in (for benchmark exploration only; not a default)

Error mapping:

- **Write-write conflict** (true conflict, not mergeable): `SQLITE_BUSY` (or `SQLITE_BUSY_SNAPSHOT` where the Oracle uses that code).
- **Serialization failure** (SSI/OCC abort): `SQLITE_BUSY`-class with a distinct extended code for “retryable serialization abort” (we standardize in our API even if SQLite’s exact extended code differs).

### 7.2 Formal MVCC Model (Executable Definitions)

We work at **page granularity** (with optional refinement via range/cell tags).

Let:

- `T` be a transaction.
- `Pgno` be a page identifier.
- `commit_seq(T)` be a monotonically increasing commit sequence number assigned at commit marker append time (not wall clock).

Each transaction has:

- `begin_seq(T)` (logical begin time; derived from current commit stream tip)
- `snapshot(T)` = `(high, in_flight)` as of begin
- `read_set(T)` and `write_set(T)` at page granularity:
  - `read_set(T) ⊆ Pgno`
  - `write_set(T) ⊆ Pgno`

Visibility predicate (page versions):

```
visible(version_created_by, snapshot) :=
  version_created_by == 0
  OR (version_created_by <= snapshot.high
      AND version_created_by ∉ snapshot.in_flight
      AND version_created_by ∈ committed_set)
```

Read rule (self-visibility wins):

```
read(P, T) =
  if P ∈ write_set(T) then T.private_version(P)
  else newest visible committed version of P under snapshot(T)
```

Write rule:

- First write to `P` creates a private delta `Δ(P,T)` in `T`’s write set.
- `Δ` is a `PageDelta` (full page, sparse patch, or algebraic patch).

### 7.3 Why SI Is Not Acceptable (Write Skew)

SI permits write skew: `T1` and `T2` read overlapping logical constraints and write disjoint pages; both commit; constraint violated.

Therefore:

- SI MAY exist only as an explicit opt-in for experiments.
- Default MUST be serializable.

### 7.4 Serializable Strategy: “Page-SSI” (Conservative SSI at Page Granularity)

We implement **Serializable Snapshot Isolation** ideas, but we are explicit about the granularity:

- We track predicate reads using **SIREAD** state on pages (and later, page ranges/cells).
- We track **rw-antidependencies**:

`T1 ->rw T2` exists if:

1. `T1` reads page/key `X` (represented as `Pgno` or `(Pgno, tag)`),
2. `T2` later writes `X`,
3. and `T1` and `T2` overlap in time.

Dangerous structure (SSI):

```
T1 ->rw T2  and  T2 ->rw T3
```

and (in classic SSI) `T3` commits before `T1`, implying a serialization cycle.

**V1 guarantee (conservative, simple, correct):**

> No transaction that commits is allowed to have *both* an incoming and outgoing rw-antidependency.

Rationale:

- Any cycle in a directed dependency graph implies every node on the cycle has at least one incoming and one outgoing edge.
- Under SI + first-committer-wins, anomalies require rw edges; preventing “rw pivots” prevents dangerous structures and therefore prevents cycles.
- This is conservative (more aborts than necessary) but serializable.

This is the “go for broke” choice: correctness first, then reduce aborts with refinements once conformance is stable.

### 7.5 Concrete SSI State (Low-Overhead, Deterministic)

We maintain:

- `TxnState`: `Active | Committed { commit_seq } | Aborted { reason }`
- `SireadTable`: predicate reads
  - baseline key: `Pgno`
  - later refinement key: `(Pgno, RangeTag)` or `(Pgno, CellTag)`
  - value: a compact set of active txn ids (SmallVec/bitset style)
- Per-txn flags:
  - `has_in_rw(T)` (someone read what T wrote, or equivalently some reader conflicts in)
  - `has_out_rw(T)` (T read what someone else later wrote)

We MUST also maintain “explainability witnesses” in debug/lab builds:

- the specific `(Pgno, reader_txn, writer_txn)` events that established `has_in_rw/has_out_rw`

### 7.6 Dependency Updates (When Reads and Writes Happen)

Operations:

```
on_read(T, P):
  read_set(T).insert(P)
  SireadTable.add(P, T)

on_write(U, P):
  write_set(U).insert(P)
  for each active reader T in SireadTable.readers(P):
    if overlaps(T, U):
      // T read something U wrote later: T has an outgoing rw; U has an incoming rw.
      has_out_rw(T) = true
      has_in_rw(U) = true
      record_witness(T, U, P)
```

Overlap predicate:

- Two transactions overlap if both were active at some instant.
- In implementation, we approximate overlap by:
  - `T` is `Active` when `U` writes, and
  - `T.begin_seq <= current_tip` etc (we define exact rules in code; tests enforce determinism).

### 7.7 Commit-Time Rule (Deterministic Abort)

On commit of `U`:

- If `has_in_rw(U) && has_out_rw(U)` then **abort U** (retryable serialization failure).
- Else, proceed to durability protocol (capsule + marker).

This is deterministic: the committing txn is the victim. (We later MAY add a more sophisticated victim selection, but V1 correctness is simplest with “abort self”.)

### 7.8 Deadlock Freedom (Structural Theorem)

Rule:

- No wait while holding page locks. Locks are try-acquire only.

Theorem:

- No deadlock is possible because there is no blocking wait; therefore there is no wait-for cycle.

This must be validated under `asupersync::LabRuntime` schedule exploration.

### 7.9 Relationship To Mergeable Writes

Write-write conflicts are not binary. Section §8 defines algebraic patches and safe merges.

Policy:

- If `U` attempts to commit but detects that a page in `write_set(U)` has been updated since `snapshot(U)`, we MAY attempt a **rebase merge**:
  - rebase `U`’s patch onto the new base version
  - if mergeable and invariant-preserving → commit proceeds
  - else → abort/retry (`BUSY`-class)

This reduces conflict rates on hot B-tree pages without row-level MVCC metadata.

### 7.10 What Must Be Proven (Tests, Not Prose)

For concurrency correctness, we require:

- **Schedule exploration** tests for invariants under DPOR (no deadlocks, no panics, bounded memory, deterministic decisions).
- **Serializability regression suite**:
  - construct known write-skew patterns and ensure at least one txn aborts under default serializable mode.
- **Oracle parity**:
  - for all sequential workloads (the vast majority of conformance), results match C SQLite.

---

## 8. Algebraic Write Merging (Conflict Reduction Without Row MVCC)

Page-level MVCC can still conflict on hot pages. We want to reduce false conflicts **without** upgrading to row-level MVCC metadata (which would break file format and cost space).

We exploit two “merge planes”:

1. **Logical plane (preferred):** merge *intent-level* B-tree operations that commute (e.g., inserts into distinct keys).
2. **Physical plane (fallback):** merge *byte-level* patches when we can prove disjointness + invariant preservation.

RaptorQ alignment:

- Physical patches are vectors over GF(256); XOR composition is natural.
- Logical intent logs are *small* and therefore encode/replicate extremely efficiently as ECS objects.

### 8.1 Patch Types (What A Transaction Actually Records)

Each writing txn records both:

1. **Intent log** (semantic operations; merge-friendly):
   - `Insert { table, key, record }`
   - `Delete { table, key }`
   - `Update { table, key, new_record }`
   - index maintenance ops as needed
2. **Materialized page deltas** (for fast intra-txn reads):
   - `FullPageImage`
   - `SparseRangeXor` (byte ranges + XOR payload)
   - `StructuredPagePatch` (header/cell/free operations)

Commit capsules MAY carry either:

- a fully materialized set of page deltas, OR
- the intent log plus enough metadata to deterministically replay it during recovery.

V1 default:

- Store intent log + the minimal stable materializations needed for fast reads and deterministic replay.

### 8.2 Logical Merge: Deterministic Rebase (The Big Win)

The dominant “same-page conflict” in SQLite workloads is: two writers insert/update rows that land on the same hot leaf page (or the same hot internal pages during splits).

Instead of treating that as fatal, we do:

1. Detect base drift:
   - `base_version(pgno)` for a txn’s write set changed since its snapshot.
2. Attempt **deterministic rebase**:
   - take the txn’s intent log
   - replay it against the *current* committed snapshot
   - produce new page deltas
3. If replay succeeds without violating constraints/invariants → commit proceeds.
4. If replay fails (true conflict, constraint violation, or non-determinism) → abort/retry.

This is “merge by re-execution”, not “merge by bytes”. It’s how we get *row-level concurrency effects* without storing row-level MVCC metadata.

Determinism requirement:

- The replay engine MUST be deterministic for a given `(intent_log, base_snapshot)` under lab runtime (no dependence on wall-clock, iteration order, hash randomization, etc.).

### 8.3 Physical Merge: GF(256) Sparse XOR Patches (When Safe)

Physical merge is useful for tiny, local, obviously-disjoint changes.

Model:

- A page is a vector `p ∈ GF(256)^n`.
- A sparse XOR patch is a vector `Δ` with support in a set of ranges.
- Apply: `p' = p ⊕ Δ`.

Merge condition:

```
disjoint(ΔA, ΔB) := support(ΔA) ∩ support(ΔB) = ∅
merge(ΔA, ΔB) := ΔA ⊕ ΔB
```

When disjoint, merges commute and associate. This gives us:

- merge without ordering assumptions
- a clean algebra that matches the coding field

### 8.4 StructuredPagePatch: Make Safety Explicit

Byte disjointness is not enough if we touch structural metadata (cell pointer array, free list, header).

We therefore define a structured patch:

```
StructuredPagePatch {
  // Serialized or “single-writer only” unless we implement a merge law
  header_ops: Vec<HeaderOp>,

  // Mergeable when disjoint by (cell_key) not merely by byte range
  cell_ops: Vec<CellOp>,

  // Default: conflict. Future: structured merge with proofs.
  free_ops: Vec<FreeSpaceOp>,

  // Escape hatch (debug only): explicit byte ranges with declared invariants
  raw_xor_ranges: Vec<RangeXorPatch>,
}
```

Key point:

- `cell_ops` SHOULD be keyed by a stable identifier (`cell_key_digest` derived from rowid/index key), not by raw offsets. This enables safe merges even when the page layout shifts.

### 8.5 Commit-Time Merge Policy (Pragmatic, Aggressive, Safe)

When a txn `U` reaches commit:

1. Run serializability rule (§7) first. If `U` is a pivot → abort.
2. For each page in `write_set(U)`:
   - if base unchanged → OK
   - else attempt merge:
     1. try deterministic rebase replay (preferred)
     2. else try structured patch merge (if supported for those ops)
     3. else try sparse XOR merge (only if ranges declared merge-safe)
     4. else abort/retry

This yields a strict safety ladder: we only take merges we can justify.

### 8.6 What Must Be Proven (And How We Prove It)

We require runnable proofs:

- **B-tree invariants** hold after replay/merge:
  - ordering
  - cell count bounds
  - free space accounting
  - overflow chain validity
- **Patch algebra invariants**:
  - `apply(p, merge(a,b)) == apply(apply(p,a), b)` when mergeable
  - commutativity for declared commutative ops
- **Determinism**:
  - identical `(intent_log, base_snapshot)` yields identical replay outcome under `LabRuntime` across seeds

These become:

- proptest suites for patch algebra and B-tree invariants
- DPOR schedule exploration tests for merge/commit interleavings

### 8.7 MVCC History Compression: “PageHistory” Objects

Storing full page images per version is not acceptable long-term. Our history representation is:

- newest committed page version: full image (for fast reads)
- older versions: patches (intent logs and/or structured patches)
- for hot pages: encode patch chains as ECS **PageHistory objects** so:
  - history itself is repairable (bounded corruption tolerated)
  - remote replicas can fetch “just enough symbols” to reconstruct a needed historical version

This is not optional fluff: it is how MVCC avoids eating memory under real write concurrency.

---

## 9. Durability: The WAL Is Dead, Long Live The Coded Commit Stream

Durability is the place where “RaptorQ everywhere” becomes real: commits are not “written to a file”, they are **encoded as symbols** and then persisted/replicated under explicit loss/corruption budgets.

### 9.1 Crash Model (Explicit Contract)

We assume:

1. Process can crash at any point.
2. `fsync()` is a durability barrier for data and metadata as documented by the OS.
3. Writes can be reordered unless constrained by fsync barriers.
4. Torn writes exist at sector granularity (tests simulate multiple sector sizes).
5. Corruption/bitrot may exist.
6. File metadata durability may require directory `fsync()` (platform-dependent); our VFS MUST model this and tests MUST include it.

### 9.2 Erasure-Coded Durability Contract (“Self-Healing WAL”)

We add a stronger contract:

> If the commit protocol reports “durable”, then the system MUST be able to reconstruct the committed capsule bytes exactly during recovery, even if some fraction of locally stored symbols are missing or corrupted within the configured tolerance budget.

This is the operational meaning of “self-healing”: we do not merely *detect* corruption; we *repair* it by decoding.

### 9.3 Durability Policy (Local vs Quorum)

Durability is policy-driven:

- **Local durability**: enough symbols persisted to local symbol store(s) such that decode will succeed under the local corruption budget.
- **Quorum durability**: enough symbols persisted across `M` of `N` replicas to survive node loss budgets (see §12).

Policy is exposed via:

- `PRAGMA durability = local`
- `PRAGMA durability = quorum(M)`

and possibly:

- `PRAGMA raptorq_overhead = <percent>` (controls repair symbol budget).

### 9.4 Commit Objects: Capsule + Marker (Atomicity by Construction)

We define two durable object types:

1. `CommitCapsule`: the payload (what changed + evidence).
2. `CommitMarker`: the atomic “this commit exists” record.

**Atomicity rule:**

- A commit is committed iff its marker is committed.
- A marker MUST reference exactly one capsule (by object id).
- Recovery MUST ignore any capsule without a committed marker.

#### CommitCapsule (V1 payload)

Capsule contains:

- `commit_seq` (assigned during marker append)
- `snapshot_basis` (what the txn read)
- `intent_log` (preferred) and/or `page_deltas` (materialized patches)
- `read_set_digest` and `write_set_digest` (for debugging + conformance witnesses)
- SSI witnesses (debug/lab builds; can be feature-gated in release)
- schema delta (DDL) if applicable

#### CommitMarker (V1 payload)

Marker contains:

- `commit_seq`
- `capsule_object_id`
- `prev_marker` pointer (hash-chain for linear history; like a linked list)
- optional `checkpoint_generation`
- integrity hash

The marker stream is append-only and totally ordered; it is the “commit clock”.

### 9.5 Commit Protocol (Native Mode, High-Concurrency)

Goal:

- Writers prepare in parallel.
- Only the minimal “publish commit” step is serialized.

Protocol for txn `T`:

1. Build `CommitCapsuleBytes(T)` deterministically.
2. Encode capsule bytes into symbols using `asupersync::raptorq::RaptorQSender`.
3. Persist symbols to local symbol logs (and optionally stream to replicas) until the durability policy is satisfied:
   - local: persist ≥K symbols plus margin
   - quorum: persist/ack ≥K symbols across M replicas (asupersync quorum combinator)
4. Create `CommitMarkerBytes(commit_seq, capsule_object_id, prev_marker, ...)`.
   - `commit_seq` is allocated at this point.
5. Encode/persist the marker as an ECS object as well, and append its id to the marker stream.
6. Return success to the client.
7. Index segments and caches update asynchronously.

**Critical ordering:** marker publication MUST happen after capsule durability is satisfied. If marker is durable but capsule is not decodable, we violated our core contract.

### 9.6 Recovery Algorithm (Native Mode)

Startup:

1. Load `RootManifest` via `ecs/root` (§6.6).
2. Locate the latest checkpoint (if any) and its manifest.
3. Scan marker stream from the checkpoint tip forward (or from genesis).
4. For each marker:
   - fetch/decode referenced capsule (repairing via symbols)
   - apply capsule to state (materialize page deltas or replay intent log)
5. Rebuild/refresh index segments and caches as needed.

Correctness requirement:

- If recovery sees a committed marker, it MUST eventually be able to decode the capsule (within configured budgets), or else the system must surface a “durability contract violated” diagnostic with decode proofs attached (lab builds).

### 9.7 Checkpointing, Compaction, and Garbage Collection

Without compaction, an append-only commit stream grows forever.

We define checkpoints as ECS objects:

- `CheckpointManifest`: declares a consistent materialized state (either a full `.db` image or a set of page images) at a specific `commit_seq`.
- `CheckpointChunk` objects: the actual bytes (full DB or page groups), encoded as symbols.

Checkpoint procedure (simplified):

1. Choose a `commit_seq` boundary `C`.
2. Materialize the DB image at `C` (or a page set).
3. Encode and persist checkpoint chunks (symbols).
4. Publish a new `RootManifest` pointing to the checkpoint.
5. Now older commits < C become reclaimable by policy (subject to replication lag and reader snapshots).

This is how we bound:

- MVCC history length
- symbol log size
- index segment count

### 9.8 Compatibility Mode Mapping (SQLite Views)

Compatibility mode exists for:

- oracle conformance
- external tooling that expects `.db/.wal`

Two mapping strategies:

1. **View-only**: materialize `.db` from ECS state at open/close/checkpoint boundaries.
2. **Shadow WAL** (optional): maintain a conventional WAL stream plus an FEC sidecar:
   - `.wal` contains normal frames
   - `.wal-fec` contains repair symbols for those bytes

In both cases, ECS remains the source-of-truth in native mode; compatibility artifacts are derived views.

---

## 10. The Radical Index: RaptorQ-Coded Index Segments

SQLite maintains a WAL index so readers can locate the latest frame for a page without scanning the whole WAL.

We go harder:

### 10.1 Index As A Stream Of Objects

We define an **IndexSegment** object:

```
IndexSegment {
  segment_id,
  covers_commits: [TxnIdStart, TxnIdEnd],
  page_to_latest: Map<Pgno, VersionPointer>,
  bloom/quotient filters: optional accelerators,
  integrity + links
}
```

This segment is itself encoded via RaptorQ and persisted via ECS. Therefore:

- the index is self-healing (lose part of it, decode from remaining symbols)
- replication is natural (symbols distributed like any other object)
- late-join replicas can reconstruct index incrementally without perfect logs

### 10.2 Why This Is Not “Too Much”

Because:

- The conformance harness is our judge.
- The lab runtime is our debugger for concurrency.
- If it fails, we can retreat to a conventional index implementation while keeping the ECS interface.

But the V1 spec assumes we **attempt** the coded index because it aligns with the core thesis: *everything durable is an erasure-coded object stream*.

---

## 11. Caching & Acceleration (ARC, Bloom/Quotient Filters, Hot Paths)

If ECS is the “durability brain”, caching is the “p95 nervous system”. The ECS design only wins if the common read/write paths are:

- O(1) or O(log n) with tiny constants
- bounded-memory under concurrency
- predictable under scans (no catastrophic cache thrash)

### 11.1 Pager Cache Policy: ARC, Not LRU

SQLite-era LRU is a known footgun for databases: one large table scan can evict the entire working set.

**V1 cache policy:** ARC (Adaptive Replacement Cache).

Why ARC:

- It adapts between recency and frequency automatically.
- It is scan-resistant compared to naive LRU.
- It is a strong “online algorithm” choice: the policy is driven by observed workload, not hard-coded heuristics.

Implementation constraints:

- MUST be safe Rust (`#[forbid(unsafe_code)]`).
- MUST be O(1) per access/update.
- MUST support “pinned pages” (pages referenced by active cursors/transactions are non-evictable).

### 11.2 MVCC Fast Path: “Most Pages Have No Versions”

MVCC overhead must be near-zero when there is no concurrent modification of a page.

We therefore maintain a **Version Presence Filter**:

- A Bloom filter (or quotient filter) keyed by `Pgno` that answers:
  - “this page definitely has no MVCC versions” (fast skip)
  - “this page might have versions” (fall back to version-chain lookup)

This makes the hot read path:

1. Check cache for current committed page
2. If miss: check filter; if “no versions” → read base page (db or compatibility view)
3. Else → consult version store / index segments

### 11.3 Index Acceleration: Filters + Two-Level Lookup

IndexSegments SHOULD embed:

- a fast filter for “does this segment mention pgno?”
- optionally, a “page hotness” summary to prioritize caching of hot ranges

Readers maintain a two-level lookup:

1. `Pgno -> candidate VersionPointer` using the newest few IndexSegments
2. `VersionPointer -> object_id` lookup via object locator cache (offsets of symbol records)

Both layers are rebuildable; correctness never depends on caches.

### 11.4 Bounded History = Bounded Overhead

MVCC overhead is proportional to version chain length.

We treat this as a bound we can reason about:

- Let `K` be average versions per hot page visible to readers.
- Let `N` be page reads per query.
- Naive MVCC overhead is `O(N*K)` for visibility scan.

Therefore:

- GC/checkpoint MUST keep `K` small.
- For read-mostly workloads, the version filter SHOULD ensure `K ~ 0` for most pages.

This “performance model” is not optional; it drives GC triggers and benchmark budgets.

### 11.5 Memory Accounting (No Surprise OOM)

Every subsystem that stores history MUST have:

- a strict byte budget
- a policy for reclamation under pressure
- metrics exported for harness + benchmarks

We do not accept unbounded growth of:

- page version chains
- SIREAD state
- symbol caches
- index segment caches

---

## 12. Replication: Fountain-Coded, Loss-Tolerant, Late-Join Friendly

We treat replication as distributing ECS symbols:

- Writers create commit capsules and markers.
- Capsules are encoded into symbols.
- Symbols are shipped over lossy transports (e.g., UDP) using asupersync networking.

Properties:

- **Late join:** replica can start receiving symbols mid-stream; any `K` symbols suffice.
- **No fragile ack protocol:** the receiver doesn’t need “symbol #17”; it needs *any set*.
- **Bandwidth optimal:** near-minimal retransmission overhead.

### 12.1 Quorum Durability / Quorum Replication (Optional Mode)

We can define a commit as “durable” only after:

- local persistence of a threshold of symbols, and/or
- acknowledgement (by symbol receipts) from a quorum of replicas

This becomes a tunable policy:

- `PRAGMA durability = local`
- `PRAGMA durability = quorum(M)`

### 12.2 Distributed Consistency (Asupersync Sheaf Lens)

When multiple nodes produce commit streams, inconsistencies can arise that are not visible pairwise. Asupersync includes a sheaf-style obstruction detector for “no global assignment explains local observations”.

We use that to detect replication anomalies early:

- phantom commits
- diverging manifests
- inconsistent merge histories

This is “alien artifact” correctness: not just “it probably works”, but “we have invariants and detectors with formal grounding”.

### 12.3 Symbol Assignment (Consistent Hashing + Quorums)

We do not replicate “files”. We replicate **symbols**.

Default distribution model (native mode):

- Each object is encoded into `K + R` symbols.
- Each symbol is assigned to one or more replicas via **consistent hashing** (asupersync distributed module).
- Durability/availability policy is expressed as “how many distinct replica symbol-stores must hold symbols so that any reader can collect at least `K` symbols”.

This is the key operational lever:

- Increase `R` (repair overhead) to tolerate loss without coordination.
- Increase replication factor (symbols stored on more replicas) to tolerate node failures.
- Tune quorum rules (commit ack) to trade latency for stronger guarantees.

---

## 13. The Asupersync Integration Contract (Cx Everywhere)

### 13.1 Cx Capability Context

All non-trivial operations must take a `&Cx` (capability context):

- VFS I/O
- networking
- timeouts
- cancellation
- deterministic test runtime hooks

We treat `Cx` as the ambient “operating semantics”. In production, it maps to real I/O and clocks. In tests, it maps to lab runtime (virtual time + deterministic scheduling).

### 13.2 Deterministic Concurrency Testing

All concurrency-critical components (MVCC/SSI, commit pipeline, replication, recovery) must be tested under:

- **Lab runtime** (virtual time, deterministic scheduling)
- **DPOR schedule exploration** (Mazurkiewicz trace equivalence classes)

We do not accept “run it 10,000 times and hope”.

### 13.3 Anytime-Valid Invariant Monitoring (e-processes)

We instrument invariants as e-processes (Ville’s inequality) to allow “peeking” without invalidating guarantees:

- MVCC invariant monitors
- replication divergence monitors
- memory growth monitors (version chains / symbol cache)

This is a core part of “operational excellence”: the system watches itself.

### 13.4 `Cx` In Every Trait (No Ambient Authority)

Concrete rule:

- Any trait method that can touch time, I/O, networking, cancellation, concurrency, or randomness must accept a `&asupersync::Cx` (usually as the first parameter after `&self`).

Examples (conceptual signatures):

```rust
use asupersync::Cx;

pub trait Vfs {
    fn open(&self, cx: &Cx, path: &str, flags: OpenFlags) -> Result<Box<dyn VfsFile>>;
}

pub trait VfsFile {
    fn read_at(&self, cx: &Cx, offset: u64, buf: &mut [u8]) -> Result<usize>;
    fn write_at(&self, cx: &Cx, offset: u64, buf: &[u8]) -> Result<()>;
    fn sync(&self, cx: &Cx, flags: SyncFlags) -> Result<()>;
}

pub trait Ecs {
    fn put_symbols(&self, cx: &Cx, obj: ObjectId, symbols: &[Symbol]) -> Result<()>;
    fn get_any_k_symbols(&self, cx: &Cx, obj: ObjectId, k: usize) -> Result<Vec<Symbol>>;
}
```

Why this matters:

- In production, `Cx` is how we get cancellation, deadlines, and observability for free.
- In tests, `Cx` is what binds us to **LabRuntime** (virtual time + deterministic scheduling), making concurrency bugs reproducible.
- In distributed mode, `Cx` is where security context and symbol authentication live.

### 13.5 Lab Runtime Is Mandatory For Concurrency-Critical Tests

Asupersync provides:

- `asupersync::LabRuntime`: deterministic scheduler + virtual time
- DPOR-style schedule exploration (Mazurkiewicz trace quotient)
- trace capture/replay

Policy:

- MVCC/SSI tests must run under `LabRuntime`.
- Commit pipeline + replication must have at least one test that explores schedules (not just fixed interleavings).

---

## 14. Conformance Harness (The Oracle Is The Judge)

Conformance is not Phase 9. It starts immediately.

### 14.1 Oracle

- Build and run C SQLite 3.52.0 from `legacy_sqlite_code/`.
- Run identical test inputs against:
  - C SQLite (oracle)
  - FrankenSQLite
- Compare structured outputs:
  - rows
  - types (where observable)
  - error codes and normalized messages

### 14.2 Test Corpora

- SQLLogicTest (SLT) ingestion (broad SQL coverage).
- Targeted SQLite tests for tricky semantics (transactions, triggers, etc.).
- Crash/recovery tests (fault injection).
- Replication tests (lossy network simulation).

### 14.3 “Golden Output” Discipline

Every optimization or behavioral change must preserve golden outputs unless an intentional divergence is explicitly approved and documented.

---

## 15. Performance Discipline (Extreme Optimization)

We operate under the strict loop:

1. Baseline
2. Profile
3. Prove behavior unchanged (oracle)
4. Implement one lever
5. Re-measure

### 15.1 Benchmarks We Must Have Early

Micro:

- page read path: resolve visible version (with varying chain lengths)
- delta apply / merge cost
- SSI tracking overhead (SIREAD locks + dangerous structure detection)
- RaptorQ encode/decode throughput (object sizes typical for capsules/index segments)
- coded index lookup

Macro:

- multi-writer throughput scaling vs conflict rate
- scan-heavy vs random workloads (cache policy sensitivity)
- replication convergence time under loss

### 15.2 Checksums / Hashes (Performance Reality)

- Use fast non-crypto hashes (e.g., xxhash3) for hot-path integrity where attacker resistance is not required.
- Reserve SHA-256 for security-critical contexts (authenticity, content addressing if desired).

### 15.3 Build Profiles (Perf First, Size As A Separate Track)

This is a database engine, not a demo binary. We want **opt-level 3** for performance-critical builds.

Policy:

- `profile.release`: performance profile (`opt-level=3`, `lto=true`, `codegen-units=1`, `panic=abort`).
- `profile.dist` (or equivalent): size profile (`opt-level="z"`, strip, etc.) for distribution experiments.

Rationale:

- Performance decisions must be measured and optimized under the same profile we intend to ship for serious use.
- Size optimization is not “free”; it can damage hot-path throughput and tail latency.

---

## 16. Implementation Plan (V1 Phases)

This plan is ordered to prevent the “build then refactor” trap.

### Phase 0: Oracle + Harness + Baselines (Days 1+)

- Build C SQLite oracle runner.
- Establish golden output format.
- Ingest SLT smoke subset.
- Stand up criterion benches for the earliest hot loops.

### Phase 1: ECS Skeleton + RaptorQ Plumbing + Cx Plumbing

- Define ECS object headers, ids, symbol store abstraction.
- Integrate asupersync RaptorQ pipeline builders.
- Thread `&Cx` through the engine’s core traits.

### Phase 2: MVCC + SSI Core (Serializable Concurrent Writers)

- Implement MVCC types and visibility.
- Implement SSI tracking at page granularity.
- Implement commit capsules (deltas) and commit marker semantics.
- Lab runtime schedule exploration for invariants.

### Phase 3: Coded Index Segments

- Implement index segment objects and lookups.
- Encode/store as ECS objects (self-healing indices).

### Phase 4: B-Tree + VDBE + Full SQL Surface (Driven By Oracle)

- Implement B-tree from spec + proptest.
- Implement VDBE mem/register + opcodes.
- Expand conformance to chase failures to zero.

### Phase 5: Replication + Snapshot Shipping

- Fountain-coded commit shipping over lossy transport.
- Late join replica bootstrap via snapshot symbols.
- Distributed consistency checks + invariant monitors.

### Phase 6: Algebraic Write Merging (Conflict Reduction)

- Introduce structured page patch format.
- Merge disjoint cell-range deltas.
- Prove correctness by invariants + oracle behavior on concurrency tests.

---

## 17. Risk Register + Open Questions

These are the “hard edges” we expect to resolve with experiments + conformance + profiling:

- Multi-process writers: do we support true multi-process concurrency in V1, or do we scope V1 to in-process and use replication for multi-node?
- SSI false positives at page granularity: how to refine with range-based SIREAD locks without blowing overhead?
- Optimal symbol sizing and overhead (object sizes vary widely).
- Where to checkpoint to canonical `.db` for compatibility without becoming a bottleneck.
- Which operations are safely algebraically mergeable on B-tree pages (requires deep invariants).

---

## 18. Local References (In This Repo)

- RFC 6330: `docs/rfc6330.txt`
- Legacy C oracle source: `legacy_sqlite_code/`
- Existing MVCC draft (superseded by this doc): `MVCC_SPECIFICATION.md`
- Beads workspace: `.beads/`
