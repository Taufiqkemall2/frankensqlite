<p align="center">
  <br>
  <img width="460" src="https://upload.wikimedia.org/wikipedia/commons/3/38/SQLite370.svg" alt="FrankenSQLite">
  <br><br>
</p>

<h1 align="center">FrankenSQLite</h1>

<p align="center">
  <strong>A clean-room Rust reimplementation of SQLite with concurrent writers.</strong>
</p>

<p align="center">
  <a href="https://github.com/Dicklesworthstone/frankensqlite/actions"><img src="https://img.shields.io/github/actions/workflow/status/Dicklesworthstone/frankensqlite/ci.yml?branch=main&label=CI" alt="CI"></a>
  <a href="https://github.com/Dicklesworthstone/frankensqlite/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT"></a>
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/rust-nightly%20%7C%20edition%202024-orange.svg" alt="Rust"></a>
  <a href="https://github.com/Dicklesworthstone/frankensqlite"><img src="https://img.shields.io/badge/unsafe-forbidden-success.svg" alt="unsafe forbidden"></a>
</p>

---

## TL;DR

**The Problem:** SQLite allows only one writer at a time. If two threads or processes try to write concurrently, one blocks (or gets `SQLITE_BUSY`). For write-heavy workloads, this single-writer bottleneck caps throughput regardless of how many cores you have.

**The Solution:** FrankenSQLite reimplements SQLite from scratch in safe Rust and replaces the single-writer lock with MVCC (Multi-Version Concurrency Control) at the page level. Multiple writers can commit simultaneously as long as they touch different pages. Readers never block. The file format stays 100% compatible with existing `.sqlite` databases.

### Why FrankenSQLite?

| Feature | C SQLite | FrankenSQLite |
|---------|----------|---------------|
| Concurrent writers | 1 (file-level lock) | Many (page-level MVCC) |
| Concurrent readers | Unlimited (WAL mode) | Unlimited (no `aReadMark[5]` limit) |
| Memory safety | Manual (C) | Guaranteed (`#[forbid(unsafe_code)]`) |
| Data races | Possible (careful C) | Impossible (Rust ownership) |
| File format | SQLite 3.x | Identical (binary compatible) |
| SQL dialect | Full | Full (same parser coverage) |
| Extensions | FTS3/4/5, R-tree, JSON1, etc. | All the same, compiled in |
| Embedded, zero-config | Yes | Yes |

---

## Design Philosophy

### 1. Clean-Room, Not a Translation

FrankenSQLite is not a C-to-Rust transpilation. It's a from-scratch reimplementation that references the C source only for behavioral specification. Every function is written in idiomatic Rust, leveraging the type system and ownership model instead of translating C idioms.

### 2. MVCC at Page Granularity

Page-level versioning hits the sweet spot between complexity and concurrency:

- **Row-level** (PostgreSQL-style) would break the file format and require VACUUM
- **Table-level** would conflict on every write to a shared table
- **Page-level** maps naturally to SQLite's B-tree structure. Writers to different leaf pages proceed in parallel. Conflicts only arise when two transactions modify the same physical page.

### 3. Zero Unsafe Code

The entire workspace enforces `#[forbid(unsafe_code)]`. Every crate, every module, every line. Memory safety isn't a goal — it's a compile-time guarantee.

### 4. File Format Compatibility Is Non-Negotiable

Databases created by FrankenSQLite must open in C SQLite and vice versa. No migration step, no conversion tool, no "FrankenSQLite format." The 100-byte header, B-tree page layout, record encoding, WAL frame format — all identical.

### 5. Snapshot Isolation with First-Committer-Wins

When two writers touch the same page, the first to commit wins. The second gets `SQLITE_BUSY` and retries. No deadlocks possible by construction (eager page locking, no wait-for cycles). Simple, predictable, and compatible with existing SQLite error handling.

---

## Architecture

FrankenSQLite is organized as a 23-crate Cargo workspace with strict layered dependencies:

```
                          ┌──────────────┐
                          │  fsqlite-cli │  Interactive SQL shell
                          └──────┬───────┘
                                 │
                          ┌──────┴───────┐
                          │   fsqlite    │  Public API facade
                          └──────┬───────┘
                                 │
                          ┌──────┴───────┐
                          │ fsqlite-core │  Engine orchestration
                          └──────┬───────┘
                                 │
            ┌────────────────────┼────────────────────┐
            │                    │                     │
     ┌──────┴──────┐    ┌───────┴───────┐    ┌────────┴───────┐
     │  SQL Layer  │    │ Storage Layer │    │   Extensions   │
     ├─────────────┤    ├───────────────┤    ├────────────────┤
     │ parser      │    │ btree         │    │ ext-fts3       │
     │ ast         │    │ pager         │    │ ext-fts5       │
     │ planner     │    │ wal           │    │ ext-rtree      │
     │ vdbe        │    │ mvcc          │    │ ext-json       │
     │ func        │    │ vfs           │    │ ext-session    │
     └──────┬──────┘    └───────┬───────┘    │ ext-icu        │
            │                   │            │ ext-misc       │
            └─────────┬─────────┘            └────────┬───────┘
                      │                               │
            ┌─────────┴───────────────────────────────┘
            │
     ┌──────┴──────┐    ┌──────────────┐
     │fsqlite-types│    │fsqlite-error │  Foundation (no internal deps)
     └─────────────┘    └──────────────┘
```

### Crate Map

| Layer | Crate | Purpose |
|-------|-------|---------|
| **Foundation** | `fsqlite-types` | PageNumber, PageSize, TxnId, SqliteValue, 190+ VDBE opcodes, serial types, limits, bitflags |
| | `fsqlite-error` | 50+ error variants, SQLite error code mapping, recovery hints, transient detection |
| **Storage** | `fsqlite-vfs` | Virtual filesystem trait (Vfs, VfsFile) abstracting all OS operations |
| | `fsqlite-pager` | Page cache, rollback journal, LRU eviction, dirty page write-back |
| | `fsqlite-wal` | Write-ahead log: frame append, checkpoint, WAL index, crash recovery |
| | `fsqlite-mvcc` | MVCC page versioning, snapshot management, conflict detection, garbage collection |
| | `fsqlite-btree` | B-tree/B+tree: cell parsing, page splitting, overflow chains, cursor navigation |
| **SQL** | `fsqlite-ast` | Typed AST nodes for all SQL statements and expressions |
| | `fsqlite-parser` | Hand-written recursive descent parser with Pratt expression parsing |
| | `fsqlite-planner` | Name resolution, WHERE analysis, join ordering, index selection |
| | `fsqlite-vdbe` | Bytecode VM: 190+ opcodes, register file, fetch-execute loop |
| | `fsqlite-func` | Scalar, aggregate, and window functions (abs, count, row_number, etc.) |
| **Extensions** | `fsqlite-ext-fts3` | FTS3/FTS4 full-text search |
| | `fsqlite-ext-fts5` | FTS5 with BM25 ranking |
| | `fsqlite-ext-rtree` | R-tree spatial indexes and geopoly |
| | `fsqlite-ext-json` | JSON1 functions (extract, set, each, tree, etc.) |
| | `fsqlite-ext-session` | Changeset/patchset generation and application |
| | `fsqlite-ext-icu` | ICU collation and Unicode case folding |
| | `fsqlite-ext-misc` | generate_series, carray, dbstat, dbpage |
| **Integration** | `fsqlite-core` | Wires all layers: connection, prepare, schema, DDL/DML codegen |
| | `fsqlite` | Public API: `Connection::open()`, `execute()`, `query()`, `prepare()` |
| | `fsqlite-cli` | Interactive REPL with dot-commands, output modes, syntax highlighting |
| | `fsqlite-harness` | Conformance test runner comparing against C SQLite |

---

## MVCC: How Concurrent Writers Work

### The Write Path

```
Transaction A: INSERT INTO users ...        Transaction B: INSERT INTO orders ...
         │                                           │
         ▼                                           ▼
  1. Acquire page lock on leaf page 47        1. Acquire page lock on leaf page 112
     (no conflict — different pages)             (no conflict — different pages)
         │                                           │
         ▼                                           ▼
  2. Copy-on-write: create new version        2. Copy-on-write: create new version
     of page 47 tagged with TxnId=42            of page 112 tagged with TxnId=43
         │                                           │
         ▼                                           ▼
  3. Commit: validate, append to WAL          3. Commit: validate, append to WAL
     (mutex held only for the append)            (mutex held only for the append)
         │                                           │
         ▼                                           ▼
  4. Release page lock                        4. Release page lock
```

Both transactions commit successfully in parallel. No blocking.

### The Read Path (Lock-Free)

```
read(page 47, snapshot TxnId=41)
  │
  ├──▶ Buffer pool hit? → Return cached version visible to snapshot
  │
  ├──▶ WAL index lookup? → Read frame, cache it, return
  │
  └──▶ Database file → Read page (implicit TxnId::ZERO), return
```

Readers never acquire locks. Unlimited concurrent readers.

### Conflict Detection

```
Transaction C: UPDATE accounts SET balance = ... WHERE id = 1
Transaction D: UPDATE accounts SET balance = ... WHERE id = 2

Both touch leaf page 47 (same B-tree leaf)?
  │
  ├── Yes → First to lock page 47 wins. Second gets SQLITE_BUSY immediately.
  │         No deadlock possible (eager locking, no wait-for cycles).
  │
  └── No (different leaf pages) → Both proceed and commit independently.
```

### Garbage Collection

Old page versions are reclaimed when no active transaction can see them:

- **GC horizon** = `min(active_snapshot_ids)` across all open transactions
- A version is reclaimable if a newer committed version exists below the horizon
- Background task runs every ~1 second, walks version chains, frees memory

---

## Current Status

FrankenSQLite is in active early development. Phase 1 (Bootstrap & Spec Extraction) is complete.

| Phase | Status | Description |
|-------|--------|-------------|
| 1. Bootstrap & Spec Extraction | **Complete** | Workspace, 23 crate stubs, core types, error handling, 77 tests |
| 2. Core Types & Storage Foundation | In Progress | VFS trait, pager, page cache, Unix I/O |
| 3. B-Tree & SQL Parser | Not Started | B-tree engine, recursive descent parser, typed AST |
| 4. VDBE & Query Pipeline | Not Started | Bytecode VM, query planner, public API facade |
| 5. Persistence, WAL & Transactions | Not Started | Durable WAL, rollback journal, crash recovery |
| 6. MVCC Concurrent Writers | Not Started | Page versioning, snapshot isolation, conflict detection |
| 7. Advanced Planner & Full VDBE | Not Started | Cost-based optimization, window functions, CTEs, triggers |
| 8. Extensions | Not Started | FTS3/4/5, R-tree, JSON1, session, ICU |
| 9. CLI Shell & Conformance | Not Started | Interactive shell, 95%+ SQLite compatibility target |

### What's Implemented (Phase 1)

**`fsqlite-types`** — 2,800+ LOC, 64 tests:
- `PageNumber` (1-based `NonZeroU32`), `PageSize` (validated power-of-2), `PageData`, `TxnId`
- `SqliteValue` enum with 5 storage classes, type coercion, SQLite sort order
- 190+ VDBE opcodes matching C SQLite exactly
- SQLite record format serialization/deserialization
- All `SQLITE_MAX_*` limits from `sqliteLimit.h`
- Bitflags: `OpenFlags`, `SyncFlags`, `VfsOpenFlags`, `AccessFlags`, `PrepareFlags`, `MemFlags`
- `DatabaseHeader` struct parsing the 100-byte file header
- `BTreePageType`, `TextEncoding`, `JournalMode`, `LockLevel`, `TypeAffinity`, etc.

**`fsqlite-error`** — 578 LOC, 13 tests:
- `FrankenError` enum with 50+ variants covering every SQLite error scenario
- `ErrorCode` enum matching C SQLite's `SQLITE_*` integer codes
- Intelligence methods: `is_user_recoverable()`, `suggestion()`, `is_transient()`, `exit_code()`
- Constraint errors: UNIQUE, NOT NULL, CHECK, FOREIGN KEY, PRIMARY KEY
- MVCC errors: WriteConflict, SerializationFailure, SnapshotTooOld

---

## Building from Source

### Prerequisites

- [Rust nightly](https://rustup.rs/) (the `rust-toolchain.toml` handles this automatically)

### Build

```bash
git clone --recursive https://github.com/Dicklesworthstone/frankensqlite.git
cd frankensqlite
cargo build
```

### Run Tests

```bash
# All 77 tests
cargo test

# With output
cargo test -- --nocapture

# Specific crate
cargo test -p fsqlite-types
cargo test -p fsqlite-error
```

### Quality Gates

```bash
# Type checking
cargo check --all-targets

# Linting (pedantic + nursery at deny level)
cargo clippy --all-targets -- -D warnings

# Formatting
cargo fmt --check
```

---

## Query Pipeline (Target Architecture)

When complete, a SQL statement will flow through this pipeline:

```
"SELECT name FROM users WHERE age > 30 ORDER BY name"
  │
  ▼
┌─────────────────────────────────────────────────────┐
│ Lexer (memchr-accelerated tokenization)             │
│ → [SELECT] [name] [FROM] [users] [WHERE] [age]     │
│   [>] [30] [ORDER] [BY] [name]                     │
└──────────────────────┬──────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────┐
│ Parser (recursive descent + Pratt expressions)      │
│ → Select { columns: [name], from: users,            │
│     where: BinOp(>, Column(age), Literal(30)),      │
│     order_by: [name ASC] }                          │
└──────────────────────┬──────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────┐
│ Planner (name resolution, index selection)          │
│ → IndexScan(idx_users_age, > 30)                    │
│   → Sort(name)                                      │
│     → Project(name)                                 │
└──────────────────────┬──────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────┐
│ VDBE Codegen (logical plan → bytecode)              │
│ → Init         → jump to 7                          │
│   Transaction  → begin read                         │
│   OpenRead     → cursor 0 on idx_users_age          │
│   SeekGT       → seek to age > 30                   │
│   Column       → read "name" into r1                │
│   ResultRow    → yield r1                            │
│   Next         → loop to 4                           │
│   Halt         → done                               │
└──────────────────────┬──────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────┐
│ Execution (register-based fetch-execute loop)       │
│ → Rows: ("Alice"), ("Bob"), ("Charlie"), ...        │
└─────────────────────────────────────────────────────┘
```

---

## File Format (Binary Compatible with SQLite)

### Database Header (100 bytes at offset 0)

```
Offset  Size  Field
──────  ────  ─────────────────────────────────────────
  0      16   Magic: "SQLite format 3\0"
 16       2   Page size (512–65536)
 18       1   Write format version (1=journal, 2=WAL)
 19       1   Read format version
 24       4   File change counter
 28       4   Database size in pages
 40       4   Schema cookie
 56       4   Text encoding (1=UTF8, 2=UTF16le, 3=UTF16be)
 96       4   SQLite version that wrote the file
```

### B-tree Page Layout

```
┌───────────────────────────────────┐
│ Page header (8 or 12 bytes)       │
├───────────────────────────────────┤
│ Cell pointer array (2B per cell)  │
├───────────────────────────────────┤
│ Unallocated space                 │
├───────────────────────────────────┤
│ Cell content (grows from bottom)  │
├───────────────────────────────────┤
│ Reserved region                   │
└───────────────────────────────────┘
```

### Record Format

```
┌─────────┬─────────────┬─────────────┬───┬──────────┬──────────┬───┐
│ Hdr size│ Serial type 1│ Serial type 2│...│ Value 1  │ Value 2  │...│
│ (varint)│ (varint)     │ (varint)     │   │ (N bytes)│ (N bytes)│   │
└─────────┴─────────────┴─────────────┴───┴──────────┴──────────┴───┘
```

Serial types: 0=NULL, 1=i8, 2=i16, 3=i24, 4=i32, 5=i48, 6=i64, 7=f64, 8=zero, 9=one, N>=12 even=blob((N-12)/2), N>=13 odd=text((N-13)/2).

---

## Comparison with Alternatives

| | **C SQLite** | **FrankenSQLite** | **libsql** | **DuckDB** |
|---|---|---|---|---|
| Language | C | Rust (safe) | C (SQLite fork) | C++ |
| Concurrent writers | No (1 writer) | Yes (page-level MVCC) | Partial (WAL extensions) | Yes (different architecture) |
| Memory safety | Manual | Compile-time guaranteed | Manual (C) | Manual (C++) |
| File format | SQLite 3.x | SQLite 3.x (compatible) | SQLite 3.x (compatible) | Own format |
| Drop-in replacement | N/A (it's the original) | Yes (file format) | Yes (API + format) | No |
| OLAP optimized | No | No | No | Yes |
| Embeddable | Yes | Yes | Yes | Yes |
| Extensions | Loadable + built-in | Built-in | Built-in + WASM | Built-in |

FrankenSQLite occupies a unique niche: it's the only option that combines SQLite file format compatibility, concurrent writers, and Rust memory safety.

---

## Limitations

- **Early development.** Only the type system and error handling are implemented. You cannot run SQL queries yet.
- **Nightly Rust required.** Uses edition 2024 features that aren't stabilized yet.
- **No C API.** The initial release targets Rust consumers only. A C-compatible FFI wrapper is a future goal.
- **No loadable extensions.** All extensions are compiled in. Dynamic `dlopen`-based loading is not planned for the initial release.
- **Unix-first.** The initial VFS targets Linux and macOS. Windows support will follow.
- **No WASM target.** Browser/edge deployment via WebAssembly is a future goal, not part of the initial release.
- **MVCC adds memory overhead.** Multiple page versions consume more RAM than single-version SQLite. Garbage collection mitigates this but introduces background work.

---

## FAQ

**Q: Can I open an existing SQLite database with FrankenSQLite?**
A: That's the goal. FrankenSQLite reads and writes the standard SQLite file format byte-for-byte. A database created by C SQLite opens in FrankenSQLite and vice versa. (Not yet implemented — this is a target, not current functionality.)

**Q: How does MVCC interact with WAL mode?**
A: WAL frames carry transaction IDs. The WAL index maps `(page_number, txn_id)` to frame offsets. Checkpoint must respect active snapshots, writing back only pages whose versions are no longer needed by any reader.

**Q: What happens when two writers conflict on the same page?**
A: The first to acquire the page lock wins. The second gets `SQLITE_BUSY` immediately (no waiting, no deadlocks). The application retries, exactly as with existing SQLite busy handling.

**Q: Why not use `unsafe` for performance-critical paths?**
A: The premise is wrong. Safe Rust with proper data structures is fast. The type system prevents entire categories of bugs that would require extensive testing to catch in C. The performance ceiling of safe Rust is more than sufficient for a database engine.

**Q: Why reimplement rather than fork?**
A: SQLite's C codebase is brilliant but carries 24 years of accumulated complexity. A clean-room Rust implementation enables MVCC without fighting the existing architecture, provides compile-time memory safety, and produces a codebase that Rust developers can contribute to naturally.

**Q: What's the conformance target?**
A: 95%+ behavioral compatibility with C SQLite 3.52.0, measured by running SQLite's test corpus against both implementations and comparing results. Known incompatibilities will be documented with rationale.

**Q: Is this production-ready?**
A: No. FrankenSQLite is in Phase 1 of 9. The foundation types and error handling are solid and well-tested, but the actual database engine (storage, parsing, execution) is not yet implemented.

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `error[E0554]: #![feature]` | Using stable Rust | Install nightly: `rustup default nightly` or let `rust-toolchain.toml` handle it |
| `cargo clippy` warnings | Pedantic + nursery lints enabled | Fix the lint or add a targeted `#[allow]` with justification |
| `edition 2024` errors | Outdated nightly | Run `rustup update nightly` |
| Submodule missing after clone | Forgot `--recursive` | Run `git submodule update --init --recursive` |
| Tests fail on `fsqlite-types` | Possible float precision | Check platform; tests use exact float comparison for known values |

---

## Project Structure

```
frankensqlite/
├── Cargo.toml                # Workspace: 23 members, shared deps, lint config
├── Cargo.lock                # Pinned dependency versions
├── rust-toolchain.toml       # Nightly channel + rustfmt + clippy
├── AGENTS.md                 # AI agent development guidelines
├── PLAN_TO_PORT_SQLITE_TO_RUST.md    # 9-phase implementation roadmap
├── PROPOSED_ARCHITECTURE.md  # Detailed crate architecture + MVCC design
├── EXISTING_SQLITE_STRUCTURE.md      # SQLite behavioral specification
├── crates/
│   ├── fsqlite-types/        # Core types (2,800+ LOC, 64 tests) ✓
│   ├── fsqlite-error/        # Error handling (578 LOC, 13 tests) ✓
│   ├── fsqlite-vfs/          # Virtual filesystem (in progress)
│   ├── fsqlite-pager/        # Page cache (stub)
│   ├── fsqlite-wal/          # Write-ahead log (stub)
│   ├── fsqlite-mvcc/         # MVCC engine (stub)
│   ├── fsqlite-btree/        # B-tree storage (stub)
│   ├── fsqlite-ast/          # SQL AST (stub)
│   ├── fsqlite-parser/       # SQL parser (stub)
│   ├── fsqlite-planner/      # Query planner (stub)
│   ├── fsqlite-vdbe/         # Bytecode VM (stub)
│   ├── fsqlite-func/         # Built-in functions (stub)
│   ├── fsqlite-ext-*/        # 7 extension crates (stubs)
│   ├── fsqlite-core/         # Engine integration (stub)
│   ├── fsqlite/              # Public API (stub)
│   ├── fsqlite-cli/          # CLI shell (stub)
│   └── fsqlite-harness/      # Conformance tests (stub)
├── legacy_sqlite_code/
│   └── sqlite/               # C SQLite reference (git submodule)
├── benches/                  # Criterion benchmarks (placeholder)
├── conformance/              # SQLite compatibility tests (placeholder)
└── tests/                    # Integration tests (placeholder)
```

---

## About Contributions

Please don't take this the wrong way, but I do not accept outside contributions for any of my projects. I simply don't have the mental bandwidth to review anything, and it's my name on the thing, so I'm responsible for any problems it causes; thus, the risk-reward is highly asymmetric from my perspective. I'd also have to worry about other "stakeholders," which seems unwise for tools I mostly make for myself for free. Feel free to submit issues, and even PRs if you want to illustrate a proposed fix, but know I won't merge them directly. Instead, I'll have Claude or Codex review submissions via `gh` and independently decide whether and how to address them. Bug reports in particular are welcome. Sorry if this offends, but I want to avoid wasted time and hurt feelings. I understand this isn't in sync with the prevailing open-source ethos that seeks community contributions, but it's the only way I can move at this velocity and keep my sanity.

---

## License

MIT
