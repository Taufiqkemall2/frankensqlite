# Benchmark Methodology

This document describes the methodology used by the `fsqlite-e2e` benchmark
suite to produce fair, repeatable, and statistically meaningful comparisons
between FrankenSQLite and C SQLite (via rusqlite).

## Principles

### 1. Warmup Before Measurement

The first **3 iterations** of every benchmark are discarded.  Warmup
eliminates cold-cache effects, OS page-fault bursts, and any first-allocation
overhead that would skew the measured distribution.

| Constant                  | Value |
|---------------------------|-------|
| `WARMUP_ITERATIONS`       | 3     |

### 2. Fixed Iteration Count

After warmup, every benchmark executes at least **20 timed iterations**.
Using a fixed floor prevents wall-clock-based runs from producing fewer
samples on slower hardware.  Criterion may request additional samples for
statistical confidence, but will never run fewer than this minimum.

| Constant                      | Value |
|-------------------------------|-------|
| `MIN_MEASUREMENT_ITERATIONS`  | 20    |
| `MEASUREMENT_TIME_SECS`       | 10    |

### 3. Median and p95 as Primary Statistics

- **Median** is the primary central-tendency measure.  It is robust to
  outliers caused by OS scheduling jitter, GC pauses in the test harness, or
  background I/O.
- **p95** captures tail latency without being dominated by single extreme
  outliers the way p99 can be at low sample counts.
- **p99** is reported when available, but is not the primary comparison
  metric.

### 4. Fresh Database Per Iteration

Each benchmark iteration starts from a clean copy of the golden database (or
a freshly-created in-memory DB).  No state leaks between iterations.  This
prevents one iteration's WAL growth, cache warming, or freelist changes from
affecting the next.

### 5. Identical PRAGMA Configuration

Both engines run with exactly the same PRAGMA settings, governed by
`HarnessSettings` in `lib.rs`.  The defaults are:

| PRAGMA            | Value    | Rationale                              |
|-------------------|----------|----------------------------------------|
| `journal_mode`    | `wal`    | Standard production mode               |
| `synchronous`     | `NORMAL` | Typical production safety level        |
| `cache_size`      | `-2000`  | ~2 MiB page cache (KiB notation)      |
| `page_size`       | `4096`   | Standard page size                     |
| `busy_timeout`    | `5000`   | 5 seconds before SQLITE_BUSY           |

Both engines receive the same PRAGMA list via `HarnessSettings::to_sqlite3_pragmas()`
and `HarnessSettings::to_fsqlite_pragmas()`.

### 6. Environment Capture

Every report records machine-specific context so that numbers are reproducible
and comparable.  The `EnvironmentMeta` struct captures:

| Field           | Source                      |
|-----------------|-----------------------------|
| `os`            | `/proc/version` (Linux)     |
| `arch`          | `std::env::consts::ARCH`    |
| `cpu_count`     | `available_parallelism()`   |
| `cpu_model`     | `/proc/cpuinfo`             |
| `ram_bytes`     | `/proc/meminfo`             |
| `rustc_version` | `rustc --version`           |
| `cargo_profile` | Build profile name          |

## Report Metadata

Every JSONL run record and every E2E report embeds two metadata blocks:

1. **`methodology`** (`MethodologyMeta`) — warmup count, iteration count,
   measurement time, primary/tail statistics, fresh-DB flag, PRAGMA-match
   flag.  Schema version: `fsqlite-e2e.methodology.v1`.

2. **`environment`** (`EnvironmentMeta`) — OS, arch, CPU, RAM, rustc,
   cargo profile.

Consumers can verify exactly how numbers were produced by reading these
fields without inspecting source code.

## Workload Determinism

All workloads use a seeded PRNG (`ChaCha8Rng` from the `rand_chacha` crate)
so that identical seeds produce identical operation sequences across runs and
platforms.  The seed, RNG algorithm, and version are recorded in the report's
`WorkloadInfo` block.

## Correctness Tiers

Performance comparisons are only meaningful when both engines produce correct
results.  The E2E suite validates correctness at three tiers:

1. **`canonical_sha256`** — SHA-256 after VACUUM INTO + checkpoint.
   The intended default compatibility proof.
2. **`logical`** — Matching logical dump + `PRAGMA integrity_check = ok`
   on both engines.  Fallback when canonicalization is unavailable.
3. **`raw_sha256`** — Informational only; page layout legitimately differs
   between engines for logically identical databases.

## Scaling Benchmarks

For concurrency scaling tests (`--concurrency 1,2,4,8`), each concurrency
level is run independently with the configured repeat count.  The scaling
summary reports median ops/s, p95 ops/s, median wall-clock time, and median
retry count per concurrency level, enabling throughput-vs-concurrency curves.

## How to Run

```bash
# Single workload, single concurrency
cargo run -p fsqlite-e2e --bin realdb-e2e -- run \
  --db chinook --workload commutative_inserts --engine sqlite3

# Scaling sweep with repeats
cargo run -p fsqlite-e2e --bin realdb-e2e -- run \
  --db chinook --workload commutative_inserts --engine sqlite3 \
  --concurrency 1,2,4,8 --repeat 5

# Criterion benchmarks (uses methodology constants)
cargo bench -p fsqlite-e2e
```

## Constants Reference

All constants are defined in `crates/fsqlite-e2e/src/methodology.rs`:

```rust
pub const WARMUP_ITERATIONS: u32 = 3;
pub const MIN_MEASUREMENT_ITERATIONS: u32 = 20;
pub const MEASUREMENT_TIME_SECS: u64 = 10;
```

These are embedded in every report via `MethodologyMeta::current()`.
