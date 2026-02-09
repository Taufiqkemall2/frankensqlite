# MVCC Specification for FrankenSQLite

> Historical reference notice: This document is retained for historical reference
> only and is superseded by `COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md`.
> If this file conflicts with the comprehensive spec, the comprehensive spec wins.

## 1. Introduction

This document formally specifies the Multi-Version Concurrency Control (MVCC)
subsystem that replaces SQLite's single-writer `WAL_WRITE_LOCK` with page-level
versioning. Every design choice is justified by a formal property or measurable
performance constraint.

### 1.1 Design Goals

1. **Concurrent writers**: Multiple transactions may write simultaneously,
   conflicting only when they touch the same database page.
2. **Snapshot isolation**: Every transaction reads from a consistent point-in-time
   view; committed writes by concurrent transactions are invisible.
3. **Deadlock freedom**: Structural guarantee (not heuristic detection).
4. **Bounded memory**: Old page versions are garbage-collected; memory usage is
   proportional to active transaction window, not total history.
5. **File format compatibility**: On-disk SQLite format is preserved. MVCC is
   purely an in-memory runtime mechanism; WAL frames use standard format.

### 1.2 Scope

This spec covers the `fsqlite-mvcc` crate and its interactions with
`fsqlite-pager`, `fsqlite-wal`, and `fsqlite-types`. It does NOT cover the
B-tree layer, SQL planner, or VDBE -- those consume MVCC through the
`MvccPager` interface.

---

## 2. Formal Model

### 2.1 Core Types

```
TxnId       := u64                          -- monotonically increasing, allocated by AtomicU64
PageNumber  := NonZeroU32                   -- 1-based page number
PageData    := Vec<u8>                      -- page content, length = page_size

Snapshot := {
    high_water_mark : TxnId,               -- all txn_ids <= this are "potentially committed"
    in_flight       : SortedSet<TxnId>,    -- txn_ids that were active at snapshot creation
}

PageVersion := {
    pgno       : PageNumber,
    created_by : TxnId,
    data       : PageData,
    prev       : Option<Box<PageVersion>>,  -- link to older version
}

PageLockTable := BTreeMap<PageNumber, TxnId>  -- exclusive page-level write locks

Transaction := {
    txn_id     : TxnId,
    snapshot   : Snapshot,
    write_set  : HashMap<PageNumber, PageVersion>,  -- pages written by this txn
    page_locks : HashSet<PageNumber>,               -- locks held
    state      : {Active, Committed, Aborted},
}

CommitLog := {
    committed : BTreeMap<TxnId, CommitRecord>,  -- all committed txn metadata
}

CommitRecord := {
    txn_id    : TxnId,
    pages     : Vec<PageNumber>,   -- pages modified
    timestamp : Instant,           -- wall-clock commit time (for GC heuristics)
}
```

### 2.2 Constants

```
TxnId::ZERO = TxnId(0)    -- implicit version for on-disk data predating any MVCC transaction
```

### 2.3 Invariants (must hold at all times)

**INV-1 (Monotonicity):** Transaction IDs are strictly monotonically increasing.
If transaction T2 begins after T1, then `T2.txn_id > T1.txn_id`.

**INV-2 (Lock Exclusivity):** For any page P, at most one active transaction
holds a lock: `|{T : T.state = Active AND P IN T.page_locks}| <= 1`.

**INV-3 (Version Chain Order):** For any page P, the version chain is ordered
by descending `created_by`: if `V.prev = Some(V')`, then
`V.created_by > V'.created_by`.

**INV-4 (Write Set Consistency):** A transaction's write set contains only
pages for which it holds locks:
`forall P in T.write_set.keys(): P in T.page_locks`.

**INV-5 (Snapshot Stability):** A transaction's snapshot is immutable after
creation. `T.snapshot` is never modified during the transaction's lifetime.

**INV-6 (Commit Atomicity):** A transaction's commit is all-or-nothing. Either
all pages in the write set become visible atomically, or none do.

---

## 3. Visibility Predicate

### 3.1 Definition

A page version `V` is **visible** to snapshot `S` if and only if:

```
visible(V, S) :=
    V.created_by <= S.high_water_mark
    AND V.created_by NOT IN S.in_flight
    AND V.created_by IN committed_txns  -- the creating txn has committed
```

Where `committed_txns` is the set of all committed transaction IDs.

Note: `TxnId::ZERO` is always considered committed (it represents on-disk data).

### 3.2 Version Resolution

To read page P under snapshot S:

```
resolve(P, S) :=
    let chain = version_chain(P)         -- newest to oldest
    return first V in chain where visible(V, S)
    // If no version found, fall back to on-disk data (TxnId::ZERO)
```

**Theorem (Deterministic Resolution):** For a given page P and snapshot S,
`resolve(P, S)` always returns the same version, regardless of concurrent
activity. This follows from INV-5 (snapshot stability) and INV-3 (chain order).

### 3.3 Self-Visibility

A transaction can always see its own uncommitted writes:

```
resolve_for_txn(P, T) :=
    if P in T.write_set:
        return T.write_set[P]
    else:
        return resolve(P, T.snapshot)
```

---

## 4. Transaction Lifecycle

### 4.1 Begin

```
begin(manager) -> Transaction:
    txn_id = manager.next_txn_id.fetch_add(1, Ordering::SeqCst)
    snapshot = capture_snapshot(manager)
    T = Transaction {
        txn_id, snapshot,
        write_set: empty, page_locks: empty,
        state: Active
    }
    manager.active_transactions.insert(txn_id, T)
    return T
```

```
capture_snapshot(manager) -> Snapshot:
    // Must hold read lock on active_transactions during this
    high_water_mark = manager.next_txn_id.load(Ordering::SeqCst) - 1
    in_flight = { T.txn_id : T in manager.active_transactions where T.state = Active }
    return Snapshot { high_water_mark, in_flight }
```

### 4.2 Read

```
read_page(T, pgno) -> PageData:
    return resolve_for_txn(pgno, T).data
```

No locks are acquired for reads. Reads never block.

### 4.3 Write

```
write_page(T, pgno, new_data) -> Result<()>:
    REQUIRE T.state = Active

    // Step 1: Acquire page lock (eager, non-blocking)
    lock_result = page_lock_table.try_acquire(pgno, T.txn_id)
    if lock_result = AlreadyHeld(other_txn_id):
        return Err(SQLITE_BUSY)    // immediate failure, no waiting

    // Step 2: Record lock ownership
    T.page_locks.insert(pgno)

    // Step 3: Copy-on-write
    base_version = resolve_for_txn(pgno, T)
    new_version = PageVersion {
        pgno, created_by: T.txn_id,
        data: new_data,
        prev: base_version,
    }
    T.write_set.insert(pgno, new_version)
    return Ok(())
```

### 4.4 Commit

```
commit(T) -> Result<()>:
    REQUIRE T.state = Active

    // Step 1: Commit validation (first-committer-wins)
    for pgno in T.write_set.keys():
        for committed_txn in commit_log.range(T.snapshot.high_water_mark+1 ..):
            if pgno in committed_txn.pages:
                // Another transaction committed a write to this page
                // after our snapshot was taken
                abort(T)
                return Err(SERIALIZATION_FAILURE)

    // Step 2: WAL append (serialized via mutex -- fast, sequential writes)
    wal_mutex.lock()
    for (pgno, version) in T.write_set:
        wal.append_frame(pgno, version.data, T.txn_id)
    wal.write_commit_record(T.txn_id, database_size)
    wal_mutex.unlock()

    // Step 3: Publish versions to version store
    for (pgno, version) in T.write_set:
        version_store.publish(pgno, version)

    // Step 4: Record in commit log
    commit_log.insert(T.txn_id, CommitRecord {
        txn_id: T.txn_id,
        pages: T.write_set.keys().collect(),
        timestamp: now(),
    })

    // Step 5: Release locks and mark committed
    for pgno in T.page_locks:
        page_lock_table.release(pgno, T.txn_id)
    T.state = Committed
    manager.active_transactions.remove(T.txn_id)
    return Ok(())
```

### 4.5 Abort / Rollback

```
abort(T):
    REQUIRE T.state = Active

    // Discard write set (versions never published)
    T.write_set.clear()

    // Release all page locks
    for pgno in T.page_locks:
        page_lock_table.release(pgno, T.txn_id)
    T.page_locks.clear()

    T.state = Aborted
    manager.active_transactions.remove(T.txn_id)
```

---

## 5. Safety Proofs

### 5.1 Deadlock Freedom

**Theorem:** The MVCC system is deadlock-free.

**Proof:** A deadlock requires a cycle in the wait-for graph. Our system has
no wait-for graph because `try_acquire()` never blocks -- it returns
`Err(SQLITE_BUSY)` immediately if the lock is held by another transaction.
Since no transaction ever waits for another transaction to release a lock,
no cycle can form. QED.

**Structural guarantee:** This is not a detection-based approach (like timeout
or cycle detection in a wait-for graph). Deadlocks are *structurally impossible*
because the `try_acquire` operation is non-blocking by construction.

### 5.2 Snapshot Isolation

**Theorem:** Every transaction observes a consistent snapshot -- it never sees
partial results of concurrent transactions.

**Proof:** Let T1 and T2 be concurrent transactions where T2 commits during
T1's execution.

Case 1: `T2.txn_id > T1.snapshot.high_water_mark`. Then T2's writes have
`created_by = T2.txn_id > T1.snapshot.high_water_mark`, so `visible(V, T1.snapshot)`
is false for all of T2's versions. T1 sees none of T2's writes.

Case 2: `T2.txn_id <= T1.snapshot.high_water_mark` AND `T2.txn_id IN T1.snapshot.in_flight`.
This means T2 was active when T1 began. The `in_flight` check excludes T2's
writes from visibility. T1 sees none of T2's writes.

Case 3: `T2.txn_id <= T1.snapshot.high_water_mark` AND `T2.txn_id NOT IN T1.snapshot.in_flight`.
This means T2 committed before T1 began. T1 sees ALL of T2's writes (which is
correct -- they were committed before our snapshot).

In no case does T1 see a partial set of T2's writes. QED.

### 5.3 First-Committer-Wins Correctness

**Theorem:** If two transactions T1, T2 both write page P, at most one commits
successfully.

**Proof:** Two sub-cases:

Case A: T1 and T2 are concurrent. When T2 attempts `write_page(P)`,
`try_acquire(P)` returns `SQLITE_BUSY` because T1 holds the lock. T2 cannot
write P at all, so it either aborts or writes different pages.

Case B: T1 commits first, then T2 attempts commit. During commit validation,
T2 finds that T1 committed page P with `T1.txn_id > T2.snapshot.high_water_mark`
(or T1 was in T2's in_flight set). The validation fails and T2 must abort.

In both cases, only one transaction's write to P survives. QED.

### 5.4 GC Safety

**Theorem:** Garbage collection never removes a version that any active or
future transaction could need.

**Proof:** See Section 7 (Garbage Collection). The GC horizon is defined as
`min(T.txn_id for T in active_transactions)`. A version V is only collected
if `V.created_by < gc_horizon` AND a newer committed version V' exists where
`V'.created_by <= gc_horizon`. Since all active transactions have
`snapshot.high_water_mark >= gc_horizon`, and V' is visible to all of them
(it's committed and below their horizon), removing V does not affect any
active transaction's reads. Future transactions will have even higher
snapshots, so they also see V' instead of V. QED.

---

## 6. Buffer Pool with ARC Eviction

### 6.1 Rationale

LRU (Least Recently Used) fails for database workloads with scan-heavy queries
because a single table scan evicts the entire working set. ARC (Adaptive
Replacement Cache) auto-tunes between recency and frequency, maintaining
competitive ratio regardless of access pattern.

### 6.2 ARC Overview

ARC maintains four lists:
- **T1**: Pages accessed exactly once recently (recency).
- **T2**: Pages accessed at least twice recently (frequency).
- **B1**: Ghost entries for pages recently evicted from T1 (tracks recency history).
- **B2**: Ghost entries for pages recently evicted from T2 (tracks frequency history).

A tuning parameter **p** (0 <= p <= capacity) controls the balance:
- T1 target size = p, T2 target size = capacity - p.
- A hit in B1 increments p (more recency). A hit in B2 decrements p (more frequency).

### 6.3 MVCC-Aware ARC

Standard ARC keys on page number. Our MVCC-aware variant keys on
`(PageNumber, TxnId)` because multiple versions of the same page coexist.

Eviction constraints:
1. Never evict a page with `ref_count > 0` (pinned by active operation).
2. Never evict a dirty page (must flush to WAL first).
3. Prefer evicting superseded versions (a newer committed version of the same
   page exists and is visible to all active snapshots).

```
CacheKey := (PageNumber, TxnId)

ArcCache := {
    t1       : LinkedHashMap<CacheKey, CachedPage>,
    t2       : LinkedHashMap<CacheKey, CachedPage>,
    b1       : LinkedHashSet<CacheKey>,    -- ghost entries (metadata only)
    b2       : LinkedHashSet<CacheKey>,    -- ghost entries (metadata only)
    p        : usize,                      -- adaptive parameter
    capacity : usize,
}
```

### 6.4 Competitive Ratio

ARC has a proven competitive ratio of 2 against the optimal offline algorithm
(OPT) for sequences that can be partitioned into at most 2 subsequences with
stack distance property. For general sequences, ARC's competitive ratio is
O(log(capacity)), which matches the theoretical lower bound for adaptive
algorithms. This is strictly better than LRU's unbounded competitive ratio
under adversarial access patterns.

**Reference:** Megiddo & Modha, "ARC: A Self-Tuning, Low Overhead Replacement
Cache" (FAST '03).

---

## 7. Garbage Collection

### 7.1 GC Horizon

```
gc_horizon() -> TxnId:
    if active_transactions.is_empty():
        return latest_committed_txn_id
    else:
        return min(T.txn_id for T in active_transactions)
```

### 7.2 Reclaimability Predicate

A version V of page P is reclaimable if and only if:

```
reclaimable(V, P, horizon) :=
    V.created_by < horizon
    AND exists V' in version_chain(P) where:
        V'.created_by > V.created_by
        AND V'.created_by <= horizon
        AND V'.created_by IN committed_txns
```

In plain English: V has been superseded by a newer committed version that is
also below the horizon. No active or future transaction can ever need V.

### 7.3 GC Algorithm

```
gc_sweep():
    horizon = gc_horizon()
    for each page P with version chain:
        let newest_committed_below_horizon = None
        for V in version_chain(P) ordered newest-first:
            if V.created_by <= horizon AND V.created_by IN committed_txns:
                if newest_committed_below_horizon is None:
                    newest_committed_below_horizon = V
                else:
                    // V is superseded by newest_committed_below_horizon
                    unlink_and_free(V)
```

### 7.4 GC Properties

**Bounded memory:** Under steady-state load with maximum transaction duration D
and commit rate R, the maximum number of retained versions per page is bounded
by `R * D + 1` (one current version plus one per concurrent transaction that
might need an older version).

**Non-blocking:** GC runs in a background task and does not acquire page locks.
It only needs a read lock on `active_transactions` to compute the horizon, then
operates on version chains with fine-grained per-chain locks.

---

## 8. Bloom Filter Optimization

### 8.1 Problem

The visibility check `V.created_by NOT IN S.in_flight` requires scanning the
in-flight set for every page version examined. For hot pages with long version
chains and many concurrent transactions, this is O(|in_flight|) per version
check.

### 8.2 Solution

Each snapshot includes a Bloom filter of its in-flight set:

```
Snapshot := {
    high_water_mark : TxnId,
    in_flight       : SortedVec<TxnId>,
    bloom           : BloomFilter,          -- Bloom filter over in_flight
}
```

The visibility check becomes:

```
visible(V, S) :=
    V.created_by <= S.high_water_mark
    AND NOT S.bloom.might_contain(V.created_by)  -- fast negative check
    // On positive (might be false positive), fall back to exact check:
    AND (S.bloom.might_contain(V.created_by) implies V.created_by NOT IN S.in_flight)
```

### 8.3 Parameters

For `n` in-flight transactions with target false positive rate `epsilon`:
- Bits: `m = -n * ln(epsilon) / (ln 2)^2`
- Hash functions: `k = (m/n) * ln 2`

With n=100 and epsilon=0.01: m=959 bits (~120 bytes), k=7 hashes.
This is negligible per-snapshot overhead for O(1) amortized visibility checks.

### 8.4 When to Skip

If `in_flight.len() < 8`, use direct linear scan instead of Bloom filter.
The overhead of Bloom filter construction and hashing exceeds the linear scan
cost for tiny sets.

---

## 9. Probabilistic Conflict Model

### 9.1 Model

Given:
- N concurrent writing transactions
- P total pages in the database
- W pages written per transaction (uniform random assumption)

The probability that any two specific transactions conflict (write the same
page) is:

```
P(conflict between T1, T2) = 1 - ((P - W) / P)^W
                            ≈ 1 - e^(-W^2 / P)    for large P
```

For N concurrent transactions, the expected number of conflicting pairs:

```
E[conflicts] = C(N, 2) * P(conflict between T1, T2)
             = N*(N-1)/2 * (1 - (1 - W/P)^W)
```

### 9.2 Practical Implications

| Scenario | N | P | W | P(any conflict) |
|----------|---|---|---|-----------------|
| Small DB, few writers | 4 | 1,000 | 10 | ~0.59 |
| Small DB, many writers | 16 | 1,000 | 10 | ~1.0 |
| Medium DB, moderate | 8 | 100,000 | 50 | ~0.07 |
| Large DB, heavy write | 16 | 1,000,000 | 100 | ~0.01 |

The model predicts that MVCC page-level locking provides excellent concurrency
for databases larger than the working set, which is the common case for
production workloads.

### 9.3 Validation Requirement

The benchmark suite (bd-zi6, bd-1fr) must measure actual conflict rates under
controlled conditions and compare against this model. The actual rate must fall
within 10% of the predicted rate for uniform-random workloads.

---

## 10. Page Lock Table

### 10.1 Interface

```rust
impl PageLockTable {
    /// Attempt to acquire exclusive lock on a page.
    /// Returns Ok(()) if acquired, Err(SQLITE_BUSY) if held by another txn.
    /// If the same txn already holds it, this is a no-op (idempotent).
    fn try_acquire(&self, pgno: PageNumber, txn_id: TxnId) -> Result<()>;

    /// Release a page lock. Panics if the lock is not held by this txn.
    fn release(&self, pgno: PageNumber, txn_id: TxnId);

    /// Release all locks held by a transaction (used during abort/commit).
    fn release_all(&self, txn_id: TxnId);
}
```

### 10.2 Implementation

Protected by `parking_lot::Mutex<BTreeMap<PageNumber, TxnId>>`. The mutex is
held only for the duration of a single `BTreeMap` lookup + insert, which is
O(log N) where N is the number of locked pages.

Why `BTreeMap` over `HashMap`:
- Predictable worst-case O(log N) vs amortized O(1) with occasional O(N) rehash.
- Memory layout is cache-friendly for range iteration (used by `release_all`).
- For the expected number of concurrent page locks (tens to low hundreds),
  the constant factor difference is negligible.

### 10.3 Sharding (Future Optimization)

For workloads with very high lock contention (many concurrent writers touching
different pages), the lock table can be sharded by `pgno % shard_count`. Each
shard has its own mutex, reducing contention. This is a backward-compatible
optimization that does not change the API.

---

## 11. Write Coordinator

### 11.1 Architecture

The write coordinator serializes the commit pipeline:

```
Writer thread (one per writing txn):
    1. B-tree modifications (parallel -- the expensive part)
    2. Send CommitRequest { write_set, response_tx } to coordinator
    3. Await response_rx

Coordinator (single background task):
    loop:
        request = commit_channel.recv()
        result = validate_and_commit(request)
        request.response_tx.send(result)
```

### 11.2 Why Single Coordinator

B-tree modifications (step 1) happen in parallel across transactions -- this is
where most of the work and latency lives. The coordinator only handles the
fast sequential part: validation (scan commit log) + WAL append (sequential
I/O). Serializing this part is both simpler and faster than concurrent WAL
writes, because:

1. WAL writes are append-only sequential I/O -- the disk/SSD's optimal access
   pattern. Interleaving frames from different transactions would require
   additional bookkeeping and synchronization.
2. Commit validation is O(W * C) where W = write set size and C = concurrent
   commits since snapshot. For typical values (W < 100, C < 10), this is
   sub-microsecond.
3. The coordinator eliminates the need for complex atomic commit protocols
   (e.g., two-phase commit between WAL and version store).

### 11.3 Throughput Model

Coordinator throughput = 1 / (T_validate + T_wal_append)

For typical values:
- T_validate: ~1us (scan 10 commit records, check 50 pages each)
- T_wal_append: ~50us (write 10 frames * 4KB = 40KB sequential I/O)

Throughput: ~20,000 commits/second. This is the theoretical maximum regardless
of concurrency. In practice, B-tree modification time dominates, so the
coordinator is rarely the bottleneck.

---

## 12. MVCC-Pager Interface

### 12.1 API

```rust
pub trait MvccPager: Send + Sync {
    /// Begin a new transaction, capturing the current snapshot.
    fn begin(&self) -> Result<Transaction>;

    /// Read a page visible to this transaction's snapshot.
    /// Falls back: version store -> WAL -> database file.
    fn get_page(&self, txn: &Transaction, pgno: PageNumber) -> Result<PageRef>;

    /// Write a page within this transaction.
    /// Acquires page lock, performs copy-on-write.
    fn write_page(&self, txn: &mut Transaction, pgno: PageNumber, data: PageData) -> Result<()>;

    /// Allocate a new page (for B-tree splits, overflow).
    fn allocate_page(&self, txn: &mut Transaction) -> Result<PageNumber>;

    /// Free a page (return to freelist).
    fn free_page(&self, txn: &mut Transaction, pgno: PageNumber) -> Result<()>;

    /// Commit the transaction. Validates, appends to WAL, publishes versions.
    fn commit(&self, txn: Transaction) -> Result<()>;

    /// Abort/rollback the transaction. Discards write set, releases locks.
    fn rollback(&self, txn: Transaction);
}
```

### 12.2 PageRef

```rust
/// A reference-counted handle to a page in the buffer pool.
/// Dropping the PageRef decrements the ref count, potentially
/// allowing the page to be evicted.
pub struct PageRef {
    pgno: PageNumber,
    txn_id: TxnId,
    data: *const [u8],       // points into buffer pool
    pool: Arc<ArcCache>,     // ref back to pool for ref-count management
}

impl Deref for PageRef {
    type Target = [u8];
    fn deref(&self) -> &[u8] { ... }
}

impl Drop for PageRef {
    fn drop(&mut self) {
        self.pool.unpin(self.pgno, self.txn_id);
    }
}
```

---

## 13. Version Store

### 13.1 Structure

```
VersionStore := {
    chains : RwLock<HashMap<PageNumber, VersionChain>>,
}

VersionChain := {
    head    : Option<Box<PageVersion>>,    -- newest version
    len     : usize,                       -- number of versions in chain
}
```

### 13.2 Operations

```
publish(pgno, version):
    // Called during commit, after WAL append
    chain = chains.write().entry(pgno).or_default()
    version.prev = chain.head.take()
    chain.head = Some(Box::new(version))
    chain.len += 1

lookup(pgno, snapshot) -> Option<&PageVersion>:
    chain = chains.read().get(pgno)?
    let v = chain.head
    while let Some(ref ver) = v:
        if visible(ver, snapshot):
            return Some(ver)
        v = &ver.prev
    return None
```

---

## 14. Crash Recovery Interaction

MVCC version chains are purely in-memory. On crash/restart:

1. All in-memory version state is lost.
2. Recovery replays the WAL to reconstruct the latest committed state.
3. Version chains are rebuilt lazily as pages are accessed.
4. The first transaction after recovery gets `TxnId(1)` (or continues from
   a persisted counter if we persist the TxnId generator to the WAL header).

The formal crash model (bd-1ud) specifies the assumptions about what survives
a crash. The key invariant is: **any data written to the WAL with a commit
record is durable.** MVCC relies on this: committed versions are always
recoverable from the WAL even if the in-memory version store is lost.

---

## 15. Testing Requirements

### 15.1 Property-Based Tests (proptest)

1. **Visibility determinism**: For any generated (page, snapshot) pair,
   `resolve()` always returns the same version.
2. **Snapshot isolation**: Two transactions with different snapshots reading
   the same page see versions consistent with their snapshot.
3. **Lock exclusivity**: No two transactions can simultaneously hold a lock
   on the same page (INV-2).
4. **GC safety**: After a GC sweep, all remaining versions are still needed
   by at least one possible active snapshot.
5. **ARC cache**: After any sequence of get/put operations, the cache size
   never exceeds capacity.

### 15.2 Stress Tests

1. **100 threads x 100 writes**: All 10,000 rows present after completion.
2. **Long reader + concurrent writers**: Reader sees consistent snapshot
   throughout its entire read sweep.
3. **Conflict storm**: N writers all targeting same page. Exactly 1 succeeds
   per round. Others get `SQLITE_BUSY`.
4. **GC under load**: Sustained write load with periodic GC. Memory stays
   bounded. No version needed by an active reader is collected.

### 15.3 Benchmark Targets

1. **Single-writer overhead**: < 2x compared to traditional pager (no MVCC).
2. **Non-contended multi-writer**: Near-linear scaling up to coordinator
   throughput limit (~20K commits/sec).
3. **Conflict rate validation**: Measured rate within 10% of probabilistic
   model prediction for uniform-random workloads.

---

## 16. Implementation Order

This spec is implemented across multiple beads with strict dependencies:

1. **bd-35w**: Core types (`TxnId`, `Snapshot`, `PageVersion`, `Transaction`,
   `PageLockTable`, `CommitLog`, `VersionStore`). Blocked by this spec (bd-w2k).
2. **bd-rmd**: Visibility engine (`visible()`, `resolve()`, `capture_snapshot()`).
   Blocked by bd-35w.
3. **bd-28o**: Conflict detection (`try_acquire()`, commit validation).
   Blocked by bd-35w.
4. **bd-7vf**: ARC cache (Section 6). Blocked by bd-rmd.
5. **bd-2x0**: Bloom filter (Section 8). Blocked by bd-35w.
6. **bd-ya9**: MVCC-native pager (Section 12, `MvccPager` trait + impl).
   Blocked by bd-rmd, bd-28o, bd-7vf, bd-2x0.
7. **bd-2e1**: Garbage collection (Section 7). Blocked by bd-rmd, bd-ya9.
8. **bd-zi6**: Benchmarks (Section 15.3). Blocked by bd-ya9.
9. **bd-2b9**: Property tests (Section 15.1). Blocked by bd-ya9.

---

## Appendix A: Comparison with C SQLite Concurrency

| Property | C SQLite (WAL mode) | FrankenSQLite (MVCC) |
|----------|--------------------|-----------------------|
| Concurrent readers | Yes (unlimited via aReadMark, max 5 distinct snapshots) | Yes (unlimited, each with own snapshot) |
| Concurrent writers | No (WAL_WRITE_LOCK is exclusive) | Yes (page-level locking) |
| Reader blocks writer | No | No |
| Writer blocks reader | No | No |
| Writer blocks writer | Always | Only on same-page conflict |
| Deadlock possible | No (single lock) | No (non-blocking try_acquire) |
| Snapshot duration limit | Limited by WAL checkpoint needs | Limited by GC memory pressure |
| Max snapshots | 5 (WAL_NREADER) | Unlimited |

## Appendix B: Notation Reference

| Symbol | Meaning |
|--------|---------|
| T | Transaction |
| V | Page version |
| S | Snapshot |
| P | Page number |
| W | Pages written per transaction |
| N | Number of concurrent transactions |
| p | ARC tuning parameter |
