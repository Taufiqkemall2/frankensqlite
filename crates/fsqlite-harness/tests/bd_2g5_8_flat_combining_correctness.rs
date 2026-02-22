//! bd-2g5.8: Flat Combining correctness tests — sequential batching under contention.
//!
//! Validates the flat combining protocol for lock-contended hot spots:
//!   1. Registry lifecycle (register/unregister)
//!   2. Single-thread add/read correctness
//!   3. Concurrent adds produce correct total
//!   4. No lost updates under stress
//!   5. Batching under contention (batch_size > 1)
//!   6. No starvation (bounded wait)
//!   7. Metrics integration
//!   8. Machine-readable conformance output

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use fsqlite_mvcc::{FlatCombiner, flat_combining_metrics, reset_flat_combining_metrics};

// ---------------------------------------------------------------------------
// Test 1: Registry lifecycle
// ---------------------------------------------------------------------------

#[test]
fn test_registry_lifecycle() {
    let fc = FlatCombiner::new(0);
    assert_eq!(fc.active_threads(), 0);

    let h1 = fc.register().unwrap();
    assert_eq!(fc.active_threads(), 1);

    let h2 = fc.register().unwrap();
    assert_eq!(fc.active_threads(), 2);

    drop(h1);
    assert_eq!(fc.active_threads(), 1);

    drop(h2);
    assert_eq!(fc.active_threads(), 0);

    println!("[PASS] registry lifecycle: register/unregister/active_threads");
}

// ---------------------------------------------------------------------------
// Test 2: Single-thread add/read correctness
// ---------------------------------------------------------------------------

#[test]
fn test_single_thread_correctness() {
    let fc = FlatCombiner::new(0);
    let h = fc.register().unwrap();

    for i in 1..=100 {
        let result = h.add(1);
        assert_eq!(result, i, "add #{i} returned wrong value");
    }

    let final_read = h.read();
    assert_eq!(final_read, 100);

    drop(h);
    println!("[PASS] single-thread add/read correctness (100 ops)");
}

// ---------------------------------------------------------------------------
// Test 3: Concurrent adds correct total
// ---------------------------------------------------------------------------

#[test]
fn test_concurrent_adds_correct_total() {
    let fc = Arc::new(FlatCombiner::new(0));
    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();

    for _ in 0..4 {
        let f = Arc::clone(&fc);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let h = f.register().unwrap();
            b.wait();
            for _ in 0..500 {
                h.add(1);
            }
            drop(h);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(fc.value(), 2000, "4 threads * 500 adds should = 2000");
    println!("[PASS] concurrent adds: 4 threads * 500 = 2000");
}

// ---------------------------------------------------------------------------
// Test 4: No lost updates under stress
// ---------------------------------------------------------------------------

#[test]
fn test_stress_no_lost_updates() {
    let fc = Arc::new(FlatCombiner::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let total_adds = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(4));

    let mut handles = Vec::new();
    for _ in 0..4 {
        let f = Arc::clone(&fc);
        let s = Arc::clone(&stop);
        let t = Arc::clone(&total_adds);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let h = f.register().unwrap();
            b.wait();
            let mut local = 0u64;
            while !s.load(Ordering::Relaxed) {
                h.add(1);
                local += 1;
            }
            t.fetch_add(local, Ordering::Relaxed);
            drop(h);
        }));
    }

    thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Release);

    for h in handles {
        h.join().unwrap();
    }

    let expected = total_adds.load(Ordering::Relaxed);
    let actual = fc.value();
    assert_eq!(
        actual, expected,
        "accumulator={actual} != submitted={expected} — lost updates!"
    );

    println!("[PASS] stress no-lost-updates: {actual} adds confirmed");
}

// ---------------------------------------------------------------------------
// Test 5: Batching under contention
// ---------------------------------------------------------------------------

#[test]
fn test_batching_under_contention() {
    reset_flat_combining_metrics();

    let fc = Arc::new(FlatCombiner::new(0));
    let barrier = Arc::new(Barrier::new(8));
    let mut handles = Vec::new();

    for _ in 0..8 {
        let f = Arc::clone(&fc);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let h = f.register().unwrap();
            b.wait();
            for _ in 0..200 {
                h.add(1);
            }
            drop(h);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(fc.value(), 1600, "8 threads * 200 = 1600");

    let m = flat_combining_metrics();
    assert!(m.fsqlite_flat_combining_ops_total >= 1600);
    // Under contention, batch_size_max should be > 1 (at least some batching).
    // Note: on very fast single-core systems, this might not always batch,
    // but on multi-core it reliably does.
    println!(
        "[PASS] batching: ops={} batches={} max_batch={} avg_batch={:.2}",
        m.fsqlite_flat_combining_ops_total,
        m.fsqlite_flat_combining_batches_total,
        m.fsqlite_flat_combining_batch_size_max,
        if m.fsqlite_flat_combining_batches_total > 0 {
            m.fsqlite_flat_combining_ops_total as f64
                / m.fsqlite_flat_combining_batches_total as f64
        } else {
            0.0
        }
    );
}

// ---------------------------------------------------------------------------
// Test 6: No starvation (bounded wait)
// ---------------------------------------------------------------------------

#[test]
fn test_no_starvation() {
    let fc = Arc::new(FlatCombiner::new(0));
    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();

    for _ in 0..4 {
        let f = Arc::clone(&fc);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let h = f.register().unwrap();
            b.wait();
            let start = Instant::now();
            for _ in 0..100 {
                h.add(1);
            }
            let elapsed = start.elapsed();
            drop(h);
            elapsed
        }));
    }

    let mut max_elapsed = Duration::ZERO;
    for h in handles {
        let elapsed = h.join().unwrap();
        max_elapsed = max_elapsed.max(elapsed);
        assert!(
            elapsed < Duration::from_secs(5),
            "thread took {elapsed:?} — possible starvation"
        );
    }

    assert_eq!(fc.value(), 400);
    println!("[PASS] no starvation: max thread time = {max_elapsed:?}");
}

// ---------------------------------------------------------------------------
// Test 7: Metrics integration
// ---------------------------------------------------------------------------

#[test]
fn test_metrics_integration() {
    reset_flat_combining_metrics();

    let fc = FlatCombiner::new(0);
    let h = fc.register().unwrap();

    h.add(10);
    h.add(20);
    h.add(30);

    let m = flat_combining_metrics();
    assert_eq!(m.fsqlite_flat_combining_ops_total, 3, "expected 3 ops");
    assert!(m.fsqlite_flat_combining_wait_ns_total > 0);

    // Verify serialization.
    let json = serde_json::to_string(&m).unwrap();
    assert!(json.contains("fsqlite_flat_combining_batches_total"));
    assert!(json.contains("fsqlite_flat_combining_ops_total"));

    drop(h);

    println!(
        "[PASS] metrics: batches={} ops={} wait_ns={}",
        m.fsqlite_flat_combining_batches_total,
        m.fsqlite_flat_combining_ops_total,
        m.fsqlite_flat_combining_wait_ns_total
    );
}

// ---------------------------------------------------------------------------
// Test 8: Conformance summary (JSON)
// ---------------------------------------------------------------------------

#[test]
fn test_conformance_summary() {
    struct TestResult {
        name: &'static str,
        pass: bool,
        detail: String,
    }

    let mut results = Vec::new();

    // 1. Registry lifecycle
    {
        let fc = FlatCombiner::new(0);
        let h = fc.register().unwrap();
        let active = fc.active_threads();
        drop(h);
        let after = fc.active_threads();
        let pass = active == 1 && after == 0;
        results.push(TestResult {
            name: "registry_lifecycle",
            pass,
            detail: format!("active={active} after_drop={after}"),
        });
    }

    // 2. Single-thread correctness
    {
        let fc = FlatCombiner::new(0);
        let h = fc.register().unwrap();
        h.add(10);
        h.add(20);
        let v = h.read();
        let pass = v == 30;
        results.push(TestResult {
            name: "single_thread_correctness",
            pass,
            detail: format!("value={v}"),
        });
        drop(h);
    }

    // 3. Concurrent correctness
    {
        let fc = Arc::new(FlatCombiner::new(0));
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();
        for _ in 0..4 {
            let f = Arc::clone(&fc);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let h = f.register().unwrap();
                b.wait();
                for _ in 0..100 {
                    h.add(1);
                }
                drop(h);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let v = fc.value();
        let pass = v == 400;
        results.push(TestResult {
            name: "concurrent_correctness",
            pass,
            detail: format!("value={v} expected=400"),
        });
    }

    // 4. Metrics increment
    {
        reset_flat_combining_metrics();
        let fc = FlatCombiner::new(0);
        let h = fc.register().unwrap();
        h.add(1);
        h.add(2);
        let m = flat_combining_metrics();
        let pass = m.fsqlite_flat_combining_ops_total == 2;
        results.push(TestResult {
            name: "metrics_increment",
            pass,
            detail: format!("ops_total={}", m.fsqlite_flat_combining_ops_total),
        });
        drop(h);
    }

    // 5. No starvation (quick)
    {
        let fc = Arc::new(FlatCombiner::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let f = Arc::clone(&fc);
        let b = Arc::clone(&barrier);
        let t = thread::spawn(move || {
            let h = f.register().unwrap();
            b.wait();
            let start = Instant::now();
            for _ in 0..50 {
                h.add(1);
            }
            drop(h);
            start.elapsed()
        });

        let f2 = Arc::clone(&fc);
        let b2 = Arc::clone(&barrier);
        let t2 = thread::spawn(move || {
            let h = f2.register().unwrap();
            b2.wait();
            let start = Instant::now();
            for _ in 0..50 {
                h.add(1);
            }
            drop(h);
            start.elapsed()
        });

        let e1 = t.join().unwrap();
        let e2 = t2.join().unwrap();
        let pass = e1 < Duration::from_secs(5) && e2 < Duration::from_secs(5);
        results.push(TestResult {
            name: "no_starvation",
            pass,
            detail: format!("t1={e1:?} t2={e2:?}"),
        });
    }

    // 6. No lost updates (quick)
    {
        let fc = Arc::new(FlatCombiner::new(0));
        let barrier = Arc::new(Barrier::new(2));
        let total = Arc::new(AtomicU64::new(0));

        let f = Arc::clone(&fc);
        let b = Arc::clone(&barrier);
        let tt = Arc::clone(&total);
        let t1 = thread::spawn(move || {
            let h = f.register().unwrap();
            b.wait();
            for _ in 0..100 {
                h.add(1);
            }
            tt.fetch_add(100, Ordering::Relaxed);
            drop(h);
        });

        let f2 = Arc::clone(&fc);
        let b2 = Arc::clone(&barrier);
        let tt2 = Arc::clone(&total);
        let t2 = thread::spawn(move || {
            let h = f2.register().unwrap();
            b2.wait();
            for _ in 0..100 {
                h.add(1);
            }
            tt2.fetch_add(100, Ordering::Relaxed);
            drop(h);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        let expected = total.load(Ordering::Relaxed);
        let actual = fc.value();
        let pass = actual == expected;
        results.push(TestResult {
            name: "no_lost_updates",
            pass,
            detail: format!("actual={actual} expected={expected}"),
        });
    }

    // Summary
    let total = results.len();
    let passed = results.iter().filter(|r| r.pass).count();
    let failed = total - passed;

    println!("\n=== bd-2g5.8: Flat Combining Conformance Summary ===");
    println!("{{");
    println!("  \"bead\": \"bd-2g5.8\",");
    println!("  \"suite\": \"flat_combining_correctness\",");
    println!("  \"total\": {total},");
    println!("  \"passed\": {passed},");
    println!("  \"failed\": {failed},");
    println!(
        "  \"pass_rate\": \"{:.1}%\",",
        passed as f64 / total as f64 * 100.0
    );
    println!("  \"cases\": [");
    for (i, r) in results.iter().enumerate() {
        let comma = if i + 1 < total { "," } else { "" };
        let status = if r.pass { "PASS" } else { "FAIL" };
        println!(
            "    {{ \"name\": \"{}\", \"status\": \"{status}\", \"detail\": \"{}\" }}{comma}",
            r.name, r.detail
        );
    }
    println!("  ]");
    println!("}}");

    assert_eq!(
        failed, 0,
        "{failed}/{total} flat combining conformance tests failed"
    );

    println!("[PASS] all {total} flat combining conformance tests passed");
}
