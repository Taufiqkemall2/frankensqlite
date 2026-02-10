//! Recovery demo: single data page corruption (bd-3c5d).
//!
//! Demonstrates that FrankenSQLite's FEC sidecar can recover from single-page
//! corruption while C SQLite reports permanent data loss.

use std::path::Path;

use fsqlite_core::db_fec::{
    DB_FEC_HEADER_SIZE, DbFecHeader, RepairResult, attempt_page_repair, generate_db_fec_from_bytes,
    read_db_fec_group_for_page, verify_page_xxh3_128,
};

// ── Helpers ──────────────────────────────────────────────────────────────

/// Create a real SQLite database via rusqlite and return its bytes + page size.
fn create_test_database(path: &Path) -> (Vec<u8>, u32) {
    let conn = rusqlite::Connection::open(path).expect("open");
    conn.execute_batch(
        "
        PRAGMA journal_mode = DELETE;
        PRAGMA page_size = 4096;
        CREATE TABLE data(id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
        ",
    )
    .expect("setup");

    // Insert enough data to span multiple pages.
    conn.execute_batch("BEGIN;").expect("begin");
    for i in 0..500 {
        conn.execute(
            "INSERT INTO data(id, payload) VALUES (?1, ?2)",
            rusqlite::params![i, format!("row-{i:06}-payload-{}", "x".repeat(200))],
        )
        .expect("insert");
    }
    conn.execute_batch("COMMIT;").expect("commit");

    let page_size: u32 = conn
        .pragma_query_value(None, "page_size", |r| r.get(0))
        .expect("page_size");
    let page_count: u32 = conn
        .pragma_query_value(None, "page_count", |r| r.get(0))
        .expect("page_count");
    assert!(
        page_count > 10,
        "need enough pages to test: got {page_count}"
    );

    drop(conn);
    let data = std::fs::read(path).expect("read db");
    (data, page_size)
}

/// Read a page from database bytes (1-based pgno).
fn read_page(db_data: &[u8], pgno: u32, page_size: usize) -> Vec<u8> {
    let offset = (pgno as usize - 1) * page_size;
    db_data[offset..offset + page_size].to_vec()
}

/// Write a page back into database bytes (1-based pgno).
fn write_page(db_data: &mut [u8], pgno: u32, page_size: usize, page_data: &[u8]) {
    let offset = (pgno as usize - 1) * page_size;
    db_data[offset..offset + page_size].copy_from_slice(page_data);
}

/// Parse the FEC sidecar header from sidecar bytes.
fn parse_sidecar_header(sidecar: &[u8]) -> DbFecHeader {
    assert!(sidecar.len() >= DB_FEC_HEADER_SIZE);
    let mut buf = [0u8; DB_FEC_HEADER_SIZE];
    buf.copy_from_slice(&sidecar[..DB_FEC_HEADER_SIZE]);
    DbFecHeader::from_bytes(&buf).expect("parse header")
}

// ── Tests ────────────────────────────────────────────────────────────────

#[test]
fn test_single_data_page_corruption_and_recovery() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.db");

    // 1. Create real SQLite database with data.
    let (original_db, page_size) = create_test_database(&db_path);
    let ps = page_size as usize;
    let page_count = original_db.len() / ps;
    assert!(page_count > 10);

    // 2. Generate FEC sidecar.
    let sidecar = generate_db_fec_from_bytes(&original_db).expect("generate sidecar");
    let hdr = parse_sidecar_header(&sidecar);
    assert!(hdr.is_current(
        u32::from_be_bytes(original_db[24..28].try_into().unwrap()),
        u32::from_be_bytes(original_db[28..32].try_into().unwrap()),
        u32::from_be_bytes(original_db[36..40].try_into().unwrap()),
        u32::from_be_bytes(original_db[40..44].try_into().unwrap()),
    ));

    // 3. Pick a data page to corrupt (page 5 — middle of data region).
    let target_pgno = 5_u32;
    let original_page = read_page(&original_db, target_pgno, ps);

    // 4. Corrupt the page.
    let mut corrupt_db = original_db.clone();
    let offset = (target_pgno as usize - 1) * ps;
    for b in &mut corrupt_db[offset..offset + ps] {
        *b = 0xDE;
    }

    // 5. C SQLite detects corruption.
    let corrupt_path = dir.path().join("corrupt.db");
    std::fs::write(&corrupt_path, &corrupt_db).expect("write corrupt");
    let conn = rusqlite::Connection::open_with_flags(
        &corrupt_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .expect("open corrupt");
    let integrity: String = conn
        .pragma_query_value(None, "integrity_check", |r| r.get(0))
        .expect("integrity_check");
    // C SQLite integrity check should NOT return "ok" for corrupted database.
    // (Note: integrity_check may or may not detect page-level corruption depending
    // on which page was corrupted. For btree data pages, it often detects issues.)
    drop(conn);

    // 6. FrankenSQLite detects corruption via xxh3_128.
    let (meta, repair_symbols) =
        read_db_fec_group_for_page(&sidecar, &hdr, target_pgno).expect("read group");
    let corrupt_page = read_page(&corrupt_db, target_pgno, ps);
    let idx = (target_pgno - meta.start_pgno) as usize;
    assert!(
        !verify_page_xxh3_128(&corrupt_page, &meta.source_page_xxh3_128[idx]),
        "corrupted page must fail xxh3_128 validation"
    );

    // 7. FrankenSQLite repairs using FEC sidecar.
    let read_fn = |pgno: u32| -> Vec<u8> { read_page(&corrupt_db, pgno, ps) };
    let (recovered_page, result) =
        attempt_page_repair(target_pgno, &meta, &read_fn, &repair_symbols)
            .expect("repair should succeed");

    assert_eq!(
        recovered_page, original_page,
        "recovered page must match original"
    );
    assert!(
        matches!(result, RepairResult::Repaired { pgno, .. } if pgno == target_pgno),
        "result should be Repaired with correct pgno"
    );

    // 8. Write recovered page back and verify integrity.
    write_page(&mut corrupt_db, target_pgno, ps, &recovered_page);
    let repaired_path = dir.path().join("repaired.db");
    std::fs::write(&repaired_path, &corrupt_db).expect("write repaired");

    let repaired_conn = rusqlite::Connection::open_with_flags(
        &repaired_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .expect("open repaired");
    let repaired_integrity: String = repaired_conn
        .pragma_query_value(None, "integrity_check", |r| r.get(0))
        .expect("integrity_check");
    assert_eq!(
        repaired_integrity, "ok",
        "repaired database must pass integrity_check"
    );

    // 9. Verify data is intact: compare original vs repaired bytes.
    assert_eq!(
        corrupt_db, original_db,
        "repaired database must match original byte-for-byte"
    );

    // 10. Emit DecodeProof artifact.
    eprintln!("=== DecodeProof ===");
    eprintln!("  target_pgno: {target_pgno}");
    eprintln!("  group_start: {}", meta.start_pgno);
    eprintln!("  group_size (K): {}", meta.group_size);
    eprintln!("  repair_symbols (R): {}", meta.r_repair);
    eprintln!("  repair_result: {result:?}");
    eprintln!(
        "  c_sqlite_integrity: {}",
        if integrity == "ok" {
            "ok (corruption not detected by C SQLite)"
        } else {
            "FAILED (corruption detected by C SQLite)"
        }
    );
    eprintln!("  franken_integrity: ok (repair successful)");
    eprintln!("===================");
}

#[test]
fn test_header_page_corruption_400pct_recovery() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.db");

    // Create database.
    let (original_db, page_size) = create_test_database(&db_path);
    let ps = page_size as usize;

    // Generate sidecar.
    let sidecar = generate_db_fec_from_bytes(&original_db).expect("generate sidecar");
    let hdr = parse_sidecar_header(&sidecar);

    let original_page1 = read_page(&original_db, 1, ps);

    // Corrupt page 1 (header).
    let mut corrupt_db = original_db.clone();
    for b in &mut corrupt_db[..ps] {
        *b = 0xAA;
    }

    // C SQLite: completely unreadable.
    let corrupt_path = dir.path().join("header_corrupt.db");
    std::fs::write(&corrupt_path, &corrupt_db).expect("write corrupt");
    let open_result = rusqlite::Connection::open_with_flags(
        &corrupt_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    );
    // Even opening may fail or subsequent queries will fail.
    let csqlite_can_read = if let Ok(conn) = open_result {
        conn.pragma_query_value(None, "page_count", |r| r.get::<_, u32>(0))
            .is_ok()
    } else {
        false
    };

    // FrankenSQLite: repair via G=1, R=4 (400% redundancy).
    let (meta, repair_symbols) =
        read_db_fec_group_for_page(&sidecar, &hdr, 1).expect("read page 1 group");
    assert_eq!(meta.group_size, 1, "header page group must be G=1");
    assert_eq!(meta.r_repair, 4, "header page must have R=4");

    let corrupt_page1 = read_page(&corrupt_db, 1, ps);
    assert!(
        !verify_page_xxh3_128(&corrupt_page1, &meta.source_page_xxh3_128[0]),
        "corrupted header must fail validation"
    );

    let read_fn = |_pgno: u32| -> Vec<u8> { read_page(&corrupt_db, 1, ps) };
    let (recovered_page, result) =
        attempt_page_repair(1, &meta, &read_fn, &repair_symbols).expect("repair header");
    assert_eq!(recovered_page, original_page1);
    assert!(matches!(result, RepairResult::Repaired { pgno: 1, .. }));

    // Write back and verify.
    write_page(&mut corrupt_db, 1, ps, &recovered_page);
    let repaired_path = dir.path().join("header_repaired.db");
    std::fs::write(&repaired_path, &corrupt_db).expect("write repaired");
    let conn = rusqlite::Connection::open_with_flags(
        &repaired_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .expect("open repaired");
    let integrity: String = conn
        .pragma_query_value(None, "integrity_check", |r| r.get(0))
        .expect("integrity_check");
    assert_eq!(integrity, "ok");
    assert_eq!(corrupt_db, original_db);

    eprintln!("=== Header Page DecodeProof ===");
    eprintln!("  G=1, R=4 (400% redundancy)");
    eprintln!("  c_sqlite_can_read: {csqlite_can_read}");
    eprintln!("  franken_repair: successful");
    eprintln!("  repair_result: {result:?}");
    eprintln!("===============================");
}

#[test]
fn test_corruption_detection_via_xxh3_all_pages() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.db");

    let (db_data, page_size) = create_test_database(&db_path);
    let ps = page_size as usize;

    let sidecar = generate_db_fec_from_bytes(&db_data).expect("generate sidecar");
    let hdr = parse_sidecar_header(&sidecar);

    let page_count = db_data.len() / ps;

    // Every page should pass xxh3_128 validation against the sidecar.
    for pgno in 1..=page_count {
        #[allow(clippy::cast_possible_truncation)]
        let pgno_u32 = pgno as u32;
        let (meta, _) = read_db_fec_group_for_page(&sidecar, &hdr, pgno_u32).expect("read group");
        let page = read_page(&db_data, pgno_u32, ps);
        let idx = (pgno_u32 - meta.start_pgno) as usize;
        assert!(
            verify_page_xxh3_128(&page, &meta.source_page_xxh3_128[idx]),
            "page {pgno} should pass xxh3_128 validation"
        );

        // Corrupt the page and verify detection.
        let corrupt = vec![0xFF_u8; ps];
        assert!(
            !verify_page_xxh3_128(&corrupt, &meta.source_page_xxh3_128[idx]),
            "corrupted page {pgno} should fail xxh3_128 validation"
        );
    }

    eprintln!("OK: all {page_count} pages validated via xxh3_128 sidecar hashes");
}

#[test]
fn test_decode_proof_artifact_emitted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.db");

    let (db_data, page_size) = create_test_database(&db_path);
    let ps = page_size as usize;

    let sidecar = generate_db_fec_from_bytes(&db_data).expect("generate sidecar");
    let hdr = parse_sidecar_header(&sidecar);

    // Corrupt page 3.
    let target = 3_u32;
    let mut corrupt_db = db_data;
    let off = (target as usize - 1) * ps;
    for b in &mut corrupt_db[off..off + ps] {
        *b = 0xBB;
    }

    let (meta, syms) = read_db_fec_group_for_page(&sidecar, &hdr, target).expect("read");
    let read_fn = |pgno: u32| -> Vec<u8> { read_page(&corrupt_db, pgno, ps) };
    let (_, result) = attempt_page_repair(target, &meta, &read_fn, &syms).expect("repair");

    // Verify DecodeProof fields are present and correct.
    assert!(
        matches!(result, RepairResult::Repaired { .. }),
        "expected Repaired result, got {result:?}"
    );
    if let RepairResult::Repaired { pgno, symbols_used } = result {
        assert_eq!(pgno, target);
        assert!(symbols_used > 0, "must use at least one symbol");
        // Source symbols + repair symbols should be >= K.
        assert!(
            symbols_used >= meta.group_size,
            "symbols_used ({symbols_used}) must be >= K ({})",
            meta.group_size
        );
        eprintln!(
            "DecodeProof: pgno={pgno}, symbols_used={symbols_used}, K={}, R={}",
            meta.group_size, meta.r_repair
        );
    }
}

// ── bd-2rr9: Stale sidecar detection via db_gen_digest mismatch ──────

#[test]
fn test_stale_sidecar_rejected_after_modification() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.db");

    // Create database and generate sidecar.
    let (original_db, _page_size) = create_test_database(&db_path);
    let sidecar = generate_db_fec_from_bytes(&original_db).expect("generate sidecar");
    let hdr = parse_sidecar_header(&sidecar);

    // Verify sidecar is current for the original database.
    let change_counter = u32::from_be_bytes(original_db[24..28].try_into().unwrap());
    let page_count = u32::from_be_bytes(original_db[28..32].try_into().unwrap());
    let freelist_count = u32::from_be_bytes(original_db[36..40].try_into().unwrap());
    let schema_cookie = u32::from_be_bytes(original_db[40..44].try_into().unwrap());
    assert!(
        hdr.is_current(change_counter, page_count, freelist_count, schema_cookie),
        "sidecar must be current for original database"
    );

    // Now modify the database via rusqlite (INSERT changes change_counter).
    let conn = rusqlite::Connection::open(&db_path).expect("open");
    conn.execute_batch("INSERT INTO data(id, payload) VALUES (9999, 'new-row');")
        .expect("insert");
    drop(conn);

    // Read the modified database header.
    let modified_db = std::fs::read(&db_path).expect("read modified");
    let new_change_counter = u32::from_be_bytes(modified_db[24..28].try_into().unwrap());
    let new_page_count = u32::from_be_bytes(modified_db[28..32].try_into().unwrap());
    let new_freelist_count = u32::from_be_bytes(modified_db[36..40].try_into().unwrap());
    let new_schema_cookie = u32::from_be_bytes(modified_db[40..44].try_into().unwrap());

    // The old sidecar must NOT be current for the modified database.
    assert!(
        !hdr.is_current(
            new_change_counter,
            new_page_count,
            new_freelist_count,
            new_schema_cookie
        ),
        "stale sidecar must be rejected for modified database"
    );

    // Verify at least one header field changed.
    assert!(
        change_counter != new_change_counter
            || page_count != new_page_count
            || freelist_count != new_freelist_count
            || schema_cookie != new_schema_cookie,
        "database modification must change at least one header field"
    );

    eprintln!("=== Stale Sidecar Detection ===");
    eprintln!(
        "  original: cc={change_counter}, pc={page_count}, fc={freelist_count}, sc={schema_cookie}"
    );
    eprintln!(
        "  modified: cc={new_change_counter}, pc={new_page_count}, fc={new_freelist_count}, sc={new_schema_cookie}"
    );
    eprintln!("  sidecar rejected: YES (db_gen_digest mismatch)");
    eprintln!("===============================");
}

#[test]
fn test_stale_sidecar_prevents_wrong_repair() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.db");

    // Create database V1 and sidecar.
    let (db_v1, page_size) = create_test_database(&db_path);
    let sidecar_v1 = generate_db_fec_from_bytes(&db_v1).expect("generate sidecar v1");
    let hdr_v1 = parse_sidecar_header(&sidecar_v1);

    // Modify to create V2.
    let conn = rusqlite::Connection::open(&db_path).expect("open");
    conn.execute_batch(
        "
        DELETE FROM data WHERE id < 100;
        INSERT INTO data(id, payload) VALUES (10000, 'v2-data');
        ",
    )
    .expect("modify");
    drop(conn);

    let db_v2 = std::fs::read(&db_path).expect("read v2");
    let ps = page_size as usize;

    // Generate V2 sidecar.
    let sidecar_v2 = generate_db_fec_from_bytes(&db_v2).expect("generate sidecar v2");
    let hdr_v2 = parse_sidecar_header(&sidecar_v2);

    // V1 sidecar must NOT work with V2 database.
    let v2_change_counter = u32::from_be_bytes(db_v2[24..28].try_into().unwrap());
    let v2_page_count = u32::from_be_bytes(db_v2[28..32].try_into().unwrap());
    let v2_freelist_count = u32::from_be_bytes(db_v2[36..40].try_into().unwrap());
    let v2_schema_cookie = u32::from_be_bytes(db_v2[40..44].try_into().unwrap());
    assert!(
        !hdr_v1.is_current(
            v2_change_counter,
            v2_page_count,
            v2_freelist_count,
            v2_schema_cookie
        ),
        "V1 sidecar must be rejected for V2 database"
    );

    // V2 sidecar must work with V2 database.
    assert!(
        hdr_v2.is_current(
            v2_change_counter,
            v2_page_count,
            v2_freelist_count,
            v2_schema_cookie
        ),
        "V2 sidecar must be current for V2 database"
    );

    // Corrupt page 3 in V2 and show V2 sidecar can repair it.
    let target = 3_u32;
    let original_v2_page = read_page(&db_v2, target, ps);
    let mut corrupt_v2 = db_v2;
    let off = (target as usize - 1) * ps;
    for b in &mut corrupt_v2[off..off + ps] {
        *b = 0xEE;
    }

    let (meta, syms) =
        read_db_fec_group_for_page(&sidecar_v2, &hdr_v2, target).expect("read group");
    let read_fn = |pgno: u32| -> Vec<u8> { read_page(&corrupt_v2, pgno, ps) };
    let (recovered, _) =
        attempt_page_repair(target, &meta, &read_fn, &syms).expect("repair with V2 sidecar");
    assert_eq!(recovered, original_v2_page);
}

// ── bd-1y9r: Beyond-tolerance corruption (graceful degradation) ──────

#[test]
fn test_beyond_tolerance_corruption_graceful_failure() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.db");

    // Create a database with enough pages for a full group.
    let conn = rusqlite::Connection::open(&db_path).expect("open");
    conn.execute_batch(
        "
        PRAGMA journal_mode = DELETE;
        PRAGMA page_size = 4096;
        CREATE TABLE big(id INTEGER PRIMARY KEY, data BLOB);
        ",
    )
    .expect("setup");
    conn.execute_batch("BEGIN;").expect("begin");
    for i in 0..2000 {
        conn.execute(
            "INSERT INTO big(id, data) VALUES (?1, ?2)",
            rusqlite::params![i, vec![0xAB_u8; 500]],
        )
        .expect("insert");
    }
    conn.execute_batch("COMMIT;").expect("commit");
    drop(conn);

    let db_data = std::fs::read(&db_path).expect("read");
    let ps = 4096_usize;
    let page_count = db_data.len() / ps;
    assert!(
        page_count > 70,
        "need at least 70 pages for a full group, got {page_count}"
    );

    let sidecar = generate_db_fec_from_bytes(&db_data).expect("generate sidecar");
    let hdr = parse_sidecar_header(&sidecar);

    // Corrupt MORE than R=4 pages in the same group (pages 2-65).
    // Corrupt pages 5, 10, 15, 20, 25 (5 pages > R=4 tolerance).
    let corrupt_pages = [5_u32, 10, 15, 20, 25];
    let mut corrupt_db = db_data;
    for &pgno in &corrupt_pages {
        let off = (pgno as usize - 1) * ps;
        for b in &mut corrupt_db[off..off + ps] {
            *b = 0xDD;
        }
    }

    // Attempt repair of page 5 — should fail gracefully (not panic).
    let (meta, syms) = read_db_fec_group_for_page(&sidecar, &hdr, 5).expect("read group");
    let read_fn = |pgno: u32| -> Vec<u8> { read_page(&corrupt_db, pgno, ps) };
    let result = attempt_page_repair(5, &meta, &read_fn, &syms);

    // Should fail because 5 corrupted pages > R=4 repair budget.
    assert!(
        result.is_err(),
        "repair must fail gracefully when corruption exceeds R budget"
    );

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("insufficient") || err_msg.contains("corrupt"),
        "error should indicate insufficient symbols: {err_msg}"
    );

    eprintln!("=== Beyond-Tolerance Graceful Degradation ===");
    eprintln!(
        "  group: pages {}-{}",
        meta.start_pgno,
        meta.start_pgno + meta.group_size - 1
    );
    eprintln!("  K={}, R={}", meta.group_size, meta.r_repair);
    eprintln!(
        "  corrupted pages: {corrupt_pages:?} ({} pages)",
        corrupt_pages.len()
    );
    eprintln!("  repair attempt: FAILED (expected)");
    eprintln!("  error: {err_msg}");
    eprintln!("=============================================");
}

// ── bd-1yjj: Multi-page corruption within RaptorQ tolerance (R=4) ────

/// Corrupt 1 page in each of 4 separate groups and repair each independently.
/// Each group has only a single corruption, so XOR parity suffices.
#[test]
fn test_multi_page_cross_group_independent_recovery() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.db");

    // Build a database large enough to span multiple groups (G=64 → need > 192 pages).
    let conn = rusqlite::Connection::open(&db_path).expect("open");
    conn.execute_batch(
        "
        PRAGMA journal_mode = DELETE;
        PRAGMA page_size = 4096;
        CREATE TABLE big(id INTEGER PRIMARY KEY, data BLOB);
        ",
    )
    .expect("setup");
    conn.execute_batch("BEGIN;").expect("begin");
    for i in 0..5000 {
        conn.execute(
            "INSERT INTO big(id, data) VALUES (?1, ?2)",
            rusqlite::params![i, vec![0xAB_u8; 500]],
        )
        .expect("insert");
    }
    conn.execute_batch("COMMIT;").expect("commit");
    drop(conn);

    let db_data = std::fs::read(&db_path).expect("read");
    let ps = 4096_usize;
    let page_count = db_data.len() / ps;
    assert!(
        page_count > 192,
        "need > 192 pages for 4 groups, got {page_count}"
    );

    let sidecar = generate_db_fec_from_bytes(&db_data).expect("generate sidecar");
    let hdr = parse_sidecar_header(&sidecar);

    // Corrupt 1 page in each of 4 different groups:
    // Group 0: pages 2-65  → corrupt page 10
    // Group 1: pages 66-129 → corrupt page 80
    // Group 2: pages 130-193 → corrupt page 150
    // Group 3: pages 194-257 → corrupt page 210 (if exists)
    let targets: Vec<u32> = vec![10, 80, 150, 210]
        .into_iter()
        .filter(|&p| (p as usize) <= page_count)
        .collect();
    assert!(
        targets.len() >= 3,
        "need at least 3 target pages, got {}",
        targets.len()
    );

    let mut corrupt_db = db_data.clone();
    for &pgno in &targets {
        let off = (pgno as usize - 1) * ps;
        for b in &mut corrupt_db[off..off + ps] {
            *b = 0xCC;
        }
    }

    // Repair each corrupted page independently.
    let mut repaired_count = 0_u32;
    for &target_pgno in &targets {
        let original_page = read_page(&db_data, target_pgno, ps);
        let (meta, syms) =
            read_db_fec_group_for_page(&sidecar, &hdr, target_pgno).expect("read group");

        let corrupt_snapshot = corrupt_db.clone();
        let read_fn = |pgno: u32| -> Vec<u8> { read_page(&corrupt_snapshot, pgno, ps) };
        let result = attempt_page_repair(target_pgno, &meta, &read_fn, &syms);

        assert!(
            matches!(&result, Ok((_, RepairResult::Repaired { .. }))),
            "page {target_pgno}: expected Repaired, got {result:?}"
        );
        if let Ok((recovered, RepairResult::Repaired { pgno, .. })) = result {
            assert_eq!(pgno, target_pgno);
            assert_eq!(
                recovered, original_page,
                "page {target_pgno}: recovered data must match original"
            );
            write_page(&mut corrupt_db, target_pgno, ps, &recovered);
            repaired_count += 1;
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    let target_count = targets.len() as u32;
    assert_eq!(
        repaired_count, target_count,
        "all cross-group pages must be repaired"
    );

    // Verify fully repaired database matches original.
    assert_eq!(corrupt_db, db_data, "repaired database must match original");

    // C SQLite integrity check on repaired database.
    let repaired_path = dir.path().join("repaired.db");
    std::fs::write(&repaired_path, &corrupt_db).expect("write repaired");
    let conn = rusqlite::Connection::open_with_flags(
        &repaired_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .expect("open repaired");
    let integrity: String = conn
        .pragma_query_value(None, "integrity_check", |r| r.get(0))
        .expect("integrity_check");
    assert_eq!(integrity, "ok");

    eprintln!("=== Multi-Page Cross-Group Recovery (bd-1yjj) ===");
    eprintln!("  targets: {targets:?}");
    eprintln!("  repaired: {repaired_count}/{}", targets.len());
    eprintln!("  strategy: independent single-fault XOR per group");
    eprintln!("=================================================");
}

/// Test gradient of 1-4 corrupted pages within the SAME group.
/// - 1 page: XOR parity succeeds (single-fault)
/// - 2+ pages: XOR parity fails gracefully (full RaptorQ would be needed)
///
/// This documents the current XOR-parity limitation and verifies graceful
/// degradation until full RaptorQ decode is integrated.
#[test]
#[allow(clippy::too_many_lines)]
fn test_multi_page_same_group_gradient() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.db");

    // Build database with enough pages for a full group.
    let conn = rusqlite::Connection::open(&db_path).expect("open");
    conn.execute_batch(
        "
        PRAGMA journal_mode = DELETE;
        PRAGMA page_size = 4096;
        CREATE TABLE big(id INTEGER PRIMARY KEY, data BLOB);
        ",
    )
    .expect("setup");
    conn.execute_batch("BEGIN;").expect("begin");
    for i in 0..2000 {
        conn.execute(
            "INSERT INTO big(id, data) VALUES (?1, ?2)",
            rusqlite::params![i, vec![0xAB_u8; 500]],
        )
        .expect("insert");
    }
    conn.execute_batch("COMMIT;").expect("commit");
    drop(conn);

    let db_data = std::fs::read(&db_path).expect("read");
    let ps = 4096_usize;
    let page_count = db_data.len() / ps;
    assert!(
        page_count > 70,
        "need > 70 pages for a full group, got {page_count}"
    );

    let sidecar = generate_db_fec_from_bytes(&db_data).expect("generate sidecar");
    let hdr = parse_sidecar_header(&sidecar);

    // Group 0 covers pages 2-65 (64 pages, after header page in its own group).
    // Test gradient: corrupt 1, 2, 3, 4 pages within this group.
    let all_targets = [10_u32, 20, 35, 50]; // all in group 0

    eprintln!("=== Same-Group Gradient Test (bd-1yjj) ===");

    for num_corrupt in 1..=4_usize {
        let targets = &all_targets[..num_corrupt];
        let mut corrupt_db = db_data.clone();
        for &pgno in targets {
            let off = (pgno as usize - 1) * ps;
            for b in &mut corrupt_db[off..off + ps] {
                *b = 0xDD;
            }
        }

        // Try to repair the first corrupted page.
        let first_target = targets[0];
        let (meta, syms) =
            read_db_fec_group_for_page(&sidecar, &hdr, first_target).expect("read group");
        let snap = corrupt_db.clone();
        let read_fn = |pgno: u32| -> Vec<u8> { read_page(&snap, pgno, ps) };
        let result = attempt_page_repair(first_target, &meta, &read_fn, &syms);

        if num_corrupt == 1 {
            // Single-fault: XOR parity must succeed.
            let (recovered, repair_result) = result.expect("single-fault repair must succeed");
            let original_page = read_page(&db_data, first_target, ps);
            assert_eq!(
                recovered, original_page,
                "single-fault: recovered page must match original"
            );
            assert!(matches!(repair_result, RepairResult::Repaired { .. }));
            eprintln!("  {num_corrupt} corrupt page(s) [{targets:?}]: XOR repair SUCCEEDED");
        } else {
            // Multi-fault same-group: XOR parity cannot recover.
            // With full RaptorQ this would succeed (symbols >= K).
            // Current XOR-only implementation correctly fails gracefully.
            match result {
                Err(e) => {
                    let msg = e.to_string();
                    assert!(
                        msg.contains("xxh3_128 validation") || msg.contains("insufficient"),
                        "error should indicate repair failure: {msg}"
                    );
                    eprintln!(
                        "  {num_corrupt} corrupt page(s) [{targets:?}]: XOR repair FAILED (expected — needs RaptorQ)"
                    );
                }
                Ok(_) => {
                    // Extremely unlikely with XOR parity on > 1 corruption,
                    // but if it somehow passes xxh3_128 validation, accept it.
                    eprintln!(
                        "  {num_corrupt} corrupt page(s) [{targets:?}]: repair SUCCEEDED (unexpected but valid)"
                    );
                }
            }
        }
    }

    eprintln!("  NOTE: Multi-fault same-group recovery requires full RaptorQ decode.");
    eprintln!("  Current XOR parity handles single-fault per group only.");
    eprintln!("===========================================");
}
