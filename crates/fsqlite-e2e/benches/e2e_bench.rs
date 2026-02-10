use criterion::{Criterion, criterion_group, criterion_main};

fn bench_csqlite_insert(c: &mut Criterion) {
    c.bench_function("csqlite_insert_100", |b| {
        b.iter(|| {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
                .unwrap();
            for i in 0..100 {
                conn.execute("INSERT INTO t VALUES (?1, ?2)", rusqlite::params![i, "val"])
                    .unwrap();
            }
        });
    });
}

criterion_group!(benches, bench_csqlite_insert);
criterion_main!(benches);
