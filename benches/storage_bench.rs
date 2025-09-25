use std::fmt::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use quill_sql::database::Database;
use quill_sql::error::QuillSQLResult;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

const ROWS: i64 = 50_000;
const INSERT_BATCH: i64 = 200;
const POINT_SAMPLES: usize = 5_000;

fn prepare_dataset() -> Database {
    let mut db = Database::new_temp().expect("temp db");
    db.run("CREATE TABLE bench(id BIGINT NOT NULL, val BIGINT)")
        .expect("create table");
    db.run("CREATE INDEX idx_bench_id ON bench(id)")
        .expect("create index");
    bulk_insert(&mut db, 0, ROWS, INSERT_BATCH).expect("bulk insert");
    db.flush().expect("flush");
    db
}

fn bulk_insert(db: &mut Database, start: i64, total: i64, batch: i64) -> QuillSQLResult<()> {
    let mut current = start;
    let end = start + total;
    while current < end {
        let mut stmt = String::from("INSERT INTO bench(id, val) VALUES ");
        let mut local = 0;
        while local < batch && current < end {
            if local > 0 {
                stmt.push_str(", ");
            }
            write!(&mut stmt, "({},{})", current, current % 10_000).unwrap();
            current += 1;
            local += 1;
        }
        db.run(&stmt)?;
    }
    Ok(())
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");
    group.throughput(Throughput::Elements((INSERT_BATCH * 50) as u64));
    group.bench_function("insert_10k", |b| {
        b.iter_batched(
            || {
                let mut db = Database::new_temp().expect("temp db");
                db.run("CREATE TABLE bench(id BIGINT NOT NULL, val BIGINT)")
                    .expect("create table");
                db
            },
            |mut db| {
                bulk_insert(&mut db, 0, 10_000, INSERT_BATCH).unwrap();
                black_box(db.flush().unwrap());
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn bench_seq_scan(c: &mut Criterion) {
    let mut db = prepare_dataset();
    let mut group = c.benchmark_group("scan");
    group.throughput(Throughput::Elements(ROWS as u64));

    group.bench_function("seq_full", |b| {
        b.iter(|| {
            let res = db.run("SELECT COUNT(*) FROM bench").expect("seq scan");
            black_box(res);
        });
    });

    let mut range_sql = Vec::with_capacity(POINT_SAMPLES);
    let mut rng = ChaCha8Rng::seed_from_u64(42);
    let mut ids: Vec<i64> = (0..ROWS).collect();
    ids.shuffle(&mut rng);
    for chunk in ids.chunks((ROWS as usize / POINT_SAMPLES.max(1)).max(1)) {
        if let Some(&id) = chunk.first() {
            range_sql.push(format!(
                "SELECT COUNT(*) FROM bench WHERE id >= {} AND id <= {}",
                id,
                id + 200
            ));
        }
        if range_sql.len() == POINT_SAMPLES {
            break;
        }
    }
    let mut idx = 0usize;
    group.bench_function("seq_range", |b| {
        b.iter(|| {
            let sql = &range_sql[idx % range_sql.len()];
            idx = (idx + 1) % range_sql.len();
            let res = db.run(sql).expect("seq range");
            black_box(res);
        });
    });

    group.finish();
}

fn bench_index_scan(c: &mut Criterion) {
    let mut db = prepare_dataset();
    let mut ids: Vec<i64> = (0..ROWS).collect();
    ids.shuffle(&mut ChaCha8Rng::seed_from_u64(123));
    let queries: Vec<String> = ids
        .iter()
        .take(POINT_SAMPLES)
        .map(|id| format!("SELECT val FROM bench WHERE id = {}", id))
        .collect();
    let q_index = AtomicUsize::new(0);

    let mut group = c.benchmark_group("index");
    group.throughput(Throughput::Elements(POINT_SAMPLES as u64));
    group.bench_function("index_point", |b| {
        b.iter(|| {
            let idx = q_index.fetch_add(1, Ordering::Relaxed) % queries.len();
            let res = db.run(&queries[idx]).expect("index point");
            black_box(res);
        });
    });

    let mut ranges = Vec::with_capacity(POINT_SAMPLES);
    for window in ids.chunks(4) {
        if let (Some(&start), Some(&end)) = (window.first(), window.last()) {
            ranges.push(format!(
                "SELECT COUNT(*) FROM bench WHERE id >= {} AND id <= {}",
                start, end
            ));
        }
        if ranges.len() == POINT_SAMPLES {
            break;
        }
    }
    let mut range_idx = 0usize;
    group.bench_function("index_range", |b| {
        b.iter(|| {
            let sql = &ranges[range_idx % ranges.len()];
            range_idx = (range_idx + 1) % ranges.len();
            let res = db.run(sql).expect("index range");
            black_box(res);
        });
    });

    group.finish();
}

fn bench_all(c: &mut Criterion) {
    bench_insert(c);
    bench_seq_scan(c);
    bench_index_scan(c);
}

criterion_group!(name = benches; config = Criterion::default().sample_size(30); targets = bench_all);
criterion_main!(benches);
