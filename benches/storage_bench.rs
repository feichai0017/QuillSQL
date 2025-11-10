use std::cell::{Cell, RefCell};
use std::fmt::Write as _; // for INSERT statement construction
use std::fs;
use std::path::Path;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use postgres::{Client, NoTls};
use pprof::criterion::{Output, PProfProfiler};
use quill_sql::database::{Database, DatabaseOptions, WalOptions};
use quill_sql::error::QuillSQLResult;
use quill_sql::session::SessionContext;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use rusqlite::Connection;

const ROWS: i64 = 50_000;
const INSERT_BATCH: i64 = 200;
const POINT_SAMPLES: usize = 5_000;
const RANGE_WINDOW: i64 = 200;
const INDEX_RANGE_WIDTH: usize = 4;

const DB_DIR_NAME: &str = "quill_bench_data";

const QUILL_TABLE: &str = "bench";
const SQLITE_TABLE: &str = "bench";
const PG_INSERT_TABLE: &str = "bench_insert";
const PG_SCAN_TABLE: &str = "bench_scan";
const PG_INDEX_TABLE: &str = "bench_index";

fn postgres_url() -> Option<String> {
    std::env::var("QUILL_BENCH_POSTGRES_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok()
}

fn quill_bench_options() -> DatabaseOptions {
    DatabaseOptions {
        wal: WalOptions {
            writer_interval_ms: None,
            synchronous_commit: Some(false),
            sync_on_flush: Some(false),
            persist_control_file_on_flush: Some(false),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn insert_batches<E, F>(
    table: &str,
    start: i64,
    total: i64,
    batch: i64,
    mut exec: F,
) -> Result<(), E>
where
    F: FnMut(&str) -> Result<(), E>,
{
    let mut current = start;
    let end = start + total;
    while current < end {
        let mut stmt = String::new();
        write!(&mut stmt, "INSERT INTO {}(id, val) VALUES ", table).unwrap();
        let mut local = 0;
        while local < batch && current < end {
            if local > 0 {
                stmt.push_str(", ");
            }
            write!(&mut stmt, "({},{})", current, current % 10_000).unwrap();
            current += 1;
            local += 1;
        }
        exec(&stmt)?;
    }
    Ok(())
}

fn prepare_dataset(db_path: &Path) -> Database {
    let mut db =
        Database::new_on_disk_with_options(db_path.to_str().unwrap(), quill_bench_options())
            .expect("on-disk db");
    db.run(&format!(
        "CREATE TABLE {}(id BIGINT NOT NULL, val BIGINT)",
        QUILL_TABLE
    ))
    .expect("create table");
    db.run(&format!(
        "CREATE INDEX idx_{}_id ON {}(id)",
        QUILL_TABLE, QUILL_TABLE
    ))
    .expect("create index");
    bulk_insert(&mut db, 0, ROWS, INSERT_BATCH).expect("bulk insert");
    db.flush().expect("flush");
    db
}

fn bulk_insert(db: &mut Database, start: i64, total: i64, batch: i64) -> QuillSQLResult<()> {
    let mut session = SessionContext::new(db.default_isolation());
    db.run_with_session(&mut session, "BEGIN")?;
    let result = insert_batches(QUILL_TABLE, start, total, batch, |sql| {
        db.run_with_session(&mut session, sql)?;
        Ok(())
    });
    match result {
        Ok(()) => db.run_with_session(&mut session, "COMMIT")?,
        Err(err) => {
            let _ = db.run_with_session(&mut session, "ROLLBACK");
            return Err(err);
        }
    };
    Ok(())
}

fn sqlite_empty_table() -> Connection {
    let conn = Connection::open_in_memory().expect("sqlite temp db");
    conn.execute(
        &format!(
            "CREATE TABLE {}(id INTEGER NOT NULL, val INTEGER)",
            SQLITE_TABLE
        ),
        [],
    )
    .expect("sqlite create table");
    conn
}

fn bulk_insert_sqlite(
    conn: &Connection,
    table: &str,
    start: i64,
    total: i64,
    batch: i64,
) -> rusqlite::Result<()> {
    insert_batches(table, start, total, batch, |sql| {
        conn.execute(sql, [])?;
        Ok(())
    })
}

fn prepare_sqlite_dataset(with_index: bool) -> Connection {
    let conn = sqlite_empty_table();
    if with_index {
        conn.execute(
            &format!(
                "CREATE INDEX idx_{}_id ON {}(id)",
                SQLITE_TABLE, SQLITE_TABLE
            ),
            [],
        )
        .expect("sqlite create index");
    }
    bulk_insert_sqlite(&conn, SQLITE_TABLE, 0, ROWS, INSERT_BATCH).expect("sqlite bulk insert");
    conn
}

fn reset_postgres_table(
    client: &mut Client,
    table: &str,
    create_index: bool,
) -> Result<(), postgres::Error> {
    client.batch_execute(&format!("DROP TABLE IF EXISTS {}", table))?;
    client.batch_execute(&format!(
        "CREATE TABLE {}(id BIGINT NOT NULL, val BIGINT)",
        table
    ))?;
    if create_index {
        client.batch_execute(&format!("CREATE INDEX idx_{}_id ON {}(id)", table, table))?;
    }
    Ok(())
}

fn bulk_insert_postgres(
    client: &mut Client,
    table: &str,
    start: i64,
    total: i64,
    batch: i64,
) -> Result<(), postgres::Error> {
    insert_batches(table, start, total, batch, |sql| {
        client.execute(sql, &[])?;
        Ok(())
    })
}

fn prepare_postgres_dataset(
    url: &str,
    table: &str,
    create_index: bool,
) -> Result<Client, postgres::Error> {
    let mut client = Client::connect(url, NoTls)?;
    reset_postgres_table(&mut client, table, create_index)?;
    bulk_insert_postgres(&mut client, table, 0, ROWS, INSERT_BATCH)?;
    client.batch_execute(&format!("ANALYZE {}", table)).ok();
    Ok(client)
}

fn build_seq_range_queries(table: &str) -> Vec<String> {
    let mut queries = Vec::with_capacity(POINT_SAMPLES);
    let mut ids: Vec<i64> = (0..ROWS).collect();
    let mut rng = ChaCha8Rng::seed_from_u64(42);
    ids.shuffle(&mut rng);
    let chunk_size = (ROWS as usize / POINT_SAMPLES.max(1)).max(1);
    for chunk in ids.chunks(chunk_size) {
        if let Some(&id) = chunk.first() {
            queries.push(format!(
                "SELECT COUNT(*) FROM {} WHERE id >= {} AND id <= {}",
                table,
                id,
                id + RANGE_WINDOW
            ));
        }
        if queries.len() == POINT_SAMPLES {
            break;
        }
    }
    queries
}

fn build_index_point_queries(table: &str) -> Vec<String> {
    let mut ids: Vec<i64> = (0..ROWS).collect();
    ids.shuffle(&mut ChaCha8Rng::seed_from_u64(123));
    ids.iter()
        .take(POINT_SAMPLES)
        .map(|id| format!("SELECT val FROM {} WHERE id = {}", table, id))
        .collect()
}

fn build_index_range_queries(table: &str) -> Vec<String> {
    let mut ranges = Vec::with_capacity(POINT_SAMPLES);
    let mut ids: Vec<i64> = (0..ROWS).collect();
    ids.shuffle(&mut ChaCha8Rng::seed_from_u64(123));
    for window in ids.chunks(INDEX_RANGE_WIDTH) {
        if let (Some(&start), Some(&end)) = (window.first(), window.last()) {
            ranges.push(format!(
                "SELECT COUNT(*) FROM {} WHERE id >= {} AND id <= {}",
                table, start, end
            ));
        }
        if ranges.len() == POINT_SAMPLES {
            break;
        }
    }
    ranges
}

fn bench_insert(c: &mut Criterion, db_dir: &Path) {
    let db_path = db_dir.join("insert.db");

    let mut group = c.benchmark_group("insert_10k");
    group.throughput(Throughput::Elements((INSERT_BATCH * 50) as u64));
    let insert_opts = quill_bench_options();
    group.bench_function("quill", move |b| {
        let create_sql = format!(
            "CREATE TABLE {}(id BIGINT NOT NULL, val BIGINT)",
            QUILL_TABLE
        );
        let drop_sql = format!("DROP TABLE IF EXISTS {}", QUILL_TABLE);
        let clear_sql = format!("DELETE FROM {}", QUILL_TABLE);
        let db = RefCell::new({
            let mut db =
                Database::new_on_disk_with_options(db_path.to_str().unwrap(), insert_opts.clone())
                    .expect("on-disk db");
            db.run(&drop_sql).expect("drop table if exists");
            db.run(&create_sql).expect("create table");
            db
        });
        b.iter(|| {
            let mut db = db.borrow_mut();
            db.run(&clear_sql).expect("clear table");
            bulk_insert(&mut db, 0, 10_000, INSERT_BATCH).unwrap();
            black_box(());
        });
    });

    group.bench_function("sqlite", |b| {
        b.iter_batched(
            || sqlite_empty_table(),
            |conn| {
                bulk_insert_sqlite(&conn, SQLITE_TABLE, 0, 10_000, INSERT_BATCH).unwrap();
                black_box(());
            },
            BatchSize::LargeInput,
        );
    });

    if let Some(url) = postgres_url() {
        group.bench_function("postgres", |b| {
            b.iter_batched(
                || {
                    let mut client = Client::connect(&url, NoTls).expect("postgres connect");
                    reset_postgres_table(&mut client, PG_INSERT_TABLE, false)
                        .expect("postgres create table");
                    client
                },
                |mut client| {
                    bulk_insert_postgres(&mut client, PG_INSERT_TABLE, 0, 10_000, INSERT_BATCH)
                        .unwrap();
                    black_box(());
                },
                BatchSize::LargeInput,
            );
        });
    } else {
        eprintln!(
            "skipping postgres insert_10k benchmark: set QUILL_BENCH_POSTGRES_URL or DATABASE_URL"
        );
    }

    group.finish();
}

fn bench_seq_scan(c: &mut Criterion, db_dir: &Path) {
    let db_path = db_dir.join("scan.db");

    let mut group = c.benchmark_group("scan_seq");
    group.throughput(Throughput::Elements(ROWS as u64));

    let mut quill_db = prepare_dataset(&db_path);
    let quill_full_sql = format!("SELECT COUNT(*) FROM {}", QUILL_TABLE);
    group.bench_function("quill_full", |b| {
        b.iter(|| {
            let res = quill_db.run(&quill_full_sql).expect("quill seq scan");
            black_box(res);
        });
    });

    let quill_range_queries = build_seq_range_queries(QUILL_TABLE);
    let quill_range_idx = Cell::new(0usize);
    group.bench_function("quill_range", |b| {
        b.iter(|| {
            let idx = quill_range_idx.get();
            let sql = &quill_range_queries[idx % quill_range_queries.len()];
            quill_range_idx.set((idx + 1) % quill_range_queries.len());
            let res = quill_db.run(sql).expect("quill seq range");
            black_box(res);
        });
    });

    let sqlite_conn = RefCell::new(prepare_sqlite_dataset(true));
    let sqlite_full_sql = format!("SELECT COUNT(*) FROM {}", SQLITE_TABLE);
    group.bench_function("sqlite_full", |b| {
        b.iter(|| {
            let count: i64 = {
                let conn = sqlite_conn.borrow();
                conn.query_row(&sqlite_full_sql, [], |row| row.get(0))
                    .expect("sqlite seq scan")
            };
            black_box(count);
        });
    });

    let sqlite_range_queries = build_seq_range_queries(SQLITE_TABLE);
    let sqlite_range_idx = Cell::new(0usize);
    group.bench_function("sqlite_range", |b| {
        b.iter(|| {
            let idx = sqlite_range_idx.get();
            let sql = &sqlite_range_queries[idx % sqlite_range_queries.len()];
            sqlite_range_idx.set((idx + 1) % sqlite_range_queries.len());
            let count: i64 = {
                let conn = sqlite_conn.borrow();
                conn.query_row(sql, [], |row| row.get(0))
                    .expect("sqlite seq range")
            };
            black_box(count);
        });
    });

    if let Some(url) = postgres_url() {
        match prepare_postgres_dataset(&url, PG_SCAN_TABLE, true) {
            Ok(client) => {
                let pg_client = RefCell::new(client);
                let pg_full_sql = format!("SELECT COUNT(*) FROM {}", PG_SCAN_TABLE);
                group.bench_function("postgres_full", |b| {
                    b.iter(|| {
                        let count: i64 = {
                            let mut client = pg_client.borrow_mut();
                            client
                                .query_one(&pg_full_sql, &[])
                                .expect("postgres seq scan")
                                .get(0)
                        };
                        black_box(count);
                    });
                });

                let pg_range_queries = build_seq_range_queries(PG_SCAN_TABLE);
                let pg_range_idx = Cell::new(0usize);
                group.bench_function("postgres_range", |b| {
                    b.iter(|| {
                        let idx = pg_range_idx.get();
                        let sql = &pg_range_queries[idx % pg_range_queries.len()];
                        pg_range_idx.set((idx + 1) % pg_range_queries.len());
                        let count: i64 = {
                            let mut client = pg_client.borrow_mut();
                            client
                                .query_one(sql, &[])
                                .expect("postgres seq range")
                                .get(0)
                        };
                        black_box(count);
                    });
                });
            }
            Err(err) => {
                eprintln!("skipping postgres scan benchmarks: {err}");
            }
        }
    } else {
        eprintln!(
            "skipping postgres scan benchmarks: set QUILL_BENCH_POSTGRES_URL or DATABASE_URL"
        );
    }

    group.finish();
}

fn bench_index_scan(c: &mut Criterion, db_dir: &Path) {
    let db_path = db_dir.join("index.db");
    let mut db = prepare_dataset(&db_path);
    let mut group = c.benchmark_group("index_scan");
    group.throughput(Throughput::Elements(POINT_SAMPLES as u64));

    let point_queries = build_index_point_queries(QUILL_TABLE);
    let point_idx = Cell::new(0usize);
    group.bench_function("quill_point", |b| {
        b.iter(|| {
            let idx = point_idx.get();
            let sql = &point_queries[idx % point_queries.len()];
            point_idx.set((idx + 1) % point_queries.len());
            let res = db.run(sql).expect("quill index point");
            black_box(res);
        });
    });

    let range_queries = build_index_range_queries(QUILL_TABLE);
    let range_idx = Cell::new(0usize);
    group.bench_function("quill_range", |b| {
        b.iter(|| {
            let idx = range_idx.get();
            let sql = &range_queries[idx % range_queries.len()];
            range_idx.set((idx + 1) % range_queries.len());
            let res = db.run(sql).expect("quill index range");
            black_box(res);
        });
    });

    let sqlite_conn = RefCell::new(prepare_sqlite_dataset(true));
    let sqlite_point_queries = build_index_point_queries(SQLITE_TABLE);
    let sqlite_point_idx = Cell::new(0usize);
    group.bench_function("sqlite_point", |b| {
        b.iter(|| {
            let idx = sqlite_point_idx.get();
            let sql = &sqlite_point_queries[idx % sqlite_point_queries.len()];
            sqlite_point_idx.set((idx + 1) % sqlite_point_queries.len());
            let result: Option<i64> = {
                let conn = sqlite_conn.borrow();
                conn.query_row(sql, [], |row| row.get(0)).ok()
            };
            black_box(result);
        });
    });

    let sqlite_range_queries = build_index_range_queries(SQLITE_TABLE);
    let sqlite_range_idx = Cell::new(0usize);
    group.bench_function("sqlite_range", |b| {
        b.iter(|| {
            let idx = sqlite_range_idx.get();
            let sql = &sqlite_range_queries[idx % sqlite_range_queries.len()];
            sqlite_range_idx.set((idx + 1) % sqlite_range_queries.len());
            let count: i64 = {
                let conn = sqlite_conn.borrow();
                conn.query_row(sql, [], |row| row.get(0))
                    .expect("sqlite index range")
            };
            black_box(count);
        });
    });

    if let Some(url) = postgres_url() {
        match prepare_postgres_dataset(&url, PG_INDEX_TABLE, true) {
            Ok(client) => {
                let pg_client = RefCell::new(client);
                let pg_point_queries = build_index_point_queries(PG_INDEX_TABLE);
                let pg_point_idx = Cell::new(0usize);
                group.bench_function("postgres_point", |b| {
                    b.iter(|| {
                        let idx = pg_point_idx.get();
                        let sql = &pg_point_queries[idx % pg_point_queries.len()];
                        pg_point_idx.set((idx + 1) % pg_point_queries.len());
                        let result: Option<i64> = {
                            let mut client = pg_client.borrow_mut();
                            client
                                .query_opt(sql, &[])
                                .expect("postgres index point")
                                .map(|row| row.get(0))
                        };
                        black_box(result);
                    });
                });

                let pg_range_queries = build_index_range_queries(PG_INDEX_TABLE);
                let pg_range_idx = Cell::new(0usize);
                group.bench_function("postgres_range", |b| {
                    b.iter(|| {
                        let idx = pg_range_idx.get();
                        let sql = &pg_range_queries[idx % pg_range_queries.len()];
                        pg_range_idx.set((idx + 1) % pg_range_queries.len());
                        let count: i64 = {
                            let mut client = pg_client.borrow_mut();
                            client
                                .query_one(sql, &[])
                                .expect("postgres index range")
                                .get(0)
                        };
                        black_box(count);
                    });
                });
            }
            Err(err) => {
                eprintln!("skipping postgres index benchmarks: {err}");
            }
        }
    } else {
        eprintln!(
            "skipping postgres index benchmarks: set QUILL_BENCH_POSTGRES_URL or DATABASE_URL"
        );
    }

    group.finish();
}

fn bench_main(c: &mut Criterion) {
    let bench_dir = std::env::current_dir().unwrap().join(DB_DIR_NAME);

    // Global setup
    fs::remove_dir_all(&bench_dir).ok();
    fs::create_dir_all(&bench_dir).expect("create bench dir");

    bench_insert(c, &bench_dir);
    bench_seq_scan(c, &bench_dir);
    bench_index_scan(c, &bench_dir);

    // Global teardown
    fs::remove_dir_all(&bench_dir).ok();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_main
);
criterion_main!(benches);
