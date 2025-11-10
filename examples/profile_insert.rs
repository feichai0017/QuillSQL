use pprof::ProfilerGuard;
use quill_sql::database::{Database, DatabaseOptions, WalOptions};
use quill_sql::error::QuillSQLResult;
use quill_sql::session::SessionContext;
use std::fs::{self, File};
use std::time::Instant;

const TABLE: &str = "bench_profile";
const ROWS: i64 = 50_000;
const BATCH: i64 = 200;

fn quill_options() -> DatabaseOptions {
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

fn bulk_insert(db: &mut Database, start: i64, total: i64, batch: i64) -> QuillSQLResult<()> {
    let mut session = SessionContext::new(db.default_isolation());
    db.run_with_session(&mut session, "BEGIN")?;
    let mut current = start;
    let end = start + total;
    while current < end {
        let mut stmt = String::from("INSERT INTO bench_profile(id, val) VALUES ");
        let mut local = 0;
        while local < batch && current < end {
            if local > 0 {
                stmt.push_str(", ");
            }
            stmt.push_str(&format!("({},{})", current, current % 10_000));
            current += 1;
            local += 1;
        }
        db.run_with_session(&mut session, &stmt)?;
    }
    db.run_with_session(&mut session, "COMMIT")?;
    Ok(())
}

fn main() -> QuillSQLResult<()> {
    let mut db = Database::new_temp_with_options(quill_options())?;
    db.run(&format!(
        "CREATE TABLE {}(id BIGINT NOT NULL, val BIGINT)",
        TABLE
    ))?;
    db.run(&format!("CREATE INDEX idx_{}_id ON {}(id)", TABLE, TABLE))?;

    let guard = ProfilerGuard::new(100).unwrap();
    let start = Instant::now();
    bulk_insert(&mut db, 0, ROWS, BATCH)?;
    db.flush()?;
    println!("bulk insert elapsed: {:.3}s", start.elapsed().as_secs_f64());

    if let Ok(report) = guard.report().build() {
        let dir = "target/profile";
        fs::create_dir_all(dir)?;
        let flame_path = format!("{dir}/insert_flamegraph.svg");
        let mut file = File::create(&flame_path)?;
        report
            .flamegraph(&mut file)
            .expect("failed to write flamegraph");
        println!("flamegraph written to {}", flame_path);
    }
    Ok(())
}
