use std::fs::{self, File};
use std::time::Instant;

use pprof::ProfilerGuard;
use quill_sql::database::{Database, DatabaseOptions, WalOptions};
use quill_sql::error::QuillSQLResult;
use quill_sql::session::SessionContext;
use rand::{rng, Rng};

const TABLE: &str = "bench_profile";
const ROWS: i64 = 50_000;
const BATCH: i64 = 200;
const PROFILE_DIR: &str = "target/profile";
const SEQ_SCAN_LOOPS: usize = 10;
const INDEX_POINT_QUERIES: usize = 200;

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
        let mut stmt = format!("INSERT INTO {}(id, val) VALUES ", TABLE);
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

fn profile_section<F>(label: &str, mut action: F) -> QuillSQLResult<()>
where
    F: FnMut() -> QuillSQLResult<()>,
{
    let guard = ProfilerGuard::new(100)
        .map_err(|e| quill_sql::error::QuillSQLError::Internal(e.to_string()))?;
    let start = Instant::now();
    action()?;
    println!("{} elapsed: {:.3}s", label, start.elapsed().as_secs_f64());
    if let Ok(report) = guard.report().build() {
        fs::create_dir_all(PROFILE_DIR)?;
        let flame_path = format!("{PROFILE_DIR}/{label}_flamegraph.svg");
        let mut file = File::create(&flame_path)?;
        report
            .flamegraph(&mut file)
            .expect("failed to write flamegraph");
        println!("flamegraph written to {}", flame_path);
    }
    Ok(())
}

fn seq_scan_workload(db: &mut Database) -> QuillSQLResult<()> {
    for _ in 0..SEQ_SCAN_LOOPS {
        db.run(&format!("SELECT COUNT(*) FROM {}", TABLE))?;
    }
    Ok(())
}

fn index_point_workload(db: &mut Database) -> QuillSQLResult<()> {
    let mut rng = rng();
    for _ in 0..INDEX_POINT_QUERIES {
        let id = rng.random_range(0..ROWS);
        db.run(&format!("SELECT val FROM {} WHERE id = {}", TABLE, id))?;
    }
    Ok(())
}

fn update_workload(db: &mut Database) -> QuillSQLResult<()> {
    let mut rng = rng();
    for _ in 0..500 {
        let id = rng.random_range(0..ROWS);
        let new_val = rng.random_range(0..100_000);
        db.run(&format!(
            "UPDATE {} SET val = {} WHERE id = {}",
            TABLE, new_val, id
        ))?;
    }
    Ok(())
}

fn delete_workload(db: &mut Database) -> QuillSQLResult<()> {
    let mut rng = rng();
    for _ in 0..200 {
        let id = rng.random_range(0..ROWS);
        db.run(&format!("DELETE FROM {} WHERE id = {}", TABLE, id))?;
    }
    // replenish deleted rows to keep table roughly stable
    bulk_insert(db, ROWS, ROWS / 50, BATCH)?;
    Ok(())
}

fn main() -> QuillSQLResult<()> {
    let mut db = Database::new_temp_with_options(quill_options())?;
    db.run(&format!(
        "CREATE TABLE {}(id BIGINT NOT NULL, val BIGINT)",
        TABLE
    ))?;
    db.run(&format!("CREATE INDEX idx_{}_id ON {}(id)", TABLE, TABLE))?;

    profile_section("insert", || {
        bulk_insert(&mut db, 0, ROWS, BATCH)?;
        db.flush()?;
        Ok(())
    })?;

    profile_section("seq_scan", || seq_scan_workload(&mut db))?;
    profile_section("index_point", || index_point_workload(&mut db))?;
    profile_section("updates", || update_workload(&mut db))?;
    profile_section("deletes", || {
        delete_workload(&mut db)?;
        db.flush()?;
        Ok(())
    })?;

    Ok(())
}
