use std::fs;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tempfile::TempDir;

use crate::buffer::{AtomicPageId, BufferManager};
use crate::catalog::{Column, DataType, Schema, SchemaRef};
use crate::config::WalConfig;
use crate::error::QuillSQLResult;
use crate::recovery::control_file::ControlFileManager;
use crate::recovery::{RecoveryManager, WalManager};
use crate::storage::disk_manager::DiskManager;
use crate::storage::disk_scheduler::DiskScheduler;
use crate::storage::heap::table_heap::TableHeap;
use crate::storage::heap::MvccHeap;
use crate::storage::tuple::Tuple;
use crate::transaction::{CommandId, TransactionId};
use crate::utils::scalar::ScalarValue;

fn wal_config(dir: &Path, segment_size: u64) -> WalConfig {
    WalConfig {
        directory: dir.to_path_buf(),
        segment_size,
        sync_on_flush: false,
        persist_control_file_on_flush: false,
        writer_interval_ms: None,
        buffer_capacity: 1_024,
        flush_coalesce_bytes: 64 * 1_024,
        synchronous_commit: false,
        checkpoint_interval_ms: None,
        retain_segments: 8,
    }
}

fn build_runtime(
    db_path: &Path,
    cfg: &WalConfig,
) -> (Arc<BufferManager>, Arc<WalManager>, Arc<DiskScheduler>) {
    fs::create_dir_all(&cfg.directory).expect("wal dir");
    let disk_manager = Arc::new(DiskManager::try_new(db_path).expect("disk manager"));
    let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager.clone()));
    let buffer_pool = Arc::new(BufferManager::new(512, disk_scheduler.clone()));
    let (control_file, init_state) =
        ControlFileManager::load_or_init(&cfg.directory, cfg.segment_size).expect("control file");
    let control_file = Arc::new(control_file);
    let wal_manager = Arc::new(
        WalManager::new_with_scheduler(
            cfg.clone(),
            Some(init_state),
            Some(control_file),
            disk_scheduler.clone(),
        )
        .expect("wal manager"),
    );
    buffer_pool.set_wal_manager(wal_manager.clone());
    (buffer_pool, wal_manager, disk_scheduler)
}

fn insert_rows(heap: Arc<TableHeap>, schema: &SchemaRef, rows: usize) {
    let mvcc = MvccHeap::new(heap.clone());
    for i in 0..rows {
        let tuple = Tuple::new(
            schema.clone(),
            vec![
                ScalarValue::Int32(Some(i as i32)),
                ScalarValue::Int32(Some((i * 2) as i32)),
            ],
        );
        let _ = mvcc
            .insert(&tuple, TransactionId::default(), CommandId::default())
            .expect("insert tuple");
    }
}

fn count_rows(heap: &TableHeap) -> QuillSQLResult<usize> {
    let mut count = 0;
    let mut rid = heap.get_first_rid()?;
    while let Some(current) = rid {
        count += 1;
        rid = heap.get_next_rid(current)?;
    }
    Ok(count)
}

#[test]
fn wal_force_flush_recovers_heap() {
    let temp = TempDir::new().expect("tempdir");
    let db_path = temp.path().join("heap_force.db");
    let wal_path = temp.path().join("wal_force");
    let cfg = wal_config(&wal_path, 16 * 1024);

    let schema = Arc::new(Schema::new(vec![
        Column::new("id", DataType::Int32, false),
        Column::new("val", DataType::Int32, false),
    ]));

    let (first_page, last_page) = {
        let (buffer, wal_manager, _scheduler) = build_runtime(&db_path, &cfg);
        let heap = Arc::new(TableHeap::try_new(schema.clone(), buffer.clone()).expect("heap"));
        insert_rows(heap.clone(), &schema, 128);
        buffer.flush_all_pages().expect("flush heap pages");
        let target = wal_manager.max_assigned_lsn();
        wal_manager.flush_until(target).expect("force wal flush");
        (
            heap.first_page_id.load(Ordering::SeqCst),
            heap.last_page_id.load(Ordering::SeqCst),
        )
    };

    let (buffer, wal_manager, scheduler) = build_runtime(&db_path, &cfg);
    let summary = RecoveryManager::new(wal_manager.clone(), scheduler.clone())
        .with_buffer_pool(buffer.clone())
        .replay()
        .expect("replay");
    assert!(summary.redo_count > 0);

    let reopened_heap = TableHeap {
        schema: schema.clone(),
        buffer_pool: buffer.clone(),
        first_page_id: AtomicPageId::new(first_page),
        last_page_id: AtomicPageId::new(last_page),
    };
    let count = count_rows(&reopened_heap).expect("row count");
    assert_eq!(count, 128);
}

#[test]
fn wal_redo_handles_segment_rotation() {
    let temp = TempDir::new().expect("tempdir");
    let db_path = temp.path().join("heap_rotate.db");
    let wal_path = temp.path().join("wal_rotate");
    let cfg = wal_config(&wal_path, 4 * 1024);

    let schema = Arc::new(Schema::new(vec![
        Column::new("id", DataType::Int32, false),
        Column::new("val", DataType::Int32, false),
    ]));

    let (first_page, last_page) = {
        let (buffer, wal_manager, _scheduler) = build_runtime(&db_path, &cfg);
        let heap = Arc::new(TableHeap::try_new(schema.clone(), buffer.clone()).expect("heap"));
        insert_rows(heap.clone(), &schema, 512);
        buffer.flush_all_pages().expect("flush heap pages");
        let target = wal_manager.max_assigned_lsn();
        wal_manager.flush_until(target).expect("wal flush");
        (
            heap.first_page_id.load(Ordering::SeqCst),
            heap.last_page_id.load(Ordering::SeqCst),
        )
    };

    let wal_files: Vec<_> = fs::read_dir(&cfg.directory)
        .expect("wal dir")
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("wal_") && name.ends_with(".log") {
                Some(entry.path())
            } else {
                None
            }
        })
        .collect();
    assert!(
        wal_files.len() > 1,
        "expected multiple WAL segments, found {}",
        wal_files.len()
    );

    let (buffer, wal_manager, scheduler) = build_runtime(&db_path, &cfg);
    let summary = RecoveryManager::new(wal_manager.clone(), scheduler.clone())
        .with_buffer_pool(buffer.clone())
        .replay()
        .expect("replay");
    assert!(summary.redo_count > 0);

    let reopened_heap = TableHeap {
        schema,
        buffer_pool: buffer.clone(),
        first_page_id: AtomicPageId::new(first_page),
        last_page_id: AtomicPageId::new(last_page),
    };
    let count = count_rows(&reopened_heap).expect("row count");
    assert_eq!(count, 512);
}
