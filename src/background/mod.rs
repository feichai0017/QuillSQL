use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use log::{debug, warn};

use crate::buffer::BufferManager;
use crate::catalog::registry::{global_index_registry, global_table_registry};
use crate::catalog::INFORMATION_SCHEMA_NAME;
use crate::config::IndexVacuumConfig;
use crate::recovery::wal::codec::CheckpointPayload;
use crate::recovery::{WalManager, WalWriterHandle};
use crate::storage::page::{RecordId, TupleMeta};
use crate::storage::table_heap::{TableHeap, TableIterator};
use crate::transaction::{TransactionId, TransactionManager, TransactionStatus};

/// High-level categories of background workers maintained by the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerKind {
    WalWriter,
    Checkpoint,
    BufferPoolWriter,
    MvccVacuum,
}

#[derive(Debug, Clone, Copy)]
pub struct WorkerMetadata {
    pub kind: WorkerKind,
    pub interval: Option<Duration>,
}

pub struct WorkerHandle {
    metadata: WorkerMetadata,
    stop_fn: Option<Box<dyn FnOnce() + Send + 'static>>,
    join_handle: Option<JoinHandle<()>>,
}

impl WorkerHandle {
    pub fn new(
        metadata: WorkerMetadata,
        stop_fn: impl FnOnce() + Send + 'static,
        join_handle: Option<JoinHandle<()>>,
    ) -> Self {
        Self {
            metadata,
            stop_fn: Some(Box::new(stop_fn)),
            join_handle,
        }
    }

    pub fn metadata(&self) -> WorkerMetadata {
        self.metadata
    }

    pub fn shutdown(&mut self) {
        if let Some(stop) = self.stop_fn.take() {
            stop();
        }
    }

    pub fn join(&mut self) {
        if let Some(handle) = self.join_handle.take() {
            if let Err(err) = handle.join() {
                log::warn!(
                    "Background worker {:?} terminated with panic: {:?}",
                    self.metadata.kind,
                    err
                );
            }
        }
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        self.shutdown();
        self.join();
    }
}

pub struct BackgroundWorkers {
    workers: Vec<WorkerHandle>,
}

impl Default for BackgroundWorkers {
    fn default() -> Self {
        Self::new()
    }
}

impl BackgroundWorkers {
    pub fn new() -> Self {
        Self {
            workers: Vec::new(),
        }
    }

    pub fn register(&mut self, handle: WorkerHandle) {
        self.workers.push(handle);
    }

    pub fn register_opt(&mut self, handle: Option<WorkerHandle>) {
        if let Some(handle) = handle {
            self.register(handle);
        }
    }

    pub fn shutdown_all(&mut self) {
        for worker in &mut self.workers {
            worker.shutdown();
        }
        for worker in &mut self.workers {
            worker.join();
        }
    }

    pub fn workers(&self) -> &[WorkerHandle] {
        &self.workers
    }

    pub fn snapshot(&self) -> Vec<WorkerMetadata> {
        self.workers
            .iter()
            .map(|worker| worker.metadata())
            .collect()
    }
}

impl Drop for BackgroundWorkers {
    fn drop(&mut self) {
        self.shutdown_all();
    }
}

pub fn wal_writer_worker(handle: WalWriterHandle, interval: Duration) -> WorkerHandle {
    WorkerHandle::new(
        WorkerMetadata {
            kind: WorkerKind::WalWriter,
            interval: Some(interval),
        },
        move || {
            if let Err(err) = handle.stop() {
                warn!("Failed to stop WAL writer: {}", err);
            }
        },
        None,
    )
}

pub fn spawn_checkpoint_worker(
    wal_manager: Arc<WalManager>,
    buffer_pool: Arc<BufferManager>,
    transaction_manager: Arc<TransactionManager>,
    interval: Option<Duration>,
) -> Option<WorkerHandle> {
    let Some(interval) = interval else {
        return None;
    };
    if interval.is_zero() {
        return None;
    }

    let wal = wal_manager.clone();
    let bp = buffer_pool.clone();
    let txn_mgr = transaction_manager.clone();

    spawn_periodic_worker(
        "checkpoint-worker",
        WorkerKind::Checkpoint,
        interval,
        move || {
            let dirty_pages = bp.dirty_page_ids();
            let dpt_snapshot = bp.dirty_page_table_snapshot();
            let active_txns = txn_mgr.active_transactions();
            let last_lsn = wal.max_assigned_lsn();

            if last_lsn != 0 {
                if let Err(e) = wal.flush_until(last_lsn) {
                    warn!("Checkpoint flush failed: {}", e);
                }
                let payload = CheckpointPayload {
                    last_lsn,
                    dirty_pages,
                    active_transactions: active_txns,
                    dpt: dpt_snapshot,
                };
                if let Err(e) = wal.log_checkpoint(payload) {
                    warn!("Checkpoint write failed: {}", e);
                }
            }
        },
    )
}

pub fn spawn_bg_writer(
    buffer_pool: Arc<BufferManager>,
    interval: Option<Duration>,
    vacuum_cfg: IndexVacuumConfig,
) -> Option<WorkerHandle> {
    let Some(interval) = interval else {
        return None;
    };
    if interval.is_zero() {
        return None;
    }
    let bp = buffer_pool.clone();
    spawn_periodic_worker(
        "bg-writer",
        WorkerKind::BufferPoolWriter,
        interval,
        move || {
            let dirty_ids = bp.dirty_page_ids();
            for page_id in dirty_ids.into_iter().take(16) {
                let _ = bp.flush_page(page_id);
            }

            let registry = global_index_registry();
            for (idx, heap) in registry.iter().take(16) {
                let pending = idx.take_pending_garbage();
                if pending >= vacuum_cfg.trigger_threshold {
                    let _ = idx.lazy_cleanup_with(
                        |rid| heap.tuple_meta(*rid).map(|m| m.is_deleted).unwrap_or(false),
                        Some(vacuum_cfg.batch_limit),
                    );
                }
            }
        },
    )
}

pub fn spawn_mvcc_vacuum_worker(
    transaction_manager: Arc<TransactionManager>,
    interval: Option<Duration>,
    batch_limit: usize,
) -> Option<WorkerHandle> {
    let Some(interval) = interval else {
        return None;
    };
    if interval.is_zero() || batch_limit == 0 {
        return None;
    }

    let txn_mgr = transaction_manager.clone();
    let tables = global_table_registry();

    spawn_periodic_worker("mvcc-vacuum", WorkerKind::MvccVacuum, interval, move || {
        let safe_xmin = txn_mgr
            .oldest_active_txn()
            .unwrap_or_else(|| txn_mgr.next_txn_id_hint());
        let mut remaining = batch_limit;

        for (table_ref, heap) in tables.iter_tables() {
            if remaining == 0 {
                break;
            }
            if matches!(table_ref.schema(), Some(schema) if schema == INFORMATION_SCHEMA_NAME) {
                continue;
            }

            match vacuum_table_versions(&heap, &txn_mgr, safe_xmin, &mut remaining) {
                Ok(cleaned) if cleaned > 0 => {
                    debug!(
                        "MVCC vacuum reclaimed {} tuple(s) from {}",
                        cleaned,
                        table_ref.to_log_string()
                    );
                }
                Ok(_) => {}
                Err(err) => {
                    warn!(
                        "MVCC vacuum on {} failed: {}",
                        table_ref.to_log_string(),
                        err
                    );
                }
            }
        }
    })
}

fn spawn_periodic_worker<F>(
    name: &str,
    kind: WorkerKind,
    interval: Duration,
    mut tick: F,
) -> Option<WorkerHandle>
where
    F: FnMut() + Send + 'static,
{
    let stop_flag = Arc::new(AtomicBool::new(false));
    let thread_flag = Arc::clone(&stop_flag);

    match thread::Builder::new().name(name.into()).spawn(move || {
        while !thread_flag.load(Ordering::Relaxed) {
            tick();
            if thread_flag.load(Ordering::Relaxed) {
                break;
            }
            thread::sleep(interval);
        }
    }) {
        Ok(join_handle) => {
            let stop_handle = Arc::clone(&stop_flag);
            Some(WorkerHandle::new(
                WorkerMetadata {
                    kind,
                    interval: Some(interval),
                },
                move || {
                    stop_handle.store(true, Ordering::Release);
                },
                Some(join_handle),
            ))
        }
        Err(err) => {
            warn!("Failed to spawn {}: {}", name, err);
            None
        }
    }
}

fn vacuum_table_versions(
    table: &Arc<TableHeap>,
    txn_mgr: &Arc<TransactionManager>,
    safe_xmin: TransactionId,
    remaining: &mut usize,
) -> crate::error::QuillSQLResult<usize> {
    if *remaining == 0 {
        return Ok(0);
    }

    let mut cleaned = 0usize;
    let mut iter = TableIterator::new(table.clone(), ..);
    while *remaining > 0 {
        match iter.next()? {
            Some((rid, meta, _tuple)) => {
                let removed = if meta.is_deleted {
                    try_reclaim_deleted(table, txn_mgr, safe_xmin, rid, &meta)?
                } else {
                    try_reclaim_aborted(table, txn_mgr, safe_xmin, rid, &meta)?
                };
                if removed {
                    cleaned += 1;
                    *remaining -= 1;
                }
            }
            None => break,
        }
    }
    Ok(cleaned)
}

fn try_reclaim_deleted(
    table: &Arc<TableHeap>,
    txn_mgr: &Arc<TransactionManager>,
    safe_xmin: TransactionId,
    rid: RecordId,
    meta: &TupleMeta,
) -> crate::error::QuillSQLResult<bool> {
    if meta.delete_txn_id == 0 {
        return Ok(false);
    }
    let status = txn_mgr.transaction_status(meta.delete_txn_id);
    let removable = match status {
        TransactionStatus::Committed => meta.delete_txn_id < safe_xmin,
        TransactionStatus::Aborted => true,
        TransactionStatus::InProgress | TransactionStatus::Unknown => false,
    };
    if !removable {
        return Ok(false);
    }

    let delete_txn = meta.delete_txn_id;
    let delete_cid = meta.delete_cid;
    let insert_txn = meta.insert_txn_id;
    table.vacuum_slot_if(rid, |current| {
        current.is_deleted
            && current.delete_txn_id == delete_txn
            && current.delete_cid == delete_cid
            && current.insert_txn_id == insert_txn
    })
}

fn try_reclaim_aborted(
    table: &Arc<TableHeap>,
    txn_mgr: &Arc<TransactionManager>,
    safe_xmin: TransactionId,
    rid: RecordId,
    meta: &TupleMeta,
) -> crate::error::QuillSQLResult<bool> {
    if meta.insert_txn_id == 0 {
        return Ok(false);
    }
    let status = txn_mgr.transaction_status(meta.insert_txn_id);
    if status != TransactionStatus::Aborted {
        return Ok(false);
    }
    if meta.insert_txn_id >= safe_xmin {
        return Ok(false);
    }

    let insert_txn = meta.insert_txn_id;
    table.vacuum_slot_if(rid, |current| {
        !current.is_deleted && current.insert_txn_id == insert_txn
    })
}
