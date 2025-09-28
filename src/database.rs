use log::{debug, warn};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

use crate::buffer::BUFFER_POOL_SIZE;
use crate::catalog::load_catalog_data;
use crate::catalog::registry::global_index_registry;
use crate::config::{IndexVacuumConfig, WalConfig};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::optimizer::LogicalOptimizer;
use crate::plan::logical_plan::LogicalPlan;
use crate::plan::PhysicalPlanner;
use crate::recovery::wal_record::CheckpointPayload;
use crate::recovery::{ControlFileManager, RecoveryManager, WalManager};
use crate::utils::util::{pretty_format_logical_plan, pretty_format_physical_plan};
use crate::{
    buffer::BufferPoolManager,
    catalog::Catalog,
    execution::{ExecutionContext, ExecutionEngine},
    plan::{LogicalPlanner, PlannerContext},
    storage::disk_manager::DiskManager,
    storage::disk_scheduler::DiskScheduler,
    storage::tuple::Tuple,
    transaction::{IsolationLevel, TransactionManager},
};

#[derive(Debug, Default, Clone)]
pub struct WalOptions {
    pub directory: Option<PathBuf>,
    pub segment_size: Option<u64>,
    pub sync_on_flush: Option<bool>,
    pub writer_interval_ms: Option<Option<u64>>,
    pub buffer_capacity: Option<usize>,
    pub flush_coalesce_bytes: Option<usize>,
    pub synchronous_commit: Option<bool>,
    pub checkpoint_interval_ms: Option<Option<u64>>,
    pub retain_segments: Option<usize>,
}

#[derive(Debug, Default, Clone)]
pub struct DatabaseOptions {
    pub wal: WalOptions,
}

pub struct Database {
    pub(crate) buffer_pool: Arc<BufferPoolManager>,
    pub(crate) catalog: Catalog,
    pub(crate) wal_manager: Arc<WalManager>,
    pub(crate) transaction_manager: Arc<TransactionManager>,
    temp_dir: Option<TempDir>,
    checkpoint_stop: Arc<AtomicBool>,
    checkpoint_handle: Option<thread::JoinHandle<()>>,
    bg_writer_stop: Arc<AtomicBool>,
    bg_writer_handle: Option<thread::JoinHandle<()>>,
}
impl Database {
    pub fn new_on_disk(db_path: &str) -> QuillSQLResult<Self> {
        Self::new_on_disk_with_options(db_path, DatabaseOptions::default())
    }

    pub fn new_on_disk_with_options(
        db_path: &str,
        options: DatabaseOptions,
    ) -> QuillSQLResult<Self> {
        let disk_manager = Arc::new(DiskManager::try_new(db_path)?);
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager.clone()));
        let buffer_pool = Arc::new(BufferPoolManager::new(
            BUFFER_POOL_SIZE,
            disk_scheduler.clone(),
        ));

        let wal_config = wal_config_for_path(db_path, &options.wal);
        let synchronous_commit = wal_config.synchronous_commit;
        let (control_file, wal_init) =
            ControlFileManager::load_or_init(&wal_config.directory, wal_config.segment_size)?;
        let control_file = Arc::new(control_file);
        let wal_manager = Arc::new(WalManager::new(
            wal_config.clone(),
            disk_scheduler.clone(),
            Some(wal_init),
            Some(control_file.clone()),
        )?);
        let transaction_manager = Arc::new(TransactionManager::new(
            wal_manager.clone(),
            synchronous_commit,
        ));
        if let Some(interval) = wal_config.writer_interval_ms {
            wal_manager.start_background_flush(Duration::from_millis(interval))?;
        }
        buffer_pool.set_wal_manager(wal_manager.clone());

        let catalog = Catalog::new(buffer_pool.clone(), disk_manager.clone());

        let recovery_summary = RecoveryManager::new(wal_manager.clone(), disk_scheduler.clone())
            .with_buffer_pool(buffer_pool.clone())
            .replay()?;
        if recovery_summary.redo_count > 0 {
            debug!(
                "Recovery replayed {} record(s) starting at LSN {}",
                recovery_summary.redo_count, recovery_summary.start_lsn
            );
        }
        if !recovery_summary.loser_transactions.is_empty() {
            warn!(
                "{} transaction(s) require undo after recovery: {:?}",
                recovery_summary.loser_transactions.len(),
                recovery_summary.loser_transactions
            );
        }

        let (checkpoint_stop, checkpoint_handle) = spawn_checkpoint_worker(
            wal_manager.clone(),
            buffer_pool.clone(),
            transaction_manager.clone(),
            wal_config.checkpoint_interval_ms,
        );

        let (bg_writer_stop, bg_writer_handle) = spawn_bg_writer(
            buffer_pool.clone(),
            std::env::var("QUILL_BG_WRITER_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok()),
            IndexVacuumConfig::default(),
        );

        let mut db = Self {
            buffer_pool,
            catalog,
            wal_manager,
            transaction_manager,
            temp_dir: None,
            checkpoint_stop,
            checkpoint_handle,
            bg_writer_stop,
            bg_writer_handle,
        };
        load_catalog_data(&mut db)?;
        Ok(db)
    }

    pub fn new_temp() -> QuillSQLResult<Self> {
        Self::new_temp_with_options(DatabaseOptions::default())
    }

    pub fn new_temp_with_options(options: DatabaseOptions) -> QuillSQLResult<Self> {
        let temp_dir = TempDir::new()?;
        let temp_path = temp_dir.path().join("test.db");
        let disk_manager =
            Arc::new(DiskManager::try_new(temp_path.to_str().ok_or(
                QuillSQLError::Internal("Invalid temp path".to_string()),
            )?)?);
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager.clone()));
        let buffer_pool = Arc::new(BufferPoolManager::new(
            BUFFER_POOL_SIZE,
            disk_scheduler.clone(),
        ));

        let wal_config = wal_config_for_temp(temp_dir.path(), &options.wal);
        let synchronous_commit = wal_config.synchronous_commit;
        let (control_file, wal_init) =
            ControlFileManager::load_or_init(&wal_config.directory, wal_config.segment_size)?;
        let control_file = Arc::new(control_file);
        let wal_manager = Arc::new(WalManager::new(
            wal_config.clone(),
            disk_scheduler.clone(),
            Some(wal_init),
            Some(control_file.clone()),
        )?);
        let transaction_manager = Arc::new(TransactionManager::new(
            wal_manager.clone(),
            synchronous_commit,
        ));
        if let Some(interval) = wal_config.writer_interval_ms {
            wal_manager.start_background_flush(Duration::from_millis(interval))?;
        }
        buffer_pool.set_wal_manager(wal_manager.clone());

        let catalog = Catalog::new(buffer_pool.clone(), disk_manager.clone());

        let recovery_summary = RecoveryManager::new(wal_manager.clone(), disk_scheduler.clone())
            .with_buffer_pool(buffer_pool.clone())
            .replay()?;
        if recovery_summary.redo_count > 0 {
            debug!(
                "Recovery replayed {} record(s) starting at LSN {}",
                recovery_summary.redo_count, recovery_summary.start_lsn
            );
        }
        if !recovery_summary.loser_transactions.is_empty() {
            warn!(
                "{} transaction(s) require undo after recovery: {:?}",
                recovery_summary.loser_transactions.len(),
                recovery_summary.loser_transactions
            );
        }

        let (checkpoint_stop, checkpoint_handle) = spawn_checkpoint_worker(
            wal_manager.clone(),
            buffer_pool.clone(),
            transaction_manager.clone(),
            wal_config.checkpoint_interval_ms,
        );

        let (bg_writer_stop, bg_writer_handle) = spawn_bg_writer(
            buffer_pool.clone(),
            std::env::var("QUILL_BG_WRITER_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok()),
            IndexVacuumConfig::default(),
        );

        let mut db = Self {
            buffer_pool,
            catalog,
            wal_manager,
            transaction_manager,
            temp_dir: Some(temp_dir),
            checkpoint_stop,
            checkpoint_handle,
            bg_writer_stop,
            bg_writer_handle,
        };
        load_catalog_data(&mut db)?;
        Ok(db)
    }

    pub fn run(&mut self, sql: &str) -> QuillSQLResult<Vec<Tuple>> {
        let logical_plan = self.create_logical_plan(sql)?;
        debug!(
            "Logical Plan: \n{}",
            pretty_format_logical_plan(&logical_plan)
        );

        let optimized_logical_plan = LogicalOptimizer::new().optimize(&logical_plan)?;
        debug!(
            "Optimized Logical Plan: \n{}",
            pretty_format_logical_plan(&logical_plan)
        );

        // logical plan -> physical plan
        let physical_planner = PhysicalPlanner {
            catalog: &self.catalog,
        };
        let physical_plan = physical_planner.create_physical_plan(optimized_logical_plan);
        debug!(
            "Physical Plan: \n{}",
            pretty_format_physical_plan(&physical_plan)
        );

        let mut txn = self
            .transaction_manager
            .begin(IsolationLevel::ReadUncommitted)?;
        let execution_ctx =
            ExecutionContext::new(&mut self.catalog, &mut txn, &self.transaction_manager);
        let mut execution_engine = ExecutionEngine {
            context: execution_ctx,
        };
        match execution_engine.execute(Arc::new(physical_plan)) {
            Ok(tuples) => {
                let _ = self.transaction_manager.commit(&mut txn);
                Ok(tuples)
            }
            Err(e) => {
                let _ = self.transaction_manager.abort(&mut txn);
                Err(e)
            }
        }
    }

    pub fn create_logical_plan(&mut self, sql: &str) -> QuillSQLResult<LogicalPlan> {
        // sql -> ast
        let stmts = crate::sql::parser::parse_sql(sql)?;
        if stmts.len() != 1 {
            return Err(QuillSQLError::NotSupport(
                "only support one sql statement".to_string(),
            ));
        }
        let stmt = &stmts[0];
        let mut planner = LogicalPlanner {
            context: PlannerContext {
                catalog: &self.catalog,
            },
        };
        // ast -> logical plan
        planner.plan(stmt)
    }

    pub fn flush(&self) -> QuillSQLResult<()> {
        let _ = self.wal_manager.flush(None)?;
        self.buffer_pool.flush_all_pages()
    }

    pub fn transaction_manager(&self) -> Arc<TransactionManager> {
        self.transaction_manager.clone()
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.checkpoint_stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.checkpoint_handle.take() {
            if let Err(join_err) = handle.join() {
                warn!("Checkpoint worker terminated with panic: {:?}", join_err);
            }
        }
        self.bg_writer_stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.bg_writer_handle.take() {
            if let Err(join_err) = handle.join() {
                warn!("BG writer terminated with panic: {:?}", join_err);
            }
        }
    }
}

fn wal_config_for_path(db_path: &str, overrides: &WalOptions) -> WalConfig {
    let mut config = WalConfig::default();
    config.directory = overrides
        .directory
        .clone()
        .unwrap_or_else(|| wal_directory_from_path(db_path));
    if let Some(size) = overrides.segment_size {
        config.segment_size = size;
    }
    if let Some(sync) = overrides.sync_on_flush {
        config.sync_on_flush = sync;
    }
    if let Some(interval) = overrides.writer_interval_ms.clone() {
        config.writer_interval_ms = interval;
    }
    if let Some(capacity) = overrides.buffer_capacity {
        config.buffer_capacity = capacity;
    }
    if let Some(bytes) = overrides.flush_coalesce_bytes {
        config.flush_coalesce_bytes = bytes;
    }
    if let Some(sync_commit) = overrides.synchronous_commit {
        config.synchronous_commit = sync_commit;
    }
    if let Some(interval) = overrides.checkpoint_interval_ms.clone() {
        config.checkpoint_interval_ms = interval;
    }
    if let Some(retain) = overrides.retain_segments {
        config.retain_segments = retain.max(1);
    }
    config
}

fn wal_directory_from_path(db_path: &str) -> PathBuf {
    let mut base = PathBuf::from(db_path);
    base.set_extension("wal");
    if base.extension().is_none() {
        PathBuf::from(format!("{}.wal", db_path))
    } else {
        base
    }
}

fn wal_config_for_temp(temp_root: &Path, overrides: &WalOptions) -> WalConfig {
    let mut config = WalConfig::default();
    config.directory = overrides
        .directory
        .clone()
        .unwrap_or_else(|| temp_root.join("wal"));
    if let Some(size) = overrides.segment_size {
        config.segment_size = size;
    }
    if let Some(sync) = overrides.sync_on_flush {
        config.sync_on_flush = sync;
    }
    if let Some(interval) = overrides.writer_interval_ms.clone() {
        config.writer_interval_ms = interval;
    }
    if let Some(capacity) = overrides.buffer_capacity {
        config.buffer_capacity = capacity;
    }
    if let Some(bytes) = overrides.flush_coalesce_bytes {
        config.flush_coalesce_bytes = bytes;
    }
    if let Some(sync_commit) = overrides.synchronous_commit {
        config.synchronous_commit = sync_commit;
    }
    if let Some(interval) = overrides.checkpoint_interval_ms.clone() {
        config.checkpoint_interval_ms = interval;
    }
    if let Some(retain) = overrides.retain_segments {
        config.retain_segments = retain.max(1);
    }
    config
}

fn spawn_checkpoint_worker(
    wal_manager: Arc<WalManager>,
    buffer_pool: Arc<BufferPoolManager>,
    transaction_manager: Arc<TransactionManager>,
    interval_ms: Option<u64>,
) -> (Arc<AtomicBool>, Option<thread::JoinHandle<()>>) {
    let stop = Arc::new(AtomicBool::new(false));
    let Some(ms) = interval_ms else {
        return (stop, None);
    };
    if ms == 0 {
        return (stop, None);
    }
    let stop_flag = stop.clone();
    let wal = wal_manager.clone();
    let bp = buffer_pool.clone();
    let txn_mgr = transaction_manager.clone();
    let interval = Duration::from_millis(ms);
    let handle = thread::Builder::new()
        .name("checkpoint-worker".into())
        .spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
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

                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                thread::sleep(interval);
            }
        })
        .map(Some)
        .unwrap_or_else(|err| {
            warn!("Failed to spawn checkpoint worker: {}", err);
            None
        });

    (stop, handle)
}

fn spawn_bg_writer(
    buffer_pool: Arc<BufferPoolManager>,
    interval_ms: Option<u64>,
    vacuum_cfg: IndexVacuumConfig,
) -> (Arc<AtomicBool>, Option<thread::JoinHandle<()>>) {
    let stop = Arc::new(AtomicBool::new(false));
    let Some(ms) = interval_ms else {
        return (stop, None);
    };
    if ms == 0 {
        return (stop, None);
    }
    let stop_flag = stop.clone();
    let bp = buffer_pool.clone();
    let interval = Duration::from_millis(ms);
    let handle = thread::Builder::new()
        .name("bg-writer".into())
        .spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                // Flush a small batch of dirty pages per cycle
                let dirty_ids = bp.dirty_page_ids();
                for page_id in dirty_ids.into_iter().take(16) {
                    let _ = bp.flush_page(page_id);
                }

                // Opportunistic index lazy cleanup based on pending_garbage counters.
                // Global registry is read-only and cheap to iterate.
                let registry = global_index_registry();
                for (idx, heap) in registry.iter().take(16) {
                    let pending = idx.take_pending_garbage();
                    if pending >= vacuum_cfg.trigger_threshold {
                        // conservative predicate using heap meta only (no txn watermarks here):
                        // delete-marked tuples are always globally invisible for readers.
                        let _ = idx.lazy_cleanup_with(
                            |rid| heap.tuple_meta(*rid).map(|m| m.is_deleted).unwrap_or(false),
                            Some(vacuum_cfg.batch_limit),
                        );
                    }
                }

                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                thread::sleep(interval);
            }
        })
        .map(Some)
        .unwrap_or_else(|err| {
            warn!("Failed to spawn bg writer: {}", err);
            None
        });
    (stop, handle)
}
