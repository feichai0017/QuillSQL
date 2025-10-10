use crate::background::{self, BackgroundWorkers};
use log::{debug, warn};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

use crate::buffer::{StandardBufferManager, BUFFER_POOL_SIZE};
use crate::catalog::load_catalog_data;
use crate::config::{background_config, IndexVacuumConfig, MvccVacuumConfig, WalConfig};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::optimizer::LogicalOptimizer;
use crate::plan::logical_plan::{LogicalPlan, TransactionScope};
use crate::plan::PhysicalPlanner;
use crate::recovery::{ControlFileManager, RecoveryManager, WalManager};
use crate::session::SessionContext;
use crate::utils::util::{pretty_format_logical_plan, pretty_format_physical_plan};
use crate::{
    catalog::Catalog,
    execution::ExecutionEngine,
    plan::{LogicalPlanner, PlannerContext},
    storage::disk_manager::DiskManager,
    storage::disk_scheduler::DiskScheduler,
    storage::tuple::Tuple,
    transaction::{IsolationLevel, TransactionManager},
};
use sqlparser::ast::TransactionAccessMode;

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

#[derive(Debug, Clone, Default)]
pub struct DatabaseOptions {
    pub wal: WalOptions,
    pub default_isolation_level: Option<IsolationLevel>,
}

pub struct Database {
    _temp_dir: Option<TempDir>,
    pub(crate) buffer_pool: Arc<StandardBufferManager>,
    pub(crate) catalog: Catalog,
    background_workers: BackgroundWorkers,
    pub(crate) wal_manager: Arc<WalManager>,
    pub(crate) transaction_manager: Arc<TransactionManager>,
    default_isolation: IsolationLevel,
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
        let buffer_pool = Arc::new(StandardBufferManager::new(
            BUFFER_POOL_SIZE,
            disk_scheduler.clone(),
        ));

        let wal_config = wal_config_for_path(db_path, &options.wal);
        let synchronous_commit = wal_config.synchronous_commit;
        let (control_file, wal_init) =
            ControlFileManager::load_or_init(&wal_config.directory, wal_config.segment_size)?;
        let control_file = Arc::new(control_file);
        let wal_manager = Arc::new(WalManager::new_with_scheduler(
            wal_config.clone(),
            Some(wal_init),
            Some(control_file.clone()),
            disk_scheduler.clone(),
        )?);
        let transaction_manager = Arc::new(TransactionManager::new(
            wal_manager.clone(),
            synchronous_commit,
        ));

        let worker_cfg = background_config(
            &wal_config,
            IndexVacuumConfig::default(),
            MvccVacuumConfig::default(),
        );
        let mut background_workers = BackgroundWorkers::new();
        if let Some(interval) = worker_cfg.wal_writer_interval {
            if let Some(handle) = wal_manager.start_background_flush(interval)? {
                background_workers.register(background::wal_writer_worker(handle, interval));
            }
        }
        buffer_pool.set_wal_manager(wal_manager.clone());

        let catalog = Catalog::new(buffer_pool.clone(), disk_manager.clone());

        let recovery_summary = RecoveryManager::<StandardBufferManager>::new(
            wal_manager.clone(),
            disk_scheduler.clone(),
        )
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

        background_workers.register_opt(
            background::spawn_checkpoint_worker::<StandardBufferManager>(
                wal_manager.clone(),
                buffer_pool.clone(),
                transaction_manager.clone(),
                worker_cfg.checkpoint_interval,
            ),
        );

        background_workers.register_opt(background::spawn_bg_writer::<StandardBufferManager>(
            buffer_pool.clone(),
            worker_cfg.bg_writer_interval,
            worker_cfg.vacuum,
        ));

        let mvcc_interval = if worker_cfg.mvcc_vacuum.interval_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(worker_cfg.mvcc_vacuum.interval_ms))
        };
        background_workers.register_opt(background::spawn_mvcc_vacuum_worker::<
            StandardBufferManager,
        >(
            transaction_manager.clone(),
            mvcc_interval,
            worker_cfg.mvcc_vacuum.batch_limit,
        ));

        let mut db = Self {
            _temp_dir: None,
            buffer_pool,
            catalog,
            background_workers,
            wal_manager,
            transaction_manager,
            default_isolation: options
                .default_isolation_level
                .unwrap_or(IsolationLevel::ReadUncommitted),
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
        let buffer_pool = Arc::new(StandardBufferManager::new(
            BUFFER_POOL_SIZE,
            disk_scheduler.clone(),
        ));

        let wal_config = wal_config_for_temp(temp_dir.path(), &options.wal);
        let synchronous_commit = wal_config.synchronous_commit;
        let (control_file, wal_init) =
            ControlFileManager::load_or_init(&wal_config.directory, wal_config.segment_size)?;
        let control_file = Arc::new(control_file);
        let wal_manager = Arc::new(WalManager::new_with_scheduler(
            wal_config.clone(),
            Some(wal_init),
            Some(control_file.clone()),
            disk_scheduler.clone(),
        )?);
        let transaction_manager = Arc::new(TransactionManager::new(
            wal_manager.clone(),
            synchronous_commit,
        ));

        let worker_cfg = background_config(
            &wal_config,
            IndexVacuumConfig::default(),
            MvccVacuumConfig::default(),
        );
        let mut background_workers = BackgroundWorkers::new();
        if let Some(interval) = worker_cfg.wal_writer_interval {
            if let Some(handle) = wal_manager.start_background_flush(interval)? {
                background_workers.register(background::wal_writer_worker(handle, interval));
            }
        }
        buffer_pool.set_wal_manager(wal_manager.clone());

        let catalog = Catalog::new(buffer_pool.clone(), disk_manager.clone());

        let recovery_summary = RecoveryManager::<StandardBufferManager>::new(
            wal_manager.clone(),
            disk_scheduler.clone(),
        )
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

        background_workers.register_opt(
            background::spawn_checkpoint_worker::<StandardBufferManager>(
                wal_manager.clone(),
                buffer_pool.clone(),
                transaction_manager.clone(),
                worker_cfg.checkpoint_interval,
            ),
        );

        background_workers.register_opt(background::spawn_bg_writer::<StandardBufferManager>(
            buffer_pool.clone(),
            worker_cfg.bg_writer_interval,
            worker_cfg.vacuum,
        ));

        let mvcc_interval = if worker_cfg.mvcc_vacuum.interval_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(worker_cfg.mvcc_vacuum.interval_ms))
        };
        background_workers.register_opt(background::spawn_mvcc_vacuum_worker::<
            StandardBufferManager,
        >(
            transaction_manager.clone(),
            mvcc_interval,
            worker_cfg.mvcc_vacuum.batch_limit,
        ));

        let mut db = Self {
            _temp_dir: Some(temp_dir),
            buffer_pool,
            catalog,
            background_workers,
            wal_manager,
            transaction_manager,
            default_isolation: options
                .default_isolation_level
                .unwrap_or(IsolationLevel::ReadUncommitted),
        };
        load_catalog_data(&mut db)?;
        Ok(db)
    }

    pub fn run(&mut self, sql: &str) -> QuillSQLResult<Vec<Tuple>> {
        let mut session = SessionContext::new(self.default_isolation);
        self.run_with_session(&mut session, sql)
    }

    pub fn run_with_session(
        &mut self,
        session: &mut SessionContext,
        sql: &str,
    ) -> QuillSQLResult<Vec<Tuple>> {
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

        let physical_planner = PhysicalPlanner {
            catalog: &self.catalog,
        };
        let physical_plan = physical_planner.create_physical_plan(optimized_logical_plan.clone());
        debug!(
            "Physical Plan: \n{}",
            pretty_format_physical_plan(&physical_plan)
        );

        match optimized_logical_plan {
            LogicalPlan::BeginTransaction(ref modes) => {
                if session.has_active_transaction() {
                    return Err(QuillSQLError::Execution(
                        "transaction already active".to_string(),
                    ));
                }
                let txn = self.transaction_manager.begin(
                    modes.unwrap_effective_isolation(session.default_isolation()),
                    modes
                        .access_mode
                        .unwrap_or(TransactionAccessMode::ReadWrite),
                )?;
                session.set_active_transaction(txn)?;
                Ok(vec![])
            }
            LogicalPlan::CommitTransaction => {
                let txn_ref = session
                    .active_txn_mut()
                    .ok_or_else(|| QuillSQLError::Execution("no active transaction".to_string()))?;
                self.transaction_manager.commit(txn_ref)?;
                session.clear_active_transaction();
                Ok(vec![])
            }
            LogicalPlan::RollbackTransaction => {
                let txn_ref = session
                    .active_txn_mut()
                    .ok_or_else(|| QuillSQLError::Execution("no active transaction".to_string()))?;
                self.transaction_manager.abort(txn_ref)?;
                session.clear_active_transaction();
                Ok(vec![])
            }
            LogicalPlan::SetTransaction {
                ref scope,
                ref modes,
            } => {
                match scope {
                    TransactionScope::Session => session.apply_session_modes(modes),
                    TransactionScope::Transaction => session.apply_transaction_modes(modes),
                }
                Ok(vec![])
            }
            _ => {
                let needs_cleanup = !session.has_active_transaction();
                let autocommit = session.autocommit();

                let result = {
                    let txn = session.ensure_active_transaction(&self.transaction_manager)?;
                    let context = crate::execution::ExecutionContext::new(
                        &mut self.catalog,
                        txn,
                        &self.transaction_manager,
                    );
                    let mut engine = ExecutionEngine { context };
                    engine.execute(Arc::new(physical_plan))?
                };

                if autocommit && needs_cleanup {
                    if let Some(txn) = session.active_txn_mut() {
                        self.transaction_manager.commit(txn)?;
                    }
                    session.clear_active_transaction();
                }

                Ok(result)
            }
        }
    }

    pub fn default_isolation(&self) -> IsolationLevel {
        self.default_isolation
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
        self.background_workers.shutdown_all();
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
