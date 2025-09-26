use log::debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

use crate::buffer::BUFFER_POOL_SIZE;
use crate::catalog::load_catalog_data;
use crate::config::WalConfig;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::optimizer::LogicalOptimizer;
use crate::plan::logical_plan::LogicalPlan;
use crate::plan::PhysicalPlanner;
use crate::recovery::WalManager;
use crate::utils::util::{pretty_format_logical_plan, pretty_format_physical_plan};
use crate::{
    buffer::BufferPoolManager,
    catalog::Catalog,
    execution::{ExecutionContext, ExecutionEngine},
    plan::{LogicalPlanner, PlannerContext},
    storage::disk_manager::DiskManager,
    storage::disk_scheduler::DiskScheduler,
    storage::tuple::Tuple,
};

#[derive(Debug, Default, Clone)]
pub struct WalOptions {
    pub directory: Option<PathBuf>,
    pub segment_size: Option<u64>,
    pub sync_on_flush: Option<bool>,
    pub writer_interval_ms: Option<Option<u64>>,
}

#[derive(Debug, Default, Clone)]
pub struct DatabaseOptions {
    pub wal: WalOptions,
}

pub struct Database {
    pub(crate) buffer_pool: Arc<BufferPoolManager>,
    pub(crate) catalog: Catalog,
    pub(crate) wal_manager: Arc<WalManager>,
    temp_dir: Option<TempDir>,
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
        let wal_manager = Arc::new(WalManager::new_with_scheduler(
            wal_config.clone(),
            disk_scheduler.clone(),
        )?);
        if let Some(interval) = wal_config.writer_interval_ms {
            wal_manager.start_background_flush(Duration::from_millis(interval))?;
        }
        buffer_pool.set_wal_manager(wal_manager.clone());

        let catalog = Catalog::new(buffer_pool.clone(), disk_manager.clone());

        let mut db = Self {
            buffer_pool,
            catalog,
            wal_manager,
            temp_dir: None,
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
        let wal_manager = Arc::new(WalManager::new_with_scheduler(
            wal_config.clone(),
            disk_scheduler.clone(),
        )?);
        if let Some(interval) = wal_config.writer_interval_ms {
            wal_manager.start_background_flush(Duration::from_millis(interval))?;
        }
        buffer_pool.set_wal_manager(wal_manager.clone());

        let catalog = Catalog::new(buffer_pool.clone(), disk_manager.clone());

        let mut db = Self {
            buffer_pool,
            catalog,
            wal_manager,
            temp_dir: Some(temp_dir),
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

        let execution_ctx = ExecutionContext::new(&mut self.catalog);
        let mut execution_engine = ExecutionEngine {
            context: execution_ctx,
        };
        let tuples = execution_engine.execute(Arc::new(physical_plan))?;
        Ok(tuples)
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
    config
}
