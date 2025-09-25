use log::debug;
use std::sync::Arc;
use tempfile::TempDir;

use crate::buffer::BUFFER_POOL_SIZE;
use crate::catalog::load_catalog_data;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::optimizer::LogicalOptimizer;
use crate::plan::logical_plan::LogicalPlan;
use crate::plan::PhysicalPlanner;
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

pub struct Database {
    pub(crate) buffer_pool: Arc<BufferPoolManager>,
    pub(crate) catalog: Catalog,
    temp_dir: Option<TempDir>,
}
impl Database {
    pub fn new_on_disk(db_path: &str) -> QuillSQLResult<Self> {
        let disk_manager = Arc::new(DiskManager::try_new(db_path)?);
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager.clone()));
        let buffer_pool = Arc::new(BufferPoolManager::new(BUFFER_POOL_SIZE, disk_scheduler));

        let catalog = Catalog::new(buffer_pool.clone(), disk_manager.clone());

        let mut db = Self {
            buffer_pool,
            catalog,
            temp_dir: None,
        };
        load_catalog_data(&mut db)?;
        Ok(db)
    }

    pub fn new_temp() -> QuillSQLResult<Self> {
        let temp_dir = TempDir::new()?;
        let temp_path = temp_dir.path().join("test.db");
        let disk_manager =
            Arc::new(DiskManager::try_new(temp_path.to_str().ok_or(
                QuillSQLError::Internal("Invalid temp path".to_string()),
            )?)?);
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager.clone()));
        let buffer_pool = Arc::new(BufferPoolManager::new(BUFFER_POOL_SIZE, disk_scheduler));

        let catalog = Catalog::new(buffer_pool.clone(), disk_manager.clone());

        let mut db = Self {
            buffer_pool,
            catalog,
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
        self.buffer_pool.flush_all_pages()
    }
}
