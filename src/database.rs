use log::debug;
use serde::Serialize;
use sqlparser::ast::TransactionAccessMode;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tempfile::TempDir;

use crate::catalog::{load_catalog_data, Catalog, TableStatistics};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::execution::physical_plan::PhysicalPlan;
use crate::execution::ExecutionEngine;
use crate::optimizer::LogicalOptimizer;
use crate::plan::logical_plan::{LogicalPlan, TransactionScope};
use crate::plan::{LogicalPlanner, PhysicalPlanner, PlannerContext};
use crate::session::SessionContext;
use crate::storage::engine::TableHandle;
use crate::storage::holt::{HoltStore, HoltTableHandle};
use crate::storage::tuple::Tuple;
use crate::storage::HoltStorage;
use crate::transaction::{
    CommandId, IsolationLevel, LockDebugSnapshot, TransactionManager, TransactionStatus,
    TxnDebugSnapshot,
};
use crate::utils::table_ref::TableReference;
use crate::utils::util::{pretty_format_logical_plan, pretty_format_physical_plan};

#[derive(Debug, Clone, Default)]
pub struct DatabaseOptions {
    pub holt: HoltOptions,
    pub default_isolation_level: Option<IsolationLevel>,
}

#[derive(Debug, Default, Clone)]
pub struct HoltOptions {
    pub directory: Option<PathBuf>,
}

#[derive(Clone)]
enum DatabaseLocation {
    OnDisk(String),
    Temporary,
}

pub struct Database {
    pub(crate) catalog: Catalog,
    pub(crate) transaction_manager: Arc<TransactionManager>,
    pub(crate) holt_store: Arc<HoltStore>,
    default_isolation: IsolationLevel,
    storage: Arc<HoltStorage>,
    debug_trace: Arc<Mutex<Option<DebugTrace>>>,
    // Must drop after every Holt owner so temporary databases can checkpoint
    // before their directory is removed.
    _temp_dir: Option<TempDir>,
}

struct PreparedStatement {
    optimized_logical_plan: LogicalPlan,
    physical_plan: PhysicalPlan,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugTrace {
    pub logical_plan: String,
    pub physical_plan: String,
    pub rows: usize,
    pub duration_ms: u128,
    pub logical_tree: DebugPlanNode,
    pub physical_tree: DebugPlanNode,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugPlanNode {
    pub op: String,
    pub children: Vec<DebugPlanNode>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugPlanSnapshot {
    pub logical: DebugPlanNode,
    pub physical: DebugPlanNode,
}

#[derive(Debug, Clone, Serialize)]
pub struct MvccVersionSample {
    pub table: String,
    pub rid: String,
    pub insert_txn: u64,
    pub delete_txn: u64,
    pub visible: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct MvccVersionsDebug {
    pub samples: Vec<MvccVersionSample>,
    pub note: String,
}

impl DebugPlanNode {
    pub fn from_physical(plan: &PhysicalPlan) -> Self {
        Self {
            op: plan.display_name(),
            children: plan
                .inputs()
                .iter()
                .map(|child| Self::from_physical(child))
                .collect(),
        }
    }

    pub fn from_logical(plan: &LogicalPlan) -> Self {
        Self {
            op: plan.to_string(),
            children: plan
                .inputs()
                .iter()
                .map(|child| Self::from_logical(child))
                .collect(),
        }
    }
}

impl Database {
    pub fn new_on_disk(db_path: &str) -> QuillSQLResult<Self> {
        Self::new_on_disk_with_options(db_path, DatabaseOptions::default())
    }

    pub fn new_on_disk_with_options(
        db_path: &str,
        options: DatabaseOptions,
    ) -> QuillSQLResult<Self> {
        Self::new_with_location(DatabaseLocation::OnDisk(db_path.to_string()), options)
    }

    pub fn new_temp() -> QuillSQLResult<Self> {
        Self::new_temp_with_options(DatabaseOptions::default())
    }

    pub fn new_temp_with_options(options: DatabaseOptions) -> QuillSQLResult<Self> {
        Self::new_with_location(DatabaseLocation::Temporary, options)
    }

    fn new_with_location(
        location: DatabaseLocation,
        options: DatabaseOptions,
    ) -> QuillSQLResult<Self> {
        let (holt_dir, temp_dir) = holt_directory_for_location(&location, &options.holt)?;
        let holt_store = Arc::new(HoltStore::open(holt_dir)?);
        let transaction_manager = Arc::new(TransactionManager::new());
        seed_holt_transaction_statuses(&holt_store, &transaction_manager)?;

        let catalog = Catalog::new(holt_store.clone());
        let storage = Arc::new(HoltStorage::new(holt_store.clone()));

        let mut db = Self {
            catalog,
            transaction_manager,
            holt_store,
            default_isolation: options
                .default_isolation_level
                .unwrap_or(IsolationLevel::ReadUncommitted),
            storage,
            debug_trace: Arc::new(Mutex::new(None)),
            _temp_dir: temp_dir,
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
        let start = Instant::now();
        let PreparedStatement {
            optimized_logical_plan,
            physical_plan,
        } = self.plan_statement(sql)?;
        let logical_plan_str = pretty_format_logical_plan(&optimized_logical_plan);
        let physical_plan_str = pretty_format_physical_plan(&physical_plan);
        let logical_tree = DebugPlanNode::from_logical(&optimized_logical_plan);
        let physical_tree = DebugPlanNode::from_physical(&physical_plan);

        if let Some(result) = self.execute_transaction_control(session, &optimized_logical_plan)? {
            return Ok(result);
        }

        let result = self.execute_physical_plan(session, physical_plan)?;

        if let Ok(mut guard) = self.debug_trace.lock() {
            *guard = Some(DebugTrace {
                logical_plan: logical_plan_str,
                physical_plan: physical_plan_str,
                logical_tree,
                physical_tree,
                rows: result.len(),
                duration_ms: start.elapsed().as_millis(),
            });
        }

        Ok(result)
    }

    pub fn default_isolation(&self) -> IsolationLevel {
        self.default_isolation
    }

    pub fn create_logical_plan(&mut self, sql: &str) -> QuillSQLResult<LogicalPlan> {
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
        planner.plan(stmt)
    }

    pub fn analyze_table(&mut self, table_ref: &TableReference) -> QuillSQLResult<TableStatistics> {
        self.catalog.analyze_table(table_ref)
    }

    pub fn flush(&self) -> QuillSQLResult<()> {
        self.holt_store
            .db()
            .checkpoint()
            .map_err(crate::storage::holt::map_holt_err)
    }

    #[cfg(test)]
    pub(crate) fn table_binding(
        &self,
        table_ref: &TableReference,
    ) -> QuillSQLResult<crate::storage::TableBinding> {
        self.storage.table(&self.catalog, table_ref)
    }

    pub fn debug_last_trace(&self) -> Option<DebugTrace> {
        self.debug_trace.lock().ok().and_then(|guard| guard.clone())
    }

    pub fn debug_lock_snapshot(&self) -> LockDebugSnapshot {
        self.transaction_manager.lock_manager_arc().debug_snapshot()
    }

    pub fn debug_txn_snapshot(&self) -> TxnDebugSnapshot {
        self.transaction_manager.debug_snapshot()
    }

    pub fn debug_mvcc_versions(&self) -> QuillSQLResult<MvccVersionsDebug> {
        let snapshot = self
            .transaction_manager
            .snapshot(self.transaction_manager.next_txn_id_hint());
        let mut samples = Vec::new();
        let max_samples = 20usize;
        for (schema_name, schema) in &self.catalog.schemas {
            if schema_name == crate::catalog::INFORMATION_SCHEMA_NAME {
                continue;
            }
            for (table_name, table) in &schema.tables {
                let Some(table_id) = table.table_id() else {
                    continue;
                };
                let table_ref = TableReference::Full {
                    catalog: crate::catalog::DEFAULT_CATALOG_NAME.to_string(),
                    schema: schema_name.clone(),
                    table: table_name.clone(),
                };
                let handle = HoltTableHandle::new(
                    table_ref.clone(),
                    table.schema.clone(),
                    table_id,
                    self.holt_store.clone(),
                );
                let mut stream = handle.full_scan()?;
                while let Some((rid, meta, _tuple)) = stream.next()? {
                    if samples.len() >= max_samples {
                        break;
                    }
                    let visible = snapshot.is_visible(&meta, 0 as CommandId, |tid| {
                        self.transaction_manager.transaction_status(tid)
                    });
                    samples.push(MvccVersionSample {
                        table: table_ref.to_string(),
                        rid: rid.to_string(),
                        insert_txn: meta.insert_txn_id,
                        delete_txn: meta.delete_txn_id,
                        visible,
                    });
                }
                if samples.len() >= max_samples {
                    break;
                }
            }
            if samples.len() >= max_samples {
                break;
            }
        }

        Ok(MvccVersionsDebug {
            samples,
            note: format!("sampled up to {} tuples from Holt tables", max_samples),
        })
    }

    pub fn debug_last_plan(&self) -> Option<DebugPlanSnapshot> {
        self.debug_trace
            .lock()
            .ok()
            .and_then(|opt| opt.clone())
            .map(|trace| DebugPlanSnapshot {
                logical: trace.logical_tree,
                physical: trace.physical_tree,
            })
    }

    pub fn table_statistics(
        &self,
        table_ref: &TableReference,
    ) -> Option<&crate::catalog::TableStatistics> {
        self.catalog.table_statistics(table_ref)
    }

    pub fn transaction_manager(&self) -> Arc<TransactionManager> {
        self.transaction_manager.clone()
    }

    fn plan_statement(&mut self, sql: &str) -> QuillSQLResult<PreparedStatement> {
        let logical_plan = self.create_logical_plan(sql)?;
        debug!(
            "Logical Plan: \n{}",
            pretty_format_logical_plan(&logical_plan)
        );

        let optimized_logical_plan = self.optimize_logical_plan(&logical_plan)?;
        debug!(
            "Optimized Logical Plan: \n{}",
            pretty_format_logical_plan(&optimized_logical_plan)
        );

        let physical_plan = self.build_physical_plan(&optimized_logical_plan);
        debug!(
            "Physical Plan: \n{}",
            pretty_format_physical_plan(&physical_plan)
        );

        Ok(PreparedStatement {
            optimized_logical_plan,
            physical_plan,
        })
    }

    fn optimize_logical_plan(&self, logical_plan: &LogicalPlan) -> QuillSQLResult<LogicalPlan> {
        LogicalOptimizer::new().optimize(logical_plan)
    }

    fn build_physical_plan(&self, logical_plan: &LogicalPlan) -> PhysicalPlan {
        let physical_planner = PhysicalPlanner::with_catalog(&self.catalog);
        physical_planner.create_physical_plan(logical_plan.clone())
    }

    fn execute_transaction_control(
        &self,
        session: &mut SessionContext,
        plan: &LogicalPlan,
    ) -> QuillSQLResult<Option<Vec<Tuple>>> {
        match plan {
            LogicalPlan::BeginTransaction(modes) => {
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
                Ok(Some(vec![]))
            }
            LogicalPlan::CommitTransaction => {
                let txn_ref = session
                    .active_txn_mut()
                    .ok_or_else(|| QuillSQLError::Execution("no active transaction".to_string()))?;
                let txn_id = txn_ref.id();
                self.transaction_manager.commit(txn_ref)?;
                self.holt_store
                    .put_txn_status(txn_id, TransactionStatus::Committed)?;
                session.clear_active_transaction();
                Ok(Some(vec![]))
            }
            LogicalPlan::RollbackTransaction => {
                let txn_ref = session
                    .active_txn_mut()
                    .ok_or_else(|| QuillSQLError::Execution("no active transaction".to_string()))?;
                let txn_id = txn_ref.id();
                self.transaction_manager.abort(txn_ref)?;
                self.holt_store
                    .put_txn_status(txn_id, TransactionStatus::Aborted)?;
                session.clear_active_transaction();
                Ok(Some(vec![]))
            }
            LogicalPlan::SetTransaction { scope, modes } => {
                match scope {
                    TransactionScope::Session => session.apply_session_modes(modes),
                    TransactionScope::Transaction => session.apply_transaction_modes(modes),
                }
                Ok(Some(vec![]))
            }
            _ => Ok(None),
        }
    }

    fn execute_physical_plan(
        &mut self,
        session: &mut SessionContext,
        physical_plan: PhysicalPlan,
    ) -> QuillSQLResult<Vec<Tuple>> {
        let needs_cleanup = !session.has_active_transaction();
        let autocommit = session.autocommit();
        let result = {
            let txn = session.ensure_active_transaction(&self.transaction_manager)?;
            let context = crate::execution::ExecutionContext::new(
                &mut self.catalog,
                txn,
                self.transaction_manager.clone(),
                self.storage.clone(),
            );
            let mut engine = ExecutionEngine { context };
            engine.execute(Rc::new(physical_plan))?
        };

        if autocommit && needs_cleanup {
            if let Some(txn) = session.active_txn_mut() {
                let txn_id = txn.id();
                self.transaction_manager.commit(txn)?;
                self.holt_store
                    .put_txn_status(txn_id, TransactionStatus::Committed)?;
            }
            session.clear_active_transaction();
        }

        Ok(result)
    }
}

fn holt_directory_for_location(
    location: &DatabaseLocation,
    overrides: &HoltOptions,
) -> QuillSQLResult<(PathBuf, Option<TempDir>)> {
    if let Some(directory) = &overrides.directory {
        return Ok((directory.clone(), None));
    }
    match location {
        DatabaseLocation::OnDisk(path) => Ok((PathBuf::from(path), None)),
        DatabaseLocation::Temporary => {
            let temp_dir = TempDir::new()?;
            let holt_dir = temp_dir.path().join("holt");
            Ok((holt_dir, Some(temp_dir)))
        }
    }
}

fn seed_holt_transaction_statuses(
    holt_store: &HoltStore,
    transaction_manager: &TransactionManager,
) -> QuillSQLResult<()> {
    let mut next_txn_id = 1;
    for (txn_id, status) in holt_store.recover_txn_statuses()? {
        transaction_manager.record_recovered_status(txn_id, status);
        next_txn_id = next_txn_id.max(txn_id.saturating_add(1));
    }
    transaction_manager.ensure_next_txn_id_at_least(next_txn_id);
    Ok(())
}
