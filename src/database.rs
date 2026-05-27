use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::LogicalPlan;
use parking_lot::Mutex;
use serde::Serialize;
use sqlparser::ast::TransactionAccessMode;
use tempfile::TempDir;

use crate::catalog::{load_catalog_data, Catalog};
use crate::df::{
    execute_logical_plan, pretty_format_batches, record_batches_to_string_rows, EngineState,
};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::holt::{map_holt_err, HoltStore, HoltTableHandle};
use crate::transaction::{
    CommandId, IsolationLevel, LockDebugSnapshot, TransactionManager, TxnDebugSnapshot,
};

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
    state: EngineState,
    debug_trace: Arc<Mutex<Option<DebugTrace>>>,
    _temp_dir: Option<TempDir>,
}

#[derive(Debug, Clone)]
pub struct QueryOutput {
    pub batches: Vec<RecordBatch>,
}

impl QueryOutput {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }

    pub fn is_empty(&self) -> bool {
        self.batches.iter().all(|batch| batch.num_rows() == 0)
    }

    pub fn rows_as_strings(&self) -> Vec<Vec<String>> {
        record_batches_to_string_rows(&self.batches)
    }

    pub fn pretty_table(&self) -> QuillSQLResult<comfy_table::Table> {
        pretty_format_batches(&self.batches)
    }
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
    fn from_logical(plan: &LogicalPlan) -> Self {
        Self {
            op: plan.display().to_string(),
            children: plan
                .inputs()
                .iter()
                .map(|child| Self::from_logical(child))
                .collect(),
        }
    }

    fn leaf(op: impl Into<String>) -> Self {
        Self {
            op: op.into(),
            children: Vec::new(),
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

        let mut catalog = Catalog::new(holt_store.clone());
        load_catalog_data(&mut catalog)?;

        let state = EngineState {
            catalog: Arc::new(Mutex::new(catalog)),
            holt_store,
            transaction_manager,
            active_txn: Arc::new(Mutex::new(None)),
            default_isolation: options
                .default_isolation_level
                .unwrap_or(IsolationLevel::ReadUncommitted),
        };

        Ok(Self {
            state,
            debug_trace: Arc::new(Mutex::new(None)),
            _temp_dir: temp_dir,
        })
    }

    pub async fn run(&self, sql: &str) -> QuillSQLResult<QueryOutput> {
        let start = Instant::now();
        if self.try_run_transaction_control(sql)? {
            self.record_trace(
                "TransactionControl".to_string(),
                "TransactionControl".to_string(),
                0,
                start.elapsed().as_millis(),
                DebugPlanNode::leaf("TransactionControl"),
            );
            return Ok(QueryOutput::new(Vec::new()));
        }

        let ctx = self.state.session_context();
        let logical_plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(map_datafusion_err)?;
        let logical_plan_str = logical_plan.display_indent().to_string();
        let logical_tree = DebugPlanNode::from_logical(&logical_plan);

        let batches = execute_logical_plan(&self.state, logical_plan.clone()).await?;
        let rows = batches.iter().map(|batch| batch.num_rows()).sum();
        self.record_trace(
            logical_plan_str,
            "DataFusion physical plan".to_string(),
            rows,
            start.elapsed().as_millis(),
            logical_tree,
        );
        Ok(QueryOutput::new(batches))
    }

    pub fn flush(&self) -> QuillSQLResult<()> {
        self.state
            .holt_store
            .db()
            .checkpoint()
            .map_err(map_holt_err)
    }

    pub fn debug_last_trace(&self) -> Option<DebugTrace> {
        self.debug_trace.lock().clone()
    }

    pub fn debug_lock_snapshot(&self) -> LockDebugSnapshot {
        self.state
            .transaction_manager
            .lock_manager_arc()
            .debug_snapshot()
    }

    pub fn debug_txn_snapshot(&self) -> TxnDebugSnapshot {
        self.state.transaction_manager.debug_snapshot()
    }

    pub fn debug_mvcc_versions(&self) -> QuillSQLResult<MvccVersionsDebug> {
        let snapshot = self
            .state
            .transaction_manager
            .snapshot(self.state.transaction_manager.next_txn_id_hint());
        let catalog = self.state.catalog.lock();
        let mut samples = Vec::new();
        let max_samples = 20usize;
        for (schema_name, schema) in &catalog.schemas {
            for (table_name, table) in &schema.tables {
                let table_id = table.table_id;
                let table_ref = crate::utils::table_ref::TableReference::Full {
                    catalog: crate::catalog::DEFAULT_CATALOG_NAME.to_string(),
                    schema: schema_name.clone(),
                    table: table_name.clone(),
                };
                let handle = HoltTableHandle::new(
                    table_ref.clone(),
                    table.schema.clone(),
                    table_id,
                    self.state.holt_store.clone(),
                );
                let mut stream = crate::storage::TableHandle::full_scan(&handle)?;
                while let Some((rid, meta, _tuple)) = stream.next()? {
                    if samples.len() >= max_samples {
                        break;
                    }
                    let visible = snapshot.is_visible(&meta, 0 as CommandId, |tid| {
                        self.state.transaction_manager.transaction_status(tid)
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
            .clone()
            .map(|trace| DebugPlanSnapshot {
                logical: trace.logical_tree,
                physical: trace.physical_tree,
            })
    }

    fn try_run_transaction_control(&self, sql: &str) -> QuillSQLResult<bool> {
        let normalized = sql
            .trim()
            .trim_end_matches(';')
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        match normalized.as_str() {
            "begin" | "begin transaction" | "start transaction" => {
                self.state.begin_transaction(
                    self.state.default_isolation,
                    TransactionAccessMode::ReadWrite,
                )?;
                Ok(true)
            }
            "begin read only" | "begin transaction read only" | "start transaction read only" => {
                self.state.begin_transaction(
                    self.state.default_isolation,
                    TransactionAccessMode::ReadOnly,
                )?;
                Ok(true)
            }
            "commit" | "commit transaction" => {
                self.state.commit_transaction()?;
                Ok(true)
            }
            "rollback" | "rollback transaction" => {
                self.state.rollback_transaction()?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn record_trace(
        &self,
        logical_plan: String,
        physical_plan: String,
        rows: usize,
        duration_ms: u128,
        logical_tree: DebugPlanNode,
    ) {
        *self.debug_trace.lock() = Some(DebugTrace {
            physical_tree: DebugPlanNode::leaf(physical_plan.clone()),
            logical_plan,
            physical_plan,
            rows,
            duration_ms,
            logical_tree,
        });
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

fn map_datafusion_err(err: datafusion::error::DataFusionError) -> QuillSQLError {
    QuillSQLError::Execution(format!("DataFusion error: {err}"))
}
