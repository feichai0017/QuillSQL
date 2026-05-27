use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::file_format::options::ParquetReadOptions;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::displayable;
use serde::Serialize;
use tempfile::TempDir;

use crate::error::{QuillSQLError, QuillSQLResult};

#[derive(Debug, Clone, Default)]
pub struct DatabaseOptions {
    pub data_dir: Option<PathBuf>,
}

pub struct Database {
    ctx: SessionContext,
    debug_trace: Arc<Mutex<Option<DebugTrace>>>,
    _temp_dir: Option<TempDir>,
    _data_dir: PathBuf,
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
    pub fn new(options: DatabaseOptions) -> QuillSQLResult<Self> {
        match options.data_dir {
            Some(data_dir) => Self::new_with_data_dir(data_dir),
            None => Self::new_temp(),
        }
    }

    pub fn new_with_data_dir(data_dir: impl Into<PathBuf>) -> QuillSQLResult<Self> {
        Self::open(data_dir.into(), None)
    }

    pub fn new_temp() -> QuillSQLResult<Self> {
        let temp_dir = TempDir::new()?;
        let data_dir = temp_dir.path().join("data");
        Self::open(data_dir, Some(temp_dir))
    }

    fn open(data_dir: PathBuf, temp_dir: Option<TempDir>) -> QuillSQLResult<Self> {
        std::fs::create_dir_all(&data_dir)?;

        let config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema("datafusion", "public");
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(crate::jit::MlirJitRule::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);

        Ok(Self {
            ctx,
            debug_trace: Arc::new(Mutex::new(None)),
            _temp_dir: temp_dir,
            _data_dir: data_dir,
        })
    }

    pub async fn run(&self, sql: &str) -> QuillSQLResult<QueryOutput> {
        let start = Instant::now();
        let logical_plan = self
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(map_datafusion_err)?;
        let logical_tree = DebugPlanNode::from_logical(&logical_plan);
        let logical_plan_str = logical_plan.display_indent().to_string();
        let physical_plan = self
            .ctx
            .state()
            .create_physical_plan(&logical_plan)
            .await
            .ok();
        let physical_plan_str = physical_plan
            .as_ref()
            .map(|plan| displayable(plan.as_ref()).indent(false).to_string())
            .unwrap_or_else(|| "DataFusion physical plan unavailable for this statement".into());

        let df = self
            .ctx
            .execute_logical_plan(logical_plan)
            .await
            .map_err(map_datafusion_err)?;
        let batches = df.collect().await.map_err(map_datafusion_err)?;
        let rows = batches.iter().map(|batch| batch.num_rows()).sum();
        self.record_trace(
            logical_plan_str,
            physical_plan_str.clone(),
            rows,
            start.elapsed().as_millis(),
            logical_tree,
            DebugPlanNode::leaf(physical_plan_str),
        );
        Ok(QueryOutput::new(batches))
    }

    pub async fn register_parquet(&self, table: &str, path: &str) -> QuillSQLResult<()> {
        self.ctx
            .register_parquet(table, path, ParquetReadOptions::default())
            .await
            .map_err(map_datafusion_err)
    }

    pub fn flush(&self) -> QuillSQLResult<()> {
        Ok(())
    }

    pub fn debug_last_trace(&self) -> Option<DebugTrace> {
        self.debug_trace.lock().expect("debug trace lock").clone()
    }

    pub fn debug_last_plan(&self) -> Option<DebugPlanSnapshot> {
        self.debug_trace
            .lock()
            .expect("debug trace lock")
            .clone()
            .map(|trace| DebugPlanSnapshot {
                logical: trace.logical_tree,
                physical: trace.physical_tree,
            })
    }

    fn record_trace(
        &self,
        logical_plan: String,
        physical_plan: String,
        rows: usize,
        duration_ms: u128,
        logical_tree: DebugPlanNode,
        physical_tree: DebugPlanNode,
    ) {
        *self.debug_trace.lock().expect("debug trace lock") = Some(DebugTrace {
            logical_plan,
            physical_plan,
            rows,
            duration_ms,
            logical_tree,
            physical_tree,
        });
    }
}

fn record_batches_to_string_rows(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(
                        |array| match ScalarValue::try_from_array(array.as_ref(), row_idx) {
                            Ok(value) => value.to_string(),
                            Err(err) => format!("<error: {err}>"),
                        },
                    )
                    .collect(),
            );
        }
    }
    rows
}

fn pretty_format_batches(batches: &[RecordBatch]) -> QuillSQLResult<comfy_table::Table> {
    let mut table = comfy_table::Table::new();
    table.load_preset("||--+-++|    ++++++");
    let Some(first) = batches.first() else {
        return Ok(table);
    };
    table.set_header(
        first
            .schema()
            .fields()
            .iter()
            .map(|field| comfy_table::Cell::new(field.name()))
            .collect::<Vec<_>>(),
    );
    for row in record_batches_to_string_rows(batches) {
        table.add_row(row);
    }
    Ok(table)
}

fn map_datafusion_err(err: datafusion::error::DataFusionError) -> QuillSQLError {
    QuillSQLError::Execution(format!("DataFusion error: {err}"))
}
