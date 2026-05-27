use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, UInt64Array};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream;

use crate::catalog::SchemaRef as QuillSchemaRef;
use crate::utils::table_ref::TableReference;

use super::arrow::{project_arrow_schema, project_batch, record_batch_from_tuples};
use super::filter::{indexed_filter_for_expr, ResolvedIndexFilter};
use super::{map_quill_to_df, EngineState};

#[derive(Debug)]
pub(super) struct HoltScanExec {
    state: EngineState,
    table_ref: TableReference,
    schema: QuillSchemaRef,
    arrow_schema: ArrowSchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    properties: Arc<PlanProperties>,
    table_id: u64,
}

impl HoltScanExec {
    pub(super) fn new(
        state: EngineState,
        table_ref: TableReference,
        schema: QuillSchemaRef,
        arrow_schema: ArrowSchemaRef,
        table_id: u64,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
    ) -> Self {
        let projected = project_arrow_schema(arrow_schema.clone(), projection.as_deref());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(projected),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            state,
            table_ref,
            schema,
            arrow_schema,
            projection,
            filters,
            properties,
            table_id,
        }
    }
}

impl DisplayAs for HoltScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => write!(
                f,
                "HoltScanExec: table={}, table_id={}",
                self.table_ref.to_log_string(),
                self.table_id
            ),
        }
    }
}

impl ExecutionPlan for HoltScanExec {
    fn name(&self) -> &str {
        "HoltScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "HoltScanExec does not accept children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "invalid HoltScanExec partition {partition}"
            )));
        }

        let rows = if let Some(index_filter) = self.index_filter() {
            let binding = self
                .state
                .table_binding(&self.table_ref)
                .map_err(map_quill_to_df)?;
            let mut stream = binding
                .index_scan(&index_filter.index_name, index_filter.request)
                .map_err(map_quill_to_df)?;
            self.state
                .visible_rows_from_stream(stream.as_mut())
                .map_err(map_quill_to_df)?
        } else {
            self.state
                .visible_rows(&self.table_ref)
                .map_err(map_quill_to_df)?
        };
        let tuples = rows.into_iter().map(|row| row.tuple).collect::<Vec<_>>();
        let batch =
            record_batch_from_tuples(self.arrow_schema.clone(), self.schema.as_ref(), &tuples)
                .map_err(map_quill_to_df)?;
        let projected = project_batch(&batch, self.projection.as_deref())?;
        let schema = projected.schema();
        let batches = if projected.num_rows() == 0 {
            Vec::new()
        } else {
            vec![Ok(projected)]
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(batches),
        )))
    }
}

impl HoltScanExec {
    fn index_filter(&self) -> Option<ResolvedIndexFilter> {
        self.filters.iter().find_map(|filter| {
            indexed_filter_for_expr(self.schema.as_ref(), &self.state, &self.table_ref, filter)
        })
    }
}

#[derive(Debug)]
pub(super) struct DmlResultExec {
    rows_affected: u64,
    schema: ArrowSchemaRef,
    properties: Arc<PlanProperties>,
}

impl DmlResultExec {
    pub(super) fn new(rows_affected: u64) -> Self {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "count",
            ArrowDataType::UInt64,
            false,
        )]));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            rows_affected,
            schema,
            properties,
        }
    }
}

impl DisplayAs for DmlResultExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "DmlResultExec: rows_affected={}", self.rows_affected)
            }
        }
    }
}

impl ExecutionPlan for DmlResultExec {
    fn name(&self) -> &str {
        "DmlResultExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "DmlResultExec does not accept children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "invalid DmlResultExec partition {partition}"
            )));
        }
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![self.rows_affected])) as ArrayRef],
        )?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream::iter(vec![Ok(batch)]),
        )))
    }
}
