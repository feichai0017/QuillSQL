use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::ProjectionExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::{ready, Stream, StreamExt};

use crate::jit::{CompiledKernel, KernelKind};

#[derive(Debug, Clone)]
pub struct CompiledFilterProjectExec {
    input: Arc<dyn ExecutionPlan>,
    predicate: Arc<dyn PhysicalExpr>,
    projections: Vec<ProjectionExpr>,
    kernel: CompiledKernel,
    schema: ArrowSchemaRef,
    cache: Arc<PlanProperties>,
}

impl CompiledFilterProjectExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        predicate: Arc<dyn PhysicalExpr>,
        projections: Vec<ProjectionExpr>,
        schema: ArrowSchemaRef,
        kernel: CompiledKernel,
    ) -> Result<Self> {
        if kernel.kind != KernelKind::FilterProject {
            return Err(DataFusionError::Internal(format!(
                "expected filter-project kernel, got {:?}",
                kernel.kind
            )));
        }

        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(input.properties().partitioning.partition_count()),
            input.properties().emission_type,
            input.properties().boundedness,
        ));

        Ok(Self {
            input,
            predicate,
            projections,
            kernel,
            schema,
            cache,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn kernel(&self) -> &CompiledKernel {
        &self.kernel
    }

    pub fn projections(&self) -> &[ProjectionExpr] {
        &self.projections
    }

    pub fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate
    }

    fn execute_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        execute_filter_project(
            &batch,
            self.predicate(),
            self.projections(),
            Arc::clone(&self.schema),
        )
    }
}

impl DisplayAs for CompiledFilterProjectExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let projections = self
                    .projections
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "CompiledFilterProjectExec: backend={}, executable={}, predicate={}, expr=[{}]",
                    self.kernel.backend, self.kernel.executable, self.predicate, projections
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(
                    f,
                    "backend={}, executable={}",
                    self.kernel.backend, self.kernel.executable
                )?;
                writeln!(f, "predicate={}", self.predicate)?;
                for (index, projection) in self.projections.iter().enumerate() {
                    writeln!(f, "expr{index}={projection}")?;
                }
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for CompiledFilterProjectExec {
    fn name(&self) -> &str {
        "CompiledFilterProjectExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "CompiledFilterProjectExec expected one child, got {}",
                children.len()
            )));
        }

        Self::try_new(
            children.swap_remove(0),
            Arc::clone(&self.predicate),
            self.projections.clone(),
            Arc::clone(&self.schema),
            self.kernel.clone(),
        )
        .map(|exec| Arc::new(exec) as Arc<dyn ExecutionPlan>)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(CompiledFilterProjectStream {
            schema: Arc::clone(&self.schema),
            input: self.input.execute(partition, context)?,
            exec: self.clone(),
        }))
    }
}

struct CompiledFilterProjectStream {
    schema: ArrowSchemaRef,
    input: SendableRecordBatchStream,
    exec: CompiledFilterProjectExec,
}

impl Stream for CompiledFilterProjectStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => Poll::Ready(Some(self.exec.execute_batch(batch))),
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for CompiledFilterProjectStream {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }
}

fn execute_filter_project(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
    projections: &[ProjectionExpr],
    schema: ArrowSchemaRef,
) -> Result<RecordBatch> {
    let predicate = predicate.evaluate(batch)?.into_array(batch.num_rows())?;
    let filter = as_boolean_array(&predicate)?;
    let filtered = filter_record_batch(batch, filter)?;

    let columns = projections
        .iter()
        .map(|projection| evaluate_projection(projection, &filtered))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, columns).map_err(Into::into)
}

fn evaluate_projection(projection: &ProjectionExpr, batch: &RecordBatch) -> Result<ArrayRef> {
    match projection.expr.evaluate(batch)? {
        ColumnarValue::Array(array) => Ok(array),
        ColumnarValue::Scalar(value) => value.to_array_of_size(batch.num_rows()),
    }
}
