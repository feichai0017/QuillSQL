use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::{ready, Stream, StreamExt};

use crate::jit::{CompiledKernel, FilterProjectKernel, KernelKind};

#[derive(Debug, Clone)]
pub struct CompiledFilterProjectExec {
    input: Arc<dyn ExecutionPlan>,
    runtime: FilterProjectKernel,
    kernel: CompiledKernel,
    schema: ArrowSchemaRef,
    cache: Arc<PlanProperties>,
}

impl CompiledFilterProjectExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        runtime: FilterProjectKernel,
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
            runtime,
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

    pub fn runtime(&self) -> &FilterProjectKernel {
        &self.runtime
    }

    fn execute_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        self.runtime.execute(&batch).map_err(Into::into)
    }
}

impl DisplayAs for CompiledFilterProjectExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let projections = self
                    .runtime
                    .projections()
                    .iter()
                    .map(|projection| projection.alias.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "CompiledFilterProjectExec: backend={}, executable={}, predicate={:?}, expr=[{}]",
                    self.kernel.backend,
                    self.kernel.executable,
                    self.runtime.predicate(),
                    projections
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(
                    f,
                    "backend={}, executable={}",
                    self.kernel.backend, self.kernel.executable
                )?;
                writeln!(f, "predicate={:?}", self.runtime.predicate())?;
                for (index, projection) in self.runtime.projections().iter().enumerate() {
                    writeln!(f, "expr{index}={}", projection.alias)?;
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
            self.runtime.clone(),
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
