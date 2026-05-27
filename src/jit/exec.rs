use std::any::Any;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{ArrayRef, Float64Array};
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

use crate::jit::{CompiledKernel, FilterProjectKernel, FilterSumKernel, KernelKind};

#[derive(Debug, Clone)]
pub struct CompiledFilterProjectExec {
    input: Arc<dyn ExecutionPlan>,
    runtime: FilterProjectKernel,
    kernel: CompiledKernel,
    schema: ArrowSchemaRef,
    cache: Arc<PlanProperties>,
}

#[derive(Debug, Clone)]
pub struct CompiledFilterSumExec {
    input: Arc<dyn ExecutionPlan>,
    runtime: FilterSumKernel,
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

impl CompiledFilterSumExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        runtime: FilterSumKernel,
        schema: ArrowSchemaRef,
        kernel: CompiledKernel,
    ) -> Result<Self> {
        if kernel.kind != KernelKind::FilterSum {
            return Err(DataFusionError::Internal(format!(
                "expected filter-sum kernel, got {:?}",
                kernel.kind
            )));
        }
        if schema.fields().len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "CompiledFilterSumExec expected one output field, got {}",
                schema.fields().len()
            )));
        }

        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
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

    pub fn kernel(&self) -> &CompiledKernel {
        &self.kernel
    }

    pub fn runtime(&self) -> &FilterSumKernel {
        &self.runtime
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

impl DisplayAs for CompiledFilterSumExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "CompiledFilterSumExec: backend={}, executable={}, predicate={:?}, measure={:?}",
                    self.kernel.backend,
                    self.kernel.executable,
                    self.runtime.predicate(),
                    self.runtime.measure()
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(
                    f,
                    "backend={}, executable={}",
                    self.kernel.backend, self.kernel.executable
                )?;
                writeln!(f, "predicate={:?}", self.runtime.predicate())?;
                writeln!(f, "measure={:?}", self.runtime.measure())
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

impl ExecutionPlan for CompiledFilterSumExec {
    fn name(&self) -> &str {
        "CompiledFilterSumExec"
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
        vec![false]
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
                "CompiledFilterSumExec expected one child, got {}",
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
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "CompiledFilterSumExec has one output partition, got request for {partition}"
            )));
        }

        let input_partitions = self.input.properties().partitioning.partition_count();
        let mut inputs = VecDeque::with_capacity(input_partitions);
        for input_partition in 0..input_partitions {
            inputs.push_back(self.input.execute(input_partition, Arc::clone(&context))?);
        }

        Ok(Box::pin(CompiledFilterSumStream {
            schema: Arc::clone(&self.schema),
            inputs,
            exec: self.clone(),
            sum: None,
            emitted: false,
        }))
    }
}

struct CompiledFilterProjectStream {
    schema: ArrowSchemaRef,
    input: SendableRecordBatchStream,
    exec: CompiledFilterProjectExec,
}

struct CompiledFilterSumStream {
    schema: ArrowSchemaRef,
    inputs: VecDeque<SendableRecordBatchStream>,
    exec: CompiledFilterSumExec,
    sum: Option<f64>,
    emitted: bool,
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

impl Stream for CompiledFilterSumStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.emitted {
            return Poll::Ready(None);
        }

        loop {
            let Some(input) = self.inputs.front_mut() else {
                self.emitted = true;
                let values = Arc::new(Float64Array::from(vec![self.sum])) as ArrayRef;
                let batch = RecordBatch::try_new(Arc::clone(&self.schema), vec![values])
                    .map_err(|err| DataFusionError::Execution(err.to_string()));
                return Poll::Ready(Some(batch));
            };

            match ready!(input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => match self.exec.runtime.execute(&batch) {
                    Ok(Some(partial)) => {
                        self.sum = Some(self.sum.unwrap_or(0.0) + partial);
                    }
                    Ok(None) => {}
                    Err(err) => return Poll::Ready(Some(Err(err.into()))),
                },
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                None => {
                    self.inputs.pop_front();
                }
            }
        }
    }
}

impl RecordBatchStream for CompiledFilterProjectStream {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }
}

impl RecordBatchStream for CompiledFilterSumStream {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }
}
