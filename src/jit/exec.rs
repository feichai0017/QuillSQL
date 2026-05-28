use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{ArrayRef, Decimal128Array, Float64Array};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, SchemaRef as ArrowSchemaRef};
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

use crate::jit::{
    CompiledKernel, FilterProjectKernel, FilterSumKernel, FilterSumValue, KernelKind,
};

#[derive(Debug, Clone)]
pub struct CompiledRecordPipelineExec {
    input: Arc<dyn ExecutionPlan>,
    runtime: FilterProjectKernel,
    kernel: CompiledKernel,
    schema: ArrowSchemaRef,
    cache: Arc<PlanProperties>,
}

#[derive(Debug, Clone)]
pub struct CompiledAggregatePipelineExec {
    input: Arc<dyn ExecutionPlan>,
    runtime: FilterSumKernel,
    kernel: CompiledKernel,
    schema: ArrowSchemaRef,
    cache: Arc<PlanProperties>,
}

impl CompiledRecordPipelineExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        runtime: FilterProjectKernel,
        schema: ArrowSchemaRef,
        kernel: CompiledKernel,
    ) -> Result<Self> {
        if kernel.kind != KernelKind::FilterProject {
            return Err(DataFusionError::Internal(format!(
                "expected record pipeline kernel, got {:?}",
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
        #[cfg(feature = "jit-mlir")]
        if let Some(output) =
            super::mlir::execute_filter_project(&self.kernel, &self.runtime, &batch)?
        {
            return Ok(output);
        }

        self.runtime.execute(&batch).map_err(Into::into)
    }
}

impl CompiledAggregatePipelineExec {
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
                "CompiledAggregatePipelineExec expected one output field, got {}",
                schema.fields().len()
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

    pub fn kernel(&self) -> &CompiledKernel {
        &self.kernel
    }

    pub fn runtime(&self) -> &FilterSumKernel {
        &self.runtime
    }

    fn execute_batch(&self, batch: &RecordBatch) -> Result<FilterSumValue> {
        #[cfg(feature = "jit-mlir")]
        if let Some(partial) = super::mlir::execute_filter_sum(&self.kernel, &self.runtime, batch)?
        {
            return Ok(partial);
        }

        self.runtime.execute(batch).map_err(Into::into)
    }
}

impl DisplayAs for CompiledRecordPipelineExec {
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
                    "CompiledRecordPipelineExec: backend={}, executable={}, predicate={:?}, expr=[{}]",
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

impl DisplayAs for CompiledAggregatePipelineExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "CompiledAggregatePipelineExec: backend={}, executable={}, predicate={:?}, measure={:?}",
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

impl ExecutionPlan for CompiledRecordPipelineExec {
    fn name(&self) -> &str {
        "CompiledRecordPipelineExec"
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
                "CompiledRecordPipelineExec expected one child, got {}",
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
        Ok(Box::pin(CompiledRecordPipelineStream {
            schema: Arc::clone(&self.schema),
            input: self.input.execute(partition, context)?,
            exec: self.clone(),
        }))
    }
}

impl ExecutionPlan for CompiledAggregatePipelineExec {
    fn name(&self) -> &str {
        "CompiledAggregatePipelineExec"
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
                "CompiledAggregatePipelineExec expected one child, got {}",
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
        Ok(Box::pin(CompiledFilterSumStream {
            schema: Arc::clone(&self.schema),
            input: self.input.execute(partition, context)?,
            exec: self.clone(),
            sum: None,
            emitted: false,
        }))
    }
}

struct CompiledRecordPipelineStream {
    schema: ArrowSchemaRef,
    input: SendableRecordBatchStream,
    exec: CompiledRecordPipelineExec,
}

struct CompiledFilterSumStream {
    schema: ArrowSchemaRef,
    input: SendableRecordBatchStream,
    exec: CompiledAggregatePipelineExec,
    sum: Option<FilterSumValue>,
    emitted: bool,
}

impl Stream for CompiledRecordPipelineStream {
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
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => match self.exec.execute_batch(&batch) {
                    Ok(partial) => {
                        if let Some(sum) = &mut self.sum {
                            if let Err(err) = sum.merge(partial) {
                                return Poll::Ready(Some(Err(err.into())));
                            }
                        } else {
                            self.sum = Some(partial);
                        }
                    }
                    Err(err) => return Poll::Ready(Some(Err(err))),
                },
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                None => {
                    self.emitted = true;
                    return Poll::Ready(Some(finish_filter_sum_batch(
                        Arc::clone(&self.schema),
                        self.sum,
                    )));
                }
            }
        }
    }
}

impl RecordBatchStream for CompiledRecordPipelineStream {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }
}

impl RecordBatchStream for CompiledFilterSumStream {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }
}

fn finish_filter_sum_batch(
    schema: ArrowSchemaRef,
    sum: Option<FilterSumValue>,
) -> Result<RecordBatch> {
    let field = schema.field(0);
    let values = match field.data_type() {
        ArrowDataType::Float64 => {
            let value = match sum {
                Some(FilterSumValue::Float64(value)) => value,
                None => None,
                Some(other) => {
                    return Err(DataFusionError::Execution(format!(
                        "expected f64 sum, got {:?}",
                        other.ty()
                    )));
                }
            };
            Arc::new(Float64Array::from(vec![value])) as ArrayRef
        }
        ArrowDataType::Decimal128(precision, scale) => {
            let value = match sum {
                Some(FilterSumValue::Decimal128 {
                    value,
                    scale: value_scale,
                }) => {
                    if value_scale != *scale {
                        return Err(DataFusionError::Execution(format!(
                            "expected decimal scale {}, got {}",
                            scale, value_scale
                        )));
                    }
                    value
                }
                None => None,
                Some(other) => {
                    return Err(DataFusionError::Execution(format!(
                        "expected decimal sum, got {:?}",
                        other.ty()
                    )));
                }
            };
            Arc::new(
                Decimal128Array::from(vec![value])
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|err| DataFusionError::Execution(err.to_string()))?,
            ) as ArrayRef
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "unsupported filter-sum output type {other:?}"
            )));
        }
    };

    RecordBatch::try_new(schema, vec![values])
        .map_err(|err| DataFusionError::Execution(err.to_string()))
}
