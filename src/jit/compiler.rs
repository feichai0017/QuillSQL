use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::Result;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::jit::{
    CompiledAggregatePipelineExec, CompiledKernel, CompiledRecordPipelineExec, FilterProjectKernel,
    FilterSumKernel, JitExpr, JitOptions, JitProjection, KernelBackend, KernelKind, MlirBackend,
    PipelineLowering,
};

use super::pipeline::{OutputAdapter, PhysicalPipeline};

#[derive(Debug)]
pub(crate) struct PipelineCompiler<'a> {
    backend: &'a MlirBackend,
    options: &'a JitOptions,
}

impl<'a> PipelineCompiler<'a> {
    pub fn new(backend: &'a MlirBackend, options: &'a JitOptions) -> Self {
        Self { backend, options }
    }

    pub fn compile(&self, pipeline: PhysicalPipeline) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let PhysicalPipeline {
            input,
            output_schema,
            ir,
            output_adapter,
        } = pipeline;

        match PipelineLowering::from_ir(&ir) {
            Some(PipelineLowering::Record {
                predicate,
                projections,
            }) => {
                let runtime = match FilterProjectKernel::try_new(
                    predicate.clone(),
                    projections.clone(),
                    Arc::clone(&output_schema),
                ) {
                    Ok(runtime) => runtime,
                    Err(_) => return Ok(None),
                };
                let kernel = self.filter_project_kernel(&predicate, &projections);
                let exec =
                    CompiledRecordPipelineExec::try_new(input, runtime, output_schema, kernel)?;
                let exec = Arc::new(exec) as Arc<dyn ExecutionPlan>;
                Self::apply_output_adapter(exec, output_adapter).map(Some)
            }
            Some(PipelineLowering::PlainSum { predicate, measure }) => {
                let runtime = match FilterSumKernel::try_new(predicate.clone(), measure.clone()) {
                    Ok(runtime) => runtime,
                    Err(_) => return Ok(None),
                };
                let kernel = self.filter_sum_kernel(&predicate, &measure);
                let exec =
                    CompiledAggregatePipelineExec::try_new(input, runtime, output_schema, kernel)?;
                Ok(Some(Arc::new(exec) as Arc<dyn ExecutionPlan>))
            }
            _ => Ok(None),
        }
    }

    fn apply_output_adapter(
        exec: Arc<dyn ExecutionPlan>,
        adapter: Option<OutputAdapter>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(adapter) = adapter else {
            return Ok(exec);
        };

        let mut repartition = RepartitionExec::try_new(exec, adapter.partitioning)?;
        if adapter.preserve_order {
            repartition = repartition.with_preserve_order();
        }
        Ok(Arc::new(repartition) as Arc<dyn ExecutionPlan>)
    }

    fn filter_project_kernel(
        &self,
        predicate: &JitExpr,
        projections: &[JitProjection],
    ) -> CompiledKernel {
        if let Ok(module) = self
            .backend
            .lower_i64_filter_project(predicate, projections)
        {
            let executable = self.backend.verify_module(&module).is_ok()
                && self.options.mlir_execution_enabled();
            return CompiledKernel::new(
                module.symbol,
                KernelKind::FilterProject,
                self.backend.name(),
                module.text,
                executable,
            );
        }

        match self.backend.compile_filter_project(
            Arc::new(ArrowSchema::empty()),
            predicate,
            projections,
        ) {
            Ok(kernel) => kernel,
            Err(_) => CompiledKernel::new(
                "filter_project_runtime",
                KernelKind::FilterProject,
                "fixed-width-runtime",
                format!("predicate={predicate:?}; projections={projections:?}"),
                false,
            ),
        }
    }

    fn filter_sum_kernel(&self, predicate: &JitExpr, measure: &JitExpr) -> CompiledKernel {
        if let Ok(module) = self.backend.lower_f64_filter_sum(predicate, measure) {
            return CompiledKernel::new(
                module.symbol,
                KernelKind::FilterSum,
                self.backend.name(),
                module.text,
                self.options.mlir_execution_enabled(),
            );
        }

        if let Ok(module) = self.backend.lower_decimal_filter_sum(predicate, measure) {
            return CompiledKernel::new(
                module.symbol,
                KernelKind::FilterSum,
                self.backend.name(),
                module.text,
                self.options.mlir_execution_enabled(),
            );
        }

        CompiledKernel::new(
            "fixed_width_filter_sum",
            KernelKind::FilterSum,
            "fixed-width-runtime",
            format!("predicate={predicate:?}; measure={measure:?}"),
            false,
        )
    }
}
