use std::sync::Arc;

use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;

use crate::jit::{
    CompiledAggregatePipelineExec, CompiledKernel, FilterSumKernel, JitExpr, JitOptions,
    KernelBackend, KernelIr, KernelKind, MlirBackend,
};

use super::pipeline::PhysicalPipeline;

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
        match pipeline.ir.first_kernel() {
            Some(KernelIr::FilterSum { predicate, measure }) => {
                let runtime = match FilterSumKernel::try_new(predicate.clone(), measure.clone()) {
                    Ok(runtime) => runtime,
                    Err(_) => return Ok(None),
                };
                let kernel = self.filter_sum_kernel(&predicate, &measure);
                let exec = CompiledAggregatePipelineExec::try_new(
                    pipeline.input,
                    runtime,
                    pipeline.output_schema,
                    kernel,
                )?;
                Ok(Some(Arc::new(exec) as Arc<dyn ExecutionPlan>))
            }
            _ => Ok(None),
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
