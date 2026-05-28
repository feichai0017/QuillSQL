use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use serde::Serialize;

use crate::{CompiledPipelineExec, JitOptions, KernelKind, MlirBackend, PipelineCandidate};

use super::{extract_pipeline_from_node, pipeline_from_node};
use crate::lower::PipelineCompiler;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JitCandidate {
    pub node: &'static str,
    pub kernel: KernelKind,
    pub backend: String,
    pub executable: bool,
}

#[derive(Debug, Default)]
pub struct MlirJitRule {
    backend: MlirBackend,
    options: JitOptions,
}

impl MlirJitRule {
    pub fn new() -> Self {
        Self::with_options(JitOptions::default())
    }

    pub fn with_options(options: JitOptions) -> Self {
        Self {
            backend: MlirBackend::new(),
            options,
        }
    }

    pub fn enabled() -> bool {
        cfg!(feature = "jit-mlir")
    }

    pub fn inspect_plan(&self, plan: Arc<dyn ExecutionPlan>) -> Vec<JitCandidate> {
        let mut candidates = Vec::new();
        let _ = plan.transform_down(|plan| {
            if let Some(candidate) = self.inspect_node(&plan) {
                candidates.push(candidate);
            }
            Ok(Transformed::no(plan))
        });
        candidates
    }

    pub fn inspect_pipelines(&self, plan: Arc<dyn ExecutionPlan>) -> Vec<PipelineCandidate> {
        let mut candidates = Vec::new();
        let _ = plan.transform_down(|plan| {
            if let Some(candidate) =
                pipeline_from_node(&plan).and_then(|pipeline| pipeline.candidate())
            {
                candidates.push(candidate);
            }
            Ok(Transformed::no(plan))
        });
        candidates
    }

    fn inspect_node(&self, plan: &Arc<dyn ExecutionPlan>) -> Option<JitCandidate> {
        if let Some(compiled) = plan.as_any().downcast_ref::<CompiledPipelineExec>() {
            return Some(JitCandidate {
                node: "CompiledPipelineExec",
                kernel: compiled.kernel().kind,
                backend: compiled.kernel().backend.clone(),
                executable: compiled.kernel().executable,
            });
        }

        None
    }
}

impl PhysicalOptimizerRule for MlirJitRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !self.options.enabled() {
            return Ok(plan);
        }

        plan.transform_up(|plan| self.try_compile_node(plan))
            .map(|transformed| transformed.data)
    }

    fn name(&self) -> &str {
        "mlir_jit"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl MlirJitRule {
    fn try_compile_node(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(pipeline) = extract_pipeline_from_node(&plan) else {
            return Ok(Transformed::no(plan));
        };

        match PipelineCompiler::new(&self.backend, &self.options).compile(pipeline)? {
            Some(compiled) => Ok(Transformed::yes(compiled)),
            None => Ok(Transformed::no(plan)),
        }
    }
}
