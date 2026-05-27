use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use serde::Serialize;

use crate::jit::{JitExpr, JitProjection, KernelBackend, KernelKind, MlirBackend};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JitCandidate {
    pub node: &'static str,
    pub kernel: KernelKind,
    pub backend: &'static str,
    pub executable: bool,
}

#[derive(Debug, Default)]
pub struct MlirJitRule {
    backend: MlirBackend,
}

impl MlirJitRule {
    pub fn new() -> Self {
        Self {
            backend: MlirBackend::new(),
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

    fn inspect_node(&self, plan: &Arc<dyn ExecutionPlan>) -> Option<JitCandidate> {
        if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
            let predicate =
                JitExpr::from_physical(filter.predicate(), filter.input().schema().as_ref())
                    .ok()?;
            let kernel = self
                .backend
                .compile_filter(filter.input().schema(), &predicate)
                .ok()?;
            return Some(JitCandidate {
                node: "FilterExec",
                kernel: KernelKind::Filter,
                backend: "mlir",
                executable: kernel.executable,
            });
        }

        if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
            let input_schema = projection.input().schema();
            let projections = lower_projection_exprs(projection, input_schema.as_ref())?;

            if let Some(filter) = projection.input().as_any().downcast_ref::<FilterExec>() {
                let predicate =
                    JitExpr::from_physical(filter.predicate(), filter.input().schema().as_ref())
                        .ok()?;
                let kernel = self
                    .backend
                    .compile_filter_project(filter.input().schema(), &predicate, &projections)
                    .ok()?;
                return Some(JitCandidate {
                    node: "FilterProjectExec",
                    kernel: KernelKind::FilterProject,
                    backend: "mlir",
                    executable: kernel.executable,
                });
            }

            let kernel = self
                .backend
                .compile_projection(input_schema, &projections)
                .ok()?;
            return Some(JitCandidate {
                node: "ProjectionExec",
                kernel: KernelKind::Projection,
                backend: "mlir",
                executable: kernel.executable,
            });
        }

        None
    }
}

fn lower_projection_exprs(
    projection: &ProjectionExec,
    input_schema: &datafusion::arrow::datatypes::Schema,
) -> Option<Vec<JitProjection>> {
    projection
        .expr()
        .iter()
        .map(|expr| {
            JitExpr::from_physical(&expr.expr, input_schema)
                .map(|jit_expr| JitProjection::new(jit_expr, expr.alias.clone()))
        })
        .collect::<crate::jit::JitResult<Vec<_>>>()
        .ok()
}

impl PhysicalOptimizerRule for MlirJitRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let _candidates = self.inspect_plan(Arc::clone(&plan));
        Ok(plan)
    }

    fn name(&self) -> &str {
        "mlir_jit"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
