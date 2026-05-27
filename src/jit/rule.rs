use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlan;
use serde::Serialize;

use crate::jit::{
    CompiledFilterProjectExec, JitExpr, JitProjection, KernelBackend, KernelKind, MlirBackend,
};

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
        if let Some(compiled) = plan.as_any().downcast_ref::<CompiledFilterProjectExec>() {
            return Some(JitCandidate {
                node: "CompiledFilterProjectExec",
                kernel: compiled.kernel().kind,
                backend: "mlir",
                executable: compiled.kernel().executable,
            });
        }

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
        let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() else {
            return Ok(Transformed::no(plan));
        };

        if let Some(filter) = projection.input().as_any().downcast_ref::<FilterExec>() {
            if let Some(compiled) = self.compile_filter_project(filter, projection)? {
                return Ok(Transformed::yes(compiled));
            }
            return Ok(Transformed::no(plan));
        }

        let Some(repartition) = projection
            .input()
            .as_any()
            .downcast_ref::<RepartitionExec>()
        else {
            return Ok(Transformed::no(plan));
        };
        if !matches!(repartition.partitioning(), Partitioning::RoundRobinBatch(_)) {
            return Ok(Transformed::no(plan));
        }
        let Some(filter) = repartition.input().as_any().downcast_ref::<FilterExec>() else {
            return Ok(Transformed::no(plan));
        };
        let Some(compiled) = self.compile_filter_project(filter, projection)? else {
            return Ok(Transformed::no(plan));
        };

        let mut new_repartition =
            RepartitionExec::try_new(compiled, repartition.partitioning().clone())?;
        if repartition.preserve_order() {
            new_repartition = new_repartition.with_preserve_order();
        }
        Ok(Transformed::yes(
            Arc::new(new_repartition) as Arc<dyn ExecutionPlan>
        ))
    }

    fn compile_filter_project(
        &self,
        filter: &FilterExec,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if filter.projection().is_some() || filter.fetch().is_some() {
            return Ok(None);
        }

        let input_schema = filter.input().schema();
        let Some(projections) = lower_projection_exprs(projection, input_schema.as_ref()) else {
            return Ok(None);
        };
        let predicate = match JitExpr::from_physical(filter.predicate(), input_schema.as_ref()) {
            Ok(predicate) => predicate,
            Err(_) => return Ok(None),
        };
        let kernel = match self.backend.compile_filter_project(
            Arc::clone(&input_schema),
            &predicate,
            &projections,
        ) {
            Ok(kernel) => kernel,
            Err(_) => return Ok(None),
        };

        let compiled = CompiledFilterProjectExec::try_new(
            Arc::clone(filter.input()),
            Arc::clone(filter.predicate()),
            projection.expr().to_vec(),
            projection.schema(),
            kernel,
        )?;
        Ok(Some(Arc::new(compiled) as Arc<dyn ExecutionPlan>))
    }
}
