use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
#[allow(deprecated)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlan;
use serde::Serialize;

use crate::jit::{
    CompiledFilterProjectExec, CompiledFilterSumExec, CompiledKernel, FilterProjectKernel,
    FilterSumKernel, JitExpr, JitProjection, KernelBackend, KernelKind, MlirBackend,
};

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
                backend: compiled.kernel().backend.clone(),
                executable: compiled.kernel().executable,
            });
        }

        if let Some(compiled) = plan.as_any().downcast_ref::<CompiledFilterSumExec>() {
            return Some(JitCandidate {
                node: "CompiledFilterSumExec",
                kernel: compiled.kernel().kind,
                backend: compiled.kernel().backend.clone(),
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
                backend: "mlir".to_string(),
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
                    backend: "mlir".to_string(),
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
                backend: "mlir".to_string(),
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
        if let Some(aggregate) = plan.as_any().downcast_ref::<AggregateExec>() {
            if let Some(compiled) = self.compile_filter_sum(aggregate)? {
                return Ok(Transformed::yes(compiled));
            }
            return Ok(Transformed::no(plan));
        }

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

        let runtime =
            match FilterProjectKernel::try_new(predicate, projections, projection.schema()) {
                Ok(runtime) => runtime,
                Err(_) => return Ok(None),
            };
        let compiled = CompiledFilterProjectExec::try_new(
            Arc::clone(filter.input()),
            runtime,
            projection.schema(),
            kernel,
        )?;
        Ok(Some(Arc::new(compiled) as Arc<dyn ExecutionPlan>))
    }

    fn compile_filter_sum(
        &self,
        aggregate: &AggregateExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if *aggregate.mode() != AggregateMode::Partial
            || !aggregate.group_expr().is_true_no_grouping()
            || aggregate.aggr_expr().len() != 1
            || aggregate.filter_expr().iter().any(Option::is_some)
            || aggregate.schema().fields().len() != 1
            || !is_supported_sum_output(aggregate.schema().field(0).data_type())
        {
            return Ok(None);
        }

        let input = strip_filter_sum_adapters(aggregate.input());
        let Some(filter) = input.as_any().downcast_ref::<FilterExec>() else {
            return Ok(None);
        };
        if filter.fetch().is_some() {
            return Ok(None);
        }

        let predicate =
            match JitExpr::from_physical(filter.predicate(), filter.input().schema().as_ref()) {
                Ok(predicate) => predicate,
                Err(_) => return Ok(None),
            };
        let measure = match lower_sum_measure(aggregate, aggregate.input().schema().as_ref()) {
            Some(measure) => measure,
            None => return Ok(None),
        };
        let measure = match remap_projection_columns(
            &measure,
            filter.projection().as_ref().map(AsRef::as_ref),
            filter.input().schema().as_ref(),
        ) {
            Some(measure) => measure,
            None => return Ok(None),
        };

        let runtime = match FilterSumKernel::try_new(predicate.clone(), measure.clone()) {
            Ok(runtime) => runtime,
            Err(_) => return Ok(None),
        };
        let kernel = self.filter_sum_kernel(&predicate, &measure);
        let compiled = CompiledFilterSumExec::try_new(
            Arc::clone(filter.input()),
            runtime,
            aggregate.schema(),
            kernel,
        )?;
        Ok(Some(Arc::new(compiled) as Arc<dyn ExecutionPlan>))
    }

    fn filter_sum_kernel(&self, predicate: &JitExpr, measure: &JitExpr) -> CompiledKernel {
        if let Ok(module) = self.backend.lower_f64_filter_sum(predicate, measure) {
            if self.backend.verify_module(&module).is_ok() {
                return CompiledKernel::new(
                    module.symbol,
                    KernelKind::FilterSum,
                    self.backend.name(),
                    module.text,
                    false,
                );
            }
        }

        if let Ok(module) = self.backend.lower_decimal_filter_sum(predicate, measure) {
            if self.backend.verify_module(&module).is_ok() {
                return CompiledKernel::new(
                    module.symbol,
                    KernelKind::FilterSum,
                    self.backend.name(),
                    module.text,
                    false,
                );
            }
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

fn is_supported_sum_output(data_type: &ArrowDataType) -> bool {
    matches!(
        data_type,
        ArrowDataType::Float64 | ArrowDataType::Decimal128(_, _)
    )
}

#[allow(deprecated)]
fn strip_filter_sum_adapters(input: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
    if let Some(coalesce) = input.as_any().downcast_ref::<CoalesceBatchesExec>() {
        return strip_filter_sum_adapters(coalesce.input());
    }
    if let Some(repartition) = input.as_any().downcast_ref::<RepartitionExec>() {
        if matches!(repartition.partitioning(), Partitioning::RoundRobinBatch(_)) {
            return strip_filter_sum_adapters(repartition.input());
        }
    }
    input
}

fn lower_sum_measure(aggregate: &AggregateExec, input_schema: &ArrowSchema) -> Option<JitExpr> {
    let aggregate_expr = aggregate.aggr_expr().first()?;
    if !aggregate_expr.fun().name().eq_ignore_ascii_case("sum")
        || aggregate_expr.is_distinct()
        || !aggregate_expr.order_bys().is_empty()
    {
        return None;
    }
    let expressions = aggregate_expr.expressions();
    if expressions.len() != 1 {
        return None;
    }
    JitExpr::from_physical(&expressions[0], input_schema).ok()
}

fn remap_projection_columns(
    expr: &JitExpr,
    projection: Option<&[usize]>,
    input_schema: &ArrowSchema,
) -> Option<JitExpr> {
    match expr {
        JitExpr::Column {
            index,
            name: _,
            ty,
            nullable,
        } => {
            let source_index = match projection {
                Some(projection) => *projection.get(*index)?,
                None => *index,
            };
            let field = input_schema.field(source_index);
            Some(JitExpr::Column {
                index: source_index,
                name: field.name().to_string(),
                ty: *ty,
                nullable: *nullable,
            })
        }
        JitExpr::Literal(value) => Some(JitExpr::Literal(value.clone())),
        JitExpr::Binary {
            op,
            left,
            right,
            ty,
            nullable,
        } => Some(JitExpr::Binary {
            op: *op,
            left: Box::new(remap_projection_columns(left, projection, input_schema)?),
            right: Box::new(remap_projection_columns(right, projection, input_schema)?),
            ty: *ty,
            nullable: *nullable,
        }),
        JitExpr::IsNull(arg) => Some(JitExpr::IsNull(Box::new(remap_projection_columns(
            arg,
            projection,
            input_schema,
        )?))),
    }
}
