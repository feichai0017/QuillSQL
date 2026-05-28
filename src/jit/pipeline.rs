use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
#[allow(deprecated)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlan;
use serde::Serialize;

use crate::jit::{CompiledFilterSumExec, JitExpr, KernelKind, PipelineIr};

#[derive(Debug, Clone)]
pub(crate) struct PipelineMatch {
    pub node: &'static str,
    pub pipeline: PipelineIr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PipelineCandidate {
    pub node: &'static str,
    pub kernel: KernelKind,
    pub operators: Vec<&'static str>,
    pub sink: &'static str,
}

#[derive(Debug, Clone)]
pub(crate) struct FilterSumPipeline {
    pub input: Arc<dyn ExecutionPlan>,
    pub predicate: JitExpr,
    pub measure: JitExpr,
}

impl PipelineMatch {
    pub fn candidate(&self) -> Option<PipelineCandidate> {
        let kernel = self.pipeline.first_kernel()?.kind();
        Some(PipelineCandidate {
            node: self.node,
            kernel,
            operators: self.pipeline.operator_names(),
            sink: self.pipeline.sink_name(),
        })
    }
}

impl FilterSumPipeline {
    pub fn pipeline_ir(&self) -> PipelineIr {
        PipelineIr::filter_sum(self.predicate.clone(), self.measure.clone())
    }
}

pub(crate) fn pipeline_from_node(plan: &Arc<dyn ExecutionPlan>) -> Option<PipelineMatch> {
    if let Some(compiled) = plan.as_any().downcast_ref::<CompiledFilterSumExec>() {
        return Some(PipelineMatch {
            node: "CompiledFilterSumExec",
            pipeline: PipelineIr::filter_sum(
                compiled.runtime().predicate().clone(),
                compiled.runtime().measure().clone(),
            ),
        });
    }

    let aggregate = plan.as_any().downcast_ref::<AggregateExec>()?;
    let pipeline = extract_filter_sum_pipeline(aggregate)?;
    Some(PipelineMatch {
        node: "AggregateExec",
        pipeline: pipeline.pipeline_ir(),
    })
}

pub(crate) fn extract_filter_sum_pipeline(aggregate: &AggregateExec) -> Option<FilterSumPipeline> {
    if *aggregate.mode() != AggregateMode::Partial
        || !aggregate.group_expr().is_true_no_grouping()
        || aggregate.aggr_expr().len() != 1
        || aggregate.filter_expr().iter().any(Option::is_some)
        || aggregate.schema().fields().len() != 1
        || !is_supported_sum_output(aggregate.schema().field(0).data_type())
    {
        return None;
    }

    let input = strip_filter_sum_adapters(aggregate.input());
    let filter = input.as_any().downcast_ref::<FilterExec>()?;
    if filter.fetch().is_some() {
        return None;
    }

    let predicate =
        JitExpr::from_physical(filter.predicate(), filter.input().schema().as_ref()).ok()?;
    let measure = lower_sum_measure(aggregate, aggregate.input().schema().as_ref())?;
    let measure = remap_projection_columns(
        &measure,
        filter.projection().as_ref().map(AsRef::as_ref),
        filter.input().schema().as_ref(),
    )?;

    Some(FilterSumPipeline {
        input: Arc::clone(filter.input()),
        predicate,
        measure,
    })
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
