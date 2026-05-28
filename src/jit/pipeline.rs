use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
#[allow(deprecated)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlan;
use serde::Serialize;

use crate::jit::{
    CompiledAggregatePipelineExec, CompiledRecordPipelineExec, JitExpr, JitProjection, PipelineIr,
    PipelineKind, PipelineLowering, PipelineOp,
};

#[derive(Debug, Clone)]
pub(crate) struct PipelineMatch {
    pub node: &'static str,
    pub pipeline: PipelineIr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PipelineCandidate {
    pub node: &'static str,
    pub kind: PipelineKind,
    pub operators: Vec<&'static str>,
    pub sink: &'static str,
}

#[derive(Debug, Clone)]
pub(crate) struct PhysicalPipeline {
    pub input: Arc<dyn ExecutionPlan>,
    pub output_schema: ArrowSchemaRef,
    pub ir: PipelineIr,
    pub output_adapter: Option<OutputAdapter>,
}

#[derive(Debug, Clone)]
pub(crate) struct OutputAdapter {
    pub partitioning: Partitioning,
    pub preserve_order: bool,
}

impl PipelineMatch {
    pub fn candidate(&self) -> Option<PipelineCandidate> {
        let kind = PipelineLowering::from_ir(&self.pipeline)?.kind();
        Some(PipelineCandidate {
            node: self.node,
            kind,
            operators: self.pipeline.operator_names(),
            sink: self.pipeline.sink_name(),
        })
    }
}

impl PhysicalPipeline {
    pub fn filter_sum(
        input: Arc<dyn ExecutionPlan>,
        output_schema: ArrowSchemaRef,
        predicate: JitExpr,
        measure: JitExpr,
    ) -> Self {
        Self {
            input,
            output_schema,
            ir: PipelineIr::filter_sum(predicate, measure),
            output_adapter: None,
        }
    }

    pub fn filter_project(
        input: Arc<dyn ExecutionPlan>,
        output_schema: ArrowSchemaRef,
        predicate: JitExpr,
        projections: Vec<JitProjection>,
        output_adapter: Option<OutputAdapter>,
    ) -> Self {
        Self {
            input,
            output_schema,
            ir: PipelineIr::new(vec![
                PipelineOp::Filter(predicate),
                PipelineOp::Projection(projections),
            ]),
            output_adapter,
        }
    }
}

pub(crate) fn extract_pipeline_from_node(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<PhysicalPipeline> {
    if let Some(aggregate) = plan.as_any().downcast_ref::<AggregateExec>() {
        return extract_filter_sum_pipeline(aggregate);
    }

    let projection = plan.as_any().downcast_ref::<ProjectionExec>()?;
    extract_filter_project_pipeline(projection)
}

pub(crate) fn pipeline_from_node(plan: &Arc<dyn ExecutionPlan>) -> Option<PipelineMatch> {
    if let Some(compiled) = plan.as_any().downcast_ref::<CompiledRecordPipelineExec>() {
        return Some(PipelineMatch {
            node: "CompiledRecordPipelineExec",
            pipeline: PipelineIr::new(vec![
                PipelineOp::Filter(compiled.runtime().predicate().clone()),
                PipelineOp::Projection(compiled.runtime().projections().to_vec()),
            ]),
        });
    }

    if let Some(compiled) = plan
        .as_any()
        .downcast_ref::<CompiledAggregatePipelineExec>()
    {
        return Some(PipelineMatch {
            node: "CompiledAggregatePipelineExec",
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
        pipeline: pipeline.ir,
    })
}

fn extract_filter_project_pipeline(projection: &ProjectionExec) -> Option<PhysicalPipeline> {
    let input = projection.input();
    if let Some(filter) = input.as_any().downcast_ref::<FilterExec>() {
        return filter_project_pipeline_from_filter(filter, projection, None);
    }

    let repartition = input.as_any().downcast_ref::<RepartitionExec>()?;
    if !matches!(repartition.partitioning(), Partitioning::RoundRobinBatch(_)) {
        return None;
    }
    let filter = repartition.input().as_any().downcast_ref::<FilterExec>()?;
    filter_project_pipeline_from_filter(
        filter,
        projection,
        Some(OutputAdapter {
            partitioning: repartition.partitioning().clone(),
            preserve_order: repartition.preserve_order(),
        }),
    )
}

fn filter_project_pipeline_from_filter(
    filter: &FilterExec,
    projection: &ProjectionExec,
    output_adapter: Option<OutputAdapter>,
) -> Option<PhysicalPipeline> {
    if filter.projection().is_some() || filter.fetch().is_some() {
        return None;
    }

    let input_schema = filter.input().schema();
    let predicate = JitExpr::from_physical(filter.predicate(), input_schema.as_ref()).ok()?;
    let projections = lower_projection_exprs(projection, input_schema.as_ref())?;
    Some(PhysicalPipeline::filter_project(
        Arc::clone(filter.input()),
        projection.schema(),
        predicate,
        projections,
        output_adapter,
    ))
}

pub(crate) fn extract_filter_sum_pipeline(aggregate: &AggregateExec) -> Option<PhysicalPipeline> {
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

    Some(PhysicalPipeline::filter_sum(
        Arc::clone(filter.input()),
        aggregate.schema(),
        predicate,
        measure,
    ))
}

fn lower_projection_exprs(
    projection: &ProjectionExec,
    input_schema: &ArrowSchema,
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
