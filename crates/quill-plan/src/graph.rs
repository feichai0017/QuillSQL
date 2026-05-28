use crate::{JitExpr, JitProjection, JitType};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineSource {
    ArrowBatch,
    ArrowStream,
}

impl PipelineSource {
    pub fn name(&self) -> &'static str {
        match self {
            Self::ArrowBatch => "arrow_batch",
            Self::ArrowStream => "arrow_stream",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PipelineStage {
    Filter(JitExpr),
    Projection(Vec<JitProjection>),
    Limit(usize),
}

#[derive(Debug, Clone, PartialEq)]
pub enum PipelineSink {
    RecordBatch,
    Sum {
        measure: JitExpr,
    },
    GroupAggregate {
        keys: Vec<JitExpr>,
        aggregates: Vec<GroupAggregate>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PipelineKind {
    Record,
    Aggregate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunc {
    Sum,
    Count,
    Min,
    Max,
}

impl AggregateFunc {
    pub fn name(self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Count => "count",
            Self::Min => "min",
            Self::Max => "max",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupAggregate {
    pub func: AggregateFunc,
    pub expr: JitExpr,
    pub output_type: JitType,
    pub alias: String,
}

impl GroupAggregate {
    pub fn new(
        func: AggregateFunc,
        expr: JitExpr,
        output_type: JitType,
        alias: impl Into<String>,
    ) -> Self {
        Self {
            func,
            expr,
            output_type,
            alias: alias.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PipelineGraph {
    pub source: PipelineSource,
    pub stages: Vec<PipelineStage>,
    pub sink: PipelineSink,
}

impl PipelineGraph {
    pub fn record(stages: Vec<PipelineStage>) -> Self {
        Self {
            source: PipelineSource::ArrowBatch,
            stages,
            sink: PipelineSink::RecordBatch,
        }
    }

    pub fn filter_sum(predicate: JitExpr, measure: JitExpr) -> Self {
        Self {
            source: PipelineSource::ArrowBatch,
            stages: vec![PipelineStage::Filter(predicate)],
            sink: PipelineSink::Sum { measure },
        }
    }

    pub fn group_aggregate(
        stages: Vec<PipelineStage>,
        keys: Vec<JitExpr>,
        aggregates: Vec<GroupAggregate>,
    ) -> Self {
        Self {
            source: PipelineSource::ArrowBatch,
            stages,
            sink: PipelineSink::GroupAggregate { keys, aggregates },
        }
    }

    pub fn stage_names(&self) -> Vec<&'static str> {
        self.stages
            .iter()
            .map(|stage| match stage {
                PipelineStage::Filter(_) => "filter",
                PipelineStage::Projection(_) => "project",
                PipelineStage::Limit(_) => "limit",
            })
            .collect()
    }

    pub fn sink_name(&self) -> &'static str {
        match &self.sink {
            PipelineSink::RecordBatch => "record_batch",
            PipelineSink::Sum { .. } => "scalar_sum",
            PipelineSink::GroupAggregate { .. } => "group_aggregate",
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        AggregateFunc, GroupAggregate, JitExpr, JitProjection, JitScalar, JitType, PipelineGraph,
    };

    #[test]
    fn records_filter_project_pipeline() {
        let predicate = JitExpr::Literal(JitScalar::Bool(true));
        let projection = JitProjection::new(JitExpr::Literal(JitScalar::Int64(1)), "one");
        let pipeline = PipelineGraph::record(vec![
            crate::PipelineStage::Filter(predicate),
            crate::PipelineStage::Projection(vec![projection]),
        ]);

        assert_eq!(pipeline.stage_names(), vec!["filter", "project"]);
        assert_eq!(pipeline.sink_name(), "record_batch");
    }

    #[test]
    fn records_projection_pipeline() {
        let projection =
            JitProjection::new(JitExpr::Literal(JitScalar::Null(JitType::Int64)), "value");
        let pipeline =
            PipelineGraph::record(vec![crate::PipelineStage::Projection(vec![projection])]);

        assert_eq!(pipeline.stage_names(), vec!["project"]);
        assert_eq!(pipeline.sink_name(), "record_batch");
    }

    #[test]
    fn records_filter_sum_pipeline() {
        let predicate = JitExpr::Literal(JitScalar::Bool(true));
        let measure = JitExpr::Literal(JitScalar::Float64(1.0));
        let pipeline = PipelineGraph::filter_sum(predicate, measure);

        assert_eq!(pipeline.stage_names(), vec!["filter"]);
        assert_eq!(pipeline.sink_name(), "scalar_sum");
    }

    #[test]
    fn records_group_aggregate_pipeline() {
        let key = JitExpr::Literal(JitScalar::Int64(1));
        let aggregate = GroupAggregate::new(
            AggregateFunc::Sum,
            JitExpr::Literal(JitScalar::Float64(1.0)),
            JitType::Float64,
            "sum_value",
        );
        let pipeline = PipelineGraph::group_aggregate(vec![], vec![key], vec![aggregate]);

        assert!(pipeline.stage_names().is_empty());
        assert_eq!(pipeline.sink_name(), "group_aggregate");
    }
}
