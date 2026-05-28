use crate::{JitExpr, JitProjection};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineSource {
    DataFusionInput,
}

impl PipelineSource {
    pub fn name(&self) -> &'static str {
        match self {
            Self::DataFusionInput => "arrow_batch",
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
    Sum { measure: JitExpr },
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
            source: PipelineSource::DataFusionInput,
            stages,
            sink: PipelineSink::RecordBatch,
        }
    }

    pub fn filter_sum(predicate: JitExpr, measure: JitExpr) -> Self {
        Self {
            source: PipelineSource::DataFusionInput,
            stages: vec![PipelineStage::Filter(predicate)],
            sink: PipelineSink::Sum { measure },
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
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{JitExpr, JitProjection, JitScalar, JitType, PipelineGraph};

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
}
