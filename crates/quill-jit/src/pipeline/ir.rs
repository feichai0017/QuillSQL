use crate::{JitExpr, JitProjection};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineSource {
    DataFusionInput,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PipelineOp {
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
pub struct PipelineIr {
    pub source: PipelineSource,
    pub operators: Vec<PipelineOp>,
    pub sink: PipelineSink,
}

impl PipelineIr {
    pub fn new(operators: Vec<PipelineOp>) -> Self {
        Self {
            source: PipelineSource::DataFusionInput,
            operators,
            sink: PipelineSink::RecordBatch,
        }
    }

    pub fn filter_sum(predicate: JitExpr, measure: JitExpr) -> Self {
        Self {
            source: PipelineSource::DataFusionInput,
            operators: vec![PipelineOp::Filter(predicate)],
            sink: PipelineSink::Sum { measure },
        }
    }

    pub fn operator_names(&self) -> Vec<&'static str> {
        self.operators
            .iter()
            .map(|operator| match operator {
                PipelineOp::Filter(_) => "filter",
                PipelineOp::Projection(_) => "projection",
                PipelineOp::Limit(_) => "limit",
            })
            .collect()
    }

    pub fn sink_name(&self) -> &'static str {
        match &self.sink {
            PipelineSink::RecordBatch => "record_batch",
            PipelineSink::Sum { .. } => "sum",
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{JitExpr, JitProjection, JitScalar, JitType, PipelineIr};

    #[test]
    fn records_filter_project_pipeline() {
        let predicate = JitExpr::Literal(JitScalar::Bool(true));
        let projection = JitProjection::new(JitExpr::Literal(JitScalar::Int64(1)), "one");
        let pipeline = PipelineIr::new(vec![
            crate::PipelineOp::Filter(predicate),
            crate::PipelineOp::Projection(vec![projection]),
        ]);

        assert_eq!(pipeline.operator_names(), vec!["filter", "projection"]);
        assert_eq!(pipeline.sink_name(), "record_batch");
    }

    #[test]
    fn records_projection_pipeline() {
        let projection =
            JitProjection::new(JitExpr::Literal(JitScalar::Null(JitType::Int64)), "value");
        let pipeline = PipelineIr::new(vec![crate::PipelineOp::Projection(vec![projection])]);

        assert_eq!(pipeline.operator_names(), vec!["projection"]);
        assert_eq!(pipeline.sink_name(), "record_batch");
    }

    #[test]
    fn records_filter_sum_pipeline() {
        let predicate = JitExpr::Literal(JitScalar::Bool(true));
        let measure = JitExpr::Literal(JitScalar::Float64(1.0));
        let pipeline = PipelineIr::filter_sum(predicate, measure);

        assert_eq!(pipeline.operator_names(), vec!["filter"]);
        assert_eq!(pipeline.sink_name(), "sum");
    }
}
