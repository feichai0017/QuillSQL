use quill_plan::{
    JitExpr, JitProjection, PipelineGraph, PipelineKind, PipelineSink, PipelineSource,
    PipelineStage,
};

mod options;

pub use options::JitOptions;

#[derive(Debug, Clone, PartialEq)]
pub enum PipelineLowering {
    Record {
        predicate: JitExpr,
        projections: Vec<JitProjection>,
    },
    PlainSum {
        predicate: JitExpr,
        measure: JitExpr,
    },
}

impl PipelineLowering {
    pub fn from_graph(graph: &PipelineGraph) -> Option<Self> {
        match (&graph.source, graph.stages.as_slice(), &graph.sink) {
            (
                PipelineSource::ArrowBatch,
                [PipelineStage::Filter(predicate), PipelineStage::Projection(projections)],
                PipelineSink::RecordBatch,
            ) => Some(Self::Record {
                predicate: predicate.clone(),
                projections: projections.clone(),
            }),
            (
                PipelineSource::ArrowBatch,
                [PipelineStage::Filter(predicate)],
                PipelineSink::Sum { measure },
            ) => Some(Self::PlainSum {
                predicate: predicate.clone(),
                measure: measure.clone(),
            }),
            _ => None,
        }
    }

    pub fn kind(&self) -> PipelineKind {
        match self {
            Self::Record { .. } => PipelineKind::Record,
            Self::PlainSum { .. } => PipelineKind::Aggregate,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        JitExpr, JitProjection, JitScalar, PipelineGraph, PipelineKind, PipelineLowering,
        PipelineStage,
    };

    #[test]
    fn lowers_filter_projection_pipeline() {
        let predicate = JitExpr::Literal(JitScalar::Bool(true));
        let projection = JitProjection::new(JitExpr::Literal(JitScalar::Int64(1)), "one");
        let pipeline = PipelineGraph::record(vec![
            PipelineStage::Filter(predicate),
            PipelineStage::Projection(vec![projection]),
        ]);

        let lowering = PipelineLowering::from_graph(&pipeline).expect("lowering");
        assert_eq!(lowering.kind(), PipelineKind::Record);
        assert!(matches!(lowering, PipelineLowering::Record { .. }));
    }

    #[test]
    fn lowers_filter_sum_pipeline() {
        let predicate = JitExpr::Literal(JitScalar::Bool(true));
        let measure = JitExpr::Literal(JitScalar::Float64(1.0));
        let pipeline = PipelineGraph::filter_sum(predicate, measure);

        let lowering = PipelineLowering::from_graph(&pipeline).expect("lowering");
        assert_eq!(lowering.kind(), PipelineKind::Aggregate);
        assert!(matches!(lowering, PipelineLowering::PlainSum { .. }));
    }
}
