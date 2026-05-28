use crate::jit::{
    JitExpr, JitProjection, KernelKind, PipelineIr, PipelineOp, PipelineSink, PipelineSource,
};

#[derive(Debug, Clone, PartialEq)]
pub enum PipelineLowering {
    Record {
        predicate: JitExpr,
        projections: Vec<JitProjection>,
    },
    Sum {
        predicate: JitExpr,
        measure: JitExpr,
    },
}

impl PipelineLowering {
    pub fn from_ir(ir: &PipelineIr) -> Option<Self> {
        match (&ir.source, ir.operators.as_slice(), &ir.sink) {
            (
                PipelineSource::DataFusionInput,
                [PipelineOp::Filter(predicate), PipelineOp::Projection(projections)],
                PipelineSink::RecordBatch,
            ) => Some(Self::Record {
                predicate: predicate.clone(),
                projections: projections.clone(),
            }),
            (
                PipelineSource::DataFusionInput,
                [PipelineOp::Filter(predicate)],
                PipelineSink::Sum { measure },
            ) => Some(Self::Sum {
                predicate: predicate.clone(),
                measure: measure.clone(),
            }),
            _ => None,
        }
    }

    pub fn kind(&self) -> KernelKind {
        match self {
            Self::Record { .. } => KernelKind::FilterProject,
            Self::Sum { .. } => KernelKind::FilterSum,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::jit::{
        JitExpr, JitProjection, JitScalar, KernelKind, PipelineIr, PipelineLowering, PipelineOp,
    };

    #[test]
    fn lowers_filter_projection_pipeline() {
        let predicate = JitExpr::Literal(JitScalar::Bool(true));
        let projection = JitProjection::new(JitExpr::Literal(JitScalar::Int64(1)), "one");
        let pipeline = PipelineIr::new(vec![
            PipelineOp::Filter(predicate),
            PipelineOp::Projection(vec![projection]),
        ]);

        let lowering = PipelineLowering::from_ir(&pipeline).expect("lowering");
        assert_eq!(lowering.kind(), KernelKind::FilterProject);
        assert!(matches!(lowering, PipelineLowering::Record { .. }));
    }

    #[test]
    fn lowers_filter_sum_pipeline() {
        let predicate = JitExpr::Literal(JitScalar::Bool(true));
        let measure = JitExpr::Literal(JitScalar::Float64(1.0));
        let pipeline = PipelineIr::filter_sum(predicate, measure);

        let lowering = PipelineLowering::from_ir(&pipeline).expect("lowering");
        assert_eq!(lowering.kind(), KernelKind::FilterSum);
        assert!(matches!(lowering, PipelineLowering::Sum { .. }));
    }
}
