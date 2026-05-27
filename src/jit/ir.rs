use crate::jit::{JitExpr, JitProjection, KernelKind};

#[derive(Debug, Clone, PartialEq)]
pub enum KernelIr {
    Filter {
        predicate: JitExpr,
    },
    Projection {
        projections: Vec<JitProjection>,
    },
    FilterProject {
        predicate: JitExpr,
        projections: Vec<JitProjection>,
    },
}

impl KernelIr {
    pub fn kind(&self) -> KernelKind {
        match self {
            Self::Filter { .. } => KernelKind::Filter,
            Self::Projection { .. } => KernelKind::Projection,
            Self::FilterProject { .. } => KernelKind::FilterProject,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Filter { .. } => "filter",
            Self::Projection { .. } => "projection",
            Self::FilterProject { .. } => "filter_project",
        }
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineSink {
    RecordBatch,
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

    pub fn first_kernel(&self) -> Option<KernelIr> {
        match self.operators.as_slice() {
            [PipelineOp::Filter(predicate), PipelineOp::Projection(projections), ..] => {
                Some(KernelIr::FilterProject {
                    predicate: predicate.clone(),
                    projections: projections.clone(),
                })
            }
            [PipelineOp::Filter(predicate), ..] => Some(KernelIr::Filter {
                predicate: predicate.clone(),
            }),
            [PipelineOp::Projection(projections), ..] => Some(KernelIr::Projection {
                projections: projections.clone(),
            }),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::jit::{
        JitExpr, JitProjection, JitScalar, JitType, KernelIr, KernelKind, PipelineIr,
    };

    #[test]
    fn fuses_filter_project_prefix() {
        let predicate = JitExpr::Literal(JitScalar::Bool(true));
        let projection = JitProjection::new(JitExpr::Literal(JitScalar::Int64(1)), "one");
        let pipeline = PipelineIr::new(vec![
            crate::jit::PipelineOp::Filter(predicate),
            crate::jit::PipelineOp::Projection(vec![projection]),
        ]);

        let kernel = pipeline.first_kernel().expect("kernel");
        assert_eq!(kernel.kind(), KernelKind::FilterProject);
        assert!(matches!(kernel, KernelIr::FilterProject { .. }));
    }

    #[test]
    fn keeps_projection_as_standalone_kernel() {
        let projection =
            JitProjection::new(JitExpr::Literal(JitScalar::Null(JitType::Int64)), "value");
        let pipeline = PipelineIr::new(vec![crate::jit::PipelineOp::Projection(vec![projection])]);

        assert_eq!(
            pipeline.first_kernel().expect("kernel").kind(),
            KernelKind::Projection
        );
    }
}
