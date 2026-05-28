use quill_plan::PipelineGraph;
use quill_runtime::CompiledKernel;

#[derive(Debug, Clone)]
pub struct CompiledPipeline {
    pub graph: PipelineGraph,
    pub kernel: CompiledKernel,
}

pub trait FrontendAdapter {
    type Plan;
    type Candidate;
    type Compiled;
    type Error;

    fn extract(&self, plan: &Self::Plan) -> Vec<Self::Candidate>;
    fn replace(
        &self,
        plan: Self::Plan,
        compiled: Vec<Self::Compiled>,
    ) -> Result<Self::Plan, Self::Error>;
}
