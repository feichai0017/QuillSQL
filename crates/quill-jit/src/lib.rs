mod dialect;
mod lower;
mod mlir;
mod pipeline;
mod runtime;

pub use dialect::{QuillDialectModule, QuillDialectOp, QuillDialectSink, QuillDialectSource};
pub use lower::{JitOptions, PipelineKind, PipelineLowering};
#[cfg(feature = "jit-mlir")]
pub use mlir::{
    CompiledDecimalFilterSum, CompiledF64FilterSum, CompiledI64Filter, CompiledI64FilterProject,
    DecimalFilterSumInput, DecimalFilterSumOutput, F64FilterSumOutput,
};
pub use mlir::{MlirBackend, MlirColumn, MlirModule};
pub use pipeline::{
    JitBinaryOp, JitCandidate, JitExpr, JitProjection, JitScalar, JitType, MlirJitRule,
    PipelineCandidate, PipelineIr, PipelineSink, PipelineSource, PipelineStage,
};
pub use runtime::{
    CompiledKernel, CompiledPipelineExec, FilterProjectKernel, FilterSumKernel, FilterSumValue,
    KernelBackend, KernelKind, PipelineRuntime, PipelineSpec, PredicateSpec,
};

use datafusion::common::DataFusionError;
use thiserror::Error;

pub type JitResult<T> = std::result::Result<T, JitError>;

#[derive(Debug, Error)]
pub enum JitError {
    #[error("unsupported JIT expression: {0}")]
    UnsupportedExpr(String),
    #[error("unsupported JIT type: {0}")]
    UnsupportedType(String),
    #[error("JIT backend error: {0}")]
    Backend(String),
}

impl From<JitError> for DataFusionError {
    fn from(value: JitError) -> Self {
        DataFusionError::Execution(value.to_string())
    }
}
