mod expr;
mod ir;
mod kernel;
mod mlir;
mod rule;

pub use expr::{JitBinaryOp, JitExpr, JitProjection, JitScalar, JitType};
pub use ir::{KernelIr, PipelineIr, PipelineOp, PipelineSink, PipelineSource};
pub use kernel::{
    ArrowArrayView, ArrowMutableArrayView, CompiledKernel, FilterKernelFn, JitTypeTag,
    KernelBackend, KernelKind, ProjectionKernelFn,
};
pub use mlir::{MlirBackend, MlirModule};
pub use rule::{JitCandidate, MlirJitRule};

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
