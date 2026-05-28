mod dialect;
mod frontend;
mod lower;
mod mlir;

pub use dialect::{QuillDialectModule, QuillDialectOp, QuillDialectSink, QuillDialectSource};
pub use frontend::{CompiledPipeline, FrontendAdapter};
pub use lower::{JitOptions, PipelineLowering};
#[cfg(feature = "jit-mlir")]
pub use mlir::{
    CompiledDecimalFilterSum, CompiledF64FilterSum, CompiledI64Filter, CompiledRecordPipeline,
    DecimalFilterSumOutput, F64FilterSumOutput, FixedColumnInput, RecordPipelineOutput,
};
pub use mlir::{MlirBackend, MlirColumn, MlirModule};
pub use quill_plan::{
    AggregateFunc, GroupAggregate, JitBinaryOp, JitError, JitExpr, JitProjection, JitResult,
    JitScalar, JitType, PipelineGraph, PipelineKind, PipelineSink, PipelineSource, PipelineStage,
};
pub use quill_runtime::{
    CompiledKernel, FilterProjectKernel, FilterSumKernel, FilterSumValue, FixedColumn,
    KernelBackend, KernelKind, PipelineSpec, PredicateSpec,
};

#[cfg(feature = "jit-mlir")]
pub use mlir::{execute_filter_project, execute_filter_sum};
