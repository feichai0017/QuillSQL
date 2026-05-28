mod array;
mod eval;
mod exec;
mod kernel;
mod record;
mod sum;
#[cfg(test)]
mod tests;
mod value;

use self::array::BatchView;
pub use self::exec::{CompiledPipelineExec, PipelineRuntime};
pub use self::kernel::{CompiledKernel, KernelBackend, KernelKind, PipelineSpec, PredicateSpec};
pub use self::record::FilterProjectKernel;
pub use self::sum::{FilterSumKernel, FilterSumValue};
