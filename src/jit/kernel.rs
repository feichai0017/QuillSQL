use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use serde::Serialize;

use crate::jit::{JitExpr, JitProjection, JitResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum JitTypeTag {
    Bool = 1,
    Int32 = 2,
    Int64 = 3,
    Float64 = 4,
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct ArrowArrayView {
    pub values: *const u8,
    pub validity: *const u8,
    pub len: usize,
    pub offset: usize,
    pub type_tag: JitTypeTag,
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct ArrowMutableArrayView {
    pub values: *mut u8,
    pub validity: *mut u8,
    pub len: usize,
    pub offset: usize,
    pub type_tag: JitTypeTag,
}

pub type FilterKernelFn =
    unsafe extern "C" fn(len: usize, input: *const ArrowArrayView, output_bitmap: *mut u8) -> i32;

pub type ProjectionKernelFn = unsafe extern "C" fn(
    len: usize,
    input: *const ArrowArrayView,
    output: *mut ArrowMutableArrayView,
) -> i32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum KernelKind {
    Filter,
    Projection,
    FilterProject,
}

#[derive(Debug, Clone)]
pub struct CompiledKernel {
    pub id: String,
    pub kind: KernelKind,
    pub backend: String,
    pub ir: String,
    pub executable: bool,
}

impl CompiledKernel {
    pub fn new(
        id: impl Into<String>,
        kind: KernelKind,
        backend: impl Into<String>,
        ir: impl Into<String>,
        executable: bool,
    ) -> Self {
        Self {
            id: id.into(),
            kind,
            backend: backend.into(),
            ir: ir.into(),
            executable,
        }
    }
}

pub trait KernelBackend: Send + Sync {
    fn name(&self) -> &str;

    fn compile_filter(
        &self,
        input_schema: ArrowSchemaRef,
        predicate: &JitExpr,
    ) -> JitResult<CompiledKernel>;

    fn compile_projection(
        &self,
        input_schema: ArrowSchemaRef,
        projections: &[JitProjection],
    ) -> JitResult<CompiledKernel>;

    fn compile_filter_project(
        &self,
        input_schema: ArrowSchemaRef,
        predicate: &JitExpr,
        projections: &[JitProjection],
    ) -> JitResult<CompiledKernel>;
}
