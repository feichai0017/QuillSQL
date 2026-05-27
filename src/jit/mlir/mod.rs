mod emit;
mod native;
#[cfg(test)]
mod tests;
mod verify;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;

use crate::jit::{CompiledKernel, JitExpr, JitProjection, JitResult, KernelBackend, KernelKind};

#[derive(Debug, Clone)]
pub struct MlirModule {
    pub symbol: String,
    pub text: String,
}

#[derive(Debug, Default)]
pub struct MlirBackend;

impl MlirBackend {
    pub fn new() -> Self {
        Self
    }

    pub fn is_available(&self) -> bool {
        cfg!(feature = "jit-mlir")
    }

    pub fn lower_filter(&self, predicate: &JitExpr) -> JitResult<MlirModule> {
        emit::lower_filter(predicate)
    }

    pub fn lower_projection(&self, projections: &[JitProjection]) -> JitResult<MlirModule> {
        emit::lower_projection(projections)
    }

    pub fn lower_filter_project(
        &self,
        predicate: &JitExpr,
        projections: &[JitProjection],
    ) -> JitResult<MlirModule> {
        emit::lower_filter_project(predicate, projections)
    }

    pub fn lower_i64_predicate(&self, predicate: &JitExpr) -> JitResult<MlirModule> {
        emit::lower_i64_predicate(predicate)
    }

    #[cfg(feature = "jit-mlir")]
    pub fn invoke_i64_predicate(&self, predicate: &JitExpr, value: i64) -> JitResult<bool> {
        let module = self.lower_i64_predicate(predicate)?;
        self.verify_module(&module)?;
        native::invoke_i64_predicate(&module, value)
    }

    pub fn verify_module(&self, module: &MlirModule) -> JitResult<()> {
        verify::verify_module(module)
    }
}

impl KernelBackend for MlirBackend {
    fn name(&self) -> &str {
        "mlir"
    }

    fn compile_filter(
        &self,
        _input_schema: ArrowSchemaRef,
        predicate: &JitExpr,
    ) -> JitResult<CompiledKernel> {
        let module = self.lower_filter(predicate)?;
        self.verify_module(&module)?;
        Ok(CompiledKernel::new(
            module.symbol,
            KernelKind::Filter,
            self.name(),
            module.text,
            false,
        ))
    }

    fn compile_projection(
        &self,
        _input_schema: ArrowSchemaRef,
        projections: &[JitProjection],
    ) -> JitResult<CompiledKernel> {
        let module = self.lower_projection(projections)?;
        self.verify_module(&module)?;
        Ok(CompiledKernel::new(
            module.symbol,
            KernelKind::Projection,
            self.name(),
            module.text,
            false,
        ))
    }

    fn compile_filter_project(
        &self,
        _input_schema: ArrowSchemaRef,
        predicate: &JitExpr,
        projections: &[JitProjection],
    ) -> JitResult<CompiledKernel> {
        let module = self.lower_filter_project(predicate, projections)?;
        self.verify_module(&module)?;
        Ok(CompiledKernel::new(
            module.symbol,
            KernelKind::FilterProject,
            self.name(),
            module.text,
            false,
        ))
    }
}
