#[cfg(feature = "jit-mlir")]
use crate::JitError;
use crate::JitResult;

use super::MlirModule;

#[cfg(feature = "jit-mlir")]
use melior::{
    dialect::DialectRegistry,
    ir::{operation::OperationLike, Module},
    utility::{register_all_dialects, register_all_llvm_translations},
    Context,
};

pub(super) fn verify_module(module: &MlirModule) -> JitResult<()> {
    verify_mlir_text(&module.text)
}

#[cfg(feature = "jit-mlir")]
fn verify_mlir_text(text: &str) -> JitResult<()> {
    let context = mlir_context();
    let module = Module::parse(&context, text)
        .ok_or_else(|| JitError::Backend("MLIR parser rejected generated module".to_string()))?;
    if module.as_operation().verify() {
        Ok(())
    } else {
        Err(JitError::Backend(
            "MLIR verifier rejected generated module".to_string(),
        ))
    }
}

#[cfg(not(feature = "jit-mlir"))]
fn verify_mlir_text(_text: &str) -> JitResult<()> {
    Ok(())
}

#[cfg(feature = "jit-mlir")]
pub(super) fn mlir_context() -> Context {
    let context = Context::new();
    let registry = DialectRegistry::new();
    register_all_dialects(&registry);
    context.append_dialect_registry(&registry);
    quill_mlir::register_dialect(&context);
    quill_mlir::register_passes();
    context.load_all_available_dialects();
    register_all_llvm_translations(&context);
    context
}
