#![cfg(feature = "jit-mlir")]

use crate::jit::{JitError, JitResult, MlirModule};

use melior::{ir::Module, pass, ExecutionEngine};

pub(super) fn invoke_i64_predicate(compiled: &MlirModule, value: i64) -> JitResult<bool> {
    let context = super::mlir_context();
    let mut module = Module::parse(&context, &compiled.text)
        .ok_or_else(|| JitError::Backend("MLIR parser rejected generated module".to_string()))?;

    let pass_manager = pass::PassManager::new(&context);
    pass_manager.add_pass(pass::conversion::create_to_llvm());
    pass_manager
        .run(&mut module)
        .map_err(|err| JitError::Backend(format!("MLIR to LLVM lowering failed: {err:?}")))?;

    let engine = ExecutionEngine::new(&module, 3, &[], false, false);
    let mut argument = value;
    let mut result = -1_i32;
    unsafe {
        engine
            .invoke_packed(
                &compiled.symbol,
                &mut [
                    &mut argument as *mut i64 as *mut (),
                    &mut result as *mut i32 as *mut (),
                ],
            )
            .map_err(|err| JitError::Backend(format!("MLIR invocation failed: {err:?}")))?;
    }
    Ok(result != 0)
}
