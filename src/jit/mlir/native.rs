#![cfg(feature = "jit-mlir")]

use crate::jit::{JitError, JitResult, MlirModule};

use melior::{ir::Module, pass, ExecutionEngine};

pub(super) struct NativeI64Predicate {
    symbol: String,
    engine: ExecutionEngine,
}

impl NativeI64Predicate {
    pub(super) fn invoke(&self, value: i64) -> JitResult<bool> {
        let mut argument = value;
        let mut result = -1_i32;
        unsafe {
            self.engine
                .invoke_packed(
                    &self.symbol,
                    &mut [
                        &mut argument as *mut i64 as *mut (),
                        &mut result as *mut i32 as *mut (),
                    ],
                )
                .map_err(|err| JitError::Backend(format!("MLIR invocation failed: {err:?}")))?;
        }
        Ok(result != 0)
    }
}

pub(super) fn compile_i64_predicate(compiled: &MlirModule) -> JitResult<NativeI64Predicate> {
    let context = super::verify::mlir_context();
    let mut module = Module::parse(&context, &compiled.text)
        .ok_or_else(|| JitError::Backend("MLIR parser rejected generated module".to_string()))?;

    let pass_manager = pass::PassManager::new(&context);
    pass_manager.add_pass(pass::conversion::create_to_llvm());
    pass_manager
        .run(&mut module)
        .map_err(|err| JitError::Backend(format!("MLIR to LLVM lowering failed: {err:?}")))?;

    Ok(NativeI64Predicate {
        symbol: compiled.symbol.clone(),
        engine: ExecutionEngine::new(&module, 3, &[], false, false),
    })
}

pub(super) fn invoke_i64_predicate(compiled: &MlirModule, value: i64) -> JitResult<bool> {
    compile_i64_predicate(compiled)?.invoke(value)
}
