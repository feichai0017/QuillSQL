#![cfg(feature = "jit-mlir")]

use crate::jit::{JitError, JitResult, MlirModule};

use melior::{ir::Module, pass, ExecutionEngine};

pub(super) struct NativeI64Predicate {
    symbol: String,
    engine: ExecutionEngine,
}

pub struct NativeI64Filter {
    symbol: String,
    engine: ExecutionEngine,
}

pub struct NativeI64FilterProject {
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

impl NativeI64Filter {
    pub fn invoke(&self, values: &[i64], output: &mut [u8]) -> JitResult<()> {
        if output.len() < values.len() {
            return Err(JitError::Backend(format!(
                "native filter output len {} is smaller than input len {}",
                output.len(),
                values.len()
            )));
        }

        let mut len = values.len() as i64;
        let mut values_ptr = values.as_ptr();
        let mut output_ptr = output.as_mut_ptr();
        let mut result = -1_i32;
        unsafe {
            self.engine
                .invoke_packed(
                    &self.symbol,
                    &mut [
                        &mut len as *mut i64 as *mut (),
                        &mut values_ptr as *mut *const i64 as *mut (),
                        &mut output_ptr as *mut *mut u8 as *mut (),
                        &mut result as *mut i32 as *mut (),
                    ],
                )
                .map_err(|err| JitError::Backend(format!("MLIR invocation failed: {err:?}")))?;
        }
        if result == 0 {
            Ok(())
        } else {
            Err(JitError::Backend(format!(
                "native filter returned status {result}"
            )))
        }
    }
}

impl NativeI64FilterProject {
    pub fn invoke(
        &self,
        predicate_values: &[i64],
        projection_values: &[i64],
        output: &mut [i64],
    ) -> JitResult<usize> {
        if projection_values.len() != predicate_values.len() {
            return Err(JitError::Backend(format!(
                "native filter-project projection len {} does not match predicate len {}",
                projection_values.len(),
                predicate_values.len()
            )));
        }
        if output.len() < predicate_values.len() {
            return Err(JitError::Backend(format!(
                "native filter-project output len {} is smaller than input len {}",
                output.len(),
                predicate_values.len()
            )));
        }

        let mut len = predicate_values.len() as i64;
        let mut predicate_ptr = predicate_values.as_ptr();
        let mut projection_ptr = projection_values.as_ptr();
        let mut output_ptr = output.as_mut_ptr();
        let mut output_len = -1_i64;
        let mut output_len_ptr = &mut output_len as *mut i64;
        let mut result = -1_i32;
        unsafe {
            self.engine
                .invoke_packed(
                    &self.symbol,
                    &mut [
                        &mut len as *mut i64 as *mut (),
                        &mut predicate_ptr as *mut *const i64 as *mut (),
                        &mut projection_ptr as *mut *const i64 as *mut (),
                        &mut output_ptr as *mut *mut i64 as *mut (),
                        &mut output_len_ptr as *mut *mut i64 as *mut (),
                        &mut result as *mut i32 as *mut (),
                    ],
                )
                .map_err(|err| JitError::Backend(format!("MLIR invocation failed: {err:?}")))?;
        }
        if result != 0 {
            return Err(JitError::Backend(format!(
                "native filter-project returned status {result}"
            )));
        }
        if output_len < 0 || output_len as usize > output.len() {
            return Err(JitError::Backend(format!(
                "native filter-project returned invalid output len {output_len}"
            )));
        }
        Ok(output_len as usize)
    }
}

pub(super) fn compile_i64_predicate(compiled: &MlirModule) -> JitResult<NativeI64Predicate> {
    Ok(NativeI64Predicate {
        symbol: compiled.symbol.clone(),
        engine: compile_engine(compiled)?,
    })
}

pub(super) fn invoke_i64_predicate(compiled: &MlirModule, value: i64) -> JitResult<bool> {
    compile_i64_predicate(compiled)?.invoke(value)
}

pub fn compile_i64_filter(compiled: &MlirModule) -> JitResult<NativeI64Filter> {
    Ok(NativeI64Filter {
        symbol: compiled.symbol.clone(),
        engine: compile_engine(compiled)?,
    })
}

pub fn compile_i64_filter_project(compiled: &MlirModule) -> JitResult<NativeI64FilterProject> {
    Ok(NativeI64FilterProject {
        symbol: compiled.symbol.clone(),
        engine: compile_engine(compiled)?,
    })
}

fn compile_engine(compiled: &MlirModule) -> JitResult<ExecutionEngine> {
    let context = super::verify::mlir_context();
    let mut module = Module::parse(&context, &compiled.text)
        .ok_or_else(|| JitError::Backend("MLIR parser rejected generated module".to_string()))?;

    let pass_manager = pass::PassManager::new(&context);
    pass_manager
        .nested_under("func.func")
        .add_pass(pass::conversion::create_arith_to_llvm());
    pass_manager
        .nested_under("func.func")
        .add_pass(pass::conversion::create_index_to_llvm());
    pass_manager.add_pass(pass::conversion::create_scf_to_control_flow());
    pass_manager
        .nested_under("func.func")
        .add_pass(pass::conversion::create_arith_to_llvm());
    pass_manager.add_pass(pass::conversion::create_control_flow_to_llvm());
    pass_manager.add_pass(pass::conversion::create_func_to_llvm());
    pass_manager.add_pass(pass::conversion::create_finalize_mem_ref_to_llvm());
    pass_manager
        .run(&mut module)
        .map_err(|err| JitError::Backend(format!("MLIR to LLVM lowering failed: {err:?}")))?;

    Ok(ExecutionEngine::new(&module, 3, &[], false, false))
}
