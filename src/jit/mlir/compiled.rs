#![cfg(feature = "jit-mlir")]

use crate::jit::{JitError, JitResult, JitType, MlirColumn, MlirModule};

use melior::{ir::Module, pass, ExecutionEngine};

pub(super) struct CompiledI64Predicate {
    symbol: String,
    engine: ExecutionEngine,
}

pub struct CompiledI64Filter {
    symbol: String,
    engine: ExecutionEngine,
}

pub struct CompiledI64FilterProject {
    symbol: String,
    engine: ExecutionEngine,
}

pub struct CompiledF64FilterSum {
    symbol: String,
    engine: ExecutionEngine,
}

pub struct CompiledDecimalFilterSum {
    symbol: String,
    engine: ExecutionEngine,
    columns: Vec<MlirColumn>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalFilterSumOutput {
    pub sum: i128,
    pub count: i64,
}

#[derive(Debug, Clone, Copy)]
pub enum DecimalFilterSumInput<'a> {
    Date32 { index: usize, values: &'a [i32] },
    Int64 { index: usize, values: &'a [i64] },
    Decimal128 { index: usize, values: &'a [i128] },
}

impl CompiledI64Predicate {
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

impl CompiledI64Filter {
    pub fn invoke(&self, values: &[i64], output: &mut [u8]) -> JitResult<()> {
        if output.len() < values.len() {
            return Err(JitError::Backend(format!(
                "compiled filter output len {} is smaller than input len {}",
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
                "compiled filter returned status {result}"
            )))
        }
    }
}

impl CompiledI64FilterProject {
    pub fn invoke(
        &self,
        predicate_values: &[i64],
        projection_values: &[i64],
        output: &mut [i64],
    ) -> JitResult<usize> {
        if projection_values.len() != predicate_values.len() {
            return Err(JitError::Backend(format!(
                "compiled filter-project projection len {} does not match predicate len {}",
                projection_values.len(),
                predicate_values.len()
            )));
        }
        if output.len() < predicate_values.len() {
            return Err(JitError::Backend(format!(
                "compiled filter-project output len {} is smaller than input len {}",
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
                "compiled filter-project returned status {result}"
            )));
        }
        if output_len < 0 || output_len as usize > output.len() {
            return Err(JitError::Backend(format!(
                "compiled filter-project returned invalid output len {output_len}"
            )));
        }
        Ok(output_len as usize)
    }
}

impl CompiledF64FilterSum {
    pub fn invoke(
        &self,
        predicate_values: &[i64],
        left_values: &[f64],
        right_values: &[f64],
    ) -> JitResult<f64> {
        if left_values.len() != predicate_values.len() {
            return Err(JitError::Backend(format!(
                "compiled filter-sum left len {} does not match predicate len {}",
                left_values.len(),
                predicate_values.len()
            )));
        }
        if right_values.len() != predicate_values.len() {
            return Err(JitError::Backend(format!(
                "compiled filter-sum right len {} does not match predicate len {}",
                right_values.len(),
                predicate_values.len()
            )));
        }

        let mut len = predicate_values.len() as i64;
        let mut predicate_ptr = predicate_values.as_ptr();
        let mut left_ptr = left_values.as_ptr();
        let mut right_ptr = right_values.as_ptr();
        let mut output_sum = 0.0_f64;
        let mut output_sum_ptr = &mut output_sum as *mut f64;
        let mut result = -1_i32;
        unsafe {
            self.engine
                .invoke_packed(
                    &self.symbol,
                    &mut [
                        &mut len as *mut i64 as *mut (),
                        &mut predicate_ptr as *mut *const i64 as *mut (),
                        &mut left_ptr as *mut *const f64 as *mut (),
                        &mut right_ptr as *mut *const f64 as *mut (),
                        &mut output_sum_ptr as *mut *mut f64 as *mut (),
                        &mut result as *mut i32 as *mut (),
                    ],
                )
                .map_err(|err| JitError::Backend(format!("MLIR invocation failed: {err:?}")))?;
        }
        if result != 0 {
            return Err(JitError::Backend(format!(
                "compiled filter-sum returned status {result}"
            )));
        }
        Ok(output_sum)
    }
}

impl CompiledDecimalFilterSum {
    pub fn invoke(
        &self,
        inputs: &[DecimalFilterSumInput<'_>],
    ) -> JitResult<DecimalFilterSumOutput> {
        let mut input_len = None;
        let mut ptr_values = Vec::with_capacity(self.columns.len());

        for column in &self.columns {
            let input = inputs
                .iter()
                .find(|input| input.index() == column.index)
                .ok_or_else(|| {
                    JitError::Backend(format!(
                        "compiled decimal filter-sum missing input column {}",
                        column.index
                    ))
                })?;
            if !input.matches_type(column.ty) {
                return Err(JitError::Backend(format!(
                    "compiled decimal filter-sum input column {} has incompatible type",
                    column.index
                )));
            }

            let len = input.len();
            match input_len {
                Some(expected) if expected != len => {
                    return Err(JitError::Backend(format!(
                        "compiled decimal filter-sum input column {} len {} does not match len {}",
                        column.index, len, expected
                    )));
                }
                Some(_) => {}
                None => input_len = Some(len),
            }
            ptr_values.push(input.ptr());
        }

        let mut len = input_len.unwrap_or(0) as i64;
        let mut output_sum = 0_i128;
        let mut output_sum_ptr = &mut output_sum as *mut i128;
        let mut output_count = 0_i64;
        let mut output_count_ptr = &mut output_count as *mut i64;
        let mut result = -1_i32;
        let mut packed_args = Vec::with_capacity(self.columns.len() + 4);
        packed_args.push(&mut len as *mut i64 as *mut ());
        for ptr in &mut ptr_values {
            packed_args.push(ptr as *mut *const () as *mut ());
        }
        packed_args.push(&mut output_sum_ptr as *mut *mut i128 as *mut ());
        packed_args.push(&mut output_count_ptr as *mut *mut i64 as *mut ());
        packed_args.push(&mut result as *mut i32 as *mut ());

        unsafe {
            self.engine
                .invoke_packed(&self.symbol, &mut packed_args)
                .map_err(|err| JitError::Backend(format!("MLIR invocation failed: {err:?}")))?;
        }
        if result != 0 {
            return Err(JitError::Backend(format!(
                "compiled decimal filter-sum returned status {result}"
            )));
        }
        Ok(DecimalFilterSumOutput {
            sum: output_sum,
            count: output_count,
        })
    }
}

impl DecimalFilterSumInput<'_> {
    fn index(&self) -> usize {
        match self {
            Self::Date32 { index, .. }
            | Self::Int64 { index, .. }
            | Self::Decimal128 { index, .. } => *index,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Date32 { values, .. } => values.len(),
            Self::Int64 { values, .. } => values.len(),
            Self::Decimal128 { values, .. } => values.len(),
        }
    }

    fn ptr(&self) -> *const () {
        match self {
            Self::Date32 { values, .. } => values.as_ptr().cast(),
            Self::Int64 { values, .. } => values.as_ptr().cast(),
            Self::Decimal128 { values, .. } => values.as_ptr().cast(),
        }
    }

    fn matches_type(&self, ty: JitType) -> bool {
        matches!(
            (self, ty),
            (Self::Date32 { .. }, JitType::Date32)
                | (Self::Int64 { .. }, JitType::Int64)
                | (Self::Decimal128 { .. }, JitType::Decimal128 { .. })
        )
    }
}

pub(super) fn compile_i64_predicate(compiled: &MlirModule) -> JitResult<CompiledI64Predicate> {
    Ok(CompiledI64Predicate {
        symbol: compiled.symbol.clone(),
        engine: compile_engine(compiled)?,
    })
}

pub(super) fn invoke_i64_predicate(compiled: &MlirModule, value: i64) -> JitResult<bool> {
    compile_i64_predicate(compiled)?.invoke(value)
}

pub fn compile_i64_filter(compiled: &MlirModule) -> JitResult<CompiledI64Filter> {
    Ok(CompiledI64Filter {
        symbol: compiled.symbol.clone(),
        engine: compile_engine(compiled)?,
    })
}

pub fn compile_i64_filter_project(compiled: &MlirModule) -> JitResult<CompiledI64FilterProject> {
    Ok(CompiledI64FilterProject {
        symbol: compiled.symbol.clone(),
        engine: compile_engine(compiled)?,
    })
}

pub fn compile_f64_filter_sum(compiled: &MlirModule) -> JitResult<CompiledF64FilterSum> {
    Ok(CompiledF64FilterSum {
        symbol: compiled.symbol.clone(),
        engine: compile_engine(compiled)?,
    })
}

pub fn compile_decimal_filter_sum(
    compiled: &MlirModule,
    columns: Vec<MlirColumn>,
) -> JitResult<CompiledDecimalFilterSum> {
    Ok(CompiledDecimalFilterSum {
        symbol: compiled.symbol.clone(),
        engine: compile_engine(compiled)?,
        columns,
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
