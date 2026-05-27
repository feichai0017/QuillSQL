use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;

use crate::jit::{
    CompiledKernel, JitBinaryOp, JitError, JitExpr, JitProjection, JitResult, JitScalar, JitType,
    KernelBackend, KernelKind,
};

#[cfg(feature = "jit-mlir")]
use melior::{
    dialect::DialectRegistry,
    ir::{operation::OperationLike, Module},
    utility::{register_all_dialects, register_all_llvm_translations},
    Context,
};

static NEXT_KERNEL_ID: AtomicU64 = AtomicU64::new(1);

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
        let symbol = next_symbol("quill_filter");
        let mut text = start_module();
        text.push_str(&scalar_function(&format!("{symbol}_expr"), predicate)?);
        append_abi_function_header(&mut text, &symbol);
        let _ = writeln!(text, "    // qjit.kind = filter");
        let _ = writeln!(text, "    // qjit.predicate = {}", format_expr(predicate));
        text.push_str("    %ok = arith.constant 0 : i32\n");
        text.push_str("    return %ok : i32\n");
        text.push_str("  }\n}\n");
        Ok(MlirModule { symbol, text })
    }

    pub fn lower_projection(&self, projections: &[JitProjection]) -> JitResult<MlirModule> {
        let symbol = next_symbol("quill_project");
        let mut text = start_module();
        for (index, projection) in projections.iter().enumerate() {
            text.push_str(&scalar_function(
                &format!("{symbol}_expr_{index}"),
                &projection.expr,
            )?);
        }
        append_abi_function_header(&mut text, &symbol);
        let _ = writeln!(text, "    // qjit.kind = projection");
        for projection in projections {
            let _ = writeln!(
                text,
                "    // qjit.project {} = {}",
                projection.alias,
                format_expr(&projection.expr)
            );
        }
        text.push_str("    %ok = arith.constant 0 : i32\n");
        text.push_str("    return %ok : i32\n");
        text.push_str("  }\n}\n");
        Ok(MlirModule { symbol, text })
    }

    pub fn verify_module(&self, module: &MlirModule) -> JitResult<()> {
        verify_mlir_text(&module.text)
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
fn mlir_context() -> Context {
    let context = Context::new();
    let registry = DialectRegistry::new();
    register_all_dialects(&registry);
    context.append_dialect_registry(&registry);
    context.load_all_available_dialects();
    register_all_llvm_translations(&context);
    context
}

fn next_symbol(prefix: &str) -> String {
    let id = NEXT_KERNEL_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{id}")
}

fn start_module() -> String {
    "module {\n".to_string()
}

fn append_abi_function_header(text: &mut String, symbol: &str) {
    let _ = writeln!(
        text,
        "  func.func @{symbol}(%len: index, %input: !llvm.ptr, %output: !llvm.ptr) -> i32 {{"
    );
}

fn scalar_function(symbol: &str, expr: &JitExpr) -> JitResult<String> {
    let mut columns = BTreeMap::new();
    collect_columns(expr, &mut columns);
    let args = columns
        .iter()
        .map(|(index, ty)| format!("%c{index}: {}", mlir_type(*ty)))
        .collect::<Vec<_>>()
        .join(", ");

    let mut emitter = ScalarEmitter::default();
    let value = emitter.emit_expr(expr)?;
    let mut text = String::new();
    let _ = writeln!(
        text,
        "  func.func private @{symbol}({args}) -> {} {{",
        mlir_type(value.ty)
    );
    for line in emitter.lines {
        let _ = writeln!(text, "{line}");
    }
    let _ = writeln!(text, "    return {} : {}", value.name, mlir_type(value.ty));
    text.push_str("  }\n");
    Ok(text)
}

fn collect_columns(expr: &JitExpr, columns: &mut BTreeMap<usize, JitType>) {
    match expr {
        JitExpr::Column { index, ty, .. } => {
            columns.insert(*index, *ty);
        }
        JitExpr::Literal(_) => {}
        JitExpr::Binary { left, right, .. } => {
            collect_columns(left, columns);
            collect_columns(right, columns);
        }
        JitExpr::IsNull(arg) => collect_columns(arg, columns),
    }
}

#[derive(Debug, Clone)]
struct ScalarValueRef {
    name: String,
    ty: JitType,
}

#[derive(Debug, Default)]
struct ScalarEmitter {
    next_id: usize,
    lines: Vec<String>,
}

impl ScalarEmitter {
    fn emit_expr(&mut self, expr: &JitExpr) -> JitResult<ScalarValueRef> {
        match expr {
            JitExpr::Column { index, ty, .. } => Ok(ScalarValueRef {
                name: format!("%c{index}"),
                ty: *ty,
            }),
            JitExpr::Literal(value) => self.emit_literal(value),
            JitExpr::Binary {
                op, left, right, ..
            } => self.emit_binary(*op, left, right),
            JitExpr::IsNull(_) => Err(JitError::UnsupportedExpr(
                "MLIR lowering does not yet model Arrow validity bitmaps".to_string(),
            )),
        }
    }

    fn emit_literal(&mut self, value: &JitScalar) -> JitResult<ScalarValueRef> {
        let ty = value.ty();
        let name = self.next_value("lit");
        match value {
            JitScalar::Null(_) => {
                return Err(JitError::UnsupportedExpr(
                    "MLIR lowering does not yet model null literals".to_string(),
                ));
            }
            JitScalar::Bool(value) => {
                self.lines.push(format!(
                    "    {name} = arith.constant {value} : {}",
                    mlir_type(ty)
                ));
            }
            JitScalar::Int32(value) => {
                self.lines.push(format!(
                    "    {name} = arith.constant {value} : {}",
                    mlir_type(ty)
                ));
            }
            JitScalar::Int64(value) => {
                self.lines.push(format!(
                    "    {name} = arith.constant {value} : {}",
                    mlir_type(ty)
                ));
            }
            JitScalar::Float64(value) => {
                self.lines.push(format!(
                    "    {name} = arith.constant {} : {}",
                    format_float(*value),
                    mlir_type(ty)
                ));
            }
        }
        Ok(ScalarValueRef { name, ty })
    }

    fn emit_binary(
        &mut self,
        op: JitBinaryOp,
        left: &JitExpr,
        right: &JitExpr,
    ) -> JitResult<ScalarValueRef> {
        let lhs = self.emit_expr(left)?;
        let rhs = self.emit_expr(right)?;
        match op {
            JitBinaryOp::Add | JitBinaryOp::Sub | JitBinaryOp::Mul | JitBinaryOp::Div => {
                self.emit_arithmetic(op, lhs, rhs)
            }
            JitBinaryOp::Eq
            | JitBinaryOp::NotEq
            | JitBinaryOp::Lt
            | JitBinaryOp::LtEq
            | JitBinaryOp::Gt
            | JitBinaryOp::GtEq => self.emit_comparison(op, lhs, rhs),
            JitBinaryOp::And | JitBinaryOp::Or => self.emit_boolean(op, lhs, rhs),
        }
    }

    fn emit_arithmetic(
        &mut self,
        op: JitBinaryOp,
        lhs: ScalarValueRef,
        rhs: ScalarValueRef,
    ) -> JitResult<ScalarValueRef> {
        ensure_same_type(&lhs, &rhs)?;
        let opcode = match (op, lhs.ty) {
            (JitBinaryOp::Add, JitType::Int32 | JitType::Int64) => "addi",
            (JitBinaryOp::Sub, JitType::Int32 | JitType::Int64) => "subi",
            (JitBinaryOp::Mul, JitType::Int32 | JitType::Int64) => "muli",
            (JitBinaryOp::Div, JitType::Int32 | JitType::Int64) => "divsi",
            (JitBinaryOp::Add, JitType::Float64) => "addf",
            (JitBinaryOp::Sub, JitType::Float64) => "subf",
            (JitBinaryOp::Mul, JitType::Float64) => "mulf",
            (JitBinaryOp::Div, JitType::Float64) => "divf",
            _ => {
                return Err(JitError::UnsupportedExpr(format!(
                    "operator {} is not supported for {}",
                    format_op(op),
                    mlir_type(lhs.ty)
                )));
            }
        };
        let result = self.next_value("arith");
        self.lines.push(format!(
            "    {result} = arith.{opcode} {}, {} : {}",
            lhs.name,
            rhs.name,
            mlir_type(lhs.ty)
        ));
        Ok(ScalarValueRef {
            name: result,
            ty: lhs.ty,
        })
    }

    fn emit_comparison(
        &mut self,
        op: JitBinaryOp,
        lhs: ScalarValueRef,
        rhs: ScalarValueRef,
    ) -> JitResult<ScalarValueRef> {
        ensure_same_type(&lhs, &rhs)?;
        let result = self.next_value("cmp");
        match lhs.ty {
            JitType::Float64 => {
                let predicate = match op {
                    JitBinaryOp::Eq => "oeq",
                    JitBinaryOp::NotEq => "one",
                    JitBinaryOp::Lt => "olt",
                    JitBinaryOp::LtEq => "ole",
                    JitBinaryOp::Gt => "ogt",
                    JitBinaryOp::GtEq => "oge",
                    _ => unreachable!(),
                };
                self.lines.push(format!(
                    "    {result} = arith.cmpf {predicate}, {}, {} : {}",
                    lhs.name,
                    rhs.name,
                    mlir_type(lhs.ty)
                ));
            }
            JitType::Bool if matches!(op, JitBinaryOp::Eq | JitBinaryOp::NotEq) => {
                let predicate = if matches!(op, JitBinaryOp::Eq) {
                    "eq"
                } else {
                    "ne"
                };
                self.lines.push(format!(
                    "    {result} = arith.cmpi {predicate}, {}, {} : {}",
                    lhs.name,
                    rhs.name,
                    mlir_type(lhs.ty)
                ));
            }
            JitType::Bool => {
                return Err(JitError::UnsupportedExpr(format!(
                    "ordered comparison {} is not supported for bool",
                    format_op(op)
                )));
            }
            JitType::Int32 | JitType::Int64 => {
                let predicate = match op {
                    JitBinaryOp::Eq => "eq",
                    JitBinaryOp::NotEq => "ne",
                    JitBinaryOp::Lt => "slt",
                    JitBinaryOp::LtEq => "sle",
                    JitBinaryOp::Gt => "sgt",
                    JitBinaryOp::GtEq => "sge",
                    _ => unreachable!(),
                };
                self.lines.push(format!(
                    "    {result} = arith.cmpi {predicate}, {}, {} : {}",
                    lhs.name,
                    rhs.name,
                    mlir_type(lhs.ty)
                ));
            }
        }
        Ok(ScalarValueRef {
            name: result,
            ty: JitType::Bool,
        })
    }

    fn emit_boolean(
        &mut self,
        op: JitBinaryOp,
        lhs: ScalarValueRef,
        rhs: ScalarValueRef,
    ) -> JitResult<ScalarValueRef> {
        ensure_same_type(&lhs, &rhs)?;
        if lhs.ty != JitType::Bool {
            return Err(JitError::UnsupportedExpr(format!(
                "boolean operator {} requires i1 inputs",
                format_op(op)
            )));
        }
        let opcode = match op {
            JitBinaryOp::And => "andi",
            JitBinaryOp::Or => "ori",
            _ => unreachable!(),
        };
        let result = self.next_value("bool");
        self.lines.push(format!(
            "    {result} = arith.{opcode} {}, {} : i1",
            lhs.name, rhs.name
        ));
        Ok(ScalarValueRef {
            name: result,
            ty: JitType::Bool,
        })
    }

    fn next_value(&mut self, prefix: &str) -> String {
        let id = self.next_id;
        self.next_id += 1;
        format!("%{prefix}{id}")
    }
}

fn ensure_same_type(lhs: &ScalarValueRef, rhs: &ScalarValueRef) -> JitResult<()> {
    if lhs.ty == rhs.ty {
        Ok(())
    } else {
        Err(JitError::UnsupportedExpr(format!(
            "type mismatch: {} vs {}",
            mlir_type(lhs.ty),
            mlir_type(rhs.ty)
        )))
    }
}

fn mlir_type(ty: JitType) -> &'static str {
    match ty {
        JitType::Bool => "i1",
        JitType::Int32 => "i32",
        JitType::Int64 => "i64",
        JitType::Float64 => "f64",
    }
}

fn format_float(value: f64) -> String {
    if value.is_finite() {
        format!("{value:e}")
    } else if value.is_nan() {
        "0.0".to_string()
    } else if value.is_sign_positive() {
        "1.7976931348623157e308".to_string()
    } else {
        "-1.7976931348623157e308".to_string()
    }
}

fn format_expr(expr: &JitExpr) -> String {
    match expr {
        JitExpr::Column {
            index,
            name,
            ty,
            nullable,
        } => format!(
            "col({index}, {name}, {}, nullable={nullable})",
            format_type(*ty)
        ),
        JitExpr::Literal(value) => format_scalar(value),
        JitExpr::Binary {
            op, left, right, ..
        } => format!(
            "({} {} {})",
            format_expr(left),
            format_op(*op),
            format_expr(right)
        ),
        JitExpr::IsNull(arg) => format!("is_null({})", format_expr(arg)),
    }
}

fn format_scalar(value: &JitScalar) -> String {
    match value {
        JitScalar::Null(ty) => format!("null:{}", format_type(*ty)),
        JitScalar::Bool(value) => value.to_string(),
        JitScalar::Int32(value) => format!("{value}:i32"),
        JitScalar::Int64(value) => format!("{value}:i64"),
        JitScalar::Float64(value) => format!("{value}:f64"),
    }
}

fn format_type(ty: JitType) -> &'static str {
    match ty {
        JitType::Bool => "i1",
        JitType::Int32 => "i32",
        JitType::Int64 => "i64",
        JitType::Float64 => "f64",
    }
}

fn format_op(op: JitBinaryOp) -> &'static str {
    match op {
        JitBinaryOp::Add => "+",
        JitBinaryOp::Sub => "-",
        JitBinaryOp::Mul => "*",
        JitBinaryOp::Div => "/",
        JitBinaryOp::Eq => "==",
        JitBinaryOp::NotEq => "!=",
        JitBinaryOp::Lt => "<",
        JitBinaryOp::LtEq => "<=",
        JitBinaryOp::Gt => ">",
        JitBinaryOp::GtEq => ">=",
        JitBinaryOp::And => "and",
        JitBinaryOp::Or => "or",
    }
}

#[cfg(test)]
mod tests {
    use crate::jit::{JitBinaryOp, JitExpr, JitScalar, JitType, MlirBackend};

    #[test]
    fn emits_textual_filter_module() {
        let expr = JitExpr::Binary {
            op: JitBinaryOp::Gt,
            left: Box::new(JitExpr::Column {
                index: 0,
                name: "a".to_string(),
                ty: JitType::Int64,
                nullable: true,
            }),
            right: Box::new(JitExpr::Literal(JitScalar::Int64(10))),
            ty: JitType::Bool,
            nullable: true,
        };

        let module = MlirBackend::new().lower_filter(&expr).unwrap();
        assert!(module.text.contains("func.func @quill_filter_"));
        assert!(module.text.contains("arith.cmpi sgt"));
        assert!(module.text.contains("qjit.kind = filter"));
        assert!(module
            .text
            .contains("col(0, a, i64, nullable=true) > 10:i64"));
    }

    #[test]
    fn backend_verifies_generated_module() {
        let expr = JitExpr::Binary {
            op: JitBinaryOp::Gt,
            left: Box::new(JitExpr::Column {
                index: 0,
                name: "a".to_string(),
                ty: JitType::Int64,
                nullable: true,
            }),
            right: Box::new(JitExpr::Literal(JitScalar::Int64(10))),
            ty: JitType::Bool,
            nullable: true,
        };

        let module = MlirBackend::new().lower_filter(&expr).unwrap();
        MlirBackend::new().verify_module(&module).unwrap();
    }
}
