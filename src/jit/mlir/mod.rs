use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;

use crate::jit::{
    CompiledKernel, JitBinaryOp, JitExpr, JitProjection, JitResult, JitScalar, JitType,
    KernelBackend, KernelKind,
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

    pub fn lower_filter(&self, predicate: &JitExpr) -> MlirModule {
        let symbol = next_symbol("quill_filter");
        let mut text = module_header(&symbol);
        let _ = writeln!(text, "    // qjit.kind = filter");
        let _ = writeln!(text, "    // qjit.predicate = {}", format_expr(predicate));
        text.push_str("    %ok = arith.constant 0 : i32\n");
        text.push_str("    return %ok : i32\n");
        text.push_str("  }\n}\n");
        MlirModule { symbol, text }
    }

    pub fn lower_projection(&self, projections: &[JitProjection]) -> MlirModule {
        let symbol = next_symbol("quill_project");
        let mut text = module_header(&symbol);
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
        MlirModule { symbol, text }
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
        let module = self.lower_filter(predicate);
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
        let module = self.lower_projection(projections);
        Ok(CompiledKernel::new(
            module.symbol,
            KernelKind::Projection,
            self.name(),
            module.text,
            false,
        ))
    }
}

fn next_symbol(prefix: &str) -> String {
    let id = NEXT_KERNEL_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{id}")
}

fn module_header(symbol: &str) -> String {
    format!(
        "module {{\n  func.func @{symbol}(%len: index, %input: !llvm.ptr, %output: !llvm.ptr) -> i32 {{\n"
    )
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

        let module = MlirBackend::new().lower_filter(&expr);
        assert!(module.text.contains("func.func @quill_filter_"));
        assert!(module.text.contains("qjit.kind = filter"));
        assert!(module
            .text
            .contains("col(0, a, i64, nullable=true) > 10:i64"));
    }
}
