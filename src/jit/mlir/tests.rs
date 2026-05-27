use crate::jit::{JitBinaryOp, JitExpr, JitProjection, JitScalar, JitType, MlirBackend};

#[test]
fn emits_textual_filter_module() {
    let expr = i64_gt_ten(true);

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
    let expr = i64_gt_ten(true);

    let module = MlirBackend::new().lower_filter(&expr).unwrap();
    MlirBackend::new().verify_module(&module).unwrap();
}

#[test]
fn emits_filter_project_module() {
    let predicate = i64_gt_ten(true);
    let projection = JitProjection::new(
        JitExpr::Binary {
            op: JitBinaryOp::Add,
            left: Box::new(JitExpr::Column {
                index: 0,
                name: "a".to_string(),
                ty: JitType::Int64,
                nullable: true,
            }),
            right: Box::new(JitExpr::Literal(JitScalar::Int64(1))),
            ty: JitType::Int64,
            nullable: true,
        },
        "a_plus_one",
    );

    let module = MlirBackend::new()
        .lower_filter_project(&predicate, &[projection])
        .unwrap();
    assert!(module.text.contains("qjit.kind = filter_project"));
    assert!(module.text.contains("arith.cmpi sgt"));
    assert!(module.text.contains("arith.addi"));
    MlirBackend::new().verify_module(&module).unwrap();
}

#[test]
fn emits_i64_predicate_module() {
    let predicate = i64_gt_ten(false);

    let module = MlirBackend::new().lower_i64_predicate(&predicate).unwrap();
    assert!(module.text.contains("llvm.emit_c_interface"));
    assert!(module.text.contains("arith.select"));
    MlirBackend::new().verify_module(&module).unwrap();
}

#[cfg(feature = "jit-mlir")]
#[test]
fn invokes_i64_predicate_with_execution_engine() {
    let predicate = i64_gt_ten(false);

    let backend = MlirBackend::new();
    assert!(!backend.invoke_i64_predicate(&predicate, 10).unwrap());
    assert!(backend.invoke_i64_predicate(&predicate, 11).unwrap());
}

fn i64_gt_ten(nullable: bool) -> JitExpr {
    JitExpr::Binary {
        op: JitBinaryOp::Gt,
        left: Box::new(JitExpr::Column {
            index: 0,
            name: "a".to_string(),
            ty: JitType::Int64,
            nullable,
        }),
        right: Box::new(JitExpr::Literal(JitScalar::Int64(10))),
        ty: JitType::Bool,
        nullable,
    }
}
