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

#[test]
fn emits_i64_filter_module() {
    let predicate = i64_gt_ten(false);

    let module = MlirBackend::new().lower_i64_filter(&predicate).unwrap();
    assert!(module.text.contains("func.func @quill_i64_filter_"));
    assert!(module.text.contains("scf.for unsigned"));
    assert!(module.text.contains("llvm.load"));
    assert!(module.text.contains("llvm.store"));
    MlirBackend::new().verify_module(&module).unwrap();
}

#[test]
fn emits_i64_filter_project_module() {
    let predicate = i64_gt_ten(false);
    let projections = vec![i64_plus_one_projection(0)];

    let module = MlirBackend::new()
        .lower_i64_filter_project(&predicate, &projections)
        .unwrap();
    assert!(module.text.contains("func.func @quill_i64_filter_project_"));
    assert!(module.text.contains("scf.for unsigned"));
    assert!(module.text.contains("scf.if"));
    assert!(module.text.contains("llvm.load"));
    assert!(module.text.contains("llvm.store"));
    MlirBackend::new().verify_module(&module).unwrap();
}

#[test]
fn emits_f64_filter_sum_module() {
    let predicate = i64_gt_ten(false);
    let measure = f64_product_measure();

    let module = MlirBackend::new()
        .lower_f64_filter_sum(&predicate, &measure)
        .unwrap();
    assert!(module.text.contains("func.func @quill_f64_filter_sum_"));
    assert!(module.text.contains("scf.for unsigned"));
    assert!(module.text.contains("scf.if"));
    assert!(module.text.contains("arith.mulf"));
    assert!(module.text.contains("arith.addf"));
    assert!(module.text.contains("llvm.store"));
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

#[cfg(feature = "jit-mlir")]
#[test]
fn reuses_compiled_i64_predicate_artifact() {
    let predicate = i64_gt_ten(false);
    let module = MlirBackend::new().lower_i64_predicate(&predicate).unwrap();
    let compiled = super::compiled::compile_i64_predicate(&module).unwrap();

    assert!(!compiled.invoke(9).unwrap());
    assert!(!compiled.invoke(10).unwrap());
    assert!(compiled.invoke(11).unwrap());
}

#[cfg(feature = "jit-mlir")]
#[test]
fn invokes_compiled_i64_filter_kernel() {
    let predicate = i64_gt_ten(false);
    let compiled = MlirBackend::new().compile_i64_filter(&predicate).unwrap();
    let input = [9_i64, 10, 11, 42];
    let mut output = [255_u8; 4];

    compiled.invoke(&input, &mut output).unwrap();

    assert_eq!(output, [0, 0, 1, 1]);
}

#[cfg(feature = "jit-mlir")]
#[test]
fn invokes_compiled_i64_filter_with_nonzero_column_index() {
    let predicate = JitExpr::Binary {
        op: JitBinaryOp::Gt,
        left: Box::new(JitExpr::Column {
            index: 1,
            name: "v".to_string(),
            ty: JitType::Int64,
            nullable: false,
        }),
        right: Box::new(JitExpr::Literal(JitScalar::Int64(10))),
        ty: JitType::Bool,
        nullable: false,
    };
    let compiled = MlirBackend::new().compile_i64_filter(&predicate).unwrap();
    let input = [10_i64, 11, 12];
    let mut output = [0_u8; 3];

    compiled.invoke(&input, &mut output).unwrap();

    assert_eq!(output, [0, 1, 1]);
}

#[cfg(feature = "jit-mlir")]
#[test]
fn invokes_compiled_i64_filter_project_kernel() {
    let predicate = JitExpr::Binary {
        op: JitBinaryOp::Gt,
        left: Box::new(JitExpr::Column {
            index: 1,
            name: "v".to_string(),
            ty: JitType::Int64,
            nullable: false,
        }),
        right: Box::new(JitExpr::Literal(JitScalar::Int64(10))),
        ty: JitType::Bool,
        nullable: false,
    };
    let projections = vec![i64_plus_one_projection(0)];
    let compiled = MlirBackend::new()
        .compile_i64_filter_project(&predicate, &projections)
        .unwrap();
    let predicate_values = [9_i64, 11, 12];
    let projection_values = [100_i64, 200, 300];
    let mut output = [0_i64; 3];

    let output_len = compiled
        .invoke(&predicate_values, &projection_values, &mut output)
        .unwrap();

    assert_eq!(output_len, 2);
    assert_eq!(&output[..output_len], [201, 301]);
}

#[cfg(feature = "jit-mlir")]
#[test]
fn invokes_compiled_f64_filter_sum_kernel() {
    let predicate = i64_gt_ten(false);
    let measure = f64_product_measure();
    let compiled = MlirBackend::new()
        .compile_f64_filter_sum(&predicate, &measure)
        .unwrap();
    let predicate_values = [9_i64, 11, 12];
    let left_values = [10.0_f64, 20.0, 30.0];
    let right_values = [0.1_f64, 0.2, 0.3];

    let output = compiled
        .invoke(&predicate_values, &left_values, &right_values)
        .unwrap();

    assert!((output - 13.0).abs() < 0.000_001);
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

fn i64_plus_one_projection(index: usize) -> JitProjection {
    JitProjection::new(
        JitExpr::Binary {
            op: JitBinaryOp::Add,
            left: Box::new(JitExpr::Column {
                index,
                name: format!("c{index}"),
                ty: JitType::Int64,
                nullable: false,
            }),
            right: Box::new(JitExpr::Literal(JitScalar::Int64(1))),
            ty: JitType::Int64,
            nullable: false,
        },
        "plus_one",
    )
}

fn f64_product_measure() -> JitExpr {
    JitExpr::Binary {
        op: JitBinaryOp::Mul,
        left: Box::new(JitExpr::Column {
            index: 0,
            name: "left".to_string(),
            ty: JitType::Float64,
            nullable: false,
        }),
        right: Box::new(JitExpr::Column {
            index: 1,
            name: "right".to_string(),
            ty: JitType::Float64,
            nullable: false,
        }),
        ty: JitType::Float64,
        nullable: false,
    }
}
