#[cfg(feature = "jit-mlir")]
use crate::DecimalFilterSumInput;
use crate::{
    JitBinaryOp, JitExpr, JitProjection, JitScalar, JitType, MlirBackend, PipelineIr, PipelineKind,
    PipelineOp,
};

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
fn emits_quill_dialect_pipeline_skeleton() {
    let predicate = i64_gt_ten(false);
    let projections = vec![i64_plus_one_projection(0)];
    let pipeline = PipelineIr::new(vec![
        PipelineOp::Filter(predicate),
        PipelineOp::Projection(projections),
    ]);

    let module = MlirBackend::new().emit_quill_dialect("record_pipeline", &pipeline);

    assert_eq!(module.kind, PipelineKind::Record);
    assert_eq!(
        module.kernel_spec().map(|spec| spec.name()),
        Some("i64_filter_project")
    );
    assert!(module.text().contains("\"quill.pipeline\""));
    assert!(module
        .text()
        .contains("// qjit.kernel = i64_filter_project"));
    assert!(module.text().contains("\"quill.exec.filter\""));
    assert!(module.text().contains("\"quill.exec.project\""));
}

#[test]
fn emits_q6_quill_dialect_kernel_spec() {
    let pipeline = PipelineIr::filter_sum(q6_decimal_predicate(), q6_decimal_measure());
    let module = MlirBackend::new().emit_quill_dialect("q6_pipeline", &pipeline);

    assert_eq!(
        module.kernel_spec().map(|spec| spec.name()),
        Some("decimal_filter_sum")
    );
    assert!(module
        .text()
        .contains("// qjit.kernel = decimal_filter_sum"));
    assert!(module.text().contains("\"quill.sink.plain_sum\""));
}

#[test]
fn lowers_q6_quill_dialect_to_mlir() {
    let pipeline = PipelineIr::filter_sum(q6_decimal_predicate(), q6_decimal_measure());
    let dialect = MlirBackend::new().emit_quill_dialect("q6_decimal_pipeline", &pipeline);

    let module = MlirBackend::new().lower_quill_dialect(&dialect).unwrap();

    assert_eq!(module.symbol, "q6_decimal_pipeline");
    assert!(module.text.contains("func.func @q6_decimal_pipeline"));
    assert!(module.text.contains("qjit.lowering = quill_dialect"));
    assert!(module.text.contains("i128"));
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
    assert!(module.text.contains("qjit.lowering = quill_dialect"));
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
    assert!(module.text.contains("qjit.lowering = quill_dialect"));
    assert!(module.text.contains("scf.for unsigned"));
    assert!(module.text.contains("scf.if"));
    assert!(module.text.contains("arith.mulf"));
    assert!(module.text.contains("arith.addf"));
    assert!(module.text.contains("llvm.store"));
    MlirBackend::new().verify_module(&module).unwrap();
}

#[test]
fn emits_decimal_filter_sum_module() {
    let predicate = q6_decimal_predicate();
    let measure = q6_decimal_measure();

    let module = MlirBackend::new()
        .lower_decimal_filter_sum(&predicate, &measure)
        .unwrap();

    assert!(module.text.contains("func.func @quill_decimal_filter_sum_"));
    assert!(module.text.contains("qjit.lowering = quill_dialect"));
    assert!(module.text.contains("scf.for unsigned"));
    assert!(module.text.contains("scf.if"));
    assert!(module.text.contains("i128"));
    assert!(module.text.contains("arith.muli"));
    assert!(module.text.contains("arith.addi"));
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

    assert!((output.sum - 13.0).abs() < 0.000_001);
    assert_eq!(output.count, 2);
}

#[cfg(feature = "jit-mlir")]
#[test]
fn invokes_compiled_decimal_filter_sum_kernel() {
    let predicate = q6_decimal_predicate();
    let measure = q6_decimal_measure();
    let compiled = MlirBackend::new()
        .compile_decimal_filter_sum(&predicate, &measure)
        .unwrap();
    let shipdates = [9_i32, 10, 12, 20];
    let prices = [10_000_i128, 20_000, 30_000, 40_000];
    let discounts = [4_i128, 5, 7, 6];
    let quantities = [1_000_i128, 2_500, 2_000, 2_000];

    let output = compiled
        .invoke(&[
            DecimalFilterSumInput::Date32 {
                index: 0,
                values: &shipdates,
            },
            DecimalFilterSumInput::Decimal128 {
                index: 1,
                values: &prices,
            },
            DecimalFilterSumInput::Decimal128 {
                index: 2,
                values: &discounts,
            },
            DecimalFilterSumInput::Decimal128 {
                index: 3,
                values: &quantities,
            },
        ])
        .unwrap();

    assert_eq!(output.sum, 210_000);
    assert_eq!(output.count, 1);
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

fn q6_decimal_predicate() -> JitExpr {
    and(
        and(
            compare(JitBinaryOp::GtEq, date_col(0, "shipdate"), date_lit(10)),
            compare(JitBinaryOp::Lt, date_col(0, "shipdate"), date_lit(20)),
        ),
        and(
            and(
                compare(
                    JitBinaryOp::GtEq,
                    decimal_col(2, "discount", 2),
                    decimal_lit(5, 15, 2),
                ),
                compare(
                    JitBinaryOp::LtEq,
                    decimal_col(2, "discount", 2),
                    decimal_lit(7, 15, 2),
                ),
            ),
            compare(
                JitBinaryOp::Lt,
                decimal_col(3, "quantity", 2),
                decimal_lit(2_400, 15, 2),
            ),
        ),
    )
}

fn q6_decimal_measure() -> JitExpr {
    JitExpr::Binary {
        op: JitBinaryOp::Mul,
        left: Box::new(decimal_col(1, "extendedprice", 2)),
        right: Box::new(decimal_col(2, "discount", 2)),
        ty: JitType::Decimal128 {
            precision: 38,
            scale: 4,
        },
        nullable: false,
    }
}

fn and(left: JitExpr, right: JitExpr) -> JitExpr {
    JitExpr::Binary {
        op: JitBinaryOp::And,
        left: Box::new(left),
        right: Box::new(right),
        ty: JitType::Bool,
        nullable: false,
    }
}

fn compare(op: JitBinaryOp, left: JitExpr, right: JitExpr) -> JitExpr {
    JitExpr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
        ty: JitType::Bool,
        nullable: false,
    }
}

fn date_col(index: usize, name: &str) -> JitExpr {
    JitExpr::Column {
        index,
        name: name.to_string(),
        ty: JitType::Date32,
        nullable: false,
    }
}

fn date_lit(value: i32) -> JitExpr {
    JitExpr::Literal(JitScalar::Date32(value))
}

fn decimal_col(index: usize, name: &str, scale: i8) -> JitExpr {
    JitExpr::Column {
        index,
        name: name.to_string(),
        ty: JitType::Decimal128 {
            precision: 15,
            scale,
        },
        nullable: false,
    }
}

fn decimal_lit(value: i128, precision: u8, scale: i8) -> JitExpr {
    JitExpr::Literal(JitScalar::Decimal128 {
        value,
        precision,
        scale,
    })
}
