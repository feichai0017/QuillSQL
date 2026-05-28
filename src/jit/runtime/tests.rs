use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, Float64Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

use crate::jit::{JitBinaryOp, JitExpr, JitProjection, JitScalar, JitType};

use super::{FilterProjectKernel, FilterSumKernel};

#[test]
fn executes_filter_project_with_nulls() {
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "next_id",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        input_schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), None])),
            Arc::new(Int64Array::from(vec![Some(10), Some(20), None, Some(40)])),
        ],
    )
    .unwrap();
    let kernel = FilterProjectKernel::try_new(
        JitExpr::Binary {
            op: JitBinaryOp::Gt,
            left: Box::new(JitExpr::Column {
                index: 1,
                name: "v".to_string(),
                ty: JitType::Int64,
                nullable: true,
            }),
            right: Box::new(JitExpr::Literal(JitScalar::Int64(10))),
            ty: JitType::Bool,
            nullable: true,
        },
        vec![JitProjection::new(
            JitExpr::Binary {
                op: JitBinaryOp::Add,
                left: Box::new(JitExpr::Column {
                    index: 0,
                    name: "id".to_string(),
                    ty: JitType::Int64,
                    nullable: true,
                }),
                right: Box::new(JitExpr::Literal(JitScalar::Int64(1))),
                ty: JitType::Int64,
                nullable: true,
            },
            "next_id",
        )],
        output_schema,
    )
    .unwrap();

    let output = kernel.execute(&batch).unwrap();
    let values = output
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values.value(0), 3);
    assert!(values.is_null(1));
}

#[test]
fn implements_sql_three_valued_boolean_logic() {
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Boolean, true),
        Field::new("b", DataType::Boolean, true),
    ]));
    let output_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, true)]));
    let batch = RecordBatch::try_new(
        input_schema,
        vec![
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(true),
                None,
                Some(false),
            ])),
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(true), None])),
        ],
    )
    .unwrap();
    let kernel = FilterProjectKernel::try_new(
        JitExpr::Binary {
            op: JitBinaryOp::Or,
            left: Box::new(JitExpr::Column {
                index: 0,
                name: "a".to_string(),
                ty: JitType::Bool,
                nullable: true,
            }),
            right: Box::new(JitExpr::Column {
                index: 1,
                name: "b".to_string(),
                ty: JitType::Bool,
                nullable: true,
            }),
            ty: JitType::Bool,
            nullable: true,
        },
        vec![JitProjection::new(
            JitExpr::Column {
                index: 0,
                name: "a".to_string(),
                ty: JitType::Bool,
                nullable: true,
            },
            "a",
        )],
        output_schema,
    )
    .unwrap();

    let output = kernel.execute(&batch).unwrap();
    let values = output
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(values.len(), 3);
    assert!(values.value(0));
    assert!(values.value(1));
    assert!(values.is_null(2));
}

#[test]
fn executes_filter_sum_fast_path_with_nulls() {
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, true),
        Field::new("price", DataType::Float64, true),
        Field::new("discount", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        input_schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(9), Some(11), None, Some(12)])),
            Arc::new(Float64Array::from(vec![
                Some(10.0),
                Some(20.0),
                Some(30.0),
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(0.1),
                Some(0.2),
                Some(0.3),
                Some(0.4),
            ])),
        ],
    )
    .unwrap();
    let kernel = FilterSumKernel::try_new(
        JitExpr::Binary {
            op: JitBinaryOp::Gt,
            left: Box::new(JitExpr::Column {
                index: 0,
                name: "v".to_string(),
                ty: JitType::Int64,
                nullable: true,
            }),
            right: Box::new(JitExpr::Literal(JitScalar::Int64(10))),
            ty: JitType::Bool,
            nullable: true,
        },
        JitExpr::Binary {
            op: JitBinaryOp::Mul,
            left: Box::new(JitExpr::Column {
                index: 1,
                name: "price".to_string(),
                ty: JitType::Float64,
                nullable: true,
            }),
            right: Box::new(JitExpr::Column {
                index: 2,
                name: "discount".to_string(),
                ty: JitType::Float64,
                nullable: true,
            }),
            ty: JitType::Float64,
            nullable: true,
        },
    )
    .unwrap();

    let output = kernel.execute(&batch).unwrap();
    assert_eq!(output, Some(4.0));
}
