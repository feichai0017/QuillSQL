use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int32Array,
    Int32Builder, Int64Array, Int64Builder,
};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, SchemaRef as ArrowSchemaRef};
use datafusion::arrow::record_batch::RecordBatch;

use crate::jit::{JitBinaryOp, JitError, JitExpr, JitProjection, JitResult, JitScalar, JitType};

#[derive(Debug, Clone)]
pub struct FilterProjectKernel {
    predicate: JitExpr,
    projections: Vec<JitProjection>,
    schema: ArrowSchemaRef,
}

impl FilterProjectKernel {
    pub fn try_new(
        predicate: JitExpr,
        projections: Vec<JitProjection>,
        schema: ArrowSchemaRef,
    ) -> JitResult<Self> {
        if predicate.ty() != JitType::Bool {
            return Err(JitError::UnsupportedExpr(format!(
                "filter predicate must be bool, got {:?}",
                predicate.ty()
            )));
        }
        ensure_supported_expr(&predicate)?;
        if projections.len() != schema.fields().len() {
            return Err(JitError::Backend(format!(
                "projection count {} does not match output schema width {}",
                projections.len(),
                schema.fields().len()
            )));
        }
        for (projection, field) in projections.iter().zip(schema.fields()) {
            ensure_supported_expr(&projection.expr)?;
            let expected = arrow_type(projection.expr.ty());
            if field.data_type() != &expected {
                return Err(JitError::Backend(format!(
                    "projection {} has type {:?}, but output schema expects {:?}",
                    projection.alias,
                    expected,
                    field.data_type()
                )));
            }
        }

        Ok(Self {
            predicate,
            projections,
            schema,
        })
    }

    pub fn predicate(&self) -> &JitExpr {
        &self.predicate
    }

    pub fn projections(&self) -> &[JitProjection] {
        &self.projections
    }

    pub fn execute(&self, batch: &RecordBatch) -> JitResult<RecordBatch> {
        let view = BatchView::try_new(batch)?;
        let mut builders = self
            .projections
            .iter()
            .map(|projection| OutputBuilder::with_capacity(projection.expr.ty(), batch.num_rows()))
            .collect::<Vec<_>>();

        for row in 0..batch.num_rows() {
            if !eval_expr(&self.predicate, &view, row)?.is_filter_true()? {
                continue;
            }
            for (projection, builder) in self.projections.iter().zip(&mut builders) {
                builder.append(eval_expr(&projection.expr, &view, row)?)?;
            }
        }

        let arrays = builders
            .into_iter()
            .map(OutputBuilder::finish)
            .collect::<JitResult<Vec<_>>>()?;
        RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .map_err(|err| JitError::Backend(err.to_string()))
    }
}

#[derive(Clone, Copy)]
enum Scalar {
    Bool(Option<bool>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float64(Option<f64>),
}

impl Scalar {
    fn is_filter_true(self) -> JitResult<bool> {
        match self {
            Self::Bool(value) => Ok(value.unwrap_or(false)),
            other => Err(JitError::Backend(format!(
                "predicate produced non-bool value {:?}",
                other.ty()
            ))),
        }
    }

    fn ty(self) -> JitType {
        match self {
            Self::Bool(_) => JitType::Bool,
            Self::Int32(_) => JitType::Int32,
            Self::Int64(_) => JitType::Int64,
            Self::Float64(_) => JitType::Float64,
        }
    }
}

struct BatchView<'a> {
    columns: Vec<ColumnView<'a>>,
}

impl<'a> BatchView<'a> {
    fn try_new(batch: &'a RecordBatch) -> JitResult<Self> {
        let columns = batch
            .columns()
            .iter()
            .map(|array| ColumnView::try_new(array.as_ref()))
            .collect::<JitResult<Vec<_>>>()?;
        Ok(Self { columns })
    }

    fn value(&self, index: usize, row: usize) -> JitResult<Scalar> {
        let column = self
            .columns
            .get(index)
            .ok_or_else(|| JitError::Backend(format!("column index {index} out of bounds")))?;
        column.value(row)
    }
}

enum ColumnView<'a> {
    Bool(&'a BooleanArray),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float64(&'a Float64Array),
}

impl<'a> ColumnView<'a> {
    fn try_new(array: &'a dyn Array) -> JitResult<Self> {
        match array.data_type() {
            ArrowDataType::Boolean => array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(Self::Bool)
                .ok_or_else(|| JitError::UnsupportedType("Boolean".to_string())),
            ArrowDataType::Int32 => array
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(Self::Int32)
                .ok_or_else(|| JitError::UnsupportedType("Int32".to_string())),
            ArrowDataType::Int64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(Self::Int64)
                .ok_or_else(|| JitError::UnsupportedType("Int64".to_string())),
            ArrowDataType::Float64 => array
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(Self::Float64)
                .ok_or_else(|| JitError::UnsupportedType("Float64".to_string())),
            other => Err(JitError::UnsupportedType(format!("{other:?}"))),
        }
    }

    fn value(&self, row: usize) -> JitResult<Scalar> {
        match self {
            Self::Bool(array) => Ok(Scalar::Bool(array.is_valid(row).then(|| array.value(row)))),
            Self::Int32(array) => Ok(Scalar::Int32(array.is_valid(row).then(|| array.value(row)))),
            Self::Int64(array) => Ok(Scalar::Int64(array.is_valid(row).then(|| array.value(row)))),
            Self::Float64(array) => Ok(Scalar::Float64(
                array.is_valid(row).then(|| array.value(row)),
            )),
        }
    }
}

enum OutputBuilder {
    Bool(BooleanBuilder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float64(Float64Builder),
}

impl OutputBuilder {
    fn with_capacity(ty: JitType, capacity: usize) -> Self {
        match ty {
            JitType::Bool => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            JitType::Int32 => Self::Int32(Int32Builder::with_capacity(capacity)),
            JitType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            JitType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
        }
    }

    fn append(&mut self, value: Scalar) -> JitResult<()> {
        match (self, value) {
            (Self::Bool(builder), Scalar::Bool(value)) => builder.append_option(value),
            (Self::Int32(builder), Scalar::Int32(value)) => builder.append_option(value),
            (Self::Int64(builder), Scalar::Int64(value)) => builder.append_option(value),
            (Self::Float64(builder), Scalar::Float64(value)) => builder.append_option(value),
            (_, value) => {
                return Err(JitError::Backend(format!(
                    "cannot append {:?} into output builder",
                    value.ty()
                )));
            }
        }
        Ok(())
    }

    fn finish(mut self) -> JitResult<ArrayRef> {
        let array = match &mut self {
            Self::Bool(builder) => Arc::new(builder.finish()) as ArrayRef,
            Self::Int32(builder) => Arc::new(builder.finish()) as ArrayRef,
            Self::Int64(builder) => Arc::new(builder.finish()) as ArrayRef,
            Self::Float64(builder) => Arc::new(builder.finish()) as ArrayRef,
        };
        Ok(array)
    }
}

fn eval_expr(expr: &JitExpr, view: &BatchView<'_>, row: usize) -> JitResult<Scalar> {
    match expr {
        JitExpr::Column { index, .. } => view.value(*index, row),
        JitExpr::Literal(value) => Ok(eval_literal(value)),
        JitExpr::Binary {
            op, left, right, ..
        } => eval_binary(
            *op,
            eval_expr(left, view, row)?,
            eval_expr(right, view, row)?,
        ),
        JitExpr::IsNull(arg) => Ok(Scalar::Bool(Some(is_null(eval_expr(arg, view, row)?)))),
    }
}

fn eval_literal(value: &JitScalar) -> Scalar {
    match value {
        JitScalar::Null(ty) => match ty {
            JitType::Bool => Scalar::Bool(None),
            JitType::Int32 => Scalar::Int32(None),
            JitType::Int64 => Scalar::Int64(None),
            JitType::Float64 => Scalar::Float64(None),
        },
        JitScalar::Bool(value) => Scalar::Bool(Some(*value)),
        JitScalar::Int32(value) => Scalar::Int32(Some(*value)),
        JitScalar::Int64(value) => Scalar::Int64(Some(*value)),
        JitScalar::Float64(value) => Scalar::Float64(Some(*value)),
    }
}

fn eval_binary(op: JitBinaryOp, lhs: Scalar, rhs: Scalar) -> JitResult<Scalar> {
    match op {
        JitBinaryOp::Add | JitBinaryOp::Sub | JitBinaryOp::Mul => eval_arithmetic(op, lhs, rhs),
        JitBinaryOp::Eq
        | JitBinaryOp::NotEq
        | JitBinaryOp::Lt
        | JitBinaryOp::LtEq
        | JitBinaryOp::Gt
        | JitBinaryOp::GtEq => eval_comparison(op, lhs, rhs),
        JitBinaryOp::And | JitBinaryOp::Or => eval_boolean(op, lhs, rhs),
        JitBinaryOp::Div => Err(JitError::UnsupportedExpr(
            "division is not yet supported by the fixed-width kernel".to_string(),
        )),
    }
}

fn eval_arithmetic(op: JitBinaryOp, lhs: Scalar, rhs: Scalar) -> JitResult<Scalar> {
    match (lhs, rhs) {
        (Scalar::Int32(lhs), Scalar::Int32(rhs)) => Ok(Scalar::Int32(option_zip(lhs, rhs).map(
            |(lhs, rhs)| match op {
                JitBinaryOp::Add => lhs + rhs,
                JitBinaryOp::Sub => lhs - rhs,
                JitBinaryOp::Mul => lhs * rhs,
                _ => unreachable!(),
            },
        ))),
        (Scalar::Int64(lhs), Scalar::Int64(rhs)) => Ok(Scalar::Int64(option_zip(lhs, rhs).map(
            |(lhs, rhs)| match op {
                JitBinaryOp::Add => lhs + rhs,
                JitBinaryOp::Sub => lhs - rhs,
                JitBinaryOp::Mul => lhs * rhs,
                _ => unreachable!(),
            },
        ))),
        (Scalar::Float64(lhs), Scalar::Float64(rhs)) => Ok(Scalar::Float64(
            option_zip(lhs, rhs).map(|(lhs, rhs)| match op {
                JitBinaryOp::Add => lhs + rhs,
                JitBinaryOp::Sub => lhs - rhs,
                JitBinaryOp::Mul => lhs * rhs,
                _ => unreachable!(),
            }),
        )),
        _ => Err(type_mismatch(lhs, rhs)),
    }
}

fn eval_comparison(op: JitBinaryOp, lhs: Scalar, rhs: Scalar) -> JitResult<Scalar> {
    let value = match (lhs, rhs) {
        (Scalar::Bool(lhs), Scalar::Bool(rhs))
            if matches!(op, JitBinaryOp::Eq | JitBinaryOp::NotEq) =>
        {
            option_zip(lhs, rhs).map(|(lhs, rhs)| compare_bool(op, lhs, rhs))
        }
        (Scalar::Int32(lhs), Scalar::Int32(rhs)) => {
            option_zip(lhs, rhs).map(|(lhs, rhs)| compare_ord(op, lhs, rhs))
        }
        (Scalar::Int64(lhs), Scalar::Int64(rhs)) => {
            option_zip(lhs, rhs).map(|(lhs, rhs)| compare_ord(op, lhs, rhs))
        }
        (Scalar::Float64(lhs), Scalar::Float64(rhs)) => {
            option_zip(lhs, rhs).map(|(lhs, rhs)| compare_ord(op, lhs, rhs))
        }
        _ => return Err(type_mismatch(lhs, rhs)),
    };
    Ok(Scalar::Bool(value))
}

fn eval_boolean(op: JitBinaryOp, lhs: Scalar, rhs: Scalar) -> JitResult<Scalar> {
    let (Scalar::Bool(lhs), Scalar::Bool(rhs)) = (lhs, rhs) else {
        return Err(type_mismatch(lhs, rhs));
    };
    let value = match op {
        JitBinaryOp::And => match (lhs, rhs) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        },
        JitBinaryOp::Or => match (lhs, rhs) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        },
        _ => unreachable!(),
    };
    Ok(Scalar::Bool(value))
}

fn ensure_supported_expr(expr: &JitExpr) -> JitResult<()> {
    match expr {
        JitExpr::Column { .. } | JitExpr::Literal(_) => Ok(()),
        JitExpr::IsNull(arg) => ensure_supported_expr(arg),
        JitExpr::Binary {
            op, left, right, ..
        } => {
            if matches!(op, JitBinaryOp::Div) {
                return Err(JitError::UnsupportedExpr(
                    "division is not yet supported by the fixed-width kernel".to_string(),
                ));
            }
            ensure_supported_expr(left)?;
            ensure_supported_expr(right)
        }
    }
}

fn arrow_type(ty: JitType) -> ArrowDataType {
    match ty {
        JitType::Bool => ArrowDataType::Boolean,
        JitType::Int32 => ArrowDataType::Int32,
        JitType::Int64 => ArrowDataType::Int64,
        JitType::Float64 => ArrowDataType::Float64,
    }
}

fn compare_bool(op: JitBinaryOp, lhs: bool, rhs: bool) -> bool {
    match op {
        JitBinaryOp::Eq => lhs == rhs,
        JitBinaryOp::NotEq => lhs != rhs,
        _ => unreachable!(),
    }
}

fn compare_ord<T: PartialOrd + PartialEq>(op: JitBinaryOp, lhs: T, rhs: T) -> bool {
    match op {
        JitBinaryOp::Eq => lhs == rhs,
        JitBinaryOp::NotEq => lhs != rhs,
        JitBinaryOp::Lt => lhs < rhs,
        JitBinaryOp::LtEq => lhs <= rhs,
        JitBinaryOp::Gt => lhs > rhs,
        JitBinaryOp::GtEq => lhs >= rhs,
        _ => unreachable!(),
    }
}

fn is_null(value: Scalar) -> bool {
    match value {
        Scalar::Bool(value) => value.is_none(),
        Scalar::Int32(value) => value.is_none(),
        Scalar::Int64(value) => value.is_none(),
        Scalar::Float64(value) => value.is_none(),
    }
}

fn option_zip<T, U>(lhs: Option<T>, rhs: Option<U>) -> Option<(T, U)> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some((lhs, rhs)),
        _ => None,
    }
}

fn type_mismatch(lhs: Scalar, rhs: Scalar) -> JitError {
    JitError::UnsupportedExpr(format!("type mismatch: {:?} vs {:?}", lhs.ty(), rhs.ty()))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{BooleanArray, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

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
}
