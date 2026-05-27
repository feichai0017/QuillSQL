use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, IsNullExpr, Literal};
use datafusion::physical_expr::PhysicalExpr;

use crate::jit::{JitError, JitResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JitType {
    Bool,
    Int32,
    Int64,
    Float64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JitScalar {
    Null(JitType),
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
}

impl JitScalar {
    pub fn ty(&self) -> JitType {
        match self {
            Self::Null(ty) => *ty,
            Self::Bool(_) => JitType::Bool,
            Self::Int32(_) => JitType::Int32,
            Self::Int64(_) => JitType::Int64,
            Self::Float64(_) => JitType::Float64,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JitBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JitExpr {
    Column {
        index: usize,
        name: String,
        ty: JitType,
        nullable: bool,
    },
    Literal(JitScalar),
    Binary {
        op: JitBinaryOp,
        left: Box<JitExpr>,
        right: Box<JitExpr>,
        ty: JitType,
        nullable: bool,
    },
    IsNull(Box<JitExpr>),
}

impl JitExpr {
    pub fn ty(&self) -> JitType {
        match self {
            Self::Column { ty, .. } => *ty,
            Self::Literal(value) => value.ty(),
            Self::Binary { ty, .. } => *ty,
            Self::IsNull(_) => JitType::Bool,
        }
    }

    pub fn nullable(&self) -> bool {
        match self {
            Self::Column { nullable, .. } => *nullable,
            Self::Literal(JitScalar::Null(_)) => true,
            Self::Literal(_) => false,
            Self::Binary { nullable, .. } => *nullable,
            Self::IsNull(_) => false,
        }
    }

    pub fn from_physical(
        expr: &Arc<dyn PhysicalExpr>,
        input_schema: &ArrowSchema,
    ) -> JitResult<Self> {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            let field = input_schema.field(column.index());
            return Ok(Self::Column {
                index: column.index(),
                name: column.name().to_string(),
                ty: jit_type(field.data_type())?,
                nullable: field.is_nullable(),
            });
        }

        if let Some(literal) = expr.as_any().downcast_ref::<Literal>() {
            return Ok(Self::Literal(jit_scalar(literal.value())?));
        }

        if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
            let left = Self::from_physical(binary.left(), input_schema)?;
            let right = Self::from_physical(binary.right(), input_schema)?;
            let output_type = expr.data_type(input_schema).map_err(|err| {
                JitError::UnsupportedExpr(format!("cannot infer binary expression type: {err}"))
            })?;
            let nullable = expr.nullable(input_schema).map_err(|err| {
                JitError::UnsupportedExpr(format!(
                    "cannot infer binary expression nullability: {err}"
                ))
            })?;
            return Ok(Self::Binary {
                op: jit_binary_op(binary.op())?,
                left: Box::new(left),
                right: Box::new(right),
                ty: jit_type(&output_type)?,
                nullable,
            });
        }

        if let Some(is_null) = expr.as_any().downcast_ref::<IsNullExpr>() {
            let arg = Self::from_physical(is_null.arg(), input_schema)?;
            return Ok(Self::IsNull(Box::new(arg)));
        }

        Err(JitError::UnsupportedExpr(expr.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct JitProjection {
    pub expr: JitExpr,
    pub alias: String,
}

impl JitProjection {
    pub fn new(expr: JitExpr, alias: impl Into<String>) -> Self {
        Self {
            expr,
            alias: alias.into(),
        }
    }
}

fn jit_type(data_type: &ArrowDataType) -> JitResult<JitType> {
    match data_type {
        ArrowDataType::Boolean => Ok(JitType::Bool),
        ArrowDataType::Int32 => Ok(JitType::Int32),
        ArrowDataType::Int64 => Ok(JitType::Int64),
        ArrowDataType::Float64 => Ok(JitType::Float64),
        other => Err(JitError::UnsupportedType(format!("{other:?}"))),
    }
}

fn jit_scalar(value: &ScalarValue) -> JitResult<JitScalar> {
    match value {
        ScalarValue::Null => Ok(JitScalar::Null(JitType::Bool)),
        ScalarValue::Boolean(Some(value)) => Ok(JitScalar::Bool(*value)),
        ScalarValue::Boolean(None) => Ok(JitScalar::Null(JitType::Bool)),
        ScalarValue::Int32(Some(value)) => Ok(JitScalar::Int32(*value)),
        ScalarValue::Int32(None) => Ok(JitScalar::Null(JitType::Int32)),
        ScalarValue::Int64(Some(value)) => Ok(JitScalar::Int64(*value)),
        ScalarValue::Int64(None) => Ok(JitScalar::Null(JitType::Int64)),
        ScalarValue::Float64(Some(value)) => Ok(JitScalar::Float64(*value)),
        ScalarValue::Float64(None) => Ok(JitScalar::Null(JitType::Float64)),
        other => Err(JitError::UnsupportedType(format!("{other:?}"))),
    }
}

fn jit_binary_op(op: &Operator) -> JitResult<JitBinaryOp> {
    match op {
        Operator::Plus => Ok(JitBinaryOp::Add),
        Operator::Minus => Ok(JitBinaryOp::Sub),
        Operator::Multiply => Ok(JitBinaryOp::Mul),
        Operator::Divide => Ok(JitBinaryOp::Div),
        Operator::Eq => Ok(JitBinaryOp::Eq),
        Operator::NotEq => Ok(JitBinaryOp::NotEq),
        Operator::Lt => Ok(JitBinaryOp::Lt),
        Operator::LtEq => Ok(JitBinaryOp::LtEq),
        Operator::Gt => Ok(JitBinaryOp::Gt),
        Operator::GtEq => Ok(JitBinaryOp::GtEq),
        Operator::And => Ok(JitBinaryOp::And),
        Operator::Or => Ok(JitBinaryOp::Or),
        other => Err(JitError::UnsupportedExpr(format!(
            "operator {other:?} is not supported"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};

    use super::{JitBinaryOp, JitExpr, JitScalar, JitType};

    #[test]
    fn lowers_simple_filter_expr() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "a",
            ArrowDataType::Int64,
            true,
        )]));
        let expr = binary(
            col("a", &schema).unwrap(),
            Operator::Gt,
            lit(ScalarValue::Int64(Some(10))),
            &schema,
        )
        .unwrap();

        let lowered = JitExpr::from_physical(&expr, &schema).unwrap();
        let JitExpr::Binary {
            op,
            left,
            right,
            ty,
            nullable,
        } = lowered
        else {
            panic!("expected binary expression");
        };

        assert_eq!(op, JitBinaryOp::Gt);
        assert_eq!(ty, JitType::Bool);
        assert!(nullable);
        assert!(matches!(
            *left,
            JitExpr::Column {
                index: 0,
                ty: JitType::Int64,
                nullable: true,
                ..
            }
        ));
        assert_eq!(*right, JitExpr::Literal(JitScalar::Int64(10)));
    }
}
