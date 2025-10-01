use crate::catalog::Schema;
use crate::catalog::{Column, DataType};
use crate::error::QuillSQLError;
use crate::error::QuillSQLResult;
use crate::expression::{Expr, ExprTrait};
use crate::storage::tuple::Tuple;
use crate::utils::scalar::ScalarValue;
use std::cmp::Ordering;

fn numeric_binary_op<F>(left: ScalarValue, right: ScalarValue, op: F) -> QuillSQLResult<ScalarValue>
where
    F: Fn(ScalarValue, ScalarValue) -> QuillSQLResult<ScalarValue>,
{
    let coercion_type =
        DataType::comparison_numeric_coercion(&left.data_type(), &right.data_type())?;
    let l_cast = left.cast_to(&coercion_type)?;
    let r_cast = right.cast_to(&coercion_type)?;
    op(l_cast, r_cast)
}

/// Binary expression
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BinaryExpr {
    /// Left-hand side of the expression
    pub left: Box<Expr>,
    /// The comparison operator
    pub op: BinaryOp,
    /// Right-hand side of the expression
    pub right: Box<Expr>,
}

impl ExprTrait for BinaryExpr {
    fn data_type(&self, input_schema: &Schema) -> QuillSQLResult<DataType> {
        let left_type = self.left.data_type(input_schema)?;
        let right_type = self.right.data_type(input_schema)?;
        match self.op {
            BinaryOp::Gt
            | BinaryOp::Lt
            | BinaryOp::GtEq
            | BinaryOp::LtEq
            | BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::And
            | BinaryOp::Or => Ok(DataType::Boolean),
            BinaryOp::Plus | BinaryOp::Minus | BinaryOp::Multiply | BinaryOp::Divide => {
                DataType::comparison_numeric_coercion(&left_type, &right_type)
            }
        }
    }

    fn nullable(&self, input_schema: &Schema) -> QuillSQLResult<bool> {
        Ok(self.left.nullable(input_schema)? || self.right.nullable(input_schema)?)
    }

    fn evaluate(&self, tuple: &Tuple) -> QuillSQLResult<ScalarValue> {
        let l = self.left.evaluate(tuple)?;
        let r = self.right.evaluate(tuple)?;
        match self.op {
            BinaryOp::Gt => evaluate_comparison(l, r, &[Ordering::Greater]),
            BinaryOp::Lt => evaluate_comparison(l, r, &[Ordering::Less]),
            BinaryOp::GtEq => evaluate_comparison(l, r, &[Ordering::Greater, Ordering::Equal]),
            BinaryOp::LtEq => evaluate_comparison(l, r, &[Ordering::Less, Ordering::Equal]),
            BinaryOp::Eq => evaluate_comparison(l, r, &[Ordering::Equal]),
            BinaryOp::NotEq => evaluate_comparison(l, r, &[Ordering::Greater, Ordering::Less]),
            BinaryOp::And => {
                let l_bool = l.as_boolean()?;
                let r_bool = r.as_boolean()?;
                Ok(ScalarValue::Boolean(Some(
                    l_bool.unwrap_or(false) && r_bool.unwrap_or(false),
                )))
            }
            BinaryOp::Or => {
                let l_bool = l.as_boolean()?;
                let r_bool = r.as_boolean()?;
                Ok(ScalarValue::Boolean(Some(
                    l_bool.unwrap_or(false) || r_bool.unwrap_or(false),
                )))
            }
            BinaryOp::Plus => numeric_binary_op(l, r, |a, b| a.wrapping_add(b)),
            BinaryOp::Minus => numeric_binary_op(l, r, |a, b| a.wrapping_sub(b)),
            BinaryOp::Multiply => numeric_binary_op(l, r, |a, b| a.wrapping_mul(b)),
            BinaryOp::Divide => numeric_binary_op(l, r, |a, b| a.wrapping_div(b)),
        }
    }

    fn to_column(&self, input_schema: &Schema) -> QuillSQLResult<Column> {
        Ok(Column::new(
            format!("{self}"),
            self.data_type(input_schema)?,
            self.nullable(input_schema)?,
        ))
    }
}

fn evaluate_comparison(
    left: ScalarValue,
    right: ScalarValue,
    accepted_orderings: &[Ordering],
) -> QuillSQLResult<ScalarValue> {
    let coercion_type =
        DataType::comparison_numeric_coercion(&left.data_type(), &right.data_type())?;
    let order = left
        .cast_to(&coercion_type)?
        .partial_cmp(&right.cast_to(&coercion_type)?)
        .ok_or(QuillSQLError::Execution(format!(
            "Can not compare {:?} and {:?}",
            left, right
        )))?;
    Ok(ScalarValue::Boolean(Some(
        accepted_orderings.contains(&order),
    )))
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum BinaryOp {
    Plus,
    Minus,
    Multiply,
    Divide,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Eq,
    NotEq,
    And,
    Or,
}

impl TryFrom<&sqlparser::ast::BinaryOperator> for BinaryOp {
    type Error = QuillSQLError;

    fn try_from(value: &sqlparser::ast::BinaryOperator) -> Result<Self, Self::Error> {
        match value {
            sqlparser::ast::BinaryOperator::Plus => Ok(BinaryOp::Plus),
            sqlparser::ast::BinaryOperator::Minus => Ok(BinaryOp::Minus),
            sqlparser::ast::BinaryOperator::Multiply => Ok(BinaryOp::Multiply),
            sqlparser::ast::BinaryOperator::Divide => Ok(BinaryOp::Divide),
            sqlparser::ast::BinaryOperator::Gt => Ok(BinaryOp::Gt),
            sqlparser::ast::BinaryOperator::Lt => Ok(BinaryOp::Lt),
            sqlparser::ast::BinaryOperator::GtEq => Ok(BinaryOp::GtEq),
            sqlparser::ast::BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
            sqlparser::ast::BinaryOperator::Eq => Ok(BinaryOp::Eq),
            sqlparser::ast::BinaryOperator::NotEq => Ok(BinaryOp::NotEq),
            sqlparser::ast::BinaryOperator::And => Ok(BinaryOp::And),
            sqlparser::ast::BinaryOperator::Or => Ok(BinaryOp::Or),
            _ => Err(QuillSQLError::NotSupport(format!(
                "sqlparser binary operator {} not supported",
                value
            ))),
        }
    }
}

impl std::fmt::Display for BinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} {} {})", self.left, self.op, self.right)
    }
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
