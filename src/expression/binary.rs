use crate::catalog::Schema;
use crate::catalog::{Column, DataType};
use crate::utils::scalar::ScalarValue;
use crate::error::QuillSQLResult;
use crate::expression::{Expr, ExprTrait};
use crate::storage::tuple::Tuple;
use crate::error::QuillSQLError;
use std::cmp::Ordering;

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
        let _left_type = self.left.data_type(input_schema)?;
        let _right_type = self.right.data_type(input_schema)?;
        match self.op {
            BinaryOp::Gt
            | BinaryOp::Lt
            | BinaryOp::GtEq
            | BinaryOp::LtEq
            | BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::And
            | BinaryOp::Or => Ok(DataType::Boolean),
            BinaryOp::Plus => todo!(),
            BinaryOp::Minus => todo!(),
            BinaryOp::Multiply => todo!(),
            BinaryOp::Divide => todo!(),
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
                match (l_bool, r_bool) {
                    (Some(v1), Some(v2)) => Ok((v1 && v2).into()),
                    (Some(_), None) | (None, Some(_)) | (None, None) => {
                        Ok(ScalarValue::Boolean(Some(false)))
                    }
                }
            }
            _ => Err(QuillSQLError::NotSupport(format!(
                "binary operator {:?} not support evaluating yet",
                self.op
            ))),
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
