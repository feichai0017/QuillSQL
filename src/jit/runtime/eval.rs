use crate::jit::{JitBinaryOp, JitError, JitExpr, JitResult, JitScalar, JitType};

use super::array::BatchView;
use super::value::{option_zip, type_mismatch, Scalar};

pub(super) fn eval_expr(expr: &JitExpr, view: &BatchView<'_>, row: usize) -> JitResult<Scalar> {
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
        JitExpr::IsNull(arg) => Ok(Scalar::Bool(Some(eval_expr(arg, view, row)?.is_null()))),
    }
}

pub(super) fn ensure_supported_expr(expr: &JitExpr) -> JitResult<()> {
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
