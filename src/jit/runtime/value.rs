use crate::jit::{JitError, JitType};

#[derive(Clone, Copy)]
pub(super) enum Scalar {
    Bool(Option<bool>),
    Date32(Option<i32>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float64(Option<f64>),
    Decimal128 {
        value: Option<i128>,
        precision: u8,
        scale: i8,
    },
}

impl Scalar {
    pub(super) fn is_filter_true(self) -> Result<bool, JitError> {
        match self {
            Self::Bool(value) => Ok(value.unwrap_or(false)),
            other => Err(JitError::Backend(format!(
                "predicate produced non-bool value {:?}",
                other.ty()
            ))),
        }
    }

    pub(super) fn ty(self) -> JitType {
        match self {
            Self::Bool(_) => JitType::Bool,
            Self::Date32(_) => JitType::Date32,
            Self::Int32(_) => JitType::Int32,
            Self::Int64(_) => JitType::Int64,
            Self::Float64(_) => JitType::Float64,
            Self::Decimal128 {
                precision, scale, ..
            } => JitType::Decimal128 { precision, scale },
        }
    }

    pub(super) fn is_null(self) -> bool {
        match self {
            Self::Bool(value) => value.is_none(),
            Self::Date32(value) => value.is_none(),
            Self::Int32(value) => value.is_none(),
            Self::Int64(value) => value.is_none(),
            Self::Float64(value) => value.is_none(),
            Self::Decimal128 { value, .. } => value.is_none(),
        }
    }
}

pub(super) fn option_zip<T, U>(lhs: Option<T>, rhs: Option<U>) -> Option<(T, U)> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some((lhs, rhs)),
        _ => None,
    }
}

pub(super) fn type_mismatch(lhs: Scalar, rhs: Scalar) -> JitError {
    JitError::UnsupportedExpr(format!("type mismatch: {:?} vs {:?}", lhs.ty(), rhs.ty()))
}
