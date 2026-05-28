use datafusion::arrow::array::{Array, Date32Array, Decimal128Array, Float64Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;

use crate::{JitBinaryOp, JitError, JitExpr, JitResult, JitScalar, JitType};

use super::eval::{ensure_supported_expr, eval_expr};
use super::value::Scalar;
use super::BatchView;

#[derive(Debug, Clone)]
pub struct FilterSumKernel {
    predicate: JitExpr,
    measure: JitExpr,
    plan: Option<FilterSumPlan>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FilterSumValue {
    Float64(Option<f64>),
    Decimal128 { value: Option<i128>, scale: i8 },
}

#[derive(Debug, Clone)]
pub(crate) enum FilterSumPlan {
    I64CompareF64Mul {
        predicate_col: usize,
        op: JitBinaryOp,
        threshold: i64,
        left_col: usize,
        right_col: usize,
    },
    FixedCompareDecimalMul {
        predicates: Vec<FixedPredicate>,
        left_col: usize,
        right_col: usize,
        scale: i8,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum FixedPredicate {
    Date32 {
        col: usize,
        op: JitBinaryOp,
        value: i32,
    },
    Decimal128 {
        col: usize,
        op: JitBinaryOp,
        value: i128,
        scale: i8,
    },
    Int64 {
        col: usize,
        op: JitBinaryOp,
        value: i64,
    },
}

impl FilterSumKernel {
    pub fn try_new(predicate: JitExpr, measure: JitExpr) -> JitResult<Self> {
        if predicate.ty() != JitType::Bool {
            return Err(JitError::UnsupportedExpr(format!(
                "filter predicate must be bool, got {:?}",
                predicate.ty()
            )));
        }
        if !matches!(measure.ty(), JitType::Float64 | JitType::Decimal128 { .. }) {
            return Err(JitError::UnsupportedExpr(format!(
                "sum measure must be f64 or decimal128, got {:?}",
                measure.ty()
            )));
        }
        ensure_supported_expr(&predicate)?;
        ensure_supported_expr(&measure)?;
        let plan = compile_filter_sum_plan(&predicate, &measure);
        Ok(Self {
            predicate,
            measure,
            plan,
        })
    }

    pub fn predicate(&self) -> &JitExpr {
        &self.predicate
    }

    pub fn measure(&self) -> &JitExpr {
        &self.measure
    }

    #[cfg(feature = "jit-mlir")]
    pub(crate) fn plan(&self) -> Option<&FilterSumPlan> {
        self.plan.as_ref()
    }

    pub fn execute(&self, batch: &RecordBatch) -> JitResult<FilterSumValue> {
        if let Some(plan) = &self.plan {
            return execute_filter_sum_plan(plan, batch);
        }

        let view = BatchView::try_new(batch)?;
        let mut sum = FilterSumValue::null(self.measure.ty())?;

        for row in 0..batch.num_rows() {
            if !eval_expr(&self.predicate, &view, row)?.is_filter_true()? {
                continue;
            }
            sum.add_scalar(eval_expr(&self.measure, &view, row)?)?;
        }

        Ok(sum)
    }
}

impl FilterSumValue {
    pub fn null(ty: JitType) -> JitResult<Self> {
        match ty {
            JitType::Float64 => Ok(Self::Float64(None)),
            JitType::Decimal128 { scale, .. } => Ok(Self::Decimal128 { value: None, scale }),
            other => Err(JitError::UnsupportedExpr(format!(
                "sum value must be f64 or decimal128, got {other:?}"
            ))),
        }
    }

    pub fn merge(&mut self, other: Self) -> JitResult<()> {
        match (self, other) {
            (Self::Float64(lhs), Self::Float64(rhs)) => {
                if let Some(rhs) = rhs {
                    *lhs = Some(lhs.unwrap_or(0.0) + rhs);
                }
                Ok(())
            }
            (
                Self::Decimal128 {
                    value: lhs,
                    scale: lhs_scale,
                },
                Self::Decimal128 {
                    value: rhs,
                    scale: rhs_scale,
                },
            ) => {
                if *lhs_scale != rhs_scale {
                    return Err(JitError::UnsupportedExpr(format!(
                        "cannot merge decimal sums with scales {} and {}",
                        *lhs_scale, rhs_scale
                    )));
                }
                if let Some(rhs) = rhs {
                    *lhs = Some(lhs.unwrap_or(0) + rhs);
                }
                Ok(())
            }
            (_, rhs) => Err(JitError::UnsupportedExpr(format!(
                "cannot merge incompatible sum value {:?}",
                rhs.ty()
            ))),
        }
    }

    pub fn ty(self) -> JitType {
        match self {
            Self::Float64(_) => JitType::Float64,
            Self::Decimal128 { scale, .. } => JitType::Decimal128 {
                precision: 38,
                scale,
            },
        }
    }

    fn add_scalar(&mut self, value: Scalar) -> JitResult<()> {
        match (self, value) {
            (Self::Float64(sum), Scalar::Float64(value)) => {
                if let Some(value) = value {
                    *sum = Some(sum.unwrap_or(0.0) + value);
                }
                Ok(())
            }
            (
                Self::Decimal128 {
                    value: sum,
                    scale: sum_scale,
                },
                Scalar::Decimal128 {
                    value,
                    scale,
                    precision: _,
                },
            ) => {
                if *sum_scale != scale {
                    return Err(JitError::UnsupportedExpr(format!(
                        "decimal sum requires scale {}, got {}",
                        *sum_scale, scale
                    )));
                }
                if let Some(value) = value {
                    *sum = Some(sum.unwrap_or(0) + value);
                }
                Ok(())
            }
            (_, other) => Err(JitError::Backend(format!(
                "sum measure produced unsupported value {:?}",
                other.ty()
            ))),
        }
    }
}

fn compile_filter_sum_plan(predicate: &JitExpr, measure: &JitExpr) -> Option<FilterSumPlan> {
    if let (Some((predicate_col, op, threshold)), Some((left_col, right_col))) =
        (parse_i64_compare(predicate), parse_f64_mul(measure))
    {
        return Some(FilterSumPlan::I64CompareF64Mul {
            predicate_col,
            op,
            threshold,
            left_col,
            right_col,
        });
    }

    let predicates = parse_fixed_predicates(predicate)?;
    let (left_col, right_col, scale) = parse_decimal_mul(measure)?;
    Some(FilterSumPlan::FixedCompareDecimalMul {
        predicates,
        left_col,
        right_col,
        scale,
    })
}

fn parse_i64_compare(expr: &JitExpr) -> Option<(usize, JitBinaryOp, i64)> {
    let JitExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    if !is_compare_op(*op) {
        return None;
    }

    if let Some((column, threshold)) = parse_i64_column_literal(left, right) {
        return Some((column, *op, threshold));
    }
    if let Some((column, threshold)) = parse_i64_column_literal(right, left) {
        return Some((column, reverse_compare_op(*op), threshold));
    }
    None
}

fn parse_i64_column_literal(column: &JitExpr, literal: &JitExpr) -> Option<(usize, i64)> {
    let JitExpr::Column {
        index,
        ty: JitType::Int64,
        ..
    } = column
    else {
        return None;
    };
    let JitExpr::Literal(JitScalar::Int64(value)) = literal else {
        return None;
    };
    Some((*index, *value))
}

fn parse_fixed_predicates(expr: &JitExpr) -> Option<Vec<FixedPredicate>> {
    let mut predicates = Vec::new();
    collect_fixed_predicates(expr, &mut predicates)?;
    Some(predicates)
}

fn collect_fixed_predicates(expr: &JitExpr, predicates: &mut Vec<FixedPredicate>) -> Option<()> {
    if let JitExpr::Binary {
        op: JitBinaryOp::And,
        left,
        right,
        ..
    } = expr
    {
        collect_fixed_predicates(left, predicates)?;
        collect_fixed_predicates(right, predicates)?;
        return Some(());
    }

    predicates.push(parse_fixed_predicate(expr)?);
    Some(())
}

fn parse_fixed_predicate(expr: &JitExpr) -> Option<FixedPredicate> {
    let JitExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    if !is_compare_op(*op) {
        return None;
    }

    if let Some(predicate) = parse_fixed_column_literal(left, *op, right) {
        return Some(predicate);
    }
    parse_fixed_column_literal(right, reverse_compare_op(*op), left)
}

fn parse_fixed_column_literal(
    column: &JitExpr,
    op: JitBinaryOp,
    literal: &JitExpr,
) -> Option<FixedPredicate> {
    match (column, literal) {
        (
            JitExpr::Column {
                index,
                ty: JitType::Date32,
                ..
            },
            JitExpr::Literal(JitScalar::Date32(value)),
        ) => Some(FixedPredicate::Date32 {
            col: *index,
            op,
            value: *value,
        }),
        (
            JitExpr::Column {
                index,
                ty: JitType::Decimal128 { scale, .. },
                ..
            },
            JitExpr::Literal(JitScalar::Decimal128 {
                value,
                scale: literal_scale,
                ..
            }),
        ) if scale == literal_scale => Some(FixedPredicate::Decimal128 {
            col: *index,
            op,
            value: *value,
            scale: *scale,
        }),
        (
            JitExpr::Column {
                index,
                ty: JitType::Int64,
                ..
            },
            JitExpr::Literal(JitScalar::Int64(value)),
        ) => Some(FixedPredicate::Int64 {
            col: *index,
            op,
            value: *value,
        }),
        _ => None,
    }
}

fn parse_f64_mul(expr: &JitExpr) -> Option<(usize, usize)> {
    let JitExpr::Binary {
        op: JitBinaryOp::Mul,
        left,
        right,
        ..
    } = expr
    else {
        return None;
    };
    Some((parse_f64_column(left)?, parse_f64_column(right)?))
}

fn parse_f64_column(expr: &JitExpr) -> Option<usize> {
    let JitExpr::Column {
        index,
        ty: JitType::Float64,
        ..
    } = expr
    else {
        return None;
    };
    Some(*index)
}

fn parse_decimal_mul(expr: &JitExpr) -> Option<(usize, usize, i8)> {
    let JitExpr::Binary {
        op: JitBinaryOp::Mul,
        left,
        right,
        ty: JitType::Decimal128 { scale, .. },
        ..
    } = expr
    else {
        return None;
    };
    Some((
        parse_decimal_column(left)?,
        parse_decimal_column(right)?,
        *scale,
    ))
}

fn parse_decimal_column(expr: &JitExpr) -> Option<usize> {
    let JitExpr::Column {
        index,
        ty: JitType::Decimal128 { .. },
        ..
    } = expr
    else {
        return None;
    };
    Some(*index)
}

fn execute_filter_sum_plan(plan: &FilterSumPlan, batch: &RecordBatch) -> JitResult<FilterSumValue> {
    match plan {
        FilterSumPlan::I64CompareF64Mul {
            predicate_col,
            op,
            threshold,
            left_col,
            right_col,
        } => execute_i64_filter_f64_sum(
            batch,
            *predicate_col,
            *op,
            *threshold,
            *left_col,
            *right_col,
        ),
        FilterSumPlan::FixedCompareDecimalMul {
            predicates,
            left_col,
            right_col,
            scale,
        } => execute_fixed_filter_decimal_sum(batch, predicates, *left_col, *right_col, *scale),
    }
}

fn execute_i64_filter_f64_sum(
    batch: &RecordBatch,
    predicate_col: usize,
    op: JitBinaryOp,
    threshold: i64,
    left_col: usize,
    right_col: usize,
) -> JitResult<FilterSumValue> {
    let predicate = int64_column(batch, predicate_col)?;
    let left = float64_column(batch, left_col)?;
    let right = float64_column(batch, right_col)?;
    let mut sum = 0.0_f64;
    let mut has_value = false;

    for row in 0..batch.num_rows() {
        if !predicate.is_valid(row) || !compare_i64(op, predicate.value(row), threshold) {
            continue;
        }
        if left.is_valid(row) && right.is_valid(row) {
            sum += left.value(row) * right.value(row);
            has_value = true;
        }
    }

    Ok(FilterSumValue::Float64(has_value.then_some(sum)))
}

fn execute_fixed_filter_decimal_sum(
    batch: &RecordBatch,
    predicates: &[FixedPredicate],
    left_col: usize,
    right_col: usize,
    scale: i8,
) -> JitResult<FilterSumValue> {
    let predicates = bind_predicates(batch, predicates)?;
    let left = decimal128_column(batch, left_col)?;
    let right = decimal128_column(batch, right_col)?;
    if left.scale().saturating_add(right.scale()) != scale {
        return Err(JitError::UnsupportedExpr(format!(
            "decimal multiply scale {} + {} does not match output scale {}",
            left.scale(),
            right.scale(),
            scale
        )));
    }

    let mut sum = 0_i128;
    let mut has_value = false;
    for row in 0..batch.num_rows() {
        if !predicates.iter().all(|predicate| predicate.matches(row)) {
            continue;
        }
        if left.is_valid(row) && right.is_valid(row) {
            sum += left.value(row) * right.value(row);
            has_value = true;
        }
    }

    Ok(FilterSumValue::Decimal128 {
        value: has_value.then_some(sum),
        scale,
    })
}

enum BoundPredicate<'a> {
    Date32 {
        array: &'a Date32Array,
        op: JitBinaryOp,
        value: i32,
    },
    Decimal128 {
        array: &'a Decimal128Array,
        op: JitBinaryOp,
        value: i128,
    },
    Int64 {
        array: &'a Int64Array,
        op: JitBinaryOp,
        value: i64,
    },
}

impl BoundPredicate<'_> {
    fn matches(&self, row: usize) -> bool {
        match self {
            Self::Date32 { array, op, value } => {
                array.is_valid(row) && compare_i32(*op, array.value(row), *value)
            }
            Self::Decimal128 { array, op, value } => {
                array.is_valid(row) && compare_i128(*op, array.value(row), *value)
            }
            Self::Int64 { array, op, value } => {
                array.is_valid(row) && compare_i64(*op, array.value(row), *value)
            }
        }
    }
}

fn bind_predicates<'a>(
    batch: &'a RecordBatch,
    predicates: &[FixedPredicate],
) -> JitResult<Vec<BoundPredicate<'a>>> {
    predicates
        .iter()
        .map(|predicate| match *predicate {
            FixedPredicate::Date32 { col, op, value } => Ok(BoundPredicate::Date32 {
                array: date32_column(batch, col)?,
                op,
                value,
            }),
            FixedPredicate::Decimal128 {
                col,
                op,
                value,
                scale,
            } => {
                let array = decimal128_column(batch, col)?;
                if array.scale() != scale {
                    return Err(JitError::UnsupportedExpr(format!(
                        "decimal predicate scale {} does not match column scale {}",
                        scale,
                        array.scale()
                    )));
                }
                Ok(BoundPredicate::Decimal128 { array, op, value })
            }
            FixedPredicate::Int64 { col, op, value } => Ok(BoundPredicate::Int64 {
                array: int64_column(batch, col)?,
                op,
                value,
            }),
        })
        .collect()
}

fn date32_column(batch: &RecordBatch, index: usize) -> JitResult<&Date32Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| JitError::Backend(format!("column {index} is not Date32")))
}

fn decimal128_column(batch: &RecordBatch, index: usize) -> JitResult<&Decimal128Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| JitError::Backend(format!("column {index} is not Decimal128")))
}

fn int64_column(batch: &RecordBatch, index: usize) -> JitResult<&Int64Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| JitError::Backend(format!("column {index} is not Int64")))
}

fn float64_column(batch: &RecordBatch, index: usize) -> JitResult<&Float64Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| JitError::Backend(format!("column {index} is not Float64")))
}

fn is_compare_op(op: JitBinaryOp) -> bool {
    matches!(
        op,
        JitBinaryOp::Eq
            | JitBinaryOp::NotEq
            | JitBinaryOp::Lt
            | JitBinaryOp::LtEq
            | JitBinaryOp::Gt
            | JitBinaryOp::GtEq
    )
}

fn reverse_compare_op(op: JitBinaryOp) -> JitBinaryOp {
    match op {
        JitBinaryOp::Lt => JitBinaryOp::Gt,
        JitBinaryOp::LtEq => JitBinaryOp::GtEq,
        JitBinaryOp::Gt => JitBinaryOp::Lt,
        JitBinaryOp::GtEq => JitBinaryOp::LtEq,
        JitBinaryOp::Eq | JitBinaryOp::NotEq => op,
        _ => unreachable!("non-comparison operator cannot be reversed"),
    }
}

fn compare_i64(op: JitBinaryOp, lhs: i64, rhs: i64) -> bool {
    match op {
        JitBinaryOp::Eq => lhs == rhs,
        JitBinaryOp::NotEq => lhs != rhs,
        JitBinaryOp::Lt => lhs < rhs,
        JitBinaryOp::LtEq => lhs <= rhs,
        JitBinaryOp::Gt => lhs > rhs,
        JitBinaryOp::GtEq => lhs >= rhs,
        _ => unreachable!("non-comparison operator cannot compare integers"),
    }
}

fn compare_i32(op: JitBinaryOp, lhs: i32, rhs: i32) -> bool {
    match op {
        JitBinaryOp::Eq => lhs == rhs,
        JitBinaryOp::NotEq => lhs != rhs,
        JitBinaryOp::Lt => lhs < rhs,
        JitBinaryOp::LtEq => lhs <= rhs,
        JitBinaryOp::Gt => lhs > rhs,
        JitBinaryOp::GtEq => lhs >= rhs,
        _ => unreachable!("non-comparison operator cannot compare integers"),
    }
}

fn compare_i128(op: JitBinaryOp, lhs: i128, rhs: i128) -> bool {
    match op {
        JitBinaryOp::Eq => lhs == rhs,
        JitBinaryOp::NotEq => lhs != rhs,
        JitBinaryOp::Lt => lhs < rhs,
        JitBinaryOp::LtEq => lhs <= rhs,
        JitBinaryOp::Gt => lhs > rhs,
        JitBinaryOp::GtEq => lhs >= rhs,
        _ => unreachable!("non-comparison operator cannot compare integers"),
    }
}
