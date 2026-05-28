mod array;
mod eval;
#[cfg(test)]
mod tests;
mod value;

use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, Int64Array};
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::record_batch::RecordBatch;

use crate::jit::{JitBinaryOp, JitError, JitExpr, JitProjection, JitResult, JitScalar, JitType};

use self::array::{arrow_type, BatchView, OutputBuilder};
use self::eval::{ensure_supported_expr, eval_expr};

#[derive(Debug, Clone)]
pub struct FilterProjectKernel {
    predicate: JitExpr,
    projections: Vec<JitProjection>,
    schema: ArrowSchemaRef,
}

#[derive(Debug, Clone)]
pub struct FilterSumKernel {
    predicate: JitExpr,
    measure: JitExpr,
    plan: Option<FilterSumPlan>,
}

#[derive(Debug, Clone)]
enum FilterSumPlan {
    I64CompareF64Mul {
        predicate_col: usize,
        op: JitBinaryOp,
        threshold: i64,
        left_col: usize,
        right_col: usize,
    },
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

impl FilterSumKernel {
    pub fn try_new(predicate: JitExpr, measure: JitExpr) -> JitResult<Self> {
        if predicate.ty() != JitType::Bool {
            return Err(JitError::UnsupportedExpr(format!(
                "filter predicate must be bool, got {:?}",
                predicate.ty()
            )));
        }
        if measure.ty() != JitType::Float64 {
            return Err(JitError::UnsupportedExpr(format!(
                "sum measure must be f64, got {:?}",
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

    pub fn execute(&self, batch: &RecordBatch) -> JitResult<Option<f64>> {
        if let Some(plan) = &self.plan {
            return execute_filter_sum_plan(plan, batch);
        }

        let view = BatchView::try_new(batch)?;
        let mut sum = 0.0_f64;
        let mut has_value = false;

        for row in 0..batch.num_rows() {
            if !eval_expr(&self.predicate, &view, row)?.is_filter_true()? {
                continue;
            }
            match eval_expr(&self.measure, &view, row)? {
                self::value::Scalar::Float64(Some(value)) => {
                    sum += value;
                    has_value = true;
                }
                self::value::Scalar::Float64(None) => {}
                other => {
                    return Err(JitError::Backend(format!(
                        "sum measure produced non-f64 value {:?}",
                        other.ty()
                    )));
                }
            }
        }

        Ok(has_value.then_some(sum))
    }
}

fn compile_filter_sum_plan(predicate: &JitExpr, measure: &JitExpr) -> Option<FilterSumPlan> {
    let (predicate_col, op, threshold) = parse_i64_compare(predicate)?;
    let (left_col, right_col) = parse_f64_mul(measure)?;
    Some(FilterSumPlan::I64CompareF64Mul {
        predicate_col,
        op,
        threshold,
        left_col,
        right_col,
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

fn execute_filter_sum_plan(plan: &FilterSumPlan, batch: &RecordBatch) -> JitResult<Option<f64>> {
    let FilterSumPlan::I64CompareF64Mul {
        predicate_col,
        op,
        threshold,
        left_col,
        right_col,
    } = *plan;
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

    Ok(has_value.then_some(sum))
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
