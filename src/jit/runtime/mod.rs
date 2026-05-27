mod array;
mod eval;
#[cfg(test)]
mod tests;
mod value;

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::record_batch::RecordBatch;

use crate::jit::{JitError, JitExpr, JitProjection, JitResult, JitType};

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
        Ok(Self { predicate, measure })
    }

    pub fn predicate(&self) -> &JitExpr {
        &self.predicate
    }

    pub fn measure(&self) -> &JitExpr {
        &self.measure
    }

    pub fn execute(&self, batch: &RecordBatch) -> JitResult<Option<f64>> {
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
