use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::record_batch::RecordBatch;

use crate::{JitError, JitExpr, JitProjection, JitResult, JitType, KernelSpec};

use super::array::{arrow_type, BatchView, OutputBuilder};
use super::eval::{ensure_supported_expr, eval_expr};

#[derive(Debug, Clone)]
pub struct FilterProjectKernel {
    predicate: JitExpr,
    projections: Vec<JitProjection>,
    schema: ArrowSchemaRef,
    spec: Option<KernelSpec>,
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

        let spec = KernelSpec::i64_filter_project(&predicate, &projections);
        Ok(Self {
            predicate,
            projections,
            schema,
            spec,
        })
    }

    pub fn predicate(&self) -> &JitExpr {
        &self.predicate
    }

    pub fn projections(&self) -> &[JitProjection] {
        &self.projections
    }

    pub fn spec(&self) -> Option<&KernelSpec> {
        self.spec.as_ref()
    }

    #[cfg(feature = "jit-mlir")]
    pub(crate) fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
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
