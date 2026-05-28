use crate::{
    JitError, JitExpr, JitProjection, JitResult, PipelineSpec, QuillDialectModule, QuillDialectOp,
    QuillDialectSink, QuillDialectSource,
};

use super::{emit, MlirModule};

pub(super) fn lower_quill_dialect(module: &QuillDialectModule) -> JitResult<MlirModule> {
    match module.pipeline_spec() {
        Some(PipelineSpec::I64FilterProject { .. }) => {
            let (predicate, projections) = filter_project_exprs(module)?;
            emit::lower_i64_filter_project_with_symbol(
                module.symbol.clone(),
                predicate,
                projections,
                Some("quill_dialect"),
            )
        }
        Some(PipelineSpec::F64FilterSum { .. }) => {
            let (predicate, measure) = filter_sum_exprs(module)?;
            emit::lower_f64_filter_sum_with_symbol(
                module.symbol.clone(),
                predicate,
                measure,
                Some("quill_dialect"),
            )
        }
        Some(PipelineSpec::DecimalFilterSum { .. }) => {
            let (predicate, measure) = filter_sum_exprs(module)?;
            emit::lower_decimal_filter_sum_with_symbol(
                module.symbol.clone(),
                predicate,
                measure,
                Some("quill_dialect"),
            )
        }
        Some(spec) => Err(JitError::UnsupportedExpr(format!(
            "quill dialect lowering does not yet support {}",
            spec.name()
        ))),
        None => Err(JitError::UnsupportedExpr(
            "quill dialect lowering requires a supported pipeline spec".to_string(),
        )),
    }
}

fn filter_project_exprs(module: &QuillDialectModule) -> JitResult<(&JitExpr, &[JitProjection])> {
    match (&module.source, module.ops.as_slice(), &module.sink) {
        (
            QuillDialectSource::DataFusionBatch,
            [QuillDialectOp::Filter { predicate }, QuillDialectOp::Project { projections }],
            QuillDialectSink::RecordBatch,
        ) => Ok((predicate, projections)),
        _ => Err(JitError::UnsupportedExpr(
            "quill dialect lowering currently supports filter -> project".to_string(),
        )),
    }
}

fn filter_sum_exprs(module: &QuillDialectModule) -> JitResult<(&JitExpr, &JitExpr)> {
    match (&module.source, module.ops.as_slice(), &module.sink) {
        (
            QuillDialectSource::DataFusionBatch,
            [QuillDialectOp::Filter { predicate }],
            QuillDialectSink::PlainSum { measure },
        ) => Ok((predicate, measure)),
        _ => Err(JitError::UnsupportedExpr(
            "quill dialect lowering currently supports filter -> plain_sum".to_string(),
        )),
    }
}
