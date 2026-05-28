#[cfg(not(feature = "jit-mlir"))]
use crate::JitProjection;
use crate::{
    JitError, JitExpr, JitResult, PipelineSpec, QuillDialectModule, QuillDialectOp,
    QuillDialectSink, QuillDialectSource,
};

use super::{emit, MlirModule};

pub(super) fn lower_quill_dialect(module: &QuillDialectModule) -> JitResult<MlirModule> {
    verify_formal_quill_module(module)?;
    match module.pipeline_spec() {
        Some(PipelineSpec::I64FilterProject { .. }) => lower_filter_project(module),
        Some(PipelineSpec::F64FilterSum { .. }) => {
            let (predicate, measure) = filter_sum_exprs(module)?;
            emit::lower_f64_filter_sum_with_symbol(
                module.symbol.clone(),
                predicate,
                measure,
                Some("quill_dialect"),
            )
        }
        Some(PipelineSpec::DecimalFilterSum { .. }) => lower_with_quill_pass(module),
        Some(spec) => Err(JitError::UnsupportedExpr(format!(
            "quill dialect lowering does not yet support {}",
            spec.name()
        ))),
        None => Err(JitError::UnsupportedExpr(
            "quill dialect lowering requires a supported pipeline spec".to_string(),
        )),
    }
}

#[cfg(feature = "jit-mlir")]
fn lower_with_quill_pass(module: &QuillDialectModule) -> JitResult<MlirModule> {
    use melior::{ir::Module, pass, utility};

    let context = super::verify::mlir_context();
    let mut parsed = Module::parse(&context, &module.to_mlir_text()?).ok_or_else(|| {
        JitError::Backend("MLIR parser rejected Quill dialect module".to_string())
    })?;
    let pass_manager = pass::PassManager::new(&context);
    utility::parse_pass_pipeline(
        pass_manager.as_operation_pass_manager(),
        "builtin.module(convert-quill-to-loops)",
    )
    .map_err(|err| JitError::Backend(format!("Quill pass pipeline parse failed: {err:?}")))?;
    pass_manager
        .run(&mut parsed)
        .map_err(|err| JitError::Backend(format!("Quill to loops lowering failed: {err:?}")))?;
    let text = parsed.as_operation().to_string();
    if text.contains("quill.") {
        return Err(JitError::Backend(
            "Quill to loops lowering left Quill dialect operations in the module".to_string(),
        ));
    }
    Ok(MlirModule {
        symbol: module.symbol.clone(),
        text,
    })
}

#[cfg(not(feature = "jit-mlir"))]
fn lower_with_quill_pass(module: &QuillDialectModule) -> JitResult<MlirModule> {
    match module.pipeline_spec() {
        Some(PipelineSpec::DecimalFilterSum { .. }) => {
            let (predicate, measure) = filter_sum_exprs(module)?;
            emit::lower_decimal_filter_sum_with_symbol(
                module.symbol.clone(),
                predicate,
                measure,
                Some("quill_dialect"),
            )
        }
        _ => Err(JitError::UnsupportedExpr(
            "Quill pass fallback only supports decimal plain_sum".to_string(),
        )),
    }
}

#[cfg(feature = "jit-mlir")]
fn lower_filter_project(module: &QuillDialectModule) -> JitResult<MlirModule> {
    lower_with_quill_pass(module)
}

#[cfg(not(feature = "jit-mlir"))]
fn lower_filter_project(module: &QuillDialectModule) -> JitResult<MlirModule> {
    let (predicate, projections) = filter_project_exprs(module)?;
    emit::lower_i64_filter_project_with_symbol(
        module.symbol.clone(),
        predicate,
        projections,
        Some("quill_dialect"),
    )
}

#[cfg(feature = "jit-mlir")]
fn verify_formal_quill_module(module: &QuillDialectModule) -> JitResult<()> {
    super::verify::verify_module(&MlirModule {
        symbol: module.symbol.clone(),
        text: module.to_mlir_text()?,
    })
}

#[cfg(not(feature = "jit-mlir"))]
fn verify_formal_quill_module(_module: &QuillDialectModule) -> JitResult<()> {
    Ok(())
}

#[cfg(not(feature = "jit-mlir"))]
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
