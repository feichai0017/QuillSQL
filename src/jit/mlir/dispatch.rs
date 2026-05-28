use std::cell::RefCell;
use std::collections::HashMap;

use datafusion::arrow::array::{Array, Date32Array, Decimal128Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};

use crate::jit::{
    CompiledDecimalFilterSum, CompiledKernel, DecimalFilterSumInput, FilterSumKernel,
    FilterSumValue, MlirBackend,
};

use super::super::runtime::{FilterSumPlan, FixedPredicate};

thread_local! {
    static DECIMAL_FILTER_SUM_CACHE: RefCell<HashMap<String, CompiledDecimalFilterSum>> =
        RefCell::new(HashMap::new());
}

pub(crate) fn execute_filter_sum(
    kernel: &CompiledKernel,
    runtime: &FilterSumKernel,
    batch: &RecordBatch,
) -> Result<Option<FilterSumValue>> {
    if !kernel.executable || kernel.backend != "mlir" {
        return Ok(None);
    }

    let Some(FilterSumPlan::FixedCompareDecimalMul {
        predicates,
        left_col,
        right_col,
        scale,
    }) = runtime.plan()
    else {
        return Ok(None);
    };

    let Some(inputs) = decimal_filter_sum_inputs(batch, predicates, *left_col, *right_col, *scale)?
    else {
        return Ok(None);
    };

    let cache_key = filter_sum_cache_key(runtime);
    let output = DECIMAL_FILTER_SUM_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if !cache.contains_key(&cache_key) {
            let compiled = MlirBackend::new()
                .compile_decimal_filter_sum(runtime.predicate(), runtime.measure())?;
            cache.insert(cache_key.clone(), compiled);
        }
        cache
            .get(&cache_key)
            .expect("compiled kernel was inserted")
            .invoke(&inputs)
    })?;

    Ok(Some(FilterSumValue::Decimal128 {
        value: (output.count > 0).then_some(output.sum),
        scale: *scale,
    }))
}

fn filter_sum_cache_key(runtime: &FilterSumKernel) -> String {
    format!("{:?}|{:?}", runtime.predicate(), runtime.measure())
}

fn decimal_filter_sum_inputs<'a>(
    batch: &'a RecordBatch,
    predicates: &[FixedPredicate],
    left_col: usize,
    right_col: usize,
    scale: i8,
) -> Result<Option<Vec<DecimalFilterSumInput<'a>>>> {
    let mut inputs = Vec::new();
    for predicate in predicates {
        if push_predicate_input(&mut inputs, batch, predicate)?.is_none() {
            return Ok(None);
        }
    }
    if push_decimal_input(&mut inputs, batch, left_col, None)?.is_none()
        || push_decimal_input(&mut inputs, batch, right_col, None)?.is_none()
    {
        return Ok(None);
    }

    let left = decimal128_column(batch, left_col)?;
    let right = decimal128_column(batch, right_col)?;
    if left.scale().saturating_add(right.scale()) != scale {
        return Err(DataFusionError::Execution(format!(
            "decimal multiply scale {} + {} does not match output scale {}",
            left.scale(),
            right.scale(),
            scale
        )));
    }
    Ok(Some(inputs))
}

fn push_predicate_input<'a>(
    inputs: &mut Vec<DecimalFilterSumInput<'a>>,
    batch: &'a RecordBatch,
    predicate: &FixedPredicate,
) -> Result<Option<()>> {
    match *predicate {
        FixedPredicate::Date32 { col, .. } => push_date32_input(inputs, batch, col),
        FixedPredicate::Decimal128 { col, scale, .. } => {
            push_decimal_input(inputs, batch, col, Some(scale))
        }
        FixedPredicate::Int64 { col, .. } => push_int64_input(inputs, batch, col),
    }
}

fn push_date32_input<'a>(
    inputs: &mut Vec<DecimalFilterSumInput<'a>>,
    batch: &'a RecordBatch,
    index: usize,
) -> Result<Option<()>> {
    if has_input(inputs, index) {
        return Ok(Some(()));
    }
    let array = date32_column(batch, index)?;
    let Some(values) = date32_values(array) else {
        return Ok(None);
    };
    inputs.push(DecimalFilterSumInput::Date32 { index, values });
    Ok(Some(()))
}

fn push_int64_input<'a>(
    inputs: &mut Vec<DecimalFilterSumInput<'a>>,
    batch: &'a RecordBatch,
    index: usize,
) -> Result<Option<()>> {
    if has_input(inputs, index) {
        return Ok(Some(()));
    }
    let array = int64_column(batch, index)?;
    let Some(values) = int64_values(array) else {
        return Ok(None);
    };
    inputs.push(DecimalFilterSumInput::Int64 { index, values });
    Ok(Some(()))
}

fn push_decimal_input<'a>(
    inputs: &mut Vec<DecimalFilterSumInput<'a>>,
    batch: &'a RecordBatch,
    index: usize,
    expected_scale: Option<i8>,
) -> Result<Option<()>> {
    if has_input(inputs, index) {
        return Ok(Some(()));
    }
    let array = decimal128_column(batch, index)?;
    if let Some(expected_scale) = expected_scale {
        if array.scale() != expected_scale {
            return Err(DataFusionError::Execution(format!(
                "decimal predicate scale {} does not match column scale {}",
                expected_scale,
                array.scale()
            )));
        }
    }
    let Some(values) = decimal128_values(array) else {
        return Ok(None);
    };
    inputs.push(DecimalFilterSumInput::Decimal128 { index, values });
    Ok(Some(()))
}

fn has_input(inputs: &[DecimalFilterSumInput<'_>], index: usize) -> bool {
    inputs.iter().any(|input| match input {
        DecimalFilterSumInput::Date32 {
            index: input_index, ..
        }
        | DecimalFilterSumInput::Int64 {
            index: input_index, ..
        }
        | DecimalFilterSumInput::Decimal128 {
            index: input_index, ..
        } => *input_index == index,
    })
}

fn date32_values(array: &Date32Array) -> Option<&[i32]> {
    if array.null_count() != 0 || array.offset() != 0 {
        return None;
    }
    Some(array.values().as_ref())
}

fn int64_values(array: &Int64Array) -> Option<&[i64]> {
    if array.null_count() != 0 || array.offset() != 0 {
        return None;
    }
    Some(array.values().as_ref())
}

fn decimal128_values(array: &Decimal128Array) -> Option<&[i128]> {
    if array.null_count() != 0 || array.offset() != 0 {
        return None;
    }
    Some(array.values().as_ref())
}

fn date32_column(batch: &RecordBatch, index: usize) -> Result<&Date32Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| DataFusionError::Execution(format!("column {index} is not Date32")))
}

fn decimal128_column(batch: &RecordBatch, index: usize) -> Result<&Decimal128Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| DataFusionError::Execution(format!("column {index} is not Decimal128")))
}

fn int64_column(batch: &RecordBatch, index: usize) -> Result<&Int64Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Execution(format!("column {index} is not Int64")))
}
