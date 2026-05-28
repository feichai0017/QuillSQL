use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Date32Array, Decimal128Array, Float64Array, Int64Array};
use arrow::datatypes::DataType as ArrowDataType;
use arrow::record_batch::RecordBatch;
use quill_plan::{JitError, JitResult};
type Result<T> = JitResult<T>;

use crate::{
    CompiledDecimalFilterSum, CompiledF64FilterSum, CompiledKernel, CompiledRecordPipeline,
    FilterProjectKernel, FilterSumKernel, FilterSumValue, FixedColumnInput, JitType, MlirBackend,
    MlirColumn, PipelineSpec, PredicateSpec, RecordPipelineOutput,
};

thread_local! {
    static RECORD_PIPELINE_CACHE: RefCell<HashMap<String, CompiledRecordPipeline>> =
        RefCell::new(HashMap::new());
    static F64_FILTER_SUM_CACHE: RefCell<HashMap<String, CompiledF64FilterSum>> =
        RefCell::new(HashMap::new());
    static DECIMAL_FILTER_SUM_CACHE: RefCell<HashMap<String, CompiledDecimalFilterSum>> =
        RefCell::new(HashMap::new());
}

pub fn execute_filter_project(
    kernel: &CompiledKernel,
    runtime: &FilterProjectKernel,
    batch: &RecordBatch,
) -> Result<Option<RecordBatch>> {
    if !kernel.executable || kernel.backend != "mlir" {
        return Ok(None);
    }

    let PipelineSpec::RecordProject {
        columns,
        output_types,
    } = &kernel.spec
    else {
        return Ok(None);
    };
    let Some(inputs) = fixed_inputs(batch, columns)? else {
        return Ok(None);
    };

    let mut buffers = output_types
        .iter()
        .map(|ty| OutputBuffer::with_capacity(*ty, batch.num_rows()))
        .collect::<Result<Vec<_>>>()?;
    let mut outputs = buffers
        .iter_mut()
        .map(OutputBuffer::as_output)
        .collect::<Vec<_>>();
    let cache_key = filter_project_cache_key(runtime);
    let output_len = RECORD_PIPELINE_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if !cache.contains_key(&cache_key) {
            let compiled = MlirBackend::new()
                .compile_record_pipeline(runtime.predicate(), runtime.projections())?;
            cache.insert(cache_key.clone(), compiled);
        }
        cache
            .get(&cache_key)
            .expect("compiled kernel was inserted")
            .invoke(&inputs, &mut outputs)
    })?;

    let arrays = buffers
        .into_iter()
        .zip(runtime.schema().fields())
        .map(|(buffer, field)| buffer.finish(output_len, field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(runtime.schema(), arrays)
        .map(Some)
        .map_err(|err| JitError::Backend(err.to_string()))
}

pub fn execute_filter_sum(
    kernel: &CompiledKernel,
    runtime: &FilterSumKernel,
    batch: &RecordBatch,
) -> Result<Option<FilterSumValue>> {
    if !kernel.executable || kernel.backend != "mlir" {
        return Ok(None);
    }

    match &kernel.spec {
        PipelineSpec::F64FilterSum {
            predicate_column,
            measure_left_column,
            measure_right_column,
            ..
        } => execute_f64_filter_sum(
            runtime,
            batch,
            *predicate_column,
            *measure_left_column,
            *measure_right_column,
        ),
        PipelineSpec::DecimalFilterSum {
            predicates,
            measure_left_column,
            measure_right_column,
            output_scale,
        } => execute_decimal_filter_sum(
            runtime,
            batch,
            predicates,
            *measure_left_column,
            *measure_right_column,
            *output_scale,
        ),
        _ => Ok(None),
    }
}

fn filter_project_cache_key(runtime: &FilterProjectKernel) -> String {
    format!("{:?}|{:?}", runtime.predicate(), runtime.projections())
}

fn execute_f64_filter_sum(
    runtime: &FilterSumKernel,
    batch: &RecordBatch,
    predicate_col: usize,
    left_col: usize,
    right_col: usize,
) -> Result<Option<FilterSumValue>> {
    let predicate = int64_column(batch, predicate_col)?;
    let left = float64_column(batch, left_col)?;
    let right = float64_column(batch, right_col)?;
    let Some(predicate_values) = int64_values(predicate) else {
        return Ok(None);
    };
    let Some(left_values) = float64_values(left) else {
        return Ok(None);
    };
    let Some(right_values) = float64_values(right) else {
        return Ok(None);
    };
    let inputs = [
        FixedColumnInput::Int64 {
            index: predicate_col,
            values: predicate_values,
        },
        FixedColumnInput::Float64 {
            index: left_col,
            values: left_values,
        },
        FixedColumnInput::Float64 {
            index: right_col,
            values: right_values,
        },
    ];

    let cache_key = filter_sum_cache_key(runtime);
    let output = F64_FILTER_SUM_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if !cache.contains_key(&cache_key) {
            let compiled = MlirBackend::new()
                .compile_f64_filter_sum(runtime.predicate(), runtime.measure())?;
            cache.insert(cache_key.clone(), compiled);
        }
        cache
            .get(&cache_key)
            .expect("compiled kernel was inserted")
            .invoke(&inputs)
    })?;

    Ok(Some(FilterSumValue::Float64(
        (output.count > 0).then_some(output.sum),
    )))
}

fn execute_decimal_filter_sum(
    runtime: &FilterSumKernel,
    batch: &RecordBatch,
    predicates: &[PredicateSpec],
    left_col: usize,
    right_col: usize,
    scale: i8,
) -> Result<Option<FilterSumValue>> {
    let Some(inputs) = decimal_filter_sum_inputs(batch, predicates, left_col, right_col, scale)?
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
        scale,
    }))
}

fn filter_sum_cache_key(runtime: &FilterSumKernel) -> String {
    format!("{:?}|{:?}", runtime.predicate(), runtime.measure())
}

fn fixed_inputs<'a>(
    batch: &'a RecordBatch,
    columns: &[MlirColumn],
) -> Result<Option<Vec<FixedColumnInput<'a>>>> {
    let mut inputs = Vec::with_capacity(columns.len());
    for column in columns {
        let Some(input) = fixed_input(batch, *column)? else {
            return Ok(None);
        };
        inputs.push(input);
    }
    Ok(Some(inputs))
}

fn fixed_input<'a>(
    batch: &'a RecordBatch,
    column: MlirColumn,
) -> Result<Option<FixedColumnInput<'a>>> {
    match column.ty {
        JitType::Date32 => {
            let array = date32_column(batch, column.index)?;
            Ok(date32_values(array).map(|values| FixedColumnInput::Date32 {
                index: column.index,
                values,
            }))
        }
        JitType::Int64 => {
            let array = int64_column(batch, column.index)?;
            Ok(int64_values(array).map(|values| FixedColumnInput::Int64 {
                index: column.index,
                values,
            }))
        }
        JitType::Float64 => {
            let array = float64_column(batch, column.index)?;
            Ok(
                float64_values(array).map(|values| FixedColumnInput::Float64 {
                    index: column.index,
                    values,
                }),
            )
        }
        JitType::Decimal128 { scale, .. } => {
            let array = decimal128_column(batch, column.index)?;
            if array.scale() != scale {
                return Err(JitError::Backend(format!(
                    "decimal input scale {} does not match column scale {}",
                    scale,
                    array.scale()
                )));
            }
            Ok(
                decimal128_values(array).map(|values| FixedColumnInput::Decimal128 {
                    index: column.index,
                    values,
                }),
            )
        }
        other => Err(JitError::Backend(format!(
            "unsupported fixed-width input type {other:?}"
        ))),
    }
}

enum OutputBuffer {
    Date32(Vec<i32>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    Decimal128(Vec<i128>),
}

impl OutputBuffer {
    fn with_capacity(ty: JitType, capacity: usize) -> Result<Self> {
        match ty {
            JitType::Date32 => Ok(Self::Date32(vec![0; capacity])),
            JitType::Int64 => Ok(Self::Int64(vec![0; capacity])),
            JitType::Float64 => Ok(Self::Float64(vec![0.0; capacity])),
            JitType::Decimal128 { .. } => Ok(Self::Decimal128(vec![0; capacity])),
            other => Err(JitError::Backend(format!(
                "unsupported record output type {other:?}"
            ))),
        }
    }

    fn as_output(&mut self) -> RecordPipelineOutput<'_> {
        match self {
            Self::Date32(values) => RecordPipelineOutput::Date32 { values },
            Self::Int64(values) => RecordPipelineOutput::Int64 { values },
            Self::Float64(values) => RecordPipelineOutput::Float64 { values },
            Self::Decimal128(values) => RecordPipelineOutput::Decimal128 { values },
        }
    }

    fn finish(self, len: usize, data_type: &ArrowDataType) -> Result<ArrayRef> {
        let array = match (self, data_type) {
            (Self::Date32(values), ArrowDataType::Date32) => {
                Arc::new(Date32Array::from(values[..len].to_vec())) as ArrayRef
            }
            (Self::Int64(values), ArrowDataType::Int64) => {
                Arc::new(Int64Array::from(values[..len].to_vec())) as ArrayRef
            }
            (Self::Float64(values), ArrowDataType::Float64) => {
                Arc::new(Float64Array::from(values[..len].to_vec())) as ArrayRef
            }
            (Self::Decimal128(values), ArrowDataType::Decimal128(precision, scale)) => Arc::new(
                Decimal128Array::from(values[..len].to_vec())
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|err| JitError::Backend(err.to_string()))?,
            )
                as ArrayRef,
            (_, other) => {
                return Err(JitError::Backend(format!(
                    "record output buffer does not match schema type {other:?}"
                )));
            }
        };
        Ok(array)
    }
}

fn decimal_filter_sum_inputs<'a>(
    batch: &'a RecordBatch,
    predicates: &[PredicateSpec],
    left_col: usize,
    right_col: usize,
    scale: i8,
) -> Result<Option<Vec<FixedColumnInput<'a>>>> {
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
        return Err(JitError::Backend(format!(
            "decimal multiply scale {} + {} does not match output scale {}",
            left.scale(),
            right.scale(),
            scale
        )));
    }
    Ok(Some(inputs))
}

fn push_predicate_input<'a>(
    inputs: &mut Vec<FixedColumnInput<'a>>,
    batch: &'a RecordBatch,
    predicate: &PredicateSpec,
) -> Result<Option<()>> {
    match *predicate {
        PredicateSpec::Date32 { column, .. } => push_date32_input(inputs, batch, column),
        PredicateSpec::Decimal128 { column, scale, .. } => {
            push_decimal_input(inputs, batch, column, Some(scale))
        }
        PredicateSpec::Int64 { column, .. } => push_int64_input(inputs, batch, column),
    }
}

fn push_date32_input<'a>(
    inputs: &mut Vec<FixedColumnInput<'a>>,
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
    inputs.push(FixedColumnInput::Date32 { index, values });
    Ok(Some(()))
}

fn push_int64_input<'a>(
    inputs: &mut Vec<FixedColumnInput<'a>>,
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
    inputs.push(FixedColumnInput::Int64 { index, values });
    Ok(Some(()))
}

fn push_decimal_input<'a>(
    inputs: &mut Vec<FixedColumnInput<'a>>,
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
            return Err(JitError::Backend(format!(
                "decimal predicate scale {} does not match column scale {}",
                expected_scale,
                array.scale()
            )));
        }
    }
    let Some(values) = decimal128_values(array) else {
        return Ok(None);
    };
    inputs.push(FixedColumnInput::Decimal128 { index, values });
    Ok(Some(()))
}

fn has_input(inputs: &[FixedColumnInput<'_>], index: usize) -> bool {
    inputs.iter().any(|input| match input {
        FixedColumnInput::Date32 {
            index: input_index, ..
        }
        | FixedColumnInput::Int64 {
            index: input_index, ..
        }
        | FixedColumnInput::Float64 {
            index: input_index, ..
        }
        | FixedColumnInput::Decimal128 {
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

fn float64_values(array: &Float64Array) -> Option<&[f64]> {
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

fn float64_column(batch: &RecordBatch, index: usize) -> Result<&Float64Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| JitError::Backend(format!("column {index} is not Float64")))
}

fn date32_column(batch: &RecordBatch, index: usize) -> Result<&Date32Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| JitError::Backend(format!("column {index} is not Date32")))
}

fn decimal128_column(batch: &RecordBatch, index: usize) -> Result<&Decimal128Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| JitError::Backend(format!("column {index} is not Decimal128")))
}

fn int64_column(batch: &RecordBatch, index: usize) -> Result<&Int64Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| JitError::Backend(format!("column {index} is not Int64")))
}
