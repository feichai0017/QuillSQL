use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::{DataFusionError, TableReference as DfTableRef};
use datafusion::logical_expr::{Expr, SortExpr};

use crate::catalog::{Column, DataType, Schema as QuillSchema, SchemaRef as QuillSchemaRef};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::tuple::Tuple;
use crate::utils::scalar::ScalarValue as QuillScalarValue;
use crate::utils::table_ref::TableReference;

pub(crate) fn record_batches_to_string_rows(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|array| array_value_to_string(array.as_ref(), row))
                    .collect(),
            );
        }
    }
    rows
}

pub(crate) fn pretty_format_batches(batches: &[RecordBatch]) -> QuillSQLResult<comfy_table::Table> {
    let mut table = comfy_table::Table::new();
    table.load_preset("||--+-++|    ++++++");
    let Some(first) = batches.first() else {
        return Ok(table);
    };
    table.set_header(
        first
            .schema()
            .fields()
            .iter()
            .map(|field| comfy_table::Cell::new(field.name()))
            .collect::<Vec<_>>(),
    );
    for row in record_batches_to_string_rows(batches) {
        table.add_row(row);
    }
    Ok(table)
}

pub(super) fn project_arrow_schema(
    schema: ArrowSchemaRef,
    projection: Option<&[usize]>,
) -> ArrowSchemaRef {
    match projection {
        Some(projection) => Arc::new(ArrowSchema::new(
            projection
                .iter()
                .map(|idx| schema.fields()[*idx].clone())
                .collect::<Vec<_>>(),
        )),
        None => schema,
    }
}

pub(super) fn project_batch(
    batch: &RecordBatch,
    projection: Option<&[usize]>,
) -> Result<RecordBatch, DataFusionError> {
    match projection {
        Some(projection) => {
            let columns = projection
                .iter()
                .map(|idx| batch.column(*idx).clone())
                .collect::<Vec<_>>();
            let schema = project_arrow_schema(batch.schema(), Some(projection));
            RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
        }
        None => Ok(batch.clone()),
    }
}

pub(super) fn record_batch_from_tuples(
    arrow_schema: ArrowSchemaRef,
    quill_schema: &QuillSchema,
    tuples: &[Tuple],
) -> QuillSQLResult<RecordBatch> {
    let arrays = quill_schema
        .columns
        .iter()
        .enumerate()
        .map(|(column_idx, column)| build_array(column.data_type, tuples, column_idx))
        .collect::<QuillSQLResult<Vec<_>>>()?;
    RecordBatch::try_new(arrow_schema, arrays).map_err(map_arrow_err)
}

fn build_array(
    data_type: DataType,
    tuples: &[Tuple],
    column_idx: usize,
) -> QuillSQLResult<ArrayRef> {
    match data_type {
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(typed_values(
            tuples,
            column_idx,
            "Boolean",
            |value| match value {
                QuillScalarValue::Boolean(v) => Some(*v),
                _ => None,
            },
        )?))),
        DataType::Int8 => Ok(Arc::new(Int8Array::from(typed_values(
            tuples,
            column_idx,
            "Int8",
            |value| match value {
                QuillScalarValue::Int8(v) => Some(*v),
                _ => None,
            },
        )?))),
        DataType::Int16 => Ok(Arc::new(Int16Array::from(typed_values(
            tuples,
            column_idx,
            "Int16",
            |value| match value {
                QuillScalarValue::Int16(v) => Some(*v),
                _ => None,
            },
        )?))),
        DataType::Int32 => Ok(Arc::new(Int32Array::from(typed_values(
            tuples,
            column_idx,
            "Int32",
            |value| match value {
                QuillScalarValue::Int32(v) => Some(*v),
                _ => None,
            },
        )?))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(typed_values(
            tuples,
            column_idx,
            "Int64",
            |value| match value {
                QuillScalarValue::Int64(v) => Some(*v),
                _ => None,
            },
        )?))),
        DataType::UInt8 => Ok(Arc::new(UInt8Array::from(typed_values(
            tuples,
            column_idx,
            "UInt8",
            |value| match value {
                QuillScalarValue::UInt8(v) => Some(*v),
                _ => None,
            },
        )?))),
        DataType::UInt16 => Ok(Arc::new(UInt16Array::from(typed_values(
            tuples,
            column_idx,
            "UInt16",
            |value| match value {
                QuillScalarValue::UInt16(v) => Some(*v),
                _ => None,
            },
        )?))),
        DataType::UInt32 => Ok(Arc::new(UInt32Array::from(typed_values(
            tuples,
            column_idx,
            "UInt32",
            |value| match value {
                QuillScalarValue::UInt32(v) => Some(*v),
                _ => None,
            },
        )?))),
        DataType::UInt64 => Ok(Arc::new(UInt64Array::from(typed_values(
            tuples,
            column_idx,
            "UInt64",
            |value| match value {
                QuillScalarValue::UInt64(v) => Some(*v),
                _ => None,
            },
        )?))),
        DataType::Float32 => Ok(Arc::new(Float32Array::from(typed_values(
            tuples,
            column_idx,
            "Float32",
            |value| match value {
                QuillScalarValue::Float32(v) => Some(*v),
                _ => None,
            },
        )?))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(typed_values(
            tuples,
            column_idx,
            "Float64",
            |value| match value {
                QuillScalarValue::Float64(v) => Some(*v),
                _ => None,
            },
        )?))),
        DataType::Varchar(_) => Ok(Arc::new(StringArray::from(typed_values(
            tuples,
            column_idx,
            "Varchar",
            |value| match value {
                QuillScalarValue::Varchar(v) => Some(v.clone()),
                _ => None,
            },
        )?))),
    }
}

fn typed_values<T, F>(
    tuples: &[Tuple],
    column_idx: usize,
    expected: &str,
    mut project: F,
) -> QuillSQLResult<Vec<Option<T>>>
where
    F: FnMut(&QuillScalarValue) -> Option<Option<T>>,
{
    tuples
        .iter()
        .map(|tuple| {
            let value = tuple.value(column_idx)?;
            project(value).ok_or_else(|| {
                QuillSQLError::Internal(format!(
                    "expected {expected} at column {column_idx}, got {value:?}"
                ))
            })
        })
        .collect()
}

pub(super) fn tuples_from_batch(
    schema: QuillSchemaRef,
    batch: &RecordBatch,
) -> QuillSQLResult<Vec<Tuple>> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let values = batch
            .columns()
            .iter()
            .map(|array| scalar_from_array(array.as_ref(), row))
            .collect::<QuillSQLResult<Vec<_>>>()?;
        rows.push(Tuple::new(schema.clone(), values));
    }
    Ok(rows)
}

fn scalar_from_array(array: &dyn Array, row: usize) -> QuillSQLResult<QuillScalarValue> {
    let data_type = quill_type_from_arrow(array.data_type())?;
    if array.is_null(row) {
        return Ok(QuillScalarValue::new_empty(data_type));
    }

    match data_type {
        DataType::Boolean => Ok(QuillScalarValue::Boolean(Some(
            downcast_array::<BooleanArray>(array, "Boolean")?.value(row),
        ))),
        DataType::Int8 => Ok(QuillScalarValue::Int8(Some(
            downcast_array::<Int8Array>(array, "Int8")?.value(row),
        ))),
        DataType::Int16 => Ok(QuillScalarValue::Int16(Some(
            downcast_array::<Int16Array>(array, "Int16")?.value(row),
        ))),
        DataType::Int32 => Ok(QuillScalarValue::Int32(Some(
            downcast_array::<Int32Array>(array, "Int32")?.value(row),
        ))),
        DataType::Int64 => Ok(QuillScalarValue::Int64(Some(
            downcast_array::<Int64Array>(array, "Int64")?.value(row),
        ))),
        DataType::UInt8 => Ok(QuillScalarValue::UInt8(Some(
            downcast_array::<UInt8Array>(array, "UInt8")?.value(row),
        ))),
        DataType::UInt16 => Ok(QuillScalarValue::UInt16(Some(
            downcast_array::<UInt16Array>(array, "UInt16")?.value(row),
        ))),
        DataType::UInt32 => Ok(QuillScalarValue::UInt32(Some(
            downcast_array::<UInt32Array>(array, "UInt32")?.value(row),
        ))),
        DataType::UInt64 => Ok(QuillScalarValue::UInt64(Some(
            downcast_array::<UInt64Array>(array, "UInt64")?.value(row),
        ))),
        DataType::Float32 => Ok(QuillScalarValue::Float32(Some(
            downcast_array::<Float32Array>(array, "Float32")?.value(row),
        ))),
        DataType::Float64 => Ok(QuillScalarValue::Float64(Some(
            downcast_array::<Float64Array>(array, "Float64")?.value(row),
        ))),
        DataType::Varchar(_) => Ok(QuillScalarValue::Varchar(Some(
            downcast_array::<StringArray>(array, "Utf8")?
                .value(row)
                .to_string(),
        ))),
    }
}

fn downcast_array<'a, T: 'static>(array: &'a dyn Array, expected: &str) -> QuillSQLResult<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        QuillSQLError::Internal(format!(
            "expected Arrow array {expected}, got {:?}",
            array.data_type()
        ))
    })
}

pub(super) fn arrow_schema_from_quill(schema: &QuillSchema) -> ArrowSchema {
    ArrowSchema::new(
        schema
            .columns
            .iter()
            .map(|column| {
                Field::new(
                    column.name.clone(),
                    arrow_type_from_quill(column.data_type),
                    column.nullable,
                )
            })
            .collect::<Vec<_>>(),
    )
}

pub(super) fn quill_schema_from_arrow(schema: &ArrowSchema) -> QuillSQLResult<QuillSchema> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            Ok(Column::new(
                field.name().clone(),
                quill_type_from_arrow(field.data_type())?,
                field.is_nullable(),
            ))
        })
        .collect::<QuillSQLResult<Vec<_>>>()?;
    Ok(QuillSchema::new(columns))
}

fn arrow_type_from_quill(data_type: DataType) -> ArrowDataType {
    match data_type {
        DataType::Boolean => ArrowDataType::Boolean,
        DataType::Int8 => ArrowDataType::Int8,
        DataType::Int16 => ArrowDataType::Int16,
        DataType::Int32 => ArrowDataType::Int32,
        DataType::Int64 => ArrowDataType::Int64,
        DataType::UInt8 => ArrowDataType::UInt8,
        DataType::UInt16 => ArrowDataType::UInt16,
        DataType::UInt32 => ArrowDataType::UInt32,
        DataType::UInt64 => ArrowDataType::UInt64,
        DataType::Float32 => ArrowDataType::Float32,
        DataType::Float64 => ArrowDataType::Float64,
        DataType::Varchar(_) => ArrowDataType::Utf8,
    }
}

fn quill_type_from_arrow(data_type: &ArrowDataType) -> QuillSQLResult<DataType> {
    match data_type {
        ArrowDataType::Boolean => Ok(DataType::Boolean),
        ArrowDataType::Int8 => Ok(DataType::Int8),
        ArrowDataType::Int16 => Ok(DataType::Int16),
        ArrowDataType::Int32 => Ok(DataType::Int32),
        ArrowDataType::Int64 => Ok(DataType::Int64),
        ArrowDataType::UInt8 => Ok(DataType::UInt8),
        ArrowDataType::UInt16 => Ok(DataType::UInt16),
        ArrowDataType::UInt32 => Ok(DataType::UInt32),
        ArrowDataType::UInt64 => Ok(DataType::UInt64),
        ArrowDataType::Float32 => Ok(DataType::Float32),
        ArrowDataType::Float64 => Ok(DataType::Float64),
        ArrowDataType::Utf8 => Ok(DataType::Varchar(None)),
        other => Err(QuillSQLError::NotSupport(format!(
            "Arrow type {other:?} is not supported by Holt SQL tables"
        ))),
    }
}

pub(super) fn index_schema_from_sort_exprs(
    table_schema: &QuillSchema,
    columns: &[SortExpr],
) -> QuillSQLResult<QuillSchema> {
    let mut out = Vec::with_capacity(columns.len());
    for sort in columns {
        let Expr::Column(column) = &sort.expr else {
            return Err(QuillSQLError::NotSupport(format!(
                "index expression {} is not a column",
                sort.expr
            )));
        };
        let idx = table_schema.index_of(None, &column.name)?;
        out.push(table_schema.columns[idx].as_ref().clone());
    }
    Ok(QuillSchema::new(out))
}

pub(super) fn default_index_name(table_ref: &TableReference, key_schema: &QuillSchema) -> String {
    let suffix = key_schema
        .columns
        .iter()
        .map(|col| col.name.as_str())
        .collect::<Vec<_>>()
        .join("_");
    format!("idx_{}_{}", table_ref.table(), suffix)
}

pub(super) fn quill_table_ref(table_ref: &DfTableRef) -> TableReference {
    match table_ref {
        DfTableRef::Bare { table } => TableReference::Bare {
            table: table.to_string(),
        },
        DfTableRef::Partial { schema, table } => TableReference::Partial {
            schema: schema.to_string(),
            table: table.to_string(),
        },
        DfTableRef::Full {
            catalog,
            schema,
            table,
        } => TableReference::Full {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        },
    }
}

fn array_value_to_string(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "NULL".to_string();
    }
    match array.data_type() {
        ArrowDataType::Boolean => downcast_array::<BooleanArray>(array, "Boolean")
            .map(|array| array.value(row).to_string())
            .unwrap_or_else(|err| err.to_string()),
        ArrowDataType::Int8 => downcast_array::<Int8Array>(array, "Int8")
            .map(|array| array.value(row).to_string())
            .unwrap_or_else(|err| err.to_string()),
        ArrowDataType::Int16 => downcast_array::<Int16Array>(array, "Int16")
            .map(|array| array.value(row).to_string())
            .unwrap_or_else(|err| err.to_string()),
        ArrowDataType::Int32 => downcast_array::<Int32Array>(array, "Int32")
            .map(|array| array.value(row).to_string())
            .unwrap_or_else(|err| err.to_string()),
        ArrowDataType::Int64 => downcast_array::<Int64Array>(array, "Int64")
            .map(|array| array.value(row).to_string())
            .unwrap_or_else(|err| err.to_string()),
        ArrowDataType::UInt8 => downcast_array::<UInt8Array>(array, "UInt8")
            .map(|array| array.value(row).to_string())
            .unwrap_or_else(|err| err.to_string()),
        ArrowDataType::UInt16 => downcast_array::<UInt16Array>(array, "UInt16")
            .map(|array| array.value(row).to_string())
            .unwrap_or_else(|err| err.to_string()),
        ArrowDataType::UInt32 => downcast_array::<UInt32Array>(array, "UInt32")
            .map(|array| array.value(row).to_string())
            .unwrap_or_else(|err| err.to_string()),
        ArrowDataType::UInt64 => downcast_array::<UInt64Array>(array, "UInt64")
            .map(|array| array.value(row).to_string())
            .unwrap_or_else(|err| err.to_string()),
        ArrowDataType::Float32 => downcast_array::<Float32Array>(array, "Float32")
            .map(|array| array.value(row).to_string())
            .unwrap_or_else(|err| err.to_string()),
        ArrowDataType::Float64 => downcast_array::<Float64Array>(array, "Float64")
            .map(|array| array.value(row).to_string())
            .unwrap_or_else(|err| err.to_string()),
        ArrowDataType::Utf8 => downcast_array::<StringArray>(array, "Utf8")
            .map(|array| array.value(row).to_string())
            .unwrap_or_else(|err| err.to_string()),
        other => format!("<unsupported {other:?}>"),
    }
}

fn map_arrow_err(err: datafusion::arrow::error::ArrowError) -> QuillSQLError {
    QuillSQLError::Execution(format!("Arrow error: {err}"))
}
