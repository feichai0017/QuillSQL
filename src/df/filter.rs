use std::ops::Bound;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::and;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::{DFSchema, DataFusionError, ScalarValue as DfScalarValue};
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_expr::create_physical_expr;

use crate::catalog::{DataType, Schema as QuillSchema, SchemaRef as QuillSchemaRef};
use crate::storage::engine::IndexScanRequest;
use crate::storage::tuple::Tuple;
use crate::utils::scalar::ScalarValue as QuillScalarValue;
use crate::utils::table_ref::TableReference;

use super::EngineState;

pub(super) struct ResolvedIndexFilter {
    pub(super) index_name: String,
    pub(super) request: IndexScanRequest,
}

struct ColumnConstraint {
    column_name: String,
    start: Bound<QuillScalarValue>,
    end: Bound<QuillScalarValue>,
}

pub(super) fn indexed_filter_for_expr(
    table_schema: &QuillSchema,
    state: &EngineState,
    table_ref: &TableReference,
    filter: &Expr,
) -> Option<ResolvedIndexFilter> {
    let constraint = column_constraint(filter)?;
    let indexes = state.catalog.lock().table_indexes(table_ref).ok()?;
    let index = indexes.into_iter().find(|index| {
        index.key_schema.columns.len() == 1
            && index.key_schema.columns[0].name == constraint.column_name
    })?;
    let key_type = index.key_schema.columns[0].data_type;

    let start = scalar_bound_to_tuple(constraint.start, index.key_schema.clone(), key_type)?;
    let end = scalar_bound_to_tuple(constraint.end, index.key_schema.clone(), key_type)?;
    if table_schema
        .index_of(None, &constraint.column_name)
        .is_err()
    {
        return None;
    }
    Some(ResolvedIndexFilter {
        index_name: index.name,
        request: IndexScanRequest::new(start, end),
    })
}

fn column_constraint(filter: &Expr) -> Option<ColumnConstraint> {
    let Expr::BinaryExpr(binary) = filter else {
        return None;
    };

    if binary.op == Operator::And {
        let left = column_constraint(binary.left.as_ref())?;
        let right = column_constraint(binary.right.as_ref())?;
        if left.column_name != right.column_name {
            return None;
        }
        return Some(ColumnConstraint {
            column_name: left.column_name,
            start: tighter_lower_bound(left.start, right.start),
            end: tighter_upper_bound(left.end, right.end),
        });
    }

    let (column_name, op, literal) =
        column_op_literal(binary.left.as_ref(), binary.op, binary.right.as_ref())?;
    let value = quill_scalar_from_df(literal)?;
    let (start, end) = match op {
        Operator::Eq => (Bound::Included(value.clone()), Bound::Included(value)),
        Operator::Gt => (Bound::Excluded(value), Bound::Unbounded),
        Operator::GtEq => (Bound::Included(value), Bound::Unbounded),
        Operator::Lt => (Bound::Unbounded, Bound::Excluded(value)),
        Operator::LtEq => (Bound::Unbounded, Bound::Included(value)),
        _ => return None,
    };
    Some(ColumnConstraint {
        column_name,
        start,
        end,
    })
}

fn column_op_literal<'a>(
    left: &'a Expr,
    op: Operator,
    right: &'a Expr,
) -> Option<(String, Operator, &'a DfScalarValue)> {
    match (left, right) {
        (Expr::Column(column), Expr::Literal(value, _)) => Some((column.name.clone(), op, value)),
        (Expr::Literal(value, _), Expr::Column(column)) => {
            Some((column.name.clone(), reverse_operator(op)?, value))
        }
        _ => None,
    }
}

fn reverse_operator(op: Operator) -> Option<Operator> {
    match op {
        Operator::Eq => Some(Operator::Eq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        _ => None,
    }
}

fn tighter_lower_bound(
    left: Bound<QuillScalarValue>,
    right: Bound<QuillScalarValue>,
) -> Bound<QuillScalarValue> {
    match (left, right) {
        (Bound::Unbounded, bound) | (bound, Bound::Unbounded) => bound,
        (left, right) => {
            let left_value = bound_value(&left);
            let right_value = bound_value(&right);
            if right_value > left_value {
                right
            } else {
                left
            }
        }
    }
}

fn tighter_upper_bound(
    left: Bound<QuillScalarValue>,
    right: Bound<QuillScalarValue>,
) -> Bound<QuillScalarValue> {
    match (left, right) {
        (Bound::Unbounded, bound) | (bound, Bound::Unbounded) => bound,
        (left, right) => {
            let left_value = bound_value(&left);
            let right_value = bound_value(&right);
            if right_value < left_value {
                right
            } else {
                left
            }
        }
    }
}

fn bound_value(bound: &Bound<QuillScalarValue>) -> &QuillScalarValue {
    match bound {
        Bound::Included(value) | Bound::Excluded(value) => value,
        Bound::Unbounded => unreachable!("unbounded handled by caller"),
    }
}

fn scalar_bound_to_tuple(
    bound: Bound<QuillScalarValue>,
    schema: QuillSchemaRef,
    data_type: DataType,
) -> Option<Bound<Tuple>> {
    match bound {
        Bound::Included(value) => Some(Bound::Included(Tuple::new(
            schema,
            vec![value.cast_to(&data_type).ok()?],
        ))),
        Bound::Excluded(value) => Some(Bound::Excluded(Tuple::new(
            schema,
            vec![value.cast_to(&data_type).ok()?],
        ))),
        Bound::Unbounded => Some(Bound::Unbounded),
    }
}

fn quill_scalar_from_df(value: &DfScalarValue) -> Option<QuillScalarValue> {
    match value {
        DfScalarValue::Null => None,
        DfScalarValue::Boolean(v) => Some(QuillScalarValue::Boolean(*v)),
        DfScalarValue::Float32(v) => Some(QuillScalarValue::Float32(*v)),
        DfScalarValue::Float64(v) => Some(QuillScalarValue::Float64(*v)),
        DfScalarValue::Int8(v) => Some(QuillScalarValue::Int8(*v)),
        DfScalarValue::Int16(v) => Some(QuillScalarValue::Int16(*v)),
        DfScalarValue::Int32(v) => Some(QuillScalarValue::Int32(*v)),
        DfScalarValue::Int64(v) => Some(QuillScalarValue::Int64(*v)),
        DfScalarValue::UInt8(v) => Some(QuillScalarValue::UInt8(*v)),
        DfScalarValue::UInt16(v) => Some(QuillScalarValue::UInt16(*v)),
        DfScalarValue::UInt32(v) => Some(QuillScalarValue::UInt32(*v)),
        DfScalarValue::UInt64(v) => Some(QuillScalarValue::UInt64(*v)),
        DfScalarValue::Utf8(v) | DfScalarValue::Utf8View(v) | DfScalarValue::LargeUtf8(v) => {
            Some(QuillScalarValue::Varchar(v.clone()))
        }
        _ => None,
    }
}

pub(super) fn evaluate_filters_to_mask(
    filters: &[Expr],
    batch: &RecordBatch,
    state: &dyn Session,
) -> Result<Option<BooleanArray>, DataFusionError> {
    if filters.is_empty() {
        return Ok(None);
    }

    let df_schema = DFSchema::try_from(batch.schema())?;
    let mut combined = None;
    for filter in filters {
        let physical = create_physical_expr(filter, &df_schema, state.execution_props())?;
        let value = physical.evaluate(batch)?;
        let array = value.into_array(batch.num_rows())?;
        let bool_array = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("filter did not evaluate to boolean".to_string())
            })?
            .clone();
        combined = Some(match combined {
            Some(existing) => and(&existing, &bool_array)?,
            None => bool_array,
        });
    }
    Ok(combined)
}
