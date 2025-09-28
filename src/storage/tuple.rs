use crate::catalog::{Schema, SchemaRef, EMPTY_SCHEMA_REF};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::page::{RecordId, TupleMeta};
use crate::transaction::TransactionId;
use crate::utils::scalar::ScalarValue;
use crate::utils::table_ref::TableReference;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, LazyLock};

pub static EMPTY_TUPLE: LazyLock<Tuple> = LazyLock::new(|| Tuple::empty(EMPTY_SCHEMA_REF.clone()));

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Tuple {
    pub schema: SchemaRef,
    pub data: Vec<ScalarValue>,
}

impl Tuple {
    pub fn new(schema: SchemaRef, data: Vec<ScalarValue>) -> Self {
        debug_assert_eq!(schema.columns.len(), data.len());
        Self { schema, data }
    }

    pub fn project_with_schema(&self, projected_schema: SchemaRef) -> QuillSQLResult<Self> {
        let indices = projected_schema
            .columns
            .iter()
            .map(|col| {
                self.schema
                    .index_of(col.relation.as_ref(), col.name.as_str())
            })
            .collect::<QuillSQLResult<Vec<usize>>>()?;
        let projected_data = indices
            .iter()
            .map(|idx| self.data[*idx].clone())
            .collect::<Vec<ScalarValue>>();
        Ok(Self::new(projected_schema, projected_data))
    }

    pub fn empty(schema: SchemaRef) -> Self {
        let mut data = vec![];
        for col in schema.columns.iter() {
            data.push(ScalarValue::new_empty(col.data_type));
        }
        Self::new(schema, data)
    }

    pub fn is_visible(meta: &TupleMeta, txn_id: TransactionId) -> bool {
        if meta.is_deleted {
            return meta.delete_txn_id == txn_id;
        }
        meta.insert_txn_id <= txn_id
    }

    pub fn try_merge(tuples: impl IntoIterator<Item = Self>) -> QuillSQLResult<Self> {
        let mut data = vec![];
        let mut merged_schema = Schema::empty();
        for tuple in tuples {
            data.extend(tuple.data);
            merged_schema = Schema::try_merge(vec![merged_schema, tuple.schema.as_ref().clone()])?;
        }
        Ok(Self::new(Arc::new(merged_schema), data))
    }

    pub fn is_null(&self) -> bool {
        self.data.iter().all(|x| x.is_null())
    }

    pub fn visible_to(&self, meta: &TupleMeta, txn_id: TransactionId) -> bool {
        if meta.is_deleted && meta.delete_txn_id <= txn_id {
            return false;
        }
        meta.insert_txn_id <= txn_id
    }

    pub fn value(&self, index: usize) -> QuillSQLResult<&ScalarValue> {
        self.data.get(index).ok_or(QuillSQLError::Internal(format!(
            "Not found column data at {} in tuple: {:?}",
            index, self
        )))
    }
    pub fn value_by_name(
        &self,
        relation: Option<&TableReference>,
        name: &str,
    ) -> QuillSQLResult<&ScalarValue> {
        let idx = self.schema.index_of(relation, name)?;
        self.value(idx)
    }

    pub fn as_rid(&self) -> QuillSQLResult<RecordId> {
        if self.data.len() < 2 {
            return Err(QuillSQLError::Execution(
                "RID tuple must have at least two columns".to_string(),
            ));
        }
        let page_id = value_as_u32(&self.data[0])?;
        let slot_num = value_as_u32(&self.data[1])?;
        Ok(RecordId::new(page_id, slot_num))
    }
}

impl Ord for Tuple {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for Tuple {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let column_count = self.schema.column_count();
        for idx in 0..column_count {
            let order = self.value(idx).ok()?.partial_cmp(other.value(idx).ok()?)?;
            if order == Ordering::Equal {
                continue;
            } else {
                return Some(order);
            }
        }
        Some(Ordering::Equal)
    }
}

impl Display for Tuple {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let values = self
            .data
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<String>>()
            .join(", ");
        write!(f, "({})", values)
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{Column, DataType, Schema};
    use std::cmp::Ordering;
    use std::sync::Arc;

    #[test]
    pub fn tuple_compare() {
        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let tuple1 = super::Tuple::new(schema.clone(), vec![1i8.into(), 2i16.into()]);
        let tuple2 = super::Tuple::new(schema.clone(), vec![1i8.into(), 2i16.into()]);
        let tuple3 = super::Tuple::new(schema.clone(), vec![1i8.into(), 3i16.into()]);
        let tuple4 = super::Tuple::new(schema.clone(), vec![2i8.into(), 2i16.into()]);
        let tuple5 = super::Tuple::new(schema.clone(), vec![1i8.into(), 1i16.into()]);

        assert_eq!(tuple1.partial_cmp(&tuple2).unwrap(), Ordering::Equal);
        assert_eq!(tuple1.partial_cmp(&tuple3).unwrap(), Ordering::Less);
        assert_eq!(tuple1.partial_cmp(&tuple4).unwrap(), Ordering::Less);
        assert_eq!(tuple1.partial_cmp(&tuple5).unwrap(), Ordering::Greater);
    }
}

fn value_as_u32(value: &ScalarValue) -> QuillSQLResult<u32> {
    let num = match value {
        ScalarValue::Int16(Some(v)) => *v as i32,
        ScalarValue::Int32(Some(v)) => *v,
        ScalarValue::Int64(Some(v)) => *v as i32,
        ScalarValue::UInt16(Some(v)) => *v as i32,
        ScalarValue::UInt32(Some(v)) => *v as i32,
        ScalarValue::UInt64(Some(v)) => *v as i32,
        ScalarValue::Int8(Some(v)) => *v as i32,
        ScalarValue::UInt8(Some(v)) => *v as i32,
        _ => {
            return Err(QuillSQLError::Execution(
                "RID column must be integer".to_string(),
            ))
        }
    };
    if num < 0 {
        return Err(QuillSQLError::Execution(
            "RID column must be positive".to_string(),
        ));
    }
    Ok(num as u32)
}
