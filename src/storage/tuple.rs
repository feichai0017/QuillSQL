use crate::catalog::{SchemaRef, EMPTY_SCHEMA_REF};
use crate::error::QuillSQLError;
use crate::utils::table_ref::TableReference;
use crate::{catalog::Schema, error::QuillSQLResult, utils::scalar::ScalarValue};
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
        debug_assert!(schema
            .columns
            .iter()
            .zip(data.iter())
            .find(|(col, val)| ScalarValue::new_empty(col.data_type).data_type() != val.data_type())
            .is_none());
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
