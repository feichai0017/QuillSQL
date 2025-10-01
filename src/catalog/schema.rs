use super::column::{Column, ColumnRef};
use crate::catalog::DataType;
use crate::error::QuillSQLError;
use crate::error::QuillSQLResult;
use crate::utils::table_ref::TableReference;
use std::sync::{Arc, LazyLock};

pub type SchemaRef = Arc<Schema>;

pub static EMPTY_SCHEMA_REF: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(Schema::empty()));
pub static INSERT_OUTPUT_SCHEMA_REF: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Column::new(
        "insert_rows",
        DataType::Int32,
        false,
    )]))
});
pub static UPDATE_OUTPUT_SCHEMA_REF: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Column::new(
        "update_rows",
        DataType::Int32,
        false,
    )]))
});
pub static DELETE_OUTPUT_SCHEMA_REF: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Column::new(
        "delete_rows",
        DataType::Int32,
        false,
    )]))
});

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Schema {
    pub columns: Vec<ColumnRef>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self::new_with_check(columns.into_iter().map(Arc::new).collect())
    }

    fn new_with_check(columns: Vec<ColumnRef>) -> Self {
        for (idx1, col1) in columns.iter().enumerate() {
            for col2 in columns.iter().skip(idx1 + 1) {
                match (&col1.relation, &col2.relation) {
                    (Some(rel1), Some(rel2)) => {
                        assert!(!(rel1.resolved_eq(rel2) && col1.name == col2.name));
                    }
                    (None, None) => assert_ne!(col1.name, col2.name),
                    (Some(_), None) | (None, Some(_)) => {}
                }
            }
        }
        Self { columns }
    }

    pub fn empty() -> Self {
        Self { columns: vec![] }
    }

    pub fn try_merge(schemas: impl IntoIterator<Item = Self>) -> QuillSQLResult<Self> {
        let mut columns = Vec::new();
        for schema in schemas {
            columns.extend(schema.columns);
        }
        Ok(Self::new_with_check(columns))
    }

    pub fn project(&self, indices: &[usize]) -> QuillSQLResult<Schema> {
        let new_columns = indices
            .iter()
            .map(|i| self.column_with_index(*i))
            .collect::<QuillSQLResult<Vec<ColumnRef>>>()?;
        Ok(Schema::new_with_check(new_columns))
    }

    pub fn column_with_name(
        &self,
        relation: Option<&TableReference>,
        name: &str,
    ) -> QuillSQLResult<ColumnRef> {
        let index = self.index_of(relation, name)?;
        Ok(self.columns[index].clone())
    }

    pub fn column_with_index(&self, index: usize) -> QuillSQLResult<ColumnRef> {
        self.columns
            .get(index)
            .cloned()
            .ok_or_else(|| QuillSQLError::Plan(format!("Unable to get column with index {index}")))
    }

    /// Find the index of the column with the given name.
    pub fn index_of(&self, relation: Option<&TableReference>, name: &str) -> QuillSQLResult<usize> {
        let (idx, _) = self
            .columns
            .iter()
            .enumerate()
            .find(|(_, col)| {
                let name_matches = col.name.eq_ignore_ascii_case(name);
                match (relation, &col.relation) {
                    (Some(rel), Some(col_rel)) => name_matches && rel.resolved_eq(col_rel),
                    (Some(_), None) => false,
                    (None, Some(_)) | (None, None) => name_matches,
                }
            })
            .ok_or_else(|| QuillSQLError::Plan(format!("Unable to get column named \"{name}\"")))?;
        Ok(idx)
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}
