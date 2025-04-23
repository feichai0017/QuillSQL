use crate::catalog::Schema;
use crate::catalog::{Column, DataType};
use crate::utils::scalar::ScalarValue;
use crate::utils::table_ref::TableReference;
use crate::error::QuillSQLResult;
use crate::expression::ExprTrait;
use crate::storage::tuple::Tuple;

/// A named reference to a qualified field in a schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnExpr {
    /// relation/table reference.
    pub relation: Option<TableReference>,
    /// field/column name.
    pub name: String,
}

impl ExprTrait for ColumnExpr {
    fn data_type(&self, input_schema: &Schema) -> QuillSQLResult<DataType> {
        let column = input_schema.column_with_name(self.relation.as_ref(), &self.name)?;
        Ok(column.data_type)
    }

    fn nullable(&self, input_schema: &Schema) -> QuillSQLResult<bool> {
        let column = input_schema.column_with_name(self.relation.as_ref(), &self.name)?;
        Ok(column.nullable)
    }

    fn evaluate(&self, tuple: &Tuple) -> QuillSQLResult<ScalarValue> {
        tuple
            .value_by_name(self.relation.as_ref(), &self.name)
            .cloned()
    }

    fn to_column(&self, input_schema: &Schema) -> QuillSQLResult<Column> {
        let column = input_schema.column_with_name(self.relation.as_ref(), &self.name)?;
        Ok(Column::new(
            self.name.clone(),
            self.data_type(input_schema)?,
            self.nullable(input_schema)?,
        )
        .with_relation(self.relation.clone().or(column.relation.clone())))
    }
}

impl std::fmt::Display for ColumnExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(relation) = self.relation.as_ref() {
            write!(f, "{}.", relation)?;
        }
        write!(f, "{}", self.name)
    }
}
