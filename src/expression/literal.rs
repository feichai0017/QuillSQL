use crate::catalog::Schema;
use crate::catalog::{Column, DataType};
use crate::utils::scalar::ScalarValue;
use crate::error::QuillSQLResult;
use crate::expression::ExprTrait;
use crate::storage::tuple::Tuple;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Literal {
    pub value: ScalarValue,
}

impl ExprTrait for Literal {
    fn data_type(&self, _input_schema: &Schema) -> QuillSQLResult<DataType> {
        Ok(self.value.data_type())
    }

    fn nullable(&self, _input_schema: &Schema) -> QuillSQLResult<bool> {
        Ok(self.value.is_null())
    }

    fn evaluate(&self, _tuple: &Tuple) -> QuillSQLResult<ScalarValue> {
        Ok(self.value.clone())
    }

    fn to_column(&self, input_schema: &Schema) -> QuillSQLResult<Column> {
        Ok(Column::new(
            format!("{}", self.value),
            self.data_type(input_schema)?,
            self.nullable(input_schema)?,
        ))
    }
}

impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}
