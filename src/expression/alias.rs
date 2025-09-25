use crate::catalog::Schema;
use crate::catalog::{Column, DataType};
use crate::error::QuillSQLResult;
use crate::expression::{Expr, ExprTrait};
use crate::storage::tuple::Tuple;
use crate::utils::scalar::ScalarValue;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Alias {
    pub expr: Box<Expr>,
    pub name: String,
}

impl ExprTrait for Alias {
    fn data_type(&self, input_schema: &Schema) -> QuillSQLResult<DataType> {
        self.expr.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> QuillSQLResult<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, tuple: &Tuple) -> QuillSQLResult<ScalarValue> {
        self.expr.evaluate(tuple)
    }

    fn to_column(&self, input_schema: &Schema) -> QuillSQLResult<Column> {
        Ok(Column::new(
            self.name.clone(),
            self.data_type(input_schema)?,
            self.nullable(input_schema)?,
        ))
    }
}

impl std::fmt::Display for Alias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} AS {}", self.expr, self.name)
    }
}
