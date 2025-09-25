use crate::catalog::{Column, DataType, Schema};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::expression::{Expr, ExprTrait};
use crate::storage::tuple::Tuple;
use crate::utils::scalar::ScalarValue;

/// Cast expression
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Cast {
    /// The expression being cast
    pub expr: Box<Expr>,
    /// The `DataType` the expression will yield
    pub data_type: DataType,
}

impl ExprTrait for Cast {
    fn data_type(&self, _input_schema: &Schema) -> QuillSQLResult<DataType> {
        Ok(self.data_type)
    }

    fn nullable(&self, input_schema: &Schema) -> QuillSQLResult<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, tuple: &Tuple) -> QuillSQLResult<ScalarValue> {
        let value = self.expr.evaluate(tuple)?;
        value.cast_to(&self.data_type)
    }

    fn to_column(&self, _input_schema: &Schema) -> QuillSQLResult<Column> {
        Err(QuillSQLError::Plan(format!(
            "expr {:?} as column not supported",
            self
        )))
    }
}

impl std::fmt::Display for Cast {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CAST {} AS {}", self.expr, self.data_type)
    }
}
