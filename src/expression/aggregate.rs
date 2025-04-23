use crate::catalog::{Column, DataType, Schema};
use crate::utils::scalar::ScalarValue;
use crate::expression::{Expr, ExprTrait};
use crate::function::AggregateFunctionKind;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::tuple::Tuple;
use std::fmt::Debug;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AggregateFunction {
    /// the function kind
    pub func_kind: AggregateFunctionKind,
    /// List of expressions to feed to the functions as arguments
    pub args: Vec<Expr>,
    /// Whether this is a DISTINCT aggregation or not
    pub distinct: bool,
}

impl ExprTrait for AggregateFunction {
    fn data_type(&self, _input_schema: &Schema) -> QuillSQLResult<DataType> {
        match self.func_kind {
            AggregateFunctionKind::Count => Ok(DataType::Int64),
            AggregateFunctionKind::Avg => Ok(DataType::Float64),
        }
    }

    fn nullable(&self, _input_schema: &Schema) -> QuillSQLResult<bool> {
        Ok(true)
    }

    fn evaluate(&self, tuple: &Tuple) -> QuillSQLResult<ScalarValue> {
        match self.func_kind {
            AggregateFunctionKind::Count | AggregateFunctionKind::Avg => {
                let expr = self.args.first().ok_or(QuillSQLError::Internal(format!(
                    "aggregate function {} should have one arg instead of {:?}",
                    self.func_kind, self.args
                )))?;
                expr.evaluate(tuple)
            }
        }
    }

    fn to_column(&self, input_schema: &Schema) -> QuillSQLResult<Column> {
        Ok(Column::new(
            format!("{}", self),
            self.data_type(input_schema)?,
            self.nullable(input_schema)?,
        ))
    }
}

impl std::fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.func_kind)
    }
}
