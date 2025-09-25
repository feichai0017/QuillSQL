mod aggregate;
mod alias;
mod binary;
mod cast;
mod column;
mod literal;
mod util;

pub use aggregate::AggregateFunction;
pub use alias::Alias;
pub use binary::BinaryExpr;
pub use cast::Cast;
pub use column::ColumnExpr;
pub use literal::Literal;
pub use util::*;

use crate::catalog::Schema;
use crate::catalog::{Column, DataType};
use crate::error::QuillSQLResult;
use crate::storage::tuple::Tuple;
use crate::utils::scalar::ScalarValue;

pub trait ExprTrait {
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> QuillSQLResult<DataType>;

    /// Determine whether this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> QuillSQLResult<bool>;

    /// Evaluate an expression against a Tuple
    fn evaluate(&self, tuple: &Tuple) -> QuillSQLResult<ScalarValue>;

    /// convert to a column with respect to a schema
    fn to_column(&self, input_schema: &Schema) -> QuillSQLResult<Column>;
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Expr {
    /// An expression with a specific name.
    Alias(Alias),
    /// A named reference to a qualified filed in a schema.
    Column(ColumnExpr),
    /// A constant value.
    Literal(Literal),
    /// A binary expression such as "age > 21"
    Binary(BinaryExpr),
    /// Casts the expression to a given type and will return a runtime error if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    Cast(Cast),
    /// Represents the call of an aggregate built-in function with arguments.
    AggregateFunction(AggregateFunction),
}

impl ExprTrait for Expr {
    fn data_type(&self, input_schema: &Schema) -> QuillSQLResult<DataType> {
        match self {
            Expr::Alias(alias) => alias.data_type(input_schema),
            Expr::Column(column) => column.data_type(input_schema),
            Expr::Literal(literal) => literal.data_type(input_schema),
            Expr::Binary(binary) => binary.data_type(input_schema),
            Expr::Cast(cast) => cast.data_type(input_schema),
            Expr::AggregateFunction(aggr) => aggr.data_type(input_schema),
        }
    }

    fn nullable(&self, input_schema: &Schema) -> QuillSQLResult<bool> {
        match self {
            Expr::Alias(alias) => alias.nullable(input_schema),
            Expr::Column(column) => column.nullable(input_schema),
            Expr::Literal(literal) => literal.nullable(input_schema),
            Expr::Binary(binary) => binary.nullable(input_schema),
            Expr::Cast(cast) => cast.nullable(input_schema),
            Expr::AggregateFunction(aggr) => aggr.nullable(input_schema),
        }
    }

    fn evaluate(&self, tuple: &Tuple) -> QuillSQLResult<ScalarValue> {
        match self {
            Expr::Alias(alias) => alias.evaluate(tuple),
            Expr::Column(column) => column.evaluate(tuple),
            Expr::Literal(literal) => literal.evaluate(tuple),
            Expr::Binary(binary) => binary.evaluate(tuple),
            Expr::Cast(cast) => cast.evaluate(tuple),
            Expr::AggregateFunction(aggr) => aggr.evaluate(tuple),
        }
    }

    fn to_column(&self, input_schema: &Schema) -> QuillSQLResult<Column> {
        match self {
            Expr::Alias(alias) => alias.to_column(input_schema),
            Expr::Column(column) => column.to_column(input_schema),
            Expr::Literal(literal) => literal.to_column(input_schema),
            Expr::Binary(binary) => binary.to_column(input_schema),
            Expr::Cast(cast) => cast.to_column(input_schema),
            Expr::AggregateFunction(aggr) => aggr.to_column(input_schema),
        }
    }
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Alias(e) => write!(f, "{e}"),
            Expr::Column(e) => write!(f, "{e}"),
            Expr::Literal(e) => write!(f, "{e}"),
            Expr::Binary(e) => write!(f, "{e}"),
            Expr::Cast(e) => write!(f, "{e}"),
            Expr::AggregateFunction(e) => write!(f, "{e}"),
        }
    }
}
