use crate::catalog::{ColumnRef, Schema};
use crate::error::QuillSQLResult;
use crate::expression::{Expr, ExprTrait};
use crate::plan::logical_plan::JoinType;
use crate::plan::logical_plan::LogicalPlan;
use std::sync::Arc;

pub fn build_join_schema(
    left: &Schema,
    right: &Schema,
    join_type: JoinType,
) -> QuillSQLResult<Schema> {
    fn nullify_columns(columns: &[ColumnRef]) -> Vec<ColumnRef> {
        columns
            .iter()
            .map(|f| Arc::new(f.as_ref().clone().with_nullable(true)))
            .collect()
    }

    let left_cols = &left.columns;
    let right_cols = &right.columns;

    let columns: Vec<ColumnRef> = match join_type {
        JoinType::Inner | JoinType::Cross => {
            left_cols.iter().chain(right_cols.iter()).cloned().collect()
        }
        JoinType::LeftOuter => left_cols
            .iter()
            .chain(&nullify_columns(right_cols))
            .cloned()
            .collect(),
        JoinType::RightOuter => nullify_columns(left_cols)
            .iter()
            .chain(right_cols.iter())
            .cloned()
            .collect(),
        JoinType::FullOuter => nullify_columns(left_cols)
            .iter()
            .chain(&nullify_columns(right_cols))
            .cloned()
            .collect(),
    };
    Ok(Schema { columns })
}

pub fn project_schema(input: &LogicalPlan, exprs: &[Expr]) -> QuillSQLResult<Schema> {
    let input_schema = &input.schema();
    let mut columns = vec![];
    for expr in exprs {
        columns.push(expr.to_column(input_schema)?)
    }
    Ok(Schema::new(columns))
}
