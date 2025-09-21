use std::sync::Arc;

use crate::catalog::{Column, DataType, Schema};
use crate::error::QuillSQLResult;
use crate::plan::logical_plan::{LogicalPlan, Values};
use crate::plan::LogicalPlanner;
use crate::utils::util::pretty_format_logical_plan;

impl LogicalPlanner<'_> {
    /// Build a plan that returns the formatted logical plan as rows of text.
    pub fn plan_explain(
        &mut self,
        statement: &sqlparser::ast::Statement,
    ) -> QuillSQLResult<LogicalPlan> {
        let inner_plan = self.plan(statement)?;
        let text = pretty_format_logical_plan(&inner_plan);
        let lines: Vec<Vec<crate::expression::Expr>> = text
            .lines()
            .map(|s| {
                vec![crate::expression::Expr::Literal(
                    crate::expression::Literal {
                        value: s.to_string().into(),
                    },
                )]
            })
            .collect();

        let schema = Arc::new(Schema::new(vec![Column::new(
            "plan",
            DataType::Varchar(None),
            false,
        )]));
        Ok(LogicalPlan::Values(Values {
            schema,
            values: lines,
        }))
    }
}
