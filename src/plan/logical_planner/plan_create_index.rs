use crate::plan::logical_plan::{CreateIndex, LogicalPlan};
use crate::error::{QuillSQLError, QuillSQLResult};

use super::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_create_index(
        &self,
        index_name: &sqlparser::ast::ObjectName,
        table_name: &sqlparser::ast::ObjectName,
        columns: &[sqlparser::ast::OrderByExpr],
    ) -> QuillSQLResult<LogicalPlan> {
        let index_name = index_name.0.first().map_or(
            Err(QuillSQLError::Plan(format!(
                "Index name {index_name} is not expected"
            ))),
            |ident| Ok(ident.value.clone()),
        )?;
        let table = self.bind_table_name(table_name)?;
        let mut columns_expr = vec![];
        for col in columns.iter() {
            let col_expr = self.bind_order_by_expr(col)?;
            columns_expr.push(col_expr);
        }
        let table_schema = self.context.catalog.table_heap(&table)?.schema.clone();
        Ok(LogicalPlan::CreateIndex(CreateIndex {
            index_name,
            table,
            table_schema,
            columns: columns_expr,
        }))
    }
}
