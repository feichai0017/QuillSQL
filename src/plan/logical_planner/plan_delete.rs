use crate::error::{QuillSQLError, QuillSQLResult};
use crate::plan::logical_plan::{Delete, LogicalPlan};
use crate::plan::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_delete(
        &self,
        table: &sqlparser::ast::TableWithJoins,
        selection: &Option<sqlparser::ast::Expr>,
    ) -> QuillSQLResult<LogicalPlan> {
        if !table.joins.is_empty() {
            return Err(QuillSQLError::Plan(
                "DELETE with joins is not supported".to_string(),
            ));
        }

        let table_ref = match &table.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => self.bind_table_name(name)?,
            _ => {
                return Err(QuillSQLError::Plan(format!(
                    "Table {} is not supported in DELETE",
                    table
                )))
            }
        };

        let table_heap = self.context.catalog.table_heap(&table_ref)?;
        let table_schema = table_heap.schema.clone();

        let predicate = match selection {
            Some(expr) => Some(self.bind_expr(expr)?),
            None => None,
        };

        Ok(LogicalPlan::Delete(Delete {
            table: table_ref,
            table_schema,
            selection: predicate,
        }))
    }
}
