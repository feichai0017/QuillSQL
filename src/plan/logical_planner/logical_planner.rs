use crate::error::{QuillSQLError, QuillSQLResult};

use crate::catalog::Catalog;
use crate::plan::logical_plan::{LogicalPlan, OrderByExpr};
use crate::sql::ast;
use crate::utils::table_ref::TableReference;

pub struct PlannerContext<'a> {
    pub catalog: &'a Catalog,
}

pub struct LogicalPlanner<'a> {
    pub context: PlannerContext<'a>,
}
impl<'a> LogicalPlanner<'a> {
    pub fn plan(&mut self, stmt: &ast::Statement) -> QuillSQLResult<LogicalPlan> {
        match stmt {
            ast::Statement::CreateTable { name, columns, .. } => {
                self.plan_create_table(name, columns)
            }
            ast::Statement::CreateIndex {
                name,
                table_name,
                columns,
                ..
            } => self.plan_create_index(name, table_name, columns),
            ast::Statement::Query(query) => self.plan_query(query),
            ast::Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => self.plan_insert(table_name, columns, source),
            ast::Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => self.plan_update(table, assignments, selection),
            ast::Statement::Explain { statement, .. } => self.plan_explain(statement),
            _ => unimplemented!(),
        }
    }

    pub fn bind_order_by_expr(&self, order_by: &ast::OrderByExpr) -> QuillSQLResult<OrderByExpr> {
        let expr = self.bind_expr(&order_by.expr)?;
        Ok(OrderByExpr {
            expr: Box::new(expr),
            asc: order_by.asc.unwrap_or(true),
            nulls_first: order_by.nulls_first.unwrap_or(false),
        })
    }

    pub fn bind_table_name(&self, table_name: &ast::ObjectName) -> QuillSQLResult<TableReference> {
        match table_name.0.as_slice() {
            [table] => Ok(TableReference::bare(table.value.clone())),
            [schema, table] => Ok(TableReference::partial(
                schema.value.clone(),
                table.value.clone(),
            )),
            [catalog, schema, table] => Ok(TableReference::full(
                catalog.value.clone(),
                schema.value.clone(),
                table.value.clone(),
            )),
            _ => Err(QuillSQLError::Plan(format!(
                "Fail to plan table name: {}",
                table_name
            ))),
        }
    }
}
