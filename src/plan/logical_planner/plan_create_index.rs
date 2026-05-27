use crate::catalog::IndexEngine;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::plan::logical_plan::{CreateIndex, LogicalPlan};

use super::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_create_index(
        &self,
        index_name: &sqlparser::ast::ObjectName,
        table_name: &sqlparser::ast::ObjectName,
        using: Option<&sqlparser::ast::Ident>,
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
        let using = parse_index_engine(using)?;
        let table_schema = self.context.catalog.table_schema(&table)?;
        Ok(LogicalPlan::CreateIndex(CreateIndex {
            index_name,
            table,
            table_schema,
            columns: columns_expr,
            using,
        }))
    }
}

fn parse_index_engine(
    using: Option<&sqlparser::ast::Ident>,
) -> QuillSQLResult<Option<IndexEngine>> {
    let Some(using) = using else {
        return Ok(None);
    };
    match using.value.to_ascii_lowercase().as_str() {
        "btree" | "bplus" | "bplustree" => Ok(Some(IndexEngine::BTree)),
        "holt" => Ok(Some(IndexEngine::Holt)),
        other => Err(QuillSQLError::NotSupport(format!(
            "index backend {other} is not supported"
        ))),
    }
}
