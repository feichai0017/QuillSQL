use crate::error::{QuillSQLError, QuillSQLResult};
use std::collections::HashSet;

use crate::catalog::{Column, DataType};
use crate::expression::Expr;
use crate::plan::logical_plan::{CreateTable, LogicalPlan};
use crate::utils::scalar::ScalarValue;

use super::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_create_table(
        &self,
        name: &sqlparser::ast::ObjectName,
        column_defs: &Vec<sqlparser::ast::ColumnDef>,
        if_not_exists: bool,
    ) -> QuillSQLResult<LogicalPlan> {
        let name = self.bind_table_name(name)?;
        let mut columns = vec![];
        for col_def in column_defs {
            let data_type: DataType = (&col_def.data_type).try_into()?;
            let not_null: bool = col_def
                .options
                .iter()
                .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull));
            let default_expr: Option<&sqlparser::ast::Expr> = col_def
                .options
                .iter()
                .find(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::Default(_)))
                .map(|opt| {
                    if let sqlparser::ast::ColumnOption::Default(expr) = &opt.option {
                        expr
                    } else {
                        unreachable!()
                    }
                });
            let default = if let Some(expr) = default_expr {
                let expr = self.bind_expr(expr)?;
                match expr {
                    Expr::Literal(lit) => lit.value.cast_to(&data_type)?,
                    _ => {
                        return Err(QuillSQLError::Internal(
                            "The expr is not literal".to_string(),
                        ))
                    }
                }
            } else {
                ScalarValue::new_empty(data_type)
            };

            columns.push(
                Column::new(col_def.name.value.clone(), data_type, !not_null)
                    .with_relation(Some(name.clone()))
                    .with_default(default),
            )
        }

        check_column_name_conflict(&columns)?;
        Ok(LogicalPlan::CreateTable(CreateTable {
            name,
            columns,
            if_not_exists,
        }))
    }
}

fn check_column_name_conflict(columns: &[Column]) -> QuillSQLResult<()> {
    let mut names = HashSet::new();
    for col in columns {
        if names.contains(col.name.as_str()) {
            return Err(QuillSQLError::Plan(format!(
                "Column names have conflict on '{}'",
                col.name
            )));
        } else {
            names.insert(col.name.as_str());
        }
    }
    Ok(())
}
