use crate::error::{QuillSQLError, QuillSQLResult};
use crate::plan::logical_plan::{DropIndex, DropTable, LogicalPlan};

use super::LogicalPlanner;

impl<'a> LogicalPlanner<'a> {
    pub fn plan_drop_table(
        &self,
        names: &[sqlparser::ast::ObjectName],
        if_exists: bool,
        cascade: bool,
        purge: bool,
    ) -> QuillSQLResult<LogicalPlan> {
        if purge {
            return Err(QuillSQLError::NotSupport(
                "DROP TABLE ... PURGE is not supported".to_string(),
            ));
        }
        if names.len() != 1 {
            return Err(QuillSQLError::NotSupport(
                "DROP TABLE only supports a single target".to_string(),
            ));
        }

        let table_ref = self.bind_table_name(&names[0])?;
        if cascade {
            // Implicitly drop dependent indexes, so CASCADE behaves the same as default.
            // No-op, but accepted for compatibility.
        }

        Ok(LogicalPlan::DropTable(DropTable {
            name: table_ref,
            if_exists,
        }))
    }

    pub fn plan_drop_index(
        &self,
        names: &[sqlparser::ast::ObjectName],
        if_exists: bool,
        cascade: bool,
        purge: bool,
    ) -> QuillSQLResult<LogicalPlan> {
        if cascade {
            return Err(QuillSQLError::NotSupport(
                "DROP INDEX ... CASCADE is not supported".to_string(),
            ));
        }
        if purge {
            return Err(QuillSQLError::NotSupport(
                "DROP INDEX ... PURGE is not supported".to_string(),
            ));
        }
        if names.len() != 1 {
            return Err(QuillSQLError::NotSupport(
                "DROP INDEX only supports a single target".to_string(),
            ));
        }

        let parts = &names[0].0;
        let (catalog, schema, name) = match parts.as_slice() {
            [ident] => (None, None, ident.value.clone()),
            [schema, ident] => (None, Some(schema.value.clone()), ident.value.clone()),
            [catalog, schema, ident] => (
                Some(catalog.value.clone()),
                Some(schema.value.clone()),
                ident.value.clone(),
            ),
            _ => {
                return Err(QuillSQLError::Plan(format!(
                    "DROP INDEX name '{}' has too many qualifiers",
                    names[0]
                )))
            }
        };

        Ok(LogicalPlan::DropIndex(DropIndex {
            name,
            schema,
            catalog,
            if_exists,
        }))
    }
}
