use crate::catalog::SchemaRef;
use crate::utils::table_ref::TableReference;
use crate::{
    catalog::Schema,
    error::QuillSQLResult,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::tuple::Tuple,
};
use std::sync::Arc;

#[derive(derive_new::new, Debug)]
pub struct PhysicalCreateTable {
    pub table: TableReference,
    pub schema: Schema,
    pub if_not_exists: bool,
}

impl VolcanoExecutor for PhysicalCreateTable {
    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        if self.if_not_exists && context.catalog.try_table_heap(&self.table).is_some() {
            return Ok(None);
        }

        context
            .catalog
            .create_table(self.table.clone(), Arc::new(self.schema.clone()))?;
        Ok(None)
    }
    fn output_schema(&self) -> SchemaRef {
        Arc::new(self.schema.clone())
    }
}

impl std::fmt::Display for PhysicalCreateTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CreateTable: {}", self.table)
    }
}
