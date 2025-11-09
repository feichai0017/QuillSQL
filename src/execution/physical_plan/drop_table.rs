use crate::catalog::{SchemaRef, EMPTY_SCHEMA_REF};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::storage::tuple::Tuple;
use crate::transaction::LockMode;
use crate::utils::table_ref::TableReference;

#[derive(Debug)]
pub struct PhysicalDropTable {
    table: TableReference,
    if_exists: bool,
}

impl PhysicalDropTable {
    pub fn new(table: TableReference, if_exists: bool) -> Self {
        Self { table, if_exists }
    }

    fn qualified_name(&self) -> String {
        self.table.to_string()
    }
}

impl VolcanoExecutor for PhysicalDropTable {
    fn init(&self, _context: &mut ExecutionContext) -> QuillSQLResult<()> {
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        if context.catalog.try_table_heap(&self.table).is_none() {
            if self.if_exists {
                return Ok(None);
            }
            return Err(QuillSQLError::Execution(format!(
                "table {} does not exist",
                self.qualified_name()
            )));
        }

        context
            .txn_ctx()
            .ensure_writable(&self.table, "DROP TABLE")?;
        context
            .txn_ctx_mut()
            .lock_table(self.table.clone(), LockMode::Exclusive)?;

        let dropped = context.catalog.drop_table(&self.table)?;
        if !dropped && !self.if_exists {
            return Err(QuillSQLError::Execution(format!(
                "table {} does not exist",
                self.qualified_name()
            )));
        }

        Ok(None)
    }

    fn output_schema(&self) -> SchemaRef {
        EMPTY_SCHEMA_REF.clone()
    }
}

impl std::fmt::Display for PhysicalDropTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DropTable: {}", self.table)
    }
}
