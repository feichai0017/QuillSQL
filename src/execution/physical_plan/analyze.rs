use std::fmt::Display;

use crate::catalog::{SchemaRef, EMPTY_SCHEMA_REF};
use crate::error::QuillSQLResult;
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::storage::tuple::Tuple;
use crate::transaction::LockMode;
use crate::utils::table_ref::TableReference;

#[derive(Debug)]
pub struct PhysicalAnalyze {
    table: TableReference,
}

impl PhysicalAnalyze {
    pub fn new(table: TableReference) -> Self {
        Self { table }
    }
}

impl VolcanoExecutor for PhysicalAnalyze {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        context
            .txn_ctx_mut()
            .lock_table(self.table.clone(), LockMode::IntentionShared)?;
        context.catalog.analyze_table(&self.table)?;
        Ok(())
    }

    fn next(&self, _context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        Ok(None)
    }

    fn output_schema(&self) -> SchemaRef {
        EMPTY_SCHEMA_REF.clone()
    }
}

impl Display for PhysicalAnalyze {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Analyze {}", self.table)
    }
}
