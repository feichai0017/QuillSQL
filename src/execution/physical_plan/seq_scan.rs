use std::sync::Mutex;

use crate::catalog::SchemaRef;
use crate::error::QuillSQLError;
use crate::storage::table_heap::TableIterator;
use crate::utils::table_ref::TableReference;
use crate::{
    execution::{ExecutionContext, VolcanoExecutor},
    storage::tuple::Tuple,
    error::QuillSQLResult,
};

#[derive(Debug)]
pub struct PhysicalSeqScan {
    pub table: TableReference,
    pub table_schema: SchemaRef,

    iterator: Mutex<Option<TableIterator>>,
}

impl PhysicalSeqScan {
    pub fn new(table: TableReference, table_schema: SchemaRef) -> Self {
        PhysicalSeqScan {
            table,
            table_schema,
            iterator: Mutex::new(None),
        }
    }
}

impl VolcanoExecutor for PhysicalSeqScan {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        let table_heap = context.catalog.table_heap(&self.table)?;
        *self.iterator.lock().unwrap() = Some(TableIterator::new(table_heap, ..));
        Ok(())
    }

    fn next(&self, _context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        let Some(iterator) = &mut *self.iterator.lock().unwrap() else {
            return Err(QuillSQLError::Execution(
                "table iterator not created".to_string(),
            ));
        };
        Ok(iterator.next()?.map(|full| full.1))
    }

    fn output_schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }
}

impl std::fmt::Display for PhysicalSeqScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SeqScan")
    }
}
