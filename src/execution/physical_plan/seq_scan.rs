use std::sync::Mutex;

use crate::catalog::SchemaRef;
use crate::error::QuillSQLError;
use crate::storage::table_heap::TableIterator;
use crate::transaction::{LockMode, TransactionId};
use crate::utils::table_ref::TableReference;
use crate::{
    error::QuillSQLResult,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::tuple::Tuple,
};

#[derive(Debug)]
pub struct PhysicalSeqScan {
    pub table: TableReference,
    pub table_schema: SchemaRef,
    pub streaming_hint: Option<bool>,

    iterator: Mutex<Option<TableIterator>>,
}

impl PhysicalSeqScan {
    pub fn new(table: TableReference, table_schema: SchemaRef) -> Self {
        PhysicalSeqScan {
            table,
            table_schema,
            streaming_hint: None,
            iterator: Mutex::new(None),
        }
    }
}

impl VolcanoExecutor for PhysicalSeqScan {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        context.lock_table(self.table.clone(), LockMode::IntentionShared)?;
        let table_heap = context.catalog.table_heap(&self.table)?;
        let iter = if let Some(h) = self.streaming_hint {
            TableIterator::new_with_hint(table_heap, .., Some(h))
        } else {
            TableIterator::new(table_heap, ..)
        };
        *self.iterator.lock().unwrap() = Some(iter);
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        let Some(iterator) = &mut *self.iterator.lock().unwrap() else {
            return Err(QuillSQLError::Execution(
                "table iterator not created".to_string(),
            ));
        };
        if let Some((rid, tuple)) = iterator.next()? {
            context.txn_mgr.record_row_lock(
                context.txn.id(),
                self.table.clone(),
                rid,
                LockMode::Shared,
            );
            return Ok(Some(tuple));
        }
        Ok(None)
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
