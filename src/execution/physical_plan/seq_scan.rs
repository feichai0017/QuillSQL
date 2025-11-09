use parking_lot::Mutex;

use super::scan::ScanPrefetch;
use crate::catalog::SchemaRef;
use crate::error::QuillSQLError;
use crate::storage::page::{RecordId, TupleMeta};
use crate::storage::table_heap::TableIterator;
use crate::transaction::LockMode;
use crate::utils::table_ref::TableReference;
use crate::{
    error::QuillSQLResult,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::tuple::Tuple,
};

const PREFETCH_BATCH: usize = 64;

#[derive(Debug)]
pub struct PhysicalSeqScan {
    pub table: TableReference,
    pub table_schema: SchemaRef,
    pub streaming_hint: Option<bool>,

    iterator: Mutex<Option<TableIterator>>,
    prefetch: ScanPrefetch,
}

impl PhysicalSeqScan {
    pub fn new(table: TableReference, table_schema: SchemaRef) -> Self {
        PhysicalSeqScan {
            table,
            table_schema,
            streaming_hint: None,
            iterator: Mutex::new(None),
            prefetch: ScanPrefetch::new(PREFETCH_BATCH),
        }
    }

    fn consume_row(
        &self,
        context: &mut ExecutionContext,
        rid: RecordId,
        meta: TupleMeta,
        tuple: Tuple,
    ) -> QuillSQLResult<Option<Tuple>> {
        context
            .txn_ctx_mut()
            .read_visible_tuple(&self.table, rid, &meta, tuple)
    }
}

impl VolcanoExecutor for PhysicalSeqScan {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        context
            .txn_ctx_mut()
            .lock_table(self.table.clone(), LockMode::IntentionShared)?;
        let table_heap = context.table_heap(&self.table)?;
        let iter = if let Some(h) = self.streaming_hint {
            TableIterator::new_with_hint(table_heap, .., Some(h))
        } else {
            TableIterator::new(table_heap, ..)
        };
        {
            let mut guard = self.iterator.lock();
            *guard = Some(iter);
        }
        self.prefetch.clear();
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        loop {
            if let Some((rid, meta, tuple)) = self.prefetch.pop_front() {
                if let Some(result) = self.consume_row(context, rid, meta, tuple)? {
                    return Ok(Some(result));
                }
                continue;
            }

            if !self.prefetch.refill(|limit, out| {
                let mut guard = self.iterator.lock();
                let iterator = guard.as_mut().ok_or_else(|| {
                    QuillSQLError::Execution("table iterator not created".to_string())
                })?;
                for _ in 0..limit {
                    match iterator.next()? {
                        Some(entry) => out.push_back(entry),
                        None => break,
                    }
                }
                Ok(())
            })? {
                return Ok(None);
            }
        }
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
