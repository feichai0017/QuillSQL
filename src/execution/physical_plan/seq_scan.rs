//! Table sequential scan operator (full-table read with MVCC filtering).

use std::cell::RefCell;
use std::sync::OnceLock;

use super::scan::ScanPrefetch;
use crate::catalog::SchemaRef;
use crate::execution::physical_plan::{resolve_table_binding, stream_not_ready};
use crate::storage::{
    engine::{TableBinding, TupleStream},
    page::{RecordId, TupleMeta},
};
use crate::transaction::LockMode;
use crate::utils::table_ref::TableReference;
use crate::{
    error::QuillSQLResult,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::tuple::Tuple,
};

const PREFETCH_BATCH: usize = 64;

pub struct PhysicalSeqScan {
    pub table: TableReference,
    pub table_schema: SchemaRef,

    iterator: RefCell<Option<Box<dyn TupleStream>>>,
    prefetch: ScanPrefetch,
    table_binding: OnceLock<TableBinding>,
}

impl PhysicalSeqScan {
    pub fn new(table: TableReference, table_schema: SchemaRef) -> Self {
        PhysicalSeqScan {
            table,
            table_schema,
            iterator: RefCell::new(None),
            prefetch: ScanPrefetch::new(PREFETCH_BATCH),
            table_binding: OnceLock::new(),
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
        let binding = resolve_table_binding(&self.table_binding, context, &self.table)?;
        let stream = binding.scan()?;
        self.iterator.replace(Some(stream));
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
                let mut guard = self.iterator.borrow_mut();
                let stream = guard.as_mut().ok_or_else(|| stream_not_ready("SeqScan"))?;
                for _ in 0..limit {
                    match stream.next()? {
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

impl std::fmt::Debug for PhysicalSeqScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhysicalSeqScan")
            .field("table", &self.table)
            .field("table_schema", &self.table_schema)
            .finish()
    }
}
