use parking_lot::Mutex;
use std::sync::OnceLock;

use super::scan::ScanPrefetch;
use crate::catalog::SchemaRef;
use crate::error::QuillSQLError;
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

    iterator: Mutex<Option<Box<dyn TupleStream>>>,
    prefetch: ScanPrefetch,
    table_binding: OnceLock<TableBinding>,
}

impl PhysicalSeqScan {
    pub fn new(table: TableReference, table_schema: SchemaRef) -> Self {
        PhysicalSeqScan {
            table,
            table_schema,
            iterator: Mutex::new(None),
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
        if self.table_binding.get().is_none() {
            let binding = context.table(&self.table)?;
            let _ = self.table_binding.set(binding);
        }
        let binding = self
            .table_binding
            .get()
            .expect("table binding not initialized");
        let stream = binding.scan()?;
        *self.iterator.lock() = Some(stream);
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
                let stream = guard.as_mut().ok_or_else(|| {
                    QuillSQLError::Execution("table stream not created".to_string())
                })?;
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
