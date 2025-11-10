use parking_lot::Mutex;
use std::ops::{Bound, RangeBounds};

use super::scan::ScanPrefetch;
use crate::catalog::SchemaRef;
use crate::error::QuillSQLError;
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::storage::{
    engine::{IndexScanRequest, TupleStream},
    page::{RecordId, TupleMeta},
};
use crate::transaction::{IsolationLevel, LockMode};
use crate::utils::table_ref::TableReference;
use crate::{error::QuillSQLResult, storage::tuple::Tuple};

const INDEX_PREFETCH_BATCH: usize = 64;
pub struct PhysicalIndexScan {
    table_ref: TableReference,
    index_name: String,
    table_schema: SchemaRef,
    start_bound: Bound<Tuple>,
    end_bound: Bound<Tuple>,
    stream: Mutex<Option<Box<dyn TupleStream>>>,
    prefetch: ScanPrefetch,
}

impl PhysicalIndexScan {
    pub fn new<R: RangeBounds<Tuple>>(
        table_ref: TableReference,
        index_name: String,
        table_schema: SchemaRef,
        range: R,
    ) -> Self {
        Self {
            table_ref,
            index_name,
            table_schema,
            start_bound: range.start_bound().cloned(),
            end_bound: range.end_bound().cloned(),
            stream: Mutex::new(None),
            prefetch: ScanPrefetch::new(INDEX_PREFETCH_BATCH),
        }
    }

    fn refill_buffer(&self) -> QuillSQLResult<bool> {
        self.prefetch.refill(|limit, out| {
            let mut stream_guard = self.stream.lock();
            let stream = stream_guard
                .as_mut()
                .ok_or_else(|| QuillSQLError::Execution("index stream not created".to_string()))?;
            for _ in 0..limit {
                match stream.next()? {
                    Some(entry) => out.push_back(entry),
                    None => break,
                }
            }
            Ok(())
        })
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
            .read_visible_tuple(&self.table_ref, rid, &meta, tuple)
    }
}

impl VolcanoExecutor for PhysicalIndexScan {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        if matches!(
            context.txn_ctx().isolation_level(),
            IsolationLevel::ReadCommitted
                | IsolationLevel::RepeatableRead
                | IsolationLevel::Serializable
        ) {
            context
                .txn_ctx_mut()
                .lock_table(self.table_ref.clone(), LockMode::IntentionShared)?;
        }

        let request = IndexScanRequest::new(self.start_bound.clone(), self.end_bound.clone());
        *self.stream.lock() =
            Some(context.index_stream(&self.table_ref, &self.index_name, request)?);

        self.prefetch.clear();

        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        loop {
            if let Some((rid, meta, tuple)) = self.prefetch.pop_front() {
                if meta.is_deleted {
                    continue;
                }
                if let Some(result) = self.consume_row(context, rid, meta, tuple)? {
                    return Ok(Some(result));
                }
                continue;
            }

            if !self.refill_buffer()? {
                return Ok(None);
            }
        }
    }

    fn output_schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }
}

impl std::fmt::Display for PhysicalIndexScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IndexScan: {}", self.index_name)
    }
}

impl std::fmt::Debug for PhysicalIndexScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhysicalIndexScan")
            .field("table_ref", &self.table_ref)
            .field("index_name", &self.index_name)
            .field("table_schema", &self.table_schema)
            .field("start_bound", &self.start_bound)
            .field("end_bound", &self.end_bound)
            .field("prefetch", &self.prefetch)
            .finish()
    }
}
