use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use parking_lot::Mutex;

use super::scan::ScanPrefetch;
use crate::catalog::SchemaRef;
use crate::error::QuillSQLError;
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::storage::index::btree_index::TreeIndexIterator;
use crate::storage::page::{RecordId, TupleMeta};
use crate::storage::table_heap::TableHeap;
use crate::transaction::{IsolationLevel, LockMode};
use crate::utils::table_ref::TableReference;
use crate::{error::QuillSQLResult, storage::tuple::Tuple};

const INDEX_PREFETCH_BATCH: usize = 64;
const INVISIBLE_THRESHOLD: usize = 2048;

#[derive(Debug)]
pub struct PhysicalIndexScan {
    table_ref: TableReference,
    index_name: String,
    table_schema: SchemaRef,
    start_bound: Bound<Tuple>,
    end_bound: Bound<Tuple>,
    iterator: Mutex<Option<TreeIndexIterator>>,
    table_heap: Mutex<Option<Arc<TableHeap>>>,
    prefetch: ScanPrefetch,
    invisible_hits: Mutex<usize>,
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
            iterator: Mutex::new(None),
            table_heap: Mutex::new(None),
            prefetch: ScanPrefetch::new(INDEX_PREFETCH_BATCH),
            invisible_hits: Mutex::new(0),
        }
    }

    fn refill_buffer(&self) -> QuillSQLResult<bool> {
        let table_heap = {
            let guard = self.table_heap.lock();
            guard
                .clone()
                .ok_or_else(|| QuillSQLError::Execution("table heap not initialized".to_string()))?
        };

        self.prefetch.refill(|limit, out| {
            let mut iter_guard = self.iterator.lock();
            let iterator = iter_guard.as_mut().ok_or_else(|| {
                QuillSQLError::Execution("index iterator not created".to_string())
            })?;
            for _ in 0..limit {
                match iterator.next()? {
                    Some(rid) => {
                        let (meta, tuple) = table_heap.full_tuple(rid)?;
                        out.push_back((rid, meta, tuple));
                    }
                    None => break,
                }
            }
            Ok(())
        })
    }

    fn handle_invisible(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        let mut cnt = self.invisible_hits.lock();
        *cnt += 1;
        if *cnt >= INVISIBLE_THRESHOLD {
            *cnt = 0;
            if let Some(index_arc) = context.catalog.index(&self.table_ref, &self.index_name)? {
                index_arc.note_potential_garbage(INVISIBLE_THRESHOLD);
            }
        }
        Ok(())
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

        let table_heap = context.table_heap(&self.table_ref)?;
        let index = context
            .catalog
            .index(&self.table_ref, &self.index_name)?
            .unwrap();

        {
            let mut iter_guard = self.iterator.lock();
            *iter_guard = Some(TreeIndexIterator::new(
                index,
                (self.start_bound.clone(), self.end_bound.clone()),
            ));
        }

        {
            let mut heap_guard = self.table_heap.lock();
            *heap_guard = Some(table_heap);
        }

        self.prefetch.clear();
        *self.invisible_hits.lock() = 0;

        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        loop {
            if let Some((rid, meta, tuple)) = self.prefetch.pop_front() {
                if meta.is_deleted {
                    self.handle_invisible(context)?;
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
