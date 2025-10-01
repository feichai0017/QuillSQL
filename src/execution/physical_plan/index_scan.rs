use crate::catalog::SchemaRef;
use crate::error::QuillSQLError;
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::storage::index::btree_index::TreeIndexIterator;
use crate::transaction::{IsolationLevel, LockMode};
use crate::utils::table_ref::TableReference;
use crate::{error::QuillSQLResult, storage::tuple::Tuple};
use std::ops::{Bound, RangeBounds};
use std::sync::Mutex;

#[derive(Debug)]
pub struct PhysicalIndexScan {
    table_ref: TableReference,
    index_name: String,
    table_schema: SchemaRef,
    start_bound: Bound<Tuple>,
    end_bound: Bound<Tuple>,
    iterator: Mutex<Option<TreeIndexIterator>>,
    // Counts invisible hits to opportunistically trigger lazy cleanup
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
            invisible_hits: Mutex::new(0),
        }
    }
}

impl VolcanoExecutor for PhysicalIndexScan {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        // Lock table IS for RC/RR; RU keeps current behavior
        if matches!(
            context.txn.isolation_level(),
            IsolationLevel::ReadCommitted
                | IsolationLevel::RepeatableRead
                | IsolationLevel::Serializable
        ) {
            context.lock_table(self.table_ref.clone(), LockMode::IntentionShared)?;
        }
        let index = context
            .catalog
            .index(&self.table_ref, &self.index_name)?
            .unwrap();
        *self.iterator.lock().unwrap() = Some(TreeIndexIterator::new(
            index,
            (self.start_bound.clone(), self.end_bound.clone()),
        ));
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        let mut guard = self.iterator.lock().unwrap();
        let Some(iterator) = &mut *guard else {
            return Err(QuillSQLError::Execution(
                "index iterator not created".to_string(),
            ));
        };
        let table_heap = context.catalog.table_heap(&self.table_ref)?;

        // thresholds: keep front-path smooth, offload to background mostly
        const INVISIBLE_THRESHOLD: usize = 2048;

        loop {
            if let Some(rid) = iterator.next()? {
                // heap visibility check
                let meta = table_heap.tuple_meta(rid)?;
                if meta.is_deleted {
                    // accumulate and maybe trigger lazy cleanup
                    let mut cnt = self.invisible_hits.lock().unwrap();
                    *cnt += 1;
                    if *cnt >= INVISIBLE_THRESHOLD {
                        *cnt = 0;
                        if let Some(index_arc) =
                            context.catalog.index(&self.table_ref, &self.index_name)?
                        {
                            // signal background worker via counter (best-effort)
                            index_arc.note_potential_garbage(INVISIBLE_THRESHOLD);
                        }
                    }
                    continue;
                }
                // Acquire S lock for RC/RR before returning tuple
                return match context.txn.isolation_level() {
                    IsolationLevel::ReadUncommitted => Ok(Some(table_heap.tuple(rid)?)),
                    IsolationLevel::ReadCommitted => {
                        context.lock_row_shared(&self.table_ref, rid, false)?;
                        let tuple = table_heap.tuple(rid)?;
                        context.unlock_row_shared(&self.table_ref, rid)?;
                        Ok(Some(tuple))
                    }
                    IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                        context.lock_row_shared(&self.table_ref, rid, true)?;
                        Ok(Some(table_heap.tuple(rid)?))
                    }
                };
            } else {
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
