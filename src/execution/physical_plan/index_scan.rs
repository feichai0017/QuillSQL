use crate::catalog::SchemaRef;
use crate::error::QuillSQLError;
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::storage::index::bplus_index::TreeIndexIterator;
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
        }
    }
}

impl VolcanoExecutor for PhysicalIndexScan {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
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
        if let Some(rid) = iterator.next()? {
            Ok(Some(table_heap.tuple(rid)?))
        } else {
            Ok(None)
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
