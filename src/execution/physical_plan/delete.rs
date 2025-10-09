use std::sync::atomic::{AtomicU32, Ordering};

use crate::catalog::{SchemaRef, DELETE_OUTPUT_SCHEMA_REF};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::expression::Expr;
use crate::expression::ExprTrait;
use crate::storage::table_heap::TableIterator;
use crate::storage::tuple::Tuple;
use crate::transaction::LockMode;
use crate::utils::scalar::ScalarValue;
use crate::utils::table_ref::TableReference;

#[derive(Debug)]
pub struct PhysicalDelete {
    pub table: TableReference,
    pub table_schema: SchemaRef,
    pub predicate: Option<Expr>,
    deleted_rows: AtomicU32,
    iterator: std::sync::Mutex<Option<TableIterator>>,
}

impl PhysicalDelete {
    pub fn new(table: TableReference, table_schema: SchemaRef, predicate: Option<Expr>) -> Self {
        Self {
            table,
            table_schema,
            predicate,
            deleted_rows: AtomicU32::new(0),
            iterator: std::sync::Mutex::new(None),
        }
    }
}

impl VolcanoExecutor for PhysicalDelete {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        self.deleted_rows.store(0, Ordering::SeqCst);
        context.ensure_writable(&self.table, "DELETE")?;
        context.lock_table(self.table.clone(), LockMode::IntentionExclusive)?;
        let table_heap = context.catalog.table_heap(&self.table)?;
        *self.iterator.lock().unwrap() = Some(TableIterator::new(table_heap, ..));
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        let Some(iterator) = &mut *self.iterator.lock().unwrap() else {
            return Err(QuillSQLError::Execution(
                "table iterator not created".to_string(),
            ));
        };

        loop {
            let Some((rid, tuple)) = iterator.next()? else {
                let deleted = self.deleted_rows.swap(0, Ordering::SeqCst);
                if deleted == 0 {
                    return Ok(None);
                }
                return Ok(Some(Tuple::new(
                    self.output_schema(),
                    vec![ScalarValue::Int32(Some(deleted as i32))],
                )));
            };

            if let Some(predicate) = &self.predicate {
                if !predicate.evaluate(&tuple)?.as_boolean()?.unwrap_or(false) {
                    continue;
                }
            }

            let table_heap = context.catalog.table_heap(&self.table)?;
            let meta = table_heap.tuple_meta(rid)?;
            if !Tuple::is_visible(&meta, context.txn.id()) {
                continue;
            }

            context.lock_row_exclusive(&self.table, rid)?;

            let old_tuple = table_heap.tuple(rid)?;
            table_heap.delete_tuple(rid, context.txn.id(), context.command_id())?;

            let mut index_entries = Vec::new();
            for index in context.catalog.table_indexes(&self.table)? {
                if let Ok(key) = old_tuple.project_with_schema(index.key_schema.clone()) {
                    index.delete(&key)?;
                    index_entries.push((index.clone(), key));
                }
            }

            context
                .txn
                .push_delete_undo(table_heap.clone(), rid, meta, old_tuple, index_entries);
            self.deleted_rows.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn output_schema(&self) -> SchemaRef {
        DELETE_OUTPUT_SCHEMA_REF.clone()
    }
}

impl std::fmt::Display for PhysicalDelete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Delete")
    }
}
