use crate::catalog::{SchemaRef, UPDATE_OUTPUT_SCHEMA_REF};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::expression::{Expr, ExprTrait};
use crate::storage::table_heap::TableIterator;
use crate::storage::tuple::{Tuple, EMPTY_TUPLE};
use crate::transaction::LockMode;
use crate::utils::scalar::ScalarValue;
use crate::utils::table_ref::TableReference;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

#[derive(Debug)]
pub struct PhysicalUpdate {
    pub table: TableReference,
    pub table_schema: SchemaRef,
    pub assignments: HashMap<String, Expr>,
    pub selection: Option<Expr>,

    update_rows: AtomicU32,
    table_iterator: Mutex<Option<TableIterator>>,
}

impl PhysicalUpdate {
    pub fn new(
        table: TableReference,
        table_schema: SchemaRef,
        assignments: HashMap<String, Expr>,
        selection: Option<Expr>,
    ) -> Self {
        Self {
            table,
            table_schema,
            assignments,
            selection,
            update_rows: AtomicU32::new(0),
            table_iterator: Mutex::new(None),
        }
    }
}

impl VolcanoExecutor for PhysicalUpdate {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        self.update_rows.store(0, Ordering::SeqCst);
        context.ensure_writable(&self.table, "UPDATE")?;
        let table_heap = context.catalog.table_heap(&self.table)?;
        *self.table_iterator.lock().unwrap() = Some(TableIterator::new(table_heap.clone(), ..));
        context
            .lock_table(self.table.clone(), LockMode::IntentionExclusive)
            .map_err(|_| {
                QuillSQLError::Execution(format!(
                    "failed to acquire IX lock on table {}",
                    self.table
                ))
            })?;
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        // TODO may scan index
        let Some(table_iterator) = &mut *self.table_iterator.lock().unwrap() else {
            return Err(QuillSQLError::Execution(
                "table iterator not created".to_string(),
            ));
        };
        let table_heap = context.catalog.table_heap(&self.table)?;

        loop {
            if let Some((rid, meta, tuple)) = table_iterator.next()? {
                // Skip versions that were created by this command so we do not
                // immediately reprocess the freshly inserted MVCC tuple and loop forever.
                if meta.insert_txn_id == context.txn_id() && meta.insert_cid == context.command_id()
                {
                    continue;
                }

                if let Some(selection) = &self.selection {
                    if !selection.evaluate(&tuple)?.as_boolean()?.unwrap_or(false) {
                        continue;
                    }
                }

                if !context.is_visible(&meta) {
                    continue;
                }

                context.lock_row_exclusive(&self.table, rid)?;

                let (prev_meta, mut current_tuple) = table_heap.full_tuple(rid)?;
                if !context.is_visible(&prev_meta) {
                    context.unlock_row(&self.table, rid);
                    continue;
                }
                let prev_tuple = current_tuple.clone();

                // update tuple data
                for (col_name, value_expr) in self.assignments.iter() {
                    let index = current_tuple.schema.index_of(None, col_name)?;
                    let col_datatype = current_tuple.schema.columns[index].data_type;
                    let new_value = value_expr.evaluate(&EMPTY_TUPLE)?.cast_to(&col_datatype)?;
                    current_tuple.data[index] = new_value;
                }
                let (new_rid, _) = table_heap.mvcc_update(
                    rid,
                    current_tuple.clone(),
                    context.txn_id(),
                    context.command_id(),
                )?;

                let mut new_keys = Vec::new();
                let indexes = context.catalog.table_indexes(&self.table)?;
                for index in indexes {
                    if let Ok(new_key_tuple) =
                        current_tuple.project_with_schema(index.key_schema.clone())
                    {
                        index.insert(&new_key_tuple, new_rid)?;
                        new_keys.push((index.clone(), new_key_tuple));
                    }
                }

                context.txn_mut().push_update_undo(
                    table_heap.clone(),
                    rid,
                    new_rid,
                    prev_meta,
                    prev_tuple,
                    new_keys,
                );

                self.update_rows.fetch_add(1, Ordering::SeqCst);
            } else {
                return if self.update_rows.load(Ordering::SeqCst) == 0 {
                    Ok(None)
                } else {
                    let update_rows = self.update_rows.swap(0, Ordering::SeqCst);
                    Ok(Some(Tuple::new(
                        self.output_schema(),
                        vec![ScalarValue::Int32(Some(update_rows as i32))],
                    )))
                };
            }
        }
    }

    fn output_schema(&self) -> SchemaRef {
        UPDATE_OUTPUT_SCHEMA_REF.clone()
    }
}

impl std::fmt::Display for PhysicalUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Update")
    }
}
