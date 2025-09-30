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
use std::collections::HashSet;
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
        let table_heap = context.catalog.table_heap(&self.table)?;
        *self.table_iterator.lock().unwrap() = Some(TableIterator::new(table_heap.clone(), ..));
        context
            .txn_mgr
            .acquire_table_lock(
                context.txn,
                self.table.clone(),
                LockMode::IntentionExclusive,
            )
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
            if let Some((rid, mut tuple)) = table_iterator.next()? {
                if let Some(selection) = &self.selection {
                    if !selection.evaluate(&tuple)?.as_boolean()?.unwrap_or(false) {
                        continue;
                    }
                }

                let meta = table_heap.tuple_meta(rid)?;
                if !Tuple::is_visible(&meta, context.txn.id()) {
                    continue;
                }

                context.lock_row_exclusive(&self.table, rid)?;

                let prev_tuple = tuple.clone();
                let prev_meta = meta;

                // update tuple data
                for (col_name, value_expr) in self.assignments.iter() {
                    let index = tuple.schema.index_of(None, col_name)?;
                    let col_datatype = tuple.schema.columns[index].data_type;
                    let new_value = value_expr.evaluate(&EMPTY_TUPLE)?.cast_to(&col_datatype)?;
                    tuple.data[index] = new_value;
                }
                table_heap.update_tuple(rid, tuple.clone())?;

                let mut new_keys = Vec::new();
                let mut old_keys = Vec::new();
                let changed_cols: HashSet<&str> =
                    self.assignments.keys().map(|s| s.as_str()).collect();
                let indexes = context.catalog.table_indexes(&self.table)?;
                for index in indexes {
                    // Skip indexes whose key columns are not affected by this update
                    let affected = index
                        .key_schema
                        .columns
                        .iter()
                        .any(|c| changed_cols.contains(c.name.as_str()));
                    if !affected {
                        continue;
                    }

                    let old_key = prev_tuple
                        .project_with_schema(index.key_schema.clone())
                        .ok();
                    let new_key = tuple.project_with_schema(index.key_schema.clone()).ok();

                    // Skip maintenance when key values are unchanged
                    if let (Some(ref ok), Some(ref nk)) = (&old_key, &new_key) {
                        if ok.data == nk.data {
                            continue;
                        }
                    }

                    if let Some(old_key_tuple) = old_key {
                        index.delete(&old_key_tuple)?;
                        old_keys.push((index.clone(), old_key_tuple));
                    }
                    if let Some(new_key_tuple) = new_key {
                        index.insert(&new_key_tuple, rid)?;
                        new_keys.push((index.clone(), new_key_tuple));
                    }
                }

                context.txn.push_update_undo(
                    table_heap.clone(),
                    rid,
                    prev_meta,
                    prev_tuple,
                    new_keys,
                    old_keys,
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
