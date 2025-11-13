use crate::catalog::{SchemaRef, UPDATE_OUTPUT_SCHEMA_REF};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::expression::Expr;
use crate::storage::{
    engine::{ScanOptions, TableBinding, TupleStream},
    tuple::Tuple,
};
use crate::transaction::LockMode;
use crate::utils::scalar::ScalarValue;
use crate::utils::table_ref::TableReference;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use std::sync::OnceLock;

pub struct PhysicalUpdate {
    pub table: TableReference,
    pub table_schema: SchemaRef,
    pub assignments: HashMap<String, Expr>,
    pub selection: Option<Expr>,

    update_rows: AtomicU32,
    table_iterator: Mutex<Option<Box<dyn TupleStream>>>,
    table_binding: OnceLock<TableBinding>,
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
            table_binding: OnceLock::new(),
        }
    }
}

impl VolcanoExecutor for PhysicalUpdate {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        self.update_rows.store(0, Ordering::SeqCst);
        context.txn_ctx().ensure_writable(&self.table, "UPDATE")?;
        if self.table_binding.get().is_none() {
            let binding = context.table(&self.table)?;
            let _ = self.table_binding.set(binding);
        }
        let binding = self
            .table_binding
            .get()
            .expect("table binding not initialized");
        let stream = binding.scan(ScanOptions::default())?;
        *self.table_iterator.lock().unwrap() = Some(stream);
        context
            .txn_ctx_mut()
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
                "table stream not created".to_string(),
            ));
        };
        loop {
            if let Some((rid, meta, tuple)) = table_iterator.next()? {
                // Skip versions that were created by this command so we do not
                // immediately reprocess the freshly inserted MVCC tuple and loop forever.
                if meta.insert_txn_id == context.txn_ctx().txn_id()
                    && meta.insert_cid == context.txn_ctx().command_id()
                {
                    continue;
                }

                if let Some(selection) = &self.selection {
                    if !context.eval_predicate(selection, &tuple)? {
                        continue;
                    }
                }

                let binding = self
                    .table_binding
                    .get()
                    .expect("table binding not initialized");
                let Some((prev_meta, mut current_tuple)) =
                    binding.prepare_row_for_write(context.txn_ctx_mut(), rid, &meta)?
                else {
                    continue;
                };
                let prev_tuple = current_tuple.clone();
                let mut eval_tuple = current_tuple.clone();

                // update tuple data
                for (col_name, value_expr) in self.assignments.iter() {
                    let index = current_tuple.schema.index_of(None, col_name)?;
                    let col_datatype = current_tuple.schema.columns[index].data_type;
                    // use the updated value for subsequent expressions in this update
                    // e.g., SET a = 1, b = a + 1
                    // should set b to 2
                    let new_value = context
                        .eval_expr(value_expr, &eval_tuple)?
                        .cast_to(&col_datatype)?;
                    current_tuple.data[index] = new_value.clone();
                    eval_tuple.data[index] = new_value;
                }
                binding.update(
                    context.txn_ctx_mut(),
                    rid,
                    current_tuple.clone(),
                    prev_meta,
                    prev_tuple,
                )?;

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

impl std::fmt::Debug for PhysicalUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhysicalUpdate")
            .field("table", &self.table)
            .field("table_schema", &self.table_schema)
            .field("assignments", &self.assignments)
            .field("selection", &self.selection)
            .field("update_rows", &self.update_rows)
            .finish()
    }
}
