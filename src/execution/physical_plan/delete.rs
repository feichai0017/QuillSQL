use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use crate::catalog::{SchemaRef, DELETE_OUTPUT_SCHEMA_REF};
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

pub struct PhysicalDelete {
    pub table: TableReference,
    pub table_schema: SchemaRef,
    pub predicate: Option<Expr>,
    deleted_rows: AtomicU32,
    iterator: std::sync::Mutex<Option<Box<dyn TupleStream>>>,
    table_binding: OnceLock<TableBinding>,
}

impl PhysicalDelete {
    pub fn new(table: TableReference, table_schema: SchemaRef, predicate: Option<Expr>) -> Self {
        Self {
            table,
            table_schema,
            predicate,
            deleted_rows: AtomicU32::new(0),
            iterator: std::sync::Mutex::new(None),
            table_binding: OnceLock::new(),
        }
    }
}

impl VolcanoExecutor for PhysicalDelete {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        self.deleted_rows.store(0, Ordering::SeqCst);
        context.txn_ctx().ensure_writable(&self.table, "DELETE")?;
        context
            .txn_ctx_mut()
            .lock_table(self.table.clone(), LockMode::IntentionExclusive)?;
        if self.table_binding.get().is_none() {
            let binding = context.table(&self.table)?;
            let _ = self.table_binding.set(binding);
        }
        let binding = self
            .table_binding
            .get()
            .expect("table binding not initialized");
        let stream = binding.scan(ScanOptions::default())?;
        *self.iterator.lock().unwrap() = Some(stream);
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        let Some(iterator) = &mut *self.iterator.lock().unwrap() else {
            return Err(QuillSQLError::Execution(
                "table stream not created".to_string(),
            ));
        };

        loop {
            let Some((rid, meta, tuple)) = iterator.next()? else {
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
                if !context.eval_predicate(predicate, &tuple)? {
                    continue;
                }
            }

            let binding = self
                .table_binding
                .get()
                .expect("table binding not initialized");
            let Some((current_meta, current_tuple)) =
                binding.prepare_row_for_write(context.txn_ctx_mut(), rid, &meta)?
            else {
                continue;
            };
            binding.delete(context.txn_ctx_mut(), rid, current_meta, current_tuple)?;
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

impl std::fmt::Debug for PhysicalDelete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhysicalDelete")
            .field("table", &self.table)
            .field("table_schema", &self.table_schema)
            .field("predicate", &self.predicate)
            .field("deleted_rows", &self.deleted_rows)
            .finish()
    }
}
