pub mod physical_plan;
use crate::catalog::SchemaRef;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::execution::physical_plan::PhysicalPlan;
use crate::expression::{Expr, ExprTrait};
use crate::storage::{
    engine::{IndexHandle, StorageEngine, TableHandle},
    page::{RecordId, TupleMeta},
    table_heap::TableHeap,
    tuple::Tuple,
};
use crate::transaction::{Transaction, TransactionManager, TxnContext};
use crate::utils::scalar::ScalarValue;
use crate::{catalog::Catalog, utils::table_ref::TableReference};
use std::sync::Arc;

pub trait VolcanoExecutor {
    fn init(&self, _context: &mut ExecutionContext) -> QuillSQLResult<()> {
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>>;

    fn output_schema(&self) -> SchemaRef;
}

/// Shared state threaded through every physical operator during execution.
/// Exposes MVCC helpers, storage access, expression evaluation and DDL utilities.
pub struct ExecutionContext<'a> {
    /// Mutable reference to the global catalog (schema + metadata).
    pub catalog: &'a mut Catalog,
    /// Pluggable storage engine used for heap/index access.
    storage: Arc<dyn StorageEngine>,
    /// Transaction runtime wrapper (snapshot, locks, undo tracking).
    txn: TxnContext<'a>,
}

impl<'a> ExecutionContext<'a> {
    pub fn new(
        catalog: &'a mut Catalog,
        txn: &'a mut Transaction,
        txn_mgr: Arc<TransactionManager>,
        storage: Arc<dyn StorageEngine>,
    ) -> Self {
        Self {
            catalog,
            storage,
            txn: TxnContext::new(txn_mgr, txn),
        }
    }

    /// Evaluate an expression expected to produce a boolean result.
    pub fn eval_predicate(&self, expr: &Expr, tuple: &Tuple) -> QuillSQLResult<bool> {
        match expr.evaluate(tuple)? {
            ScalarValue::Boolean(Some(v)) => Ok(v),
            ScalarValue::Boolean(None) => Ok(false),
            other => Err(QuillSQLError::Execution(format!(
                "predicate value must be boolean, got {}",
                other
            ))),
        }
    }

    /// Evaluate an arbitrary scalar expression.
    pub fn eval_expr(&self, expr: &Expr, tuple: &Tuple) -> QuillSQLResult<ScalarValue> {
        expr.evaluate(tuple)
    }

    /// Look up the table heap through the storage engine.
    pub fn table_handle(&self, table: &TableReference) -> QuillSQLResult<Arc<dyn TableHandle>> {
        self.storage.table(self.catalog, table)
    }

    pub fn table_heap(&self, table: &TableReference) -> QuillSQLResult<Arc<TableHeap>> {
        Ok(self.table_handle(table)?.table_heap())
    }

    /// Fetch all indexes defined on a table.
    pub fn table_indexes(
        &self,
        table: &TableReference,
    ) -> QuillSQLResult<Vec<Arc<dyn IndexHandle>>> {
        self.storage.indexes(self.catalog, table)
    }

    pub fn txn_ctx(&self) -> &TxnContext<'a> {
        &self.txn
    }

    pub fn txn_ctx_mut(&mut self) -> &mut TxnContext<'a> {
        &mut self.txn
    }

    pub fn insert_tuple_with_indexes(
        &mut self,
        table: &TableReference,
        tuple: &Tuple,
    ) -> QuillSQLResult<()> {
        let handle = self.table_handle(table)?;
        let indexes = self.table_indexes(table)?;
        handle.insert(self.txn_ctx_mut(), tuple, &indexes)
    }

    pub fn apply_delete(
        &mut self,
        table: &TableReference,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
    ) -> QuillSQLResult<()> {
        let handle = self.table_handle(table)?;
        handle.delete(self.txn_ctx_mut(), rid, prev_meta, prev_tuple)
    }

    pub fn apply_update(
        &mut self,
        table: &TableReference,
        rid: RecordId,
        new_tuple: Tuple,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
    ) -> QuillSQLResult<RecordId> {
        let handle = self.table_handle(table)?;
        let indexes = self.table_indexes(table)?;
        handle.update(
            self.txn_ctx_mut(),
            rid,
            new_tuple,
            prev_meta,
            prev_tuple,
            &indexes,
        )
    }

    pub fn prepare_row_for_write(
        &mut self,
        table: &TableReference,
        rid: RecordId,
        observed_meta: &TupleMeta,
    ) -> QuillSQLResult<Option<(TupleMeta, Tuple)>> {
        let handle = self.table_handle(table)?;
        handle.prepare_row_for_write(self.txn_ctx_mut(), rid, observed_meta)
    }
}

pub struct ExecutionEngine<'a> {
    pub context: ExecutionContext<'a>,
}
impl<'a> ExecutionEngine<'a> {
    pub fn execute(&mut self, plan: Arc<PhysicalPlan>) -> QuillSQLResult<Vec<Tuple>> {
        plan.init(&mut self.context)?;
        let mut result = Vec::new();
        loop {
            let next_tuple = plan.next(&mut self.context)?;
            if let Some(tuple) = next_tuple {
                result.push(tuple);
            } else {
                break;
            }
        }
        Ok(result)
    }
}
