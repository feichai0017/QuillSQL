pub mod physical_plan;

use std::sync::Arc;

use crate::catalog::SchemaRef;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::execution::physical_plan::PhysicalPlan;
use crate::expression::{Expr, ExprTrait};
use crate::storage::{
    engine::StorageEngine,
    index::btree_index::BPlusTreeIndex,
    page::{RecordId, TupleMeta},
    table_heap::TableHeap,
    tuple::Tuple,
};
use crate::transaction::{
    CommandId, IsolationLevel, LockManager, RowLockGuard, Transaction, TransactionManager,
    TransactionSnapshot, TxnReadGuard, TxnRuntime,
};
use crate::utils::scalar::ScalarValue;
use crate::{catalog::Catalog, transaction::LockMode, utils::table_ref::TableReference};
use log::warn;
use sqlparser::ast::TransactionAccessMode;
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

pub struct TxnContext<'a> {
    runtime: TxnRuntime<'a>,
    lock_manager: Arc<LockManager>,
}

impl<'a> TxnContext<'a> {
    fn new(manager: &'a TransactionManager, txn: &'a mut Transaction) -> Self {
        Self {
            runtime: TxnRuntime::new(manager, txn),
            lock_manager: manager.lock_manager_arc(),
        }
    }

    pub fn command_id(&self) -> CommandId {
        self.runtime.command_id()
    }

    pub fn snapshot(&self) -> &TransactionSnapshot {
        self.runtime.snapshot()
    }

    pub fn is_visible(&self, meta: &TupleMeta) -> bool {
        self.runtime.is_visible(meta)
    }

    pub fn read_visible_tuple(
        &mut self,
        table: &TableReference,
        rid: RecordId,
        meta: &TupleMeta,
        tuple: Tuple,
    ) -> QuillSQLResult<Option<Tuple>> {
        match self.transaction().isolation_level() {
            IsolationLevel::ReadUncommitted => {
                if self.is_visible(meta) {
                    Ok(Some(tuple))
                } else {
                    Ok(None)
                }
            }
            IsolationLevel::ReadCommitted => {
                let guard = self.acquire_shared_guard(table, rid, false)?;
                let visible = self.is_visible(meta);
                guard.release();
                if visible {
                    Ok(Some(tuple))
                } else {
                    Ok(None)
                }
            }
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                let guard = self.acquire_shared_guard(table, rid, true)?;
                let visible = self.is_visible(meta);
                guard.release();
                if visible {
                    Ok(Some(tuple))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn acquire_shared_guard(
        &mut self,
        table: &TableReference,
        rid: RecordId,
        retain: bool,
    ) -> QuillSQLResult<TxnReadGuard> {
        if !self
            .lock_manager
            .lock_row(self.transaction(), LockMode::Shared, table.clone(), rid)
        {
            return Err(QuillSQLError::Execution(
                "failed to acquire shared row lock".to_string(),
            ));
        }
        if retain {
            self.manager()
                .record_shared_row_lock(self.transaction().id(), table.clone(), rid);
            Ok(TxnReadGuard::Retained)
        } else {
            Ok(TxnReadGuard::Temporary(RowLockGuard::new(
                self.lock_manager.clone(),
                self.transaction().id(),
                table.clone(),
                rid,
            )))
        }
    }

    pub fn lock_row_exclusive(
        &mut self,
        table: &TableReference,
        rid: RecordId,
    ) -> QuillSQLResult<()> {
        if !self
            .runtime_mut()
            .try_lock_row(table.clone(), rid, LockMode::Exclusive)?
        {
            return Err(QuillSQLError::Execution(
                "failed to acquire row exclusive lock".to_string(),
            ));
        }
        Ok(())
    }

    pub fn lock_table(&mut self, table: TableReference, mode: LockMode) -> QuillSQLResult<()> {
        self.runtime_mut().lock_table(table, mode)
    }

    pub fn ensure_writable(&self, table: &TableReference, operation: &str) -> QuillSQLResult<()> {
        if matches!(
            self.transaction().access_mode(),
            TransactionAccessMode::ReadOnly
        ) {
            warn!(
                "read-only txn {} attempted '{}' on {}",
                self.runtime.id(),
                operation,
                table.to_log_string()
            );
            return Err(QuillSQLError::Execution(format!(
                "operation '{}' on table {} is not allowed in READ ONLY transaction",
                operation,
                table.to_log_string()
            )));
        }
        Ok(())
    }

    pub fn isolation_level(&self) -> IsolationLevel {
        self.transaction().isolation_level()
    }

    pub fn transaction(&self) -> &Transaction {
        self.runtime.transaction()
    }

    pub fn transaction_mut(&mut self) -> &mut Transaction {
        self.runtime.transaction_mut()
    }

    pub fn manager(&self) -> &TransactionManager {
        self.runtime.manager()
    }

    pub fn txn_runtime(&self) -> &TxnRuntime<'a> {
        &self.runtime
    }

    pub fn txn_id(&self) -> crate::transaction::TransactionId {
        self.runtime.id()
    }

    pub fn unlock_row(&self, table: &TableReference, rid: RecordId) {
        self.runtime.unlock_row(table, rid);
    }

    fn runtime_mut(&mut self) -> &mut TxnRuntime<'a> {
        &mut self.runtime
    }
}

impl<'a> ExecutionContext<'a> {
    pub fn new(
        catalog: &'a mut Catalog,
        txn: &'a mut Transaction,
        txn_mgr: &'a TransactionManager,
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

    /// Insert a tuple, update all indexes, and register undo state.
    pub fn insert_tuple_with_indexes(
        &mut self,
        table: &TableReference,
        tuple: &Tuple,
    ) -> QuillSQLResult<()> {
        let (table_heap, rid) = self.storage.mvcc_insert(
            self.catalog,
            table,
            tuple,
            self.txn.txn_id(),
            self.txn.command_id(),
        )?;

        let mut index_links = Vec::new();
        for index in self.table_indexes(table)? {
            if let Ok(key_tuple) = tuple.project_with_schema(index.key_schema.clone()) {
                index.insert(&key_tuple, rid)?;
                index_links.push((index.clone(), key_tuple));
            }
        }

        self.txn
            .transaction_mut()
            .push_insert_undo(table_heap, rid, index_links);
        Ok(())
    }

    /// Mark a row deleted via MVCC and enqueue undo data.
    pub fn apply_delete(
        &mut self,
        table_heap: Arc<TableHeap>,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
    ) -> QuillSQLResult<()> {
        self.storage
            .mvcc_delete(&table_heap, rid, self.txn.txn_id(), self.txn.command_id())?;
        self.txn
            .transaction_mut()
            .push_delete_undo(table_heap, rid, prev_meta, prev_tuple);
        Ok(())
    }

    /// Create a new MVCC version, update indexes, and log undo.
    pub fn apply_update(
        &mut self,
        table: &TableReference,
        table_heap: Arc<TableHeap>,
        rid: RecordId,
        new_tuple: Tuple,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
    ) -> QuillSQLResult<()> {
        let (new_rid, _) = self.storage.mvcc_update(
            &table_heap,
            rid,
            new_tuple.clone(),
            self.txn.txn_id(),
            self.txn.command_id(),
        )?;

        let mut new_keys = Vec::new();
        for index in self.table_indexes(table)? {
            if let Ok(new_key_tuple) = new_tuple.project_with_schema(index.key_schema.clone()) {
                index.insert(&new_key_tuple, new_rid)?;
                new_keys.push((index.clone(), new_key_tuple));
            }
        }

        self.txn
            .transaction_mut()
            .push_update_undo(table_heap, rid, new_rid, prev_meta, prev_tuple, new_keys);
        Ok(())
    }

    /// Acquire an exclusive lock and re-check visibility before mutating a row.
    /// Returns the current tuple version if it is still visible.
    pub fn prepare_row_for_write(
        &mut self,
        table: &TableReference,
        rid: crate::storage::page::RecordId,
        table_heap: &Arc<TableHeap>,
        observed_meta: &crate::storage::page::TupleMeta,
    ) -> QuillSQLResult<Option<(crate::storage::page::TupleMeta, Tuple)>> {
        if !self.txn.is_visible(observed_meta) {
            return Ok(None);
        }
        self.txn.lock_row_exclusive(table, rid)?;
        let (current_meta, current_tuple) = table_heap.full_tuple(rid)?;
        if !self.txn.is_visible(&current_meta) {
            self.txn.unlock_row(table, rid);
            return Ok(None);
        }
        Ok(Some((current_meta, current_tuple)))
    }

    /// Look up the table heap through the storage engine.
    pub fn table_heap(&self, table: &TableReference) -> QuillSQLResult<Arc<TableHeap>> {
        self.storage.table_heap(self.catalog, table)
    }

    /// Fetch all indexes defined on a table.
    pub fn table_indexes(
        &self,
        table: &TableReference,
    ) -> QuillSQLResult<Vec<Arc<BPlusTreeIndex>>> {
        self.storage.table_indexes(self.catalog, table)
    }

    pub fn txn_ctx(&self) -> &TxnContext<'a> {
        &self.txn
    }

    pub fn txn_ctx_mut(&mut self) -> &mut TxnContext<'a> {
        &mut self.txn
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
