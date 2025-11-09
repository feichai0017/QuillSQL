pub mod physical_plan;

use std::sync::Arc;

use crate::catalog::{Schema, SchemaRef};
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
    CommandId, IsolationLevel, Transaction, TransactionManager, TransactionSnapshot, TxnRuntime,
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
    txn: TxnRuntime<'a>,
}

impl<'a> ExecutionContext<'a> {
    pub fn new(
        catalog: &'a mut Catalog,
        txn: &'a mut Transaction,
        txn_mgr: &'a TransactionManager,
        storage: Arc<dyn StorageEngine>,
    ) -> Self {
        let runtime = TxnRuntime::new(txn_mgr, txn);
        Self {
            catalog,
            storage,
            txn: runtime,
        }
    }

    pub fn command_id(&self) -> CommandId {
        self.txn.command_id()
    }

    pub fn snapshot(&self) -> &TransactionSnapshot {
        self.txn.snapshot()
    }

    pub fn is_visible(&self, meta: &crate::storage::page::TupleMeta) -> bool {
        self.txn.is_visible(meta)
    }

    /// Perform MVCC visibility checks (and shared locks) based on the current
    /// isolation level, returning the tuple only if it is visible.
    pub fn read_visible_tuple(
        &mut self,
        table: &TableReference,
        rid: crate::storage::page::RecordId,
        meta: &crate::storage::page::TupleMeta,
        tuple: Tuple,
    ) -> QuillSQLResult<Option<Tuple>> {
        match self.txn().isolation_level() {
            IsolationLevel::ReadUncommitted => {
                if self.is_visible(meta) {
                    Ok(Some(tuple))
                } else {
                    Ok(None)
                }
            }
            IsolationLevel::ReadCommitted => {
                self.lock_row_shared(table, rid, false)?;
                let visible = self.is_visible(meta);
                self.unlock_row_shared(table, rid)?;
                if visible {
                    Ok(Some(tuple))
                } else {
                    Ok(None)
                }
            }
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                self.lock_row_shared(table, rid, true)?;
                let visible = self.is_visible(meta);
                self.unlock_row_shared(table, rid)?;
                if visible {
                    Ok(Some(tuple))
                } else {
                    Ok(None)
                }
            }
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
            self.txn_id(),
            self.command_id(),
        )?;

        let mut index_links = Vec::new();
        for index in self.table_indexes(table)? {
            if let Ok(key_tuple) = tuple.project_with_schema(index.key_schema.clone()) {
                index.insert(&key_tuple, rid)?;
                index_links.push((index.clone(), key_tuple));
            }
        }

        self.txn_mut()
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
            .mvcc_delete(&table_heap, rid, self.txn_id(), self.command_id())?;
        self.txn_mut()
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
            self.txn_id(),
            self.command_id(),
        )?;

        let mut new_keys = Vec::new();
        for index in self.table_indexes(table)? {
            if let Ok(new_key_tuple) = new_tuple.project_with_schema(index.key_schema.clone()) {
                index.insert(&new_key_tuple, new_rid)?;
                new_keys.push((index.clone(), new_key_tuple));
            }
        }

        self.txn_mut()
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
        if !self.is_visible(observed_meta) {
            return Ok(None);
        }
        self.lock_row_exclusive(table, rid)?;
        let (current_meta, current_tuple) = table_heap.full_tuple(rid)?;
        if !self.is_visible(&current_meta) {
            self.unlock_row(table, rid);
            return Ok(None);
        }
        Ok(Some((current_meta, current_tuple)))
    }

    pub fn lock_table(&mut self, table: TableReference, mode: LockMode) -> QuillSQLResult<()> {
        self.txn.lock_table(table, mode)
    }

    pub fn lock_row_shared(
        &mut self,
        table: &TableReference,
        rid: crate::storage::page::RecordId,
        retain: bool,
    ) -> QuillSQLResult<()> {
        let acquired = self
            .txn
            .try_lock_row(table.clone(), rid, LockMode::Shared)?;
        if !acquired {
            return Err(QuillSQLError::Execution(
                "failed to acquire shared row lock".to_string(),
            ));
        }
        if retain {
            self.txn.record_shared_row_lock(table.clone(), rid);
        } else {
            // Track transient shared locks so subsequent attempts still go through the lock manager.
            self.txn.remove_row_key_marker(table, rid);
        }
        Ok(())
    }

    pub fn unlock_row_shared(
        &mut self,
        table: &TableReference,
        rid: crate::storage::page::RecordId,
    ) -> QuillSQLResult<()> {
        self.txn.try_unlock_shared_row(table, rid)
    }

    pub fn lock_row_exclusive(
        &mut self,
        table: &TableReference,
        rid: crate::storage::page::RecordId,
    ) -> QuillSQLResult<()> {
        if !self
            .txn
            .try_lock_row(table.clone(), rid, LockMode::Exclusive)?
        {
            return Err(QuillSQLError::Execution(
                "failed to acquire row exclusive lock".to_string(),
            ));
        }
        Ok(())
    }

    /// Ensure that the current transaction is allowed to perform a write on the given table.
    pub fn ensure_writable(&self, table: &TableReference, operation: &str) -> QuillSQLResult<()> {
        if matches!(
            self.txn.transaction().access_mode(),
            TransactionAccessMode::ReadOnly
        ) {
            warn!(
                "read-only txn {} attempted '{}' on {}",
                self.txn.id(),
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

    pub fn txn(&self) -> &Transaction {
        self.txn.transaction()
    }

    pub fn txn_mut(&mut self) -> &mut Transaction {
        self.txn.transaction_mut()
    }

    pub fn txn_manager(&self) -> &TransactionManager {
        self.txn.manager()
    }

    pub fn txn_runtime(&self) -> &TxnRuntime<'a> {
        &self.txn
    }

    pub fn txn_id(&self) -> crate::transaction::TransactionId {
        self.txn.id()
    }

    pub fn unlock_row(&self, table: &TableReference, rid: crate::storage::page::RecordId) {
        self.txn.unlock_row(table, rid);
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

    /// Non-allocating helper used by DDL to test for table existence.
    pub fn try_table_heap(&self, table: &TableReference) -> Option<Arc<TableHeap>> {
        self.catalog.try_table_heap(table)
    }

    /// Create a table (used by the CREATE TABLE physical operator).
    pub fn create_table(
        &mut self,
        table: TableReference,
        schema: Arc<Schema>,
    ) -> QuillSQLResult<()> {
        self.catalog.create_table(table, schema).map(|_| ())
    }

    /// Drop a table, returning whether it existed.
    pub fn drop_table(&mut self, table: &TableReference) -> QuillSQLResult<bool> {
        self.catalog.drop_table(table)
    }

    /// Create an index (used by CREATE INDEX).
    pub fn create_index(
        &mut self,
        name: String,
        table: &TableReference,
        key_schema: Arc<Schema>,
    ) -> QuillSQLResult<()> {
        self.catalog
            .create_index(name, table, key_schema)
            .map(|_| ())
    }

    /// Drop an index, returning whether it existed.
    pub fn drop_index(&mut self, table: &TableReference, name: &str) -> QuillSQLResult<bool> {
        self.catalog.drop_index(table, name)
    }

    /// Resolve the table that owns `catalog.schema.index`.
    pub fn find_index_owner(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        name: &str,
    ) -> Option<TableReference> {
        self.catalog.find_index_owner(catalog, schema, name)
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
