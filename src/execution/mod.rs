pub mod physical_plan;

use std::sync::Arc;

use crate::catalog::SchemaRef;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::execution::physical_plan::PhysicalPlan;
use crate::transaction::{
    CommandId, Transaction, TransactionManager, TransactionSnapshot, TxnRuntime,
};
use crate::{
    catalog::Catalog, storage::tuple::Tuple, transaction::LockMode,
    utils::table_ref::TableReference,
};
use log::warn;
use sqlparser::ast::TransactionAccessMode;
pub trait VolcanoExecutor {
    fn init(&self, _context: &mut ExecutionContext) -> QuillSQLResult<()> {
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>>;

    fn output_schema(&self) -> SchemaRef;
}

pub struct ExecutionContext<'a> {
    pub catalog: &'a mut Catalog,
    txn: TxnRuntime<'a>,
}

impl<'a> ExecutionContext<'a> {
    pub fn new(
        catalog: &'a mut Catalog,
        txn: &'a mut Transaction,
        txn_mgr: &'a TransactionManager,
    ) -> Self {
        let runtime = TxnRuntime::new(txn_mgr, txn);
        Self {
            catalog,
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
