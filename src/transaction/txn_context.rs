use std::sync::Arc;

use crate::error::QuillSQLError;
use crate::storage::{
    page::{RecordId, TupleMeta},
    tuple::Tuple,
};
use crate::transaction::{
    CommandId, IsolationLevel, LockManager, LockMode, RowLockGuard, Transaction, TransactionId,
    TransactionManager, TransactionSnapshot, TxnReadGuard, TxnRuntime,
};
use crate::utils::table_ref::TableReference;
use log::warn;
use sqlparser::ast::TransactionAccessMode;

pub struct TxnContext<'a> {
    runtime: TxnRuntime<'a>,
    lock_manager: Arc<LockManager>,
}

impl<'a> TxnContext<'a> {
    pub fn new(manager: Arc<TransactionManager>, txn: &'a mut Transaction) -> Self {
        let lock_manager = manager.lock_manager_arc();
        Self {
            runtime: TxnRuntime::new(manager, txn),
            lock_manager,
        }
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
    ) -> crate::error::QuillSQLResult<Option<Tuple>> {
        match self.transaction().isolation_level() {
            crate::transaction::IsolationLevel::ReadUncommitted => {
                if self.is_visible(meta) {
                    Ok(Some(tuple))
                } else {
                    Ok(None)
                }
            }
            _ => {
                let guard = self.acquire_shared_guard(table, rid)?;
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

    pub fn acquire_shared_guard(
        &mut self,
        table: &TableReference,
        rid: RecordId,
    ) -> crate::error::QuillSQLResult<TxnReadGuard> {
        if !self
            .lock_manager
            .lock_row(self.transaction(), LockMode::Shared, table.clone(), rid)
        {
            return Err(QuillSQLError::Execution(
                "failed to acquire shared row lock".to_string(),
            ));
        }
        Ok(TxnReadGuard::Temporary(RowLockGuard::new(
            self.lock_manager.clone(),
            self.transaction().id(),
            table.clone(),
            rid,
        )))
    }

    pub fn lock_row_exclusive(
        &mut self,
        table: &TableReference,
        rid: RecordId,
    ) -> crate::error::QuillSQLResult<()> {
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

    pub fn lock_table(
        &mut self,
        table: TableReference,
        mode: LockMode,
    ) -> crate::error::QuillSQLResult<()> {
        self.runtime_mut().lock_table(table, mode)
    }

    pub fn ensure_writable(
        &self,
        table: &TableReference,
        operation: &str,
    ) -> crate::error::QuillSQLResult<()> {
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

    pub fn txn_id(&self) -> TransactionId {
        self.runtime.id()
    }

    pub fn unlock_row(&self, table: &TableReference, rid: RecordId) {
        self.runtime.unlock_row(table, rid);
    }

    pub fn command_id(&self) -> CommandId {
        self.runtime.command_id()
    }

    fn runtime_mut(&mut self) -> &mut TxnRuntime<'a> {
        &mut self.runtime
    }
}
