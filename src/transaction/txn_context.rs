use std::sync::Arc;

use crate::error::QuillSQLError;
use crate::storage::{
    page::{RecordId, TupleMeta},
    tuple::Tuple,
};
use crate::transaction::{
    CommandId, IsolationLevel, LockManager, LockMode, RowLockGuard, Transaction, TransactionId,
    TransactionManager, TransactionSnapshot, TxnReadGuard,
};
use crate::utils::table_ref::TableReference;
use log::warn;
use sqlparser::ast::TransactionAccessMode;

pub struct TxnContext<'a> {
    txn: &'a mut Transaction,
    manager: Arc<TransactionManager>,
    snapshot: TransactionSnapshot,
    command_id: CommandId,
    lock_manager: Arc<LockManager>,
}

impl<'a> TxnContext<'a> {
    pub fn new(manager: Arc<TransactionManager>, txn: &'a mut Transaction) -> Self {
        let lock_manager = manager.lock_manager_arc();
        let command_id = txn.begin_command();
        let snapshot = match txn.isolation_level() {
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                if let Some(existing) = txn.snapshot().cloned() {
                    existing
                } else {
                    let snap = manager.snapshot(txn.id());
                    txn.set_snapshot(snap.clone());
                    snap
                }
            }
            IsolationLevel::ReadCommitted | IsolationLevel::ReadUncommitted => {
                let snap = manager.snapshot(txn.id());
                txn.clear_snapshot();
                snap
            }
        };
        Self {
            txn,
            manager,
            snapshot,
            command_id,
            lock_manager,
        }
    }

    pub fn snapshot(&self) -> &TransactionSnapshot {
        &self.snapshot
    }

    pub fn is_visible(&self, meta: &TupleMeta) -> bool {
        self.snapshot.is_visible(meta, self.command_id, |txn_id| {
            self.manager.transaction_status(txn_id)
        })
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
            .manager
            .try_acquire_row_lock(self.txn, table.clone(), rid, LockMode::Exclusive)?
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
        self.manager
            .acquire_table_lock(self.txn, table.clone(), mode)
            .map_err(|e| QuillSQLError::Execution(format!("lock error: {}", e)))
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

    pub fn isolation_level(&self) -> IsolationLevel {
        self.txn.isolation_level()
    }

    pub fn transaction(&self) -> &Transaction {
        self.txn
    }

    pub fn transaction_mut(&mut self) -> &mut Transaction {
        self.txn
    }

    pub fn manager(&self) -> &TransactionManager {
        self.manager.as_ref()
    }

    pub fn txn_id(&self) -> TransactionId {
        self.txn.id()
    }

    pub fn unlock_row(&self, table: &TableReference, rid: RecordId) {
        self.manager.unlock_row(self.txn.id(), table, rid);
    }

    pub fn command_id(&self) -> CommandId {
        self.command_id
    }
}
