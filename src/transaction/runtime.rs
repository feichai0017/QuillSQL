use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::page::TupleMeta;
use crate::transaction::{
    IsolationLevel, LockMode, Transaction, TransactionManager, TransactionSnapshot,
};
use crate::utils::table_ref::TableReference;

use crate::storage::page::RecordId;

/// Runtime wrapper that bundles a transaction handle with its manager, snapshot, and command id.
/// Execution operators interact with this struct instead of juggling raw references, which keeps
/// MVCC/2PL details inside the transaction layer.
pub struct TxnRuntime<'a> {
    txn: &'a mut Transaction,
    manager: &'a TransactionManager,
    snapshot: TransactionSnapshot,
    command_id: crate::transaction::CommandId,
}

impl<'a> TxnRuntime<'a> {
    pub fn new(manager: &'a TransactionManager, txn: &'a mut Transaction) -> Self {
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
        }
    }

    pub fn id(&self) -> crate::transaction::TransactionId {
        self.txn.id()
    }

    pub fn command_id(&self) -> crate::transaction::CommandId {
        self.command_id
    }

    pub fn snapshot(&self) -> &TransactionSnapshot {
        &self.snapshot
    }

    pub fn transaction(&self) -> &Transaction {
        self.txn
    }

    pub fn transaction_mut(&mut self) -> &mut Transaction {
        self.txn
    }

    pub fn manager(&self) -> &TransactionManager {
        self.manager
    }

    pub fn is_visible(&self, meta: &TupleMeta) -> bool {
        self.snapshot.is_visible(meta, self.command_id, |txn_id| {
            self.manager.transaction_status(txn_id)
        })
    }

    pub fn lock_table(&self, table: TableReference, mode: LockMode) -> QuillSQLResult<()> {
        self.manager
            .acquire_table_lock(self.txn, table.clone(), mode)
            .map_err(|e| QuillSQLError::Execution(format!("lock error: {}", e)))
    }

    pub fn try_lock_row(
        &self,
        table: TableReference,
        rid: RecordId,
        mode: LockMode,
    ) -> QuillSQLResult<bool> {
        self.manager
            .try_acquire_row_lock(self.txn, table, rid, mode)
    }

    pub fn record_shared_row_lock(&self, table: TableReference, rid: RecordId) {
        self.manager
            .record_shared_row_lock(self.txn.id(), table, rid);
    }

    pub fn remove_row_key_marker(&self, table: &TableReference, rid: RecordId) {
        self.manager
            .remove_row_key_marker(self.txn.id(), table, rid);
    }

    pub fn try_unlock_shared_row(
        &self,
        table: &TableReference,
        rid: RecordId,
    ) -> QuillSQLResult<()> {
        self.manager
            .try_unlock_shared_row(self.txn.id(), table, rid)
    }

    pub fn unlock_row(&self, table: &TableReference, rid: RecordId) {
        self.manager.unlock_row(self.txn.id(), table, rid);
    }
}
