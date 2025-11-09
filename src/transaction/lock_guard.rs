use std::sync::Arc;

use crate::storage::page::RecordId;
use crate::transaction::{LockManager, TransactionId};
use crate::utils::table_ref::TableReference;

pub struct RowLockGuard {
    manager: Arc<LockManager>,
    txn_id: TransactionId,
    table: TableReference,
    rid: RecordId,
    released: bool,
}

impl RowLockGuard {
    pub fn new(
        manager: Arc<LockManager>,
        txn_id: TransactionId,
        table: TableReference,
        rid: RecordId,
    ) -> Self {
        Self {
            manager,
            txn_id,
            table,
            rid,
            released: false,
        }
    }

    pub fn release(mut self) {
        self.do_release();
    }

    fn do_release(&mut self) {
        if !self.released {
            let _ = self
                .manager
                .unlock_row_raw(self.txn_id, self.table.clone(), self.rid);
            self.released = true;
        }
    }
}

impl Drop for RowLockGuard {
    fn drop(&mut self) {
        self.do_release();
    }
}

pub enum TxnReadGuard {
    Temporary(RowLockGuard),
    Retained,
}

impl TxnReadGuard {
    pub fn release(self) {
        if let TxnReadGuard::Temporary(guard) = self {
            guard.release();
        }
    }
}
