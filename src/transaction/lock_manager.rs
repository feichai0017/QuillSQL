use crate::storage::page::RecordId;
use crate::transaction::{Transaction, TransactionId};
use crate::utils::table_ref::TableReference;
use std::collections::HashMap;

#[derive(Debug)]
pub enum LockMode {
    Shared,
    Exclusive,
    IntentionShared,
    IntentionExclusive,
    SharedIntentionExclusive,
}

pub struct LockRequest {
    txn_id: TransactionId,
    lock_mod: LockMode,
    table_ref: TableReference,
    rid: Option<RecordId>,
    granted: bool,
}

pub struct LockManager {
    table_lock_map: HashMap<TableReference, Vec<LockRequest>>,
    row_lock_map: HashMap<RecordId, Vec<LockRequest>>,
}

impl LockManager {
    pub fn lock_table(
        &self,
        _txn: &Transaction,
        _mode: LockMode,
        _table_ref: TableReference,
    ) -> bool {
        todo!()
    }

    pub fn unlock_table(&self, _txn: &Transaction, _table_ref: TableReference) -> bool {
        todo!()
    }

    pub fn lock_row(
        &self,
        _txn: &Transaction,
        _mode: LockMode,
        _table_ref: TableReference,
        _rid: RecordId,
    ) -> bool {
        todo!()
    }

    pub fn unlock_row(
        &self,
        _txn: &Transaction,
        _table_ref: TableReference,
        _rid: RecordId,
        _force: bool,
    ) -> bool {
        todo!()
    }

    pub fn unlock_all(&self) {
        todo!()
    }
}
