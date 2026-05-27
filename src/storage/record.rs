use std::fmt::{Display, Formatter};

use crate::transaction::{CommandId, TransactionId, INVALID_COMMAND_ID};

pub type PageId = u32;
pub const INVALID_PAGE_ID: PageId = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleMeta {
    pub insert_txn_id: TransactionId,
    pub insert_cid: CommandId,
    pub delete_txn_id: TransactionId,
    pub delete_cid: CommandId,
    pub is_deleted: bool,
    pub next_version: Option<RecordId>,
    pub prev_version: Option<RecordId>,
}

impl TupleMeta {
    pub fn new(insert_txn_id: TransactionId, insert_cid: CommandId) -> Self {
        Self {
            insert_txn_id,
            insert_cid,
            delete_txn_id: 0,
            delete_cid: INVALID_COMMAND_ID,
            is_deleted: false,
            next_version: None,
            prev_version: None,
        }
    }

    pub fn mark_deleted(&mut self, txn_id: TransactionId, delete_cid: CommandId) {
        self.is_deleted = true;
        self.delete_txn_id = txn_id;
        self.delete_cid = delete_cid;
    }

    pub fn clear_delete(&mut self) {
        self.is_deleted = false;
        self.delete_txn_id = 0;
        self.delete_cid = INVALID_COMMAND_ID;
    }

    pub fn set_next_version(&mut self, next: Option<RecordId>) {
        self.next_version = next;
    }

    pub fn set_prev_version(&mut self, prev: Option<RecordId>) {
        self.prev_version = prev;
    }

    pub fn clear_chain(&mut self) {
        self.next_version = None;
        self.prev_version = None;
    }
}

pub const EMPTY_TUPLE_META: TupleMeta = TupleMeta {
    insert_txn_id: 0,
    insert_cid: 0,
    delete_txn_id: 0,
    delete_cid: INVALID_COMMAND_ID,
    is_deleted: false,
    next_version: None,
    prev_version: None,
};

pub const INVALID_RID: RecordId = RecordId {
    page_id: INVALID_PAGE_ID,
    slot_num: 0,
};

#[derive(derive_new::new, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecordId {
    pub page_id: PageId,
    pub slot_num: u32,
}

impl Display for RecordId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.page_id, self.slot_num)
    }
}
