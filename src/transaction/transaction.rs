use crate::storage::tuple::Tuple;

pub type TransactionId = u64;
pub const INVALID_TRANSACTION_ID: TransactionId = 0;

pub enum TransactionState {
    Running,
    Tainted,
    Committed,
    Aborted,
}

pub struct Transaction {}

/// Represents a link to a previous version of this tuple
pub struct UndoLink {
    prev_txn: TransactionId,
    prev_log_idx: u32,
}

impl UndoLink {
    pub fn is_valid(&self) -> bool {
        self.prev_txn != INVALID_TRANSACTION_ID
    }
}

pub struct UndoLog {
    is_deleted: bool,
    modified_fields: Vec<bool>,
    tuple: Tuple,
    timestamp: u64,
    prev_version: UndoLink,
}
