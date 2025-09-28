use crate::recovery::Lsn;

pub type TransactionId = u64;
pub const INVALID_TRANSACTION_ID: TransactionId = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    SnapshotIsolation,
    Serializable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Running,
    Tainted,
    Committed,
    Aborted,
}

#[derive(Debug, Clone)]
pub struct Transaction {
    id: TransactionId,
    isolation_level: IsolationLevel,
    state: TransactionState,
    synchronous_commit: bool,
    begin_lsn: Option<Lsn>,
    last_lsn: Option<Lsn>,
}

impl Transaction {
    pub fn new(
        id: TransactionId,
        isolation_level: IsolationLevel,
        synchronous_commit: bool,
    ) -> Self {
        Self {
            id,
            isolation_level,
            state: TransactionState::Running,
            synchronous_commit,
            begin_lsn: None,
            last_lsn: None,
        }
    }

    pub fn id(&self) -> TransactionId {
        self.id
    }

    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    pub fn state(&self) -> TransactionState {
        self.state
    }

    pub fn synchronous_commit(&self) -> bool {
        self.synchronous_commit
    }

    pub fn begin_lsn(&self) -> Option<Lsn> {
        self.begin_lsn
    }

    pub fn last_lsn(&self) -> Option<Lsn> {
        self.last_lsn
    }

    pub(crate) fn set_begin_lsn(&mut self, lsn: Lsn) {
        self.begin_lsn = Some(lsn);
        self.last_lsn = Some(lsn);
    }

    pub(crate) fn record_lsn(&mut self, lsn: Lsn) {
        self.last_lsn = Some(lsn);
    }

    pub(crate) fn set_state(&mut self, state: TransactionState) {
        self.state = state;
    }

    pub(crate) fn mark_tainted(&mut self) {
        self.state = TransactionState::Tainted;
    }
}

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
    // TODO: replace Tuple with a more compact redo/undo representation.
    tuple: crate::storage::tuple::Tuple,
    timestamp: u64,
    prev_version: UndoLink,
}
