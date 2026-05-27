use crate::error::QuillSQLResult;
use crate::recovery::Lsn;
use crate::storage::heap::wal_codec::HeapRecordPayload;
use crate::storage::page::{RecordId, TupleMeta};
use crate::storage::tuple::Tuple;
use crate::storage::{IndexHandle, TableHandle};
use sqlparser::ast::TransactionAccessMode;
use std::str::FromStr;
use std::sync::Arc;

use super::TransactionSnapshot;

pub type TransactionId = u64;
pub type CommandId = u32;

pub const INVALID_COMMAND_ID: CommandId = CommandId::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl IsolationLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "read-uncommitted",
            IsolationLevel::ReadCommitted => "read-committed",
            IsolationLevel::RepeatableRead => "repeatable-read",
            IsolationLevel::Serializable => "serializable",
        }
    }
}

impl FromStr for IsolationLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "read-uncommitted" | "ru" => Ok(IsolationLevel::ReadUncommitted),
            "read-committed" | "rc" => Ok(IsolationLevel::ReadCommitted),
            "repeatable-read" | "rr" => Ok(IsolationLevel::RepeatableRead),
            "serializable" | "sr" | "serial" => Ok(IsolationLevel::Serializable),
            other => Err(format!("unknown isolation level '{}'", other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Running,
    Tainted,
    Committed,
    Aborted,
}

#[derive(Clone)]
pub enum UndoAction {
    Insert {
        table: Arc<dyn TableHandle>,
        rid: RecordId,
        indexes: Vec<IndexUndoLink>,
    },
    Delete {
        table: Arc<dyn TableHandle>,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
        indexes: Vec<IndexUndoLink>,
    },
}

#[derive(Clone)]
pub struct IndexUndoLink {
    pub index: Arc<dyn IndexHandle>,
    pub key: Tuple,
    pub rid: RecordId,
}

impl std::fmt::Debug for IndexUndoLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexUndoLink")
            .field("index", &self.index.name())
            .field("key", &self.key)
            .field("rid", &self.rid)
            .finish()
    }
}

impl std::fmt::Debug for UndoAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Insert {
                table,
                rid,
                indexes,
            } => f
                .debug_struct("Insert")
                .field("table", &table.table_ref())
                .field("rid", rid)
                .field("indexes", indexes)
                .finish(),
            Self::Delete {
                table,
                rid,
                prev_meta,
                prev_tuple,
                indexes,
            } => f
                .debug_struct("Delete")
                .field("table", &table.table_ref())
                .field("rid", rid)
                .field("prev_meta", prev_meta)
                .field("prev_tuple", prev_tuple)
                .field("indexes", indexes)
                .finish(),
        }
    }
}

impl UndoAction {
    pub fn undo(self, txn_id: TransactionId) -> QuillSQLResult<()> {
        match self {
            UndoAction::Insert {
                table,
                rid,
                indexes,
            } => {
                for link in indexes.into_iter() {
                    link.index.delete(&link.key, link.rid, txn_id)?;
                }
                table.undo_insert(rid, txn_id)?;
                Ok(())
            }
            UndoAction::Delete {
                table,
                rid,
                prev_meta,
                prev_tuple,
                indexes,
            } => {
                for link in indexes {
                    link.index.insert(&link.key, link.rid, txn_id)?;
                }
                table.undo_delete(rid, prev_meta, prev_tuple)?;
                Ok(())
            }
        }
    }

    pub fn to_heap_payload(
        &self,
        txn_id: TransactionId,
    ) -> QuillSQLResult<Option<HeapRecordPayload>> {
        match self {
            UndoAction::Insert { table, rid, .. } => table.undo_insert_payload(*rid, txn_id),
            UndoAction::Delete {
                table,
                rid,
                prev_meta,
                prev_tuple,
                ..
            } => table.undo_delete_payload(*rid, *prev_meta, prev_tuple, txn_id),
        }
    }
}

pub struct Transaction {
    id: TransactionId,
    isolation_level: IsolationLevel,
    access_mode: TransactionAccessMode,
    state: TransactionState,
    synchronous_commit: bool,
    begin_lsn: Option<Lsn>,
    last_lsn: Option<Lsn>,
    undo_actions: Vec<UndoAction>,
    current_command_id: CommandId,
    next_command_id: CommandId,
    snapshot: Option<TransactionSnapshot>,
}

impl Transaction {
    pub fn new(
        id: TransactionId,
        isolation_level: IsolationLevel,
        access_mode: TransactionAccessMode,
        synchronous_commit: bool,
    ) -> Self {
        Self {
            id,
            isolation_level,
            access_mode,
            state: TransactionState::Running,
            synchronous_commit,
            begin_lsn: None,
            last_lsn: None,
            undo_actions: Vec::new(),
            current_command_id: INVALID_COMMAND_ID,
            next_command_id: 0,
            snapshot: None,
        }
    }

    pub fn id(&self) -> TransactionId {
        self.id
    }

    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    pub fn access_mode(&self) -> TransactionAccessMode {
        self.access_mode
    }

    pub fn set_isolation_level(&mut self, isolation_level: IsolationLevel) {
        self.isolation_level = isolation_level;
        if matches!(
            isolation_level,
            IsolationLevel::ReadCommitted | IsolationLevel::ReadUncommitted
        ) {
            self.clear_snapshot();
        }
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

    pub fn begin_command(&mut self) -> CommandId {
        let cid = self.next_command_id;
        self.current_command_id = cid;
        self.next_command_id = self.next_command_id.wrapping_add(1);
        cid
    }

    pub fn current_command_id(&self) -> CommandId {
        self.current_command_id
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

    pub fn update_access_mode(&mut self, access_mode: TransactionAccessMode) {
        self.access_mode = access_mode;
    }

    pub fn push_insert_undo(
        &mut self,
        table: Arc<dyn TableHandle>,
        rid: RecordId,
        indexes: Vec<(Arc<dyn IndexHandle>, Tuple, RecordId)>,
    ) {
        self.undo_actions.push(UndoAction::Insert {
            table,
            rid,
            indexes: indexes
                .into_iter()
                .map(|(index, key, rid)| IndexUndoLink { index, key, rid })
                .collect(),
        });
    }

    pub fn push_update_undo(
        &mut self,
        table: Arc<dyn TableHandle>,
        old_rid: RecordId,
        new_rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
        new_keys: Vec<(Arc<dyn IndexHandle>, Tuple, RecordId)>,
        old_keys: Vec<(Arc<dyn IndexHandle>, Tuple, RecordId)>,
    ) {
        self.undo_actions.push(UndoAction::Insert {
            table: table.clone(),
            rid: new_rid,
            indexes: new_keys
                .into_iter()
                .map(|(index, key, rid)| IndexUndoLink { index, key, rid })
                .collect(),
        });
        self.undo_actions.push(UndoAction::Delete {
            table: table.clone(),
            rid: old_rid,
            prev_meta,
            prev_tuple,
            indexes: old_keys
                .into_iter()
                .map(|(index, key, rid)| IndexUndoLink { index, key, rid })
                .collect(),
        });
    }

    pub fn push_delete_undo(
        &mut self,
        table: Arc<dyn TableHandle>,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
        indexes: Vec<(Arc<dyn IndexHandle>, Tuple, RecordId)>,
    ) {
        self.undo_actions.push(UndoAction::Delete {
            table,
            rid,
            prev_meta,
            prev_tuple,
            indexes: indexes
                .into_iter()
                .map(|(index, key, rid)| IndexUndoLink { index, key, rid })
                .collect(),
        });
    }

    pub fn pop_undo_action(&mut self) -> Option<UndoAction> {
        self.undo_actions.pop()
    }

    pub fn clear_undo(&mut self) {
        self.undo_actions.clear();
    }

    pub fn snapshot(&self) -> Option<&TransactionSnapshot> {
        self.snapshot.as_ref()
    }

    pub fn set_snapshot(&mut self, snapshot: TransactionSnapshot) {
        self.snapshot = Some(snapshot);
    }

    pub fn clear_snapshot(&mut self) {
        self.snapshot = None;
    }
}
