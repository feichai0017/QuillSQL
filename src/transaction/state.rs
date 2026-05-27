use crate::error::QuillSQLResult;
use crate::storage::record::{RecordId, TupleMeta};
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

pub(crate) type IndexUndoEntries = Vec<(Arc<dyn IndexHandle>, Tuple, RecordId)>;

pub(crate) struct UpdateUndo {
    pub old_rid: RecordId,
    pub new_rid: RecordId,
    pub prev_meta: TupleMeta,
    pub prev_tuple: Tuple,
    pub new_keys: IndexUndoEntries,
    pub old_keys: IndexUndoEntries,
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
}

pub struct Transaction {
    id: TransactionId,
    isolation_level: IsolationLevel,
    access_mode: TransactionAccessMode,
    state: TransactionState,
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
    ) -> Self {
        Self {
            id,
            isolation_level,
            access_mode,
            state: TransactionState::Running,
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

    pub fn begin_command(&mut self) -> CommandId {
        let cid = self.next_command_id;
        self.current_command_id = cid;
        self.next_command_id = self.next_command_id.wrapping_add(1);
        cid
    }

    pub fn current_command_id(&self) -> CommandId {
        self.current_command_id
    }

    pub(crate) fn set_state(&mut self, state: TransactionState) {
        self.state = state;
    }

    pub fn update_access_mode(&mut self, access_mode: TransactionAccessMode) {
        self.access_mode = access_mode;
    }

    pub(crate) fn push_insert_undo(
        &mut self,
        table: Arc<dyn TableHandle>,
        rid: RecordId,
        indexes: IndexUndoEntries,
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

    pub(crate) fn push_update_undo(&mut self, table: Arc<dyn TableHandle>, undo: UpdateUndo) {
        self.undo_actions.push(UndoAction::Insert {
            table: table.clone(),
            rid: undo.new_rid,
            indexes: undo
                .new_keys
                .into_iter()
                .map(|(index, key, rid)| IndexUndoLink { index, key, rid })
                .collect(),
        });
        self.undo_actions.push(UndoAction::Delete {
            table: table.clone(),
            rid: undo.old_rid,
            prev_meta: undo.prev_meta,
            prev_tuple: undo.prev_tuple,
            indexes: undo
                .old_keys
                .into_iter()
                .map(|(index, key, rid)| IndexUndoLink { index, key, rid })
                .collect(),
        });
    }

    pub(crate) fn push_delete_undo(
        &mut self,
        table: Arc<dyn TableHandle>,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
        indexes: IndexUndoEntries,
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

    pub(crate) fn pop_undo_action(&mut self) -> Option<UndoAction> {
        self.undo_actions.pop()
    }

    pub(crate) fn clear_undo(&mut self) {
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
