use crate::error::QuillSQLResult;
use crate::recovery::Lsn;
use crate::storage::codec::TupleCodec;
use crate::storage::heap::wal_codec::{
    HeapDeletePayload, HeapInsertPayload, HeapRecordPayload, HeapUpdatePayload, TupleMetaRepr,
};
use crate::storage::index::btree_index::BPlusTreeIndex;
use crate::storage::page::{RecordId, TupleMeta};
use crate::storage::table_heap::TableHeap;
use crate::storage::tuple::Tuple;
use sqlparser::ast::TransactionAccessMode;
use std::str::FromStr;
use std::sync::Arc;

pub type TransactionId = u64;

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

#[derive(Debug, Clone)]
pub enum UndoAction {
    Insert {
        table: Arc<TableHeap>,
        rid: RecordId,
        indexes: Vec<(Arc<BPlusTreeIndex>, Tuple)>,
    },
    Update {
        table: Arc<TableHeap>,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
        new_keys: Vec<(Arc<BPlusTreeIndex>, Tuple)>,
        old_keys: Vec<(Arc<BPlusTreeIndex>, Tuple)>,
    },
    Delete {
        table: Arc<TableHeap>,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
        indexes: Vec<(Arc<BPlusTreeIndex>, Tuple)>,
    },
}

impl UndoAction {
    pub fn undo(self, txn_id: TransactionId) -> QuillSQLResult<()> {
        match self {
            UndoAction::Insert {
                table,
                rid,
                indexes,
            } => {
                for (index, key) in indexes.into_iter() {
                    index.delete(&key)?;
                }
                table.recover_delete_tuple(rid, txn_id)?;
                Ok(())
            }
            UndoAction::Update {
                table,
                rid,
                prev_meta,
                prev_tuple,
                new_keys,
                old_keys,
            } => {
                table.recover_restore_tuple(rid, prev_meta, &prev_tuple)?;
                for (index, key) in new_keys.into_iter() {
                    index.delete(&key)?;
                }
                for (index, key) in old_keys.into_iter() {
                    index.insert(&key, rid)?;
                }
                Ok(())
            }
            UndoAction::Delete {
                table,
                rid,
                prev_meta,
                prev_tuple,
                indexes,
            } => {
                table.recover_restore_tuple(rid, prev_meta, &prev_tuple)?;
                for (index, key) in indexes.into_iter() {
                    index.insert(&key, rid)?;
                }
                Ok(())
            }
        }
    }

    pub fn to_heap_payload(&self) -> QuillSQLResult<HeapRecordPayload> {
        match self {
            UndoAction::Insert { table, rid, .. } => {
                let (meta, tuple) = table.full_tuple(*rid)?;
                Ok(HeapRecordPayload::Delete(HeapDeletePayload {
                    relation: table.relation_ident(),
                    page_id: rid.page_id,
                    slot_id: rid.slot_num as u16,
                    op_txn_id: meta.insert_txn_id,
                    old_tuple_meta: TupleMetaRepr::from(meta),
                    old_tuple_data: Some(TupleCodec::encode(&tuple)),
                }))
            }
            UndoAction::Update {
                table,
                rid,
                prev_meta,
                prev_tuple,
                ..
            } => Ok(HeapRecordPayload::Update(HeapUpdatePayload {
                relation: table.relation_ident(),
                page_id: rid.page_id,
                slot_id: rid.slot_num as u16,
                op_txn_id: prev_meta.insert_txn_id,
                new_tuple_meta: TupleMetaRepr::from(*prev_meta),
                new_tuple_data: TupleCodec::encode(prev_tuple),
                old_tuple_meta: None,
                old_tuple_data: None,
            })),
            UndoAction::Delete {
                table,
                rid,
                prev_meta,
                prev_tuple,
                ..
            } => Ok(HeapRecordPayload::Insert(HeapInsertPayload {
                relation: table.relation_ident(),
                page_id: rid.page_id,
                slot_id: rid.slot_num as u16,
                op_txn_id: prev_meta.insert_txn_id,
                tuple_meta: TupleMetaRepr::from(*prev_meta),
                tuple_data: TupleCodec::encode(prev_tuple),
            })),
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

    #[allow(dead_code)]
    pub(crate) fn set_access_mode(&mut self, access_mode: TransactionAccessMode) {
        self.access_mode = access_mode;
    }

    pub fn update_access_mode(&mut self, access_mode: TransactionAccessMode) {
        self.access_mode = access_mode;
    }

    #[allow(dead_code)]
    pub(crate) fn mark_tainted(&mut self) {
        self.state = TransactionState::Tainted;
    }

    pub fn push_insert_undo(
        &mut self,
        table: Arc<TableHeap>,
        rid: RecordId,
        indexes: Vec<(Arc<BPlusTreeIndex>, Tuple)>,
    ) {
        self.undo_actions.push(UndoAction::Insert {
            table,
            rid,
            indexes,
        });
    }

    pub fn push_update_undo(
        &mut self,
        table: Arc<TableHeap>,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
        new_keys: Vec<(Arc<BPlusTreeIndex>, Tuple)>,
        old_keys: Vec<(Arc<BPlusTreeIndex>, Tuple)>,
    ) {
        self.undo_actions.push(UndoAction::Update {
            table,
            rid,
            prev_meta,
            prev_tuple,
            new_keys,
            old_keys,
        });
    }

    pub fn push_delete_undo(
        &mut self,
        table: Arc<TableHeap>,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
        indexes: Vec<(Arc<BPlusTreeIndex>, Tuple)>,
    ) {
        self.undo_actions.push(UndoAction::Delete {
            table,
            rid,
            prev_meta,
            prev_tuple,
            indexes,
        });
    }

    pub fn pop_undo_action(&mut self) -> Option<UndoAction> {
        self.undo_actions.pop()
    }

    pub fn clear_undo(&mut self) {
        self.undo_actions.clear();
    }
}
