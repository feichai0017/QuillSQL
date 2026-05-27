use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::record::RecordId;
use crate::transaction::{
    IsolationLevel, LockManager, LockMode, Transaction, TransactionId, TransactionSnapshot,
    TransactionState, TransactionStatus,
};
use crate::utils::table_ref::TableReference;
use dashmap::{DashMap, DashSet};
use serde::Serialize;
use sqlparser::ast::TransactionAccessMode;

#[derive(Debug, Default)]
struct HeldLocks {
    tables: Vec<(TableReference, LockMode)>,
    rows: Vec<(TableReference, RecordId, LockMode)>,
    row_keys: HashSet<(TableReference, RecordId)>,
}

pub struct TransactionManager {
    next_txn_id: AtomicU64,
    active_txns: DashSet<TransactionId>,
    lock_manager: Arc<LockManager>,
    held_locks: DashMap<TransactionId, HeldLocks>,
    txn_statuses: DashMap<TransactionId, TransactionStatus>,
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TxnDebugEntry {
    pub txn_id: TransactionId,
    pub status: TransactionStatus,
    pub held_tables: usize,
    pub held_rows: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct TxnDebugSnapshot {
    pub active: Vec<TxnDebugEntry>,
    pub oldest_active: Option<TransactionId>,
    pub next_txn_id: TransactionId,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            active_txns: DashSet::new(),
            lock_manager: Arc::new(LockManager::new()),
            held_locks: DashMap::new(),
            txn_statuses: DashMap::new(),
        }
    }

    pub fn with_lock_manager(lock_manager: Arc<LockManager>) -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            active_txns: DashSet::new(),
            lock_manager,
            held_locks: DashMap::new(),
            txn_statuses: DashMap::new(),
        }
    }

    pub fn lock_manager_arc(&self) -> Arc<LockManager> {
        self.lock_manager.clone()
    }

    pub fn begin(
        &self,
        isolation_level: IsolationLevel,
        access_mode: TransactionAccessMode,
    ) -> QuillSQLResult<Transaction> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        if txn_id == 0 {
            return Err(QuillSQLError::Internal(
                "Transaction ID wrapped around".to_string(),
            ));
        }
        let txn = Transaction::new(txn_id, isolation_level, access_mode);
        self.active_txns.insert(txn_id);
        self.txn_statuses
            .insert(txn_id, TransactionStatus::InProgress);
        self.held_locks.insert(txn_id, HeldLocks::default());
        Ok(txn)
    }

    pub fn acquire_table_lock(
        &self,
        txn: &Transaction,
        table: TableReference,
        mode: LockMode,
    ) -> QuillSQLResult<()> {
        if self.lock_manager.lock_table(txn, mode, table.clone()) {
            if let Some(mut entry) = self.held_locks.get_mut(&txn.id()) {
                entry.tables.push((table, mode));
            } else {
                let mut new_entry = HeldLocks::default();
                new_entry.tables.push((table, mode));
                self.held_locks.insert(txn.id(), new_entry);
            }
            Ok(())
        } else {
            Err(QuillSQLError::Internal(format!(
                "Failed to acquire table lock for txn {}",
                txn.id()
            )))
        }
    }

    pub fn try_acquire_row_lock(
        &self,
        txn: &Transaction,
        table: TableReference,
        rid: RecordId,
        mode: LockMode,
    ) -> QuillSQLResult<bool> {
        let key = (table.clone(), rid);
        if let Some(entry) = self.held_locks.get(&txn.id()) {
            if entry.row_keys.contains(&key) {
                return Ok(true);
            }
        }
        if self.lock_manager.lock_row(txn, mode, table.clone(), rid) {
            self.record_row_lock(txn.id(), table, rid, mode);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn acquire_row_lock(
        &self,
        txn: &Transaction,
        table: TableReference,
        rid: RecordId,
        mode: LockMode,
    ) -> QuillSQLResult<()> {
        if !self.try_acquire_row_lock(txn, table.clone(), rid, mode)? {
            return Err(QuillSQLError::Internal(format!(
                "Failed to acquire row lock for txn {}",
                txn.id()
            )));
        }
        Ok(())
    }

    pub fn commit(&self, txn: &mut Transaction) -> QuillSQLResult<()> {
        match txn.state() {
            TransactionState::Running | TransactionState::Tainted => {}
            TransactionState::Committed => {
                return Err(QuillSQLError::Internal(format!(
                    "Transaction {} already committed",
                    txn.id()
                )))
            }
            TransactionState::Aborted => {
                return Err(QuillSQLError::Internal(format!(
                    "Transaction {} already aborted",
                    txn.id()
                )))
            }
        }

        let txn_id = txn.id();
        txn.set_state(TransactionState::Committed);

        self.active_txns.remove(&txn_id);
        self.txn_statuses
            .insert(txn_id, TransactionStatus::Committed);
        self.release_all_locks(txn_id);
        txn.clear_undo();
        txn.clear_snapshot();
        Ok(())
    }

    pub fn abort(&self, txn: &mut Transaction) -> QuillSQLResult<()> {
        match txn.state() {
            TransactionState::Committed => {
                return Err(QuillSQLError::Internal(format!(
                    "Transaction {} already committed",
                    txn.id()
                )))
            }
            TransactionState::Aborted => return Ok(()),
            TransactionState::Running | TransactionState::Tainted => {}
        }

        let txn_id = txn.id();
        while let Some(action) = txn.pop_undo_action() {
            action.undo(txn_id)?;
        }

        txn.set_state(TransactionState::Aborted);

        self.active_txns.remove(&txn_id);
        self.txn_statuses.insert(txn_id, TransactionStatus::Aborted);
        self.release_all_locks(txn_id);
        txn.clear_undo();
        txn.clear_snapshot();
        Ok(())
    }

    pub fn active_transactions(&self) -> Vec<TransactionId> {
        self.active_txns.iter().map(|txn| *txn).collect()
    }

    pub fn snapshot(&self, txn_id: TransactionId) -> TransactionSnapshot {
        let active: Vec<TransactionId> = self
            .active_txns
            .iter()
            .map(|id| *id)
            .filter(|id| *id != txn_id)
            .collect();
        let xmax = self.next_txn_id.load(Ordering::SeqCst);
        let xmin = active.iter().copied().min().unwrap_or(xmax);
        TransactionSnapshot::new(txn_id, xmin, xmax, active)
    }

    pub fn transaction_status(&self, txn_id: TransactionId) -> TransactionStatus {
        if txn_id == 0 {
            return TransactionStatus::Committed;
        }
        self.txn_statuses
            .get(&txn_id)
            .map(|entry| *entry.value())
            .unwrap_or(TransactionStatus::Unknown)
    }

    pub fn record_recovered_status(&self, txn_id: TransactionId, status: TransactionStatus) {
        if txn_id == 0 {
            return;
        }
        self.txn_statuses.insert(txn_id, status);
    }

    pub fn ensure_next_txn_id_at_least(&self, next_txn_id: TransactionId) {
        let mut current = self.next_txn_id.load(Ordering::SeqCst);
        while current < next_txn_id {
            match self.next_txn_id.compare_exchange(
                current,
                next_txn_id,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    pub fn oldest_active_txn(&self) -> Option<TransactionId> {
        self.active_txns.iter().map(|txn| *txn).min()
    }

    pub fn next_txn_id_hint(&self) -> TransactionId {
        self.next_txn_id.load(Ordering::SeqCst)
    }

    pub fn debug_snapshot(&self) -> TxnDebugSnapshot {
        let active_ids = self.active_transactions();
        let mut active = Vec::with_capacity(active_ids.len());
        for txn_id in active_ids {
            let status = self.transaction_status(txn_id);
            let held = self.held_locks.get(&txn_id);
            let (held_tables, held_rows) = held
                .map(|locks| (locks.tables.len(), locks.rows.len()))
                .unwrap_or((0, 0));
            active.push(TxnDebugEntry {
                txn_id,
                status,
                held_tables,
                held_rows,
            });
        }
        TxnDebugSnapshot {
            active,
            oldest_active: self.oldest_active_txn(),
            next_txn_id: self.next_txn_id_hint(),
        }
    }

    pub fn record_row_lock(
        &self,
        txn_id: TransactionId,
        table: TableReference,
        rid: RecordId,
        mode: LockMode,
    ) {
        let mut entry = self.held_locks.entry(txn_id).or_default();
        if entry.row_keys.insert((table.clone(), rid)) {
            entry.rows.push((table, rid, mode));
        }
    }

    pub fn unlock_row(&self, txn_id: TransactionId, table: &TableReference, rid: RecordId) {
        if self.lock_manager.unlock_row_raw(txn_id, table.clone(), rid) {
            if let Some(mut entry) = self.held_locks.get_mut(&txn_id) {
                entry.row_keys.remove(&(table.clone(), rid));
                entry.rows.retain(|(t, r, _)| !(t == table && *r == rid));
            }
        }
    }

    fn release_all_locks(&self, txn_id: TransactionId) {
        if let Some((_, mut held)) = self.held_locks.remove(&txn_id) {
            for (table, rid, _) in held.rows.drain(..).rev() {
                let _ = self.lock_manager.unlock_row_raw(txn_id, table, rid);
            }
            for (table, _) in held.tables.drain(..).rev() {
                let _ = self.lock_manager.unlock_table_raw(txn_id, table);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::TupleMeta;

    #[test]
    fn commit_marks_state_and_status() {
        let manager = TransactionManager::new();

        let mut txn = manager
            .begin(
                IsolationLevel::ReadUncommitted,
                TransactionAccessMode::ReadWrite,
            )
            .expect("begin txn");
        manager.commit(&mut txn).expect("commit");

        assert_eq!(txn.state(), TransactionState::Committed);
        assert_eq!(
            manager.transaction_status(txn.id()),
            TransactionStatus::Committed
        );
    }

    #[test]
    fn abort_marks_state_and_status() {
        let manager = TransactionManager::new();

        let mut txn = manager
            .begin(
                IsolationLevel::ReadUncommitted,
                TransactionAccessMode::ReadWrite,
            )
            .expect("begin txn");
        manager.abort(&mut txn).expect("abort");

        assert_eq!(txn.state(), TransactionState::Aborted);
        assert_eq!(
            manager.transaction_status(txn.id()),
            TransactionStatus::Aborted
        );
    }

    #[test]
    fn snapshot_excludes_running_insert_until_commit() {
        let manager = TransactionManager::new();

        let mut writer = manager
            .begin(
                IsolationLevel::ReadCommitted,
                TransactionAccessMode::ReadWrite,
            )
            .expect("writer");
        let meta = TupleMeta::new(writer.id(), 0);

        let mut reader = manager
            .begin(
                IsolationLevel::ReadCommitted,
                TransactionAccessMode::ReadWrite,
            )
            .expect("reader");
        let snapshot = manager.snapshot(reader.id());
        assert!(
            !snapshot.is_visible(&meta, 0, |tid| manager.transaction_status(tid)),
            "running writer should not be visible",
        );

        manager.commit(&mut writer).expect("commit writer");
        let snapshot_after_commit = manager.snapshot(reader.id());
        assert!(snapshot_after_commit.is_visible(&meta, 0, |tid| manager.transaction_status(tid)));

        manager.abort(&mut reader).expect("abort reader");
    }

    #[test]
    fn snapshot_treats_committed_delete_as_invisible() {
        let manager = TransactionManager::new();

        let mut inserter = manager
            .begin(
                IsolationLevel::ReadCommitted,
                TransactionAccessMode::ReadWrite,
            )
            .expect("insert txn");
        let mut meta = TupleMeta::new(inserter.id(), 0);
        manager.commit(&mut inserter).expect("commit insert");

        let mut deleter = manager
            .begin(
                IsolationLevel::ReadCommitted,
                TransactionAccessMode::ReadWrite,
            )
            .expect("delete txn");
        meta.mark_deleted(deleter.id(), 0);

        let mut reader = manager
            .begin(
                IsolationLevel::ReadCommitted,
                TransactionAccessMode::ReadWrite,
            )
            .expect("reader txn");

        let before_commit = manager.snapshot(reader.id());
        assert!(before_commit.is_visible(&meta, 0, |tid| manager.transaction_status(tid)));

        manager.commit(&mut deleter).expect("commit delete");
        let after_commit = manager.snapshot(reader.id());
        assert!(
            !after_commit.is_visible(&meta, 0, |tid| manager.transaction_status(tid)),
            "committed delete should hide tuple",
        );

        manager.abort(&mut reader).expect("abort reader");
    }
}
