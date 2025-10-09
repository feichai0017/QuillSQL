use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::wal::codec::{ClrPayload, TransactionPayload, TransactionRecordKind};
use crate::recovery::wal_record::WalRecordPayload;
use crate::recovery::{Lsn, WalManager};
use crate::storage::page::RecordId;
use crate::transaction::{
    IsolationLevel, LockManager, LockMode, Transaction, TransactionId, TransactionSnapshot,
    TransactionState, TransactionStatus,
};
use crate::utils::table_ref::TableReference;
use dashmap::{DashMap, DashSet};
use sqlparser::ast::TransactionAccessMode;

#[derive(Debug, Default)]
struct HeldLocks {
    tables: Vec<(TableReference, LockMode)>,
    rows: Vec<(TableReference, RecordId, LockMode)>,
    row_keys: HashSet<(TableReference, RecordId)>,
    shared_rows: HashSet<(TableReference, RecordId)>,
}

pub struct TransactionManager {
    wal: Arc<WalManager>,
    next_txn_id: AtomicU64,
    synchronous_commit: AtomicBool,
    active_txns: DashSet<TransactionId>,
    lock_manager: Arc<LockManager>,
    held_locks: DashMap<TransactionId, HeldLocks>,
    txn_statuses: DashMap<TransactionId, TransactionStatus>,
}

impl TransactionManager {
    pub fn new(wal: Arc<WalManager>, synchronous_commit: bool) -> Self {
        Self {
            wal,
            next_txn_id: AtomicU64::new(1),
            synchronous_commit: AtomicBool::new(synchronous_commit),
            active_txns: DashSet::new(),
            lock_manager: Arc::new(LockManager::new()),
            held_locks: DashMap::new(),
            txn_statuses: DashMap::new(),
        }
    }

    pub fn with_lock_manager(
        wal: Arc<WalManager>,
        synchronous_commit: bool,
        lock_manager: Arc<LockManager>,
    ) -> Self {
        Self {
            wal,
            next_txn_id: AtomicU64::new(1),
            synchronous_commit: AtomicBool::new(synchronous_commit),
            active_txns: DashSet::new(),
            lock_manager,
            held_locks: DashMap::new(),
            txn_statuses: DashMap::new(),
        }
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
        let sync_commit = self.synchronous_commit.load(Ordering::Relaxed);
        let mut txn = Transaction::new(txn_id, isolation_level, access_mode, sync_commit);
        let append = self.wal.append_record_with(|_| {
            WalRecordPayload::Transaction(TransactionPayload {
                marker: TransactionRecordKind::Begin,
                txn_id,
            })
        })?;
        txn.set_begin_lsn(append.end_lsn);
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
        let append = self.wal.append_record_with(|_| {
            WalRecordPayload::Transaction(TransactionPayload {
                marker: TransactionRecordKind::Commit,
                txn_id,
            })
        })?;
        txn.record_lsn(append.end_lsn);
        txn.set_state(TransactionState::Committed);

        self.active_txns.remove(&txn_id);
        self.txn_statuses
            .insert(txn_id, TransactionStatus::Committed);
        self.release_all_locks(txn_id);
        txn.clear_undo();
        self.finish_commit(txn, append.end_lsn)
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
        let mut undo_next: Option<Lsn> = None;
        while let Some(action) = txn.pop_undo_action() {
            let payload = action.to_heap_payload()?;
            let clr_result = self.wal.append_record_with(|ctx| {
                txn.record_lsn(ctx.end_lsn);
                WalRecordPayload::Clr(ClrPayload {
                    txn_id,
                    undone_lsn: ctx.start_lsn,
                    undo_next_lsn: undo_next.unwrap_or(0),
                })
            })?;
            txn.record_lsn(clr_result.end_lsn);
            let heap_result = self.wal.append_record_with(|ctx| {
                txn.record_lsn(ctx.end_lsn);
                WalRecordPayload::Heap(payload.clone())
            })?;
            txn.record_lsn(heap_result.end_lsn);
            action.undo(txn_id)?;
            undo_next = Some(heap_result.start_lsn);
        }

        let append = self.wal.append_record_with(|_| {
            WalRecordPayload::Transaction(TransactionPayload {
                marker: TransactionRecordKind::Abort,
                txn_id,
            })
        })?;
        txn.record_lsn(append.end_lsn);
        txn.set_state(TransactionState::Aborted);

        self.active_txns.remove(&txn_id);
        self.txn_statuses.insert(txn_id, TransactionStatus::Aborted);
        self.release_all_locks(txn_id);
        txn.clear_undo();
        self.finish_commit(txn, append.end_lsn)
    }

    pub fn synchronous_commit(&self) -> bool {
        self.synchronous_commit.load(Ordering::Relaxed)
    }

    pub fn set_synchronous_commit(&self, value: bool) {
        self.synchronous_commit.store(value, Ordering::Relaxed);
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

    pub fn oldest_active_txn(&self) -> Option<TransactionId> {
        self.active_txns.iter().map(|txn| *txn).min()
    }

    pub fn next_txn_id_hint(&self) -> TransactionId {
        self.next_txn_id.load(Ordering::SeqCst)
    }

    fn finish_commit(&self, txn: &Transaction, lsn: Lsn) -> QuillSQLResult<()> {
        if txn.synchronous_commit() {
            self.wal.wait_for_durable(lsn)?;
        } else {
            let _ = self.wal.flush_until(lsn)?;
        }
        Ok(())
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

    pub fn remove_row_key_marker(
        &self,
        txn_id: TransactionId,
        table: &TableReference,
        rid: RecordId,
    ) {
        if let Some(mut entry) = self.held_locks.get_mut(&txn_id) {
            entry.row_keys.remove(&(table.clone(), rid));
        }
    }

    pub fn record_shared_row_lock(
        &self,
        txn_id: TransactionId,
        table: TableReference,
        rid: RecordId,
    ) {
        let mut entry = self.held_locks.entry(txn_id).or_default();
        entry.shared_rows.insert((table, rid));
    }

    pub fn remove_shared_row_lock(
        &self,
        txn_id: TransactionId,
        table: &TableReference,
        rid: RecordId,
    ) {
        if let Some(mut entry) = self.held_locks.get_mut(&txn_id) {
            entry.shared_rows.remove(&(table.clone(), rid));
        }
    }

    pub fn try_unlock_shared_row(
        &self,
        txn_id: TransactionId,
        table: &TableReference,
        rid: RecordId,
    ) -> QuillSQLResult<()> {
        let unlocked = self.lock_manager.unlock_row_raw(txn_id, table.clone(), rid);
        if !unlocked {
            return Err(QuillSQLError::Execution(format!(
                "failed to release shared row lock for txn {} on {}",
                txn_id, table
            )));
        }
        self.remove_shared_row_lock(txn_id, table, rid);
        Ok(())
    }

    pub fn unlock_row(&self, txn_id: TransactionId, table: &TableReference, rid: RecordId) {
        if self.lock_manager.unlock_row_raw(txn_id, table.clone(), rid) {
            if let Some(mut entry) = self.held_locks.get_mut(&txn_id) {
                entry.row_keys.remove(&(table.clone(), rid));
                entry.rows.retain(|(t, r, _)| !(t == table && *r == rid));
                entry.shared_rows.remove(&(table.clone(), rid));
            }
        }
    }

    fn release_all_locks(&self, txn_id: TransactionId) {
        if let Some((_, mut held)) = self.held_locks.remove(&txn_id) {
            for (table, rid, _) in held.rows.drain(..).rev() {
                let _ = self.lock_manager.unlock_row_raw(txn_id, table, rid);
            }
            for (table, rid) in held.shared_rows.drain() {
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
    use crate::config::WalConfig;
    use crate::recovery::WalManager;
    use crate::storage::page::TupleMeta;
    use tempfile::TempDir;

    fn build_wal(temp_dir: &TempDir) -> Arc<WalManager> {
        let wal_path = temp_dir.path().join("wal");
        let config = WalConfig {
            directory: wal_path,
            sync_on_flush: false,
            ..WalConfig::default()
        };
        Arc::new(WalManager::new(config, None, None).expect("wal manager"))
    }

    #[test]
    fn commit_waits_for_durable_when_sync() {
        let temp = TempDir::new().expect("tempdir");
        let wal = build_wal(&temp);
        let manager = TransactionManager::new(wal.clone(), true);

        let mut txn = manager
            .begin(
                IsolationLevel::ReadUncommitted,
                TransactionAccessMode::ReadWrite,
            )
            .expect("begin txn");
        manager.commit(&mut txn).expect("commit");

        assert_eq!(txn.state(), TransactionState::Committed);
        let lsn = txn.last_lsn().expect("commit lsn");
        assert!(wal.durable_lsn() >= lsn);
    }

    #[test]
    fn abort_records_wal_and_marks_state() {
        let temp = TempDir::new().expect("tempdir");
        let wal = build_wal(&temp);
        let manager = TransactionManager::new(wal.clone(), false);

        let mut txn = manager
            .begin(
                IsolationLevel::ReadUncommitted,
                TransactionAccessMode::ReadWrite,
            )
            .expect("begin txn");
        manager.abort(&mut txn).expect("abort");

        assert_eq!(txn.state(), TransactionState::Aborted);
        let lsn = txn.last_lsn().expect("abort lsn");
        // Async commit still triggers flush_until, so durable LSN should advance.
        assert!(wal.durable_lsn() >= lsn);
    }

    #[test]
    fn snapshot_excludes_running_insert_until_commit() {
        let temp = TempDir::new().expect("tempdir");
        let wal = build_wal(&temp);
        let manager = TransactionManager::new(wal, true);

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
        let temp = TempDir::new().expect("tempdir");
        let wal = build_wal(&temp);
        let manager = TransactionManager::new(wal, true);

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
