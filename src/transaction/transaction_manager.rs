use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::wal_record::{TransactionPayload, TransactionRecordKind, WalRecordPayload};
use crate::recovery::{Lsn, WalManager};

use dashmap::DashSet;

use super::{IsolationLevel, Transaction, TransactionId, TransactionState};

pub struct TransactionManager {
    wal: Arc<WalManager>,
    next_txn_id: AtomicU64,
    synchronous_commit: AtomicBool,
    active_txns: DashSet<TransactionId>,
}

impl TransactionManager {
    pub fn new(wal: Arc<WalManager>, synchronous_commit: bool) -> Self {
        Self {
            wal,
            next_txn_id: AtomicU64::new(1),
            synchronous_commit: AtomicBool::new(synchronous_commit),
            active_txns: DashSet::new(),
        }
    }

    pub fn begin(&self, isolation_level: IsolationLevel) -> QuillSQLResult<Transaction> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        if txn_id == 0 {
            return Err(QuillSQLError::Internal(
                "Transaction ID wrapped around".to_string(),
            ));
        }
        let sync_commit = self.synchronous_commit.load(Ordering::Relaxed);
        let mut txn = Transaction::new(txn_id, isolation_level, sync_commit);
        let append = self.wal.append_record_with(|_| {
            WalRecordPayload::Transaction(TransactionPayload {
                marker: TransactionRecordKind::Begin,
                txn_id,
            })
        })?;
        txn.set_begin_lsn(append.end_lsn);
        self.active_txns.insert(txn_id);
        Ok(txn)
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
        let append = self.wal.append_record_with(|_| {
            WalRecordPayload::Transaction(TransactionPayload {
                marker: TransactionRecordKind::Abort,
                txn_id,
            })
        })?;
        txn.record_lsn(append.end_lsn);
        txn.set_state(TransactionState::Aborted);

        self.active_txns.remove(&txn_id);
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

    fn finish_commit(&self, txn: &Transaction, lsn: Lsn) -> QuillSQLResult<()> {
        if txn.synchronous_commit() {
            self.wal.wait_for_durable(lsn)?;
        } else {
            let _ = self.wal.flush_until(lsn)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WalConfig;
    use crate::recovery::WalManager;
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;
    use std::path::Path;
    use tempfile::TempDir;

    fn build_scheduler(path: &Path) -> Arc<DiskScheduler> {
        let disk_manager = Arc::new(DiskManager::try_new(path).expect("disk manager"));
        Arc::new(DiskScheduler::new(disk_manager))
    }

    fn build_wal(temp_dir: &TempDir) -> Arc<WalManager> {
        let wal_path = temp_dir.path().join("wal");
        let db_path = temp_dir.path().join("test.db");
        let mut config = WalConfig::default();
        config.directory = wal_path;
        config.sync_on_flush = false;
        let scheduler = build_scheduler(&db_path);
        Arc::new(WalManager::new(config, scheduler, None, None).expect("wal manager"))
    }

    #[test]
    fn commit_waits_for_durable_when_sync() {
        let temp = TempDir::new().expect("tempdir");
        let wal = build_wal(&temp);
        let manager = TransactionManager::new(wal.clone(), true);

        let mut txn = manager
            .begin(IsolationLevel::ReadUncommitted)
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
            .begin(IsolationLevel::ReadUncommitted)
            .expect("begin txn");
        manager.abort(&mut txn).expect("abort");

        assert_eq!(txn.state(), TransactionState::Aborted);
        let lsn = txn.last_lsn().expect("abort lsn");
        // Async commit still triggers flush_until, so durable LSN should advance.
        assert!(wal.durable_lsn() >= lsn);
    }
}
