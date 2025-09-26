use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;

use crate::buffer::PAGE_SIZE;
use crate::error::QuillSQLResult;
use crate::recovery::wal_record::{
    CheckpointPayload, PageWritePayload, TransactionPayload, TransactionRecordKind,
    WalRecordPayload,
};
use crate::recovery::{Lsn, WalManager};
use crate::storage::disk_scheduler::DiskScheduler;

pub struct RecoveryManager {
    wal: Arc<WalManager>,
    disk_scheduler: Arc<DiskScheduler>,
}

#[derive(Debug, Default, Clone)]
pub struct RecoverySummary {
    pub start_lsn: Lsn,
    pub redo_count: usize,
    pub loser_transactions: Vec<u64>,
}

impl RecoveryManager {
    pub fn new(wal: Arc<WalManager>, disk_scheduler: Arc<DiskScheduler>) -> Self {
        Self {
            wal,
            disk_scheduler,
        }
    }

    pub fn replay(&self) -> QuillSQLResult<RecoverySummary> {
        let mut reader = match self.wal.reader() {
            Ok(r) => r,
            Err(_) => return Ok(RecoverySummary::default()),
        };

        let mut frames = Vec::new();
        let mut checkpoint: Option<(Lsn, CheckpointPayload)> = None;
        while let Some(frame) = reader.next_frame()? {
            if let WalRecordPayload::Checkpoint(payload) = &frame.payload {
                checkpoint = Some((frame.lsn, payload.clone()));
            }
            frames.push(frame);
        }

        if frames.is_empty() {
            return Ok(RecoverySummary::default());
        }

        let (start_lsn, mut active_txns) = if let Some((checkpoint_lsn, payload)) = checkpoint {
            let active: HashSet<u64> = payload.active_transactions.iter().copied().collect();
            (payload.last_lsn.max(checkpoint_lsn), active)
        } else {
            (0, HashSet::new())
        };

        let mut redo_count = 0usize;
        for frame in frames.iter().filter(|f| f.lsn >= start_lsn) {
            match &frame.payload {
                WalRecordPayload::PageWrite(payload) => {
                    self.redo_page_write(payload.clone())?;
                    redo_count += 1;
                }
                WalRecordPayload::Heap(_) => {
                    // Heap redo TBD.
                }
                WalRecordPayload::Transaction(tx) => {
                    self.update_active_transactions(&mut active_txns, tx);
                }
                WalRecordPayload::Checkpoint(_) => {}
            }
        }

        let mut losers: Vec<u64> = active_txns.into_iter().collect();
        losers.sort_unstable();

        if !losers.is_empty() {
            log::warn!(
                "Recovery completed with {} outstanding transaction(s) requiring undo",
                losers.len()
            );
        }

        Ok(RecoverySummary {
            start_lsn,
            redo_count,
            loser_transactions: losers,
        })
    }

    fn redo_page_write(&self, payload: PageWritePayload) -> QuillSQLResult<()> {
        debug_assert_eq!(payload.page_image.len(), PAGE_SIZE);
        let bytes = Bytes::from(payload.page_image);
        let rx = self.disk_scheduler.schedule_write(payload.page_id, bytes)?;
        rx.recv().map_err(|e| {
            crate::error::QuillSQLError::Internal(format!("WAL recovery write recv failed: {}", e))
        })??;
        Ok(())
    }

    fn update_active_transactions(&self, active: &mut HashSet<u64>, txn: &TransactionPayload) {
        match txn.marker {
            TransactionRecordKind::Begin => {
                active.insert(txn.txn_id);
            }
            TransactionRecordKind::Commit | TransactionRecordKind::Abort => {
                active.remove(&txn.txn_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RecoveryManager;
    use crate::config::WalConfig;
    use crate::recovery::wal_record::{
        PageWritePayload, TransactionPayload, TransactionRecordKind, WalRecordPayload,
    };
    use crate::recovery::WalManager;
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn build_scheduler(db_path: &Path) -> Arc<DiskScheduler> {
        let disk_manager = Arc::new(DiskManager::try_new(db_path).expect("disk manager"));
        Arc::new(DiskScheduler::new(disk_manager))
    }

    #[test]
    fn replay_applies_page_writes() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("test.db");
        let wal_dir = temp.path().join("wal");

        let scheduler = build_scheduler(&db_path);
        let mut config = WalConfig::default();
        config.directory = wal_dir.clone();
        config.sync_on_flush = false;
        let wal = Arc::new(WalManager::new(config.clone(), scheduler.clone(), None, None).unwrap());

        let page_bytes = vec![0xAB; crate::buffer::PAGE_SIZE];
        wal.append_record_with(|_| {
            WalRecordPayload::PageWrite(PageWritePayload {
                page_id: 1,
                prev_page_lsn: 0,
                page_image: page_bytes.clone(),
            })
        })
        .unwrap();
        wal.flush(None).unwrap();
        drop(wal);

        let scheduler = build_scheduler(&db_path);
        let wal = Arc::new(WalManager::new(config, scheduler.clone(), None, None).unwrap());
        let recovery = RecoveryManager::new(wal, scheduler.clone());
        let summary = recovery.replay().unwrap();
        assert_eq!(summary.redo_count, 1);
        assert!(summary.loser_transactions.is_empty());

        let rx = scheduler.schedule_read(1).unwrap();
        let data = rx.recv().unwrap().unwrap();
        assert_eq!(data.len(), crate::buffer::PAGE_SIZE);
        assert!(data.iter().all(|byte| *byte == 0xAB));
    }

    #[test]
    fn replay_reports_loser_transactions() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("loser.db");
        let wal_dir = temp.path().join("wal");

        let scheduler = build_scheduler(&db_path);
        let mut config = WalConfig::default();
        config.directory = wal_dir.clone();
        config.sync_on_flush = false;
        let wal = WalManager::new(config.clone(), scheduler.clone(), None, None).unwrap();

        wal.append_record_with(|_| {
            WalRecordPayload::Transaction(TransactionPayload {
                marker: TransactionRecordKind::Begin,
                txn_id: 7,
            })
        })
        .expect("append begin");
        wal.flush(None).unwrap();
        drop(wal);

        let scheduler = build_scheduler(&db_path);
        let wal = Arc::new(WalManager::new(config, scheduler.clone(), None, None).unwrap());
        let recovery = RecoveryManager::new(wal, scheduler.clone());
        let summary = recovery.replay().unwrap();
        assert_eq!(summary.loser_transactions, vec![7]);
    }
}
