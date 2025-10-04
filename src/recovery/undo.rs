use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::buffer::BufferManager;
use crate::error::QuillSQLResult;
use crate::recovery::resource_manager::{
    ensure_default_resource_managers_registered, get_resource_manager, UndoContext,
};
use crate::recovery::wal::codec::{
    decode_clr, decode_transaction, ClrPayload, ResourceManagerId, TransactionRecordKind, WalFrame,
};
use crate::recovery::wal_record::WalRecordPayload;
use crate::recovery::{Lsn, WalManager};
use crate::storage::disk_scheduler::DiskScheduler;

#[derive(Default)]
struct UndoRecord {
    next: Option<Lsn>,
    payload: Option<WalFrame>,
}

#[derive(Default)]
struct UndoIndex {
    heads: HashMap<u64, Option<Lsn>>,
    entries: HashMap<Lsn, UndoRecord>,
    active: HashSet<u64>,
}

impl UndoIndex {
    fn observe(&mut self, frame: &WalFrame) -> QuillSQLResult<()> {
        match frame.rmid {
            ResourceManagerId::Clr => {
                let clr = decode_clr(&frame.body)?;
                let next = if clr.undo_next_lsn == 0 {
                    None
                } else {
                    Some(clr.undo_next_lsn)
                };
                self.entries.insert(
                    frame.lsn,
                    UndoRecord {
                        next,
                        payload: None,
                    },
                );
                self.heads.insert(clr.txn_id, next);
                self.active.insert(clr.txn_id);
            }
            ResourceManagerId::Transaction => {
                let tx = decode_transaction(&frame.body, frame.info)?;
                match tx.marker {
                    TransactionRecordKind::Begin => {
                        self.active.insert(tx.txn_id);
                        self.heads.entry(tx.txn_id).or_insert(None);
                    }
                    TransactionRecordKind::Commit | TransactionRecordKind::Abort => {
                        self.active.remove(&tx.txn_id);
                        self.heads.insert(tx.txn_id, None);
                    }
                }
            }
            _ => {
                if let Some(manager) = get_resource_manager(frame.rmid) {
                    if let Some(txn_id) = manager.transaction_id(frame) {
                        let prev = self.head_for(txn_id);
                        self.entries.insert(
                            frame.lsn,
                            UndoRecord {
                                next: prev,
                                payload: Some(frame.clone()),
                            },
                        );
                        self.heads.insert(txn_id, Some(frame.lsn));
                        self.active.insert(txn_id);
                    }
                }
            }
        }
        Ok(())
    }

    fn head_for(&self, txn_id: u64) -> Option<Lsn> {
        self.heads.get(&txn_id).copied().flatten()
    }

    fn entry(&self, lsn: Lsn) -> Option<&UndoRecord> {
        self.entries.get(&lsn)
    }

    fn active_transactions(&self) -> Vec<u64> {
        let mut txns: Vec<u64> = self.active.iter().copied().collect();
        txns.sort_unstable();
        txns
    }
}

#[derive(Default)]
pub struct UndoOutcome {
    pub loser_transactions: Vec<u64>,
    pub max_clr_lsn: Lsn,
}

pub struct UndoExecutor {
    wal: Arc<WalManager>,
    disk_scheduler: Arc<DiskScheduler>,
    buffer_pool: Option<Arc<BufferManager>>,
    index: UndoIndex,
}

impl UndoExecutor {
    pub fn new(
        wal: Arc<WalManager>,
        disk_scheduler: Arc<DiskScheduler>,
        buffer_pool: Option<Arc<BufferManager>>,
    ) -> Self {
        ensure_default_resource_managers_registered();
        Self {
            wal,
            disk_scheduler,
            buffer_pool,
            index: UndoIndex::default(),
        }
    }

    pub fn observe(&mut self, frame: &WalFrame) -> QuillSQLResult<()> {
        self.index.observe(frame)
    }

    pub fn finalize(self) -> QuillSQLResult<UndoOutcome> {
        let losers = self.index.active_transactions();
        let mut max_clr_lsn = 0;

        for txn_id in losers.iter().copied() {
            let mut cursor = self.index.head_for(txn_id);
            while let Some(lsn) = cursor {
                let Some(entry) = self.index.entry(lsn) else {
                    break;
                };
                if let Some(rec) = &entry.payload {
                    if let Some(manager) = get_resource_manager(rec.rmid) {
                        let ctx = UndoContext {
                            disk_scheduler: self.disk_scheduler.clone(),
                            buffer_pool: self.buffer_pool.clone(),
                        };
                        manager.undo(rec, &ctx)?;
                    }
                    let undo_next = entry.next.unwrap_or(0);
                    let clr = self.wal.append_record_with(|_| {
                        WalRecordPayload::Clr(ClrPayload {
                            txn_id,
                            undone_lsn: lsn,
                            undo_next_lsn: undo_next,
                        })
                    })?;
                    if clr.end_lsn > max_clr_lsn {
                        max_clr_lsn = clr.end_lsn;
                    }
                }
                cursor = entry.next;
            }
        }

        Ok(UndoOutcome {
            loser_transactions: losers,
            max_clr_lsn,
        })
    }
}
