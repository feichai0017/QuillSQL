use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::buffer::{BufferManager, PAGE_SIZE};
use crate::error::QuillSQLResult;
use crate::recovery::wal_record::{
    ClrPayload, HeapRecordPayload, TransactionRecordKind, WalFrame, WalRecordPayload,
};
use crate::recovery::{Lsn, WalManager};
use crate::storage::codec::TablePageHeaderCodec;
use crate::storage::disk_scheduler::DiskScheduler;
use crate::storage::table_heap::TableHeap;

#[derive(Default)]
struct UndoRecord {
    next: Option<Lsn>,
    payload: Option<HeapRecordPayload>,
}

#[derive(Default)]
struct UndoIndex {
    heads: HashMap<u64, Option<Lsn>>,
    entries: HashMap<Lsn, UndoRecord>,
    active: HashSet<u64>,
}

impl UndoIndex {
    fn observe(&mut self, frame: &WalFrame) {
        match &frame.payload {
            WalRecordPayload::Heap(rec) => {
                let txn_id = heap_txn_id(rec);
                let prev = self.head_for(txn_id);
                self.entries.insert(
                    frame.lsn,
                    UndoRecord {
                        next: prev,
                        payload: Some(rec.clone()),
                    },
                );
                self.heads.insert(txn_id, Some(frame.lsn));
                self.active.insert(txn_id);
            }
            WalRecordPayload::Clr(clr) => {
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
            WalRecordPayload::Transaction(tx) => match tx.marker {
                TransactionRecordKind::Begin => {
                    self.active.insert(tx.txn_id);
                    self.heads.entry(tx.txn_id).or_insert(None);
                }
                TransactionRecordKind::Commit | TransactionRecordKind::Abort => {
                    self.active.remove(&tx.txn_id);
                    self.heads.insert(tx.txn_id, None);
                }
            },
            _ => {}
        }
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
        Self {
            wal,
            disk_scheduler,
            buffer_pool,
            index: UndoIndex::default(),
        }
    }

    pub fn observe(&mut self, frame: &WalFrame) {
        self.index.observe(frame);
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
                    self.undo_heap_record(rec)?;
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

    fn undo_heap_record(&self, rec: &HeapRecordPayload) -> QuillSQLResult<()> {
        match rec {
            HeapRecordPayload::Insert(body) => {
                self.apply_tuple_meta_flag(body.page_id, body.slot_id as usize, true)
            }
            HeapRecordPayload::Update(body) => {
                if let (Some(old_meta), Some(old_bytes)) =
                    (&body.old_tuple_meta, &body.old_tuple_data)
                {
                    self.restore_tuple(body.page_id, body.slot_id as usize, *old_meta, old_bytes)
                } else {
                    Ok(())
                }
            }
            HeapRecordPayload::Delete(body) => {
                if let Some(old_bytes) = &body.old_tuple_data {
                    self.restore_tuple(
                        body.page_id,
                        body.slot_id as usize,
                        body.old_tuple_meta,
                        old_bytes,
                    )
                } else {
                    self.apply_tuple_meta_flag(body.page_id, body.slot_id as usize, false)
                }
            }
        }
    }

    fn apply_tuple_meta_flag(
        &self,
        page_id: u32,
        slot_idx: usize,
        deleted: bool,
    ) -> QuillSQLResult<()> {
        if let Some(bpm) = &self.buffer_pool {
            let rid = crate::storage::page::RecordId::new(page_id, slot_idx as u32);
            let guard = bpm.fetch_page_read(page_id)?;
            let (header, _hdr_len) = TablePageHeaderCodec::decode(guard.data())?;
            drop(guard);
            if slot_idx >= header.tuple_infos.len() {
                return Ok(());
            }
            let mut new_meta = header.tuple_infos[slot_idx].meta;
            new_meta.is_deleted = deleted;
            let heap = TableHeap::recovery_view(bpm.clone());
            let _ = heap.recover_set_tuple_meta(rid, new_meta);
            let _ = bpm.flush_page(page_id);
            return Ok(());
        }
        Ok(())
    }

    fn restore_tuple(
        &self,
        page_id: u32,
        slot_idx: usize,
        old_meta: crate::recovery::wal_record::TupleMetaRepr,
        old_bytes: &[u8],
    ) -> QuillSQLResult<()> {
        if let Some(bpm) = &self.buffer_pool {
            let heap = TableHeap::recovery_view(bpm.clone());
            let rid = crate::storage::page::RecordId::new(page_id, slot_idx as u32);
            let _ = heap.recover_set_tuple_bytes(rid, old_bytes);
            let restored_meta: crate::storage::page::TupleMeta = old_meta.into();
            let _ = heap.recover_set_tuple_meta(rid, restored_meta);
            let _ = bpm.flush_page(page_id);
            return Ok(());
        }
        let rx = self.disk_scheduler.schedule_read(page_id)?;
        let buf: BytesMut = rx.recv().map_err(|e| {
            crate::error::QuillSQLError::Internal(format!("WAL recovery read recv failed: {}", e))
        })??;
        let page_bytes = buf.to_vec();
        let (mut header, _hdr_len) = TablePageHeaderCodec::decode(&page_bytes)?;
        if slot_idx >= header.tuple_infos.len() {
            return Ok(());
        }
        let tuple_count = header.tuple_infos.len();
        let mut tuples_bytes: Vec<Vec<u8>> = Vec::with_capacity(tuple_count);
        for i in 0..tuple_count {
            let info = &header.tuple_infos[i];
            let slice = &page_bytes[info.offset as usize..(info.offset + info.size) as usize];
            if i == slot_idx {
                tuples_bytes.push(old_bytes.to_vec());
            } else {
                tuples_bytes.push(slice.to_vec());
            }
        }
        let mut tail = PAGE_SIZE;
        for i in 0..tuple_count {
            let size = tuples_bytes[i].len();
            tail = tail.saturating_sub(size);
            header.tuple_infos[i].offset = tail as u16;
            header.tuple_infos[i].size = size as u16;
        }
        let restored_meta: crate::storage::page::TupleMeta = old_meta.into();
        header.tuple_infos[slot_idx].meta = restored_meta;
        let new_header_bytes = TablePageHeaderCodec::encode(&header);
        let mut new_page = vec![0u8; PAGE_SIZE];
        let max_hdr = std::cmp::min(new_header_bytes.len(), PAGE_SIZE);
        new_page[0..max_hdr].copy_from_slice(&new_header_bytes[..max_hdr]);
        for i in 0..tuple_count {
            let off = header.tuple_infos[i].offset as usize;
            let sz = header.tuple_infos[i].size as usize;
            if off + sz <= PAGE_SIZE {
                new_page[off..off + sz].copy_from_slice(&tuples_bytes[i][..sz]);
            }
        }
        let rxw = self
            .disk_scheduler
            .schedule_write(page_id, Bytes::from(new_page))?;
        rxw.recv().map_err(|e| {
            crate::error::QuillSQLError::Internal(format!("WAL recovery write recv failed: {}", e))
        })??;
        Ok(())
    }
}

fn heap_txn_id(rec: &HeapRecordPayload) -> u64 {
    match rec {
        HeapRecordPayload::Insert(p) => p.op_txn_id,
        HeapRecordPayload::Update(p) => p.op_txn_id,
        HeapRecordPayload::Delete(p) => p.op_txn_id,
    }
}
