use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;

use crate::buffer::PAGE_SIZE;
use crate::error::QuillSQLResult;
use crate::recovery::wal_record::{
    CheckpointPayload, ClrPayload, HeapRecordPayload, PageDeltaPayload, PageWritePayload,
    TransactionPayload, TransactionRecordKind, WalRecordPayload,
};
use crate::recovery::{Lsn, WalManager};
use crate::storage::codec::TablePageHeaderCodec;
use crate::storage::disk_scheduler::DiskScheduler;
use crate::storage::table_heap::TableHeap;

pub struct RecoveryManager {
    wal: Arc<WalManager>,
    disk_scheduler: Arc<DiskScheduler>,
    buffer_pool: Option<Arc<crate::buffer::BufferPoolManager>>,
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
            buffer_pool: None,
        }
    }

    /// Optionally attach BufferPoolManager to enable TableHeap recovery APIs
    pub fn with_buffer_pool(mut self, bpm: Arc<crate::buffer::BufferPoolManager>) -> Self {
        self.buffer_pool = Some(bpm);
        self
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

        let mut frame_index = HashMap::with_capacity(frames.len());
        for (idx, frame) in frames.iter().enumerate() {
            frame_index.insert(frame.lsn, idx);
        }

        let cf_snapshot = self.wal.control_file().map(|ctrl| ctrl.snapshot());
        let (start_lsn, mut active_txns) = if let Some((checkpoint_lsn, payload)) = checkpoint {
            let checkpoint_redo_start = cf_snapshot
                .map(|snap| snap.checkpoint_redo_start)
                .filter(|redo| *redo >= payload.last_lsn && *redo <= checkpoint_lsn)
                .unwrap_or_else(|| {
                    payload
                        .dpt
                        .iter()
                        .map(|(_, lsn)| *lsn)
                        .min()
                        .unwrap_or(payload.last_lsn)
                });
            let active: HashSet<u64> = payload.active_transactions.iter().copied().collect();
            (checkpoint_redo_start, active)
        } else {
            (0, HashSet::new())
        };

        let mut redo_count = 0usize;
        let mut undo_heads: HashMap<u64, Option<Lsn>> = HashMap::new();
        let mut undo_links: HashMap<Lsn, Option<Lsn>> = HashMap::new();
        for frame in frames.iter() {
            match &frame.payload {
                WalRecordPayload::Heap(rec) => {
                    let txn_id = match rec {
                        HeapRecordPayload::Insert(p) => p.op_txn_id,
                        HeapRecordPayload::Update(p) => p.op_txn_id,
                        HeapRecordPayload::Delete(p) => p.op_txn_id,
                    };
                    let prev = undo_heads.get(&txn_id).copied().flatten();
                    undo_links.insert(frame.lsn, prev);
                    undo_heads.insert(txn_id, Some(frame.lsn));
                }
                WalRecordPayload::Clr(clr) => {
                    let next = if clr.undo_next_lsn == 0 {
                        None
                    } else {
                        Some(clr.undo_next_lsn)
                    };
                    undo_links.insert(frame.lsn, next);
                    undo_heads.insert(clr.txn_id, next);
                }
                WalRecordPayload::Transaction(tx) => {
                    self.update_active_transactions(&mut active_txns, tx, &mut undo_heads);
                }
                WalRecordPayload::Checkpoint(_) => {}
                _ => {}
            }

            if frame.lsn < start_lsn {
                continue;
            }

            match &frame.payload {
                WalRecordPayload::PageWrite(payload) => {
                    self.redo_page_write(payload.clone())?;
                    redo_count += 1;
                }
                WalRecordPayload::PageDelta(payload) => {
                    self.redo_page_delta(payload.clone())?;
                    redo_count += 1;
                }
                WalRecordPayload::Checkpoint(_) => {}
                WalRecordPayload::Clr(_) => {
                    // CLR redo is a no-op
                }
                WalRecordPayload::Heap(_) => {}
                WalRecordPayload::Transaction(_) => {}
            }
        }

        let mut losers: Vec<u64> = active_txns.clone().into_iter().collect();
        losers.sort_unstable();

        if !losers.is_empty() {
            log::warn!(
                "Recovery completed with {} outstanding transaction(s) requiring undo",
                losers.len()
            );
        }

        // Logical UNDO pass (reverse order): apply only loser heap records, and write CLR
        if !active_txns.is_empty() {
            for txn_id in active_txns.iter().copied() {
                let mut head = undo_heads.get(&txn_id).copied().flatten();
                while let Some(lsn) = head {
                    let next = undo_links.get(&lsn).copied().flatten();
                    if let Some(frame_idx) = frame_index.get(&lsn) {
                        if let Some(WalRecordPayload::Heap(rec)) =
                            frames.get(*frame_idx).map(|f| &f.payload)
                        {
                            self.undo_heap_record(rec)?;
                            let undo_next = next.unwrap_or(0);
                            let _ = self.wal.append_record_with(|_| {
                                WalRecordPayload::Clr(ClrPayload {
                                    txn_id,
                                    undone_lsn: lsn,
                                    undo_next_lsn: undo_next,
                                })
                            })?;
                        }
                    }

                    head = next;
                }
            }
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

    fn redo_page_delta(&self, payload: PageDeltaPayload) -> QuillSQLResult<()> {
        use bytes::BytesMut;
        // Read current page image
        let rx = self.disk_scheduler.schedule_read(payload.page_id)?;
        let buf: BytesMut = rx.recv().map_err(|e| {
            crate::error::QuillSQLError::Internal(format!("WAL recovery read recv failed: {}", e))
        })??;
        if buf.len() != PAGE_SIZE {
            return Err(crate::error::QuillSQLError::Internal(format!(
                "Unexpected page size {} while applying delta",
                buf.len()
            )));
        }
        let mut page_bytes = buf.to_vec();
        let start = payload.offset as usize;
        let end = start + payload.data.len();
        if end > PAGE_SIZE {
            return Err(crate::error::QuillSQLError::Internal(format!(
                "PageDelta out of bounds: offset={} len={} page_size={}",
                start,
                payload.data.len(),
                PAGE_SIZE
            )));
        }
        page_bytes[start..end].copy_from_slice(&payload.data);
        let rxw = self
            .disk_scheduler
            .schedule_write(payload.page_id, Bytes::from(page_bytes))?;
        rxw.recv().map_err(|e| {
            crate::error::QuillSQLError::Internal(format!("WAL recovery write recv failed: {}", e))
        })??;
        Ok(())
    }

    fn update_active_transactions(
        &self,
        active: &mut HashSet<u64>,
        txn: &TransactionPayload,
        undo_heads: &mut HashMap<u64, Option<Lsn>>,
    ) {
        match txn.marker {
            TransactionRecordKind::Begin => {
                active.insert(txn.txn_id);
            }
            TransactionRecordKind::Commit | TransactionRecordKind::Abort => {
                active.remove(&txn.txn_id);
                undo_heads.insert(txn.txn_id, None);
            }
        }
    }

    /// Undo heap-level changes using recover APIs (respects rewritten undo chains).
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

    /// Helper: set or clear is_deleted flag for a slot's TupleMeta (recovery path).
    fn apply_tuple_meta_flag(
        &self,
        page_id: u32,
        slot_idx: usize,
        deleted: bool,
    ) -> QuillSQLResult<()> {
        // Prefer buffer_pool path: read header to get current meta, toggle flag, persist via recovery API
        if let Some(bpm) = &self.buffer_pool {
            let rid = crate::storage::page::RecordId::new(page_id, slot_idx as u32);
            // Read current meta from header without decoding tuple payload
            let guard = bpm.fetch_page_read(page_id)?;
            let (header, _hdr_len) = TablePageHeaderCodec::decode(&guard.data)?;
            drop(guard);
            if slot_idx >= header.tuple_infos.len() {
                return Ok(());
            }
            let mut new_meta = header.tuple_infos[slot_idx].meta;
            new_meta.is_deleted = deleted;
            let heap = TableHeap::recovery_view(bpm.clone());
            let _ = heap.recover_set_tuple_meta(rid, new_meta);
            // Ensure visibility to disk for tests that read via DiskScheduler directly
            let _ = bpm.flush_page(page_id);
            return Ok(());
        }
        Ok(())
    }

    /// Helper: restore tuple meta and bytes for a slot.
    fn restore_tuple(
        &self,
        page_id: u32,
        slot_idx: usize,
        _old_meta: crate::recovery::wal_record::TupleMetaRepr,
        old_bytes: &[u8],
    ) -> QuillSQLResult<()> {
        if let Some(bpm) = &self.buffer_pool {
            let heap = TableHeap::recovery_view(bpm.clone());
            let rid = crate::storage::page::RecordId::new(page_id, slot_idx as u32);
            let _ = heap.recover_set_tuple_bytes(rid, old_bytes);
            let restored_meta: crate::storage::page::TupleMeta = _old_meta.into();
            let _ = heap.recover_set_tuple_meta(rid, restored_meta);
            // Flush to make changes visible to direct disk reads
            let _ = bpm.flush_page(page_id);
            return Ok(());
        }
        use bytes::BytesMut;
        let rx = self.disk_scheduler.schedule_read(page_id)?;
        let buf: BytesMut = rx.recv().map_err(|e| {
            crate::error::QuillSQLError::Internal(format!("WAL recovery read recv failed: {}", e))
        })??;
        let page_bytes = buf.to_vec();
        // Decode header to locate tuple slot
        let (mut header, _hdr_len) = TablePageHeaderCodec::decode(&page_bytes)?;
        if slot_idx >= header.tuple_infos.len() {
            return Ok(());
        }
        // Build new packed layout from existing tuples, replacing target with old_bytes
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
        // Repack from page tail
        let mut tail = PAGE_SIZE;
        for i in 0..tuple_count {
            let size = tuples_bytes[i].len();
            tail = tail.saturating_sub(size);
            header.tuple_infos[i].offset = tail as u16;
            header.tuple_infos[i].size = size as u16;
        }
        // Restore meta from old_meta for the target slot
        let restored_meta: crate::storage::page::TupleMeta = _old_meta.into();
        header.tuple_infos[slot_idx].meta = restored_meta;
        // Re-encode header and rebuild page bytes
        let new_header_bytes = TablePageHeaderCodec::encode(&header);
        // Reset page buffer to zeros
        let mut new_page = vec![0u8; PAGE_SIZE];
        // Copy header (cap to page size)
        let max_hdr = std::cmp::min(new_header_bytes.len(), PAGE_SIZE);
        new_page[0..max_hdr].copy_from_slice(&new_header_bytes[..max_hdr]);
        // Copy tuples into their new offsets
        for i in 0..tuple_count {
            let off = header.tuple_infos[i].offset as usize;
            let sz = header.tuple_infos[i].size as usize;
            if off + sz <= PAGE_SIZE {
                new_page[off..off + sz].copy_from_slice(&tuples_bytes[i][..sz]);
            }
        }
        let rxw = self
            .disk_scheduler
            .schedule_write(page_id, bytes::Bytes::from(new_page))?;
        rxw.recv().map_err(|e| {
            crate::error::QuillSQLError::Internal(format!("WAL recovery write recv failed: {}", e))
        })??;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::RecoveryManager;
    use crate::buffer::{BufferPoolManager, INVALID_PAGE_ID};
    use crate::config::WalConfig;
    use crate::recovery::wal_record::{
        PageDeltaPayload, PageWritePayload, TransactionPayload, TransactionRecordKind,
        WalRecordPayload,
    };
    use crate::recovery::WalManager;
    use crate::storage::codec::TablePageHeaderCodec;
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;
    use crate::storage::page::{TablePageHeader, TupleInfo, TupleMeta};
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
    fn replay_applies_page_delta() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("delta.db");
        let wal_dir = temp.path().join("wal");

        let scheduler = build_scheduler(&db_path);
        let mut config = WalConfig::default();
        config.directory = wal_dir.clone();
        config.sync_on_flush = false;
        let wal = Arc::new(WalManager::new(config.clone(), scheduler.clone(), None, None).unwrap());

        // Seed a page with zeros
        let zero = vec![0u8; crate::buffer::PAGE_SIZE];
        wal.append_record_with(|_| {
            WalRecordPayload::PageWrite(PageWritePayload {
                page_id: 2,
                prev_page_lsn: 0,
                page_image: zero.clone(),
            })
        })
        .unwrap();
        wal.flush(None).unwrap();

        // Apply a delta at offset 100
        wal.append_record_with(|_| {
            WalRecordPayload::PageDelta(PageDeltaPayload {
                page_id: 2,
                prev_page_lsn: 0,
                offset: 100,
                data: vec![1, 2, 3, 4, 5],
            })
        })
        .unwrap();
        wal.flush(None).unwrap();
        drop(wal);

        let scheduler = build_scheduler(&db_path);
        let wal = Arc::new(WalManager::new(config, scheduler.clone(), None, None).unwrap());
        let recovery = RecoveryManager::new(wal, scheduler.clone());
        let summary = recovery.replay().unwrap();
        assert!(summary.redo_count >= 2);

        let rx = scheduler.schedule_read(2).unwrap();
        let data = rx.recv().unwrap().unwrap();
        assert_eq!(data[100..105], [1, 2, 3, 4, 5]);
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

    #[test]
    fn undo_marks_insert_deleted_and_writes_clr() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("undo_insert.db");
        let wal_dir = temp.path().join("wal");

        let scheduler = build_scheduler(&db_path);
        let mut config = WalConfig::default();
        config.directory = wal_dir.clone();
        config.sync_on_flush = false;
        let wal = Arc::new(WalManager::new(config.clone(), scheduler.clone(), None, None).unwrap());

        // Build a page with 1 tuple (slot 0), not deleted
        let page_id = 3u32;
        let mut page = vec![0u8; crate::buffer::PAGE_SIZE];
        let header = TablePageHeader {
            next_page_id: INVALID_PAGE_ID,
            num_tuples: 1,
            num_deleted_tuples: 0,
            tuple_infos: vec![TupleInfo {
                offset: (crate::buffer::PAGE_SIZE - 16) as u16,
                size: 16,
                meta: TupleMeta {
                    insert_txn_id: 100,
                    delete_txn_id: 0,
                    is_deleted: false,
                },
            }],
            lsn: 0,
        };
        let header_bytes = TablePageHeaderCodec::encode(&header);
        let copy_len = std::cmp::min(header_bytes.len(), crate::buffer::PAGE_SIZE);
        page[0..copy_len].copy_from_slice(&header_bytes[..copy_len]);
        // Seed tuple bytes at the offset
        let off = header.tuple_infos[0].offset as usize;
        for i in 0..16 {
            page[off + i] = i as u8;
        }

        // Persist initial page
        scheduler
            .schedule_write(page_id, bytes::Bytes::from(page.clone()))
            .unwrap()
            .recv()
            .unwrap()
            .unwrap();

        // Write a loser transaction begin + heap insert record
        wal.append_record_with(|_| {
            WalRecordPayload::Transaction(TransactionPayload {
                marker: TransactionRecordKind::Begin,
                txn_id: 1,
            })
        })
        .unwrap();

        wal.append_record_with(|_| {
            WalRecordPayload::Heap(crate::recovery::wal_record::HeapRecordPayload::Insert(
                crate::recovery::wal_record::HeapInsertPayload {
                    relation: crate::recovery::wal_record::RelationIdent { root_page_id: 0 },
                    page_id,
                    slot_id: 0,
                    op_txn_id: 1,
                    tuple_meta: crate::recovery::wal_record::TupleMetaRepr {
                        insert_txn_id: 1,
                        delete_txn_id: 0,
                        is_deleted: false,
                    },
                    tuple_data: vec![0u8; 16],
                },
            ))
        })
        .unwrap();

        // Emit checkpoint with loser txn and DPT
        let last = wal.max_assigned_lsn();
        wal.append_record_with(|_| {
            WalRecordPayload::Checkpoint(crate::recovery::wal_record::CheckpointPayload {
                last_lsn: last,
                dirty_pages: vec![page_id],
                active_transactions: vec![1],
                dpt: vec![(page_id, last)],
            })
        })
        .unwrap();
        wal.flush(None).unwrap();
        drop(wal);

        // Reopen wal and run recovery (inject BufferPool to use TableHeap recovery APIs)
        let scheduler = build_scheduler(&db_path);
        let bpm = Arc::new(BufferPoolManager::new(64, scheduler.clone()));
        let wal = Arc::new(WalManager::new(config, scheduler.clone(), None, None).unwrap());
        let recovery = RecoveryManager::new(wal, scheduler.clone()).with_buffer_pool(bpm);
        let _summary = recovery.replay().unwrap();

        // Verify header is_deleted flipped to true
        let rx = scheduler.schedule_read(page_id).unwrap();
        let data = rx.recv().unwrap().unwrap();
        let (header2, _c) = TablePageHeaderCodec::decode(&data).unwrap();
        assert_eq!(header2.tuple_infos[0].meta.is_deleted, true);
    }

    #[test]
    fn undo_update_restores_old_bytes_and_meta() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("undo_update.db");
        let wal_dir = temp.path().join("wal");

        let scheduler = build_scheduler(&db_path);
        let mut config = WalConfig::default();
        config.directory = wal_dir.clone();
        config.sync_on_flush = false;
        let wal = Arc::new(WalManager::new(config.clone(), scheduler.clone(), None, None).unwrap());

        let page_id = 4u32;
        // Build header with 1 slot
        let old_meta = TupleMeta {
            insert_txn_id: 1,
            delete_txn_id: 0,
            is_deleted: false,
        };
        let header = TablePageHeader {
            next_page_id: INVALID_PAGE_ID,
            num_tuples: 1,
            num_deleted_tuples: 0,
            tuple_infos: vec![TupleInfo {
                offset: (crate::buffer::PAGE_SIZE - 16) as u16,
                size: 16,
                meta: old_meta,
            }],
            lsn: 0,
        };
        let mut page = vec![0u8; crate::buffer::PAGE_SIZE];
        let header_bytes = TablePageHeaderCodec::encode(&header);
        page[..header_bytes.len()].copy_from_slice(&header_bytes);
        let off = header.tuple_infos[0].offset as usize;
        // old bytes
        for i in 0..16 {
            page[off + i] = (i as u8) ^ 0xAA;
        }
        scheduler
            .schedule_write(page_id, bytes::Bytes::from(page.clone()))
            .unwrap()
            .recv()
            .unwrap()
            .unwrap();

        // Create an update WAL (loser txn)
        wal.append_record_with(|_| {
            WalRecordPayload::Transaction(TransactionPayload {
                marker: TransactionRecordKind::Begin,
                txn_id: 2,
            })
        })
        .unwrap();
        let new_meta = TupleMeta {
            insert_txn_id: 1,
            delete_txn_id: 0,
            is_deleted: false,
        };
        let old_tuple_bytes = vec![0u8 ^ 0xAA; 16];
        let new_tuple_bytes = vec![0xFF; 24]; // different size to exercise repack
        wal.append_record_with(|_| {
            WalRecordPayload::Heap(crate::recovery::wal_record::HeapRecordPayload::Update(
                crate::recovery::wal_record::HeapUpdatePayload {
                    relation: crate::recovery::wal_record::RelationIdent { root_page_id: 0 },
                    page_id,
                    slot_id: 0,
                    op_txn_id: 2,
                    new_tuple_meta: crate::recovery::wal_record::TupleMetaRepr::from(new_meta),
                    new_tuple_data: new_tuple_bytes.clone(),
                    old_tuple_meta: Some(crate::recovery::wal_record::TupleMetaRepr::from(
                        old_meta,
                    )),
                    old_tuple_data: Some(old_tuple_bytes.clone()),
                },
            ))
        })
        .unwrap();
        // checkpoint
        let last = wal.max_assigned_lsn();
        wal.append_record_with(|_| {
            WalRecordPayload::Checkpoint(crate::recovery::wal_record::CheckpointPayload {
                last_lsn: last,
                dirty_pages: vec![page_id],
                active_transactions: vec![2],
                dpt: vec![(page_id, last)],
            })
        })
        .unwrap();
        wal.flush(None).unwrap();
        drop(wal);

        // Simulate on-disk "new" bytes and size before recovery
        let mut header2 = header;
        let mut page2 = vec![0u8; crate::buffer::PAGE_SIZE];
        header2.tuple_infos[0].size = 24;
        header2.tuple_infos[0].offset = (crate::buffer::PAGE_SIZE - 24) as u16;
        let header_bytes2 = TablePageHeaderCodec::encode(&header2);
        page2[..header_bytes2.len()].copy_from_slice(&header_bytes2);
        let off2 = header2.tuple_infos[0].offset as usize;
        page2[off2..off2 + 24].copy_from_slice(&new_tuple_bytes);
        scheduler
            .schedule_write(page_id, bytes::Bytes::from(page2))
            .unwrap()
            .recv()
            .unwrap()
            .unwrap();

        // Recover (inject BufferPool)
        let scheduler = build_scheduler(&db_path);
        let bpm = Arc::new(BufferPoolManager::new(64, scheduler.clone()));
        let wal = Arc::new(WalManager::new(config, scheduler.clone(), None, None).unwrap());
        let recovery = RecoveryManager::new(wal, scheduler.clone()).with_buffer_pool(bpm);
        let _ = recovery.replay().unwrap();

        // Verify old bytes restored and size back to 16
        let rx = scheduler.schedule_read(page_id).unwrap();
        let data = rx.recv().unwrap().unwrap();
        let (hdr, _c) = TablePageHeaderCodec::decode(&data).unwrap();
        assert_eq!(hdr.tuple_infos[0].size, 16);
        let off3 = hdr.tuple_infos[0].offset as usize;
        assert_eq!(&data[off3..off3 + 16], &old_tuple_bytes[..]);
    }

    #[test]
    fn undo_delete_restores_visibility_and_bytes() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("undo_delete.db");
        let wal_dir = temp.path().join("wal");

        let scheduler = build_scheduler(&db_path);
        let mut config = WalConfig::default();
        config.directory = wal_dir.clone();
        config.sync_on_flush = false;
        let wal = Arc::new(WalManager::new(config.clone(), scheduler.clone(), None, None).unwrap());

        let page_id = 5u32;
        let old_meta = TupleMeta {
            insert_txn_id: 5,
            delete_txn_id: 0,
            is_deleted: false,
        };
        let mut header = TablePageHeader {
            next_page_id: INVALID_PAGE_ID,
            num_tuples: 1,
            num_deleted_tuples: 0,
            tuple_infos: vec![TupleInfo {
                offset: (crate::buffer::PAGE_SIZE - 8) as u16,
                size: 8,
                meta: old_meta,
            }],
            lsn: 0,
        };
        let mut page = vec![0u8; crate::buffer::PAGE_SIZE];
        let header_bytes = TablePageHeaderCodec::encode(&header);
        page[..header_bytes.len()].copy_from_slice(&header_bytes);
        let off = header.tuple_infos[0].offset as usize;
        let old_bytes = vec![9u8; 8];
        page[off..off + 8].copy_from_slice(&old_bytes);
        scheduler
            .schedule_write(page_id, bytes::Bytes::from(page))
            .unwrap()
            .recv()
            .unwrap()
            .unwrap();

        wal.append_record_with(|_| {
            WalRecordPayload::Transaction(TransactionPayload {
                marker: TransactionRecordKind::Begin,
                txn_id: 6,
            })
        })
        .unwrap();
        wal.append_record_with(|_| {
            WalRecordPayload::Heap(crate::recovery::wal_record::HeapRecordPayload::Delete(
                crate::recovery::wal_record::HeapDeletePayload {
                    relation: crate::recovery::wal_record::RelationIdent { root_page_id: 0 },
                    page_id,
                    slot_id: 0,
                    op_txn_id: 6,
                    old_tuple_meta: crate::recovery::wal_record::TupleMetaRepr::from(old_meta),
                    old_tuple_data: Some(old_bytes.clone()),
                },
            ))
        })
        .unwrap();
        let last = wal.max_assigned_lsn();
        wal.append_record_with(|_| {
            WalRecordPayload::Checkpoint(crate::recovery::wal_record::CheckpointPayload {
                last_lsn: last,
                dirty_pages: vec![page_id],
                active_transactions: vec![6],
                dpt: vec![(page_id, last)],
            })
        })
        .unwrap();
        wal.flush(None).unwrap();
        drop(wal);

        // simulate deleted on disk
        let mut page2 = vec![0u8; crate::buffer::PAGE_SIZE];
        header.tuple_infos[0].meta.is_deleted = true;
        header.num_deleted_tuples = 1;
        let h2 = TablePageHeaderCodec::encode(&header);
        page2[..h2.len()].copy_from_slice(&h2);
        scheduler
            .schedule_write(page_id, bytes::Bytes::from(page2))
            .unwrap()
            .recv()
            .unwrap()
            .unwrap();

        // recover (inject BufferPool)
        let scheduler = build_scheduler(&db_path);
        let bpm = Arc::new(BufferPoolManager::new(64, scheduler.clone()));
        let wal = Arc::new(WalManager::new(config, scheduler.clone(), None, None).unwrap());
        let recovery = RecoveryManager::new(wal, scheduler.clone()).with_buffer_pool(bpm);
        let _ = recovery.replay().unwrap();

        let rx = scheduler.schedule_read(page_id).unwrap();
        let data = rx.recv().unwrap().unwrap();
        let (hdr, _c) = TablePageHeaderCodec::decode(&data).unwrap();
        assert_eq!(hdr.tuple_infos[0].meta.is_deleted, false);
        let off3 = hdr.tuple_infos[0].offset as usize;
        assert_eq!(&data[off3..off3 + 8], &old_bytes[..]);
    }

    #[test]
    fn replay_is_idempotent_on_page_state() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("idem.db");
        let wal_dir = temp.path().join("wal");

        let scheduler = build_scheduler(&db_path);
        let mut config = WalConfig::default();
        config.directory = wal_dir.clone();
        config.sync_on_flush = false;
        let wal = Arc::new(WalManager::new(config.clone(), scheduler.clone(), None, None).unwrap());

        // Seed a page with one visible tuple
        let page_id = 6u32;
        let header = TablePageHeader {
            next_page_id: INVALID_PAGE_ID,
            num_tuples: 1,
            num_deleted_tuples: 0,
            tuple_infos: vec![TupleInfo {
                offset: (crate::buffer::PAGE_SIZE - 8) as u16,
                size: 8,
                meta: TupleMeta {
                    insert_txn_id: 9,
                    delete_txn_id: 0,
                    is_deleted: false,
                },
            }],
            lsn: 0,
        };
        let mut page = vec![0u8; crate::buffer::PAGE_SIZE];
        let hb = TablePageHeaderCodec::encode(&header);
        page[..hb.len()].copy_from_slice(&hb);
        scheduler
            .schedule_write(page_id, bytes::Bytes::from(page))
            .unwrap()
            .recv()
            .unwrap()
            .unwrap();

        // Loser insert (we just need op_txn_id=10 and a checkpoint active_txns=[10])
        wal.append_record_with(|_| {
            WalRecordPayload::Transaction(TransactionPayload {
                marker: TransactionRecordKind::Begin,
                txn_id: 10,
            })
        })
        .unwrap();
        wal.append_record_with(|_| {
            WalRecordPayload::Heap(crate::recovery::wal_record::HeapRecordPayload::Insert(
                crate::recovery::wal_record::HeapInsertPayload {
                    relation: crate::recovery::wal_record::RelationIdent { root_page_id: 0 },
                    page_id,
                    slot_id: 0,
                    op_txn_id: 10,
                    tuple_meta: crate::recovery::wal_record::TupleMetaRepr {
                        insert_txn_id: 10,
                        delete_txn_id: 0,
                        is_deleted: false,
                    },
                    tuple_data: vec![0u8; 8],
                },
            ))
        })
        .unwrap();
        let last = wal.max_assigned_lsn();
        wal.append_record_with(|_| {
            WalRecordPayload::Checkpoint(crate::recovery::wal_record::CheckpointPayload {
                last_lsn: last,
                dirty_pages: vec![page_id],
                active_transactions: vec![10],
                dpt: vec![(page_id, last)],
            })
        })
        .unwrap();
        wal.flush(None).unwrap();
        drop(wal);

        // First recovery (inject BufferPool)
        let scheduler = build_scheduler(&db_path);
        let bpm = Arc::new(BufferPoolManager::new(64, scheduler.clone()));
        let wal = Arc::new(WalManager::new(config.clone(), scheduler.clone(), None, None).unwrap());
        let recovery = RecoveryManager::new(wal, scheduler.clone()).with_buffer_pool(bpm);
        let _ = recovery.replay().unwrap();
        let rx = scheduler.schedule_read(page_id).unwrap();
        let data1 = rx.recv().unwrap().unwrap();
        let (hdr1, _c1) = TablePageHeaderCodec::decode(&data1).unwrap();
        assert!(hdr1.tuple_infos[0].meta.is_deleted);

        // Second recovery (should not change page state) - inject BufferPool again
        let scheduler2 = build_scheduler(&db_path);
        let bpm2 = Arc::new(BufferPoolManager::new(64, scheduler2.clone()));
        let wal2 = Arc::new(WalManager::new(config, scheduler2.clone(), None, None).unwrap());
        let recovery2 = RecoveryManager::new(wal2, scheduler2.clone()).with_buffer_pool(bpm2);
        let _ = recovery2.replay().unwrap();
        let rx2 = scheduler2.schedule_read(page_id).unwrap();
        let data2 = rx2.recv().unwrap().unwrap();
        let (hdr2, _c2) = TablePageHeaderCodec::decode(&data2).unwrap();
        assert!(hdr2.tuple_infos[0].meta.is_deleted);
    }

    #[test]
    fn redo_uses_dpt_min_lsn_across_segments() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("dpt_segments.db");
        let wal_dir = temp.path().join("wal");

        let scheduler = build_scheduler(&db_path);
        let mut config = WalConfig::default();
        config.directory = wal_dir.clone();
        config.sync_on_flush = false;
        config.segment_size = 1024; // force frequent rotation
        let wal = Arc::new(WalManager::new(config.clone(), scheduler.clone(), None, None).unwrap());

        // Write initial full page at pid=7
        let pid = 7u32;
        let base = vec![0u8; crate::buffer::PAGE_SIZE];
        wal.append_record_with(|_| {
            WalRecordPayload::PageWrite(PageWritePayload {
                page_id: pid,
                prev_page_lsn: 0,
                page_image: base.clone(),
            })
        })
        .unwrap();
        wal.flush(None).unwrap();

        // Write many small deltas to rotate segments, record an early recLSN
        let mut first_delta_lsn = 0u64;
        for i in 0..128u16 {
            // enough to rotate
            let res = wal
                .append_record_with(|_| {
                    WalRecordPayload::PageDelta(PageDeltaPayload {
                        page_id: pid,
                        prev_page_lsn: 0,
                        offset: (i % 32) * 8,
                        data: vec![i as u8; 8],
                    })
                })
                .unwrap();
            if i == 0 {
                first_delta_lsn = res.start_lsn;
            }
        }

        // Emit checkpoint with DPT recLSN = first_delta_lsn
        wal.append_record_with(|_| {
            WalRecordPayload::Checkpoint(crate::recovery::wal_record::CheckpointPayload {
                last_lsn: wal.max_assigned_lsn(),
                dirty_pages: vec![pid],
                active_transactions: vec![],
                dpt: vec![(pid, first_delta_lsn)],
            })
        })
        .unwrap();
        wal.flush(None).unwrap();
        drop(wal);

        // Recover and ensure deltas applied
        let scheduler = build_scheduler(&db_path);
        let wal = Arc::new(WalManager::new(config, scheduler.clone(), None, None).unwrap());
        let recovery = RecoveryManager::new(wal, scheduler.clone());
        let summary = recovery.replay().unwrap();
        assert!(summary.redo_count >= 1);

        let rx = scheduler.schedule_read(pid).unwrap();
        let data = rx.recv().unwrap().unwrap();
        // spot-check a few offsets
        for i in 0..4u16 {
            let off = ((i % 32) * 8) as usize;
            // Last writer for this offset is i + 96 within 0..128 sequence
            let expected = vec![(i + 96) as u8; 8];
            assert_eq!(&data[off..off + 8], &expected[..]);
        }
    }

    #[test]
    fn fpw_and_delta_replay_consistent() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("fpw_delta.db");
        let wal_dir = temp.path().join("wal");

        let scheduler = build_scheduler(&db_path);
        let mut config = WalConfig::default();
        config.directory = wal_dir.clone();
        config.sync_on_flush = false;
        let wal = Arc::new(WalManager::new(config.clone(), scheduler.clone(), None, None).unwrap());

        let pid = 8u32;
        // First touch should be FPW (simulate TableHeap behavior by writing a full page)
        let mut page = vec![0u8; crate::buffer::PAGE_SIZE];
        page[100..104].copy_from_slice(&[1, 2, 3, 4]);
        wal.append_record_with(|_| {
            WalRecordPayload::PageWrite(PageWritePayload {
                page_id: pid,
                prev_page_lsn: 0,
                page_image: page.clone(),
            })
        })
        .unwrap();
        // Subsequent small change as delta
        wal.append_record_with(|_| {
            WalRecordPayload::PageDelta(PageDeltaPayload {
                page_id: pid,
                prev_page_lsn: 0,
                offset: 102,
                data: vec![9, 9],
            })
        })
        .unwrap();
        wal.flush(None).unwrap();
        drop(wal);

        // Recover and verify
        let scheduler = build_scheduler(&db_path);
        let wal = Arc::new(WalManager::new(config, scheduler.clone(), None, None).unwrap());
        let recovery = RecoveryManager::new(wal, scheduler.clone());
        let _ = recovery.replay().unwrap();
        let rx = scheduler.schedule_read(pid).unwrap();
        let data = rx.recv().unwrap().unwrap();
        assert_eq!(&data[100..104], &[1, 2, 9, 9]);
    }

    #[test]
    fn partial_rollback_multi_update_idempotent() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("multi_update.db");
        let wal_dir = temp.path().join("wal");

        let scheduler = build_scheduler(&db_path);
        let mut config = WalConfig::default();
        config.directory = wal_dir.clone();
        config.sync_on_flush = false;
        let wal = Arc::new(WalManager::new(config.clone(), scheduler.clone(), None, None).unwrap());

        // Seed a page with one tuple of 16 bytes
        let page_id = 9u32;
        let header = TablePageHeader {
            next_page_id: INVALID_PAGE_ID,
            num_tuples: 1,
            num_deleted_tuples: 0,
            tuple_infos: vec![TupleInfo {
                offset: (crate::buffer::PAGE_SIZE - 16) as u16,
                size: 16,
                meta: TupleMeta {
                    insert_txn_id: 1,
                    delete_txn_id: 0,
                    is_deleted: false,
                },
            }],
            lsn: 0,
        };
        let mut page = vec![0u8; crate::buffer::PAGE_SIZE];
        let hb = TablePageHeaderCodec::encode(&header);
        page[..hb.len()].copy_from_slice(&hb);
        let off = header.tuple_infos[0].offset as usize;
        let orig = vec![7u8; 16];
        page[off..off + 16].copy_from_slice(&orig);
        scheduler
            .schedule_write(page_id, bytes::Bytes::from(page))
            .unwrap()
            .recv()
            .unwrap()
            .unwrap();

        // Loser txn performs two updates (different sizes)
        wal.append_record_with(|_| {
            WalRecordPayload::Transaction(TransactionPayload {
                marker: TransactionRecordKind::Begin,
                txn_id: 42,
            })
        })
        .unwrap();
        // First update expands tuple to 20 bytes
        wal.append_record_with(|_| {
            WalRecordPayload::Heap(crate::recovery::wal_record::HeapRecordPayload::Update(
                crate::recovery::wal_record::HeapUpdatePayload {
                    relation: crate::recovery::wal_record::RelationIdent { root_page_id: 0 },
                    page_id,
                    slot_id: 0,
                    op_txn_id: 42,
                    new_tuple_meta: crate::recovery::wal_record::TupleMetaRepr::from(TupleMeta {
                        insert_txn_id: 1,
                        delete_txn_id: 0,
                        is_deleted: false,
                    }),
                    new_tuple_data: vec![0x11; 20],
                    old_tuple_meta: Some(crate::recovery::wal_record::TupleMetaRepr::from(
                        TupleMeta {
                            insert_txn_id: 1,
                            delete_txn_id: 0,
                            is_deleted: false,
                        },
                    )),
                    old_tuple_data: Some(orig.clone()),
                },
            ))
        })
        .unwrap();
        // Second update shrinks to 8 bytes
        wal.append_record_with(|_| {
            WalRecordPayload::Heap(crate::recovery::wal_record::HeapRecordPayload::Update(
                crate::recovery::wal_record::HeapUpdatePayload {
                    relation: crate::recovery::wal_record::RelationIdent { root_page_id: 0 },
                    page_id,
                    slot_id: 0,
                    op_txn_id: 42,
                    new_tuple_meta: crate::recovery::wal_record::TupleMetaRepr::from(TupleMeta {
                        insert_txn_id: 1,
                        delete_txn_id: 0,
                        is_deleted: false,
                    }),
                    new_tuple_data: vec![0x22; 8],
                    old_tuple_meta: Some(crate::recovery::wal_record::TupleMetaRepr::from(
                        TupleMeta {
                            insert_txn_id: 1,
                            delete_txn_id: 0,
                            is_deleted: false,
                        },
                    )),
                    old_tuple_data: Some(vec![0x11; 20]),
                },
            ))
        })
        .unwrap();
        let last = wal.max_assigned_lsn();
        wal.append_record_with(|_| {
            WalRecordPayload::Checkpoint(crate::recovery::wal_record::CheckpointPayload {
                last_lsn: last,
                dirty_pages: vec![page_id],
                active_transactions: vec![42],
                dpt: vec![(page_id, last)],
            })
        })
        .unwrap();
        wal.flush(None).unwrap();

        // First recovery should undo both updates and write two CLRs
        let scheduler1 = build_scheduler(&db_path);
        let bpm1 = Arc::new(BufferPoolManager::new(64, scheduler1.clone()));
        let wal1 =
            Arc::new(WalManager::new(config.clone(), scheduler1.clone(), None, None).unwrap());
        let recovery1 =
            RecoveryManager::new(wal1.clone(), scheduler1.clone()).with_buffer_pool(bpm1);
        let _ = recovery1.replay().unwrap();
        // Ensure CLRs appended during UNDO are durable before reading WAL files
        wal1.flush(None).unwrap();
        let rx = scheduler1.schedule_read(page_id).unwrap();
        let data = rx.recv().unwrap().unwrap();
        let (hdr, _c) = TablePageHeaderCodec::decode(&data).unwrap();
        assert_eq!(hdr.tuple_infos[0].size, 16);
        let off2 = hdr.tuple_infos[0].offset as usize;
        assert_eq!(&data[off2..off2 + 16], &orig[..]);
        // Count CLRs
        let mut r = wal1.reader().unwrap();
        let mut clr_cnt = 0usize;
        while let Some(f) = r.next_frame().unwrap() {
            if let WalRecordPayload::Clr(_) = f.payload {
                clr_cnt += 1;
            }
        }
        assert!(clr_cnt >= 2);

        // Second recovery: no further changes
        let scheduler2 = build_scheduler(&db_path);
        let bpm2 = Arc::new(BufferPoolManager::new(64, scheduler2.clone()));
        let wal2 =
            Arc::new(WalManager::new(config.clone(), scheduler2.clone(), None, None).unwrap());
        let recovery2 = RecoveryManager::new(wal2, scheduler2.clone()).with_buffer_pool(bpm2);
        let _ = recovery2.replay().unwrap();
        let rx2 = scheduler2.schedule_read(page_id).unwrap();
        let data2 = rx2.recv().unwrap().unwrap();
        let (hdr2, _c2) = TablePageHeaderCodec::decode(&data2).unwrap();
        assert_eq!(hdr2.tuple_infos[0].size, 16);
        let off3 = hdr2.tuple_infos[0].offset as usize;
        assert_eq!(&data2[off3..off3 + 16], &orig[..]);
    }

    #[test]
    fn undo_across_two_pages_idempotent() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("two_pages.db");
        let wal_dir = temp.path().join("wal");

        let scheduler = build_scheduler(&db_path);
        let mut config = WalConfig::default();
        config.directory = wal_dir.clone();
        config.sync_on_flush = false;
        let wal = Arc::new(WalManager::new(config.clone(), scheduler.clone(), None, None).unwrap());

        let pid_a = 10u32;
        let pid_b = 11u32;
        for (pid, bytes) in [(pid_a, vec![1u8; 8]), (pid_b, vec![2u8; 12])] {
            let header = TablePageHeader {
                next_page_id: INVALID_PAGE_ID,
                num_tuples: 1,
                num_deleted_tuples: 0,
                tuple_infos: vec![TupleInfo {
                    offset: (crate::buffer::PAGE_SIZE - bytes.len()) as u16,
                    size: bytes.len() as u16,
                    meta: TupleMeta {
                        insert_txn_id: 5,
                        delete_txn_id: 0,
                        is_deleted: false,
                    },
                }],
                lsn: 0,
            };
            let mut page = vec![0u8; crate::buffer::PAGE_SIZE];
            let hb = TablePageHeaderCodec::encode(&header);
            page[..hb.len()].copy_from_slice(&hb);
            let off = header.tuple_infos[0].offset as usize;
            page[off..off + bytes.len()].copy_from_slice(&bytes);
            scheduler
                .schedule_write(pid, bytes::Bytes::from(page))
                .unwrap()
                .recv()
                .unwrap()
                .unwrap();
        }

        wal.append_record_with(|_| {
            WalRecordPayload::Transaction(TransactionPayload {
                marker: TransactionRecordKind::Begin,
                txn_id: 99,
            })
        })
        .unwrap();
        wal.append_record_with(|_| {
            WalRecordPayload::Heap(crate::recovery::wal_record::HeapRecordPayload::Insert(
                crate::recovery::wal_record::HeapInsertPayload {
                    relation: crate::recovery::wal_record::RelationIdent { root_page_id: 0 },
                    page_id: pid_a,
                    slot_id: 0,
                    op_txn_id: 99,
                    tuple_meta: crate::recovery::wal_record::TupleMetaRepr {
                        insert_txn_id: 99,
                        delete_txn_id: 0,
                        is_deleted: false,
                    },
                    tuple_data: vec![0u8; 8],
                },
            ))
        })
        .unwrap();
        wal.append_record_with(|_| {
            WalRecordPayload::Heap(crate::recovery::wal_record::HeapRecordPayload::Delete(
                crate::recovery::wal_record::HeapDeletePayload {
                    relation: crate::recovery::wal_record::RelationIdent { root_page_id: 0 },
                    page_id: pid_a,
                    slot_id: 0,
                    op_txn_id: 99,
                    old_tuple_meta: crate::recovery::wal_record::TupleMetaRepr::from(TupleMeta {
                        insert_txn_id: 5,
                        delete_txn_id: 0,
                        is_deleted: false,
                    }),
                    old_tuple_data: Some(vec![1u8; 8]),
                },
            ))
        })
        .unwrap();
        wal.append_record_with(|_| {
            WalRecordPayload::Heap(crate::recovery::wal_record::HeapRecordPayload::Update(
                crate::recovery::wal_record::HeapUpdatePayload {
                    relation: crate::recovery::wal_record::RelationIdent { root_page_id: 0 },
                    page_id: pid_b,
                    slot_id: 0,
                    op_txn_id: 99,
                    new_tuple_meta: crate::recovery::wal_record::TupleMetaRepr::from(TupleMeta {
                        insert_txn_id: 5,
                        delete_txn_id: 0,
                        is_deleted: false,
                    }),
                    new_tuple_data: vec![9u8; 10],
                    old_tuple_meta: Some(crate::recovery::wal_record::TupleMetaRepr::from(
                        TupleMeta {
                            insert_txn_id: 5,
                            delete_txn_id: 0,
                            is_deleted: false,
                        },
                    )),
                    old_tuple_data: Some(vec![2u8; 12]),
                },
            ))
        })
        .unwrap();
        let last = wal.max_assigned_lsn();
        wal.append_record_with(|_| {
            WalRecordPayload::Checkpoint(crate::recovery::wal_record::CheckpointPayload {
                last_lsn: last,
                dirty_pages: vec![pid_a, pid_b],
                active_transactions: vec![99],
                dpt: vec![(pid_a, last), (pid_b, last)],
            })
        })
        .unwrap();
        wal.flush(None).unwrap();

        for _ in 0..2 {
            let scheduler_r = build_scheduler(&db_path);
            let bpm_r = Arc::new(BufferPoolManager::new(64, scheduler_r.clone()));
            let wal_r =
                Arc::new(WalManager::new(config.clone(), scheduler_r.clone(), None, None).unwrap());
            let recovery = RecoveryManager::new(wal_r, scheduler_r.clone()).with_buffer_pool(bpm_r);
            let _ = recovery.replay().unwrap();

            let rx_a = scheduler_r.schedule_read(pid_a).unwrap();
            let data_a = rx_a.recv().unwrap().unwrap();
            let (hdr_a, _ca) = TablePageHeaderCodec::decode(&data_a).unwrap();
            assert_eq!(hdr_a.tuple_infos[0].meta.is_deleted, true);

            let rx_b = scheduler_r.schedule_read(pid_b).unwrap();
            let data_b = rx_b.recv().unwrap().unwrap();
            let (hdr_b, _cb) = TablePageHeaderCodec::decode(&data_b).unwrap();
            assert_eq!(hdr_b.tuple_infos[0].size, 12);
            let offb = hdr_b.tuple_infos[0].offset as usize;
            assert_eq!(&data_b[offb..offb + 12], &vec![2u8; 12][..]);
        }
    }
}
