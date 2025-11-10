use std::sync::Arc;

use crate::buffer::BufferManager;
use crate::error::QuillSQLResult;
use crate::recovery::analysis::{AnalysisPass, AnalysisResult};
use crate::recovery::redo::RedoExecutor;
use crate::recovery::undo::{UndoExecutor, UndoOutcome};
use crate::recovery::wal::codec::WalFrame;
use crate::recovery::{Lsn, WalManager};
use crate::storage::disk_scheduler::DiskScheduler;

pub struct RecoveryManager {
    wal: Arc<WalManager>,
    disk_scheduler: Arc<DiskScheduler>,
    buffer_pool: Option<Arc<BufferManager>>,
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

    /// Optionally attach BufferManager to enable TableHeap recovery APIs
    pub fn with_buffer_pool(mut self, bpm: Arc<BufferManager>) -> Self {
        self.buffer_pool = Some(bpm);
        self
    }

    pub fn replay(&self) -> QuillSQLResult<RecoverySummary> {
        let mut reader = match self.wal.reader() {
            Ok(r) => r,
            Err(_) => return Ok(RecoverySummary::default()),
        };

        let control_snapshot = self.wal.control_file().map(|ctrl| ctrl.snapshot());
        let mut analysis = AnalysisPass::new(control_snapshot);
        let mut undo = UndoExecutor::new(
            self.wal.clone(),
            self.disk_scheduler.clone(),
            self.buffer_pool.clone(),
        );
        let redo = RedoExecutor::new(self.disk_scheduler.clone(), self.buffer_pool.clone());

        let mut frames: Vec<WalFrame> = Vec::new();
        while let Some(frame) = reader.next_frame()? {
            analysis.observe(&frame);
            undo.observe(&frame)?;
            frames.push(frame);
        }

        let AnalysisResult {
            start_lsn,
            has_frames,
        } = analysis.finalize()?;

        if !has_frames {
            return Ok(RecoverySummary::default());
        }

        let mut redo_count = 0usize;
        for frame in &frames {
            if frame.lsn < start_lsn {
                continue;
            }
            redo_count += redo.apply(frame)?;
        }

        let UndoOutcome {
            loser_transactions,
            max_clr_lsn,
        } = undo.finalize()?;

        if !loser_transactions.is_empty() {
            log::warn!(
                "Recovery completed with {} outstanding transaction(s) requiring undo",
                loser_transactions.len()
            );
        }

        if max_clr_lsn != 0 {
            self.wal.flush_until(max_clr_lsn)?;
        }

        Ok(RecoverySummary {
            start_lsn,
            redo_count,
            loser_transactions,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RecoveryManager;
    use crate::buffer::{BufferManager, INVALID_PAGE_ID};
    use crate::catalog::{Column, DataType, Schema};
    use crate::config::{IOSchedulerConfig, WalConfig};
    use crate::recovery::analysis::{AnalysisPass, AnalysisResult};
    use crate::recovery::redo::RedoExecutor;
    use crate::recovery::undo::{UndoExecutor, UndoOutcome};
    use crate::recovery::wal::page::WAL_PAGE_SIZE;
    use crate::recovery::wal_record::{
        HeapDeletePayload, HeapInsertPayload, HeapRecordPayload, HeapUpdatePayload,
        PageDeltaPayload, PageWritePayload, RelationIdent, TransactionPayload,
        TransactionRecordKind, TupleMetaRepr, WalRecordPayload,
    };
    use crate::recovery::WalManager;
    use crate::storage::codec::TablePageHeaderCodec;
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;
    use crate::storage::index::btree_index::BPlusTreeIndex;
    use crate::storage::page::{RecordId, TablePageHeader, TupleInfo, TupleMeta};
    use crate::storage::tuple::Tuple;
    use crate::transaction::INVALID_COMMAND_ID;
    use crate::utils::scalar::ScalarValue;
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn build_scheduler(db_path: &Path) -> Arc<DiskScheduler> {
        std::env::set_var("QUILL_DISABLE_DIRECT_IO", "1");
        let disk_manager = Arc::new(DiskManager::try_new(db_path).expect("disk manager"));
        let mut io_config = IOSchedulerConfig::default();
        io_config.workers = 1;
        #[cfg(target_os = "linux")]
        {
            io_config.iouring_queue_depth = 8;
            io_config.iouring_fixed_buffers = 8;
        }
        Arc::new(DiskScheduler::new_with_config(disk_manager, io_config))
    }

    fn build_wal(config: WalConfig, db_path: &Path) -> (Arc<WalManager>, Arc<DiskScheduler>) {
        let scheduler = build_scheduler(db_path);
        let wal = Arc::new(
            WalManager::new_with_scheduler(config, None, None, scheduler.clone()).unwrap(),
        );
        (wal, scheduler)
    }

    fn run_stage_pipeline(
        wal: Arc<WalManager>,
        scheduler: Arc<DiskScheduler>,
        buffer_pool: Option<Arc<BufferManager>>,
    ) -> (AnalysisResult, UndoOutcome, usize) {
        let mut reader = wal.reader().expect("wal reader");
        let control_snapshot = wal.control_file().map(|ctrl| ctrl.snapshot());
        let mut analysis = AnalysisPass::new(control_snapshot);
        let mut undo = UndoExecutor::new(wal.clone(), scheduler.clone(), buffer_pool.clone());
        let redo = RedoExecutor::new(scheduler, buffer_pool);
        let mut frames = Vec::new();
        while let Some(frame) = reader.next_frame().expect("wal frame") {
            analysis.observe(&frame);
            undo.observe(&frame).expect("undo observe");
            frames.push(frame);
        }
        let analysis_result = analysis.finalize().expect("analysis finalize");
        if !analysis_result.has_frames {
            return (analysis_result, UndoOutcome::default(), 0);
        }
        let mut redo_count = 0usize;
        for frame in &frames {
            if frame.lsn < analysis_result.start_lsn {
                continue;
            }
            redo_count += redo.apply(frame).expect("redo apply");
        }
        let undo_outcome = undo.finalize().expect("undo finalize");
        if undo_outcome.max_clr_lsn != 0 {
            wal.flush_until(undo_outcome.max_clr_lsn)
                .expect("wal flush until");
        }
        (analysis_result, undo_outcome, redo_count)
    }

    #[test]
    fn replay_applies_page_writes() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("test.db");
        let wal_dir = temp.path().join("wal");

        let config = WalConfig {
            directory: wal_dir.clone(),
            sync_on_flush: false,
            ..WalConfig::default()
        };
        let (wal, seed_scheduler) = build_wal(config.clone(), &db_path);

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

        let seg_path = wal_dir.join("wal_0000000000000001.log");
        let seg_size = std::fs::metadata(&seg_path).expect("wal segment").len();
        assert_eq!(seg_size % WAL_PAGE_SIZE as u64, 0);
        assert!(seg_size >= 2 * WAL_PAGE_SIZE as u64);

        drop(wal);
        drop(seed_scheduler);

        let (wal, scheduler) = build_wal(config.clone(), &db_path);
        let (analysis, undo_outcome, redo_count) =
            run_stage_pipeline(wal.clone(), scheduler.clone(), None);
        assert!(analysis.has_frames);
        assert_eq!(analysis.start_lsn, 0);
        assert_eq!(redo_count, 1);
        assert!(undo_outcome.loser_transactions.is_empty());

        let recovery = RecoveryManager::new(wal, scheduler.clone());
        let summary = recovery.replay().unwrap();
        assert_eq!(summary.redo_count, redo_count);
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

        let config = WalConfig {
            directory: wal_dir.clone(),
            sync_on_flush: false,
            ..WalConfig::default()
        };
        let (wal, seed_scheduler) = build_wal(config.clone(), &db_path);

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
        drop(seed_scheduler);

        let (wal, scheduler) = build_wal(config.clone(), &db_path);
        let (analysis, undo_outcome, redo_count) =
            run_stage_pipeline(wal.clone(), scheduler.clone(), None);
        assert!(analysis.has_frames);
        assert_eq!(analysis.start_lsn, 0);
        assert_eq!(redo_count, 2);
        assert!(undo_outcome.loser_transactions.is_empty());

        let recovery = RecoveryManager::new(wal, scheduler.clone());
        let summary = recovery.replay().unwrap();
        assert_eq!(summary.redo_count, redo_count);

        let rx = scheduler.schedule_read(2).unwrap();
        let data = rx.recv().unwrap().unwrap();
        assert_eq!(data[100..105], [1, 2, 3, 4, 5]);
    }

    #[test]
    fn replay_rebuilds_heap_from_logical_records() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("heap_logic.db");
        let wal_dir = temp.path().join("wal");

        let config = WalConfig {
            directory: wal_dir.clone(),
            sync_on_flush: false,
            ..WalConfig::default()
        };
        let (wal, seed_scheduler) = build_wal(config.clone(), &db_path);

        let page_id = 11u32;
        let base_bytes = vec![7u8; 8];
        let updated_bytes = vec![8u8; 8];
        let zero_page = vec![0u8; crate::buffer::PAGE_SIZE];
        seed_scheduler
            .schedule_write(page_id, bytes::Bytes::from(zero_page))
            .unwrap()
            .recv()
            .unwrap()
            .unwrap();
        wal.append_record_with(|_| {
            WalRecordPayload::Transaction(TransactionPayload {
                marker: TransactionRecordKind::Begin,
                txn_id: 1,
            })
        })
        .unwrap();

        let insert_meta = TupleMetaRepr {
            insert_txn_id: 1,
            insert_cid: 0,
            delete_txn_id: 0,
            delete_cid: INVALID_COMMAND_ID,
            is_deleted: false,
            next_version: None,
            prev_version: None,
        };
        let updated_meta = TupleMetaRepr {
            insert_txn_id: 1,
            insert_cid: 1,
            delete_txn_id: 0,
            delete_cid: INVALID_COMMAND_ID,
            is_deleted: false,
            next_version: None,
            prev_version: None,
        };
        let mut deleted_meta: TupleMeta = updated_meta.into();
        deleted_meta.mark_deleted(1, 2);

        wal.append_record_with(|_| {
            WalRecordPayload::Heap(HeapRecordPayload::Insert(HeapInsertPayload {
                relation: RelationIdent {
                    root_page_id: page_id,
                },
                page_id,
                slot_id: 0,
                op_txn_id: 1,
                tuple_meta: insert_meta,
                tuple_data: base_bytes.clone(),
            }))
        })
        .unwrap();
        wal.append_record_with(|_| {
            WalRecordPayload::Heap(HeapRecordPayload::Update(HeapUpdatePayload {
                relation: RelationIdent {
                    root_page_id: page_id,
                },
                page_id,
                slot_id: 0,
                op_txn_id: 1,
                new_tuple_meta: updated_meta,
                new_tuple_data: updated_bytes.clone(),
                old_tuple_meta: Some(insert_meta),
                old_tuple_data: Some(base_bytes.clone()),
            }))
        })
        .unwrap();
        wal.append_record_with(|_| {
            WalRecordPayload::Heap(HeapRecordPayload::Delete(HeapDeletePayload {
                relation: RelationIdent {
                    root_page_id: page_id,
                },
                page_id,
                slot_id: 0,
                op_txn_id: 1,
                new_tuple_meta: TupleMetaRepr::from(deleted_meta),
                old_tuple_meta: updated_meta,
                old_tuple_data: Some(updated_bytes.clone()),
            }))
        })
        .unwrap();
        wal.append_record_with(|_| {
            WalRecordPayload::Transaction(TransactionPayload {
                marker: TransactionRecordKind::Commit,
                txn_id: 1,
            })
        })
        .unwrap();
        wal.flush(None).unwrap();
        drop(wal);
        drop(seed_scheduler);

        let (wal, scheduler) = build_wal(config.clone(), &db_path);
        let recovery = RecoveryManager::new(wal.clone(), scheduler.clone());
        let summary = recovery.replay().unwrap();
        assert_eq!(summary.redo_count, 3);

        let rx = scheduler.schedule_read(page_id).unwrap();
        let data = rx.recv().unwrap().unwrap();
        let (header, _) = TablePageHeaderCodec::decode(&data).unwrap();
        assert_eq!(header.num_tuples, 1);
        assert_eq!(header.tuple_infos[0].meta.is_deleted, true);
        let off = header.tuple_infos[0].offset as usize;
        let size = header.tuple_infos[0].size as usize;
        assert_eq!(&data[off..off + size], &updated_bytes[..]);
        drop(wal);
    }

    #[test]
    fn replay_rebuilds_index_from_logical_records() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("index_logic.db");
        let wal_dir = temp.path().join("wal");

        let config = WalConfig {
            directory: wal_dir.clone(),
            sync_on_flush: false,
            ..WalConfig::default()
        };
        let (wal, scheduler) = build_wal(config.clone(), &db_path);
        let bpm = Arc::new(BufferManager::new(64, scheduler.clone()));
        bpm.set_wal_manager(wal.clone());

        let schema = Arc::new(Schema::new(vec![Column::new("k", DataType::Int32, false)]));
        let index = BPlusTreeIndex::new(schema.clone(), bpm.clone(), 4, 4);
        let header_page_id = index.header_page_id;

        for k in 0..3 {
            let tuple = Tuple::new(schema.clone(), vec![ScalarValue::Int32(Some(k))]);
            let rid = RecordId::new(1, k as u32);
            index.insert(&tuple, rid).unwrap();
        }
        let delete_key = Tuple::new(schema.clone(), vec![ScalarValue::Int32(Some(1))]);
        index.delete(&delete_key).unwrap();

        wal.flush(None).unwrap();
        bpm.flush_all_pages().unwrap();
        drop(index);
        drop(bpm);
        drop(wal);
        drop(scheduler);

        let (wal_r, scheduler_r) = build_wal(config.clone(), &db_path);
        let bpm_r = Arc::new(BufferManager::new(64, scheduler_r.clone()));
        bpm_r.set_wal_manager(wal_r.clone());
        let recovery = RecoveryManager::new(wal_r.clone(), scheduler_r.clone())
            .with_buffer_pool(bpm_r.clone());
        let summary = recovery.replay().unwrap();
        assert!(summary.redo_count >= 3);

        let reopened = BPlusTreeIndex::open(schema.clone(), bpm_r.clone(), 4, 4, header_page_id);
        for k in [0, 2] {
            let tuple = Tuple::new(schema.clone(), vec![ScalarValue::Int32(Some(k))]);
            let rid = RecordId::new(1, k as u32);
            assert_eq!(reopened.get(&tuple).unwrap(), Some(rid));
        }
        let deleted = Tuple::new(schema.clone(), vec![ScalarValue::Int32(Some(1))]);
        assert!(reopened.get(&deleted).unwrap().is_none());
    }

    #[test]
    fn replay_reports_loser_transactions() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("loser.db");
        let wal_dir = temp.path().join("wal");

        let config = WalConfig {
            directory: wal_dir.clone(),
            sync_on_flush: false,
            ..WalConfig::default()
        };
        let (wal, seed_scheduler) = build_wal(config.clone(), &db_path);

        wal.append_record_with(|_| {
            WalRecordPayload::Transaction(TransactionPayload {
                marker: TransactionRecordKind::Begin,
                txn_id: 7,
            })
        })
        .expect("append begin");
        wal.flush(None).unwrap();
        drop(wal);
        drop(seed_scheduler);

        let (wal, scheduler) = build_wal(config.clone(), &db_path);
        let (analysis, undo_outcome, redo_count) =
            run_stage_pipeline(wal.clone(), scheduler.clone(), None);
        assert!(analysis.has_frames);
        assert_eq!(redo_count, 0);
        assert_eq!(undo_outcome.loser_transactions, vec![7]);

        let recovery = RecoveryManager::new(wal, scheduler.clone());
        let summary = recovery.replay().unwrap();
        assert_eq!(summary.redo_count, redo_count);
        assert_eq!(summary.loser_transactions, undo_outcome.loser_transactions);
    }

    #[test]
    fn undo_marks_insert_deleted_and_writes_clr() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("undo_insert.db");
        let wal_dir = temp.path().join("wal");
        let config = WalConfig {
            directory: wal_dir.clone(),
            sync_on_flush: false,
            ..WalConfig::default()
        };
        let (wal, scheduler) = build_wal(config.clone(), &db_path);

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
                meta: TupleMeta::new(100, 0),
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
                        insert_cid: 0,
                        delete_txn_id: 0,
                        delete_cid: INVALID_COMMAND_ID,
                        is_deleted: false,
                        next_version: None,
                        prev_version: None,
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
        drop(scheduler);

        // Reopen wal and run recovery (inject BufferPool to use TableHeap recovery APIs)
        let (wal, scheduler) = build_wal(config.clone(), &db_path);
        let bpm = Arc::new(BufferManager::new(64, scheduler.clone()));
        let (analysis, undo_outcome, redo_count) =
            run_stage_pipeline(wal.clone(), scheduler.clone(), Some(bpm.clone()));
        assert!(analysis.has_frames);
        assert_eq!(redo_count, 0);
        assert_eq!(undo_outcome.loser_transactions, vec![1]);
        assert!(undo_outcome.max_clr_lsn > 0);

        let recovery = RecoveryManager::new(wal, scheduler.clone()).with_buffer_pool(bpm);
        let summary = recovery.replay().unwrap();
        assert_eq!(summary.loser_transactions, undo_outcome.loser_transactions);

        // Verify header is_deleted flipped to true
        let rx = scheduler.schedule_read(page_id).unwrap();
        let data = rx.recv().unwrap().unwrap();
        let (header2, _c) = TablePageHeaderCodec::decode(&data).unwrap();
        assert!(header2.tuple_infos[0].meta.is_deleted);
    }

    #[test]
    fn undo_update_restores_old_bytes_and_meta() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("undo_update.db");
        let wal_dir = temp.path().join("wal");
        let mut config = WalConfig::default();
        config.directory = wal_dir.clone();
        config.sync_on_flush = false;
        let (wal, scheduler) = build_wal(config.clone(), &db_path);

        let page_id = 4u32;
        // Build header with 1 slot
        let old_meta = TupleMeta::new(1, 0);
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
        let new_meta = TupleMeta::new(1, 0);
        let old_tuple_bytes = vec![0xAA; 16];
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
        let bpm = Arc::new(BufferManager::new(64, scheduler.clone()));
        let wal = Arc::new(WalManager::new(config.clone(), None, None).unwrap());
        let (analysis, undo_outcome, redo_count) =
            run_stage_pipeline(wal.clone(), scheduler.clone(), Some(bpm.clone()));
        assert!(analysis.has_frames);
        assert_eq!(redo_count, 0);
        assert_eq!(undo_outcome.loser_transactions, vec![2]);
        assert!(undo_outcome.max_clr_lsn > 0);

        let recovery = RecoveryManager::new(wal, scheduler.clone()).with_buffer_pool(bpm);
        let summary = recovery.replay().unwrap();
        assert_eq!(summary.redo_count, redo_count);
        assert_eq!(summary.loser_transactions, undo_outcome.loser_transactions);

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
        let wal = Arc::new(WalManager::new(config.clone(), None, None).unwrap());

        let page_id = 5u32;
        let old_meta = TupleMeta::new(5, 0);
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
        let mut deleted_meta = old_meta;
        deleted_meta.mark_deleted(6, 0);
        wal.append_record_with(|_| {
            WalRecordPayload::Heap(crate::recovery::wal_record::HeapRecordPayload::Delete(
                crate::recovery::wal_record::HeapDeletePayload {
                    relation: crate::recovery::wal_record::RelationIdent { root_page_id: 0 },
                    page_id,
                    slot_id: 0,
                    op_txn_id: 6,
                    new_tuple_meta: crate::recovery::wal_record::TupleMetaRepr::from(deleted_meta),
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
        let bpm = Arc::new(BufferManager::new(64, scheduler.clone()));
        let wal = Arc::new(WalManager::new(config.clone(), None, None).unwrap());
        let (analysis, undo_outcome, redo_count) =
            run_stage_pipeline(wal.clone(), scheduler.clone(), Some(bpm.clone()));
        assert!(analysis.has_frames);
        assert_eq!(redo_count, 0);
        assert_eq!(undo_outcome.loser_transactions, vec![6]);
        assert!(undo_outcome.max_clr_lsn > 0);

        let recovery = RecoveryManager::new(wal, scheduler.clone()).with_buffer_pool(bpm);
        let summary = recovery.replay().unwrap();
        assert_eq!(summary.redo_count, redo_count);
        assert_eq!(summary.loser_transactions, undo_outcome.loser_transactions);

        let rx = scheduler.schedule_read(page_id).unwrap();
        let data = rx.recv().unwrap().unwrap();
        let (hdr, _c) = TablePageHeaderCodec::decode(&data).unwrap();
        assert!(!hdr.tuple_infos[0].meta.is_deleted);
        let off3 = hdr.tuple_infos[0].offset as usize;
        assert_eq!(&data[off3..off3 + 8], &old_bytes[..]);
    }

    #[test]
    fn replay_is_idempotent_on_page_state() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("idem.db");
        let wal_dir = temp.path().join("wal");
        let mut config = WalConfig::default();
        config.directory = wal_dir.clone();
        config.sync_on_flush = false;
        let wal = Arc::new(WalManager::new(config.clone(), None, None).unwrap());
        let scheduler = build_scheduler(&db_path);

        // Seed a page with one visible tuple
        let page_id = 6u32;
        let header = TablePageHeader {
            next_page_id: INVALID_PAGE_ID,
            num_tuples: 1,
            num_deleted_tuples: 0,
            tuple_infos: vec![TupleInfo {
                offset: (crate::buffer::PAGE_SIZE - 8) as u16,
                size: 8,
                meta: TupleMeta::new(9, 0),
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
                        insert_cid: 0,
                        delete_txn_id: 0,
                        delete_cid: INVALID_COMMAND_ID,
                        is_deleted: false,
                        next_version: None,
                        prev_version: None,
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
        let bpm = Arc::new(BufferManager::new(64, scheduler.clone()));
        let wal = Arc::new(WalManager::new(config.clone(), None, None).unwrap());
        let (analysis1, outcome1, redo_count1) =
            run_stage_pipeline(wal.clone(), scheduler.clone(), Some(bpm.clone()));
        assert!(analysis1.has_frames);
        assert_eq!(redo_count1, 0);
        assert_eq!(outcome1.loser_transactions, vec![10]);
        assert!(outcome1.max_clr_lsn > 0);

        let recovery = RecoveryManager::new(wal, scheduler.clone()).with_buffer_pool(bpm);
        let summary1 = recovery.replay().unwrap();
        assert_eq!(summary1.redo_count, redo_count1);
        assert_eq!(summary1.loser_transactions, outcome1.loser_transactions);
        let rx = scheduler.schedule_read(page_id).unwrap();
        let data1 = rx.recv().unwrap().unwrap();
        let (hdr1, _c1) = TablePageHeaderCodec::decode(&data1).unwrap();
        assert!(hdr1.tuple_infos[0].meta.is_deleted);

        // Second recovery (should not change page state) - inject BufferPool again
        let scheduler2 = build_scheduler(&db_path);
        let bpm2 = Arc::new(BufferManager::new(64, scheduler2.clone()));
        let wal2 = Arc::new(WalManager::new(config, None, None).unwrap());
        let (analysis2, outcome2, redo_count2) =
            run_stage_pipeline(wal2.clone(), scheduler2.clone(), Some(bpm2.clone()));
        assert!(analysis2.has_frames);
        assert_eq!(redo_count2, 0);
        assert_eq!(outcome2.loser_transactions, vec![10]);

        let recovery2 = RecoveryManager::new(wal2, scheduler2.clone()).with_buffer_pool(bpm2);
        let summary2 = recovery2.replay().unwrap();
        assert_eq!(summary2.redo_count, redo_count2);
        assert_eq!(summary2.loser_transactions, outcome2.loser_transactions);
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

        let mut config = WalConfig::default();
        config.directory = wal_dir.clone();
        config.sync_on_flush = false;
        config.segment_size = 1024; // force frequent rotation
        let wal = Arc::new(WalManager::new(config.clone(), None, None).unwrap());

        // Write initial full page at pid=7
        let pid = 7u32;
        let base = vec![0u8; crate::buffer::PAGE_SIZE];
        let scheduler = build_scheduler(&db_path);
        scheduler
            .schedule_write(pid, bytes::Bytes::from(base.clone()))
            .unwrap()
            .recv()
            .unwrap()
            .unwrap();
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
        let wal = Arc::new(WalManager::new(config.clone(), None, None).unwrap());
        let (analysis, undo_outcome, redo_count) =
            run_stage_pipeline(wal.clone(), scheduler.clone(), None);
        assert!(analysis.has_frames);
        assert!(redo_count >= 1);
        assert!(undo_outcome.loser_transactions.is_empty());

        let recovery = RecoveryManager::new(wal, scheduler.clone());
        let summary = recovery.replay().unwrap();
        assert_eq!(summary.redo_count, redo_count);

        let rx = scheduler.schedule_read(pid).unwrap();
        let data = rx.recv().unwrap().unwrap();
        // spot-check a few offsets
        for i in 0..4u16 {
            let off = ((i % 32) * 8) as usize;
            // Last writer for this offset is i + 96 within 0..128 sequence
            let expected = [(i + 96) as u8; 8];
            assert_eq!(&data[off..off + 8], &expected[..]);
        }
    }

    #[test]
    fn fpw_and_delta_replay_consistent() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("fpw_delta.db");
        let wal_dir = temp.path().join("wal");

        let mut config = WalConfig::default();
        config.directory = wal_dir.clone();
        config.sync_on_flush = false;
        let wal = Arc::new(WalManager::new(config.clone(), None, None).unwrap());

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
        let wal = Arc::new(WalManager::new(config, None, None).unwrap());
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
        let wal = Arc::new(WalManager::new(config.clone(), None, None).unwrap());

        // Seed a page with one tuple of 16 bytes
        let page_id = 9u32;
        let header = TablePageHeader {
            next_page_id: INVALID_PAGE_ID,
            num_tuples: 1,
            num_deleted_tuples: 0,
            tuple_infos: vec![TupleInfo {
                offset: (crate::buffer::PAGE_SIZE - 16) as u16,
                size: 16,
                meta: TupleMeta::new(1, 0),
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
                    new_tuple_meta: crate::recovery::wal_record::TupleMetaRepr::from(
                        TupleMeta::new(1, 0),
                    ),
                    new_tuple_data: vec![0x11; 20],
                    old_tuple_meta: Some(crate::recovery::wal_record::TupleMetaRepr::from(
                        TupleMeta::new(1, 0),
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
                    new_tuple_meta: crate::recovery::wal_record::TupleMetaRepr::from(
                        TupleMeta::new(1, 0),
                    ),
                    new_tuple_data: vec![0x22; 8],
                    old_tuple_meta: Some(crate::recovery::wal_record::TupleMetaRepr::from(
                        TupleMeta::new(1, 0),
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
        let bpm1 = Arc::new(BufferManager::new(64, scheduler1.clone()));
        let wal1 = Arc::new(WalManager::new(config.clone(), None, None).unwrap());
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
            if f.rmid == crate::recovery::wal_record::ResourceManagerId::Clr {
                clr_cnt += 1;
            }
        }
        assert!(clr_cnt >= 2);

        // Second recovery: no further changes
        let scheduler2 = build_scheduler(&db_path);
        let bpm2 = Arc::new(BufferManager::new(64, scheduler2.clone()));
        let wal2 = Arc::new(WalManager::new(config.clone(), None, None).unwrap());
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
        let wal = Arc::new(WalManager::new(config.clone(), None, None).unwrap());

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
                    meta: TupleMeta::new(5, 0),
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
                        insert_cid: 0,
                        delete_txn_id: 0,
                        delete_cid: INVALID_COMMAND_ID,
                        is_deleted: false,
                        next_version: None,
                        prev_version: None,
                    },
                    tuple_data: vec![0u8; 8],
                },
            ))
        })
        .unwrap();
        let insert_meta = TupleMeta {
            insert_txn_id: 5,
            insert_cid: 0,
            delete_txn_id: 0,
            delete_cid: INVALID_COMMAND_ID,
            is_deleted: false,
            next_version: None,
            prev_version: None,
        };
        let mut deleted_meta = insert_meta;
        deleted_meta.mark_deleted(99, 0);
        wal.append_record_with(|_| {
            WalRecordPayload::Heap(crate::recovery::wal_record::HeapRecordPayload::Delete(
                crate::recovery::wal_record::HeapDeletePayload {
                    relation: crate::recovery::wal_record::RelationIdent { root_page_id: 0 },
                    page_id: pid_a,
                    slot_id: 0,
                    op_txn_id: 99,
                    new_tuple_meta: crate::recovery::wal_record::TupleMetaRepr::from(deleted_meta),
                    old_tuple_meta: crate::recovery::wal_record::TupleMetaRepr::from(insert_meta),
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
                    new_tuple_meta: crate::recovery::wal_record::TupleMetaRepr::from(
                        TupleMeta::new(5, 0),
                    ),
                    new_tuple_data: vec![9u8; 10],
                    old_tuple_meta: Some(crate::recovery::wal_record::TupleMetaRepr::from(
                        TupleMeta::new(5, 0),
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
            let bpm_r = Arc::new(BufferManager::new(64, scheduler_r.clone()));
            let wal_r = Arc::new(WalManager::new(config.clone(), None, None).unwrap());
            let recovery = RecoveryManager::new(wal_r, scheduler_r.clone()).with_buffer_pool(bpm_r);
            let _ = recovery.replay().unwrap();

            let rx_a = scheduler_r.schedule_read(pid_a).unwrap();
            let data_a = rx_a.recv().unwrap().unwrap();
            let (hdr_a, _ca) = TablePageHeaderCodec::decode(&data_a).unwrap();
            assert!(hdr_a.tuple_infos[0].meta.is_deleted);

            let rx_b = scheduler_r.schedule_read(pid_b).unwrap();
            let data_b = rx_b.recv().unwrap().unwrap();
            let (hdr_b, _cb) = TablePageHeaderCodec::decode(&data_b).unwrap();
            assert_eq!(hdr_b.tuple_infos[0].size, 12);
            let offb = hdr_b.tuple_infos[0].offset as usize;
            assert_eq!(&data_b[offb..offb + 12], &vec![2u8; 12][..]);
        }
    }

    #[test]
    fn replay_heap_update_restores_old_bytes() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("heap.db");
        let wal_dir = temp.path().join("wal");

        let config = WalConfig {
            directory: wal_dir.clone(),
            sync_on_flush: false,
            ..WalConfig::default()
        };

        let wal = Arc::new(WalManager::new(config.clone(), None, None).unwrap());
        let scheduler = build_scheduler(&db_path);

        let page_id = 8u32;
        let mut page = vec![0u8; crate::buffer::PAGE_SIZE];
        let header = TablePageHeader {
            next_page_id: INVALID_PAGE_ID,
            num_tuples: 1,
            num_deleted_tuples: 0,
            tuple_infos: vec![TupleInfo {
                offset: (crate::buffer::PAGE_SIZE - 8) as u16,
                size: 8,
                meta: TupleMeta::new(5, 0),
            }],
            lsn: 0,
        };
        let header_bytes = TablePageHeaderCodec::encode(&header);
        let copy_len = std::cmp::min(header_bytes.len(), crate::buffer::PAGE_SIZE);
        page[0..copy_len].copy_from_slice(&header_bytes[..copy_len]);

        let new_bytes = vec![0xAA; 8];
        let old_bytes = vec![0xBB; 8];
        let off = header.tuple_infos[0].offset as usize;
        page[off..off + 8].copy_from_slice(&new_bytes);

        scheduler
            .schedule_write(page_id, bytes::Bytes::from(page.clone()))
            .unwrap()
            .recv()
            .unwrap()
            .unwrap();

        wal.append_record_with(|_| {
            WalRecordPayload::Transaction(TransactionPayload {
                marker: TransactionRecordKind::Begin,
                txn_id: 2,
            })
        })
        .unwrap();

        wal.append_record_with(|_| {
            WalRecordPayload::Heap(HeapRecordPayload::Update(HeapUpdatePayload {
                relation: RelationIdent { root_page_id: 0 },
                page_id,
                slot_id: 0,
                op_txn_id: 2,
                new_tuple_meta: TupleMetaRepr {
                    insert_txn_id: 2,
                    insert_cid: 0,
                    delete_txn_id: 0,
                    delete_cid: INVALID_COMMAND_ID,
                    is_deleted: false,
                    next_version: None,
                    prev_version: None,
                },
                new_tuple_data: new_bytes.clone(),
                old_tuple_meta: Some(TupleMetaRepr {
                    insert_txn_id: 5,
                    insert_cid: 0,
                    delete_txn_id: 0,
                    delete_cid: INVALID_COMMAND_ID,
                    is_deleted: false,
                    next_version: None,
                    prev_version: None,
                }),
                old_tuple_data: Some(old_bytes.clone()),
            }))
        })
        .unwrap();

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

        let scheduler = build_scheduler(&db_path);
        let bpm = Arc::new(BufferManager::new(64, scheduler.clone()));
        let wal = Arc::new(WalManager::new(config.clone(), None, None).unwrap());
        let recovery = RecoveryManager::new(wal, scheduler.clone()).with_buffer_pool(bpm);
        let summary = recovery.replay().unwrap();
        assert_eq!(summary.loser_transactions, vec![2]);

        let rx = scheduler.schedule_read(page_id).unwrap();
        let data = rx.recv().unwrap().unwrap();
        let (header_after, _c) = TablePageHeaderCodec::decode(&data).unwrap();
        let off_after = header_after.tuple_infos[0].offset as usize;
        assert_eq!(
            &data[off_after..off_after + old_bytes.len()],
            &old_bytes[..]
        );
    }
}
