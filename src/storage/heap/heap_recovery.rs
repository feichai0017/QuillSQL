use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::buffer::{PageId, PAGE_SIZE};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::resource_manager::{
    register_resource_manager, RedoContext, ResourceManager, UndoContext,
};
use crate::recovery::wal::codec::{ResourceManagerId, WalFrame};
use crate::recovery::Lsn;
use crate::storage::codec::TablePageHeaderCodec;
use crate::storage::heap::wal_codec::{
    decode_heap_record, HeapDeletePayload, HeapInsertPayload, HeapRecordPayload, HeapUpdatePayload,
    TupleMetaRepr,
};
use crate::storage::page::{RecordId, TablePageHeader, TupleInfo, TupleMeta};
use crate::storage::table_heap::TableHeap;
use std::sync::OnceLock;

#[derive(Debug)]
struct PageImage {
    header: TablePageHeader,
    slots: Vec<Vec<u8>>,
}

impl PageImage {
    fn load(bytes: &[u8]) -> QuillSQLResult<Self> {
        let (mut header, _) = TablePageHeaderCodec::decode(bytes)?;
        let mut slots = Vec::with_capacity(header.tuple_infos.len());
        for info in &header.tuple_infos {
            let start = info.offset as usize;
            let end = start + info.size as usize;
            if end > bytes.len() {
                return Err(QuillSQLError::Storage(format!(
                    "heap page tuple range [{}, {}) exceeds page size {}",
                    start,
                    end,
                    bytes.len()
                )));
            }
            slots.push(bytes[start..end].to_vec());
        }
        header.num_tuples = header.tuple_infos.len() as u16;
        header.num_deleted_tuples = header
            .tuple_infos
            .iter()
            .filter(|info| info.meta.is_deleted)
            .count() as u16;
        Ok(Self { header, slots })
    }

    fn refresh_counts(&mut self) {
        self.header.num_tuples = self.slots.len() as u16;
        self.header.num_deleted_tuples = self
            .header
            .tuple_infos
            .iter()
            .filter(|info| info.meta.is_deleted)
            .count() as u16;
    }

    fn persist(&mut self, data: &mut [u8]) -> QuillSQLResult<()> {
        if self.header.tuple_infos.len() != self.slots.len() {
            return Err(QuillSQLError::Internal(
                "heap redo tuple metadata count mismatch".to_string(),
            ));
        }
        self.refresh_counts();
        data.fill(0);
        let mut tail = PAGE_SIZE;
        for (info, tuple) in self.header.tuple_infos.iter_mut().zip(self.slots.iter()) {
            let len = tuple.len();
            if len > PAGE_SIZE {
                return Err(QuillSQLError::Storage(
                    "tuple length exceeds page capacity".to_string(),
                ));
            }
            if tail < len {
                return Err(QuillSQLError::Storage(
                    "insufficient free space while rebuilding heap page".to_string(),
                ));
            }
            tail -= len;
            info.offset = tail as u16;
            info.size = len as u16;
        }
        let header_bytes = TablePageHeaderCodec::encode(&self.header);
        if header_bytes.len() > tail {
            return Err(QuillSQLError::Storage(
                "heap page header overlaps tuple data during redo".to_string(),
            ));
        }
        data[..header_bytes.len()].copy_from_slice(&header_bytes);
        for (info, tuple) in self.header.tuple_infos.iter().zip(self.slots.iter()) {
            let start = info.offset as usize;
            let end = start + tuple.len();
            data[start..end].copy_from_slice(tuple);
        }
        Ok(())
    }

    fn insert_at(&mut self, slot: usize, meta: TupleMeta, bytes: &[u8]) -> QuillSQLResult<()> {
        if slot > self.slots.len() {
            return Err(QuillSQLError::Storage(format!(
                "heap redo insert slot {} out of bounds (len={})",
                slot,
                self.slots.len()
            )));
        }
        self.slots.insert(slot, bytes.to_vec());
        self.header.tuple_infos.insert(
            slot,
            TupleInfo {
                offset: 0,
                size: bytes.len() as u16,
                meta,
            },
        );
        self.refresh_counts();
        Ok(())
    }

    fn overwrite_slot(&mut self, slot: usize, meta: TupleMeta, bytes: &[u8]) -> QuillSQLResult<()> {
        if slot >= self.slots.len() {
            return Err(QuillSQLError::Storage(format!(
                "heap redo update slot {} out of bounds (len={})",
                slot,
                self.slots.len()
            )));
        }
        self.slots[slot] = bytes.to_vec();
        self.header.tuple_infos[slot].meta = meta;
        self.refresh_counts();
        Ok(())
    }

    fn set_slot_meta(&mut self, slot: usize, meta: TupleMeta) -> QuillSQLResult<()> {
        if slot >= self.slots.len() {
            return Ok(());
        }
        self.header.tuple_infos[slot].meta = meta;
        self.refresh_counts();
        Ok(())
    }
}

#[derive(Default)]
struct HeapResourceManager;

impl HeapResourceManager {
    fn decode_payload(&self, frame: &WalFrame) -> QuillSQLResult<HeapRecordPayload> {
        decode_heap_record(&frame.body, frame.info)
    }

    fn heap_txn_id(payload: &HeapRecordPayload) -> u64 {
        match payload {
            HeapRecordPayload::Insert(p) => p.op_txn_id,
            HeapRecordPayload::Update(p) => p.op_txn_id,
            HeapRecordPayload::Delete(p) => p.op_txn_id,
        }
    }

    fn apply_tuple_meta_flag(
        &self,
        ctx: &UndoContext,
        page_id: u32,
        slot_idx: usize,
        deleted: bool,
    ) -> QuillSQLResult<()> {
        if let Some(bpm) = &ctx.buffer_pool {
            let rid = RecordId::new(page_id, slot_idx as u32);
            let guard = bpm.fetch_page_read(page_id)?;
            let (header, _hdr_len) = TablePageHeaderCodec::decode(guard.data())?;
            drop(guard);
            if slot_idx >= header.tuple_infos.len() {
                return Ok(());
            }
            let mut new_meta = header.tuple_infos[slot_idx].meta;
            if deleted {
                new_meta.is_deleted = true;
            } else {
                new_meta.clear_delete();
            }
            let heap = TableHeap::recovery_view(bpm.clone());
            let _ = heap.recover_set_tuple_meta(rid, new_meta);
            let _ = bpm.flush_page(page_id);
            return Ok(());
        }
        Ok(())
    }

    fn restore_tuple(
        &self,
        ctx: &UndoContext,
        page_id: u32,
        slot_idx: usize,
        old_meta: TupleMetaRepr,
        old_bytes: &[u8],
    ) -> QuillSQLResult<()> {
        if let Some(bpm) = &ctx.buffer_pool {
            let heap = TableHeap::recovery_view(bpm.clone());
            let rid = RecordId::new(page_id, slot_idx as u32);
            let _ = heap.recover_set_tuple_bytes(rid, old_bytes);
            let restored_meta: TupleMeta = old_meta.into();
            let _ = heap.recover_set_tuple_meta(rid, restored_meta);
            let _ = bpm.flush_page(page_id);
            return Ok(());
        }

        let rx = ctx.disk_scheduler.schedule_read(page_id)?;
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
        let restored_meta: TupleMeta = old_meta.into();
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
        let rxw = ctx
            .disk_scheduler
            .schedule_write(page_id, Bytes::from(new_page))?;
        rxw.recv().map_err(|e| {
            crate::error::QuillSQLError::Internal(format!("WAL recovery write recv failed: {}", e))
        })??;
        Ok(())
    }

    fn apply_with_page<F>(
        &self,
        ctx: &RedoContext,
        page_id: PageId,
        frame_lsn: Lsn,
        mutator: F,
    ) -> QuillSQLResult<bool>
    where
        F: FnOnce(&mut [u8], Lsn) -> QuillSQLResult<bool>,
    {
        if let Some(bpm) = &ctx.buffer_pool {
            if let Ok(mut guard) = bpm.fetch_page_write(page_id) {
                let applied = mutator(guard.data_mut(), frame_lsn)?;
                if applied {
                    guard.set_lsn(frame_lsn);
                    guard.mark_dirty();
                }
                return Ok(applied);
            }
        }

        let mut data = Self::read_or_zero(ctx, page_id);
        let applied = mutator(&mut data[..], frame_lsn)?;
        if applied {
            let rxw = ctx
                .disk_scheduler
                .schedule_write(page_id, Bytes::from(data))?;
            rxw.recv().map_err(|e| {
                QuillSQLError::Internal(format!("WAL recovery write recv failed: {}", e))
            })??;
        }
        Ok(applied)
    }

    fn read_or_zero(ctx: &RedoContext, page_id: PageId) -> Vec<u8> {
        match ctx.disk_scheduler.schedule_read(page_id) {
            Ok(rx) => match rx.recv() {
                Ok(Ok(bytes)) => {
                    if bytes.len() == PAGE_SIZE {
                        bytes.to_vec()
                    } else {
                        vec![0u8; PAGE_SIZE]
                    }
                }
                _ => vec![0u8; PAGE_SIZE],
            },
            Err(_) => vec![0u8; PAGE_SIZE],
        }
    }

    fn redo_insert(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        body: &HeapInsertPayload,
    ) -> QuillSQLResult<bool> {
        self.apply_with_page(ctx, body.page_id, frame.lsn, |page_bytes, lsn| {
            let mut image = PageImage::load(page_bytes)?;
            if lsn != 0 && image.header.lsn >= lsn {
                return Ok(false);
            }
            let slot = body.slot_id as usize;
            let meta: TupleMeta = body.tuple_meta.into();
            if slot == image.slots.len() {
                image.insert_at(slot, meta, &body.tuple_data)?;
            } else if slot < image.slots.len() {
                image.overwrite_slot(slot, meta, &body.tuple_data)?;
            } else {
                return Err(QuillSQLError::Storage(format!(
                    "heap insert slot {} beyond tuple count {}",
                    slot,
                    image.slots.len()
                )));
            }
            image.header.lsn = lsn;
            image.persist(page_bytes)?;
            Ok(true)
        })
    }

    fn redo_update(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        body: &HeapUpdatePayload,
    ) -> QuillSQLResult<bool> {
        self.apply_with_page(ctx, body.page_id, frame.lsn, |page_bytes, lsn| {
            let mut image = PageImage::load(page_bytes)?;
            if lsn != 0 && image.header.lsn >= lsn {
                return Ok(false);
            }
            let slot = body.slot_id as usize;
            if slot >= image.slots.len() {
                return Ok(false);
            }
            let meta: TupleMeta = body.new_tuple_meta.into();
            image.overwrite_slot(slot, meta, &body.new_tuple_data)?;
            image.header.lsn = lsn;
            image.persist(page_bytes)?;
            Ok(true)
        })
    }

    fn redo_delete(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        body: &HeapDeletePayload,
    ) -> QuillSQLResult<bool> {
        self.apply_with_page(ctx, body.page_id, frame.lsn, |page_bytes, lsn| {
            let mut image = PageImage::load(page_bytes)?;
            if lsn != 0 && image.header.lsn >= lsn {
                return Ok(false);
            }
            let slot = body.slot_id as usize;
            if slot >= image.slots.len() {
                return Ok(false);
            }
            let meta: TupleMeta = body.new_tuple_meta.into();
            image.set_slot_meta(slot, meta)?;
            image.header.lsn = lsn;
            image.persist(page_bytes)?;
            Ok(true)
        })
    }
}
impl ResourceManager for HeapResourceManager {
    fn redo(&self, frame: &WalFrame, ctx: &RedoContext) -> QuillSQLResult<usize> {
        let payload = self.decode_payload(frame)?;
        let applied = match &payload {
            HeapRecordPayload::Insert(body) => self.redo_insert(frame, ctx, body)?,
            HeapRecordPayload::Update(body) => self.redo_update(frame, ctx, body)?,
            HeapRecordPayload::Delete(body) => self.redo_delete(frame, ctx, body)?,
        };
        Ok(applied as usize)
    }

    fn undo(&self, frame: &WalFrame, ctx: &UndoContext) -> QuillSQLResult<()> {
        let payload = self.decode_payload(frame)?;
        match payload {
            HeapRecordPayload::Insert(body) => {
                self.apply_tuple_meta_flag(ctx, body.page_id, body.slot_id as usize, true)
            }
            HeapRecordPayload::Update(body) => {
                if let (Some(old_meta), Some(old_bytes)) =
                    (body.old_tuple_meta, body.old_tuple_data.as_deref())
                {
                    self.restore_tuple(
                        ctx,
                        body.page_id,
                        body.slot_id as usize,
                        old_meta,
                        old_bytes,
                    )
                } else {
                    Ok(())
                }
            }
            HeapRecordPayload::Delete(body) => {
                if let Some(old_bytes) = body.old_tuple_data.as_deref() {
                    self.restore_tuple(
                        ctx,
                        body.page_id,
                        body.slot_id as usize,
                        body.old_tuple_meta,
                        old_bytes,
                    )
                } else {
                    self.apply_tuple_meta_flag(ctx, body.page_id, body.slot_id as usize, false)
                }
            }
        }
    }

    fn transaction_id(&self, frame: &WalFrame) -> Option<u64> {
        let payload = self.decode_payload(frame).ok()?;
        Some(Self::heap_txn_id(&payload))
    }
}

static REGISTER: OnceLock<()> = OnceLock::new();

pub fn ensure_heap_resource_manager_registered() {
    REGISTER.get_or_init(|| {
        register_resource_manager(
            ResourceManagerId::Heap,
            Arc::new(HeapResourceManager::default()),
        );
    });
}
