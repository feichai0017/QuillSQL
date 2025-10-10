use std::any::TypeId;
use std::collections::HashSet;
use std::sync::{Arc, OnceLock, RwLock};

use bytes::{Bytes, BytesMut};

use crate::buffer::{BufferEngine, PAGE_SIZE};
use crate::error::QuillSQLResult;
use crate::recovery::resource_manager::{
    register_resource_manager, RedoContext, ResourceManager, UndoContext,
};
use crate::recovery::wal::codec::{ResourceManagerId, WalFrame};
use crate::storage::codec::TablePageHeaderCodec;
use crate::storage::heap::wal_codec::{decode_heap_record, HeapRecordPayload, TupleMetaRepr};
use crate::storage::page::{RecordId, TupleMeta};
use crate::storage::table_heap::TableHeap;

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

    fn apply_tuple_meta_flag<B: BufferEngine>(
        &self,
        ctx: &UndoContext<B>,
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

    fn restore_tuple<B: BufferEngine>(
        &self,
        ctx: &UndoContext<B>,
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
}

impl<B: BufferEngine> ResourceManager<B> for HeapResourceManager {
    fn redo(&self, _frame: &WalFrame, _ctx: &RedoContext<B>) -> QuillSQLResult<usize> {
        Ok(0)
    }

    fn undo(&self, frame: &WalFrame, ctx: &UndoContext<B>) -> QuillSQLResult<()> {
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

fn heap_registry_tracker() -> &'static RwLock<HashSet<TypeId>> {
    static TRACKER: OnceLock<RwLock<HashSet<TypeId>>> = OnceLock::new();
    TRACKER.get_or_init(|| RwLock::new(HashSet::new()))
}

pub fn ensure_heap_resource_manager_registered<B: BufferEngine + 'static>() {
    let tracker = heap_registry_tracker();
    {
        let guard = tracker.read().unwrap();
        if guard.contains(&TypeId::of::<B>()) {
            return;
        }
    }
    let mut guard = tracker.write().unwrap();
    if guard.insert(TypeId::of::<B>()) {
        register_resource_manager::<B>(
            ResourceManagerId::Heap,
            Arc::new(HeapResourceManager::default()),
        );
    }
}
