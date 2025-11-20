use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::buffer::{BufferManager, PageId, PAGE_SIZE};
use crate::catalog::EMPTY_SCHEMA_REF;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::resource_manager::{
    register_resource_manager, RedoContext, ResourceManager, UndoContext,
};
use crate::recovery::wal::codec::{ResourceManagerId, WalFrame};
use crate::recovery::Lsn;
use crate::storage::codec::{TablePageHeaderCodec, TupleCodec};
use crate::storage::heap::wal_codec::{
    decode_heap_record, HeapDeletePayload, HeapInsertPayload, HeapRecordPayload, TupleMetaRepr,
};
use crate::storage::page::{RecordId, TablePageHeader, TupleInfo, TupleMeta};
use crate::storage::table_heap::TableHeap;
use crate::storage::tuple::Tuple;
use crate::transaction::{CommandId, TransactionId};
use std::sync::OnceLock;

#[derive(Default)]
struct HeapResourceManager;

impl HeapResourceManager {
    fn decode_payload(&self, frame: &WalFrame) -> QuillSQLResult<HeapRecordPayload> {
        decode_heap_record(&frame.body, frame.info)
    }

    fn heap_txn_id(payload: &HeapRecordPayload) -> u64 {
        match payload {
            HeapRecordPayload::Insert(p) => p.op_txn_id,
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
            Self::apply_tuple_update(
                page_bytes,
                lsn,
                body.slot_id as usize,
                Some(body.tuple_meta.into()),
                Some(&body.tuple_data),
                true,
            )
        })
    }

    fn redo_delete(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        body: &HeapDeletePayload,
    ) -> QuillSQLResult<bool> {
        self.apply_with_page(ctx, body.page_id, frame.lsn, |page_bytes, lsn| {
            Self::apply_tuple_update(
                page_bytes,
                lsn,
                body.slot_id as usize,
                Some(body.new_tuple_meta.into()),
                None,
                false,
            )
        })
    }

    fn collect_slots(header: &TablePageHeader, page: &[u8]) -> QuillSQLResult<Vec<Vec<u8>>> {
        let mut slots = Vec::with_capacity(header.tuple_infos.len());
        for info in &header.tuple_infos {
            let start = info.offset as usize;
            let end = start + info.size as usize;
            if end > page.len() {
                return Err(QuillSQLError::Storage(format!(
                    "heap redo tuple range [{}, {}) exceeds page size {}",
                    start,
                    end,
                    page.len()
                )));
            }
            slots.push(page[start..end].to_vec());
        }
        Ok(slots)
    }

    fn pack_page(
        header: &mut TablePageHeader,
        tuples: &[Vec<u8>],
        dest: &mut [u8],
    ) -> QuillSQLResult<()> {
        if header.tuple_infos.len() != tuples.len() {
            return Err(QuillSQLError::Internal(
                "heap redo tuple metadata count mismatch".to_string(),
            ));
        }
        dest.fill(0);
        let mut tail = PAGE_SIZE;
        for (info, tuple) in header.tuple_infos.iter_mut().zip(tuples.iter()) {
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
        header.num_tuples = header.tuple_infos.len() as u16;
        header.num_deleted_tuples = header
            .tuple_infos
            .iter()
            .filter(|info| info.meta.is_deleted)
            .count() as u16;
        let header_bytes = TablePageHeaderCodec::encode(header);
        if header_bytes.len() > tail {
            return Err(QuillSQLError::Storage(
                "heap page header overlaps tuple data during redo".to_string(),
            ));
        }
        dest[..header_bytes.len()].copy_from_slice(&header_bytes);
        for (info, tuple) in header.tuple_infos.iter().zip(tuples.iter()) {
            let start = info.offset as usize;
            let end = start + tuple.len();
            dest[start..end].copy_from_slice(tuple);
        }
        Ok(())
    }

    fn apply_tuple_update(
        page_bytes: &mut [u8],
        lsn: Lsn,
        slot: usize,
        new_meta: Option<TupleMeta>,
        new_tuple: Option<&[u8]>,
        allow_insert: bool,
    ) -> QuillSQLResult<bool> {
        let (mut header, _) = TablePageHeaderCodec::decode(page_bytes)?;
        if lsn != 0 && header.lsn >= lsn {
            return Ok(false);
        }
        let existing_slots = header.tuple_infos.len();
        if slot > existing_slots {
            return Err(QuillSQLError::Storage(format!(
                "heap redo slot {} beyond tuple count {}",
                slot, existing_slots
            )));
        }

        let mut tuples = Self::collect_slots(&header, page_bytes)?;

        if slot == existing_slots {
            if !allow_insert {
                return Ok(false);
            }
            let Some(bytes) = new_tuple else {
                return Err(QuillSQLError::Storage(
                    "heap redo insert missing tuple bytes".to_string(),
                ));
            };
            let Some(meta) = new_meta else {
                return Err(QuillSQLError::Storage(
                    "heap redo insert missing tuple metadata".to_string(),
                ));
            };
            header.tuple_infos.push(TupleInfo {
                offset: 0,
                size: 0,
                meta,
            });
            tuples.push(bytes.to_vec());
        } else {
            if let Some(meta) = new_meta {
                header.tuple_infos[slot].meta = meta;
            }
            if let Some(bytes) = new_tuple {
                tuples[slot] = bytes.to_vec();
            }
        }

        header.lsn = lsn;
        Self::pack_page(&mut header, &tuples, page_bytes)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::TupleMeta;

    fn decode_header(page: &[u8]) -> TablePageHeader {
        TablePageHeaderCodec::decode(page).unwrap().0
    }

    #[test]
    fn apply_tuple_update_inserts_and_updates_lsn() {
        let mut page = vec![0u8; PAGE_SIZE];
        let lsn1 = 10;
        let meta = TupleMeta::new(1, 0);
        let data1 = vec![1u8, 2, 3];
        let applied = HeapResourceManager::apply_tuple_update(
            &mut page,
            lsn1,
            0,
            Some(meta),
            Some(&data1),
            true,
        )
        .expect("apply insert");
        assert!(applied);
        let header = decode_header(&page);
        assert_eq!(header.lsn, lsn1);
        assert_eq!(header.num_tuples, 1);
        let info = &header.tuple_infos[0];
        let stored = &page[info.offset as usize..(info.offset + info.size) as usize];
        assert_eq!(stored, data1.as_slice());

        // Older LSN should be ignored
        let skipped =
            HeapResourceManager::apply_tuple_update(&mut page, lsn1 - 1, 0, None, None, false)
                .expect("apply older lsn");
        assert!(!skipped);
        let header_after = decode_header(&page);
        assert_eq!(header_after.lsn, lsn1);
    }

    #[test]
    fn apply_tuple_update_overwrites_and_repacks() {
        let mut page = vec![0u8; PAGE_SIZE];
        let lsn1 = 5;
        let mut meta = TupleMeta::new(2, 0);
        let data_small = vec![7u8, 7];
        HeapResourceManager::apply_tuple_update(
            &mut page,
            lsn1,
            0,
            Some(meta),
            Some(&data_small),
            true,
        )
        .unwrap();

        let lsn2 = 20;
        meta.mark_deleted(9, 0);
        let data_large = vec![9u8, 9, 9, 9, 9];
        let applied = HeapResourceManager::apply_tuple_update(
            &mut page,
            lsn2,
            0,
            Some(meta),
            Some(&data_large),
            false,
        )
        .unwrap();
        assert!(applied);

        let header = decode_header(&page);
        assert_eq!(header.lsn, lsn2);
        assert_eq!(header.num_tuples, 1);
        assert!(header.tuple_infos[0].meta.is_deleted);
        let info = &header.tuple_infos[0];
        let stored = &page[info.offset as usize..(info.offset + info.size) as usize];
        assert_eq!(stored, data_large.as_slice());
    }
}
impl ResourceManager for HeapResourceManager {
    fn redo(&self, frame: &WalFrame, ctx: &RedoContext) -> QuillSQLResult<usize> {
        let payload = self.decode_payload(frame)?;
        let applied = match payload {
            HeapRecordPayload::Insert(ref body) => self.redo_insert(frame, ctx, body)?,
            HeapRecordPayload::Delete(ref body) => self.redo_delete(frame, ctx, body)?,
        };
        Ok(applied as usize)
    }

    fn undo(&self, frame: &WalFrame, ctx: &UndoContext) -> QuillSQLResult<()> {
        let payload = self.decode_payload(frame)?;
        match payload {
            HeapRecordPayload::Insert(body) => {
                self.apply_tuple_meta_flag(ctx, body.page_id, body.slot_id as usize, true)
            }
            HeapRecordPayload::Delete(body) => self.restore_tuple(
                ctx,
                body.page_id,
                body.slot_id as usize,
                body.old_tuple_meta,
                &body.old_tuple_data,
            ),
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

impl TableHeap {
    /// Construct a lightweight TableHeap view for recovery operations.
    /// This instance uses an empty schema and does not rely on first/last page ids.
    pub fn recovery_view(buffer_pool: Arc<BufferManager>) -> Self {
        Self {
            schema: EMPTY_SCHEMA_REF.clone(),
            buffer_pool,
            first_page_id: AtomicU32::new(0),
            last_page_id: AtomicU32::new(0),
        }
    }

    /// Recovery-only API: set tuple meta without emitting WAL.
    /// Only used by RecoveryManager during UNDO.
    pub fn recover_set_tuple_meta(&self, rid: RecordId, meta: TupleMeta) -> QuillSQLResult<()> {
        let mut guard = self.buffer_pool.fetch_page_write(rid.page_id)?;
        let (mut header, hdr_len) = TablePageHeaderCodec::decode(guard.data())?;
        if (rid.slot_num as usize) >= header.tuple_infos.len() {
            return Ok(());
        }
        let info = &mut header.tuple_infos[rid.slot_num as usize];
        if info.meta.is_deleted != meta.is_deleted {
            if meta.is_deleted {
                header.num_deleted_tuples = header.num_deleted_tuples.saturating_add(1);
            } else {
                header.num_deleted_tuples = header.num_deleted_tuples.saturating_sub(1);
            }
        }
        info.meta = meta;
        let new_header = TablePageHeaderCodec::encode(&header);
        let copy_len = std::cmp::min(hdr_len, new_header.len());
        guard.data_mut()[0..copy_len].copy_from_slice(&new_header[..copy_len]);
        guard.mark_dirty();
        Ok(())
    }

    /// Recovery-only API: set tuple raw bytes without emitting WAL.
    /// If size mismatches, repack tuple area and update offsets.
    pub fn recover_set_tuple_bytes(&self, rid: RecordId, new_bytes: &[u8]) -> QuillSQLResult<()> {
        let mut guard = self.buffer_pool.fetch_page_write(rid.page_id)?;
        let (mut header, _hdr_len) = TablePageHeaderCodec::decode(guard.data())?;
        if (rid.slot_num as usize) >= header.tuple_infos.len() {
            return Ok(());
        }
        let slot = rid.slot_num as usize;
        let info = &mut header.tuple_infos[slot];
        let off = info.offset as usize;
        let sz = info.size as usize;
        if new_bytes.len() == sz {
            if off + sz <= crate::buffer::PAGE_SIZE {
                guard.data_mut()[off..off + sz].copy_from_slice(new_bytes);
            }
            guard.mark_dirty();
            return Ok(());
        }
        let n = header.tuple_infos.len();
        let mut tuples: Vec<Vec<u8>> = Vec::with_capacity(n);
        for i in 0..n {
            let inf = &header.tuple_infos[i];
            let s = &guard.data()[inf.offset as usize..(inf.offset + inf.size) as usize];
            if i == slot {
                tuples.push(new_bytes.to_vec());
            } else {
                tuples.push(s.to_vec());
            }
        }
        let mut tail = crate::buffer::PAGE_SIZE;
        for i in 0..n {
            let sz = tuples[i].len();
            tail = tail.saturating_sub(sz);
            header.tuple_infos[i].offset = tail as u16;
            header.tuple_infos[i].size = sz as u16;
        }
        let new_header = TablePageHeaderCodec::encode(&header);
        for b in guard.data_mut().iter_mut() {
            *b = 0;
        }
        let hdr_copy = std::cmp::min(new_header.len(), crate::buffer::PAGE_SIZE);
        guard.data_mut()[0..hdr_copy].copy_from_slice(&new_header[..hdr_copy]);
        for i in 0..n {
            let off = header.tuple_infos[i].offset as usize;
            let sz = header.tuple_infos[i].size as usize;
            if off + sz <= crate::buffer::PAGE_SIZE {
                guard.data_mut()[off..off + sz].copy_from_slice(&tuples[i][..sz]);
            }
        }
        guard.mark_dirty();
        Ok(())
    }

    pub fn recover_restore_tuple(
        &self,
        rid: RecordId,
        meta: TupleMeta,
        tuple: &Tuple,
    ) -> QuillSQLResult<()> {
        let bytes = TupleCodec::encode(tuple);
        self.recover_set_tuple_bytes(rid, &bytes)?;
        self.recover_set_tuple_meta(rid, meta)
    }

    pub fn recover_delete_tuple(
        &self,
        rid: RecordId,
        txn_id: TransactionId,
        cid: CommandId,
    ) -> QuillSQLResult<()> {
        let mut meta = self.tuple_meta(rid)?;
        if meta.is_deleted {
            return Ok(());
        }
        meta.mark_deleted(txn_id, cid);
        self.recover_set_tuple_meta(rid, meta)
    }
}
