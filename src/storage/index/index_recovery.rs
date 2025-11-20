use std::sync::Arc;

use bytes::Bytes;

use crate::buffer::{PageId, INVALID_PAGE_ID, PAGE_SIZE};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::resource_manager::{
    register_resource_manager, RedoContext, ResourceManager, UndoContext,
};
use crate::recovery::wal::codec::{ResourceManagerId, WalFrame};
use crate::recovery::Lsn;
use crate::storage::codec::{
    BPlusTreeHeaderPageCodec, BPlusTreeInternalPageCodec, BPlusTreeLeafPageCodec, TupleCodec,
};
use crate::storage::index::wal_codec::{
    decode_index_record, IndexInternalMergePayload, IndexInternalRedistributePayload,
    IndexInternalSplitPayload, IndexLeafDeletePayload, IndexLeafInsertPayload,
    IndexLeafMergePayload, IndexLeafRedistributePayload, IndexLeafSplitPayload,
    IndexParentDeletePayload, IndexParentInsertPayload, IndexParentUpdatePayload,
    IndexRecordPayload, IndexRootAdoptPayload, IndexRootInstallInternalPayload,
    IndexRootInstallLeafPayload, IndexRootResetPayload,
};
use crate::storage::page::{BPlusTreeHeaderPage, BPlusTreeInternalPage, BPlusTreeLeafPage};
use crate::storage::tuple::Tuple;
use crate::transaction::TransactionId;
use std::sync::OnceLock;

#[derive(Default)]
struct IndexResourceManager;

impl IndexResourceManager {
    fn decode_payload(&self, frame: &WalFrame) -> QuillSQLResult<IndexRecordPayload> {
        decode_index_record(&frame.body, frame.info)
    }

    fn transaction_id_of(payload: &IndexRecordPayload) -> Option<TransactionId> {
        match payload {
            IndexRecordPayload::LeafInsert(body) if body.op_txn_id != 0 => Some(body.op_txn_id),
            IndexRecordPayload::LeafDelete(body) if body.op_txn_id != 0 => Some(body.op_txn_id),
            _ => None,
        }
    }

    fn apply_leaf<F>(
        &self,
        ctx: &RedoContext,
        relation: &crate::storage::index::wal_codec::IndexRelationIdent,
        page_id: u32,
        frame_lsn: Lsn,
        mutator: F,
    ) -> QuillSQLResult<bool>
    where
        F: FnOnce(&mut crate::storage::page::BPlusTreeLeafPage) -> QuillSQLResult<bool>,
    {
        let schema = relation.schema_ref();
        if let Some(bpm) = &ctx.buffer_pool {
            if let Ok(mut guard) = bpm.fetch_page_write(page_id) {
                if frame_lsn != 0 && guard.lsn() >= frame_lsn {
                    return Ok(false);
                }
                let (mut leaf_page, _) =
                    BPlusTreeLeafPageCodec::decode(guard.data(), schema.clone())?;
                let applied = mutator(&mut leaf_page)?;
                if applied {
                    let encoded = BPlusTreeLeafPageCodec::encode(&leaf_page);
                    guard.overwrite(&encoded, Some(frame_lsn));
                }
                return Ok(applied);
            }
        }

        let mut data = self.read_or_zero(ctx, page_id);
        let (mut leaf_page, _) = BPlusTreeLeafPageCodec::decode(&data, schema)?;
        if frame_lsn != 0 && leaf_page.header.version as u64 >= frame_lsn {
            return Ok(false);
        }
        let applied = mutator(&mut leaf_page)?;
        if applied {
            let encoded = BPlusTreeLeafPageCodec::encode(&leaf_page);
            data[..encoded.len()].copy_from_slice(&encoded);
            let rx = ctx
                .disk_scheduler
                .schedule_write(page_id, Bytes::from(data))?;
            rx.recv().map_err(|e| {
                QuillSQLError::Internal(format!("Index recovery write completion dropped: {}", e))
            })??;
        }
        Ok(applied)
    }

    fn apply_internal<F>(
        &self,
        ctx: &RedoContext,
        relation: &crate::storage::index::wal_codec::IndexRelationIdent,
        page_id: PageId,
        frame_lsn: Lsn,
        mutator: F,
    ) -> QuillSQLResult<bool>
    where
        F: FnOnce(&mut BPlusTreeInternalPage) -> QuillSQLResult<bool>,
    {
        let schema = relation.schema_ref();
        if let Some(bpm) = &ctx.buffer_pool {
            if let Ok(mut guard) = bpm.fetch_page_write(page_id) {
                if frame_lsn != 0 && guard.lsn() >= frame_lsn {
                    return Ok(false);
                }
                let (mut page, _) =
                    BPlusTreeInternalPageCodec::decode(guard.data(), schema.clone())?;
                let applied = mutator(&mut page)?;
                if applied {
                    let encoded = BPlusTreeInternalPageCodec::encode(&page);
                    guard.overwrite(&encoded, Some(frame_lsn));
                }
                return Ok(applied);
            }
        }

        let mut data = self.read_or_zero(ctx, page_id);
        let (mut page, _) = BPlusTreeInternalPageCodec::decode(&data, schema.clone())?;
        if frame_lsn != 0 && page.header.version as u64 >= frame_lsn {
            return Ok(false);
        }
        let applied = mutator(&mut page)?;
        if applied {
            let encoded = BPlusTreeInternalPageCodec::encode(&page);
            data[..encoded.len()].copy_from_slice(&encoded);
            let rx = ctx
                .disk_scheduler
                .schedule_write(page_id, Bytes::from(data))?;
            rx.recv().map_err(|e| {
                QuillSQLError::Internal(format!("Index recovery write completion dropped: {}", e))
            })??;
        }
        Ok(applied)
    }

    fn redo_leaf_split(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        body: &IndexLeafSplitPayload,
    ) -> QuillSQLResult<bool> {
        let mut applied = false;
        let split_index = body.split_index as usize;
        let left_applied =
            self.apply_leaf(ctx, &body.relation, body.left_page_id, frame.lsn, |leaf| {
                let current = leaf.header.current_size as usize;
                if current <= split_index && leaf.header.next_page_id == body.right_page_id {
                    return Ok(false);
                }
                if current < split_index {
                    return Ok(false);
                }
                leaf.array.truncate(split_index);
                leaf.header.current_size = split_index as u32;
                leaf.header.next_page_id = body.right_page_id;
                leaf.header.version += 1;
                Ok(true)
            })?;
        if left_applied {
            applied = true;
        }
        if self.write_leaf_split_right(ctx, frame.lsn, body)? {
            applied = true;
        }
        Ok(applied)
    }

    fn redo_leaf_merge(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        body: &IndexLeafMergePayload,
    ) -> QuillSQLResult<bool> {
        let schema = body.relation.schema_ref();
        let mut leaf = BPlusTreeLeafPage::new(schema.clone(), body.leaf_max_size);
        leaf.array.clear();
        for entry in &body.entries {
            let (tuple, _) = TupleCodec::decode(&entry.key_data, schema.clone())?;
            leaf.array.push((tuple, entry.rid));
        }
        leaf.header.current_size = leaf.array.len() as u32;
        leaf.header.next_page_id = body.left_next_page_id;
        leaf.header.version += 1;
        self.write_leaf_page(ctx, frame.lsn, body.left_page_id, &leaf)
    }

    fn write_leaf_page(
        &self,
        ctx: &RedoContext,
        lsn: Lsn,
        page_id: PageId,
        leaf: &BPlusTreeLeafPage,
    ) -> QuillSQLResult<bool> {
        let encoded = BPlusTreeLeafPageCodec::encode(leaf);
        if let Some(bpm) = &ctx.buffer_pool {
            if let Ok(mut guard) = bpm.fetch_page_write(page_id) {
                if guard.lsn() >= lsn {
                    return Ok(false);
                }
                guard.overwrite(&encoded, Some(lsn));
                return Ok(true);
            }
        }
        let rx = ctx
            .disk_scheduler
            .schedule_write(page_id, Bytes::from(encoded))?;
        rx.recv().map_err(|e| {
            QuillSQLError::Internal(format!("Index redo leaf split write failed: {}", e))
        })??;
        Ok(true)
    }

    fn write_leaf_split_right(
        &self,
        ctx: &RedoContext,
        lsn: Lsn,
        body: &IndexLeafSplitPayload,
    ) -> QuillSQLResult<bool> {
        let schema = body.relation.schema_ref();
        let mut leaf = BPlusTreeLeafPage::new(schema.clone(), body.leaf_max_size);
        for entry in &body.entries {
            let (tuple, _) = TupleCodec::decode(&entry.key_data, schema.clone())?;
            leaf.array.push((tuple, entry.rid));
        }
        leaf.header.current_size = leaf.array.len() as u32;
        leaf.header.next_page_id = body.right_next_page_id;
        leaf.header.version += 1;
        self.write_leaf_page(ctx, lsn, body.right_page_id, &leaf)
    }

    fn write_internal_page(
        &self,
        ctx: &RedoContext,
        lsn: Lsn,
        page_id: PageId,
        page: &BPlusTreeInternalPage,
    ) -> QuillSQLResult<bool> {
        let encoded = BPlusTreeInternalPageCodec::encode(page);
        if let Some(bpm) = &ctx.buffer_pool {
            if let Ok(mut guard) = bpm.fetch_page_write(page_id) {
                if lsn != 0 && guard.lsn() >= lsn {
                    return Ok(false);
                }
                guard.overwrite(&encoded, Some(lsn));
                return Ok(true);
            }
        }
        let rx = ctx
            .disk_scheduler
            .schedule_write(page_id, Bytes::from(encoded))?;
        rx.recv().map_err(|e| {
            QuillSQLError::Internal(format!("Index recovery write completion dropped: {}", e))
        })??;
        Ok(true)
    }

    fn write_header_root(
        &self,
        ctx: &RedoContext,
        lsn: Lsn,
        relation: &crate::storage::index::wal_codec::IndexRelationIdent,
        root_page_id: PageId,
    ) -> QuillSQLResult<bool> {
        let header = BPlusTreeHeaderPage { root_page_id };
        let encoded = BPlusTreeHeaderPageCodec::encode(&header);
        if let Some(bpm) = &ctx.buffer_pool {
            if let Ok(mut guard) = bpm.fetch_page_write(relation.header_page_id) {
                if lsn != 0 && guard.lsn() >= lsn {
                    return Ok(false);
                }
                guard.overwrite(&encoded, Some(lsn));
                return Ok(true);
            }
        }
        let rx = ctx
            .disk_scheduler
            .schedule_write(relation.header_page_id, Bytes::from(encoded))?;
        rx.recv().map_err(|e| {
            QuillSQLError::Internal(format!("Index recovery header write failed: {}", e))
        })??;
        Ok(true)
    }

    fn redo_root_install_leaf(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        body: &IndexRootInstallLeafPayload,
    ) -> QuillSQLResult<bool> {
        let schema = body.relation.schema_ref();
        let mut leaf = BPlusTreeLeafPage::new(schema.clone(), body.leaf_max_size);
        leaf.array.clear();
        for entry in &body.entries {
            let tuple = decode_tuple(&body.relation, &entry.key_data)?;
            leaf.array.push((tuple, entry.rid));
        }
        leaf.header.current_size = leaf.array.len() as u32;
        leaf.header.next_page_id = body.next_page_id;
        leaf.header.version += 1;

        let leaf_applied = self.write_leaf_page(ctx, frame.lsn, body.page_id, &leaf)?;
        let header_applied =
            self.write_header_root(ctx, frame.lsn, &body.relation, body.page_id)?;
        Ok(leaf_applied || header_applied)
    }

    fn redo_root_install_internal(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        body: &IndexRootInstallInternalPayload,
    ) -> QuillSQLResult<bool> {
        let schema = body.relation.schema_ref();
        let mut page = BPlusTreeInternalPage::new(schema.clone(), body.internal_max_size);
        page.array.clear();
        for entry in &body.entries {
            let tuple = decode_tuple(&body.relation, &entry.key_data)?;
            page.array.push((tuple, entry.child_page_id));
        }
        page.header.current_size = page.array.len() as u32;
        page.high_key = match &body.high_key {
            Some(bytes) => Some(decode_tuple(&body.relation, bytes)?),
            None => None,
        };
        page.header.next_page_id = body.next_page_id;
        page.header.version += 1;

        let page_applied = self.write_internal_page(ctx, frame.lsn, body.page_id, &page)?;
        let header_applied =
            self.write_header_root(ctx, frame.lsn, &body.relation, body.page_id)?;
        Ok(page_applied || header_applied)
    }

    fn redo_root_adopt(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        body: &IndexRootAdoptPayload,
    ) -> QuillSQLResult<bool> {
        self.write_header_root(ctx, frame.lsn, &body.relation, body.new_root_page_id)
    }

    fn redo_root_reset(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        body: &IndexRootResetPayload,
    ) -> QuillSQLResult<bool> {
        self.write_header_root(ctx, frame.lsn, &body.relation, INVALID_PAGE_ID)
    }

    fn redo_internal_split(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        body: &IndexInternalSplitPayload,
    ) -> QuillSQLResult<bool> {
        let left_high = match &body.left_high_key {
            Some(bytes) => Some(decode_tuple(&body.relation, bytes)?),
            None => None,
        };
        let left_applied =
            self.apply_internal(ctx, &body.relation, body.left_page_id, frame.lsn, |page| {
                let desired = body.left_new_size as usize;
                if page.header.current_size as usize == desired
                    && page.header.next_page_id == body.left_next_page_id
                    && Self::high_key_equal(page.high_key.as_ref(), left_high.as_ref())
                {
                    return Ok(false);
                }
                if page.array.len() >= desired {
                    page.array.truncate(desired);
                } else {
                    return Err(QuillSQLError::Internal(
                        "internal split redo: left page smaller than expected".to_string(),
                    ));
                }
                page.header.current_size = desired as u32;
                page.high_key = left_high.clone();
                page.header.next_page_id = body.left_next_page_id;
                page.header.version += 1;
                Ok(true)
            })?;

        let schema = body.relation.schema_ref();
        let mut right_page = BPlusTreeInternalPage::new(schema.clone(), body.internal_max_size);
        right_page.array.clear();
        for entry in &body.right_entries {
            let tuple = decode_tuple(&body.relation, &entry.key_data)?;
            right_page.array.push((tuple, entry.child_page_id));
        }
        right_page.header.current_size = right_page.array.len() as u32;
        right_page.high_key = match &body.right_high_key {
            Some(bytes) => Some(decode_tuple(&body.relation, bytes)?),
            None => None,
        };
        right_page.header.next_page_id = body.right_next_page_id;
        right_page.header.version += 1;
        let right_applied =
            self.write_internal_page(ctx, frame.lsn, body.right_page_id, &right_page)?;
        Ok(left_applied || right_applied)
    }

    fn redo_internal_merge(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        body: &IndexInternalMergePayload,
    ) -> QuillSQLResult<bool> {
        let schema = body.relation.schema_ref();
        let mut page = BPlusTreeInternalPage::new(schema.clone(), body.internal_max_size);
        page.array.clear();
        for entry in &body.left_entries {
            let tuple = decode_tuple(&body.relation, &entry.key_data)?;
            page.array.push((tuple, entry.child_page_id));
        }
        page.header.current_size = page.array.len() as u32;
        page.high_key = match &body.high_key {
            Some(bytes) => Some(decode_tuple(&body.relation, bytes)?),
            None => None,
        };
        page.header.next_page_id = body.next_page_id;
        page.header.version += 1;
        self.write_internal_page(ctx, frame.lsn, body.left_page_id, &page)
    }

    fn redo_parent_insert(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        body: &IndexParentInsertPayload,
    ) -> QuillSQLResult<bool> {
        self.apply_internal(
            ctx,
            &body.relation,
            body.parent_page_id,
            frame.lsn,
            |page| {
                let tuple = decode_tuple(&body.relation, &body.key_data)?;
                if let Some(pos) = page
                    .array
                    .iter()
                    .position(|(_, pid)| *pid == body.left_child_page_id)
                {
                    if pos + 1 < page.array.len()
                        && page.array[pos + 1].1 == body.right_child_page_id
                    {
                        return Ok(false);
                    }
                    page.array
                        .insert(pos + 1, (tuple, body.right_child_page_id));
                    page.header.current_size += 1;
                    page.header.version += 1;
                    Ok(true)
                } else {
                    Ok(false)
                }
            },
        )
    }

    fn redo_parent_delete(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        body: &IndexParentDeletePayload,
    ) -> QuillSQLResult<bool> {
        self.apply_internal(
            ctx,
            &body.relation,
            body.parent_page_id,
            frame.lsn,
            |page| {
                if let Some(idx) = page.value_index(body.child_page_id) {
                    page.array.remove(idx);
                    page.header.current_size -= 1;
                    page.header.version += 1;
                    Ok(true)
                } else {
                    Ok(false)
                }
            },
        )
    }

    fn redo_parent_update(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        body: &IndexParentUpdatePayload,
    ) -> QuillSQLResult<bool> {
        let tuple = decode_tuple(&body.relation, &body.key_data)?;
        self.apply_internal(
            ctx,
            &body.relation,
            body.parent_page_id,
            frame.lsn,
            |page| {
                if let Some(idx) = page.value_index(body.child_page_id) {
                    page.array[idx].0 = tuple.clone();
                    page.header.version += 1;
                    Ok(true)
                } else {
                    Ok(false)
                }
            },
        )
    }

    fn read_or_zero(&self, ctx: &RedoContext, page_id: u32) -> Vec<u8> {
        match ctx.disk_scheduler.schedule_read(page_id) {
            Ok(rx) => match rx.recv() {
                Ok(Ok(bytes)) if bytes.len() == PAGE_SIZE => bytes.to_vec(),
                _ => vec![0u8; PAGE_SIZE],
            },
            Err(_) => vec![0u8; PAGE_SIZE],
        }
    }

    fn high_key_equal(a: Option<&Tuple>, b: Option<&Tuple>) -> bool {
        match (a, b) {
            (None, None) => true,
            (Some(x), Some(y)) => x == y,
            _ => false,
        }
    }

    fn redo_insert(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        payload: &IndexLeafInsertPayload,
    ) -> QuillSQLResult<bool> {
        self.apply_leaf(ctx, &payload.relation, payload.page_id, frame.lsn, |leaf| {
            let tuple = decode_tuple(&payload.relation, &payload.key_data)?;
            if let Some(existing) = leaf.look_up_mut(&tuple) {
                *existing = payload.rid;
            } else {
                leaf.insert(tuple, payload.rid);
                leaf.header.version += 1;
            }
            Ok(true)
        })
    }

    fn redo_delete(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        payload: &IndexLeafDeletePayload,
    ) -> QuillSQLResult<bool> {
        self.apply_leaf(ctx, &payload.relation, payload.page_id, frame.lsn, |leaf| {
            let tuple = decode_tuple(&payload.relation, &payload.key_data)?;
            if leaf.look_up(&tuple).is_some() {
                leaf.delete(&tuple);
                leaf.header.version += 1;
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }

    fn redo_leaf_redistribute(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        payload: &IndexLeafRedistributePayload,
    ) -> QuillSQLResult<bool> {
        let mut applied = false;
        let tuple = decode_tuple(&payload.relation, &payload.moved_entry.key_data)?;
        let from_rid = payload.moved_entry.rid;
        let from_applied = self.apply_leaf(
            ctx,
            &payload.relation,
            payload.from_page_id,
            frame.lsn,
            |leaf| {
                if leaf.header.current_size == 0 {
                    return Ok(false);
                }
                if payload.from_is_left {
                    let idx = leaf.header.current_size as usize - 1;
                    let existing = leaf.array[idx].clone();
                    if existing.0 != tuple || existing.1 != from_rid {
                        return Ok(false);
                    }
                    leaf.array.remove(idx);
                } else {
                    let existing = leaf.array.first().cloned();
                    if let Some(kv) = existing {
                        if kv.0 != tuple || kv.1 != from_rid {
                            return Ok(false);
                        }
                        leaf.array.remove(0);
                    } else {
                        return Ok(false);
                    }
                }
                leaf.header.current_size = leaf.array.len() as u32;
                leaf.header.version += 1;
                Ok(true)
            },
        )?;
        if from_applied {
            applied = true;
        }
        let to_applied = self.apply_leaf(
            ctx,
            &payload.relation,
            payload.to_page_id,
            frame.lsn,
            |leaf| {
                if payload.from_is_left {
                    leaf.array.insert(0, (tuple.clone(), from_rid));
                } else {
                    leaf.array.push((tuple.clone(), from_rid));
                }
                leaf.header.current_size = leaf.array.len() as u32;
                leaf.header.version += 1;
                Ok(true)
            },
        )?;
        if to_applied {
            applied = true;
        }
        Ok(applied)
    }

    fn redo_internal_redistribute(
        &self,
        frame: &WalFrame,
        ctx: &RedoContext,
        payload: &IndexInternalRedistributePayload,
    ) -> QuillSQLResult<bool> {
        let mut applied = false;
        let moved_tuple = decode_tuple(&payload.relation, &payload.moved_entry.key_data)?;
        let separator_tuple = decode_tuple(&payload.relation, &payload.separator_key_data)?;
        let to_high = match &payload.to_new_high_key {
            Some(bytes) => Some(decode_tuple(&payload.relation, bytes)?),
            None => None,
        };
        let from_high = match &payload.from_new_high_key {
            Some(bytes) => Some(decode_tuple(&payload.relation, bytes)?),
            None => None,
        };
        let from_applied = self.apply_internal(
            ctx,
            &payload.relation,
            payload.from_page_id,
            frame.lsn,
            |page| {
                if page.header.current_size == 0 {
                    return Ok(false);
                }
                if payload.from_is_left {
                    let idx = page.header.current_size as usize - 1;
                    let existing = page.array[idx].clone();
                    if existing.0 != moved_tuple || existing.1 != payload.moved_entry.child_page_id
                    {
                        return Ok(false);
                    }
                    page.array.remove(idx);
                } else {
                    if page.array.len() <= 1 {
                        return Ok(false);
                    }
                    let existing = page.array[1].clone();
                    if existing.0 != moved_tuple || existing.1 != payload.moved_entry.child_page_id
                    {
                        return Ok(false);
                    }
                    page.array.remove(1);
                    page.array[0].1 = payload.moved_entry.child_page_id;
                }
                page.header.current_size = page.array.len() as u32;
                page.high_key = from_high.clone();
                page.header.next_page_id = payload.from_new_next_page_id;
                page.header.version += 1;
                Ok(true)
            },
        )?;
        if from_applied {
            applied = true;
        }
        let to_applied = self.apply_internal(
            ctx,
            &payload.relation,
            payload.to_page_id,
            frame.lsn,
            |page| {
                if payload.from_is_left {
                    page.array
                        .insert(1, (separator_tuple.clone(), payload.to_old_sentinel));
                    page.array[0].1 = payload.moved_entry.child_page_id;
                } else {
                    page.array
                        .push((separator_tuple.clone(), payload.from_old_sentinel));
                }
                page.header.current_size = page.array.len() as u32;
                page.high_key = to_high.clone();
                page.header.next_page_id = payload.to_new_next_page_id;
                page.header.version += 1;
                Ok(true)
            },
        )?;
        if to_applied {
            applied = true;
        }
        Ok(applied)
    }

    fn undo_insert(
        &self,
        ctx: &UndoContext,
        payload: &IndexLeafInsertPayload,
    ) -> QuillSQLResult<()> {
        let redo_like = RedoContext {
            disk_scheduler: ctx.disk_scheduler.clone(),
            buffer_pool: ctx.buffer_pool.clone(),
        };
        self.apply_leaf(&redo_like, &payload.relation, payload.page_id, 0, |leaf| {
            let tuple = decode_tuple(&payload.relation, &payload.key_data)?;
            leaf.delete(&tuple);
            Ok(true)
        })?;
        Ok(())
    }

    fn undo_delete(
        &self,
        ctx: &UndoContext,
        payload: &IndexLeafDeletePayload,
    ) -> QuillSQLResult<()> {
        let redo_like = RedoContext {
            disk_scheduler: ctx.disk_scheduler.clone(),
            buffer_pool: ctx.buffer_pool.clone(),
        };
        self.apply_leaf(&redo_like, &payload.relation, payload.page_id, 0, |leaf| {
            let tuple = decode_tuple(&payload.relation, &payload.key_data)?;
            leaf.insert(tuple, payload.old_rid);
            Ok(true)
        })?;
        Ok(())
    }
}

impl ResourceManager for IndexResourceManager {
    fn redo(&self, frame: &WalFrame, ctx: &RedoContext) -> QuillSQLResult<usize> {
        let payload = self.decode_payload(frame)?;
        let applied = match &payload {
            IndexRecordPayload::LeafInsert(body) => self.redo_insert(frame, ctx, body)?,
            IndexRecordPayload::LeafDelete(body) => self.redo_delete(frame, ctx, body)?,
            IndexRecordPayload::LeafSplit(body) => self.redo_leaf_split(frame, ctx, body)?,
            IndexRecordPayload::InternalSplit(body) => {
                self.redo_internal_split(frame, ctx, body)?
            }
            IndexRecordPayload::ParentInsert(body) => self.redo_parent_insert(frame, ctx, body)?,
            IndexRecordPayload::LeafMerge(body) => self.redo_leaf_merge(frame, ctx, body)?,
            IndexRecordPayload::InternalMerge(body) => {
                self.redo_internal_merge(frame, ctx, body)?
            }
            IndexRecordPayload::ParentDelete(body) => self.redo_parent_delete(frame, ctx, body)?,
            IndexRecordPayload::ParentUpdate(body) => self.redo_parent_update(frame, ctx, body)?,
            IndexRecordPayload::LeafRedistribute(body) => {
                self.redo_leaf_redistribute(frame, ctx, body)?
            }
            IndexRecordPayload::InternalRedistribute(body) => {
                self.redo_internal_redistribute(frame, ctx, body)?
            }
            IndexRecordPayload::RootInstallLeaf(body) => {
                self.redo_root_install_leaf(frame, ctx, body)?
            }
            IndexRecordPayload::RootInstallInternal(body) => {
                self.redo_root_install_internal(frame, ctx, body)?
            }
            IndexRecordPayload::RootAdopt(body) => self.redo_root_adopt(frame, ctx, body)?,
            IndexRecordPayload::RootReset(body) => self.redo_root_reset(frame, ctx, body)?,
        };
        Ok(applied as usize)
    }

    fn undo(&self, frame: &WalFrame, ctx: &UndoContext) -> QuillSQLResult<()> {
        let payload = self.decode_payload(frame)?;
        match payload {
            IndexRecordPayload::LeafInsert(body) => self.undo_insert(ctx, &body),
            IndexRecordPayload::LeafDelete(body) => self.undo_delete(ctx, &body),
            IndexRecordPayload::LeafSplit(_) => Ok(()),
            IndexRecordPayload::InternalSplit(_) => Ok(()),
            IndexRecordPayload::ParentInsert(_) => Ok(()),
            IndexRecordPayload::LeafMerge(_) => Ok(()),
            IndexRecordPayload::InternalMerge(_) => Ok(()),
            IndexRecordPayload::ParentDelete(_) => Ok(()),
            IndexRecordPayload::ParentUpdate(_) => Ok(()),
            IndexRecordPayload::LeafRedistribute(_) => Ok(()),
            IndexRecordPayload::InternalRedistribute(_) => Ok(()),
            IndexRecordPayload::RootInstallLeaf(_) => Ok(()),
            IndexRecordPayload::RootInstallInternal(_) => Ok(()),
            IndexRecordPayload::RootAdopt(_) => Ok(()),
            IndexRecordPayload::RootReset(_) => Ok(()),
        }
    }

    fn transaction_id(&self, frame: &WalFrame) -> Option<u64> {
        self.decode_payload(frame)
            .ok()
            .and_then(|payload| Self::transaction_id_of(&payload))
    }
}

fn decode_tuple(
    relation: &crate::storage::index::wal_codec::IndexRelationIdent,
    key_data: &[u8],
) -> QuillSQLResult<Tuple> {
    let schema = relation.schema_ref();
    let (tuple, _) = TupleCodec::decode(key_data, schema)?;
    Ok(tuple)
}

static REGISTER: OnceLock<()> = OnceLock::new();

pub fn ensure_index_resource_manager_registered() {
    REGISTER.get_or_init(|| {
        register_resource_manager(
            ResourceManagerId::Index,
            Arc::new(IndexResourceManager::default()),
        );
    });
}
