use std::sync::Arc;

use bytes::Bytes;

use crate::buffer::PAGE_SIZE;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::resource_manager::{
    register_resource_manager, RedoContext, ResourceManager, UndoContext,
};
use crate::recovery::wal::codec::{ResourceManagerId, WalFrame};
use crate::recovery::Lsn;
use crate::storage::codec::{BPlusTreeLeafPageCodec, TupleCodec};
use crate::storage::index::wal_codec::{
    decode_index_record, IndexLeafDeletePayload, IndexLeafInsertPayload, IndexRecordPayload,
};
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
            IndexRecordPayload::LeafInsert(_) => None,
            IndexRecordPayload::LeafDelete(_) => None,
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

    fn read_or_zero(&self, ctx: &RedoContext, page_id: u32) -> Vec<u8> {
        match ctx.disk_scheduler.schedule_read(page_id) {
            Ok(rx) => match rx.recv() {
                Ok(Ok(bytes)) if bytes.len() == PAGE_SIZE => bytes.to_vec(),
                _ => vec![0u8; PAGE_SIZE],
            },
            Err(_) => vec![0u8; PAGE_SIZE],
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
        };
        Ok(applied as usize)
    }

    fn undo(&self, frame: &WalFrame, ctx: &UndoContext) -> QuillSQLResult<()> {
        let payload = self.decode_payload(frame)?;
        match payload {
            IndexRecordPayload::LeafInsert(body) => self.undo_insert(ctx, &body),
            IndexRecordPayload::LeafDelete(body) => self.undo_delete(ctx, &body),
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
