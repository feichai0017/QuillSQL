use std::convert::TryFrom;

use crate::buffer::PageId;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::codec::RidCodec;
use crate::storage::page::{RecordId, TupleMeta};
use crate::transaction::{CommandId, TransactionId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationIdent {
    pub root_page_id: PageId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleMetaRepr {
    pub insert_txn_id: TransactionId,
    pub insert_cid: CommandId,
    pub delete_txn_id: TransactionId,
    pub delete_cid: CommandId,
    pub is_deleted: bool,
    pub next_version: Option<RecordId>,
    pub prev_version: Option<RecordId>,
}

impl From<TupleMetaRepr> for TupleMeta {
    fn from(value: TupleMetaRepr) -> Self {
        TupleMeta {
            insert_txn_id: value.insert_txn_id,
            insert_cid: value.insert_cid,
            delete_txn_id: value.delete_txn_id,
            delete_cid: value.delete_cid,
            is_deleted: value.is_deleted,
            next_version: value.next_version,
            prev_version: value.prev_version,
        }
    }
}

impl From<TupleMeta> for TupleMetaRepr {
    fn from(value: TupleMeta) -> Self {
        TupleMetaRepr {
            insert_txn_id: value.insert_txn_id,
            insert_cid: value.insert_cid,
            delete_txn_id: value.delete_txn_id,
            delete_cid: value.delete_cid,
            is_deleted: value.is_deleted,
            next_version: value.next_version,
            prev_version: value.prev_version,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HeapInsertPayload {
    pub relation: RelationIdent,
    pub page_id: PageId,
    pub slot_id: u16,
    /// transaction id that produced this heap operation
    pub op_txn_id: TransactionId,
    pub tuple_meta: TupleMetaRepr,
    pub tuple_data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct HeapUpdatePayload {
    pub relation: RelationIdent,
    pub page_id: PageId,
    pub slot_id: u16,
    /// transaction id that produced this heap operation
    pub op_txn_id: TransactionId,
    pub new_tuple_meta: TupleMetaRepr,
    pub new_tuple_data: Vec<u8>,
    pub old_tuple_meta: Option<TupleMetaRepr>,
    pub old_tuple_data: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct HeapDeletePayload {
    pub relation: RelationIdent,
    pub page_id: PageId,
    pub slot_id: u16,
    /// transaction id that produced this heap operation
    pub op_txn_id: TransactionId,
    pub old_tuple_meta: TupleMetaRepr,
    pub old_tuple_data: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub enum HeapRecordPayload {
    Insert(HeapInsertPayload),
    Update(HeapUpdatePayload),
    Delete(HeapDeletePayload),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HeapRecordKind {
    Insert = 1,
    Update = 2,
    Delete = 3,
}

impl TryFrom<u8> for HeapRecordKind {
    type Error = QuillSQLError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(HeapRecordKind::Insert),
            2 => Ok(HeapRecordKind::Update),
            3 => Ok(HeapRecordKind::Delete),
            other => Err(QuillSQLError::Internal(format!(
                "Unknown heap record kind: {}",
                other
            ))),
        }
    }
}

pub fn encode_heap_record(payload: &HeapRecordPayload) -> (u8, Vec<u8>) {
    match payload {
        HeapRecordPayload::Insert(body) => (HeapRecordKind::Insert as u8, encode_heap_insert(body)),
        HeapRecordPayload::Update(body) => (HeapRecordKind::Update as u8, encode_heap_update(body)),
        HeapRecordPayload::Delete(body) => (HeapRecordKind::Delete as u8, encode_heap_delete(body)),
    }
}

pub fn decode_heap_record(bytes: &[u8], info: u8) -> QuillSQLResult<HeapRecordPayload> {
    match HeapRecordKind::try_from(info)? {
        HeapRecordKind::Insert => Ok(HeapRecordPayload::Insert(decode_heap_insert(bytes)?)),
        HeapRecordKind::Update => Ok(HeapRecordPayload::Update(decode_heap_update(bytes)?)),
        HeapRecordKind::Delete => Ok(HeapRecordPayload::Delete(decode_heap_delete(bytes)?)),
    }
}

fn encode_relation_ident(relation: &RelationIdent, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&relation.root_page_id.to_le_bytes());
}

fn decode_relation_ident(bytes: &[u8]) -> QuillSQLResult<(RelationIdent, usize)> {
    if bytes.len() < 4 {
        return Err(QuillSQLError::Internal(
            "Heap payload too short for relation ident".to_string(),
        ));
    }
    let root_page_id = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as PageId;
    Ok((RelationIdent { root_page_id }, 4))
}

fn encode_tuple_meta(meta: &TupleMetaRepr, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&meta.insert_txn_id.to_le_bytes());
    buf.extend_from_slice(&meta.insert_cid.to_le_bytes());
    buf.extend_from_slice(&meta.delete_txn_id.to_le_bytes());
    buf.extend_from_slice(&meta.delete_cid.to_le_bytes());
    buf.push(meta.is_deleted as u8);
    if let Some(next) = meta.next_version {
        buf.push(1);
        buf.extend(RidCodec::encode(&next));
    } else {
        buf.push(0);
    }
    if let Some(prev) = meta.prev_version {
        buf.push(1);
        buf.extend(RidCodec::encode(&prev));
    } else {
        buf.push(0);
    }
}

fn decode_tuple_meta(bytes: &[u8]) -> QuillSQLResult<(TupleMetaRepr, usize)> {
    if bytes.len() < 8 + 4 + 8 + 4 + 1 + 1 + 1 {
        return Err(QuillSQLError::Internal(
            "Heap payload too short for tuple meta".to_string(),
        ));
    }
    let insert_txn_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as TransactionId;
    let insert_cid = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as CommandId;
    let delete_txn_id = u64::from_le_bytes(bytes[12..20].try_into().unwrap()) as TransactionId;
    let delete_cid = u32::from_le_bytes(bytes[20..24].try_into().unwrap()) as CommandId;
    let is_deleted = bytes[24] != 0;
    let mut offset = 25;

    let has_next = bytes
        .get(offset)
        .copied()
        .ok_or_else(|| QuillSQLError::Internal("tuple meta missing next flag".to_string()))?
        != 0;
    offset += 1;
    let next_version = if has_next {
        let (rid, consumed) = RidCodec::decode(&bytes[offset..])?;
        offset += consumed;
        Some(rid)
    } else {
        None
    };

    let has_prev = bytes
        .get(offset)
        .copied()
        .ok_or_else(|| QuillSQLError::Internal("tuple meta missing prev flag".to_string()))?
        != 0;
    offset += 1;
    let prev_version = if has_prev {
        let (rid, consumed) = RidCodec::decode(&bytes[offset..])?;
        offset += consumed;
        Some(rid)
    } else {
        None
    };

    Ok((
        TupleMetaRepr {
            insert_txn_id,
            insert_cid,
            delete_txn_id,
            delete_cid,
            is_deleted,
            next_version,
            prev_version,
        },
        offset,
    ))
}

fn encode_bytes(data: &[u8], buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(data);
}

fn decode_bytes(bytes: &[u8]) -> QuillSQLResult<(Vec<u8>, usize)> {
    if bytes.len() < 4 {
        return Err(QuillSQLError::Internal(
            "Heap payload missing length prefix".to_string(),
        ));
    }
    let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if bytes.len() < 4 + len {
        return Err(QuillSQLError::Internal(
            "Heap payload length prefix out of bounds".to_string(),
        ));
    }
    Ok((bytes[4..4 + len].to_vec(), 4 + len))
}

fn encode_optional_bytes(opt: &Option<Vec<u8>>, buf: &mut Vec<u8>) {
    match opt {
        Some(data) => {
            buf.push(1);
            encode_bytes(data, buf);
        }
        None => buf.push(0),
    }
}

fn decode_optional_bytes(bytes: &[u8]) -> QuillSQLResult<(Option<Vec<u8>>, usize)> {
    if bytes.is_empty() {
        return Err(QuillSQLError::Internal(
            "Heap payload missing option flag".to_string(),
        ));
    }
    let flag = bytes[0];
    if flag == 0 {
        Ok((None, 1))
    } else {
        let (data, consumed) = decode_bytes(&bytes[1..])?;
        Ok((Some(data), consumed + 1))
    }
}

fn encode_optional_meta(opt: &Option<TupleMetaRepr>, buf: &mut Vec<u8>) {
    match opt {
        Some(meta) => {
            buf.push(1);
            encode_tuple_meta(meta, buf);
        }
        None => buf.push(0),
    }
}

fn decode_optional_meta(bytes: &[u8]) -> QuillSQLResult<(Option<TupleMetaRepr>, usize)> {
    if bytes.is_empty() {
        return Err(QuillSQLError::Internal(
            "Heap payload missing option flag".to_string(),
        ));
    }
    let flag = bytes[0];
    if flag == 0 {
        Ok((None, 1))
    } else {
        let (meta, consumed) = decode_tuple_meta(&bytes[1..])?;
        Ok((Some(meta), consumed + 1))
    }
}

fn encode_heap_insert(body: &HeapInsertPayload) -> Vec<u8> {
    // Heap/Insert (rmid=Heap, info=1)
    // body: relation(root_id u32) + page_id(4) + slot_id(2) + op_txn_id(8) + tuple_meta(17B) + tuple_data_len+data
    let mut buf = Vec::new();
    encode_relation_ident(&body.relation, &mut buf);
    buf.extend_from_slice(&body.page_id.to_le_bytes());
    buf.extend_from_slice(&body.slot_id.to_le_bytes());
    buf.extend_from_slice(&body.op_txn_id.to_le_bytes());
    encode_tuple_meta(&body.tuple_meta, &mut buf);
    encode_bytes(&body.tuple_data, &mut buf);
    buf
}

fn decode_heap_insert(bytes: &[u8]) -> QuillSQLResult<HeapInsertPayload> {
    let (relation, mut offset) = decode_relation_ident(bytes)?;
    if bytes.len() < offset + 4 + 2 {
        return Err(QuillSQLError::Internal(
            "Heap insert payload too short".to_string(),
        ));
    }
    let page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as PageId;
    offset += 4;
    let slot_id = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
    offset += 2;
    if bytes.len() < offset + 8 {
        return Err(QuillSQLError::Internal(
            "Heap insert payload missing op_txn_id".to_string(),
        ));
    }
    let op_txn_id =
        u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as TransactionId;
    offset += 8;
    let (tuple_meta, consumed) = decode_tuple_meta(&bytes[offset..])?;
    offset += consumed;
    let (tuple_data, _consumed) = decode_bytes(&bytes[offset..])?;
    Ok(HeapInsertPayload {
        relation,
        page_id,
        slot_id,
        op_txn_id,
        tuple_meta,
        tuple_data,
    })
}

fn encode_heap_update(body: &HeapUpdatePayload) -> Vec<u8> {
    // Heap/Update (rmid=Heap, info=2)
    // body: relation + page_id + slot_id + op_txn_id + new_meta(17B) + new_data + has_old_meta+old_meta + has_old_data+old_data
    let mut buf = Vec::new();
    encode_relation_ident(&body.relation, &mut buf);
    buf.extend_from_slice(&body.page_id.to_le_bytes());
    buf.extend_from_slice(&body.slot_id.to_le_bytes());
    buf.extend_from_slice(&body.op_txn_id.to_le_bytes());
    encode_tuple_meta(&body.new_tuple_meta, &mut buf);
    encode_bytes(&body.new_tuple_data, &mut buf);
    encode_optional_meta(&body.old_tuple_meta, &mut buf);
    encode_optional_bytes(&body.old_tuple_data, &mut buf);
    buf
}

fn decode_heap_update(bytes: &[u8]) -> QuillSQLResult<HeapUpdatePayload> {
    let (relation, mut offset) = decode_relation_ident(bytes)?;
    if bytes.len() < offset + 4 + 2 {
        return Err(QuillSQLError::Internal(
            "Heap update payload too short".to_string(),
        ));
    }
    let page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as PageId;
    offset += 4;
    let slot_id = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
    offset += 2;
    if bytes.len() < offset + 8 {
        return Err(QuillSQLError::Internal(
            "Heap update payload missing op_txn_id".to_string(),
        ));
    }
    let op_txn_id =
        u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as TransactionId;
    offset += 8;
    let (new_tuple_meta, consumed) = decode_tuple_meta(&bytes[offset..])?;
    offset += consumed;
    let (new_tuple_data, consumed) = decode_bytes(&bytes[offset..])?;
    offset += consumed;
    let (old_tuple_meta, consumed) = decode_optional_meta(&bytes[offset..])?;
    offset += consumed;
    let (old_tuple_data, _consumed) = decode_optional_bytes(&bytes[offset..])?;
    Ok(HeapUpdatePayload {
        relation,
        page_id,
        slot_id,
        op_txn_id,
        new_tuple_meta,
        new_tuple_data,
        old_tuple_meta,
        old_tuple_data,
    })
}

fn encode_heap_delete(body: &HeapDeletePayload) -> Vec<u8> {
    // Heap/Delete (rmid=Heap, info=3)
    // body: relation + page_id + slot_id + op_txn_id + old_meta(17B) + has_old_data+len+data
    let mut buf = Vec::new();
    encode_relation_ident(&body.relation, &mut buf);
    buf.extend_from_slice(&body.page_id.to_le_bytes());
    buf.extend_from_slice(&body.slot_id.to_le_bytes());
    buf.extend_from_slice(&body.op_txn_id.to_le_bytes());
    encode_tuple_meta(&body.old_tuple_meta, &mut buf);
    encode_optional_bytes(&body.old_tuple_data, &mut buf);
    buf
}

fn decode_heap_delete(bytes: &[u8]) -> QuillSQLResult<HeapDeletePayload> {
    let (relation, mut offset) = decode_relation_ident(bytes)?;
    if bytes.len() < offset + 4 + 2 {
        return Err(QuillSQLError::Internal(
            "Heap delete payload too short".to_string(),
        ));
    }
    let page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as PageId;
    offset += 4;
    let slot_id = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
    offset += 2;
    if bytes.len() < offset + 8 {
        return Err(QuillSQLError::Internal(
            "Heap delete payload missing op_txn_id".to_string(),
        ));
    }
    let op_txn_id =
        u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as TransactionId;
    offset += 8;
    let (old_tuple_meta, consumed) = decode_tuple_meta(&bytes[offset..])?;
    offset += consumed;
    let (old_tuple_data, _consumed) = decode_optional_bytes(&bytes[offset..])?;
    Ok(HeapDeletePayload {
        relation,
        page_id,
        slot_id,
        op_txn_id,
        old_tuple_meta,
        old_tuple_data,
    })
}
