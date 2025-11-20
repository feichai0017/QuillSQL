use crate::buffer::PageId;
use crate::catalog::{Column, DataType, Schema, SchemaRef};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::codec::{CommonCodec, RidCodec};
use crate::storage::page::RecordId;
use crate::transaction::TransactionId;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRelationIdent {
    pub header_page_id: PageId,
    pub schema: SchemaRepr,
}

impl IndexRelationIdent {
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.to_schema_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaRepr {
    pub columns: Vec<ColumnRepr>,
}

impl From<SchemaRef> for SchemaRepr {
    fn from(schema: SchemaRef) -> Self {
        let columns = schema
            .columns
            .iter()
            .map(|col| ColumnRepr {
                name: col.name.clone(),
                data_type: col.data_type,
                nullable: col.nullable,
            })
            .collect();
        Self { columns }
    }
}

impl SchemaRepr {
    pub fn to_schema_ref(&self) -> SchemaRef {
        let cols = self
            .columns
            .iter()
            .map(|c| Column::new(&c.name, c.data_type, c.nullable))
            .collect::<Vec<_>>();
        Arc::new(Schema::new(cols))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRepr {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct IndexLeafInsertPayload {
    pub relation: IndexRelationIdent,
    pub page_id: PageId,
    pub op_txn_id: TransactionId,
    pub key_data: Vec<u8>,
    pub rid: RecordId,
}

#[derive(Debug, Clone)]
pub struct IndexLeafDeletePayload {
    pub relation: IndexRelationIdent,
    pub page_id: PageId,
    pub op_txn_id: TransactionId,
    pub key_data: Vec<u8>,
    pub old_rid: RecordId,
}

#[derive(Debug, Clone)]
pub struct IndexLeafSplitEntryPayload {
    pub key_data: Vec<u8>,
    pub rid: RecordId,
}

#[derive(Debug, Clone)]
pub struct IndexLeafSplitPayload {
    pub relation: IndexRelationIdent,
    pub left_page_id: PageId,
    pub right_page_id: PageId,
    pub leaf_max_size: u32,
    pub split_index: u16,
    pub right_next_page_id: PageId,
    pub entries: Vec<IndexLeafSplitEntryPayload>,
}

#[derive(Debug, Clone)]
pub struct IndexLeafMergePayload {
    pub relation: IndexRelationIdent,
    pub left_page_id: PageId,
    pub right_page_id: PageId,
    pub leaf_max_size: u32,
    pub left_next_page_id: PageId,
    pub entries: Vec<IndexLeafSplitEntryPayload>,
}

#[derive(Debug, Clone)]
pub struct IndexInternalEntryPayload {
    pub key_data: Vec<u8>,
    pub child_page_id: PageId,
}

#[derive(Debug, Clone)]
pub struct IndexInternalSplitPayload {
    pub relation: IndexRelationIdent,
    pub left_page_id: PageId,
    pub left_new_size: u16,
    pub left_high_key: Option<Vec<u8>>,
    pub left_next_page_id: PageId,
    pub right_page_id: PageId,
    pub internal_max_size: u32,
    pub right_entries: Vec<IndexInternalEntryPayload>,
    pub right_high_key: Option<Vec<u8>>,
    pub right_next_page_id: PageId,
}

#[derive(Debug, Clone)]
pub struct IndexInternalMergePayload {
    pub relation: IndexRelationIdent,
    pub left_page_id: PageId,
    pub right_page_id: PageId,
    pub internal_max_size: u32,
    pub left_entries: Vec<IndexInternalEntryPayload>,
    pub high_key: Option<Vec<u8>>,
    pub next_page_id: PageId,
}

#[derive(Debug, Clone)]
pub struct IndexParentInsertPayload {
    pub relation: IndexRelationIdent,
    pub parent_page_id: PageId,
    pub left_child_page_id: PageId,
    pub right_child_page_id: PageId,
    pub key_data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct IndexParentDeletePayload {
    pub relation: IndexRelationIdent,
    pub parent_page_id: PageId,
    pub child_page_id: PageId,
}

#[derive(Debug, Clone)]
pub struct IndexParentUpdatePayload {
    pub relation: IndexRelationIdent,
    pub parent_page_id: PageId,
    pub child_page_id: PageId,
    pub key_data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct IndexLeafRedistributePayload {
    pub relation: IndexRelationIdent,
    pub from_page_id: PageId,
    pub to_page_id: PageId,
    pub from_is_left: bool,
    pub moved_entry: IndexLeafSplitEntryPayload,
}

#[derive(Debug, Clone)]
pub struct IndexInternalRedistributePayload {
    pub relation: IndexRelationIdent,
    pub from_page_id: PageId,
    pub to_page_id: PageId,
    pub parent_page_id: PageId,
    pub from_is_left: bool,
    pub from_old_sentinel: PageId,
    pub to_old_sentinel: PageId,
    pub moved_entry: IndexInternalEntryPayload,
    pub separator_key_data: Vec<u8>,
    pub to_new_high_key: Option<Vec<u8>>,
    pub to_new_next_page_id: PageId,
    pub from_new_high_key: Option<Vec<u8>>,
    pub from_new_next_page_id: PageId,
}

#[derive(Debug, Clone)]
pub struct IndexRootInstallLeafPayload {
    pub relation: IndexRelationIdent,
    pub page_id: PageId,
    pub leaf_max_size: u32,
    pub next_page_id: PageId,
    pub entries: Vec<IndexLeafSplitEntryPayload>,
}

#[derive(Debug, Clone)]
pub struct IndexRootInstallInternalPayload {
    pub relation: IndexRelationIdent,
    pub page_id: PageId,
    pub internal_max_size: u32,
    pub entries: Vec<IndexInternalEntryPayload>,
    pub high_key: Option<Vec<u8>>,
    pub next_page_id: PageId,
}

#[derive(Debug, Clone)]
pub struct IndexRootAdoptPayload {
    pub relation: IndexRelationIdent,
    pub new_root_page_id: PageId,
}

#[derive(Debug, Clone)]
pub struct IndexRootResetPayload {
    pub relation: IndexRelationIdent,
}

#[derive(Debug, Clone)]
pub enum IndexRecordPayload {
    LeafInsert(IndexLeafInsertPayload),
    LeafDelete(IndexLeafDeletePayload),
    LeafSplit(IndexLeafSplitPayload),
    InternalSplit(IndexInternalSplitPayload),
    ParentInsert(IndexParentInsertPayload),
    LeafMerge(IndexLeafMergePayload),
    InternalMerge(IndexInternalMergePayload),
    ParentDelete(IndexParentDeletePayload),
    ParentUpdate(IndexParentUpdatePayload),
    LeafRedistribute(IndexLeafRedistributePayload),
    InternalRedistribute(IndexInternalRedistributePayload),
    RootInstallLeaf(IndexRootInstallLeafPayload),
    RootInstallInternal(IndexRootInstallInternalPayload),
    RootAdopt(IndexRootAdoptPayload),
    RootReset(IndexRootResetPayload),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexRecordKind {
    LeafInsert = 1,
    LeafDelete = 2,
    LeafSplit = 3,
    InternalSplit = 4,
    ParentInsert = 5,
    LeafMerge = 6,
    InternalMerge = 7,
    ParentDelete = 8,
    ParentUpdate = 9,
    LeafRedistribute = 10,
    InternalRedistribute = 11,
    RootInstallLeaf = 12,
    RootInstallInternal = 13,
    RootAdopt = 14,
    RootReset = 15,
}

impl TryFrom<u8> for IndexRecordKind {
    type Error = QuillSQLError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(IndexRecordKind::LeafInsert),
            2 => Ok(IndexRecordKind::LeafDelete),
            3 => Ok(IndexRecordKind::LeafSplit),
            4 => Ok(IndexRecordKind::InternalSplit),
            5 => Ok(IndexRecordKind::ParentInsert),
            6 => Ok(IndexRecordKind::LeafMerge),
            7 => Ok(IndexRecordKind::InternalMerge),
            8 => Ok(IndexRecordKind::ParentDelete),
            9 => Ok(IndexRecordKind::ParentUpdate),
            10 => Ok(IndexRecordKind::LeafRedistribute),
            11 => Ok(IndexRecordKind::InternalRedistribute),
            12 => Ok(IndexRecordKind::RootInstallLeaf),
            13 => Ok(IndexRecordKind::RootInstallInternal),
            14 => Ok(IndexRecordKind::RootAdopt),
            15 => Ok(IndexRecordKind::RootReset),
            other => Err(QuillSQLError::Internal(format!(
                "Unknown index record kind: {}",
                other
            ))),
        }
    }
}

pub fn encode_index_record(payload: &IndexRecordPayload) -> (u8, Vec<u8>) {
    match payload {
        IndexRecordPayload::LeafInsert(body) => {
            (IndexRecordKind::LeafInsert as u8, encode_leaf_insert(body))
        }
        IndexRecordPayload::LeafDelete(body) => {
            (IndexRecordKind::LeafDelete as u8, encode_leaf_delete(body))
        }
        IndexRecordPayload::LeafSplit(body) => {
            (IndexRecordKind::LeafSplit as u8, encode_leaf_split(body))
        }
        IndexRecordPayload::InternalSplit(body) => (
            IndexRecordKind::InternalSplit as u8,
            encode_internal_split(body),
        ),
        IndexRecordPayload::ParentInsert(body) => (
            IndexRecordKind::ParentInsert as u8,
            encode_parent_insert(body),
        ),
        IndexRecordPayload::LeafMerge(body) => {
            (IndexRecordKind::LeafMerge as u8, encode_leaf_merge(body))
        }
        IndexRecordPayload::InternalMerge(body) => (
            IndexRecordKind::InternalMerge as u8,
            encode_internal_merge(body),
        ),
        IndexRecordPayload::ParentDelete(body) => (
            IndexRecordKind::ParentDelete as u8,
            encode_parent_delete(body),
        ),
        IndexRecordPayload::ParentUpdate(body) => (
            IndexRecordKind::ParentUpdate as u8,
            encode_parent_update(body),
        ),
        IndexRecordPayload::LeafRedistribute(body) => (
            IndexRecordKind::LeafRedistribute as u8,
            encode_leaf_redistribute(body),
        ),
        IndexRecordPayload::InternalRedistribute(body) => (
            IndexRecordKind::InternalRedistribute as u8,
            encode_internal_redistribute(body),
        ),
        IndexRecordPayload::RootInstallLeaf(body) => (
            IndexRecordKind::RootInstallLeaf as u8,
            encode_root_install_leaf(body),
        ),
        IndexRecordPayload::RootInstallInternal(body) => (
            IndexRecordKind::RootInstallInternal as u8,
            encode_root_install_internal(body),
        ),
        IndexRecordPayload::RootAdopt(body) => {
            (IndexRecordKind::RootAdopt as u8, encode_root_adopt(body))
        }
        IndexRecordPayload::RootReset(body) => {
            (IndexRecordKind::RootReset as u8, encode_root_reset(body))
        }
    }
}

pub fn decode_index_record(bytes: &[u8], info: u8) -> QuillSQLResult<IndexRecordPayload> {
    match IndexRecordKind::try_from(info)? {
        IndexRecordKind::LeafInsert => {
            Ok(IndexRecordPayload::LeafInsert(decode_leaf_insert(bytes)?))
        }
        IndexRecordKind::LeafDelete => {
            Ok(IndexRecordPayload::LeafDelete(decode_leaf_delete(bytes)?))
        }
        IndexRecordKind::LeafSplit => Ok(IndexRecordPayload::LeafSplit(decode_leaf_split(bytes)?)),
        IndexRecordKind::InternalSplit => Ok(IndexRecordPayload::InternalSplit(
            decode_internal_split(bytes)?,
        )),
        IndexRecordKind::ParentInsert => Ok(IndexRecordPayload::ParentInsert(
            decode_parent_insert(bytes)?,
        )),
        IndexRecordKind::LeafMerge => Ok(IndexRecordPayload::LeafMerge(decode_leaf_merge(bytes)?)),
        IndexRecordKind::InternalMerge => Ok(IndexRecordPayload::InternalMerge(
            decode_internal_merge(bytes)?,
        )),
        IndexRecordKind::ParentDelete => Ok(IndexRecordPayload::ParentDelete(
            decode_parent_delete(bytes)?,
        )),
        IndexRecordKind::ParentUpdate => Ok(IndexRecordPayload::ParentUpdate(
            decode_parent_update(bytes)?,
        )),
        IndexRecordKind::LeafRedistribute => Ok(IndexRecordPayload::LeafRedistribute(
            decode_leaf_redistribute(bytes)?,
        )),
        IndexRecordKind::InternalRedistribute => Ok(IndexRecordPayload::InternalRedistribute(
            decode_internal_redistribute(bytes)?,
        )),
        IndexRecordKind::RootInstallLeaf => Ok(IndexRecordPayload::RootInstallLeaf(
            decode_root_install_leaf(bytes)?,
        )),
        IndexRecordKind::RootInstallInternal => Ok(IndexRecordPayload::RootInstallInternal(
            decode_root_install_internal(bytes)?,
        )),
        IndexRecordKind::RootAdopt => Ok(IndexRecordPayload::RootAdopt(decode_root_adopt(bytes)?)),
        IndexRecordKind::RootReset => Ok(IndexRecordPayload::RootReset(decode_root_reset(bytes)?)),
    }
}

fn encode_leaf_insert(body: &IndexLeafInsertPayload) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_relation(&body.relation, &mut buf);
    buf.extend_from_slice(&body.page_id.to_le_bytes());
    buf.extend_from_slice(&body.op_txn_id.to_le_bytes());
    encode_bytes(&body.key_data, &mut buf);
    buf.extend(RidCodec::encode(&body.rid));
    buf
}

fn decode_leaf_insert(bytes: &[u8]) -> QuillSQLResult<IndexLeafInsertPayload> {
    let (relation, mut offset) = decode_relation(bytes)?;
    if bytes.len() < offset + 4 + 8 {
        return Err(QuillSQLError::Internal(
            "Index leaf insert payload too short".to_string(),
        ));
    }
    let page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let op_txn_id =
        u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as TransactionId;
    offset += 8;
    let (key_data, consumed) = decode_bytes(&bytes[offset..])?;
    offset += consumed;
    let (rid, _) = RidCodec::decode(&bytes[offset..])?;
    Ok(IndexLeafInsertPayload {
        relation,
        page_id,
        op_txn_id,
        key_data,
        rid,
    })
}

fn encode_leaf_delete(body: &IndexLeafDeletePayload) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_relation(&body.relation, &mut buf);
    buf.extend_from_slice(&body.page_id.to_le_bytes());
    buf.extend_from_slice(&body.op_txn_id.to_le_bytes());
    encode_bytes(&body.key_data, &mut buf);
    buf.extend(RidCodec::encode(&body.old_rid));
    buf
}

fn decode_leaf_delete(bytes: &[u8]) -> QuillSQLResult<IndexLeafDeletePayload> {
    let (relation, mut offset) = decode_relation(bytes)?;
    if bytes.len() < offset + 4 + 8 {
        return Err(QuillSQLError::Internal(
            "Index leaf delete payload too short".to_string(),
        ));
    }
    let page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let op_txn_id =
        u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as TransactionId;
    offset += 8;
    let (key_data, consumed) = decode_bytes(&bytes[offset..])?;
    offset += consumed;
    let (old_rid, _) = RidCodec::decode(&bytes[offset..])?;
    Ok(IndexLeafDeletePayload {
        relation,
        page_id,
        op_txn_id,
        key_data,
        old_rid,
    })
}

fn encode_leaf_split_entry(entry: &IndexLeafSplitEntryPayload, buf: &mut Vec<u8>) {
    encode_bytes(&entry.key_data, buf);
    buf.extend(RidCodec::encode(&entry.rid));
}

fn decode_leaf_split_entry(bytes: &[u8]) -> QuillSQLResult<(IndexLeafSplitEntryPayload, usize)> {
    let (key_data, consumed_key) = decode_bytes(bytes)?;
    let (rid, consumed_rid) = RidCodec::decode(&bytes[consumed_key..])?;
    Ok((
        IndexLeafSplitEntryPayload { key_data, rid },
        consumed_key + consumed_rid,
    ))
}

fn encode_leaf_split(body: &IndexLeafSplitPayload) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_relation(&body.relation, &mut buf);
    buf.extend_from_slice(&body.left_page_id.to_le_bytes());
    buf.extend_from_slice(&body.right_page_id.to_le_bytes());
    buf.extend_from_slice(&body.leaf_max_size.to_le_bytes());
    buf.extend_from_slice(&body.split_index.to_le_bytes());
    buf.extend_from_slice(&body.right_next_page_id.to_le_bytes());
    buf.extend_from_slice(&(body.entries.len() as u32).to_le_bytes());
    for entry in &body.entries {
        encode_leaf_split_entry(entry, &mut buf);
    }
    buf
}

fn decode_leaf_split(bytes: &[u8]) -> QuillSQLResult<IndexLeafSplitPayload> {
    let (relation, mut offset) = decode_relation(bytes)?;
    let required = offset + 4 + 4 + 4 + 2 + 4;
    if bytes.len() < required {
        return Err(QuillSQLError::Internal(
            "Index leaf split payload too short".to_string(),
        ));
    }
    let left_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let right_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let leaf_max_size = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let split_index = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
    offset += 2;
    let right_next_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    if bytes.len() < offset + 4 {
        return Err(QuillSQLError::Internal(
            "Index leaf split entries length missing".to_string(),
        ));
    }
    let entry_count = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let mut entries = Vec::with_capacity(entry_count);
    let mut cursor = offset;
    for _ in 0..entry_count {
        if cursor >= bytes.len() {
            return Err(QuillSQLError::Internal(
                "Index leaf split entries truncated".to_string(),
            ));
        }
        let (entry, consumed) = decode_leaf_split_entry(&bytes[cursor..])?;
        entries.push(entry);
        cursor += consumed;
    }
    Ok(IndexLeafSplitPayload {
        relation,
        left_page_id,
        right_page_id,
        leaf_max_size,
        split_index,
        right_next_page_id,
        entries,
    })
}

fn encode_leaf_merge(body: &IndexLeafMergePayload) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_relation(&body.relation, &mut buf);
    buf.extend_from_slice(&body.left_page_id.to_le_bytes());
    buf.extend_from_slice(&body.right_page_id.to_le_bytes());
    buf.extend_from_slice(&body.leaf_max_size.to_le_bytes());
    buf.extend_from_slice(&body.left_next_page_id.to_le_bytes());
    buf.extend_from_slice(&(body.entries.len() as u32).to_le_bytes());
    for entry in &body.entries {
        encode_leaf_split_entry(entry, &mut buf);
    }
    buf
}

fn decode_leaf_merge(bytes: &[u8]) -> QuillSQLResult<IndexLeafMergePayload> {
    let (relation, mut offset) = decode_relation(bytes)?;
    let required = offset + 4 + 4 + 4 + 4;
    if bytes.len() < required {
        return Err(QuillSQLError::Internal(
            "Index leaf merge payload too short".to_string(),
        ));
    }
    let left_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let right_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let leaf_max_size = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let left_next_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    if bytes.len() < offset + 4 {
        return Err(QuillSQLError::Internal(
            "Index leaf merge entries length missing".to_string(),
        ));
    }
    let entry_count = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let mut entries = Vec::with_capacity(entry_count);
    let mut cursor = offset;
    for _ in 0..entry_count {
        if cursor >= bytes.len() {
            return Err(QuillSQLError::Internal(
                "Index leaf merge entries truncated".to_string(),
            ));
        }
        let (entry, consumed) = decode_leaf_split_entry(&bytes[cursor..])?;
        entries.push(entry);
        cursor += consumed;
    }
    Ok(IndexLeafMergePayload {
        relation,
        left_page_id,
        right_page_id,
        leaf_max_size,
        left_next_page_id,
        entries,
    })
}

fn encode_internal_entry(entry: &IndexInternalEntryPayload, buf: &mut Vec<u8>) {
    encode_bytes(&entry.key_data, buf);
    buf.extend_from_slice(&entry.child_page_id.to_le_bytes());
}

fn decode_internal_entry(bytes: &[u8]) -> QuillSQLResult<(IndexInternalEntryPayload, usize)> {
    let (key_data, consumed_key) = decode_bytes(bytes)?;
    if bytes.len() < consumed_key + 4 {
        return Err(QuillSQLError::Internal(
            "Index internal entry truncated".to_string(),
        ));
    }
    let child_page_id =
        u32::from_le_bytes(bytes[consumed_key..consumed_key + 4].try_into().unwrap());
    Ok((
        IndexInternalEntryPayload {
            key_data,
            child_page_id,
        },
        consumed_key + 4,
    ))
}

fn encode_internal_split(body: &IndexInternalSplitPayload) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_relation(&body.relation, &mut buf);
    buf.extend_from_slice(&body.left_page_id.to_le_bytes());
    buf.extend_from_slice(&body.left_new_size.to_le_bytes());
    match &body.left_high_key {
        Some(bytes) => {
            buf.push(1);
            encode_bytes(bytes, &mut buf);
        }
        None => buf.push(0),
    }
    buf.extend_from_slice(&body.left_next_page_id.to_le_bytes());
    buf.extend_from_slice(&body.right_page_id.to_le_bytes());
    buf.extend_from_slice(&body.internal_max_size.to_le_bytes());
    buf.extend_from_slice(&(body.right_entries.len() as u32).to_le_bytes());
    for entry in &body.right_entries {
        encode_internal_entry(entry, &mut buf);
    }
    match &body.right_high_key {
        Some(bytes) => {
            buf.push(1);
            encode_bytes(bytes, &mut buf);
        }
        None => buf.push(0),
    }
    buf.extend_from_slice(&body.right_next_page_id.to_le_bytes());
    buf
}

fn decode_internal_split(bytes: &[u8]) -> QuillSQLResult<IndexInternalSplitPayload> {
    let (relation, mut offset) = decode_relation(bytes)?;
    if bytes.len() < offset + 4 + 2 + 1 + 4 + 4 + 4 {
        return Err(QuillSQLError::Internal(
            "Index internal split payload too short".to_string(),
        ));
    }
    let left_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let left_new_size = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
    offset += 2;
    let has_left_high = bytes[offset] != 0;
    offset += 1;
    let left_high_key = if has_left_high {
        let (data, consumed) = decode_bytes(&bytes[offset..])?;
        offset += consumed;
        Some(data)
    } else {
        None
    };
    let left_next_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let right_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let internal_max_size = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    if bytes.len() < offset + 4 {
        return Err(QuillSQLError::Internal(
            "Index internal split entries length missing".to_string(),
        ));
    }
    let entry_count = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let mut entries = Vec::with_capacity(entry_count);
    let mut cursor = offset;
    for _ in 0..entry_count {
        if cursor >= bytes.len() {
            return Err(QuillSQLError::Internal(
                "Index internal split entries truncated".to_string(),
            ));
        }
        let (entry, consumed) = decode_internal_entry(&bytes[cursor..])?;
        entries.push(entry);
        cursor += consumed;
    }
    if bytes.len() <= cursor {
        return Err(QuillSQLError::Internal(
            "Index internal split missing right high key flag".to_string(),
        ));
    }
    let has_right_high = bytes[cursor] != 0;
    cursor += 1;
    let right_high_key = if has_right_high {
        let (data, consumed) = decode_bytes(&bytes[cursor..])?;
        cursor += consumed;
        Some(data)
    } else {
        None
    };
    if bytes.len() < cursor + 4 {
        return Err(QuillSQLError::Internal(
            "Index internal split missing right_next_page_id".to_string(),
        ));
    }
    let right_next_page_id = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap());
    Ok(IndexInternalSplitPayload {
        relation,
        left_page_id,
        left_new_size,
        left_high_key,
        left_next_page_id,
        right_page_id,
        internal_max_size,
        right_entries: entries,
        right_high_key,
        right_next_page_id,
    })
}

fn encode_internal_merge(body: &IndexInternalMergePayload) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_relation(&body.relation, &mut buf);
    buf.extend_from_slice(&body.left_page_id.to_le_bytes());
    buf.extend_from_slice(&body.right_page_id.to_le_bytes());
    buf.extend_from_slice(&body.internal_max_size.to_le_bytes());
    buf.extend_from_slice(&(body.left_entries.len() as u32).to_le_bytes());
    for entry in &body.left_entries {
        encode_internal_entry(entry, &mut buf);
    }
    match &body.high_key {
        Some(bytes) => {
            buf.push(1);
            encode_bytes(bytes, &mut buf);
        }
        None => buf.push(0),
    }
    buf.extend_from_slice(&body.next_page_id.to_le_bytes());
    buf
}

fn decode_internal_merge(bytes: &[u8]) -> QuillSQLResult<IndexInternalMergePayload> {
    let (relation, mut offset) = decode_relation(bytes)?;
    if bytes.len() < offset + 4 + 4 + 4 {
        return Err(QuillSQLError::Internal(
            "Index internal merge payload too short".to_string(),
        ));
    }
    let left_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let right_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let internal_max_size = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    if bytes.len() < offset + 4 {
        return Err(QuillSQLError::Internal(
            "Index internal merge entry length missing".to_string(),
        ));
    }
    let entry_count = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let mut left_entries = Vec::with_capacity(entry_count);
    let mut cursor = offset;
    for _ in 0..entry_count {
        if cursor >= bytes.len() {
            return Err(QuillSQLError::Internal(
                "Index internal merge entries truncated".to_string(),
            ));
        }
        let (entry, consumed) = decode_internal_entry(&bytes[cursor..])?;
        left_entries.push(entry);
        cursor += consumed;
    }
    if bytes.len() <= cursor {
        return Err(QuillSQLError::Internal(
            "Index internal merge missing high key flag".to_string(),
        ));
    }
    let has_high_key = bytes[cursor] != 0;
    cursor += 1;
    let high_key = if has_high_key {
        let (data, consumed) = decode_bytes(&bytes[cursor..])?;
        cursor += consumed;
        Some(data)
    } else {
        None
    };
    if bytes.len() < cursor + 4 {
        return Err(QuillSQLError::Internal(
            "Index internal merge missing next_page_id".to_string(),
        ));
    }
    let next_page_id = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap());
    Ok(IndexInternalMergePayload {
        relation,
        left_page_id,
        right_page_id,
        internal_max_size,
        left_entries,
        high_key,
        next_page_id,
    })
}

fn encode_parent_insert(body: &IndexParentInsertPayload) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_relation(&body.relation, &mut buf);
    buf.extend_from_slice(&body.parent_page_id.to_le_bytes());
    buf.extend_from_slice(&body.left_child_page_id.to_le_bytes());
    buf.extend_from_slice(&body.right_child_page_id.to_le_bytes());
    encode_bytes(&body.key_data, &mut buf);
    buf
}

fn decode_parent_insert(bytes: &[u8]) -> QuillSQLResult<IndexParentInsertPayload> {
    let (relation, mut offset) = decode_relation(bytes)?;
    if bytes.len() < offset + 4 * 3 {
        return Err(QuillSQLError::Internal(
            "Index parent insert payload too short".to_string(),
        ));
    }
    let parent_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let left_child_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let right_child_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let (key_data, _) = decode_bytes(&bytes[offset..])?;
    Ok(IndexParentInsertPayload {
        relation,
        parent_page_id,
        left_child_page_id,
        right_child_page_id,
        key_data,
    })
}

fn encode_parent_delete(body: &IndexParentDeletePayload) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_relation(&body.relation, &mut buf);
    buf.extend_from_slice(&body.parent_page_id.to_le_bytes());
    buf.extend_from_slice(&body.child_page_id.to_le_bytes());
    buf
}

fn decode_parent_delete(bytes: &[u8]) -> QuillSQLResult<IndexParentDeletePayload> {
    let (relation, mut offset) = decode_relation(bytes)?;
    if bytes.len() < offset + 8 {
        return Err(QuillSQLError::Internal(
            "Index parent delete payload too short".to_string(),
        ));
    }
    let parent_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let child_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    Ok(IndexParentDeletePayload {
        relation,
        parent_page_id,
        child_page_id,
    })
}

fn encode_parent_update(body: &IndexParentUpdatePayload) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_relation(&body.relation, &mut buf);
    buf.extend_from_slice(&body.parent_page_id.to_le_bytes());
    buf.extend_from_slice(&body.child_page_id.to_le_bytes());
    encode_bytes(&body.key_data, &mut buf);
    buf
}

fn decode_parent_update(bytes: &[u8]) -> QuillSQLResult<IndexParentUpdatePayload> {
    let (relation, mut offset) = decode_relation(bytes)?;
    if bytes.len() < offset + 8 {
        return Err(QuillSQLError::Internal(
            "Index parent update payload too short".to_string(),
        ));
    }
    let parent_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let child_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let (key_data, _) = decode_bytes(&bytes[offset..])?;
    Ok(IndexParentUpdatePayload {
        relation,
        parent_page_id,
        child_page_id,
        key_data,
    })
}

fn encode_leaf_redistribute(body: &IndexLeafRedistributePayload) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_relation(&body.relation, &mut buf);
    buf.extend_from_slice(&body.from_page_id.to_le_bytes());
    buf.extend_from_slice(&body.to_page_id.to_le_bytes());
    buf.push(if body.from_is_left { 1 } else { 0 });
    encode_leaf_split_entry(&body.moved_entry, &mut buf);
    buf
}

fn decode_leaf_redistribute(bytes: &[u8]) -> QuillSQLResult<IndexLeafRedistributePayload> {
    let (relation, mut offset) = decode_relation(bytes)?;
    if bytes.len() < offset + 4 + 4 + 1 {
        return Err(QuillSQLError::Internal(
            "Index leaf redistribute payload too short".to_string(),
        ));
    }
    let from_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let to_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let from_is_left = bytes[offset] != 0;
    offset += 1;
    let (moved_entry, _) = decode_leaf_split_entry(&bytes[offset..])?;
    Ok(IndexLeafRedistributePayload {
        relation,
        from_page_id,
        to_page_id,
        from_is_left,
        moved_entry,
    })
}

fn encode_internal_redistribute(body: &IndexInternalRedistributePayload) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_relation(&body.relation, &mut buf);
    buf.extend_from_slice(&body.from_page_id.to_le_bytes());
    buf.extend_from_slice(&body.to_page_id.to_le_bytes());
    buf.extend_from_slice(&body.parent_page_id.to_le_bytes());
    buf.push(if body.from_is_left { 1 } else { 0 });
    buf.extend_from_slice(&body.from_old_sentinel.to_le_bytes());
    buf.extend_from_slice(&body.to_old_sentinel.to_le_bytes());
    encode_internal_entry(&body.moved_entry, &mut buf);
    encode_bytes(&body.separator_key_data, &mut buf);
    match &body.to_new_high_key {
        Some(bytes) => {
            buf.push(1);
            encode_bytes(bytes, &mut buf);
        }
        None => buf.push(0),
    }
    buf.extend_from_slice(&body.to_new_next_page_id.to_le_bytes());
    match &body.from_new_high_key {
        Some(bytes) => {
            buf.push(1);
            encode_bytes(bytes, &mut buf);
        }
        None => buf.push(0),
    }
    buf.extend_from_slice(&body.from_new_next_page_id.to_le_bytes());
    buf
}

fn decode_internal_redistribute(bytes: &[u8]) -> QuillSQLResult<IndexInternalRedistributePayload> {
    let (relation, mut offset) = decode_relation(bytes)?;
    if bytes.len() < offset + 4 + 4 + 4 + 1 + 4 + 4 {
        return Err(QuillSQLError::Internal(
            "Index internal redistribute payload too short".to_string(),
        ));
    }
    let from_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let to_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let parent_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let from_is_left = bytes[offset] != 0;
    offset += 1;
    let from_old_sentinel = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let to_old_sentinel = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let (moved_entry, consumed_entry) = decode_internal_entry(&bytes[offset..])?;
    offset += consumed_entry;
    let (separator_key_data, consumed_sep) = decode_bytes(&bytes[offset..])?;
    offset += consumed_sep;
    if bytes.len() <= offset {
        return Err(QuillSQLError::Internal(
            "Index internal redistribute missing to high key flag".to_string(),
        ));
    }
    let has_to_high = bytes[offset] != 0;
    offset += 1;
    let to_new_high_key = if has_to_high {
        let (data, consumed) = decode_bytes(&bytes[offset..])?;
        offset += consumed;
        Some(data)
    } else {
        None
    };
    if bytes.len() < offset + 4 {
        return Err(QuillSQLError::Internal(
            "Index internal redistribute missing to next page".to_string(),
        ));
    }
    let to_new_next_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    if bytes.len() <= offset {
        return Err(QuillSQLError::Internal(
            "Index internal redistribute missing from high key flag".to_string(),
        ));
    }
    let has_from_high = bytes[offset] != 0;
    offset += 1;
    let from_new_high_key = if has_from_high {
        let (data, consumed) = decode_bytes(&bytes[offset..])?;
        offset += consumed;
        Some(data)
    } else {
        None
    };
    if bytes.len() < offset + 4 {
        return Err(QuillSQLError::Internal(
            "Index internal redistribute missing from next page".to_string(),
        ));
    }
    let from_new_next_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    Ok(IndexInternalRedistributePayload {
        relation,
        from_page_id,
        to_page_id,
        parent_page_id,
        from_is_left,
        from_old_sentinel,
        to_old_sentinel,
        moved_entry,
        separator_key_data,
        to_new_high_key,
        to_new_next_page_id,
        from_new_high_key,
        from_new_next_page_id,
    })
}

fn encode_root_install_leaf(body: &IndexRootInstallLeafPayload) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_relation(&body.relation, &mut buf);
    buf.extend_from_slice(&body.page_id.to_le_bytes());
    buf.extend_from_slice(&body.leaf_max_size.to_le_bytes());
    buf.extend_from_slice(&body.next_page_id.to_le_bytes());
    buf.extend_from_slice(&(body.entries.len() as u32).to_le_bytes());
    for entry in &body.entries {
        encode_leaf_split_entry(entry, &mut buf);
    }
    buf
}

fn decode_root_install_leaf(bytes: &[u8]) -> QuillSQLResult<IndexRootInstallLeafPayload> {
    let (relation, mut offset) = decode_relation(bytes)?;
    if bytes.len() < offset + 4 + 4 + 4 {
        return Err(QuillSQLError::Internal(
            "Index root install leaf payload too short".to_string(),
        ));
    }
    let page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let leaf_max_size = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let next_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    if bytes.len() < offset + 4 {
        return Err(QuillSQLError::Internal(
            "Index root install leaf entries length missing".to_string(),
        ));
    }
    let entry_count = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let mut entries = Vec::with_capacity(entry_count);
    let mut cursor = offset;
    for _ in 0..entry_count {
        if cursor >= bytes.len() {
            return Err(QuillSQLError::Internal(
                "Index root install leaf entries truncated".to_string(),
            ));
        }
        let (entry, consumed) = decode_leaf_split_entry(&bytes[cursor..])?;
        entries.push(entry);
        cursor += consumed;
    }
    Ok(IndexRootInstallLeafPayload {
        relation,
        page_id,
        leaf_max_size,
        next_page_id,
        entries,
    })
}

fn encode_root_install_internal(body: &IndexRootInstallInternalPayload) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_relation(&body.relation, &mut buf);
    buf.extend_from_slice(&body.page_id.to_le_bytes());
    buf.extend_from_slice(&body.internal_max_size.to_le_bytes());
    buf.extend_from_slice(&(body.entries.len() as u32).to_le_bytes());
    for entry in &body.entries {
        encode_internal_entry(entry, &mut buf);
    }
    match &body.high_key {
        Some(bytes) => {
            buf.push(1);
            encode_bytes(bytes, &mut buf);
        }
        None => buf.push(0),
    }
    buf.extend_from_slice(&body.next_page_id.to_le_bytes());
    buf
}

fn decode_root_install_internal(bytes: &[u8]) -> QuillSQLResult<IndexRootInstallInternalPayload> {
    let (relation, mut offset) = decode_relation(bytes)?;
    if bytes.len() < offset + 4 + 4 {
        return Err(QuillSQLError::Internal(
            "Index root install internal payload too short".to_string(),
        ));
    }
    let page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let internal_max_size = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    if bytes.len() < offset + 4 {
        return Err(QuillSQLError::Internal(
            "Index root install internal entries length missing".to_string(),
        ));
    }
    let entry_count = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let mut entries = Vec::with_capacity(entry_count);
    let mut cursor = offset;
    for _ in 0..entry_count {
        if cursor >= bytes.len() {
            return Err(QuillSQLError::Internal(
                "Index root install internal entries truncated".to_string(),
            ));
        }
        let (entry, consumed) = decode_internal_entry(&bytes[cursor..])?;
        entries.push(entry);
        cursor += consumed;
    }
    if bytes.len() <= cursor {
        return Err(QuillSQLError::Internal(
            "Index root install internal missing high key flag".to_string(),
        ));
    }
    let has_high_key = bytes[cursor] != 0;
    cursor += 1;
    let high_key = if has_high_key {
        let (data, consumed) = decode_bytes(&bytes[cursor..])?;
        cursor += consumed;
        Some(data)
    } else {
        None
    };
    if bytes.len() < cursor + 4 {
        return Err(QuillSQLError::Internal(
            "Index root install internal missing next_page_id".to_string(),
        ));
    }
    let next_page_id = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap());
    Ok(IndexRootInstallInternalPayload {
        relation,
        page_id,
        internal_max_size,
        entries,
        high_key,
        next_page_id,
    })
}

fn encode_root_adopt(body: &IndexRootAdoptPayload) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_relation(&body.relation, &mut buf);
    buf.extend_from_slice(&body.new_root_page_id.to_le_bytes());
    buf
}

fn decode_root_adopt(bytes: &[u8]) -> QuillSQLResult<IndexRootAdoptPayload> {
    let (relation, offset) = decode_relation(bytes)?;
    if bytes.len() < offset + 4 {
        return Err(QuillSQLError::Internal(
            "Index root adopt payload too short".to_string(),
        ));
    }
    let new_root_page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    Ok(IndexRootAdoptPayload {
        relation,
        new_root_page_id,
    })
}

fn encode_root_reset(body: &IndexRootResetPayload) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_relation(&body.relation, &mut buf);
    buf
}

fn decode_root_reset(bytes: &[u8]) -> QuillSQLResult<IndexRootResetPayload> {
    let (relation, _) = decode_relation(bytes)?;
    Ok(IndexRootResetPayload { relation })
}

fn encode_relation(relation: &IndexRelationIdent, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&relation.header_page_id.to_le_bytes());
    encode_schema(&relation.schema, buf);
}

fn decode_relation(bytes: &[u8]) -> QuillSQLResult<(IndexRelationIdent, usize)> {
    if bytes.len() < 4 {
        return Err(QuillSQLError::Internal(
            "Index relation ident too short".to_string(),
        ));
    }
    let header_page_id = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let (schema, consumed) = decode_schema(&bytes[4..])?;
    Ok((
        IndexRelationIdent {
            header_page_id,
            schema,
        },
        4 + consumed,
    ))
}

fn encode_schema(schema: &SchemaRepr, buf: &mut Vec<u8>) {
    buf.extend(CommonCodec::encode_u16(schema.columns.len() as u16));
    for column in &schema.columns {
        encode_string(&column.name, buf);
        encode_data_type(column.data_type, buf);
        buf.push(column.nullable as u8);
    }
}

fn encode_data_type(data_type: DataType, buf: &mut Vec<u8>) {
    match data_type {
        DataType::Boolean => buf.push(0),
        DataType::Int8 => buf.push(1),
        DataType::Int16 => buf.push(2),
        DataType::Int32 => buf.push(3),
        DataType::Int64 => buf.push(4),
        DataType::UInt8 => buf.push(5),
        DataType::UInt16 => buf.push(6),
        DataType::UInt32 => buf.push(7),
        DataType::UInt64 => buf.push(8),
        DataType::Float32 => buf.push(9),
        DataType::Float64 => buf.push(10),
        DataType::Varchar(len) => {
            buf.push(11);
            match len {
                Some(n) => {
                    buf.push(1);
                    buf.extend(CommonCodec::encode_u32(n as u32));
                }
                None => buf.push(0),
            }
        }
    }
}

fn decode_schema(bytes: &[u8]) -> QuillSQLResult<(SchemaRepr, usize)> {
    let (col_count, mut offset) = CommonCodec::decode_u16(bytes)?;
    let mut columns = Vec::with_capacity(col_count as usize);
    for _ in 0..col_count {
        let (name, consumed_name) = decode_string(&bytes[offset..])?;
        offset += consumed_name;
        let (data_type, consumed_dt) = decode_data_type(&bytes[offset..])?;
        offset += consumed_dt;
        if offset >= bytes.len() {
            return Err(QuillSQLError::Internal(
                "Schema payload truncated (nullable)".to_string(),
            ));
        }
        let nullable = bytes[offset] != 0;
        offset += 1;
        columns.push(ColumnRepr {
            name,
            data_type,
            nullable,
        });
    }
    Ok((SchemaRepr { columns }, offset))
}

fn decode_data_type(bytes: &[u8]) -> QuillSQLResult<(DataType, usize)> {
    if bytes.is_empty() {
        return Err(QuillSQLError::Internal(
            "Schema payload truncated (datatype tag)".to_string(),
        ));
    }
    let tag = bytes[0];
    let mut consumed = 1;
    let data_type = match tag {
        0 => DataType::Boolean,
        1 => DataType::Int8,
        2 => DataType::Int16,
        3 => DataType::Int32,
        4 => DataType::Int64,
        5 => DataType::UInt8,
        6 => DataType::UInt16,
        7 => DataType::UInt32,
        8 => DataType::UInt64,
        9 => DataType::Float32,
        10 => DataType::Float64,
        11 => {
            if bytes.len() < consumed + 1 {
                return Err(QuillSQLError::Internal(
                    "Schema payload truncated (varchar flag)".to_string(),
                ));
            }
            let has_len = bytes[consumed] != 0;
            consumed += 1;
            if has_len {
                let (len, len_consumed) = CommonCodec::decode_u32(&bytes[consumed..])?;
                consumed += len_consumed;
                DataType::Varchar(Some(len as usize))
            } else {
                DataType::Varchar(None)
            }
        }
        other => {
            return Err(QuillSQLError::Internal(format!(
                "Unknown datatype tag {} in schema repr",
                other
            )))
        }
    };
    Ok((data_type, consumed))
}

fn encode_bytes(data: &[u8], buf: &mut Vec<u8>) {
    buf.extend(CommonCodec::encode_u32(data.len() as u32));
    buf.extend_from_slice(data);
}

fn decode_bytes(bytes: &[u8]) -> QuillSQLResult<(Vec<u8>, usize)> {
    let (len, mut offset) = CommonCodec::decode_u32(bytes)?;
    let len = len as usize;
    if bytes.len() < offset + len {
        return Err(QuillSQLError::Internal(
            "Index WAL payload truncated while decoding bytes".to_string(),
        ));
    }
    let data = bytes[offset..offset + len].to_vec();
    offset += len;
    Ok((data, offset))
}

fn encode_string(value: &str, buf: &mut Vec<u8>) {
    let bytes = value.as_bytes();
    buf.extend(CommonCodec::encode_u16(bytes.len() as u16));
    buf.extend_from_slice(bytes);
}

fn decode_string(bytes: &[u8]) -> QuillSQLResult<(String, usize)> {
    let (len, mut offset) = CommonCodec::decode_u16(bytes)?;
    let len = len as usize;
    if bytes.len() < offset + len {
        return Err(QuillSQLError::Internal(
            "Index WAL payload truncated while decoding string".to_string(),
        ));
    }
    let value = std::str::from_utf8(&bytes[offset..offset + len])
        .map_err(|e| QuillSQLError::Internal(format!("Invalid utf8 in schema repr: {}", e)))?
        .to_string();
    offset += len;
    Ok((value, offset))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Column, DataType, Schema};
    use crate::storage::codec::TupleCodec;
    use crate::storage::tuple::Tuple;
    use crate::utils::scalar::ScalarValue;
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Column::new("k", DataType::Int32, false)]))
    }

    fn roundtrip(payload: IndexRecordPayload, kind: IndexRecordKind) {
        let (info, bytes) = encode_index_record(&payload);
        assert_eq!(info, kind as u8);
        let decoded = decode_index_record(&bytes, info).unwrap();
        match (payload, decoded) {
            (IndexRecordPayload::LeafInsert(a), IndexRecordPayload::LeafInsert(b)) => {
                assert_eq!(a.relation.header_page_id, b.relation.header_page_id);
                assert_eq!(a.page_id, b.page_id);
                assert_eq!(a.op_txn_id, b.op_txn_id);
                assert_eq!(a.key_data, b.key_data);
                assert_eq!(a.rid, b.rid);
            }
            (IndexRecordPayload::LeafDelete(a), IndexRecordPayload::LeafDelete(b)) => {
                assert_eq!(a.relation.header_page_id, b.relation.header_page_id);
                assert_eq!(a.page_id, b.page_id);
                assert_eq!(a.op_txn_id, b.op_txn_id);
                assert_eq!(a.key_data, b.key_data);
                assert_eq!(a.old_rid, b.old_rid);
            }
            (
                IndexRecordPayload::RootInstallInternal(a),
                IndexRecordPayload::RootInstallInternal(b),
            ) => {
                assert_eq!(a.relation.header_page_id, b.relation.header_page_id);
                assert_eq!(a.page_id, b.page_id);
                assert_eq!(a.internal_max_size, b.internal_max_size);
                assert_eq!(a.entries.len(), b.entries.len());
                assert_eq!(a.high_key, b.high_key);
                assert_eq!(a.next_page_id, b.next_page_id);
            }
            (lhs, rhs) => panic!("payload variant mismatch: {:?} vs {:?}", lhs, rhs),
        }
    }

    #[test]
    fn leaf_insert_roundtrip() {
        let schema = schema();
        let payload = IndexRecordPayload::LeafInsert(IndexLeafInsertPayload {
            relation: IndexRelationIdent {
                header_page_id: 99,
                schema: SchemaRepr::from(schema.clone()),
            },
            page_id: 7,
            op_txn_id: 1,
            key_data: TupleCodec::encode(&Tuple::new(schema, vec![ScalarValue::Int32(Some(3))])),
            rid: RecordId::new(4, 2),
        });
        roundtrip(payload, IndexRecordKind::LeafInsert);
    }

    #[test]
    fn leaf_delete_roundtrip() {
        let schema = schema();
        let payload = IndexRecordPayload::LeafDelete(IndexLeafDeletePayload {
            relation: IndexRelationIdent {
                header_page_id: 3,
                schema: SchemaRepr::from(schema.clone()),
            },
            page_id: 2,
            op_txn_id: 4,
            key_data: TupleCodec::encode(&Tuple::new(schema, vec![ScalarValue::Int32(Some(5))])),
            old_rid: RecordId::new(9, 1),
        });
        roundtrip(payload, IndexRecordKind::LeafDelete);
    }

    #[test]
    fn root_install_internal_roundtrip() {
        let schema = schema();
        let payload = IndexRecordPayload::RootInstallInternal(IndexRootInstallInternalPayload {
            relation: IndexRelationIdent {
                header_page_id: 5,
                schema: SchemaRepr::from(schema.clone()),
            },
            page_id: 6,
            internal_max_size: 8,
            entries: vec![IndexInternalEntryPayload {
                key_data: TupleCodec::encode(&Tuple::new(
                    schema.clone(),
                    vec![ScalarValue::Int32(Some(10))],
                )),
                child_page_id: 11,
            }],
            high_key: None,
            next_page_id: 12,
        });
        roundtrip(payload, IndexRecordKind::RootInstallInternal);
    }
}
