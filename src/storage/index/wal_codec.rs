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
pub struct IndexLeafUpdatePayload {
    pub relation: IndexRelationIdent,
    pub page_id: PageId,
    pub op_txn_id: TransactionId,
    pub key_data: Vec<u8>,
    pub old_rid: RecordId,
    pub new_rid: RecordId,
}

#[derive(Debug, Clone)]
pub enum IndexRecordPayload {
    LeafInsert(IndexLeafInsertPayload),
    LeafDelete(IndexLeafDeletePayload),
    LeafUpdate(IndexLeafUpdatePayload),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexRecordKind {
    LeafInsert = 1,
    LeafDelete = 2,
    LeafUpdate = 3,
}

impl TryFrom<u8> for IndexRecordKind {
    type Error = QuillSQLError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(IndexRecordKind::LeafInsert),
            2 => Ok(IndexRecordKind::LeafDelete),
            3 => Ok(IndexRecordKind::LeafUpdate),
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
        IndexRecordPayload::LeafUpdate(body) => {
            (IndexRecordKind::LeafUpdate as u8, encode_leaf_update(body))
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
        IndexRecordKind::LeafUpdate => {
            Ok(IndexRecordPayload::LeafUpdate(decode_leaf_update(bytes)?))
        }
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
    let op_txn_id = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
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
    let op_txn_id = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
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

fn encode_leaf_update(body: &IndexLeafUpdatePayload) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_relation(&body.relation, &mut buf);
    buf.extend_from_slice(&body.page_id.to_le_bytes());
    buf.extend_from_slice(&body.op_txn_id.to_le_bytes());
    encode_bytes(&body.key_data, &mut buf);
    buf.extend(RidCodec::encode(&body.old_rid));
    buf.extend(RidCodec::encode(&body.new_rid));
    buf
}

fn decode_leaf_update(bytes: &[u8]) -> QuillSQLResult<IndexLeafUpdatePayload> {
    let (relation, mut offset) = decode_relation(bytes)?;
    if bytes.len() < offset + 4 + 8 {
        return Err(QuillSQLError::Internal(
            "Index leaf update payload too short".to_string(),
        ));
    }
    let page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let op_txn_id = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
    offset += 8;
    let (key_data, consumed) = decode_bytes(&bytes[offset..])?;
    offset += consumed;
    let (old_rid, consumed_old) = RidCodec::decode(&bytes[offset..])?;
    offset += consumed_old;
    let (new_rid, _) = RidCodec::decode(&bytes[offset..])?;
    Ok(IndexLeafUpdatePayload {
        relation,
        page_id,
        op_txn_id,
        key_data,
        old_rid,
        new_rid,
    })
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
