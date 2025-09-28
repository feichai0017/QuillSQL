use std::convert::TryFrom;

use crc32fast::Hasher;

// WAL Frame (version = 2)
// +------------+------------+----------------+----------------+------+-----+------------+---------+
// |  magic(4)  | ver(2)=2   | lsn(8,LE)      | prev_lsn(8,LE) | rmid |info | body_len(4)|  body   |
// +------------+------------+----------------+----------------+------+-----+------------+---------+
// |            header (WAL_HEADER_LEN = 28 bytes)                         |         |
// +------------------------------------------------------------------------+---------+
// | crc32(header+body)(4)                                                 |
// +------------------------------------------------------------------------+
// - magic: 0x5157_414c ("QWAL"), little-endian
// - rmid: ResourceManagerId (1=Page, 2=Transaction, 3=Heap, 4=Checkpoint, 5=Clr)
// - info: Record-kind inside rmid
// - body: payload encoded by encoder below; CRC covers header+body

use crate::buffer::PageId;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::Lsn;
use crate::storage::page::TupleMeta;
use crate::transaction::TransactionId;

pub const WAL_MAGIC: u32 = 0x5157_414c; // "QWAL"
pub const WAL_VERSION_V1: u16 = 1;
pub const WAL_VERSION: u16 = 2;
pub const WAL_HEADER_LEN: usize = 4 + 2 + 8 + 8 + 1 + 1 + 4;
pub const WAL_CRC_LEN: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ResourceManagerId {
    Page = 1,
    Transaction = 2,
    Heap = 3,
    Checkpoint = 4,
    Clr = 5,
}

impl TryFrom<u8> for ResourceManagerId {
    type Error = QuillSQLError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ResourceManagerId::Page),
            2 => Ok(ResourceManagerId::Transaction),
            3 => Ok(ResourceManagerId::Heap),
            4 => Ok(ResourceManagerId::Checkpoint),
            5 => Ok(ResourceManagerId::Clr),
            other => Err(QuillSQLError::Internal(format!(
                "Unknown WAL resource manager id: {}",
                other
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub enum WalRecordPayload {
    PageWrite(PageWritePayload),
    /// Apply a byte-range delta to a page (physiological logging)
    PageDelta(PageDeltaPayload),
    Transaction(TransactionPayload),
    Heap(HeapRecordPayload),
    Checkpoint(CheckpointPayload),
    /// Compensation log record: documents an UNDO action; redo is a no-op.
    Clr(ClrPayload),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationIdent {
    pub root_page_id: PageId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleMetaRepr {
    pub insert_txn_id: TransactionId,
    pub delete_txn_id: TransactionId,
    pub is_deleted: bool,
}

impl From<TupleMetaRepr> for TupleMeta {
    fn from(value: TupleMetaRepr) -> Self {
        TupleMeta {
            insert_txn_id: value.insert_txn_id,
            delete_txn_id: value.delete_txn_id,
            is_deleted: value.is_deleted,
        }
    }
}

impl From<TupleMeta> for TupleMetaRepr {
    fn from(value: TupleMeta) -> Self {
        TupleMetaRepr {
            insert_txn_id: value.insert_txn_id,
            delete_txn_id: value.delete_txn_id,
            is_deleted: value.is_deleted,
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
pub struct CheckpointPayload {
    pub last_lsn: Lsn,
    pub dirty_pages: Vec<PageId>,
    pub active_transactions: Vec<TransactionId>,
    /// Dirty Page Table: (page_id, recLSN)
    pub dpt: Vec<(PageId, Lsn)>,
}

#[derive(Debug, Clone)]
pub struct PageWritePayload {
    pub page_id: PageId,
    pub prev_page_lsn: Lsn,
    pub page_image: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct PageDeltaPayload {
    pub page_id: PageId,
    pub prev_page_lsn: Lsn,
    pub offset: u16,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct TransactionPayload {
    pub marker: TransactionRecordKind,
    pub txn_id: TransactionId,
}

#[derive(Debug, Clone)]
pub struct ClrPayload {
    pub txn_id: TransactionId,
    pub undone_lsn: Lsn,
    pub undo_next_lsn: Lsn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransactionRecordKind {
    Begin = 1,
    Commit = 2,
    Abort = 3,
}

impl TransactionRecordKind {
    pub fn from_u8(value: u8) -> QuillSQLResult<Self> {
        match value {
            1 => Ok(TransactionRecordKind::Begin),
            2 => Ok(TransactionRecordKind::Commit),
            3 => Ok(TransactionRecordKind::Abort),
            other => Err(QuillSQLError::Internal(format!(
                "Unknown transaction record kind: {}",
                other
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalFrame {
    pub lsn: Lsn,
    pub prev_lsn: Lsn,
    pub rmid: ResourceManagerId,
    pub info: u8,
    pub payload: WalRecordPayload,
}

impl WalRecordPayload {
    pub fn encode(&self, lsn: Lsn, prev_lsn: Lsn) -> Vec<u8> {
        let (rmid, info, body_bytes) = encode_body(self);
        build_frame(lsn, prev_lsn, rmid, info, &body_bytes)
    }
}

pub fn decode_frame(bytes: &[u8]) -> QuillSQLResult<(WalFrame, usize)> {
    if bytes.len() < 6 {
        return Err(QuillSQLError::Internal(
            "WAL frame too short to contain version".to_string(),
        ));
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if magic != WAL_MAGIC {
        return Err(QuillSQLError::Internal(format!(
            "Invalid WAL magic: {:x}",
            magic
        )));
    }
    let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
    match version {
        WAL_VERSION => decode_frame_v2(bytes),
        WAL_VERSION_V1 => decode_frame_v1(bytes),
        other => Err(QuillSQLError::Internal(format!(
            "Unsupported WAL version: {}",
            other
        ))),
    }
}

fn decode_frame_v2(bytes: &[u8]) -> QuillSQLResult<(WalFrame, usize)> {
    if bytes.len() < WAL_HEADER_LEN + WAL_CRC_LEN {
        return Err(QuillSQLError::Internal(
            "WAL frame too short to contain header".to_string(),
        ));
    }
    let lsn = u64::from_le_bytes(bytes[6..14].try_into().unwrap());
    let prev_lsn = u64::from_le_bytes(bytes[14..22].try_into().unwrap());
    let rmid = ResourceManagerId::try_from(bytes[22])?;
    let info = bytes[23];
    let body_len = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
    let total_len = WAL_HEADER_LEN + body_len + WAL_CRC_LEN;
    if bytes.len() < total_len {
        return Err(QuillSQLError::Internal(
            "WAL frame truncated before body end".to_string(),
        ));
    }

    let body = &bytes[WAL_HEADER_LEN..WAL_HEADER_LEN + body_len];
    let expected_crc = u32::from_le_bytes(
        bytes[WAL_HEADER_LEN + body_len..total_len]
            .try_into()
            .unwrap(),
    );
    let mut hasher = Hasher::new();
    hasher.update(&bytes[0..WAL_HEADER_LEN + body_len]);
    let actual_crc = hasher.finalize();
    if expected_crc != actual_crc {
        return Err(QuillSQLError::Internal(
            "CRC mismatch for WAL frame".to_string(),
        ));
    }

    let payload = match rmid {
        ResourceManagerId::Page => match info {
            0 => WalRecordPayload::PageWrite(decode_page_write(body)?),
            1 => WalRecordPayload::PageDelta(decode_page_delta(body)?),
            other => {
                return Err(QuillSQLError::Internal(format!(
                    "Unknown Page info kind: {}",
                    other
                )))
            }
        },
        ResourceManagerId::Transaction => {
            WalRecordPayload::Transaction(decode_transaction(body, info)?)
        }
        ResourceManagerId::Heap => WalRecordPayload::Heap(decode_heap(body, info)?),
        ResourceManagerId::Checkpoint => WalRecordPayload::Checkpoint(decode_checkpoint(body)?),
        ResourceManagerId::Clr => WalRecordPayload::Clr(decode_clr(body)?),
    };

    Ok((
        WalFrame {
            lsn,
            prev_lsn,
            rmid,
            info,
            payload,
        },
        total_len,
    ))
}

fn decode_frame_v1(bytes: &[u8]) -> QuillSQLResult<(WalFrame, usize)> {
    const HEADER_LEN_V1: usize = 4 + 2 + 8 + 1 + 4; // magic + version + lsn + kind + body_len
    if bytes.len() < HEADER_LEN_V1 + WAL_CRC_LEN {
        return Err(QuillSQLError::Internal(
            "WAL frame too short to contain header".to_string(),
        ));
    }
    let lsn = u64::from_le_bytes(bytes[6..14].try_into().unwrap());
    let kind = bytes[14];
    let rmid = ResourceManagerId::try_from(kind)?;
    let body_len = u32::from_le_bytes(bytes[15..19].try_into().unwrap()) as usize;
    let total_len = HEADER_LEN_V1 + body_len + WAL_CRC_LEN;
    if bytes.len() < total_len {
        return Err(QuillSQLError::Internal(
            "WAL frame truncated before body end".to_string(),
        ));
    }

    let body = &bytes[HEADER_LEN_V1..HEADER_LEN_V1 + body_len];
    let expected_crc = u32::from_le_bytes(
        bytes[HEADER_LEN_V1 + body_len..total_len]
            .try_into()
            .unwrap(),
    );
    let mut hasher = Hasher::new();
    hasher.update(&bytes[0..HEADER_LEN_V1 + body_len]);
    let actual_crc = hasher.finalize();
    if expected_crc != actual_crc {
        return Err(QuillSQLError::Internal(
            "CRC mismatch for WAL frame".to_string(),
        ));
    }

    let (info, payload) = match rmid {
        ResourceManagerId::Page => (0, WalRecordPayload::PageWrite(decode_page_write(body)?)),
        ResourceManagerId::Transaction => {
            if body.len() != 9 {
                return Err(QuillSQLError::Internal(
                    "Legacy transaction payload must be 9 bytes".to_string(),
                ));
            }
            let marker = body[8];
            let payload = WalRecordPayload::Transaction(decode_transaction(&body[..8], marker)?);
            (marker, payload)
        }
        ResourceManagerId::Heap => {
            if body.is_empty() {
                return Err(QuillSQLError::Internal(
                    "Legacy heap payload missing kind byte".to_string(),
                ));
            }
            let kind = body[0];
            let payload = WalRecordPayload::Heap(decode_heap(&body[1..], kind)?);
            (kind, payload)
        }
        ResourceManagerId::Checkpoint => {
            (0, WalRecordPayload::Checkpoint(decode_checkpoint(body)?))
        }
        ResourceManagerId::Clr => (0, WalRecordPayload::Clr(decode_clr(body)?)),
    };

    Ok((
        WalFrame {
            lsn,
            prev_lsn: lsn.saturating_sub(1),
            rmid,
            info,
            payload,
        },
        total_len,
    ))
}

pub(crate) fn encode_body(payload: &WalRecordPayload) -> (ResourceManagerId, u8, Vec<u8>) {
    match payload {
        WalRecordPayload::PageWrite(body) => (ResourceManagerId::Page, 0, encode_page_write(body)),
        WalRecordPayload::PageDelta(body) => (ResourceManagerId::Page, 1, encode_page_delta(body)),
        WalRecordPayload::Transaction(body) => {
            let (info, buf) = encode_transaction(body);
            (ResourceManagerId::Transaction, info, buf)
        }
        WalRecordPayload::Heap(body) => {
            let (info, buf) = encode_heap(body);
            (ResourceManagerId::Heap, info, buf)
        }
        WalRecordPayload::Checkpoint(body) => {
            (ResourceManagerId::Checkpoint, 0, encode_checkpoint(body))
        }
        WalRecordPayload::Clr(body) => (ResourceManagerId::Clr, 0, encode_clr(body)),
    }
}

pub(crate) fn build_frame(
    lsn: Lsn,
    prev_lsn: Lsn,
    rmid: ResourceManagerId,
    info: u8,
    body_bytes: &[u8],
) -> Vec<u8> {
    let mut frame = Vec::with_capacity(WAL_HEADER_LEN + body_bytes.len() + WAL_CRC_LEN);
    frame.extend_from_slice(&WAL_MAGIC.to_le_bytes());
    frame.extend_from_slice(&WAL_VERSION.to_le_bytes());
    frame.extend_from_slice(&lsn.to_le_bytes());
    frame.extend_from_slice(&prev_lsn.to_le_bytes());
    frame.push(rmid as u8);
    frame.push(info);
    frame.extend_from_slice(&(body_bytes.len() as u32).to_le_bytes());
    frame.extend_from_slice(body_bytes);

    let mut hasher = Hasher::new();
    hasher.update(&frame);
    let crc = hasher.finalize();
    frame.extend_from_slice(&crc.to_le_bytes());
    frame
}

fn encode_page_write(body: &PageWritePayload) -> Vec<u8> {
    // Page/PageWrite (rmid=Page, info=0)
    // body: page_id(4) + prev_page_lsn(8) + image_len(4) + page_image[]
    let mut buf = Vec::with_capacity(4 + 8 + 4 + body.page_image.len());
    buf.extend_from_slice(&body.page_id.to_le_bytes());
    buf.extend_from_slice(&body.prev_page_lsn.to_le_bytes());
    buf.extend_from_slice(&(body.page_image.len() as u32).to_le_bytes());
    buf.extend_from_slice(&body.page_image);
    buf
}

fn encode_page_delta(body: &PageDeltaPayload) -> Vec<u8> {
    // Page/PageDelta (rmid=Page, info=1)
    // body: page_id(4) + prev_page_lsn(8) + offset(2) + len(4) + data[]
    let mut buf = Vec::with_capacity(4 + 8 + 2 + 4 + body.data.len());
    buf.extend_from_slice(&body.page_id.to_le_bytes());
    buf.extend_from_slice(&body.prev_page_lsn.to_le_bytes());
    buf.extend_from_slice(&body.offset.to_le_bytes());
    buf.extend_from_slice(&(body.data.len() as u32).to_le_bytes());
    buf.extend_from_slice(&body.data);
    buf
}

fn decode_page_write(bytes: &[u8]) -> QuillSQLResult<PageWritePayload> {
    if bytes.len() < 4 + 8 + 4 {
        return Err(QuillSQLError::Internal(
            "PageWrite payload too short".to_string(),
        ));
    }
    let page_id = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as PageId;
    let prev_page_lsn = u64::from_le_bytes(bytes[4..12].try_into().unwrap()) as Lsn;
    let image_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    if bytes.len() != 16 + image_len {
        return Err(QuillSQLError::Internal(
            "PageWrite payload length mismatch".to_string(),
        ));
    }
    let page_image = bytes[16..].to_vec();
    Ok(PageWritePayload {
        page_id,
        prev_page_lsn,
        page_image,
    })
}

fn decode_page_delta(bytes: &[u8]) -> QuillSQLResult<PageDeltaPayload> {
    if bytes.len() < 4 + 8 + 2 + 4 {
        return Err(QuillSQLError::Internal(
            "PageDelta payload too short".to_string(),
        ));
    }
    let page_id = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as PageId;
    let prev_page_lsn = u64::from_le_bytes(bytes[4..12].try_into().unwrap()) as Lsn;
    let offset = u16::from_le_bytes(bytes[12..14].try_into().unwrap());
    let data_len = u32::from_le_bytes(bytes[14..18].try_into().unwrap()) as usize;
    if bytes.len() != 18 + data_len {
        return Err(QuillSQLError::Internal(
            "PageDelta payload length mismatch".to_string(),
        ));
    }
    let data = bytes[18..].to_vec();
    Ok(PageDeltaPayload {
        page_id,
        prev_page_lsn,
        offset,
        data,
    })
}

fn encode_transaction(body: &TransactionPayload) -> (u8, Vec<u8>) {
    let mut buf = Vec::with_capacity(8);
    buf.extend_from_slice(&body.txn_id.to_le_bytes());
    (body.marker as u8, buf)
}

fn encode_clr(body: &ClrPayload) -> Vec<u8> {
    // CLR (rmid=Clr, info=0)
    // body: txn_id(8) + undone_lsn(8) + undo_next_lsn(8)
    let mut buf = Vec::with_capacity(24);
    buf.extend_from_slice(&body.txn_id.to_le_bytes());
    buf.extend_from_slice(&body.undone_lsn.to_le_bytes());
    buf.extend_from_slice(&body.undo_next_lsn.to_le_bytes());
    buf
}

fn decode_transaction(bytes: &[u8], info: u8) -> QuillSQLResult<TransactionPayload> {
    if bytes.len() != 8 {
        return Err(QuillSQLError::Internal(
            "Transaction payload must be 8 bytes".to_string(),
        ));
    }
    let txn_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as TransactionId;
    let marker = TransactionRecordKind::from_u8(info)?;
    Ok(TransactionPayload { marker, txn_id })
}

fn decode_clr(bytes: &[u8]) -> QuillSQLResult<ClrPayload> {
    if bytes.len() < 24 {
        return Err(QuillSQLError::Internal("CLR body too small".to_string()));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[0..8]);
    let txn_id = u64::from_le_bytes(arr);
    arr.copy_from_slice(&bytes[8..16]);
    let undone_lsn = u64::from_le_bytes(arr);
    arr.copy_from_slice(&bytes[16..24]);
    let undo_next_lsn = u64::from_le_bytes(arr);
    Ok(ClrPayload {
        txn_id,
        undone_lsn,
        undo_next_lsn,
    })
}

fn encode_checkpoint(body: &CheckpointPayload) -> Vec<u8> {
    // Checkpoint (rmid=Checkpoint, info=0)
    // body: last_lsn(8) + dirty_pages_len(4)+[u32]*N + active_txn_len(4)+[u64]*M + dpt_len(4)+[(u32,u64)]*K
    let mut buf = Vec::new();
    buf.extend_from_slice(&body.last_lsn.to_le_bytes());
    buf.extend_from_slice(&(body.dirty_pages.len() as u32).to_le_bytes());
    for page_id in &body.dirty_pages {
        buf.extend_from_slice(&page_id.to_le_bytes());
    }
    buf.extend_from_slice(&(body.active_transactions.len() as u32).to_le_bytes());
    for txn in &body.active_transactions {
        buf.extend_from_slice(&txn.to_le_bytes());
    }
    // Encode DPT (length + pairs)
    buf.extend_from_slice(&(body.dpt.len() as u32).to_le_bytes());
    for (pid, lsn) in &body.dpt {
        buf.extend_from_slice(&pid.to_le_bytes());
        buf.extend_from_slice(&lsn.to_le_bytes());
    }
    buf
}

fn decode_checkpoint(bytes: &[u8]) -> QuillSQLResult<CheckpointPayload> {
    if bytes.len() < 8 + 4 + 4 {
        return Err(QuillSQLError::Internal(
            "Checkpoint payload too short".to_string(),
        ));
    }
    let last_lsn = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let mut offset = 8;
    let dirty_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let mut dirty_pages = Vec::with_capacity(dirty_len);
    let required = offset + dirty_len * 4;
    if bytes.len() < required + 4 {
        return Err(QuillSQLError::Internal(
            "Checkpoint dirty page list truncated".to_string(),
        ));
    }
    for chunk in bytes[offset..required].chunks_exact(4) {
        dirty_pages.push(u32::from_le_bytes(chunk.try_into().unwrap()));
    }
    offset = required;
    let txn_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let required_txn = offset + txn_len * 8;
    if bytes.len() < required_txn {
        return Err(QuillSQLError::Internal(
            "Checkpoint active txn list truncated".to_string(),
        ));
    }
    let mut active_transactions = Vec::with_capacity(txn_len);
    for chunk in bytes[offset..required_txn].chunks_exact(8) {
        active_transactions.push(u64::from_le_bytes(chunk.try_into().unwrap()));
    }
    offset = required_txn;
    // Optional DPT section: if no more bytes, default empty
    let mut dpt = Vec::new();
    if bytes.len() >= offset + 4 {
        let dpt_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let required_dpt = offset + dpt_len * (4 + 8);
        if bytes.len() < required_dpt {
            return Err(QuillSQLError::Internal(
                "Checkpoint DPT truncated".to_string(),
            ));
        }
        dpt.reserve(dpt_len);
        let mut cur = offset;
        for _ in 0..dpt_len {
            let pid = u32::from_le_bytes(bytes[cur..cur + 4].try_into().unwrap());
            cur += 4;
            let lsn = u64::from_le_bytes(bytes[cur..cur + 8].try_into().unwrap());
            cur += 8;
            dpt.push((pid, lsn));
        }
    }
    Ok(CheckpointPayload {
        last_lsn,
        dirty_pages,
        active_transactions,
        dpt,
    })
}

fn encode_heap(payload: &HeapRecordPayload) -> (u8, Vec<u8>) {
    match payload {
        HeapRecordPayload::Insert(body) => (HeapRecordKind::Insert as u8, encode_heap_insert(body)),
        HeapRecordPayload::Update(body) => (HeapRecordKind::Update as u8, encode_heap_update(body)),
        HeapRecordPayload::Delete(body) => (HeapRecordKind::Delete as u8, encode_heap_delete(body)),
    }
}

fn decode_heap(bytes: &[u8], info: u8) -> QuillSQLResult<HeapRecordPayload> {
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
    buf.extend_from_slice(&meta.delete_txn_id.to_le_bytes());
    buf.push(meta.is_deleted as u8);
}

fn decode_tuple_meta(bytes: &[u8]) -> QuillSQLResult<(TupleMetaRepr, usize)> {
    if bytes.len() < 8 + 8 + 1 {
        return Err(QuillSQLError::Internal(
            "Heap payload too short for tuple meta".to_string(),
        ));
    }
    let insert_txn_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as TransactionId;
    let delete_txn_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as TransactionId;
    let is_deleted = bytes[16] != 0;
    Ok((
        TupleMetaRepr {
            insert_txn_id,
            delete_txn_id,
            is_deleted,
        },
        17,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_page_write() {
        let payload = WalRecordPayload::PageWrite(PageWritePayload {
            page_id: 42,
            prev_page_lsn: 7,
            page_image: vec![1, 2, 3, 4, 5],
        });
        let bytes = payload.encode(100, 99);
        let (frame, len) = decode_frame(&bytes).unwrap();
        assert_eq!(len, bytes.len());
        match frame.payload {
            WalRecordPayload::PageWrite(body) => {
                assert_eq!(frame.lsn, 100);
                assert_eq!(frame.prev_lsn, 99);
                assert_eq!(frame.rmid, ResourceManagerId::Page);
                assert_eq!(frame.info, 0);
                assert_eq!(body.page_id, 42);
                assert_eq!(body.prev_page_lsn, 7);
                assert_eq!(body.page_image, vec![1, 2, 3, 4, 5]);
            }
            _ => panic!("unexpected payload variant"),
        }
    }

    #[test]
    fn decode_legacy_v1_frame() {
        let payload = PageWritePayload {
            page_id: 11,
            prev_page_lsn: 5,
            page_image: vec![0xAA, 0xBB],
        };
        let body = encode_page_write(&payload);
        let lsn = 123u64;

        let mut frame = Vec::new();
        frame.extend_from_slice(&WAL_MAGIC.to_le_bytes());
        frame.extend_from_slice(&WAL_VERSION_V1.to_le_bytes());
        frame.extend_from_slice(&lsn.to_le_bytes());
        frame.push(ResourceManagerId::Page as u8);
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
        frame.extend_from_slice(&body);
        let mut hasher = Hasher::new();
        hasher.update(&frame);
        let crc = hasher.finalize();
        frame.extend_from_slice(&crc.to_le_bytes());

        let (decoded, consumed) = decode_frame(&frame).unwrap();
        assert_eq!(consumed, frame.len());
        assert_eq!(decoded.lsn, lsn);
        assert_eq!(decoded.prev_lsn, lsn - 1);
        assert_eq!(decoded.rmid, ResourceManagerId::Page);
        assert_eq!(decoded.info, 0);
        match decoded.payload {
            WalRecordPayload::PageWrite(restored) => {
                assert_eq!(restored.page_id, payload.page_id);
                assert_eq!(restored.prev_page_lsn, payload.prev_page_lsn);
                assert_eq!(restored.page_image, payload.page_image);
            }
            other => panic!("unexpected payload variant: {:?}", other),
        }
    }

    #[test]
    fn encode_decode_page_delta() {
        let payload = WalRecordPayload::PageDelta(PageDeltaPayload {
            page_id: 7,
            prev_page_lsn: 3,
            offset: 10,
            data: vec![9, 8, 7, 6],
        });
        let bytes = payload.encode(200, 150);
        let (frame, len) = decode_frame(&bytes).unwrap();
        assert_eq!(len, bytes.len());
        match frame.payload {
            WalRecordPayload::PageDelta(body) => {
                assert_eq!(frame.lsn, 200);
                assert_eq!(frame.prev_lsn, 150);
                assert_eq!(frame.rmid, ResourceManagerId::Page);
                assert_eq!(frame.info, 1);
                assert_eq!(body.page_id, 7);
                assert_eq!(body.prev_page_lsn, 3);
                assert_eq!(body.offset, 10);
                assert_eq!(body.data, vec![9, 8, 7, 6]);
            }
            _ => panic!("unexpected payload variant"),
        }
    }

    #[test]
    fn encode_decode_transaction() {
        let payload = WalRecordPayload::Transaction(TransactionPayload {
            marker: TransactionRecordKind::Commit,
            txn_id: 88,
        });
        let bytes = payload.encode(200, 150);
        let (frame, len) = decode_frame(&bytes).unwrap();
        assert_eq!(len, bytes.len());
        match frame.payload {
            WalRecordPayload::Transaction(body) => {
                assert_eq!(frame.lsn, 200);
                assert_eq!(frame.prev_lsn, 150);
                assert_eq!(frame.rmid, ResourceManagerId::Transaction);
                assert_eq!(frame.info, TransactionRecordKind::Commit as u8);
                assert_eq!(body.marker, TransactionRecordKind::Commit);
                assert_eq!(body.txn_id, 88);
            }
            _ => panic!("unexpected payload variant"),
        }
    }

    #[test]
    fn encode_decode_heap_insert() {
        let payload = WalRecordPayload::Heap(HeapRecordPayload::Insert(HeapInsertPayload {
            relation: RelationIdent { root_page_id: 10 },
            page_id: 12,
            slot_id: 2,
            op_txn_id: 1,
            tuple_meta: TupleMetaRepr {
                insert_txn_id: 1,
                delete_txn_id: 0,
                is_deleted: false,
            },
            tuple_data: vec![7, 8, 9],
        }));
        let bytes = payload.encode(55, 54);
        let (frame, consumed) = decode_frame(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        match frame.payload {
            WalRecordPayload::Heap(HeapRecordPayload::Insert(body)) => {
                assert_eq!(frame.lsn, 55);
                assert_eq!(frame.prev_lsn, 54);
                assert_eq!(frame.rmid, ResourceManagerId::Heap);
                assert_eq!(frame.info, HeapRecordKind::Insert as u8);
                assert_eq!(body.relation.root_page_id, 10);
                assert_eq!(body.page_id, 12);
                assert_eq!(body.slot_id, 2);
                assert_eq!(body.tuple_meta.insert_txn_id, 1);
                assert_eq!(body.tuple_data, vec![7, 8, 9]);
            }
            other => panic!("unexpected payload variant: {:?}", other),
        }
    }

    #[test]
    fn encode_decode_heap_update() {
        let payload = WalRecordPayload::Heap(HeapRecordPayload::Update(HeapUpdatePayload {
            relation: RelationIdent { root_page_id: 99 },
            page_id: 44,
            slot_id: 5,
            op_txn_id: 11,
            new_tuple_meta: TupleMetaRepr {
                insert_txn_id: 11,
                delete_txn_id: 0,
                is_deleted: false,
            },
            new_tuple_data: vec![1, 2, 3, 4],
            old_tuple_meta: Some(TupleMetaRepr {
                insert_txn_id: 5,
                delete_txn_id: 7,
                is_deleted: true,
            }),
            old_tuple_data: Some(vec![9, 9, 9]),
        }));
        let bytes = payload.encode(77, 70);
        let (frame, consumed) = decode_frame(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        match frame.payload {
            WalRecordPayload::Heap(HeapRecordPayload::Update(body)) => {
                assert_eq!(frame.lsn, 77);
                assert_eq!(frame.prev_lsn, 70);
                assert_eq!(frame.rmid, ResourceManagerId::Heap);
                assert_eq!(frame.info, HeapRecordKind::Update as u8);
                assert_eq!(body.relation.root_page_id, 99);
                assert_eq!(body.page_id, 44);
                assert_eq!(body.slot_id, 5);
                assert_eq!(body.new_tuple_meta.insert_txn_id, 11);
                assert_eq!(body.new_tuple_data, vec![1, 2, 3, 4]);
                let old_meta = body.old_tuple_meta.unwrap();
                let old_data = body.old_tuple_data.unwrap();
                assert!(old_meta.is_deleted);
                assert_eq!(old_data, vec![9, 9, 9]);
            }
            other => panic!("unexpected payload variant: {:?}", other),
        }
    }

    #[test]
    fn encode_decode_heap_delete() {
        let payload = WalRecordPayload::Heap(HeapRecordPayload::Delete(HeapDeletePayload {
            relation: RelationIdent { root_page_id: 7 },
            page_id: 3,
            slot_id: 1,
            op_txn_id: 4,
            old_tuple_meta: TupleMetaRepr {
                insert_txn_id: 2,
                delete_txn_id: 4,
                is_deleted: true,
            },
            old_tuple_data: None,
        }));
        let bytes = payload.encode(88, 87);
        let (frame, consumed) = decode_frame(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        match frame.payload {
            WalRecordPayload::Heap(HeapRecordPayload::Delete(body)) => {
                assert_eq!(frame.lsn, 88);
                assert_eq!(frame.prev_lsn, 87);
                assert_eq!(frame.rmid, ResourceManagerId::Heap);
                assert_eq!(frame.info, HeapRecordKind::Delete as u8);
                assert_eq!(body.relation.root_page_id, 7);
                assert_eq!(body.page_id, 3);
                assert_eq!(body.slot_id, 1);
                let old_meta = body.old_tuple_meta;
                assert_eq!(old_meta.delete_txn_id, 4);
                assert!(old_meta.is_deleted);
                assert!(body.old_tuple_data.is_none());
            }
            other => panic!("unexpected payload variant: {:?}", other),
        }
    }

    #[test]
    fn encode_decode_checkpoint() {
        let payload = WalRecordPayload::Checkpoint(CheckpointPayload {
            last_lsn: 123,
            dirty_pages: vec![10, 11, 12],
            active_transactions: vec![1, 2, 3],
            dpt: vec![(10, 1000), (11, 1100)],
        });
        let bytes = payload.encode(999, 900);
        let (frame, consumed) = decode_frame(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        match frame.payload {
            WalRecordPayload::Checkpoint(body) => {
                assert_eq!(frame.lsn, 999);
                assert_eq!(frame.prev_lsn, 900);
                assert_eq!(frame.rmid, ResourceManagerId::Checkpoint);
                assert_eq!(frame.info, 0);
                assert_eq!(body.last_lsn, 123);
                assert_eq!(body.dirty_pages, vec![10, 11, 12]);
                assert_eq!(body.active_transactions, vec![1, 2, 3]);
                assert_eq!(body.dpt, vec![(10, 1000), (11, 1100)]);
            }
            other => panic!("unexpected payload variant: {:?}", other),
        }
    }

    #[test]
    fn encode_decode_clr() {
        let payload = ClrPayload {
            txn_id: 11,
            undone_lsn: 1234,
            undo_next_lsn: 0,
        };
        let rec = WalRecordPayload::Clr(payload.clone());
        let bytes = rec.encode(200, 150);
        let (frame, _len) = decode_frame(&bytes).unwrap();
        match frame.payload {
            WalRecordPayload::Clr(p) => {
                assert_eq!(p.txn_id, payload.txn_id);
                assert_eq!(p.undone_lsn, payload.undone_lsn);
            }
            _ => panic!("wrong kind"),
        }
    }
}
