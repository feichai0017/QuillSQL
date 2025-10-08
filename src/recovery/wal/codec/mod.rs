use std::convert::TryFrom;

use crc32fast::Hasher;

use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::wal_record::WalRecordPayload;
use crate::recovery::Lsn;
use crate::storage::heap::wal_codec::{decode_heap_record, encode_heap_record, HeapRecordKind};

pub mod checkpoint;
pub mod clr;
pub mod page;
pub mod txn;

pub use checkpoint::{decode_checkpoint, encode_checkpoint, CheckpointPayload};
pub use clr::{decode_clr, encode_clr, ClrPayload};
pub use page::{
    decode_page_delta, decode_page_write, encode_page_delta, encode_page_write, PageDeltaPayload,
    PageWritePayload,
};
pub use txn::{decode_transaction, encode_transaction, TransactionPayload, TransactionRecordKind};

pub const WAL_MAGIC: u32 = 0x5157_414c; // "QWAL" (LE)
pub const WAL_VERSION_V1: u16 = 1;
pub const WAL_VERSION: u16 = 2;
pub const WAL_HEADER_LEN: usize = 4 + 2 + 8 + 8 + 1 + 1 + 4;
pub const WAL_CRC_LEN: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
pub struct WalFrame {
    pub lsn: Lsn,
    pub prev_lsn: Lsn,
    pub rmid: ResourceManagerId,
    pub info: u8,
    pub body: Vec<u8>,
}

pub fn encode_frame(lsn: Lsn, prev_lsn: Lsn, payload: &WalRecordPayload) -> Vec<u8> {
    let (rmid, info, body_bytes) = encode_body(payload);
    build_frame(lsn, prev_lsn, rmid, info, &body_bytes)
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

pub(crate) fn encode_body(payload: &WalRecordPayload) -> (ResourceManagerId, u8, Vec<u8>) {
    match payload {
        WalRecordPayload::PageWrite(body) => (ResourceManagerId::Page, 0, encode_page_write(body)),
        WalRecordPayload::PageDelta(body) => (ResourceManagerId::Page, 1, encode_page_delta(body)),
        WalRecordPayload::Transaction(body) => {
            let (info, buf) = encode_transaction(body);
            (ResourceManagerId::Transaction, info, buf)
        }
        WalRecordPayload::Heap(body) => {
            let (info, buf) = encode_heap_record(body);
            (ResourceManagerId::Heap, info, buf)
        }
        WalRecordPayload::Checkpoint(body) => {
            (ResourceManagerId::Checkpoint, 0, encode_checkpoint(body))
        }
        WalRecordPayload::Clr(body) => (ResourceManagerId::Clr, 0, encode_clr(body)),
    }
}

fn build_frame(
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

    Ok((
        WalFrame {
            lsn,
            prev_lsn,
            rmid,
            info,
            body: body.to_vec(),
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

    let info = match rmid {
        ResourceManagerId::Page => 0,
        ResourceManagerId::Transaction => {
            if body.len() != 9 {
                return Err(QuillSQLError::Internal(
                    "Legacy transaction payload must be 9 bytes".to_string(),
                ));
            }
            body[8]
        }
        ResourceManagerId::Heap => {
            if body.is_empty() {
                return Err(QuillSQLError::Internal(
                    "Legacy heap payload missing kind byte".to_string(),
                ));
            }
            body[0]
        }
        ResourceManagerId::Checkpoint | ResourceManagerId::Clr => 0,
    };

    Ok((
        WalFrame {
            lsn,
            prev_lsn: lsn.saturating_sub(1),
            rmid,
            info,
            body: match rmid {
                ResourceManagerId::Page
                | ResourceManagerId::Checkpoint
                | ResourceManagerId::Clr => body.to_vec(),
                ResourceManagerId::Transaction => body[..8].to_vec(),
                ResourceManagerId::Heap => body[1..].to_vec(),
            },
        },
        total_len,
    ))
}

pub fn decode_payload(frame: &WalFrame) -> QuillSQLResult<WalRecordPayload> {
    match frame.rmid {
        ResourceManagerId::Page => match frame.info {
            0 => Ok(WalRecordPayload::PageWrite(decode_page_write(&frame.body)?)),
            1 => Ok(WalRecordPayload::PageDelta(decode_page_delta(&frame.body)?)),
            other => Err(QuillSQLError::Internal(format!(
                "Unknown Page info kind: {}",
                other
            ))),
        },
        ResourceManagerId::Transaction => Ok(WalRecordPayload::Transaction(decode_transaction(
            &frame.body,
            frame.info,
        )?)),
        ResourceManagerId::Heap => Ok(WalRecordPayload::Heap(decode_heap_record(
            &frame.body,
            frame.info,
        )?)),
        ResourceManagerId::Checkpoint => Ok(WalRecordPayload::Checkpoint(decode_checkpoint(
            &frame.body,
        )?)),
        ResourceManagerId::Clr => Ok(WalRecordPayload::Clr(decode_clr(&frame.body)?)),
    }
}

pub fn heap_record_kind_to_info(kind: HeapRecordKind) -> u8 {
    kind as u8
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recovery::wal_record::WalRecordPayload;
    use crate::storage::heap::wal_codec::{
        HeapDeletePayload, HeapInsertPayload, HeapRecordPayload, HeapUpdatePayload, RelationIdent,
        TupleMetaRepr,
    };
    use crate::transaction::INVALID_COMMAND_ID;

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
        assert_eq!(frame.lsn, 100);
        assert_eq!(frame.prev_lsn, 99);
        assert_eq!(frame.rmid, ResourceManagerId::Page);
        assert_eq!(frame.info, 0);
        let decoded = decode_payload(&frame).unwrap();
        match decoded {
            WalRecordPayload::PageWrite(body) => {
                assert_eq!(body.page_id, 42);
                assert_eq!(body.prev_page_lsn, 7);
                assert_eq!(body.page_image, vec![1, 2, 3, 4, 5]);
            }
            other => panic!("unexpected payload variant: {:?}", other),
        }
    }

    #[test]
    fn encode_decode_page_delta() {
        let payload = WalRecordPayload::PageDelta(PageDeltaPayload {
            page_id: 17,
            prev_page_lsn: 8,
            offset: 5,
            data: vec![9, 9, 9],
        });
        let bytes = payload.encode(200, 150);
        let (frame, len) = decode_frame(&bytes).unwrap();
        assert_eq!(len, bytes.len());
        assert_eq!(frame.rmid, ResourceManagerId::Page);
        assert_eq!(frame.info, 1);
        let decoded = decode_payload(&frame).unwrap();
        match decoded {
            WalRecordPayload::PageDelta(body) => {
                assert_eq!(body.page_id, 17);
                assert_eq!(body.offset, 5);
                assert_eq!(body.data, vec![9, 9, 9]);
            }
            other => panic!("unexpected payload variant: {:?}", other),
        }
    }

    #[test]
    fn encode_decode_transaction() {
        let payload = WalRecordPayload::Transaction(TransactionPayload {
            marker: TransactionRecordKind::Commit,
            txn_id: 99,
        });
        let bytes = payload.encode(300, 200);
        let (frame, len) = decode_frame(&bytes).unwrap();
        assert_eq!(len, bytes.len());
        assert_eq!(frame.rmid, ResourceManagerId::Transaction);
        assert_eq!(frame.info, TransactionRecordKind::Commit as u8);
        let decoded = decode_payload(&frame).unwrap();
        match decoded {
            WalRecordPayload::Transaction(body) => {
                assert_eq!(body.marker, TransactionRecordKind::Commit);
                assert_eq!(body.txn_id, 99);
            }
            other => panic!("unexpected payload variant: {:?}", other),
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
                    insert_cid: 0,
                    delete_txn_id: 0,
                    delete_cid: INVALID_COMMAND_ID,
                    is_deleted: false,
                },
                tuple_data: vec![7, 8, 9],
            }));
            let bytes = payload.encode(123, 100);
            let (frame, len) = decode_frame(&bytes).unwrap();
            assert_eq!(len, bytes.len());
            assert_eq!(frame.rmid, ResourceManagerId::Heap);
            assert_eq!(frame.info, HeapRecordKind::Insert as u8);
            let decoded = decode_payload(&frame).unwrap();
            match decoded {
                WalRecordPayload::Heap(HeapRecordPayload::Insert(body)) => {
                    assert_eq!(body.relation.root_page_id, 10);
                    assert_eq!(body.page_id, 12);
                    assert_eq!(body.slot_id, 2);
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
                        insert_cid: 0,
                        delete_txn_id: 0,
                        delete_cid: INVALID_COMMAND_ID,
                        is_deleted: false,
                    },
                    new_tuple_data: vec![1, 2, 3, 4],
                    old_tuple_meta: Some(TupleMetaRepr {
                        insert_txn_id: 5,
                        insert_cid: 0,
                        delete_txn_id: 7,
                        delete_cid: 0,
                        is_deleted: true,
                    }),
                    old_tuple_data: Some(vec![9, 9, 9]),
                }));
                let bytes = payload.encode(200, 150);
                let (frame, len) = decode_frame(&bytes).unwrap();
                assert_eq!(len, bytes.len());
                assert_eq!(frame.rmid, ResourceManagerId::Heap);
                assert_eq!(frame.info, HeapRecordKind::Update as u8);
                let decoded = decode_payload(&frame).unwrap();
                match decoded {
                    WalRecordPayload::Heap(HeapRecordPayload::Update(body)) => {
                        assert_eq!(body.relation.root_page_id, 99);
                        assert_eq!(body.new_tuple_data, vec![1, 2, 3, 4]);
                        assert!(body.old_tuple_meta.unwrap().is_deleted);
                        assert_eq!(body.old_tuple_data.unwrap(), vec![9, 9, 9]);
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
                        insert_cid: 0,
                        delete_txn_id: 4,
                        delete_cid: 0,
                        is_deleted: true,
                    },
                    old_tuple_data: None,
                }));
                let bytes = payload.encode(80, 60);
                let (frame, len) = decode_frame(&bytes).unwrap();
                assert_eq!(len, bytes.len());
                assert_eq!(frame.rmid, ResourceManagerId::Heap);
                assert_eq!(frame.info, HeapRecordKind::Delete as u8);
                let decoded = decode_payload(&frame).unwrap();
                match decoded {
                    WalRecordPayload::Heap(HeapRecordPayload::Delete(body)) => {
                        assert_eq!(body.relation.root_page_id, 7);
                        assert!(body.old_tuple_meta.is_deleted);
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
        let (frame, len) = decode_frame(&bytes).unwrap();
        assert_eq!(len, bytes.len());
        assert_eq!(frame.rmid, ResourceManagerId::Checkpoint);
        let decoded = decode_payload(&frame).unwrap();
        match decoded {
            WalRecordPayload::Checkpoint(body) => {
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
        let clr = ClrPayload {
            txn_id: 11,
            undone_lsn: 1234,
            undo_next_lsn: 0,
        };
        let payload = WalRecordPayload::Clr(clr.clone());
        let bytes = payload.encode(200, 150);
        let (frame, len) = decode_frame(&bytes).unwrap();
        assert_eq!(len, bytes.len());
        assert_eq!(frame.rmid, ResourceManagerId::Clr);
        let decoded = decode_payload(&frame).unwrap();
        match decoded {
            WalRecordPayload::Clr(body) => {
                assert_eq!(body.txn_id, clr.txn_id);
                assert_eq!(body.undone_lsn, clr.undone_lsn);
                assert_eq!(body.undo_next_lsn, clr.undo_next_lsn);
            }
            other => panic!("unexpected payload variant: {:?}", other),
        }
    }
}
