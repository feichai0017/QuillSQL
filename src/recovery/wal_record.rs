use std::convert::TryFrom;

use crc32fast::Hasher;

use crate::buffer::PageId;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::Lsn;
use crate::transaction::TransactionId;

const WAL_MAGIC: u32 = 0x5157_414c; // "QWAL"
const WAL_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalFrameKind {
    PageWrite = 1,
    Transaction = 2,
}

impl TryFrom<u8> for WalFrameKind {
    type Error = QuillSQLError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(WalFrameKind::PageWrite),
            2 => Ok(WalFrameKind::Transaction),
            other => Err(QuillSQLError::Internal(format!(
                "Unknown WAL frame kind: {}",
                other
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub enum WalRecordPayload {
    PageWrite(PageWritePayload),
    Transaction(TransactionPayload),
}

#[derive(Debug, Clone)]
pub struct PageWritePayload {
    pub page_id: PageId,
    pub prev_page_lsn: Lsn,
    pub page_image: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct TransactionPayload {
    pub marker: TransactionRecordKind,
    pub txn_id: TransactionId,
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
    pub payload: WalRecordPayload,
}

impl WalRecordPayload {
    pub fn encode(&self, lsn: Lsn) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&WAL_MAGIC.to_le_bytes());
        frame.extend_from_slice(&WAL_VERSION.to_le_bytes());
        frame.extend_from_slice(&lsn.to_le_bytes());

        match self {
            WalRecordPayload::PageWrite(body) => {
                frame.push(WalFrameKind::PageWrite as u8);
                let body_bytes = encode_page_write(body);
                frame.extend_from_slice(&(body_bytes.len() as u32).to_le_bytes());
                frame.extend_from_slice(&body_bytes);
            }
            WalRecordPayload::Transaction(body) => {
                frame.push(WalFrameKind::Transaction as u8);
                let body_bytes = encode_transaction(body);
                frame.extend_from_slice(&(body_bytes.len() as u32).to_le_bytes());
                frame.extend_from_slice(&body_bytes);
            }
        }

        let mut hasher = Hasher::new();
        hasher.update(&frame);
        let crc = hasher.finalize();
        frame.extend_from_slice(&crc.to_le_bytes());
        frame
    }
}

pub fn decode_frame(bytes: &[u8]) -> QuillSQLResult<(WalFrame, usize)> {
    const HEADER_LEN: usize = 4 + 2 + 8 + 1 + 4; // magic + version + lsn + kind + body_len
    if bytes.len() < HEADER_LEN + 4 {
        return Err(QuillSQLError::Internal(
            "WAL frame too short to contain header".to_string(),
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
    if version != WAL_VERSION {
        return Err(QuillSQLError::Internal(format!(
            "Unsupported WAL version: {}",
            version
        )));
    }
    let lsn = u64::from_le_bytes(bytes[6..14].try_into().unwrap());
    let kind = WalFrameKind::try_from(bytes[14])?;
    let body_len = u32::from_le_bytes(bytes[15..19].try_into().unwrap()) as usize;
    let total_len = HEADER_LEN + body_len + 4;
    if bytes.len() < total_len {
        return Err(QuillSQLError::Internal(
            "WAL frame truncated before body end".to_string(),
        ));
    }

    let body = &bytes[19..19 + body_len];
    let expected_crc = u32::from_le_bytes(bytes[19 + body_len..total_len].try_into().unwrap());
    let mut hasher = Hasher::new();
    hasher.update(&bytes[0..19 + body_len]);
    let actual_crc = hasher.finalize();
    if expected_crc != actual_crc {
        return Err(QuillSQLError::Internal(
            "CRC mismatch for WAL frame".to_string(),
        ));
    }

    let payload = match kind {
        WalFrameKind::PageWrite => WalRecordPayload::PageWrite(decode_page_write(body)?),
        WalFrameKind::Transaction => WalRecordPayload::Transaction(decode_transaction(body)?),
    };

    Ok((WalFrame { lsn, payload }, total_len))
}

fn encode_page_write(body: &PageWritePayload) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + 8 + 4 + body.page_image.len());
    buf.extend_from_slice(&body.page_id.to_le_bytes());
    buf.extend_from_slice(&body.prev_page_lsn.to_le_bytes());
    buf.extend_from_slice(&(body.page_image.len() as u32).to_le_bytes());
    buf.extend_from_slice(&body.page_image);
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

fn encode_transaction(body: &TransactionPayload) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + 1);
    buf.extend_from_slice(&body.txn_id.to_le_bytes());
    buf.push(body.marker as u8);
    buf
}

fn decode_transaction(bytes: &[u8]) -> QuillSQLResult<TransactionPayload> {
    if bytes.len() != 9 {
        return Err(QuillSQLError::Internal(
            "Transaction payload must be 9 bytes".to_string(),
        ));
    }
    let txn_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as TransactionId;
    let marker = TransactionRecordKind::from_u8(bytes[8])?;
    Ok(TransactionPayload { marker, txn_id })
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
        let bytes = payload.encode(100);
        let (frame, len) = decode_frame(&bytes).unwrap();
        assert_eq!(len, bytes.len());
        match frame.payload {
            WalRecordPayload::PageWrite(body) => {
                assert_eq!(frame.lsn, 100);
                assert_eq!(body.page_id, 42);
                assert_eq!(body.prev_page_lsn, 7);
                assert_eq!(body.page_image, vec![1, 2, 3, 4, 5]);
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
        let bytes = payload.encode(200);
        let (frame, len) = decode_frame(&bytes).unwrap();
        assert_eq!(len, bytes.len());
        match frame.payload {
            WalRecordPayload::Transaction(body) => {
                assert_eq!(frame.lsn, 200);
                assert_eq!(body.marker, TransactionRecordKind::Commit);
                assert_eq!(body.txn_id, 88);
            }
            _ => panic!("unexpected payload variant"),
        }
    }
}
