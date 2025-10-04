use crate::error::{QuillSQLError, QuillSQLResult};
use crate::transaction::TransactionId;

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
pub struct TransactionPayload {
    pub marker: TransactionRecordKind,
    pub txn_id: TransactionId,
}

pub fn encode_transaction(body: &TransactionPayload) -> (u8, Vec<u8>) {
    let mut buf = Vec::with_capacity(8);
    buf.extend_from_slice(&body.txn_id.to_le_bytes());
    (body.marker as u8, buf)
}

pub fn decode_transaction(bytes: &[u8], info: u8) -> QuillSQLResult<TransactionPayload> {
    if bytes.len() != 8 {
        return Err(QuillSQLError::Internal(
            "Transaction payload must be 8 bytes".to_string(),
        ));
    }
    let txn_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as TransactionId;
    let marker = TransactionRecordKind::from_u8(info)?;
    Ok(TransactionPayload { marker, txn_id })
}
