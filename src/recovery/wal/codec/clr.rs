use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::Lsn;
use crate::transaction::TransactionId;

#[derive(Debug, Clone)]
pub struct ClrPayload {
    pub txn_id: TransactionId,
    pub undone_lsn: Lsn,
    pub undo_next_lsn: Lsn,
}

pub fn encode_clr(body: &ClrPayload) -> Vec<u8> {
    // CLR (rmid=Clr, info=0)
    // body: txn_id(8) + undone_lsn(8) + undo_next_lsn(8)
    let mut buf = Vec::with_capacity(24);
    buf.extend_from_slice(&body.txn_id.to_le_bytes());
    buf.extend_from_slice(&body.undone_lsn.to_le_bytes());
    buf.extend_from_slice(&body.undo_next_lsn.to_le_bytes());
    buf
}

pub fn decode_clr(bytes: &[u8]) -> QuillSQLResult<ClrPayload> {
    if bytes.len() != 24 {
        return Err(QuillSQLError::Internal(
            "CLR payload must be 24 bytes".to_string(),
        ));
    }
    let txn_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as TransactionId;
    let undone_lsn = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let undo_next_lsn = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    Ok(ClrPayload {
        txn_id,
        undone_lsn,
        undo_next_lsn,
    })
}
