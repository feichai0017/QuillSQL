use crate::buffer::PageId;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::Lsn;
use crate::transaction::TransactionId;

#[derive(Debug, Clone)]
pub struct CheckpointPayload {
    pub last_lsn: Lsn,
    pub dirty_pages: Vec<PageId>,
    pub active_transactions: Vec<TransactionId>,
    /// Dirty Page Table: (page_id, recLSN)
    pub dpt: Vec<(PageId, Lsn)>,
}

pub fn encode_checkpoint(body: &CheckpointPayload) -> Vec<u8> {
    // Checkpoint (rmid=Checkpoint, info=0)
    // body: last_lsn(8) + dirty_pages_count(4) + dirty_pages[] + active_txns_count(4) + active_txns[] + dpt_count(4) + dpt[]
    let mut buf = Vec::new();
    buf.extend_from_slice(&body.last_lsn.to_le_bytes());
    buf.extend_from_slice(&(body.dirty_pages.len() as u32).to_le_bytes());
    for page_id in &body.dirty_pages {
        buf.extend_from_slice(&page_id.to_le_bytes());
    }
    buf.extend_from_slice(&(body.active_transactions.len() as u32).to_le_bytes());
    for txn_id in &body.active_transactions {
        buf.extend_from_slice(&txn_id.to_le_bytes());
    }
    buf.extend_from_slice(&(body.dpt.len() as u32).to_le_bytes());
    for (page_id, rec_lsn) in &body.dpt {
        buf.extend_from_slice(&page_id.to_le_bytes());
        buf.extend_from_slice(&rec_lsn.to_le_bytes());
    }
    buf
}

pub fn decode_checkpoint(bytes: &[u8]) -> QuillSQLResult<CheckpointPayload> {
    if bytes.len() < 8 + 4 + 4 + 4 {
        return Err(QuillSQLError::Internal(
            "Checkpoint payload too short".to_string(),
        ));
    }
    let last_lsn = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let mut offset = 8;
    let dirty_pages_len =
        u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let mut dirty_pages = Vec::with_capacity(dirty_pages_len);
    for _ in 0..dirty_pages_len {
        if bytes.len() < offset + 4 {
            return Err(QuillSQLError::Internal(
                "Checkpoint dirty pages truncated".to_string(),
            ));
        }
        let page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        dirty_pages.push(page_id);
    }
    if bytes.len() < offset + 4 {
        return Err(QuillSQLError::Internal(
            "Checkpoint active transactions truncated".to_string(),
        ));
    }
    let active_txn_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let mut active_transactions = Vec::with_capacity(active_txn_len);
    for _ in 0..active_txn_len {
        if bytes.len() < offset + 8 {
            return Err(QuillSQLError::Internal(
                "Checkpoint active transactions truncated".to_string(),
            ));
        }
        let txn_id = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        active_transactions.push(txn_id);
    }
    if bytes.len() < offset + 4 {
        return Err(QuillSQLError::Internal(
            "Checkpoint DPT length missing".to_string(),
        ));
    }
    let dpt_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let required_dpt = offset + dpt_len * (4 + 8);
    if bytes.len() < required_dpt {
        return Err(QuillSQLError::Internal(
            "Checkpoint DPT truncated".to_string(),
        ));
    }
    let mut dpt = Vec::with_capacity(dpt_len);
    let mut cur = offset;
    for _ in 0..dpt_len {
        let pid = u32::from_le_bytes(bytes[cur..cur + 4].try_into().unwrap());
        cur += 4;
        let lsn = u64::from_le_bytes(bytes[cur..cur + 8].try_into().unwrap());
        cur += 8;
        dpt.push((pid, lsn));
    }
    Ok(CheckpointPayload {
        last_lsn,
        dirty_pages,
        active_transactions,
        dpt,
    })
}
