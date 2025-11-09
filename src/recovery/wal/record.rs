use bytes::Bytes;

use crate::recovery::wal::Lsn;

#[derive(Clone, Debug)]
pub struct WalRecord {
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,
    pub payload: Bytes,
}

impl WalRecord {
    pub fn encoded_len(&self) -> u64 {
        self.end_lsn.saturating_sub(self.start_lsn)
    }
}
