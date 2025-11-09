use crate::recovery::wal::Lsn;

use super::record::WalRecord;

#[derive(Debug)]
pub struct WalBuffer {
    records: Vec<WalRecord>,
    bytes: usize,
    last_record_start: Lsn,
}

impl WalBuffer {
    pub fn new(last_start: Lsn) -> Self {
        Self {
            records: Vec::new(),
            bytes: 0,
            last_record_start: last_start,
        }
    }

    pub fn push(&mut self, record: WalRecord) {
        self.last_record_start = record.start_lsn;
        self.bytes = self.bytes.saturating_add(record.encoded_len() as usize);
        self.records.push(record);
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn last_record_start(&self) -> Lsn {
        self.last_record_start
    }

    pub fn highest_end_lsn(&self) -> Lsn {
        self.records
            .last()
            .map(|record| record.end_lsn)
            .unwrap_or(self.last_record_start)
    }

    pub fn drain_until(&mut self, upto: Lsn) -> (Vec<WalRecord>, usize) {
        let count = self
            .records
            .iter()
            .take_while(|record| record.end_lsn <= upto)
            .count();
        if count == 0 {
            return (Vec::new(), 0);
        }
        let drained: Vec<WalRecord> = self.records.drain(..count).collect();
        let released: usize = drained
            .iter()
            .map(|record| record.encoded_len() as usize)
            .sum();
        self.bytes = self.bytes.saturating_sub(released);
        (drained, released)
    }

    pub fn pending(&self) -> Vec<WalRecord> {
        self.records.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_record(start: Lsn, len: usize) -> WalRecord {
        WalRecord {
            start_lsn: start,
            end_lsn: start + len as u64,
            payload: Bytes::from(vec![0u8; len]),
        }
    }

    #[test]
    fn push_updates_length_and_bytes() {
        let mut buffer = WalBuffer::new(0);
        buffer.push(make_record(0, 16));
        buffer.push(make_record(16, 32));

        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.bytes(), 48);
        assert_eq!(buffer.last_record_start(), 16);
        assert_eq!(buffer.highest_end_lsn(), 48);
    }

    #[test]
    fn drain_until_releases_records_and_bytes() {
        let mut buffer = WalBuffer::new(0);
        buffer.push(make_record(0, 10));
        buffer.push(make_record(10, 20));
        buffer.push(make_record(30, 5));

        let (drained, released) = buffer.drain_until(30);
        assert_eq!(drained.len(), 2);
        assert_eq!(released, 30);
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.bytes(), 5);
        assert_eq!(buffer.highest_end_lsn(), 35);
    }
}
