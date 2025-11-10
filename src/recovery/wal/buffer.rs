use crate::recovery::wal::Lsn;
use crate::utils::ring_buffer::ConcurrentRingBuffer;

use super::record::WalRecord;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Debug)]
pub struct WalBuffer {
    queue: ConcurrentRingBuffer<WalRecord>,
    len: AtomicUsize,
    bytes: AtomicUsize,
    last_enqueued_end: AtomicU64,
}

impl WalBuffer {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            queue: ConcurrentRingBuffer::with_capacity(capacity.max(1)),
            len: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            last_enqueued_end: AtomicU64::new(0),
        }
    }

    pub fn push(&self, record: WalRecord) {
        let encoded_len = record.encoded_len() as usize;
        let end_lsn = record.end_lsn;
        let mut pending = record;
        loop {
            match self.queue.try_push(pending) {
                Ok(()) => break,
                Err(returned) => {
                    pending = returned;
                    std::hint::spin_loop();
                }
            }
        }
        self.len.fetch_add(1, Ordering::Release);
        self.bytes.fetch_add(encoded_len, Ordering::Release);
        self.last_enqueued_end.store(end_lsn, Ordering::Release);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    #[inline]
    pub fn bytes(&self) -> usize {
        self.bytes.load(Ordering::Acquire)
    }

    #[inline]
    pub fn highest_end_lsn(&self) -> Lsn {
        self.last_enqueued_end.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len.load(Ordering::Acquire) == 0
    }

    pub fn drain_until(&self, upto: Lsn) -> (Vec<WalRecord>, usize) {
        let mut drained = Vec::new();
        let mut released = 0usize;
        loop {
            let Some(front) = self.queue.peek_clone() else {
                break;
            };
            if front.end_lsn > upto {
                break;
            }
            if let Some(record) = self.queue.pop() {
                released += record.encoded_len() as usize;
                drained.push(record);
            } else {
                break;
            }
        }
        if !drained.is_empty() {
            self.len.fetch_sub(drained.len(), Ordering::Release);
            self.bytes.fetch_sub(released, Ordering::Release);
        }
        (drained, released)
    }

    pub fn pending(&self) -> Vec<WalRecord> {
        self.queue.snapshot()
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
        let buffer = WalBuffer::with_capacity(8);
        buffer.push(make_record(0, 16));
        buffer.push(make_record(16, 32));

        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.bytes(), 48);
        assert_eq!(buffer.highest_end_lsn(), 48);
    }

    #[test]
    fn drain_until_releases_records_and_bytes() {
        let buffer = WalBuffer::with_capacity(8);
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
