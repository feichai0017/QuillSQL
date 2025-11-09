use std::collections::VecDeque;

use parking_lot::Mutex;

use crate::error::QuillSQLResult;
use crate::storage::page::{RecordId, TupleMeta};
use crate::storage::tuple::Tuple;

pub type ScanEntry = (RecordId, TupleMeta, Tuple);

#[derive(Debug)]
pub struct ScanPrefetch {
    buffer: Mutex<VecDeque<ScanEntry>>,
    batch_size: usize,
}

impl ScanPrefetch {
    pub fn new(batch_size: usize) -> Self {
        Self {
            buffer: Mutex::new(VecDeque::new()),
            batch_size,
        }
    }

    pub fn pop_front(&self) -> Option<ScanEntry> {
        self.buffer.lock().pop_front()
    }

    pub fn clear(&self) {
        self.buffer.lock().clear();
    }

    pub fn refill<F>(&self, mut producer: F) -> QuillSQLResult<bool>
    where
        F: FnMut(usize, &mut VecDeque<ScanEntry>) -> QuillSQLResult<()>,
    {
        let mut fetched = VecDeque::with_capacity(self.batch_size);
        producer(self.batch_size, &mut fetched)?;
        if fetched.is_empty() {
            return Ok(false);
        }
        let mut buffer = self.buffer.lock();
        buffer.extend(fetched);
        Ok(true)
    }
}
