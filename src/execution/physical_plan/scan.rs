//! Shared prefetch buffer utilities for scan operators.

use std::cell::RefCell;
use std::collections::VecDeque;

use crate::error::QuillSQLResult;
use crate::storage::page::{RecordId, TupleMeta};
use crate::storage::tuple::Tuple;

pub type ScanEntry = (RecordId, TupleMeta, Tuple);

#[derive(Debug)]
pub struct ScanPrefetch {
    buffer: RefCell<VecDeque<ScanEntry>>,
    batch_size: usize,
}

impl ScanPrefetch {
    pub fn new(batch_size: usize) -> Self {
        Self {
            buffer: RefCell::new(VecDeque::new()),
            batch_size,
        }
    }

    pub fn pop_front(&self) -> Option<ScanEntry> {
        self.buffer.borrow_mut().pop_front()
    }

    pub fn clear(&self) {
        self.buffer.borrow_mut().clear();
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
        self.buffer.borrow_mut().extend(fetched);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefetch_refill_and_pop() {
        let prefetch = ScanPrefetch::new(2);
        let rid = RecordId::new(1, 0);
        let meta = TupleMeta::new(1, 0);
        let tuple = Tuple::empty(crate::catalog::EMPTY_SCHEMA_REF.clone());
        let produced = prefetch
            .refill(|_, out| {
                out.push_back((rid, meta, tuple.clone()));
                Ok(())
            })
            .expect("refill should succeed");
        assert!(produced);
        assert!(prefetch.pop_front().is_some());
        assert!(prefetch.pop_front().is_none());
    }
}
