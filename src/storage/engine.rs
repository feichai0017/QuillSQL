use crate::error::Result;
use std::ops::RangeBounds;
use std::ops::Bound;

pub trait Engine {
    type EngineIterator<'a>: EngineIterator where Self: 'a;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>>;

    fn delete(&mut self, key: Vec<u8>) -> Result<()>;

    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_>;

    fn scan_prefix(&mut self, prefix: Vec<u8>) -> Self::EngineIterator<'_> {
        let start = Bound::Included(prefix.clone());
        let mut bound_prefix = prefix.clone();

        let end = match bound_prefix.iter().rposition(|b| *b != 255) {
            Some(pos) => {
                bound_prefix[pos] += 1;
                bound_prefix.truncate(pos + 1);
                Bound::Excluded(bound_prefix)
            }
            None => Bound::Unbounded,
        };

        self.scan((start, end))
    }
}

pub trait EngineIterator: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}
