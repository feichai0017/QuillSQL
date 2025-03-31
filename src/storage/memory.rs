use std::collections::{btree_map, BTreeMap};

use crate::error::Result;

// 内存存储引擎定义
pub struct MemoryEngine {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MemoryEngine {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }
}

impl super::engine::Engine for MemoryEngine {
    type EngineIterator<'a> = MemoryEngineIterator<'a>;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.data.insert(key, value);
        Ok(())
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let value = self.data.get(&key).cloned();
        Ok(value)
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.data.remove(&key);
        Ok(())
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        MemoryEngineIterator {
            inner: self.data.range(range),
        }
    }
}

// 内存存储引擎迭代器
pub struct MemoryEngineIterator<'a> {
    inner: btree_map::Range<'a, Vec<u8>, Vec<u8>>,
}

impl<'a> super::engine::EngineIterator for MemoryEngineIterator<'a> {}

impl<'a> MemoryEngineIterator<'a> {
    fn map(item: (&Vec<u8>, &Vec<u8>)) -> <Self as Iterator>::Item {
        let (k, v) = item;
        Ok((k.clone(), v.clone()))
    }
}

impl<'a> Iterator for MemoryEngineIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Self::map)
    }
}

impl<'a> DoubleEndedIterator for MemoryEngineIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(Self::map)
    }
}
