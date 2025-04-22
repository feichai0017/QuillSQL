pub mod bitcask;
pub mod cache;
pub mod datafile;
pub mod indexfile;
pub mod keydir;

use std::ops::Bound;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use self::bitcask::{Bitcask, Options};
use self::keydir::KeyDirEntry;
use crate::error::{Error, Result};
use crate::storage::engine::{Engine, EngineIterator};

pub const REMOVE_TOMBSTONE: &[u8] = b"%_%_%_%<!(R|E|M|O|V|E|D)!>%_%_%_%_";
pub static DATA_FILE_GLOB_FORMAT: &str = "data.*";

pub fn data_file_format(id: u128) -> String {
    format!("data.{}", id)
}

/// BitcaskEngine 是对 Bitcask 的包装，实现 Engine trait
pub struct BitcaskEngine {
    bitcask: Arc<Mutex<Bitcask>>,
}

impl BitcaskEngine {
    pub fn new(path: PathBuf) -> Result<Self> {
        let options = Options {
            base_dir: path,
            data_file_limit: 1024 * 1024 * 8, // 8MB
        };

        let bitcask = bitcask::new(options)?;
        Ok(Self {
            bitcask: Arc::new(Mutex::new(bitcask)),
        })
    }
}

impl Engine for BitcaskEngine {
    type EngineIterator<'a>
        = BitcaskEngineIterator<'a>
    where
        Self: 'a;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut db = self.bitcask.lock()?;
        db.write(&key, &value)
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let mut db = self.bitcask.lock()?;
        match db.read_cache(&key) {
            Ok(value) => Ok(Some(value)),
            Err(Error::Internal(msg)) if msg.contains("key not found") => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        let mut db = self.bitcask.lock()?;
        match db.remove(&key) {
            Ok(_) => Ok(()),
            Err(Error::Internal(msg)) if msg.contains("key not found") => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        let db = self.bitcask.clone();
        BitcaskEngineIterator::new(db, range)
    }
}

/// BitcaskEngine的迭代器实现
pub struct BitcaskEngineIterator<'a> {
    db: Arc<Mutex<Bitcask>>,
    keys: Vec<Vec<u8>>,
    entries: Vec<KeyDirEntry>,
    index: usize,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> BitcaskEngineIterator<'a> {
    fn new<R: RangeBounds<Vec<u8>>>(db: Arc<Mutex<Bitcask>>, range: R) -> Self {
        let mut keys = Vec::new();
        let mut entries = Vec::new();

        // 尝试获取锁并收集匹配范围的键
        if let Ok(db_locked) = db.lock() {
            let start_bound = match range.start_bound() {
                Bound::Included(key) => Some(key.as_slice()),
                Bound::Excluded(key) => Some(key.as_slice()),
                Bound::Unbounded => None,
            };

            let end_bound = match range.end_bound() {
                Bound::Included(key) => Some(key.as_slice()),
                Bound::Excluded(key) => Some(key.as_slice()),
                Bound::Unbounded => None,
            };

            // 根据边界条件选择使用哪种范围迭代方法
            let iter: Box<dyn Iterator<Item = (&Vec<u8>, &KeyDirEntry)>> =
                match (start_bound, end_bound) {
                    (Some(min), Some(max)) => Box::new(db_locked.keys_range(min, max)),
                    (Some(min), None) => Box::new(db_locked.keys_range_min(min)),
                    (None, Some(max)) => Box::new(db_locked.keys_range_max(max)),
                    (None, None) => Box::new(db_locked.iter()),
                };

            // 根据range的边界过滤
            for (key, entry) in iter {
                if range.contains(key) {
                    keys.push(key.clone());
                    entries.push(*entry);
                }
            }
        }

        Self {
            db,
            keys,
            entries,
            index: 0,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'a> Iterator for BitcaskEngineIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.keys.len() {
            return None;
        }

        let key = self.keys[self.index].clone();
        self.index += 1;

        let value = match self.db.lock() {
            Ok(db) => match db.read(&key) {
                Ok(v) => Ok(v),
                Err(e) => Err(e),
            },
            Err(e) => Err(Error::from(e)),
        };

        Some(value.map(|v| (key, v)))
    }
}

impl<'a> DoubleEndedIterator for BitcaskEngineIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index >= self.keys.len() {
            return None;
        }

        let last_idx = self.keys.len() - 1;
        let key = self.keys[last_idx].clone();

        self.keys.pop();
        self.entries.pop();

        let value = match self.db.lock() {
            Ok(db) => match db.read(&key) {
                Ok(v) => Ok(v),
                Err(e) => Err(e),
            },
            Err(e) => Err(Error::from(e)),
        };

        Some(value.map(|v| (key, v)))
    }
}

impl<'a> EngineIterator for BitcaskEngineIterator<'a> {}



#[cfg(test)]
mod tests {
    use super::BitcaskEngine;
    use crate::error::Result;
    use crate::storage::engine::Engine;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_bitcask_engine_basic() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("db.log");
        let mut engine = BitcaskEngine::new(path.clone())?;
        
        // 基本的写入和读取
        engine.set(b"test_key".to_vec(), b"test_value".to_vec())?;
        let value = engine.get(b"test_key".to_vec())?;
        assert_eq!(value, Some(b"test_value".to_vec()));
        
        // 不存在的键
        let missing = engine.get(b"not_exists".to_vec())?;
        assert_eq!(missing, None);
        
        // 删除数据
        engine.delete(b"test_key".to_vec())?;
        let after_delete = engine.get(b"test_key".to_vec())?;
        assert_eq!(after_delete, None);
        
        // 清理测试资源
        drop(engine);
        let _ = fs::remove_dir_all(dir.path());
        
        Ok(())
    }
    
    #[test]
    fn test_bitcask_engine_scan() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("db.log");
        let mut engine = BitcaskEngine::new(path.clone())?;
        
        // 添加一些测试数据
        engine.set(b"a1".to_vec(), b"value1".to_vec())?;
        engine.set(b"a2".to_vec(), b"value2".to_vec())?;
        engine.set(b"b1".to_vec(), b"value3".to_vec())?;
        engine.set(b"b2".to_vec(), b"value4".to_vec())?;
        engine.set(b"c1".to_vec(), b"value5".to_vec())?;
        
        // 测试范围扫描
        use std::ops::Bound;
        let start = Bound::Included(b"a1".to_vec());
        let end = Bound::Excluded(b"b2".to_vec());
        
        let mut results = Vec::new();
        for item in engine.scan((start, end)) {
            let (key, value) = item?;
            results.push((String::from_utf8(key)?, String::from_utf8(value)?));
        }
        
        results.sort();
        assert_eq!(
            results,
            vec![
                ("a1".to_string(), "value1".to_string()),
                ("a2".to_string(), "value2".to_string()),
                ("b1".to_string(), "value3".to_string()),
            ]
        );
        
        // 测试前缀扫描
        let mut prefix_results = Vec::new();
        for item in engine.scan_prefix(b"b".to_vec()) {
            let (key, value) = item?;
            prefix_results.push((String::from_utf8(key)?, String::from_utf8(value)?));
        }
        
        prefix_results.sort();
        assert_eq!(
            prefix_results,
            vec![
                ("b1".to_string(), "value3".to_string()),
                ("b2".to_string(), "value4".to_string()),
            ]
        );
        
        // 测试反向迭代
        let start = Bound::Included(b"a".to_vec());
        let end = Bound::Excluded(b"c".to_vec());
        
        let mut reverse_results = Vec::new();
        let mut iter = engine.scan((start, end));
        
        while let Some(item) = iter.next_back() {
            let (key, value) = item?;
            reverse_results.push((String::from_utf8(key)?, String::from_utf8(value)?));
        }
        
        assert_eq!(
            reverse_results,
            vec![
                ("b2".to_string(), "value4".to_string()),
                ("b1".to_string(), "value3".to_string()),
                ("a2".to_string(), "value2".to_string()),
                ("a1".to_string(), "value1".to_string()),
            ]
        );
        
        // 清理测试资源
        drop(engine);
        let _ = fs::remove_dir_all(dir.path());
        
        Ok(())
    }
} 
