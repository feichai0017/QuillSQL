pub mod btree;
pub mod node;
pub mod node_type;
pub mod page;
pub mod page_layout;
pub mod wal;
pub mod disk_manager;
pub mod buffer_pool;
pub use btree::BTree;
use node_type::{Key, KeyValuePair}; // Import necessary types
use std::marker::PhantomData; // Use PhantomData for iterator lifetime
use std::vec;

use crate::error::{Error, Result};
use crate::storage::engine::{Engine, EngineIterator};
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Mutex;

/// Wrapper struct to implement the Engine trait for the B+ Tree.
pub struct BPlusTreeEngine {
    // Use Mutex for thread safety if Engine might be shared
    btree: Mutex<BTree>,
    _path: &'static Path, // path is needed by BTreeBuilder, store it if needed later
}

impl BPlusTreeEngine {
    /// Creates a new BPlusTreeEngine.
    pub fn new(path: &'static Path, b_parameter: usize) -> Result<Self> {
        let btree = crate::storage::bptree::btree::BTreeBuilder::new()
            .path(path)
            .b_parameter(b_parameter)
            .build()?;
        Ok(Self {
            btree: Mutex::new(btree),
            _path: path,
        })
    }
}

impl Engine for BPlusTreeEngine {
    type EngineIterator<'a> = BPlusTreeIterator<'a>;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let key_str = String::from_utf8(key).map_err(|_| Error::UTF8Error)?;
        let value_str = String::from_utf8(value).map_err(|_| Error::UTF8Error)?;
        let kv = KeyValuePair::new(key_str, value_str);
        // Assuming original BTree::insert takes &mut self
        self.btree
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?
            .insert(kv)
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let key_str = String::from_utf8(key).map_err(|_| Error::UTF8Error)?;
        // Assuming original BTree::search takes &mut self
        match self
            .btree
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?
            .search(key_str)
        {
            Ok(kv) => Ok(Some(kv.value.into_bytes())),
            Err(Error::KeyNotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        let key_str = String::from_utf8(key).map_err(|_| Error::UTF8Error)?;
        let btree_key = Key(key_str);
        // Assuming original BTree::delete takes &mut self
        match self
            .btree
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?
            .delete(btree_key)
        {
            Ok(_) => Ok(()),
            Err(Error::KeyNotFound) => Ok(()), // Deleting non-existent key is OK in Engine trait
            Err(e) => Err(e),
        }
    }

    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        // Helper to convert Bound<Vec<u8>> to Bound<String>
        let convert_bound = |bound: Bound<&Vec<u8>>| -> Result<Bound<String>> {
            match bound {
                Bound::Included(b) => String::from_utf8(b.clone())
                    .map(Bound::Included)
                    .map_err(|_| Error::UTF8Error),
                Bound::Excluded(b) => String::from_utf8(b.clone())
                    .map(Bound::Excluded)
                    .map_err(|_| Error::UTF8Error),
                Bound::Unbounded => Ok(Bound::Unbounded),
            }
        };

        let start_bound_res = convert_bound(range.start_bound());
        let end_bound_res = convert_bound(range.end_bound());

        let results = match (start_bound_res, end_bound_res) {
            (Ok(start), Ok(end)) => {
                // Lock BTree and call the new BTree::scan method
                self.btree
                    .lock()
                    .map_err(|e| Error::Internal(e.to_string())) // Handle Mutex poison error
                    .and_then(|mut guard| guard.scan((start, end))) // Call scan on the locked BTree
            }
            (Err(e), _) | (_, Err(e)) => Err(e), // Propagate UTF8 error from bound conversion
        };

        // Process the result of BTree::scan
        let iter = match results {
            Ok(kv_pairs) => {
                // Convert Vec<KeyValuePair> to iterator yielding Result<(Vec<u8>, Vec<u8>)>
                let items: Vec<Result<(Vec<u8>, Vec<u8>)>> = kv_pairs
                    .into_iter()
                    .map(|kv| Ok((kv.key.into_bytes(), kv.value.into_bytes())))
                    .collect();
                items.into_iter()
            }
            Err(e) => {
                // If BTree::scan failed, return an iterator yielding that single error
                vec![Err(e)].into_iter()
            }
        };

        BPlusTreeIterator {
            inner: iter,
            _phantom: PhantomData,
        }
    }
}

// --- Iterator Implementation ---

pub struct BPlusTreeIterator<'a> {
    // inner holds an iterator over the results collected by BTree::scan
    inner: vec::IntoIter<Result<(Vec<u8>, Vec<u8>)>>,
    // PhantomData to link the lifetime 'a from EngineIterator<'a>
    _phantom: PhantomData<&'a ()>,
}

impl<'a> EngineIterator for BPlusTreeIterator<'a> {}

impl<'a> Iterator for BPlusTreeIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<'a> DoubleEndedIterator for BPlusTreeIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back()
    }
}

#[cfg(test)]
mod tests {
    use super::{BPlusTreeEngine, Engine, Error, Result}; // Import from parent module
    use std::path::Path;
    use tempfile::tempdir;

    // Helper to setup a BPlusTreeEngine in a temp dir
    fn setup_engine(b: usize) -> Result<(tempfile::TempDir, BPlusTreeEngine)> {
        let dir = tempdir().map_err(|e| Error::Io(e.to_string()))?;
        let file_path = dir.path().join("test_bptree_engine.db");
        // Leak the path to get a 'static lifetime
        let path: &'static Path = Box::leak(file_path.into_boxed_path());
        // Use BPlusTreeEngine::new directly
        let engine = BPlusTreeEngine::new(path, b)?;
        Ok((dir, engine))
    }

    // --- Tests adapted from src/storage/engine.rs ---

    #[test]
    fn test_bptree_engine_point_operations() -> Result<()> {
        let (_dir, mut engine) = setup_engine(2)?;

        // Get non-existent key
        assert_eq!(engine.get(b"aa".to_vec())?, None);

        // Set and get
        engine.set(b"aa".to_vec(), vec![1, 2])?;
        assert_eq!(engine.get(b"aa".to_vec())?, Some(vec![1, 2]));
        engine.set(b"bb".to_vec(), vec![3, 4])?;
        assert_eq!(engine.get(b"bb".to_vec())?, Some(vec![3, 4]));
        assert_eq!(engine.get(b"aa".to_vec())?, Some(vec![1, 2])); // Check aa again

        // Overwrite
        engine.set(b"aa".to_vec(), vec![5, 6])?;
        assert_eq!(engine.get(b"aa".to_vec())?, Some(vec![5, 6]));

        // Delete bb
        engine.delete(b"bb".to_vec())?;
        assert_eq!(engine.get(b"bb".to_vec())?, None);
        assert_eq!(engine.get(b"aa".to_vec())?, Some(vec![5, 6])); // Check aa remains

        // Delete aa
        engine.delete(b"aa".to_vec())?;
        assert_eq!(engine.get(b"aa".to_vec())?, None);

        // Delete non-existent key
        engine.delete(b"cc".to_vec())?;
        assert_eq!(engine.get(b"cc".to_vec())?, None);

        Ok(())
    }

    #[test]
    fn test_bptree_engine_scan_basic() -> Result<()> {
        let (_dir, mut engine) = setup_engine(2)?;
        engine.set(b"a".to_vec(), b"1".to_vec())?;
        engine.set(b"b".to_vec(), b"2".to_vec())?;
        engine.set(b"c".to_vec(), b"3".to_vec())?;
        engine.set(b"d".to_vec(), b"4".to_vec())?;

        // Full scan
        let mut iter_all = engine.scan(..);
        assert_eq!(
            iter_all.next().transpose()?,
            Some((b"a".to_vec(), b"1".to_vec()))
        );
        assert_eq!(
            iter_all.next().transpose()?,
            Some((b"b".to_vec(), b"2".to_vec()))
        );
        assert_eq!(
            iter_all.next().transpose()?,
            Some((b"c".to_vec(), b"3".to_vec()))
        );
        assert_eq!(
            iter_all.next().transpose()?,
            Some((b"d".to_vec(), b"4".to_vec()))
        );
        assert_eq!(iter_all.next().transpose()?, None);
        drop(iter_all); // Explicitly drop iterator

        // Partial scan (inclusive)
        let mut iter_partial = engine.scan(b"b".to_vec()..=b"c".to_vec());
        assert_eq!(
            iter_partial.next().transpose()?,
            Some((b"b".to_vec(), b"2".to_vec()))
        );
        assert_eq!(
            iter_partial.next().transpose()?,
            Some((b"c".to_vec(), b"3".to_vec()))
        );
        assert_eq!(iter_partial.next().transpose()?, None);
        drop(iter_partial);

        // Scan prefix
        let mut iter_prefix = engine.scan_prefix(b"c".to_vec());
        assert_eq!(
            iter_prefix.next().transpose()?,
            Some((b"c".to_vec(), b"3".to_vec()))
        );
        assert_eq!(iter_prefix.next().transpose()?, None);
        drop(iter_prefix);

        // Reverse scan
        let mut iter_rev = engine.scan(..).rev();
        assert_eq!(
            iter_rev.next().transpose()?,
            Some((b"d".to_vec(), b"4".to_vec()))
        );
        assert_eq!(
            iter_rev.next().transpose()?,
            Some((b"c".to_vec(), b"3".to_vec()))
        );
        assert_eq!(
            iter_rev.next().transpose()?,
            Some((b"b".to_vec(), b"2".to_vec()))
        );
        assert_eq!(
            iter_rev.next().transpose()?,
            Some((b"a".to_vec(), b"1".to_vec()))
        );
        assert_eq!(iter_rev.next().transpose()?, None);

        Ok(())
    }

    // TODO: Add more sophisticated scan tests (different ranges, empty results etc.)
}
