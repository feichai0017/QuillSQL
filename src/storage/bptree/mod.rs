pub mod btree;
pub mod node;
pub mod node_type;
pub mod page;
pub mod page_layout;
pub mod pager;
pub mod wal;

// Re-export the core BTree structure
pub use btree::BTree;
use node_type::{Key, KeyValuePair}; // Import necessary types

// --- Engine Trait Implementation ---
use crate::error::{Error, Result}; // Assuming Result<T> = std::result::Result<T, crate::error::Error>
use crate::storage::engine::{Engine, EngineIterator};
use std::cell::RefCell;
use std::ops::{Bound, RangeBounds};
use std::path::Path; // <-- 导入 RefCell

use std::marker::PhantomData; // Using PhantomData until the real iterator exists
type ActualBTreeIterator<'a> = PhantomData<&'a KeyValuePair>;

/// Wrapper struct to implement the Engine trait for the B+ Tree.
pub struct BPlusTreeEngine {
    btree: RefCell<BTree>,
    path: &'static Path,
}

impl BPlusTreeEngine {
    /// Creates a new BPlusTreeEngine.
    /// Assumes BTreeBuilder exists and works as shown in btree.rs
    /// You might need to adjust parameters based on your BTreeBuilder.
    pub fn new(path: &'static Path, b_parameter: usize) -> Result<Self> {
        // Use the BTreeBuilder from btree.rs
        let btree = btree::BTreeBuilder::new()
            .path(path)
            .b_parameter(b_parameter) // Or use a default like 200
            .build()?; // Propagate BTree build errors
        Ok(Self {
            btree: RefCell::new(btree), // <-- 将 BTree 放入 RefCell
            path,
        })
    }
}

impl Engine for BPlusTreeEngine {
    type EngineIterator<'a> = BPlusTreeIterator<'a>;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let key_str = String::from_utf8(key).map_err(|_| Error::UTF8Error)?;
        let value_str = String::from_utf8(value).map_err(|_| Error::UTF8Error)?;
        let kv = KeyValuePair::new(key_str, value_str);
        self.btree.borrow_mut().insert(kv)
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let key_str = String::from_utf8(key).map_err(|_| Error::UTF8Error)?;
        // Revert to borrow_mut() for now, requires BTree::search signature fix later
        match self.btree.borrow_mut().search(key_str) {
            Ok(kv) => Ok(Some(kv.value.clone().into_bytes())),
            Err(Error::KeyNotFound) => Ok(None),
            Err(e) => Err(e), // Assuming Error is Clone or search returns owned Error
        }
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        let key_str = String::from_utf8(key).map_err(|_| Error::UTF8Error)?;
        let btree_key = Key(key_str);
        match self.btree.borrow_mut().delete(btree_key) {
            Ok(_) => Ok(()),
            Err(Error::KeyNotFound) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn scan(&mut self, _range: impl RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        // Revert to todo!() as BTree::scan is missing
        todo!("BTree::scan method needs to be implemented in btree.rs first");
        // The BPlusTreeScanIterator below also needs its `inner` field and methods implemented.
    }
}

// --- Iterator Implementation ---

/// Iterator for BPlusTreeEngine scan results.
pub struct BPlusTreeIterator<'a> {
    inner: PhantomData<&'a ()>,
}

impl<'a> BPlusTreeIterator<'a> {}

impl<'a> EngineIterator for BPlusTreeIterator<'a> {}

impl<'a> Iterator for BPlusTreeIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Revert to todo!() as the inner iterator logic is missing
        todo!("BPlusTreeScanIterator::next needs to be implemented based on BTree scan iterator");
    }
}

impl<'a> DoubleEndedIterator for BPlusTreeIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        // Revert to todo!() as the inner iterator logic is missing
        todo!(
            "BPlusTreeScanIterator::next_back needs to be implemented based on BTree scan iterator"
        );
    }
}