pub mod b_plus_tree_index;
pub mod buffer_pool_manager;
pub mod codec;
pub mod comparator;
pub mod disk;
pub mod page;
pub mod table_heap;

use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::error::{Error, Result};
use crate::storage::b_plus_tree::b_plus_tree_index::KeyComparator;
use crate::storage::b_plus_tree::b_plus_tree_index::{BPlusTreeIndex, TreeIndexIterator};
use crate::storage::b_plus_tree::buffer_pool_manager::BufferPoolManager;
use crate::storage::b_plus_tree::disk::disk_manager::DiskManager;
use crate::storage::b_plus_tree::disk::disk_scheduler::DiskScheduler;
use crate::storage::b_plus_tree::page::table_page::EMPTY_TUPLE_META;
use crate::storage::b_plus_tree::table_heap::TableHeap;
use crate::storage::engine::{Engine, EngineIterator};
use b_plus_tree_index::default_comparator;

/// B+树存储引擎，实现Engine trait
pub struct BPlusTreeEngine {
    // Index mapping Key -> RecordId
    index: RwLock<Arc<BPlusTreeIndex>>,
    comparator: KeyComparator,
    // Heap managing RecordId -> Vec<u8> (data)
    table_heap: Arc<TableHeap>,
}

impl BPlusTreeEngine {
    /// 创建一个新的BPlusTreeEngine
    pub fn new(path: &'static Path, b_parameter: usize) -> Result<Self> {
        // 创建磁盘管理器
        let disk_manager = Arc::new(DiskManager::try_new(path.to_path_buf())?);
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));

        // 创建缓冲池
        let buffer_pool = Arc::new(BufferPoolManager::new(1000, 2, disk_scheduler)?);

        // 创建 TableHeap
        let table_heap = Arc::new(TableHeap::try_new(buffer_pool.clone())?);

        // 创建B+树索引 (still needs buffer_pool)
        let index = Arc::new(BPlusTreeIndex::new(
            default_comparator,
            buffer_pool.clone(), // Index needs buffer pool too
            b_parameter as u32,  // 内部节点大小
            b_parameter as u32,  // 叶子节点大小
        ));

        Ok(Self {
            index: RwLock::new(index),
            comparator: default_comparator,
            table_heap,  // Store Arc<TableHeap>
        })
    }
}

impl Engine for BPlusTreeEngine {
    type EngineIterator<'a> = BPlusTreeIterator<'a>;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // Ensure overwrite semantics: delete existing key first, ignore error if not found.
        // This delete call now uses the TableHeap internally to mark the old tuple.
        match self.delete(key.clone()) {
            Ok(_) => println!("DEBUG: Overwriting key, previous entry deleted/marked."),
            // Adjust error checking if delete returns specific not found errors
            Err(Error::Internal(ref msg)) if msg.contains("not found") => {
                println!("DEBUG: Inserting new key (delete reported not found).");
                // Key not found during delete is expected for new inserts, ignore.
            }
            Err(e) => return Err(e), // Propagate other unexpected errors from delete
        };

        // 1. Insert data into the TableHeap to get the RecordId
        // For now, use empty meta. In a transactional system, this would hold transaction IDs.
        let record_id = self.table_heap.insert_data(&EMPTY_TUPLE_META, &value)?;

        // 2. Lock the index
        let index_guard = self
            .index
            .write()
            .map_err(|e| Error::Internal(e.to_string()))?;

        // 3. Insert the (key, record_id) into the B+Tree index
        index_guard.insert(&key, record_id)?;

        Ok(())
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let index_guard = self
            .index
            .read()
            .map_err(|e| Error::Internal(e.to_string()))?;

        match index_guard.get(&key)? {
            Some(record_id) => {
                // Use TableHeap to get the full data (meta + value)
                match self.table_heap.get_full_data(record_id) {
                    Ok((meta, data)) => {
                        if meta.is_deleted {
                            Ok(None) // Found but marked deleted
                        } else {
                            Ok(Some(data)) // Found and valid
                        }
                    }
                    Err(e) => Err(e), // Error fetching from TableHeap
                }
            }
            None => Ok(None), // Key not found in index
        }
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        let index_guard = self
            .index
            .write()
            .map_err(|e| Error::Internal(e.to_string()))?;

        // 1. Find the RecordId associated with the key
        let record_id_option = index_guard.get(&key)?;

        if let Some(record_id) = record_id_option {
            // 2. Delete the key from the index *first* (idempotent)
            index_guard.delete(&key)?;

            // 3. Mark the tuple in the TableHeap as deleted
            match self.table_heap.tuple_meta(record_id) {
                Ok(mut meta) => {
                    if !meta.is_deleted {
                        meta.is_deleted = true;
                        // TODO: Set delete_txn_id in a real transactional context
                        if let Err(e) = self.table_heap.update_tuple_meta(meta, record_id) {
                            eprintln!(
                                "ERROR: Failed to update tuple meta for RID {:?}: {:?}",
                                record_id, e
                            );
                            // Potentially return error here, or just log and continue?
                            // For now, log and continue, index entry is already deleted.
                            return Err(e); // Let's return the error for now
                        } else {
                            println!(
                                "DEBUG: Marked tuple deleted in TableHeap for RID {:?}",
                                record_id
                            );
                        }
                    }
                    // Already deleted, nothing to do in TableHeap
                }
                Err(e) => {
                    eprintln!(
                        "ERROR: Failed to get tuple meta for RID {:?} during delete: {:?}",
                        record_id, e
                    );
                    // If we can't get meta, we can't mark deleted.
                    // Index entry is already removed. Return error.
                    return Err(e);
                }
            }
        } else {
            // Key not found in index, maybe return a specific error?
            return Err(Error::Internal(format!(
                "Key not found for deletion: {:?}",
                key
            )));
        }

        Ok(())
    }

    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        // Clone bounds directly as they are Vec<u8>
        let start_bound = range.start_bound().cloned();
        let end_bound = range.end_bound().cloned();

        // 创建B+树迭代器
        let index_arc = match self.index.read() {
            Ok(index) => index.clone(),
            Err(_) => {
                // Fallback/Error handling: Create a temporary index instance
                // This part might need better error handling depending on requirements
                let temp_path: &'static Path = Box::leak(
                    Path::new("/tmp/fallback.db")
                        .to_path_buf()
                        .into_boxed_path(),
                );
                Arc::new(BPlusTreeIndex::new(
                    self.comparator, // Use existing comparator
                    Arc::new(
                        BufferPoolManager::new(
                            100, // Smaller pool for fallback
                            2,
                            Arc::new(DiskScheduler::new(Arc::new(
                                DiskManager::try_new(temp_path.to_path_buf()).unwrap(), // Handle error better
                            ))),
                        )
                        .unwrap(), // Handle error better
                    ),
                    4, // Default size
                    4, // Default size
                ))
            }
        };

        let tree_iter = TreeIndexIterator::new(index_arc, (start_bound, end_bound));

        BPlusTreeIterator {
            inner: tree_iter,
            engine: self, // Pass mutable reference to engine
            _phantom: PhantomData,
        }
    }
}

// B+树迭代器实现
pub struct BPlusTreeIterator<'a> {
    inner: TreeIndexIterator,
    engine: &'a BPlusTreeEngine, // Store reference to engine to call get_value_from_record
    _phantom: PhantomData<&'a ()>,
}

impl<'a> EngineIterator for BPlusTreeIterator<'a> {}

impl<'a> Iterator for BPlusTreeIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Loop to skip deleted tuples
            // 获取下一个结果 (key, record_id) from the underlying index iterator
            match self.inner.next() {
                Ok(Some((key, record_id))) => {
                    // 使用 TableHeap 获取数据和元数据
                    match self.engine.table_heap.get_full_data(record_id) {
                        Ok((meta, value)) => {
                            // Found a tuple, check if deleted
                            if meta.is_deleted {
                                println!("DEBUG: Iterator skipped deleted RID {:?}", record_id);
                                continue; // Skip deleted tuple
                            } else {
                                return Some(Ok((key, value))); // Return valid tuple
                            }
                        }
                        Err(e) => {
                            // Error fetching value from TableHeap
                            eprintln!(
                                "ERROR: Iterator failed to get full data for RID {:?}: {:?}",
                                record_id, e
                            );
                            return Some(Err(e));
                        }
                    }
                }
                Ok(None) => {
                    // End of iteration from the index iterator
                    return None;
                }
                Err(e) => {
                    // Error from the index iterator itself
                    return Some(Err(e));
                }
            }
        }
    }
}

impl<'a> DoubleEndedIterator for BPlusTreeIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        // B+树迭代器目前不支持反向迭代，返回None
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{BPlusTreeEngine, Engine, Error, Result};
    use std::path::Path;
    use tempfile::tempdir;

    // 创建测试引擎
    fn setup_engine(b: usize) -> Result<(tempfile::TempDir, BPlusTreeEngine)> {
        let dir = tempdir().map_err(|e| Error::Io(e.to_string()))?;
        let file_path = dir.path().join("test_bptree_engine.db");
        // 使用静态生命周期
        let path: &'static Path = Box::leak(file_path.into_boxed_path());

        let engine = BPlusTreeEngine::new(path, b)?;
        Ok((dir, engine))
    }

    #[test]
    fn test_bptree_engine_point_operations() -> Result<()> {
        let (_dir, mut engine) = setup_engine(4)?;

        // 获取不存在的键
        assert_eq!(engine.get(b"aa".to_vec())?, None);

        // 设置和获取
        engine.set(b"aa".to_vec(), b"val_aa".to_vec())?;
        // 获取键，检查是否存在，并且现在应该可以验证值的精确匹配了
        assert_eq!(engine.get(b"aa".to_vec())?, Some(b"val_aa".to_vec())); // Uncommented value check

        // 删除键
        engine.delete(b"aa".to_vec())?;
        assert_eq!(engine.get(b"aa".to_vec())?, None);

        Ok(())
    }

    #[test]
    fn test_bptree_engine_overwrite() -> Result<()> {
        let (_dir, mut engine) = setup_engine(4)?;

        engine.set(b"overwrite_key".to_vec(), b"value_initial".to_vec())?;
        assert_eq!(
            engine.get(b"overwrite_key".to_vec())?,
            Some(b"value_initial".to_vec())
        );

        engine.set(b"overwrite_key".to_vec(), b"value_new".to_vec())?;
        assert_eq!(
            engine.get(b"overwrite_key".to_vec())?,
            Some(b"value_new".to_vec())
        );

        Ok(())
    }

    #[test]
    fn test_bptree_engine_scan_with_delete() -> Result<()> {
        let (_dir, mut engine) = setup_engine(4)?;

        engine.set(b"scan_del_1".to_vec(), b"val1".to_vec())?;
        engine.set(b"scan_del_2".to_vec(), b"val2".to_vec())?;
        engine.set(b"scan_del_3".to_vec(), b"val3".to_vec())?;

        engine.delete(b"scan_del_2".to_vec())?;

        // Verify get returns None for deleted key
        assert_eq!(engine.get(b"scan_del_2".to_vec())?, None);

        let range = b"scan_del_1".to_vec()..=b"scan_del_3".to_vec();
        let mut results = vec![];
        let mut iter = engine.scan(range);
        while let Some(result) = iter.next() {
            results.push(result?);
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (b"scan_del_1".to_vec(), b"val1".to_vec()));
        assert_eq!(results[1], (b"scan_del_3".to_vec(), b"val3".to_vec()));

        Ok(())
    }
}
