use crate::catalog::SchemaRef;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::index::Index;
use crate::storage::page::RecordId;
use crate::storage::tuple::Tuple;
use bztree::BzTree;
use crossbeam_epoch as epoch;
use std::fmt;

pub struct BzTreeIndex {
    /// The underlying BzTree instance.
    tree: BzTree<Tuple, RecordId>,
    /// The schema of the keys.
    schema: SchemaRef,
}

impl BzTreeIndex {
    pub fn new(key_schema: SchemaRef) -> Self {
        Self {
            tree: BzTree::new(),
            schema: key_schema,
        }
    }
}

impl Index for BzTreeIndex {
    fn insert(&self, key: &Tuple, value: RecordId) -> QuillSQLResult<()> {
        // 1. Pin the current thread's epoch guard. This is required by bztree.
        let guard = &epoch::pin();
        
        // 2. BzTree's insert returns false if the key already exists.
        //    We can treat this as an error or handle it as needed.
        let success = self.tree.insert(key.clone(), value, guard);
        if success {
            Ok(())
        } else {
            Err(QuillSQLError::Storage(format!(
                "Failed to insert duplicate key into BzTreeIndex: {:?}",
                key
            )))
        }
    }

    fn get(&self, key: &Tuple) -> QuillSQLResult<Option<RecordId>> {
        let guard = &epoch::pin();
        
        // `get` returns a guarded pointer to the value. We need to dereference and clone it.
        let result = self.tree.get(key, guard).map(|v| *v);
        Ok(result)
    }

    fn delete(&self, key: &Tuple) -> QuillSQLResult<()> {
        let guard = &epoch::pin();

        // `delete` returns the deleted value. We don't need it here.
        self.tree.delete(key, guard);
        Ok(())
    }

    fn key_schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl fmt::Debug for BzTreeIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BzTreeIndex")
         .field("schema", &self.schema)
         .field("tree", &"<BzTree instance>") 
         .finish()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Column, DataType, Schema};
    use crate::index::Index;
    use crate::storage::tuple::Tuple;
    use rand::seq::{IndexedRandom, SliceRandom};
    use rand::Rng;
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    #[test]
    fn test_bztree_index_basic_operations() {
        let key_schema = Arc::new(Schema::new(vec![Column::new("a", DataType::Int32, false)]));
        let index = BzTreeIndex::new(key_schema.clone());

        let key1 = Tuple::new(key_schema.clone(), vec![10i32.into()]);
        let rid1 = RecordId::new(1, 1);
        let key2 = Tuple::new(key_schema.clone(), vec![20i32.into()]);
        let rid2 = RecordId::new(2, 2);

        // 1. 测试 Insert 和 Get
        assert!(index.insert(&key1, rid1).is_ok(), "Insert key1 failed");
        assert!(index.insert(&key2, rid2).is_ok(), "Insert key2 failed");

        let retrieved_rid1 = index.get(&key1).expect("Get key1 failed");
        assert_eq!(retrieved_rid1, Some(rid1), "Incorrect RID for key1");

        let retrieved_rid2 = index.get(&key2).expect("Get key2 failed");
        assert_eq!(retrieved_rid2, Some(rid2), "Incorrect RID for key2");

        // 2. 测试重复插入
        // bztree::insert 返回 false 如果 key 已存在, 我们的适配器应该将其转为错误
        assert!(index.insert(&key1, rid1).is_err(), "Should fail on duplicate insert");

        // 3. 测试 Get 不存在的 Key
        let key3 = Tuple::new(key_schema.clone(), vec![30i32.into()]);
        let retrieved_rid3 = index.get(&key3).expect("Get key3 failed");
        assert_eq!(retrieved_rid3, None, "Found a non-existent key");

        // 4. 测试 Delete
        index.delete(&key1).expect("Delete key1 failed");
        let retrieved_rid1_after_delete = index.get(&key1).expect("Get key1 after delete failed");
        assert_eq!(retrieved_rid1_after_delete, None, "Key1 should be deleted");

        // 确保其他 key 还在
        let retrieved_rid2_after_delete = index.get(&key2).expect("Get key2 after key1 delete failed");
        assert_eq!(retrieved_rid2_after_delete, Some(rid2), "Key2 should still exist");

        println!("BzTreeIndex basic operations test passed!");
    }

    #[test]
    fn test_bztree_index_high_concurrency() {
        const NUM_KEYS: i32 = 2000;
        const NUM_THREADS: usize = 16;
        const OPS_PER_THREAD: usize = 1000;

        let key_schema = Arc::new(Schema::new(vec![Column::new("a", DataType::Int32, false)]));
        // 将 BzTreeIndex 包装在 Arc 中以便在线程间共享
        let index = Arc::new(BzTreeIndex::new(key_schema.clone()));

        // 预先生成所有可能的 keys
        let all_keys: Arc<Vec<Tuple>> = Arc::new((0..NUM_KEYS).map(|i| {
            Tuple::new(key_schema.clone(), vec![i.into()])
        }).collect());

        let mut handles = vec![];

        for thread_id in 0..NUM_THREADS {
            let index_clone = Arc::clone(&index);
            let keys_clone = Arc::clone(&all_keys);

            let handle = thread::spawn(move || {
                let mut rng = rand::rng();
                for op_idx in 0..OPS_PER_THREAD {
                    // 随机选择一个 key 进行操作
                    let key = keys_clone.choose(&mut rng).unwrap();
                    let i = match key.value(0).unwrap() {
                        crate::utils::scalar::ScalarValue::Int32(Some(v)) => *v,
                        _ => unreachable!(),
                    };
                    let rid = RecordId::new(i as u32, i as u32);

                    // 随机选择操作类型：50% 读, 30% 写, 20% 删
                    let op_type = rng.random_range(0..10);

                    if op_type < 5 { // GET (50% chance)
                        let res = index_clone.get(key);
                        assert!(res.is_ok(), "Thread {} get failed: {:?}", thread_id, res.err());
                    } else if op_type < 8 { // INSERT (30% chance)
                        // insert 可能会因为 key 已存在而失败，这是正常的，所以我们不 panic
                        let _ = index_clone.insert(key, rid);
                    } else { // DELETE (20% chance)
                        let res = index_clone.delete(key);
                        assert!(res.is_ok(), "Thread {} delete failed: {:?}", thread_id, res.err());
                    }
                    
                    // 为了增加线程交错的可能性，偶尔让出 CPU
                    if op_idx % 100 == 0 {
                        thread::yield_now();
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("A worker thread panicked!");
        }

        println!("BzTreeIndex high concurrency test passed with {} threads and {} ops/thread.", NUM_THREADS, OPS_PER_THREAD);
    }
}