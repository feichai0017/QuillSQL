use crate::{catalog::SchemaRef, error::QuillSQLResult, storage::page::RecordId, storage::tuple::Tuple};

pub mod bplus_index;
pub mod bz_index;



/// A generic trait for database indexes.
/// This allows for different underlying implementations (e.g., B+Tree, BzTree).
pub trait Index: std::fmt::Debug + Send + Sync {
    /// Inserts a key-value pair into the index.
    fn insert(&self, key: &Tuple, value: RecordId) -> QuillSQLResult<()>;

    /// Retrieves the value associated with a key.
    fn get(&self, key: &Tuple) -> QuillSQLResult<Option<RecordId>>;
    
    /// Deletes a key-value pair from the index.
    fn delete(&self, key: &Tuple) -> QuillSQLResult<()>;

    /// Returns the schema of the keys in the index.
    fn key_schema(&self) -> &SchemaRef;
}


mod test {
    use std::{sync::Arc, thread, time::Instant};

    use rand::{seq::IndexedRandom, Rng, thread_rng};
    use tempfile::TempDir;

    use crate::{buffer::BufferPoolManager, catalog::{Column, DataType, Schema, SchemaRef}, error::QuillSQLResult, storage::{disk_manager::DiskManager, disk_scheduler::DiskScheduler, index::{Index, bplus_index::BPlusTreeIndex, bz_index::BzTreeIndex}, page::RecordId, tuple::Tuple}};

    #[test]
    //#[ignore]
    fn benchmark_index_implementations() {
        const NUM_KEYS: i32 = 50_000; // 使用更大的数据集
        const NUM_THREADS: usize = 16;
        
        let key_schema = Arc::new(Schema::new(vec![Column::new("a", DataType::Int32, false)]));

        // --- BPlusTreeIndex (并发版本) 设置 ---
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test_bplus.db");
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferPoolManager::new(2000, disk_scheduler));
        let bplus_tree_index = Arc::new(BPlusTreeIndex::new(key_schema.clone(), buffer_pool, 128, 128));

        // --- BzTreeIndex 设置 ---
        let bz_tree_index = Arc::new(BzTreeIndex::new(key_schema.clone()));

        // 预生成所有 keys
        let all_keys: Arc<Vec<Tuple>> = Arc::new((0..NUM_KEYS).map(|i| {
            Tuple::new(key_schema.clone(), vec![i.into()])
        }).collect());

        println!("\n--- Index Performance Benchmark ---");
        println!("Dataset: {} keys, Threads: {}", NUM_KEYS, NUM_THREADS);

        // --- 性能测试函数 ---
        fn run_benchmark(
            name: &str,
            index: Arc<dyn Index + Send + Sync>,
            keys: Arc<Vec<Tuple>>,
        ) {
            // 并发插入
            let start_time = Instant::now();
            let mut handles = vec![];
            let keys_per_thread = keys.len() / NUM_THREADS;

            for i in 0..NUM_THREADS {
                let index_clone = Arc::clone(&index);
                let thread_keys: Vec<Tuple> = keys.iter()
                    .skip(i * keys_per_thread)
                    .take(keys_per_thread)
                    .cloned()
                    .collect();
                
                let handle = thread::spawn(move || {
                    for key in thread_keys {
                        let i = match key.value(0).unwrap() {
                            crate::utils::scalar::ScalarValue::Int32(Some(v)) => *v,
                            _ => unreachable!(),
                        };
                        let rid = RecordId::new(i as u32, i as u32);
                        index_clone.insert(&key, rid).unwrap();
                    }
                });
                handles.push(handle);
            }
            for handle in handles {
                handle.join().unwrap();
            }
            let insert_duration = start_time.elapsed();
            println!("[{}] Concurrent Insert Time: {:?}", name, insert_duration);

            // 并发读取
            let start_time = Instant::now();
            let mut handles = vec![];
            for _ in 0..NUM_THREADS {
                let index_clone = Arc::clone(&index);
                let keys_clone = Arc::clone(&keys);
                
                let handle = thread::spawn(move || {
                    let mut rng = rand::rng();
                    for _ in 0..keys_per_thread {
                        let key = keys_clone.choose(&mut rng).unwrap();
                        index_clone.get(key).unwrap();
                    }
                });
                handles.push(handle);
            }
            for handle in handles {
                handle.join().unwrap();
            }
            let read_duration = start_time.elapsed();
            println!("[{}] Concurrent Read Time:   {:?}", name, read_duration);
        }
        
        // 运行两种实现的基准测试
        run_benchmark("BPlusTreeIndex (Lock-based)", bplus_tree_index, all_keys.clone());
        run_benchmark("BzTreeIndex (Latch-free)", bz_tree_index, all_keys.clone());
    }
}