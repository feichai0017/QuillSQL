# QuillSQL 并发控制示例

本文档展示 QuillSQL 数据库的并发控制机制和使用示例。

## 基础事务管理示例

```rust
use quill_sql::transaction::{TransactionManager, IsolationLevel};
use quill_sql::error::QuillSQLResult;

fn basic_transaction_example() -> QuillSQLResult<()> {
    let txn_manager = TransactionManager::new();
    
    // 开始一个新事务
    let txn = txn_manager.begin(IsolationLevel::ReadUncommitted)?;
    println!("Started transaction with ID: {}", txn.txn_id);
    
    // 执行一些数据库操作...
    // (这里需要结合其他组件实现具体的SQL操作)
    
    // 提交事务
    txn_manager.commit(txn)?;
    println!("Transaction committed successfully");
    
    Ok(())
}
```

## 缓冲池并发访问示例

```rust
use quill_sql::buffer::BufferPoolManager;
use quill_sql::storage::disk_scheduler::DiskScheduler;
use quill_sql::storage::disk_manager::DiskManager;
use std::sync::Arc;
use std::thread;

fn buffer_pool_concurrency_example() -> QuillSQLResult<()> {
    // 创建磁盘管理器和调度器
    let disk_manager = Arc::new(DiskManager::try_new("test.db")?);
    let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager.clone()));
    let buffer_pool = Arc::new(BufferPoolManager::new(1000, disk_scheduler));
    
    // 创建多个线程并发访问缓冲池
    let mut handles = vec![];
    
    for i in 0..4 {
        let bp = buffer_pool.clone();
        let handle = thread::spawn(move || {
            // 创建新页面
            if let Ok(mut page_guard) = bp.new_page() {
                println!("Thread {} created page {}", i, page_guard.page_id());
                
                // 写入一些数据
                page_guard.data[0] = i as u8;
                
                // 页面会在 guard 被 drop 时自动解锁和 unpin
            }
        });
        handles.push(handle);
    }
    
    // 等待所有线程完成
    for handle in handles {
        handle.join().unwrap();
    }
    
    Ok(())
}
```

## B+ Tree 索引并发操作示例

```rust
use quill_sql::storage::index::btree_index::BPlusTreeIndex;
use quill_sql::storage::tuple::Tuple;
use quill_sql::catalog::Schema;
use std::sync::Arc;
use std::thread;

fn btree_concurrency_example() -> QuillSQLResult<()> {
    // 创建索引 (需要适当的 schema 和 buffer pool)
    // let schema = Schema::new(...);
    // let index = BPlusTreeIndex::new(...);
    
    // 创建多个线程并发插入
    let mut handles = vec![];
    
    for i in 0..10 {
        // let index_clone = index.clone();
        let handle = thread::spawn(move || {
            // 创建测试数据
            // let key = Tuple::new(...);
            // let value = RecordId::new(...);
            
            // 并发插入
            // index_clone.insert(&key, value);
            
            println!("Thread {} completed insert operation", i);
        });
        handles.push(handle);
    }
    
    // 等待所有插入完成
    for handle in handles {
        handle.join().unwrap();
    }
    
    Ok(())
}
```

## 并发控制关键特性

### 1. 页面级锁定

```rust
// 读取页面（共享锁）
let read_guard = buffer_pool.fetch_page_read(page_id)?;
// 可以有多个线程同时持有读锁

// 写入页面（排他锁）
let write_guard = buffer_pool.fetch_page_write(page_id)?;
// 只能有一个线程持有写锁
```

### 2. 原子操作优化

```rust
// 页面的 pin_count 使用原子操作，避免锁竞争
let pin_count = page.get_pin_count(); // 原子读取
page.pin(); // 原子增加
page.unpin(); // 原子减少
```

### 3. RAII 自动资源管理

```rust
{
    let page_guard = buffer_pool.fetch_page_write(page_id)?;
    // 页面自动被锁定和 pin
    
    // 在这里进行页面操作...
    
} // page_guard 在这里自动 drop，释放锁和 unpin 页面
```

### 4. 死锁避免策略

QuillSQL 使用以下策略避免死锁：

1. **有序锁获取**: B+ 树操作按照从上到下的顺序获取锁
2. **蟹行协议**: 在安全节点释放祖先节点的锁
3. **超时机制**: 锁等待超时自动回滚
4. **死锁检测**: 周期性检测并解决死锁

## 性能优化技术

### 1. 无锁数据结构

```rust
// 使用 DashMap 实现无锁页面映射
pub(crate) page_table: Arc<DashMap<PageId, FrameId>>,

// 原子操作避免锁竞争
pub pin_count: AtomicU32,
```

### 2. 细粒度锁定

- 页面级别的独立锁，而不是全局锁
- 读写锁分离，支持并发读取
- 锁的范围最小化

### 3. 异步 I/O

```rust
// 非阻塞磁盘操作
let rx = disk_scheduler.schedule_read(page_id)?;
let data = rx.recv()?; // 在后台线程完成I/O
```

## 监控和调试

### 1. 事务状态监控

```rust
let active_count = txn_manager.active_transaction_count()?;
println!("Active transactions: {}", active_count);

let is_active = txn_manager.is_transaction_active(txn_id)?;
```

### 2. 缓冲池统计

```rust
// 可以添加统计信息收集
// buffer_pool.get_hit_ratio();
// buffer_pool.get_eviction_count();
```

## 最佳实践

1. **尽早释放锁**: 使用 RAII guard 确保锁自动释放
2. **避免长事务**: 减少锁持有时间
3. **合理选择隔离级别**: 根据需求平衡一致性和性能
4. **监控并发性能**: 定期检查锁竞争和死锁情况

## 结论

QuillSQL 的并发控制设计体现了现代数据库系统的最佳实践：

- 使用 Rust 的类型系统保证内存安全
- 细粒度锁定减少竞争
- 无锁数据结构提升性能
- RAII 模式简化资源管理
- 异步 I/O 提高吞吐量

这为构建高性能、安全的关系型数据库奠定了坚实的基础。