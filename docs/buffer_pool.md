# Buffer Pool Manager — Architecture, Features, Optimizations

## 1. Architecture Overview

The Buffer Pool Manager (BPM) is responsible for managing pages in memory, acting as a cache between the disk and the execution engine. It fetches pages from disk into memory frames, allows threads to "pin" them for safe access, and writes dirty pages back to disk.

### 1.1 Core Components

```
+-----------------------------------------------------------------+
|                        Buffer Pool Manager                        |
|                                                                 |
|  +-----------------+   +------------------+   +----------------+  |
|  |   Page Table    |   |    Replacer      |   |   Free List    |  |
|  | (DashMap<P,F>)  |<->| (Sharded LRU-K)  |<->| (VecDeque<F>)  |  |
|  +-----------------+   +------------------+   +----------------+  |
|        ^   |                      ^                               |
|        |   |                      |                               |
|        |   v                      |                               |
|  +------------------------------------------------------------+  |
|  |                         Frame Pool                         |  |
|  |      (Vec<Arc<RwLock<Page>>>)                             |  |
|  |  [ Frame 0 ] [ Frame 1 ] ... [ Frame N ]                   |  |
|  +------------------------------------------------------------+  |
|        ^   |                      ^   |                         |
|        |   | (pins/unpins)        |   | (page data)             |
|        |   v                      |   v                         |
|  +--------------------+   +---------------------+   +---------------------+
|  |   Client Thread    |   |  Background Cleaner |   |    Disk Scheduler   |
|  | (e.g., Executor)   |   | (Optional Thread)   |   |  (I/O Background    |
|  | - fetch_page_*()   |<->| - Flushes dirty     |<->|    Thread)          |
|  | - new_page()       |   | - Ages replacer     |   | - read_page()       |
|  | - unpin_page()     |   +---------------------+   | - write_page()      |
|  | - new_page()       |   |                       |   | - write_page()      |
|  +--------------------+                             +---------------------+
|                                                           |   ^
|                                                           v   |
|                                                     +----------------+
|                                                     |   Disk File    |
|                                                     +----------------+
+-----------------------------------------------------------------+
```

-   **Frame Pool (`pool`)**: A `Vec<Arc<RwLock<Page>>>` representing the main memory area managed by the BPM. Each `Page` is protected by its own `RwLock` for fine-grained concurrent access.
-   **Page Table (`page_table`)**: A `DashMap<PageId, FrameId>` for efficiently mapping logical page IDs to their physical frame locations in the pool. `DashMap` provides high-performance concurrent lookups, insertions, and removals.
-   **Replacer (`replacer`)**: A `ShardedLRUKReplacer` that implements the page replacement policy. When a new page needs to be brought into memory and no frames are free, the replacer selects a victim frame to evict. The sharded design reduces lock contention on this critical data structure.
-   **Free List (`free_list`)**: A `VecDeque<FrameId>` that tracks available frames, allowing for quick allocation without needing to consult the replacer.
-   **Disk Scheduler (`disk_scheduler`)**: An asynchronous I/O backend. The BPM offloads all disk operations (read, write, allocate) to a dedicated background thread managed by the `DiskScheduler`. This prevents client threads from blocking on slow disk I/O. Communication happens via message-passing channels.

### 1.2 Pin Protocol & Page Guards

To ensure safe memory access, the BPM uses a pin/unpin protocol. A page is "pinned" when a thread is actively using it, preventing it from being evicted. The pin count on a page tracks how many threads are currently using it.

This protocol is enforced through RAII guards:
-   `ReadPageGuard`: Provides immutable access to a page's data.
-   `WritePageGuard`: Provides mutable access to a page's data.

When a guard is acquired (via `fetch_page_read`/`fetch_page_write`), the page's pin count is incremented, and it's marked as non-evictable. When the guard goes out of scope, its `Drop` implementation automatically decrements the pin count. If the count reaches zero, the page is marked as evictable again. This design makes memory management safe and largely automatic for the caller.

## 2. Concurrency & Safety

-   **Per-Frame Locking**: Using an `RwLock` for each `Page` allows multiple threads to read the same page concurrently, or one thread to write to it, without blocking access to other pages.
-   **Thundering Herd Prevention**: If multiple threads request the same non-resident page simultaneously, only the first thread will issue a disk read. Subsequent threads will wait on a page-specific `Mutex` stored in `inflight_loads`. Once the page is loaded, waiting threads are woken up and can proceed, avoiding redundant disk I/O. This is implemented with a double-check lock pattern inside `fetch_page_*`.
-   **Lock Order Inversion Avoidance**: The `complete_unpin` logic, which marks a page as evictable, is designed to run without holding any page locks. This prevents potential deadlocks that could arise from acquiring the replacer's lock while a page lock is held.
-   **Safe Eviction**: The eviction process is careful to only select victim pages with a pin count of zero. If a dirty page is chosen for eviction, it is first flushed to disk before its frame is reused.

## 3. Optimizations

-   **Sharded LRU-K Replacer**: The replacer is partitioned into multiple shards, each with its own lock. Accesses are distributed across shards based on `PageId`, significantly reducing lock contention on hot paths like `record_access` and `evict`.
-   **TinyLFU Admission Filter (Optional)**: An approximate frequency-based filter that helps protect the buffer pool from pollution caused by large, one-time scans. It estimates the access frequency of incoming pages and may deny admission to "cold" pages, forcing them to be used without being cached.
-   **Background Cleaner (Optional)**: A dedicated thread that periodically scans the buffer pool for dirty pages and schedules them for flushing. This helps to smooth out write I/O, reducing latency spikes during eviction or shutdown. It is enabled via the `QUILL_BPM_CLEANER=1` environment variable.
-   **Prefetch API**: The `prefetch_page` method allows other components (like the B+Tree iterator) to hint that a page will be needed soon. The BPM will try to load it into the buffer pool ahead of time, improving performance for predictable access patterns like range scans.

## 4. Benchmarking and Performance Tuning

Effective benchmarking is crucial for evaluating and tuning the BPM. The goal is typically to measure throughput (operations per second) or latency under different workloads.

### 4.1 Example: Hot Read Benchmark

This benchmark simulates a workload where a small, "hot" subset of data is accessed frequently. It measures the BPM's ability to keep the working set in memory.

```rust
// Pseudo-code for a hot-read benchmark
use std::time::Instant;

fn benchmark_hot_reads(index: &BPlusTreeIndex, total_keys: i64, num_ops: usize) {
    // 1. Identify the "hot set" (e.g., the last 10% of keys)
    let hot_set_start = (total_keys as f64 * 0.9) as i64;
    let hot_keys: Vec<_> = (hot_set_start..=total_keys).collect();

    // 2. Warm up the cache by reading the hot set once
    for key in &hot_keys {
        let tuple = create_tuple_from_key(*key, index.key_schema.clone());
        index.get(&tuple).unwrap();
    }

    // 3. Run the benchmark
    let start = Instant::now();
    for _ in 0..num_ops {
        let key_to_read = hot_keys[rand::random::<usize>() % hot_keys.len()];
        let tuple = create_tuple_from_key(key_to_read, index.key_schema.clone());
        // The get() call will trigger the BPM's fetch_page_read path
        index.get(&tuple).unwrap();
    }
    let elapsed = start.elapsed();
    let qps = num_ops as f64 / elapsed.as_secs_f64();
    println!("Hot Read QPS: {:.2}", qps);
}
```

### 4.2 Tuning Parameters

-   **Environment Variables**:
    -   `QUILL_BPM_CLEANER=1`: Enables the background cleaner thread.
    -   `QUILL_BPM_SLEEP_MS`: Sets the sleep duration for the cleaner (default: 50ms).
    -   `QUILL_BPM_DIRTY_PCT`: The dirty page ratio that triggers the cleaner (default: 0.2).
    -   `QUILL_BPM_BATCH`: The number of dirty pages the cleaner flushes per batch (default: 32).
-   **Admission Policy**: For pure read-heavy benchmarks on a known dataset, the TinyLFU admission policy might initially deny hot pages. It can be configured to be less aggressive or switched to a "count-only" mode to ensure the working set is admitted quickly.
-   **Buffer Pool Size**: The most critical parameter. An undersized pool will lead to high eviction rates (thrashing) and poor performance. Its size should be configured based on the available memory and the expected working set size of the workload.
