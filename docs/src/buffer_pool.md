# Buffer Manager — Architecture and Streaming Scan

## 1. Architecture Overview

The Buffer Manager is responsible for managing pages in memory, acting as a cache between the disk and the execution engine. It fetches pages from disk into memory frames, allows threads to "pin" them for safe access, and writes dirty pages back to disk while coordinating with WAL.

### 1.1 Core Components

```
+-------------------------------------------------------------------+
|                           Buffer Manager                           |
|                                                                   |
|  +-----------------+   +------------------+   +------------------+  |
|  |   Page Table    |   |    Replacer      |   |    Free List     |  |
|  | (DashMap<P,F>)  |<->|    (LRU-K)       |<->|  (VecDeque<F>)   |  |
|  +-----------------+   +------------------+   +------------------+  |
|        ^   |                        ^                               |
|        |   |                        |                               |
|        |   v                        |                               |
|  +----------------------------------------------------------------+ |
|  |                           Frame Pool                           | |
|  |              (Vec<Arc<RwLock<Page>>> + FrameMeta)               | |
|  |  ┌────────────┬────────────┬────────────┬────────────┐         | |
|  |  | Frame 0     | Frame 1    | Frame 2    | Frame 3    |  ...    | |
|  |  | page_id=34  | page_id=8  | page_id=0  | page_id=55 |         | |
|  |  | LeafPage    | Internal   | FREE       | TablePage  |         | |
|  |  | pin=2 dirty | pin=0      | pin=0      | pin=1 dirty|         | |
|  |  └────────────┴────────────┴────────────┴────────────┘         | |
|  +----------------------------------------------------------------+ |
|        ^   |                        ^   |                           |
|        |   | (pins/unpins)          |   | (page bytes)              |
|        |   v                        |   v                           |
|  +--------------------+              +---------------------+        |
|  |   Client Thread    |              |    Disk Scheduler   |        |
|  | (e.g., Executor)   |<------------>|  (I/O Background    |        |
|  | - fetch_page_*()   |              |    Thread)          |        |
|  | - new_page()       |              | - read/write/alloc  |        |
|  | - prefetch_page()  |              +---------------------+        |
|  | - unpin_page()     |                       ^                    |
|  +--------------------+                       |                    |
|        ^                                       |                    |
|        |                                       v                    |
|   +-----------+                       +--------------------+        |
|   | BG Writer |---------------------->| flush_page batches |        |
|   +-----------+                       +--------------------+        |
|                                                                   | |
|                                                       +----------------+
|                                                       |    Disk File   |
|                                                       +----------------+
+-------------------------------------------------------------------+
```

-   **Frame Pool (`pool`)**: A `Vec<Arc<RwLock<Page>>>` representing the main memory area managed by the Buffer Manager. Each frame is protected by its own `RwLock` for fine-grained concurrent access, and `FrameMeta` captures `page_id`, `pin_count`, `is_dirty`, and `lsn` for eviction and durability decisions.
-   **Page Table (`page_table`)**: A `DashMap<PageId, FrameId>` for efficiently mapping logical page IDs to their physical frame locations. `DashMap` provides high-performance concurrent lookups, insertions, and removals.
-   **Replacer (`replacer`)**: An `LRU-K` replacer that implements the page replacement policy. When the free list is empty, it selects a victim frame to evict. The replacer is internally sharded to reduce lock contention.
-   **Free List (`free_list`)**: A `VecDeque<FrameId>` that tracks available frames, allowing for quick allocation without needing to consult the replacer.
-   **Disk Scheduler (`disk_scheduler`)**: Asynchronous I/O backend. The Buffer Manager offloads reads, writes, allocate/deallocate, and WAL requests to the scheduler via channels so client threads do not block on syscalls.

-   **Dirty Page Table (`dirty_page_table`)**: A `DashMap<PageId, Lsn>` that tracks the first log sequence number (`rec_lsn`) that dirtied each page. This snapshot feeds checkpoints and recovery decisions. The table always records the earliest `lsn` emitted by the current write guard so the background writer can flush in WAL order.

-   **Background Writer**: Optional thread that periodically calls `flush_page` on batches of dirty frames, smoothing out latency for foreground work.

### 1.2 Frame & Page Layout

The Buffer Pool keeps hot pages in a compact arena. Each frame holds one page-sized slice plus metadata:

```
FrameMeta {
    page_id: PageId,       // INVALID_PAGE_ID if the slot is free
    pin_count: u32,        // >0 blocks eviction
    is_dirty: bool,        // true means bytes differ from disk
    lsn: Lsn,              // latest WAL record applied to this frame
}
```

Example slice of the arena:

```
Frame 0 ──┬─ Page 34 (Leaf | pin=2 | dirty | lsn=148)
          │   • entries: [10→RID(1,3), 24→RID(2,8)]
Frame 1 ──┼─ Page 8  (Internal | pin=0 | clean | lsn=120)
          │   • children: [4, 9, 17]
Frame 2 ──┼─ FREE (INVALID_PAGE_ID)
Frame 3 ──┴─ Table Page 55 (Heap | pin=1 | dirty | lsn=151)
```

Frames with `pin_count == 0` and `is_dirty == false` are immediately evictable. Dirty frames enter the `dirty_pages` set and must flush after `ensure_wal_durable(lsn)` succeeds.

### 1.3 Pin Protocol, WAL, and Page Guards

To ensure safe memory access, the BPM uses a pin/unpin protocol. A page is "pinned" when a thread is actively using it, preventing it from being evicted. The pin count on a page tracks how many threads are currently using it.

This protocol is enforced through RAII guards:
-   `ReadPageGuard`: Provides immutable access to a page's data.
-   `WritePageGuard`: Provides mutable access and records the first dirty LSN. On drop it passes a `rec_lsn` hint into the Buffer Manager so WAL ordering stays correct.

When a guard is acquired (via `fetch_page_read`/`fetch_page_write`), the page's pin count is incremented, and it's marked as non-evictable. When the guard goes out of scope, its `Drop` implementation automatically decrements the pin count. If the count reaches zero, the page is marked as evictable again. This design makes memory management safe and largely automatic for the caller.

The Buffer Manager is WAL-aware: before flushing or evicting a dirty page, it invokes `ensure_wal_durable(lsn)` to guarantee the corresponding log records are persisted. Dirty pages are also tracked in `dirty_pages`/`dirty_page_table`, enabling checkpoints to log accurate metadata. `WritePageGuard::drop` passes the first dirty `lsn` it observed so `dirty_page_table` always points to the earliest log record that must be durable.

### 1.4 Page Lifecycle & State Transitions

1. **Lookup**: `page_table` maps a logical `PageId` to a `FrameId`. Cache hits skip disk I/O and simply increment `pin_count`.
2. **Miss Handling**: If `page_id` is absent, `ensure_frame` either pops a free frame or consults the LRU-K replacer for a victim. Dirty victims flush after WAL durability is confirmed.
3. **Pin Acquisition**: `fetch_page_read`/`fetch_page_write` increments `pin_count` before handing out a guard. The guard owns the frame lock (`RwLock`), enforcing isolation.
4. **Mutation**: Writers call `set_lsn` and `mark_dirty`. The first `lsn` observed becomes the page's `rec_lsn`, enabling ARIES-style recovery.
5. **Unpin**: Guard `Drop` calls `complete_unpin`. When `pin_count` falls to zero the frame becomes evictable. Dirty frames remain tracked until flushed.
6. **Flush/Evict**: `flush_page` writes bytes back to disk, clears `is_dirty`, and removes the entry from dirty tracking. Eviction finally resets the frame and returns it to the free list.

All state changes are serialized through per-frame locks, so metadata updates stay consistent even under heavy concurrency.

## 2. Buffer Pool Internals

### 2.1 Memory Layout & Data Structures

-   **Arena (`arena`)**: A contiguous `Box<[UnsafeCell<u8>]>` sized at `capacity * PAGE_SIZE`. Each frame's bytes occupy a stable slot, so pointers remain valid across fetches.
-   **Frame Locks (`locks`)**: A `Vec<RwLock<()>>` giving each frame its own synchronization primitive. Guards in `page.rs` transmute these locks into `'static` scope for RAII.
-   **Frame Metadata (`meta`)**: A `Vec<Mutex<FrameMeta>>` storing the eviction-critical fields described above. `Mutex` keeps metadata updates isolated from page bytes.
-   **Page Table (`page_table`)**: `DashMap<PageId, FrameId>` enabling lock-free lookups for hits. Removal helpers (`remove_mapping_if`) guard against stale mappings when frames are reassigned.
-   **Free List (`free_list`)**: `Mutex<VecDeque<FrameId>>` tracking unused frames. Frames return here after eviction and reset.
-   **Disk Scheduler (`disk_scheduler`)**: Shared async backend for reads, writes, allocations, and deallocations. BufferPool methods schedule work and wait on the returned receiver.

### 2.2 Frame Allocation Flow

1. **`pop_free_frame`**: Preferred path—O(1) pop from the free list yields an unused frame without touching the replacer.
2. **`evict_victim_frame`**: When the free list is empty, BufferManager asks the LRUK replacer for a victim. BufferPool cooperates by exposing metadata and providing `reset_frame` to zero the data region.
3. **Metadata Reset**: Eviction clears `FrameMeta` and marks `page_id` as `INVALID_PAGE_ID`, after which the frame re-enters the free list.
4. **Admission Checks**: TinyLFU (if enabled) can reject low-frequency pages before a victim is chosen, reducing churn.

### 2.3 Disk I/O Primitives

-   **`load_page_into_frame`**: Schedules a disk read, copies bytes into the target frame, zero-fills any short read, and resets metadata. The copy uses the unsafe slice helpers but remains bounded by `PAGE_SIZE`.
-   **`write_page_to_disk`**: Copies the in-memory page into a `Bytes` buffer and issues an async write via the scheduler. Success clears dirty flags upstream.
-   **`allocate_page_id` / `schedule_deallocate`**: Abstract page-ID management so higher layers never touch the disk manager directly.

### 2.4 Invariants & Safety Notes

-   Frame bytes must only be accessed while holding the corresponding `RwLock`; guards enforce this.
-   `FrameMeta.pin_count > 0` implies the frame is marked non-evictable by the replacer.
-   Disk scheduler channels are assumed reliable; every caller maps disconnection into `QuillSQLError::Internal`.
-   `reset_frame` always zeroes the buffer to prevent dirty data from leaking into newly allocated pages.

These invariants keep BufferPool deterministic even under concurrent fetches and evictions.

## 3. Concurrency & Safety

-   **Per-Frame Locking**: Using an `RwLock` for each `Page` allows multiple threads to read the same page concurrently, or one thread to write to it, without blocking access to other pages.
-   **Thundering Herd Prevention**: If multiple threads request the same non-resident page simultaneously, only the first thread issues a disk read. Subsequent threads wait on a page-specific `Mutex` stored in `inflight_loads`. Once the page is resident, waiters resume, avoiding redundant disk I/O. This uses a double-check lock pattern inside `fetch_page_*`.
-   **Lock Order Inversion Avoidance**: The `complete_unpin` logic, which marks a page as evictable, is designed to run without holding any page locks. This prevents potential deadlocks that could arise from acquiring the replacer's lock while a page lock is held.
-   **Safe Eviction**: The eviction process only selects frames with pin count zero. If a victim is dirty, WAL durability is enforced, the page is flushed, and the dirty bookkeeping cleared before reuse.

-   **Prefetch API**: `prefetch_page(page_id)` lets callers warm the cache asynchronously. Prefetch respects the inflight table so redundant loads are coalesced.

## 4. Optimizations

-   **Sharded LRU-K Replacer**: The replacer is internally partitioned so hot pages can update their visitation history without heavy locking. Together with TinyLFU admission, it keeps the cache resilient to one-off scans.
-   **TinyLFU Admission Filter (Optional)**: An approximate frequency-based filter that helps protect the cache from pollution caused by large, one-time scans by estimating access frequency and denying admission to "cold" pages when necessary.
-   **Prefetch Hooks**: Iterators (e.g., B+Tree) call `prefetch_page` to stage predictable access patterns, improving hit ratios for near-future reads.

-   **Sequential Scan Ring Buffer (Bypass)**: For full sequential scans that would otherwise thrash the cache, the iterator switches to a direct I/O ring (`DirectRingBuffer`). It reads and decodes pages through the DiskScheduler with a small in-memory window (readahead) and does not admit these pages into the main buffer pool. This significantly reduces cache pollution for large scans. Control via: `QUILL_STREAM_SCAN` (1/0), `QUILL_STREAM_THRESHOLD` (default ≈ BUFFER_POOL_SIZE/4 frames), `QUILL_STREAM_READAHEAD` (default 2 pages), and per-query planner hint `QUILL_STREAM_HINT`.
-   **Prefetch API**: The `prefetch_page` method allows components (e.g., B+Tree iterator) to warm the cache opportunistically for predictable patterns such as short range scans.
-   **Flush-on-evict & explicit flush**: There is no background cleaner thread. Dirty pages are flushed when a victim is chosen, and the engine can call `flush_all_pages()` to guarantee durability or visibility to direct I/O paths.

## 5. Benchmarking and Performance Tuning

Effective benchmarking is crucial for evaluating and tuning the Buffer Manager. The goal is typically to measure throughput (operations per second) or latency under different workloads.

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
        // The get() call will trigger the Buffer Manager's fetch_page_read path
        index.get(&tuple).unwrap();
    }
    let elapsed = start.elapsed();
    let qps = num_ops as f64 / elapsed.as_secs_f64();
    println!("Hot Read QPS: {:.2}", qps);
}
```

### 4.2 Tuning Parameters

-   **Environment Variables (selected)**:
    -   `QUILL_STREAM_SCAN` / `QUILL_STREAM_THRESHOLD` / `QUILL_STREAM_READAHEAD` / `QUILL_STREAM_HINT`
-   **Admission Policy**: TinyLFU can be used to prevent one-time scans from polluting the pool. Adjust its aggressiveness based on workload characteristics.
-   **Streaming Scan**: Ensure that large sequential scans no longer degrade hit ratios. Compare QPS/latency with and without the ring buffer, and tune `QUILL_STREAM_READAHEAD` (typically 2–8).
-   **Buffer Pool Size**: The most critical lever. Too small → frequent evictions and poor performance. Size it to the expected hot working set.
