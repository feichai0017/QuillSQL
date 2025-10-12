# The Buffer Pool: Architecture and Lifecycle

## 1. Core Components & Architecture

QuillSQL's buffer management is split into two main structs, reflecting a separation of concerns:

- **`BufferPool` (`buffer/buffer_pool.rs`)**: A low-level, "dumb" container. It owns the actual memory arena for pages and provides basic mapping from a `PageId` to a memory location (`FrameId`).
- **`BufferManager` (`buffer/buffer_manager.rs`)**: The high-level, "smart" coordinator. It contains the `BufferPool` and implements all the logic for fetching pages, choosing which pages to evict, and interacting with other database components like the transaction and recovery managers.

This architecture is centered around three key data structures:

1.  **Frames (The Arena)**: The `BufferPool` pre-allocates a large, contiguous block of memory on the heap. This block is divided into a fixed number of smaller chunks called **frames**. Each frame is exactly `PAGE_SIZE` (4KB) and can hold the contents of one disk page.

2.  **Page Table (`page_table`)**: A hash map (specifically, a concurrent `DashMap`) that maps a logical `PageId` to the `FrameId` where it currently resides in memory. This provides fast O(1) lookups to check if a page is already in the buffer pool.

3.  **Replacer (`LRUKReplacer`)**: When a requested page is not in memory and the buffer pool is full, one of the existing pages must be **evicted** to make room. The `Replacer` is the component that implements the page replacement policy and decides which page is the best candidate for eviction. QuillSQL uses an **LRU-K** replacement policy, a sophisticated variant of the classic Least Recently Used (LRU) algorithm.

## 2. The Lifecycle of a Page Request

When another part of the database (e.g., the execution engine) needs to access a page, it calls `buffer_manager.fetch_page_read(page_id)` or `fetch_page_write(page_id)`. This initiates a critical sequence of events.

The flow is as follows:

1.  **Request**: An executor requests Page `P`.
2.  **Lookup**: The `BufferManager` consults the `PageTable`.

3.  **Case 1: Cache Hit**
    - The `PageTable` contains an entry for `P`, mapping it to `FrameId` `F`.
    - The `BufferManager` increments the pin count for frame `F`.
    - It informs the `LRUKReplacer` that the page has been accessed (updating its priority).
    - It returns a `PageGuard` wrapping a reference to the memory in frame `F`.

4.  **Case 2: Cache Miss**
    - The `PageTable` has no entry for `P`.
    - The `BufferManager` must bring the page from disk. It asks the `Replacer` to choose a **victim frame** `F_v` to evict.
    - **If the victim frame `F_v` is dirty**: The `BufferManager` first writes the contents of `F_v` back to disk via the `DiskScheduler`. This is essential for data durability.
    - The `PageTable` entry for the old page residing in `F_v` is removed.
    - The `BufferManager` issues a read request to the `DiskScheduler` to load page `P`'s data from disk into frame `F_v`.
    - The `PageTable` is updated with the new mapping: `P -> F_v`.
    - The process then continues like a cache hit: frame `F_v` is pinned and a `PageGuard` is returned.

## 3. Concurrency

The buffer pool is a shared resource accessed by many concurrent threads. QuillSQL uses a combination of locking strategies:

- **Page Table**: A `DashMap` is used for the page table, which is highly optimized for concurrent reads and writes.
- **Frame-Level Locks**: Each frame in the pool has its own `RwLock`. A `ReadPageGuard` acquires a read lock on the frame, allowing multiple threads to read the same page concurrently. A `WritePageGuard` acquires an exclusive write lock, ensuring that only one thread can modify a page at a time.
- **Replacer/Free List**: These shared structures are protected by a `Mutex`.

## 4. Integration with WAL and Recovery

The Buffer Manager is a key player in the ARIES recovery protocol.

- **Dirty Pages**: When a `WritePageGuard` is dropped, if it has modified the page, it marks the page as **dirty**. The `BufferManager` maintains a `dirty_page_table` that tracks all dirty pages and the Log Sequence Number (LSN) of the first WAL record that caused the page to become dirty.
- **Forced WAL (Write-Ahead Logging)**: Before a dirty page can be written back to disk (either during eviction or a checkpoint), the `BufferManager` must ensure that all WAL records up to that page's current LSN have been flushed to durable storage. This is the fundamental **WAL rule** and is enforced by the `flush_page` method, which calls `wal_manager.flush(lsn)` before writing the page to disk.

---

## For Study & Discussion

1.  **Replacement Policies**: QuillSQL uses LRU-K. What are the potential advantages of LRU-K over a simple LRU policy? What kind of workload would benefit most? Conversely, what are the trade-offs of using Clock/Second-Chance instead?

2.  **The "Double Caching" Problem**: We mentioned that using Direct I/O helps avoid double caching. If Direct I/O is disabled, how does the database's buffer pool interact with the operating system's file system cache? Why can this lead to suboptimal performance?

3.  **Programming Exercise**: Implement a `ClockReplacer` that adheres to the `Replacer` trait. Modify the `BufferManager` to use your new replacer instead of `LRUKReplacer`. Run the benchmark suite and compare the performance. Does it change?

4.  **Programming Exercise**: Add metrics to the `BufferManager` to track the buffer pool hit rate (i.e., `num_hits / (num_hits + num_misses)`). You could expose this via a new `SHOW BUFFER_STATS;` SQL command.
