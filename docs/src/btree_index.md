# B+ Tree Index — Architecture and Concurrency

## 1. Architecture Overview

### 1.1 Node and Page Structure
-   **Node Types**: `Internal` and `Leaf`.
    -   **Internal Nodes**: Store separator keys and pointers to child pages. The first pointer is a "sentinel" that points to the subtree for all keys less than the first key in the node.
    -   **Leaf Nodes**: Store `(key, RecordId)` pairs in sorted order. Leaves are linked together in a singly linked list (`next_page_id`) to allow for efficient range scans.
-   **Header Page**: A fixed page (`header_page_id`) that acts as the entry point to the tree. It stores the `root_page_id`, allowing for atomic updates to the tree root when it splits or shrinks.
-   **Page Layout**:
    -   **Internal Page**: `{Header, High Key, Array<(Key, ChildPageId)>}`. The `High Key` is part of the B-link optimization.
    -   **Leaf Page**: `{Header, Array<(Key, RID)>}`.

### 1.2 B-link Structure

The tree uses B-link pointers (`next_page_id`) on all levels (both internal and leaf nodes). This creates a "side-link" to the right sibling. This is crucial for concurrency, as it allows readers to recover from transient inconsistent states caused by concurrent page splits by "chasing" the link to the right sibling.

```
              +------------------+
              |   Root (Int)     |
              +------------------+
             /         |         \
   +--------+      +--------+      +--------+
   | Int P1 |----->| Int P2 |----->| Int P3 |  (Internal B-link pointers)
   +--------+      +--------+      +--------+
   /   |   \      /   |   \      /   |   \
+----+ +----+  +----+ +----+  +----+ +----+
| L1 |-| L2 |->| L3 |-| L4 |->| L5 |-| L6 | (Leaf chain / B-links)
+----+ +----+  +----+ +----+  +----+ +----+
```

## 2. Concurrency Control

The B+Tree employs a sophisticated, lock-free read path and a high-concurrency write path using latch crabbing.

### 2.1 Read Path: Optimistic Lock Coupling (OLC) with B-links

-   Readers traverse the tree from the root without taking any locks.
-   On each page, a `version` number is read before and after processing the page's contents. If the version changes, it indicates a concurrent modification, and the read operation restarts from the root.
-   If a reader is traversing an internal node and the search key is greater than or equal to the node's `high_key`, it knows a split has occurred. Instead of restarting, it can use the `next_page_id` B-link to "chase" to the right sibling and continue the search, minimizing restarts.

### 2.2 Write Path: Latch Crabbing

Writers (insert/delete) use a technique called **latch crabbing** (or lock coupling) to ensure safe concurrent modifications.

-   **Process**: A writer acquires a write latch on a parent node before fetching and latching a child node. Once the child is latched, the writer checks if the child is "safe" for the operation (i.e., not full for an insert, not at minimum size for a delete).
    -   If the child is **safe**, the latch on the parent (and all other ancestors) is released.
    -   If the child is **unsafe**, the parent latch is held, as the child might need to split or merge, which would require modifying the parent.
-   **`Context` Struct**: This process is managed by a `Context` struct that holds the stack of write guards (`write_set`) for the current traversal path. Releasing ancestor latches is as simple as clearing this stack.

```
Latch Crabbing on Insert:
1. Latch(Root)
2. Descend to Child C1. Latch(C1).
3. Check if C1 is safe (not full).
   IF SAFE:
     ReleaseLatch(Root). Path is now just {C1}.
   IF UNSAFE (full):
     Keep Latch(Root). Path is {Root, C1}.
4. Descend from C1 to C2. Latch(C2).
5. Check if C2 is safe... and so on.
```

### 2.3 Deadlock Avoidance

When modifying siblings (during merge or redistribution), deadlocks are possible if two threads try to acquire latches on the same two pages in opposite orders. This is prevented by enforcing a strict **PageId-ordered locking** protocol. When two sibling pages must be latched, the page with the lower `PageId` is always latched first.

## 3. Key Algorithms & Features

-   **Parent-Guided Redirection**: During an insert or delete, after a writer has descended to a leaf, it re-validates its position using the parent (if a latch is still held). If a concurrent split has moved the target key range to a different sibling, the writer can jump directly to the correct page instead of traversing the leaf chain, preventing race conditions.
-   **Iterator**: The iterator performs a forward scan by following the leaf chain (`next_page_id`). It uses a lightweight form of OLC, checking the leaf page version to detect concurrent modifications and restart if necessary to ensure it doesn't miss keys.
    -   Sequential Scan Optimization (RingBuffer): For large range scans, the iterator switches to a "synchronous batch fetch + local ring buffer" mode. It fills a small `RingBuffer<BytesMut>` with consecutive leaf pages (by following `next_page_id`) and then decodes KVs locally without holding page guards for long. This reduces buffer pool pollution and syscall/lock overhead.
    -   Two Iteration Modes:
        -   Guard Mode: Keep a `ReadPageGuard` and decode per step; prefetch next leaf best-effort.
        -   Bytes Mode: After switching, decode from `BytesMut` buffers in the local ring; when a leaf is exhausted, pop next bytes from the ring or refill by following the chain.
-   **Prefix Compression**: Keys in internal nodes are prefix-compressed to save space. Each key is stored as `(lcp, suffix_len, suffix)`. This reduces the size of internal pages, increasing the tree's fanout and reducing its height, which improves cache performance and reduces I/O.
-   **Split/Merge Safety**:
    -   **Split**: When a node splits, the new right sibling is written first. Then, the B-link pointer and separator key are published atomically by updating the left sibling and parent. This ensures readers can always navigate the structure correctly.
    -   **Merge/Redistribute**: When a node is underfull, the implementation first tries to borrow an entry from a sibling (redistribute). If both siblings are at minimum size, it merges with a sibling. All these operations carefully maintain the B-link chain and parent pointers.

## 4. Benchmarks & Performance

### 4.1 Example: Range Scan Benchmark

This benchmark measures the efficiency of the leaf-chain traversal, which is critical for `SELECT` queries with `WHERE` clauses on indexed columns. It benefits from iterator prefetching and prefix compression.

```rust
// Pseudo-code for a range scan benchmark
use std::time::Instant;

fn benchmark_range_scan(index: Arc<BPlusTreeIndex>, num_keys: i64, num_passes: usize) {
    // 1. Populate the index with sequential keys
    for key in 1..=num_keys {
        let tuple = create_tuple_from_key(key, index.key_schema.clone());
        index.insert(&tuple, create_rid_from_key(key)).unwrap();
    }

    // 2. Run the benchmark
    let start = Instant::now();
    let mut count = 0;
    for _ in 0..num_passes {
        // Create an iterator over the full key range
        let mut iter = TreeIndexIterator::new(index.clone(), ..);
        while let Some(_) = iter.next().unwrap() {
            count += 1;
        }
    }
    let elapsed = start.elapsed();
    let total_items_scanned = num_keys as usize * num_passes;
    let items_per_sec = total_items_scanned as f64 / elapsed.as_secs_f64();

    println!(
        "Range Scan: Scanned {} items in {:?}. Throughput: {:.2} items/sec",
        total_items_scanned, elapsed, items_per_sec
    );
}
```

### 4.2 Performance Notes
-   **Hot Reads**: Performance on hot-spot reads depends on keeping upper levels and hot leaves resident in the buffer pool. Warm up the cache for read-heavy benchmarks. Protect hot pages from pollution by enabling TinyLFU admission.
-   **Large Range Scans**: Prefer table SeqScan with ring buffer (bypass) when scanning most of the table. For index scans over very large ranges, consider future Bitmap Heap Scan rather than bypassing the pool for leaves.

### 4.3 Configuration
-   `config::BTreeConfig` controls iterator behavior:
    -   `seq_batch_enable` (bool): enable batch mode with local ring buffer.
    -   `seq_window` (usize): number of leaf pages to prefill into the ring per refill.
    -   `prefetch_enable`/`prefetch_window`: guard-mode prefetch hints to buffer pool.
  Defaults are conservative; increase `seq_window` for long scans to reduce I/O hop.

## 5. Future Work
-   Stronger OLC with bounded retries and telemetry.
-   CSB+-like internal layout and columnar key prefixing.
-   NUMA-aware partitioning and router.
-   WAL/MVCC for crash recovery and snapshot isolation.
