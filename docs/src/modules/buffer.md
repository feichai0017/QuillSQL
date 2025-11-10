# Buffer Manager

The buffer manager (`src/buffer/`) implements QuillSQL’s shared buffer pool, bridging the
speed gap between RAM and disk. It lets storage/execution read and write pages safely
while coordinating with WAL and asynchronous I/O.

---

## Responsibilities

- Maintain a fixed-size set of page frames caching `TableHeap` and B+Tree pages.
- Expose RAII-style guards (pin/unpin) that enforce safe concurrent access.
- Keep the page table, replacement policy, dirty-page tracking, and WAL coordination in
  sync.
- Submit async I/O through `DiskScheduler`.

---

## Directory Layout

| Path | Description | Key Types |
| ---- | ----------- | --------- |
| `buffer_manager.rs` | Core buffer pool. | `BufferManager`, `BufferFrame` |
| `page.rs` | Guard types and pin/unpin logic. | `ReadPageGuard`, `WritePageGuard` |
| `replacer.rs` | LRU-K + TinyLFU replacement. | `Replacer` |
| `metrics.rs` | Optional instrumentation hooks. | `BufferMetrics` |

---

## Key Mechanisms

### Guard Model
- `ReadPageGuard`, `WritePageGuard`, and `UpgradeableGuard` ensure only compatible access
  modes coexist on a page.
- Guards drop automatically to release pins; paired with Rust’s borrow checker, they make
  latch semantics tangible.

### Replacement Policy
- **LRU-K** tracks the last K touches to protect hot pages from scan pollution.
- **TinyLFU** decides whether a new page should enter the cache, offering probabilistic
  admission against noisy workloads.

### WAL Coordination
- Before flushing a dirty page, the buffer checks `page_lsn` and asks `WalManager` to
  flush up to that LSN (write-ahead rule).
- `set_wal_manager` wires the buffer to WAL so checkpoints can inspect the oldest dirty
  LSN.

### Disk Scheduler
- All physical reads/writes go through `DiskScheduler::submit_*`, sharing worker threads
  with WAL and demonstrating the benefits of a unified I/O layer.

---

## Interactions

- **Storage engine** – `TableHeap` and `BPlusTreeIndex` access pages exclusively through
  the buffer manager.
- **Recovery** – checkpoints consult the buffer’s dirty page table to build the ARIES DPT.
- **Background writer** – periodically walks `dirty_frames` to flush pages in the
  background.

---

## Teaching Ideas

- Disable TinyLFU via feature flag, rerun sqllogictest, and compare hit rates.
- Swap the replacement policy with CLOCK to experiment with cache algorithms.
- Enable `RUST_LOG=buffer=debug` and trace the pin/unpin lifecycle of hot pages.

---

Further reading: [Page & Page Guards](../buffer/page.md),
[The Buffer Pool](../buffer/buffer_pool.md)
