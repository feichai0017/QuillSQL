# Storage Engine

The storage engine persists relational data, covering heap files, indexes, page formats,
and the handles exposed to execution. Understanding this layer is key to reasoning about
performance, MVCC, and recovery.

---

## Responsibilities

- Manage `TableHeap` insert/delete/update paths and their MVCC metadata.
- Maintain indexes (see the [Index module](./index.md) for details).
- Expose the `StorageEngine` trait so execution can fetch `TableHandle` / `IndexHandle`
  instances per table.
- Provide `TupleStream` so sequential and index scans share a unified interface.

---

## Directory Layout

| Path | Purpose | Key Types |
| ---- | ------- | --------- |
| `engine.rs` | Default engine plus handle definitions. | `StorageEngine`, `TableHandle`, `TupleStream`, `ScanOptions` |
| `table_heap/` | Heap storage + MVCC logic. | `TableHeap`, `MvccHeap` |
| `index/` | B+Tree implementation. | `BPlusTreeIndex` |
| `page/` | Page, RID, tuple metadata. | `Page`, `RecordId`, `TupleMeta` |
| `tuple/` | Row encoding and projection helpers. | `Tuple` |
| `disk_manager.rs` | File layout and page I/O. | `DiskManager` |
| `disk_scheduler.rs` | `io_uring`-backed async scheduler. | `DiskScheduler` |

---

## Core Abstractions

### StorageEngine Trait
```rust
pub trait StorageEngine {
    fn table(&self, catalog: &Catalog, table: &TableReference)
        -> QuillSQLResult<Arc<dyn TableHandle>>;
    fn indexes(&self, catalog: &Catalog, table: &TableReference)
        -> QuillSQLResult<Vec<Arc<dyn IndexHandle>>>;
}
```
The default implementation wraps the row-oriented heap + B+Tree combo, but the trait is
ready for column stores, remote storage, or async engines.

### TableHandle
Offers `full_scan(ScanOptions)`, `insert`, `delete`, `update`, and
`prepare_row_for_write`. MVCC, undo, and locking concerns live here so execution operators
only describe intent.

### TupleStream
Minimal iterator that returns `(RecordId, TupleMeta, Tuple)` triples. `ScanOptions` carry
streaming hints, projection lists, or batch sizes; index scans use `IndexScanRequest` to
describe ranges.

---

## Interactions

- **Execution** – `ExecutionContext::table_stream` / `index_stream` delegate to handles.
- **Transaction** – Handle methods call into `TxnContext` to acquire locks, record undo,
  and emit WAL.
- **Buffer Manager** – `TableHeap`/`BPlusTreeIndex` access pages through the shared buffer
  pool.
- **Recovery** – Heap/index mutations generate WAL records (`HeapInsert`, `HeapUpdate`,
  `IndexInsert`, …) that ARIES replays.
- **Background** – MVCC vacuum and index cleanup obtain handles and iterate tuples via
  the same abstractions as foreground scans.

---

## Teaching Ideas

- Implement a toy columnar handle to show how the execution engine can stay agnostic to
  storage layout.
- Use `ScanOptions::projection` to push down column pruning into `TableIterator`.
- Enable `RUST_LOG=storage::table_heap=trace` and trace MVCC version chains as updates
  occur.

---

Further reading: [Disk I/O](../storage/disk_io.md),
[Page & Tuple Layout](../storage/page_layouts.md),
[Table Heap & MVCC](../storage/table_heap.md)
