# Table Heap and MVCC

The `TableHeap` (`storage/table_heap.rs`) is the component responsible for managing the collection of pages that belong to a single table. While the `TablePage` defines the *layout* within a single page, the `TableHeap` provides the high-level API for inserting, updating, and deleting tuples, making it central to the implementation of MVCC.

## TableBinding & StorageEngine

From the executor’s perspective, heap access is routed through `storage::engine::TableBinding`. A binding is produced by the `StorageEngine` (consulting the catalog) and carries:

- An `Arc<TableHeap>` for physical page access.
- An `MvccHeap` wrapper that maintains version chains.
- All indexes defined on the table.

The binding exposes ergonomic methods such as `scan`, `index_scan`, `insert`, `update`, `delete`, and `prepare_row_for_write`. Each method internally uses the `TableHeap` + `MvccHeap` pair, appends the appropriate logical WAL record (HeapInsert/HeapDelete), and keeps indexes in sync. This keeps physical operators extremely small and makes it easy to swap out the storage engine for experiments.

## `Tuple` Serialization

A logical `Tuple` struct in memory must be converted into a byte array to be stored on a page. This process is handled by `storage/codec/tuple.rs`.

The serialized format of a tuple consists of two parts:

1.  **Null Bitmap**: A compact bitmap at the beginning of the tuple data. Each bit corresponds to a column in the schema; if the bit is `1`, the column's value is `NULL`. This avoids storing any data for null fields.
2.  **Attribute Data**: The actual data for all non-null columns, serialized one after another.

## `TupleMeta`: The Heart of MVCC

The most important part of a tuple's on-disk metadata is the `TupleMeta` struct, which is stored directly within the `TupleInfo` slot in the page header. This is the heart of QuillSQL's **Multi-Version Concurrency Control (MVCC)** implementation.

- **`insert_txn_id`**: The ID of the transaction that created this version of the tuple.
- **`delete_txn_id`**: The ID of the transaction that "deleted" this version. (A value of `0` or `INVALID` means it's not deleted).
- **`is_deleted`**: A boolean flag indicating the tuple version is considered deleted.
- **`prev_version: Option<RecordId>`**: A link (RID) to the *previous* version of this logical row.
- **`next_version: Option<RecordId>`**: A link (RID) to the *next* version of this logical row.

These fields allow QuillSQL to maintain a **version chain** for each logical row. When a row is updated, the `TableHeap`'s `mvcc_update` method is called, which, instead of overwriting the data, performs the following steps:

1.  Creates a new tuple version with the new data by calling `insert_tuple`.
2.  Sets the `prev_version` pointer of this new version to the RID of the old version.
3.  Updates the `next_version` pointer of the old version to point to the new version.
4.  Sets the `delete_txn_id` on the old version to mark it as "dead".

A transaction can then traverse this chain and use the transaction IDs in the `TupleMeta` to determine which version of a row is visible to it based on its own transaction ID and isolation level, thus achieving transaction isolation without long-held read locks.

## Logical Logging (Redo + Undo)

Every heap mutation emits a logical WAL record before the page frame is dirtied:

| Operation | Payload | Redo | Undo |
| --------- | ------- | ---- | ---- |
| Insert | `HeapInsertPayload` | Re-insert tuple bytes into the slot | Remove the tuple (implicit via delete logic) |
| Delete | `HeapDeletePayload` (new tombstone + old tuple) | Mark slot deleted and update version chain | Recreate the prior tuple image |

This makes crash recovery straightforward: redo simply replays the payload, and undo uses the “old” portion of the payload even if the page is currently inconsistent. Physical `PageWrite`/`PageDelta` records are still used for metadata-heavy pages (meta, freelist, etc.) to guarantee a consistent starting point, but ordinary table DML lives entirely in these logical heap records.

---

## For Study & Discussion

1.  **Version Chain Traversal**: Imagine a transaction with ID `T10` needs to read a row. It finds a version of the row that was inserted by `T5` (committed) but deleted by `T12` (in-progress). Should `T10` see this version? What if the deleter was `T8` (committed)? What if the deleter was `T10` itself? Walk through the visibility check logic.

2.  **Garbage Collection**: The MVCC model creates many "dead" tuple versions that are no longer visible to any active or future transaction. What is the long-term problem with leaving these dead versions in the database? This problem is typically solved by a process called **vacuuming** or garbage collection. When is it safe to physically remove a dead tuple version?

3.  **Programming Exercise (Advanced)**: Implement a basic `VACUUM` function for a `TableHeap`. This function should scan the table and identify dead tuple versions that are no longer visible to *any* currently active transaction. Once identified, it should physically remove them from their pages. This is a challenging exercise that touches transaction management, storage, and concurrency.
