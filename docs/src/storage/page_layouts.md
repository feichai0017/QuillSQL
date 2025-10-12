# Page and Tuple Layout

## 1. The Page: The Atomic Unit of I/O

A database file is not treated as one continuous stream of data. Instead, it is broken down into fixed-size blocks called **pages**. A page is the atomic unit of transfer between the disk and the in-memory [Buffer Pool](../modules/buffer.md). Whenever the database needs to read a piece of data (like a single row), it must load the *entire page* containing that data into memory.

In QuillSQL, the page size is a constant defined at `quill-sql/src/buffer/mod.rs`:
- **`PAGE_SIZE`**: 4096 bytes (4 KB)

This fixed-size approach simplifies buffer management and allows for efficient, aligned I/O operations, especially when using Direct I/O to bypass the OS cache.

## 2. `TablePage`: The Slotted Page Layout

While a page is a generic 4KB block of bytes, pages that store actual table data are structured in a specific way. QuillSQL uses a classic **Slotted Page** layout, which is a core concept in database implementation (as taught in CMU 15-445).

A `TablePage` is organized into three main parts:

```
<------------------------------ 4KB ------------------------------>
+----------------+-----------------+-----------------+--------------+
|  Page Header   |   Slot Array    |      Free       |   Tuple      |
| (grows ->)     |   (grows ->)    |      Space      |     Data     |
|                |                 |                 | (<- grows)   |
+----------------+-----------------+-----------------+--------------+
```

1.  **Page Header (`TablePageHeader`)**: Located at the beginning of the page. It contains metadata about the page itself.
2.  **Slot Array (`tuple_infos`)**: An array of `TupleInfo` structs that grows from after the header. Each entry in this array acts as a "pointer" or "directory entry" for a tuple on the page.
3.  **Tuple Data**: The actual raw data of the tuples is stored starting from the **end** of the page, growing backwards towards the middle.

This design has a key advantage: **it decouples a tuple's logical identifier from its physical location on the page.**

### The `RecordId` (RID)

A specific tuple is uniquely identified by a `RecordId` (RID). The RID is a stable pointer composed of two parts:
- **`page_id`**: The ID of the page where the tuple resides.
- **`slot_num`**: The **index** into the Slot Array on that page.

So, `RID = (page_id, slot_num)`.

When the database needs to delete a tuple or if a variable-length tuple is updated and grows in size, the tuple's data might need to be moved within the page (for compaction). In a slotted page design, we only need to update the `offset` in the corresponding slot array entry. The tuple's RID (`page_id`, `slot_num`) **remains unchanged**. This prevents a cascade of updates to all secondary indexes that might be pointing to that tuple.

### `TablePageHeader` and Slot (`TupleInfo`)

Let's look at the physical layout, as defined in `storage/codec/table_page.rs`:

- **`TablePageHeader`**:
    - `lsn`: The Log Sequence Number of the last change made to this page, crucial for WAL recovery.
    - `next_page_id`: The ID of the next page in this table, forming a linked list of pages.
    - `num_tuples`: The number of active tuples on the page.
    - `num_deleted_tuples`: The number of "dead" or deleted tuples.
    - `tuple_infos`: The slot array itself.

- **`TupleInfo`** (A single slot in the array):
    - `offset`: The byte offset from the beginning of the page where the tuple's data begins.
    - `size`: The size of the tuple's data in bytes.
    - `meta`: A nested `TupleMeta` struct containing critical information for concurrency control.

---

## For Study & Discussion

1.  **Layout Trade-offs**: What is the main benefit of having the tuple data grow from the end of the page backwards, while the header and slot array grow from the beginning forwards? What happens when they meet?

2.  **Record ID Stability**: Why is it so important that a tuple's `RecordId` does not change even if the tuple's physical data is moved within the page? What would break if the RID was just a direct byte offset?

3.  **Large Objects**: The current design assumes a tuple fits entirely on one page. How would you modify this page layout to support tuples that are larger than 4KB (e.g., a long blog post stored in a `VARCHAR` column)? Research how systems like PostgreSQL handle this with their "TOAST" (The Oversized-Attribute Storage Technique) mechanism.

4.  **Programming Exercise**: Implement a `defragment()` method for the `TablePage`. After several insertions and deletions, the free space on a page can become fragmented into small, unusable chunks. This method should reorganize the page by moving the existing tuples to be contiguous, creating a single, large block of free space. Remember to update the `offset` in each `TupleInfo` slot after moving its corresponding tuple data!
