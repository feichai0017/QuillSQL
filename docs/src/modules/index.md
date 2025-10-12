# Indexes

Indexes are crucial for database performance. They are redundant data structures that allow the system to find rows matching specific criteria quickly, without having to scan the entire table. While indexes speed up queries (reads), they incur a cost during data modification (writes), as both the table and its indexes must be updated.

QuillSQL currently provides a `BPlusTreeIndex`.

## Key Concepts

- **Data Structure**: The index is built using a B+Tree, a self-balancing tree data structure that maintains sorted data and allows for efficient insertion, deletion, and search operations (typically in O(log n) time).

- **Index Keys**: The B+Tree stores key-value pairs. The "key" is the value from the indexed column(s) (e.g., a user's ID), and the "value" is the `RecordId` (RID) of the row containing that key. This allows the database to first find the key in the B+Tree and then use the RID to immediately locate the full row data on its data page.

- **Concurrency Control**: The B+Tree implementation must be highly concurrent. QuillSQL's implementation uses advanced techniques like **B-link (or B-link tree)** page connections and **latch-crabbing** to allow multiple readers and writers to access the tree simultaneously with minimal contention.

This section contains:

- **[B+Tree](./index/btree_index.md)**: A look at the classic B+Tree data structure.