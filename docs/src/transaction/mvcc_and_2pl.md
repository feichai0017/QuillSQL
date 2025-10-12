# MVCC and 2PL

Of the four ACID properties, **Isolation** is often the most complex to implement. It ensures that concurrent transactions do not interfere with each other, making it appear as if each transaction is executing sequentially, one after another. Without proper isolation, a database would suffer from concurrency-related anomalies like dirty reads, non-repeatable reads, and phantom reads.

Databases typically use two main strategies for concurrency control:

1.  **Pessimistic Concurrency Control**: Assumes conflicts are likely and prevents them from happening by using locks. The most common protocol is **Two-Phase Locking (2PL)**.
2.  **Optimistic Concurrency Control**: Assumes conflicts are rare. Transactions proceed without locking, and the database validates at commit time that no conflicts occurred. A popular variant is **Multi-Version Concurrency Control (MVCC)**.

QuillSQL, like many modern relational databases (e.g., PostgreSQL, Oracle), implements a powerful **hybrid model that combines MVCC with 2PL**.

## 1. MVCC: Reading without Blocking

The core idea of MVCC is **"writers don't block readers, and readers don't block writers."** This is achieved by never overwriting data in-place. When a row is updated, the database creates a *new version* of that row, preserving the old one.

### Version Chains and `TupleMeta`

As discussed in the [Storage Engine](../storage/table_heap.md) chapter, every tuple on disk has associated metadata (`TupleMeta`) that includes:
- `insert_txn_id`: The ID of the transaction that created this version.
- `delete_txn_id`: The ID of the transaction that marked this version as deleted.
- `prev_version` / `next_version`: Pointers (RIDs) that form a linked list of versions for a single logical row, called the **version chain**.

### Transaction Snapshots

When a transaction begins, it asks the `TransactionManager` for a **snapshot** of the database state. This snapshot, defined in `transaction/mvcc.rs`, contains three key pieces of information:

- `xmin`: The oldest active transaction ID at the time of the snapshot. Any transaction with an ID less than `xmin` is guaranteed to be either committed or aborted.
- `xmax`: The next transaction ID to be assigned. Any transaction with an ID greater than or equal to `xmax` was not yet started when the snapshot was taken.
- `active_txns`: A list of all other transaction IDs that were active when the snapshot was taken.

### The Visibility Check

When a transaction scans the database, for every tuple version it encounters, it performs a **visibility check** using its snapshot. The logic in `MvccSnapshot::is_visible` determines if the version should be "seen" by the current transaction. In simplified terms, a tuple version is visible if:

1.  Its `insert_txn_id` belongs to a transaction that was already committed before our snapshot was taken.
2.  **AND** its `delete_txn_id` is either not set, OR it belongs to a transaction that was not yet committed when our snapshot was taken.

This mechanism elegantly solves several concurrency problems. Since a reader transaction only ever sees a consistent snapshot of the database, it is completely immune to changes being made by other concurrent writer transactions. This prevents dirty reads and non-repeatable reads.

## 2. Two-Phase Locking (2PL): Preventing Write-Write Conflicts

While MVCC is excellent for read-write conflicts, it does not, by itself, prevent two transactions from trying to modify the same logical row at the same time (a write-write conflict). This is where locking comes in.

QuillSQL implements a strict **Two-Phase Locking** protocol via the `LockManager` (`transaction/lock_manager.rs`).

### The Two Phases

1.  **Growing Phase**: The transaction can acquire new locks as it executes.
2.  **Shrinking Phase**: Once the transaction releases its first lock, it cannot acquire any new locks.

In practice, QuillSQL uses **Strict 2PL**, where the shrinking phase is delayed until the transaction commits or aborts. All locks are held until the very end.

### Hierarchical Locking (Intention Locks)

To be efficient, the `LockManager` uses a multi-granularity, hierarchical locking scheme. Before a transaction can take a fine-grained lock on a row (a `Shared` or `Exclusive` lock), it must first acquire a coarser-grained **intention lock** on the table.

- **`IntentionShared (IS)`**: Signals the intent to read rows from the table.
- **`IntentionExclusive (IX)`**: Signals the intent to modify rows in the table.

This prevents a transaction wanting to lock the entire table from conflicting with another transaction that is already modifying a single row. For example, a `SELECT * FROM users FOR UPDATE` (which needs an `X` lock on the table) will be blocked if another transaction already holds an `IX` lock on `users`.

### Deadlock Detection

When two or more transactions are waiting for each other to release locks in a circular chain, a **deadlock** occurs. The `LockManager` detects this by building a **waits-for graph**. When a transaction `T1` has to wait for a lock held by `T2`, an edge `T1 -> T2` is added to the graph. If adding an edge creates a cycle, a deadlock is detected, and one of the transactions (the victim) is immediately aborted to break the cycle.

## 3. The Hybrid Model: MVCC + 2PL in Action

In QuillSQL, these two mechanisms work in concert:

- A **reader** (`SELECT`) transaction acquires an MVCC snapshot. It uses this snapshot to determine visibility. It only needs to acquire `Shared` (S) locks on the rows it reads to prevent other transactions from modifying them, thus ensuring repeatable reads in higher isolation levels.

- A **writer** (`UPDATE`, `DELETE`) transaction must acquire an `Exclusive` (X) lock on the specific row it intends to modify. Once the lock is granted, it knows no other writer can interfere. It can then safely create a new tuple version as part of the MVCC protocol.

This hybrid approach provides the best of both worlds: reads are fast and non-blocking, while write-write conflicts are safely prevented by the locking protocol.

---

## For Study & Discussion

1.  **Isolation Levels**: QuillSQL supports multiple SQL isolation levels. How does the behavior of MVCC snapshots and 2PL change between `ReadCommitted` and `RepeatableRead`? In `ReadCommitted`, a transaction gets a new snapshot for every statement, whereas in `RepeatableRead`, it uses the same snapshot for the entire transaction. What concurrency anomalies does this difference prevent?

2.  **Phantom Reads**: Even with MVCC and row-level 2PL, a `RepeatableRead` transaction can suffer from *phantom reads*. Imagine `T1` runs `SELECT COUNT(*) FROM users WHERE age > 30`. Then, `T2` inserts a new user with `age = 40` and commits. If `T1` runs its `SELECT` query again, it will see a new "phantom" row that wasn't there before. How can a database prevent this to achieve the `Serializable` isolation level? (Hint: research predicate locking and index-range locking).

3.  **Deadlock Handling**: QuillSQL detects deadlocks by building a waits-for graph and aborting a transaction. What is an alternative strategy for handling deadlocks? For example, what are the pros and cons of using lock timeouts instead of cycle detection?

4.  **Programming Exercise (Advanced)**: A full implementation of `Serializable` isolation often requires index-range locking to prevent phantoms. Extend the `LockManager` to support locking a *range* of keys within a B+Tree index. This would require a new lock type and a way to check for overlapping ranges. When a `SELECT ... WHERE age > 30` query runs, it would place a shared lock on the `(30, +∞)` range in the index on the `age` column, preventing any other transaction from inserting a new user with an age in that range.
