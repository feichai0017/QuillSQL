# The ARIES Recovery Protocol

Of the four ACID properties, **Durability** is the one that guarantees that once a transaction is committed, its changes will survive any subsequent system failure. In a disk-based database, this is achieved through a careful and robust recovery protocol. QuillSQL implements a recovery strategy inspired by the well-known **ARIES** (Algorithm for Recovery and Isolation Exploiting Semantics) protocol, centered around a **Write-Ahead Log (WAL)**.

## 1. The Write-Ahead Logging (WAL) Principle

The fundamental rule of WAL is simple but powerful:

> **Before a modified data page is ever written from memory back to disk, the log record describing that modification must first be written to durable storage (the log file).**

This allows the database to perform most of its data modifications in memory on the [Buffer Pool](../modules/buffer.md) without needing to synchronously write every changed page to disk, which would be extremely slow. In case of a crash, the database can use the log to reconstruct the state of the system and ensure all committed changes are present and all uncommitted changes are undone.

### Log Records (`WalRecord`)

The WAL is a sequential, append-only file composed of **log records**. Each record is assigned a unique, monotonically increasing **Log Sequence Number (LSN)**. A log record in QuillSQL (`src/recovery/wal_record.rs`) generally contains:
- **LSN**: The address of the log record.
- **Transaction ID**: The ID of the transaction that generated this record.
- **Payload**: The actual content of the log record, which varies by type.

QuillSQL uses several types of log records:

- **`Transaction`**: Marks the `BEGIN`, `COMMIT`, or `ABORT` of a transaction.
- **`PageWrite` / `PageDelta`**: Physical/Physiological records describing a change to a page. `PageWrite` contains a full image of the page (used for the first write after a checkpoint, a technique called First-Page-Write or FPW), while `PageDelta` contains only the changed bytes.
- **`HeapInsert` / `HeapUpdate` / `HeapDelete`**: Logical records describing a high-level heap operation. These are primarily used for generating precise undo operations.
- **`Checkpoint`**: A special record that marks a point of partial durability, allowing the log to be truncated.
- **`CLR` (Compensation Log Record)**: A special record written during recovery to describe an **undo** action. CLRs are redo-only and are never undone themselves.

## 2. The ARIES Recovery Protocol

On database startup, the `RecoveryManager` (`recovery/recovery_manager.rs`) is invoked to `replay()` the WAL. This process follows the three phases of ARIES.

### Phase 1: Analysis

The goal of the Analysis phase (`recovery/analysis.rs`) is to figure out the state of the database at the exact moment of the crash.

1.  It starts by finding the last successful `Checkpoint` record in the WAL.
2.  It then scans the log **forward** from that checkpoint to the end of the log.
3.  During this scan, it builds two critical data structures:
    -   **Active Transaction Table (ATT)**: A list of all transactions that have a `BEGIN` record but no corresponding `COMMIT` or `ABORT` record. These are the potential "loser" transactions that will need to be undone.
    -   **Dirty Page Table (DPT)**: A list of all pages that were modified in the buffer pool but might not have been written to disk before the crash. For each dirty page, it records the LSN of the *first* log record that made it dirty (this is called the `recLSN`).

At the end of this phase, the `RecoveryManager` knows exactly which transactions to roll back and the earliest point in the log from which it needs to start re-applying changes.

### Phase 2: Redo (Repeating History)

The goal of the Redo phase (`recovery/redo.rs`) is to restore the database to its exact state at the moment of the crash, including all changes from both committed and uncommitted (loser) transactions.

1.  The Redo phase finds the smallest `recLSN` from the Dirty Page Table built during Analysis. This LSN is the starting point for the redo scan.
2.  It scans the log **forward** from this starting point.
3.  For every log record it encounters that describes a physical change to a page (e.g., `PageWrite`, `PageDelta`, `Heap*`), it re-applies the change. This is idempotent: if the change is already present on the page (because it was successfully flushed to disk before the crash), re-applying it does no harm.

At the end of this phase, the database state on disk is identical to how it was in memory right before the crash.

### Phase 3: Undo (Rolling Back Losers)

The final phase (`recovery/undo.rs`) is responsible for rolling back all the "loser" transactions identified during Analysis.

1.  The `UndoExecutor` takes the list of loser transactions.
2.  For each loser transaction, it scans the WAL **backwards**, following the chain of log records for that transaction.
3.  For each operation record (like `HeapInsert`, `HeapUpdate`), it performs the logical inverse operation:
    -   To undo an `Insert`, it performs a `Delete`.
    -   To undo a `Delete`, it restores the deleted data.
    -   To undo an `Update`, it restores the data from before the update.
4.  **Crucially**, for every undo action it performs, it writes a **Compensation Log Record (CLR)** to the WAL. The CLR contains information about the undo action and, importantly, a pointer to the *next* log record that needs to be undone for that transaction. 

This use of CLRs makes the recovery process itself crash-proof. If the system crashes *during the undo phase*, the next time recovery runs, it will see the CLRs. It will simply continue the undo process from where it left off by following the pointers in the CLRs, without ever having to undo an undo action.

## 3. Checkpoints

If the log were allowed to grow forever, recovery would become slower and slower. **Checkpoints** are a background process that periodically creates a point of partial durability.

A checkpoint does the following:
1.  Temporarily stops new transactions from starting.
2.  Writes a `BEGIN_CHECKPOINT` record to the WAL, containing the current ATT and DPT.
3.  Flushes all dirty pages from the buffer pool to disk.
4.  Writes an `END_CHECKPOINT` record to the WAL.

Once a checkpoint is complete, the `RecoveryManager` knows that all changes described in log records before the `BEGIN_CHECKPOINT` record are safely on disk. Therefore, those older parts of the WAL file are no longer needed for recovery and can be truncated or recycled, keeping the recovery process efficient.

---

## For Study & Discussion

1.  **Repeating History**: The Redo phase re-applies changes from *all* transactions, including those that will be undone later (the "losers"). This seems wasteful. Why is this "repeating history" approach a core principle of ARIES? What would go wrong if we tried to only redo changes from committed transactions?

2.  **Compensation Log Records (CLRs)**: What specific problem would occur if the system crashed during the Undo phase and we *didn't* write CLRs? How does a CLR's "redo-only" nature make the recovery process idempotent and robust against repeated crashes?

3.  **Checkpointing Frequency**: What are the performance trade-offs between checkpointing very frequently versus very infrequently? Consider both normal runtime performance and the time it takes to recover after a crash.

4.  **Programming Exercise**: Add a new WAL record type. For example, a `HeapReclaim` record that logs the physical removal of a dead tuple by a vacuum process. To do this, you would need to:
    a.  Add a variant to the `HeapRecordPayload` enum in `wal_record.rs`.
    b.  Update the `codec` functions to handle its serialization.
    c.  Decide what information the `HeapReclaim` record needs to contain.
    d.  Implement the `redo` logic for this new record in the appropriate `ResourceManager`. What should happen when you redo a `HeapReclaim` record?
