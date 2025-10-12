# Write-Ahead Logging (WAL) — Architecture and Recovery

This document details the architecture of QuillSQL's Write-Ahead Logging (WAL) system, which is essential for ensuring crash recovery, data durability, and atomicity (the 'A' and 'D' in ACID).

## 1. High-Level Overview

The core principle of WAL is simple but powerful: **before any data page modification is written to the main database file, a log record describing that change must first be written and persisted to a separate log file.**

This approach provides two primary benefits:
1.  **Durability & Atomicity**: If the system crashes, we can replay the log to restore the database to a consistent state, redoing committed changes and undoing incomplete ones.
2.  **Performance**: It transforms many small, random disk writes (updating various pages in the data file) into a single, highly efficient sequential append to the log file. The actual data file pages can then be flushed to disk in the background in a more optimized manner.

## 2. Architecture & Components

Our WAL implementation is inspired by the ARIES recovery algorithm and consists of several key components that work in concert.

```
+-------------------+   writes   +-----------------+   queued    +-----------------------+
| Execution Engine  |----------->|   WalManager    |-----------> | DiskScheduler/io_uring |
| (e.g., TableHeap) |            | (in-memory buf) |   (MPSC)    | (shared worker pool)  |
+-------------------+            +-----------------+             +-----------------------+
        |                                  |                              |
(modifies pages)                       (flushes)                     (reads/writes)
        v                                  v                              v
+-------------------+  (dirty pages) +---------------+            +----------------+
|  Buffer Pool Mgr  |<------------->|  Checkpointer |----------->|   WAL Files    |
+-------------------+                +---------------+            +----------------+
        |  (flushes pages)                                            ^
        v                                                             |
+-------------------+                +-------------------+            |
|  Data Files (.db) |                |  RecoveryManager  |<-----------+
+-------------------+                +-------------------+
```

-   **`WalManager` (`src/recovery/wal.rs`)**: The central coordinator of the WAL system.
    -   **Responsibilities**: Assigns unique Log Sequence Numbers (LSNs), manages an in-memory log buffer, and provides the primary interface (`append_record_with`) for other parts of the system to write log records. It also tracks the `durable_lsn`—the LSN up to which logs have been successfully flushed to disk.
    -   **FPW (First-Page-Write)**: Tracks pages modified since the last checkpoint to enforce a full-page write for the first modification, protecting against torn-page scenarios during recovery.
    -   **I/O Runtime Integration**: `WalManager` now submits WAL writes directly to the shared `DiskScheduler` io_uring workers. Each flush request enqueues sequential writes (and optional `fdatasync`) and waits for the corresponding completion events to guarantee durability ordering.

-   **`Checkpointer` (Background Thread)**: A periodic process crucial for bounding recovery time.
    -   **Responsibilities**:
        1.  Writes a `Checkpoint` record to the WAL. This record contains a snapshot of the current state, including the list of active transactions (ATT) and the Dirty Page Table (DPT). The DPT lists all pages that are dirty in the buffer pool, along with the LSN of the log record that first made them dirty (`recLSN`).
        2.  After a checkpoint is durable, it triggers the recycling of old, no-longer-needed WAL segment files, preventing infinite log growth.

-   **`RecoveryManager` (`src/recovery/recovery_manager.rs`)**: The engine that performs crash recovery upon database startup. It implements a three-phase recovery process and talks to the shared `DiskScheduler` (backed by io_uring) for data-page redo/undo. WAL replay and data I/O remain decoupled but reside in the same module for tighter invariants.

-   **`ControlFile` (`src/recovery/control_file.rs`)**: A small, critical file (`control.dat`) that bootstraps the recovery process. It stores essential metadata like the system identifier, WAL segment size, and, most importantly, the LSN of the last successful checkpoint.

## 3. Log Record Format

All log records are encapsulated in a `WalFrame` and written to disk. The structure is defined in `src/recovery/wal_record.rs`.

-   **`WalFrame`**: Contains a header with metadata (`lsn`, `prev_lsn`, CRC for integrity) and a `WalRecordPayload`.
-   **`WalRecordPayload`**: An enum representing the actual log content. We use a mix of logging types:
    -   **Physical/Physiological Logs (for Redo)**:
        -   `PageWrite`: A full 4KB image of a page. Used for FPW.
        -   `PageDelta`: Records only the changed byte range within a page (`offset`, `data`). This is much more efficient for subsequent writes to the same page.
    -   **Logical Logs (for Undo)**:
        -   `HeapInsert`, `HeapUpdate`, `HeapDelete`: These records describe the logical operation performed on the table heap. For example, `HeapUpdate` contains both the new and old versions of the tuple, allowing for a precise rollback. Each record contains the `op_txn_id` to identify which transaction performed the action.
    -   **Control Flow Logs**:
        -   `Transaction`: Records `Begin`, `Commit`, and `Abort` events for transactions.
        -   `Checkpoint`: The record written by the `Checkpointer`.
        -   `Clr (Compensation Log Record)`: A special logical log written during the Undo phase to record that an operation has been undone. This ensures that if the system crashes during recovery, Undo operations are not applied more than once (idempotence).

## 4. Key Processes

### 4.1 Normal Operation (e.g., an `UPDATE`)

1.  **Logical Log**: The `TableHeap` first modifies a page in the buffer pool. It then immediately generates a logical `HeapUpdate` record containing the old and new tuple data and sends it to the `WalManager`.
2.  **Physical/Physiological Log**: When the `WritePageGuard` for the modified page is dropped, the buffer pool triggers a write-back hook. This hook calculates the physical changes to the page and generates either a `PageWrite` (on first touch) or a `PageDelta` record, which is also sent to the `WalManager`. The page's own LSN (`page_lsn`) is updated to match that of this physical log record.

### 4.2 Transaction Commit

-   A `Transaction(Commit)` record is written to the WAL.
-   If `synchronous_commit` is enabled, the transaction thread will block until the `WalManager` confirms that the commit record's LSN is durable (flushed to disk).

### 4.3 Crash Recovery Process

Upon restart, `RecoveryManager::replay()` is executed:

1.  **Analysis Phase**:
    -   It starts from the last checkpoint recorded in `control.dat` and scans the WAL forward to the end.
    -   It reconstructs the state at the moment of the crash: the set of uncommitted transactions (the "losers") and the dirty page table (DPT).

2.  **Redo Phase**:
    -   It finds the smallest `recLSN` from the DPT. This is the earliest point from which modifications might have been lost.
    -   It scans forward from this `recLSN`, reapplying all physical and physiological changes (`PageWrite`, `PageDelta`) to the data pages. This phase is idempotent and brings the database to its exact state at the time of the crash, including changes from "loser" transactions. CLR records are ignored.

3.  **Undo Phase**:
    -   It scans the WAL backward from the end.
    -   For each log record belonging to a "loser" transaction (identified via `op_txn_id`), it performs the corresponding logical inverse operation (e.g., for a `HeapInsert`, it deletes the tuple).
    -   Crucially, after performing an undo action, it writes a **CLR** to the WAL. This CLR points to the LSN of the record that was just undone. If a crash happens during Undo, upon restart, the recovery process will see the CLR during the Redo phase and know not to re-apply the already-undone operation.

## 5. Configuration

The WAL system's behavior can be tuned via `WalOptions` in `src/database.rs`, which can be set via command-line arguments or environment variables. Key parameters include:
-   `wal_segment_size`: The size of individual log files.
-   `wal_writer_interval_ms`: The wake-up interval for the background `WalWriter`.
-   `wal_checkpoint_interval_ms`: The frequency of checkpoints.
-   `synchronous_commit`: Whether commits wait for disk durability.

## 6. Transaction Manager Integration

The WAL stack is tightly coupled with the transaction subsystem:

- `TransactionManager::begin` writes a `Transaction(Begin)` record and stamps the transaction's `begin_lsn`.
- `commit` appends `Transaction(Commit)` and blocks on durability when `synchronous_commit` is enabled; async mode still calls `flush_until`.
- `abort` walks the transaction's undo stack, emits CLRs + logical undo records, then appends `Transaction(Abort)`.
- Executors record undo actions (insert/update/delete) so abort/rollback can replay the logical inverse.
- Isolation/access modes (`ReadUncommitted`, `ReadCommitted`, `RepeatableRead`, `Serializable`, `READ ONLY`) are managed in `SessionContext` and enforced by the planner/execution. `READ ONLY` transactions fail early on DML via `ExecutionContext::ensure_writable`, but still participate in WAL for consistency (BEGIN/COMMIT records).

## 7. Observability & Logging

- Lock manager adds trace logs (`lock granted`, `wait edge`) and WARN on deadlock detection, assisting investigation of blocking scenarios.
- WAL exposes `durable_lsn`, `max_assigned_lsn`, and background flush status.
- Integration tests in `transaction_tests.rs` cover read-only rejection, RC update visibility, and RR blocking behavior.
