# Recovery Manager (WAL)

The Recovery Manager is the component that ensures the **Durability** aspect of ACID. It guarantees that once a transaction is committed, its effects are permanent, even in the face of system crashes or power failures.

This is achieved through a **Write-Ahead Log (WAL)** and a recovery protocol inspired by **ARIES**.

## Core Concepts

- **Write-Ahead Logging**: The fundamental principle is that any change to a data page must be recorded in a log file on durable storage *before* the data page itself is written back to disk.

- **Log Records**: The WAL is a sequence of records, each with a unique Log Sequence Number (LSN). Records describe changes (`PageDelta`, `HeapInsert`), transaction outcomes (`Commit`, `Abort`), or internal state (`Checkpoint`).

- **ARIES Protocol**: On startup, the database replays the log to restore a consistent state. This involves three phases: **Analysis** (to figure out what was happening during the crash), **Redo** (to re-apply all changes since the last checkpoint), and **Undo** (to roll back any transactions that did not commit).

This section contains:

- **[The ARIES Protocol in QuillSQL](./../recovery/aries.md)**: A deep dive into the three phases of recovery and how they are implemented.