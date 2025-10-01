# QuillSQL Architecture

This document gives a high-level overview of QuillSQL’s end-to-end architecture, major components, and data flow.

## System Overview

```
+-------------------------------+        +---------------------------+
|            Client             |        |         Frontend          |
|  - CLI (bin/client)          |        |  Static UI (public/)      |
|  - HTTP (bin/server, /api)   |<------>|  Calls /api/sql(_batch)   |
+-------------------------------+        +---------------------------+
                 |
                 v
+---------------------------------------------------------------+
|                           SQL Layer                           |
|  sql::parser  ->  plan::LogicalPlanner  ->  LogicalPlan AST   |
+---------------------------------------------------------------+
                 |
                 v
+---------------------------------------------------------------+
|                      Transaction Layer                       |
|  session::SessionContext  ->  transaction::TransactionManager |
|  -> LockManager (table/row 2PL) + WAL integration             |
+---------------------------------------------------------------+
                |
                v
+------------------------+     +--------------------------------+
|  Optimizer (logical)   | --> |  Optimized LogicalPlan         |
|  - Eliminate/Merge/PD  |     |  (rule-based, no stats)        |
+------------------------+     +--------------------------------+
                 |
                 v
+---------------------------------------------------------------+
|                        Physical Planning                      |
|  plan::PhysicalPlanner -> execution::physical_plan::*         |
|  - SeqScan / IndexScan / Project / Filter / Sort / Join / ... |
+---------------------------------------------------------------+
                 |
                 v
+---------------------------------------------------------------+
|                 Execution Engine (Volcano model)              |
|  - VolcanoExecutor::init/next                                 |
|  - Pull-based iterators produce Tuples                        |
+---------------------------------------------------------------+
                 |
                 v
+-------------------------+        +-----------------------------+
|       Storage Layer     |        |       Buffer Pool           |
|  - TableHeap (heap)     |        |  - Frames + PageTable       |
|  - B+Tree Index         |        |  - LRU-K + TinyLFU (opt)    |
|  - Page codecs          |        |  - Pin/Unpin RAII           |
+-------------------------+        +-----------------------------+
                 |                                   |
                 v                                   v
          +----------------------+        +-------------------------+
          |  Disk Scheduler      |<------>|   Async I/O background  |
          |  (channels + thread) |        |   read/write/alloc      |
          +----------------------+        +-------------------------+
```

## Key Components

- SQL Layer
  - Parser builds AST using sqlparser.
  - LogicalPlanner converts AST -> LogicalPlan (CreateTable/Index, Project, Filter, Join, Sort, Values, Insert, Update, TableScan, Limit, Aggregate, ...).

- Optimizer (Rule-based)
  - A few safe rules (EliminateLimit, MergeLimit, PushDownLimit into Sort) without statistics.

- Physical Planning
  - LogicalPlan -> PhysicalPlan (one-to-one mapping, recursive construction).

- Execution Engine (Volcano)
  - Each operator implements `init()` and `next()`; pull model materializes result tuples.

- Storage Layer
  - TableHeap: linked heap pages with tuple metadata, append-friendly.
  - B+Tree Index: B-link pages (internal and leaf) for high concurrency, header page tracks root.
  - Page codecs: encode/decode pages and tuples to/from fixed-size bytes.

- Buffer Pool Manager
  - Page table (DashMap<PageId, FrameId>), per-frame RwLock, RAII guards.
  - LRU-K replacer (optionally TinyLFU admission to resist cache pollution).
  - Safe eviction, flush-on-evict, and explicit `flush_all_pages()` for durability.

- Disk I/O
  - Asynchronous scheduler thread (channels) handles Read/Write/Allocate/Deallocate.

- Web Service
  - Axum router exposes `/api/sql` and `/api/sql_batch`; serves static `public/` and `docs/`.

## Streaming SeqScan (Ring Buffer Bypass)

- Motivation: large sequential scans can evict hot pages from the buffer pool.
- Design: `DirectRingBuffer` reads pages via the DiskScheduler into a small in-memory window (readahead) and decodes them without admitting them into the buffer pool.
- Trigger:
  - Enabled for full table scans when approx table pages ≥ threshold (default ≈ BUFFER_POOL_SIZE/4), or forced via env `QUILL_STREAM_SCAN=1`.
  - Per-query planner hint via env `QUILL_STREAM_HINT=1/0` overrides the auto rule.
- Caveats: With no WAL/MVCC yet, visibility relies on explicit `flush_all_pages()` before direct reads (the iterator does this on first page).

## Concurrency Highlights

- Buffer pool uses per-page RwLock; RAII guards manage pin/unpin and evictability.
- Inflight guards serialize concurrent loads for the same page to prevent thundering herd.
- B+Tree relies on B-links and context-managed latch crabbing to avoid deadlocks.

## Observability & Configuration

- Logging via `RUST_LOG`.
- Key env vars: `QUILL_DB_FILE`, `PORT`/`QUILL_HTTP_ADDR`, `QUILL_STREAM_SCAN`, `QUILL_STREAM_THRESHOLD`, `QUILL_STREAM_READAHEAD`, `QUILL_STREAM_HINT`, `QUILL_DEBUG_LOCK`.

## Future Work

- Cost model and statistics-driven planning.
- Bitmap Heap Scan for large index ranges.
- WAL/MVCC for crash recovery and snapshot isolation.

## Transaction & Session Subsystem

- `SessionContext`
  - Per-connection state: default isolation/access mode, autocommit, active transaction handle.
  - Applies `SET TRANSACTION`/`SET SESSION TRANSACTION` requests and propagates modes when a transaction is started.
- `TransactionManager`
  - Entry point for `begin / commit / abort`.
  - Assigns transaction IDs, logs begin/commit/abort records into WAL, maintains undo stacks for DML.
  - Exposes helpers for executors to acquire table/row locks respecting isolation and access modes (ReadWrite vs ReadOnly).
- `LockManager`
  - Central 2PL implementation with table+row granularity (IS/IX/S/SIX/X).
  - Maintains per-resource queues, wait-for graph and deadlock detection; shared row locks can be released eagerly for RC, retained for RR/Serializable.
  - Trace logging records grant/wait/deadlock events for observability.
- Isolation support
  - `ReadUncommitted`: direct access, no row locks.
  - `ReadCommitted`: S locks acquired but released after read.
  - `RepeatableRead` / `Serializable`: S locks retained until commit; RR still relies on explicit locking (no MVCC yet).
  - Transactions can be marked `READ ONLY`, causing DML to fail early via `ExecutionContext::ensure_writable`.
- WAL interaction
  - TransactionManager writes begin/commit/abort and logical undo records.
  - Buffer pool ensures page_lsn ordering; commit waits respect synchronous/asynchronous modes.
