# Transactions & Concurrency

## Overview

QuillSQL currently provides single-node ACID-style transactions backed by two-phase locking (2PL) and write-ahead logging. The subsystem is designed to be simple, explicit, and observable—no MVCC yet, but easy to reason about.

```
SessionContext
   ↓ (start/commit/abort, set modes)
TransactionManager ───────► WalManager (begin/commit/abort records)
   │                        │
   ├─ LockManager (table/row IS/IX/S/SIX/X locks)
   └─ Undo stack (Insert/Update/Delete logical undo)
```

### Components

- **SessionContext** (`src/session/mod.rs`)
  - Stores default isolation level, access mode (ReadWrite/ReadOnly), autocommit flag, and the active `Transaction` handle.
  - Applies `SET TRANSACTION` / `SET SESSION TRANSACTION` requests, so newly started transactions inherit the correct modes.
  - Enforces `READ ONLY` by marking transactions and letting the executor check before DML.

- **Transaction** (`src/transaction/transaction.rs`)
  - Holds transaction id, isolation level, access mode, state (Running/Tainted/Committed/Aborted), WAL LSNs, and an undo stack.
  - Undo entries record sufficient information to roll back inserts, updates, and deletes.

- **TransactionManager** (`src/transaction/transaction_manager.rs`)
  - Creates transactions (`begin`), logs WAL records, and finalizes undo/redo sequences (`commit`/`abort`).
  - Provides helpers for executors to acquire table/row locks while respecting 2PL rules.
  - Maintains per-transaction lock tracking so locks are released safely at commit/abort.

- **LockManager** (`src/transaction/lock_manager.rs`)
  - Centralized lock table supporting multi-granularity modes: `IS`, `IX`, `S`, `SIX`, `X`.
  - Uses FIFO queues per resource plus a wait-for graph; detects deadlocks and logs them.
  - Trace logs (`lock granted`, `wait edge`) improve visibility when diagnosing contention.

- **WalManager** (`src/recovery/wal.rs`)
  - Records transaction begin/commit/abort and logical undo payloads.
  - Commit respects `synchronous_commit`; asynchronous mode still flushes best-effort.

## Isolation Levels

| Level           | Behavior                                                               |
| --------------- | ---------------------------------------------------------------------- |
| ReadUncommitted | No shared row locks; writer may see dirty reads.                       |
| ReadCommitted   | Shared row locks held during read then released; prevents dirty reads. |
| RepeatableRead  | Shared locks retained until commit, avoiding non-repeatable reads.     |
| Serializable    | Same as RR today (strict 2PL).                                         |

Additionally, `READ ONLY` transactions are supported; any `INSERT/UPDATE/DELETE` attempted will fail with an execution error.

## Execution Checks

`ExecutionContext::ensure_writable` is invoked by `INSERT`, `UPDATE`, and `DELETE` physical operators. If the current transaction is `READ ONLY`, the operator logs a warning and returns an error before performing any work.

## Tests

`src/tests/transaction_tests.rs` covers:

- `read_only_transaction_rejects_dml` – ensures DML fails in `READ ONLY` transactions.
- `read_committed_allows_update_after_select` – demonstrates RC visibility semantics.
- `repeatable_read_blocks_update_until_commit` – asserts that RR reader holds locks and writer is blocked until commit (uses `TransactionManager` directly).

Lock manager unit tests exercise shared/exclusive compatibility and row-level blocking.

## Future Work

- MVCC snapshot support for snapshot isolation / true serializable semantics without long-held locks.
- Predicate/next-key locking to prevent phantom reads.
- Query planner awareness of transaction modes for smarter lock acquisition.
- User-visible metrics (lock waits, deadlock count, active transaction list).
