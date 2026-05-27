# Transaction Module

`src/transaction/` enforces the Atomicity and Isolation parts of ACID. It combines MVCC
with strict two-phase locking so reads and writes can proceed concurrently without
violating correctness.

---

## Main Components

| Type | Role |
| ---- | ---- |
| `TransactionManager` | Creates/commits/aborts transactions, assigns txn & command ids, and tracks recovered status. |
| `Transaction` | Stores state, held locks, undo chain, and cached snapshot. |
| `TxnContext` | Statement-time wrapper exposing MVCC + locking helpers to Holt table/index code. |
| `LockManager` | Multi-granularity locking (IS/IX/S/SIX/X) with deadlock detection. |
| `TransactionSnapshot` | Tracks `xmin/xmax/active_txns` for visibility checks. |

---

## Workflow

1. `Database::run` intercepts transaction control statements and calls
   `TransactionManager::begin/commit/abort`.
2. Each SQL statement that reaches the Holt provider builds a `TxnContext`, yielding a
   fresh command id and snapshot.
3. DataFusion drives the physical plan; Holt scan/DML hooks call
   `TxnContext::lock_table/lock_row` to obey strict 2PL.
4. Holt table and index handles record undo, acquire locks, and write through Holt.
5. Commit: mark the transaction committed, persist final Holt txn status, and release
   locks.
6. Abort: walk the undo list, restore row/index state through Holt handles, persist final
   Holt txn status, and release locks.

---

## MVCC Details

- `TupleMeta` stores inserting/deleting txn ids and command ids. `read_visible_tuple`
  checks snapshots and, if needed, rewinds to the latest visible version.
- Isolation levels:
  - **Read Uncommitted** – minimal snapshot caching.
  - **Read Committed** – refresh snapshot each command to avoid dirty reads.
  - **Repeatable Read / Serializable** – capture the snapshot once; RR releases shared
    locks at statement end, Serializable holds them to commit to avoid phantoms.
- UPDATE skips versions created by the same `(txn_id, command_id)` to avoid looping back
  over freshly inserted tuples.

---

## Locking

- Multi-granularity hierarchy: table-level IS/IX/S/SIX/X plus row-level S/X.
- Deadlock detection: `LockManager` maintains a wait-for graph and periodically chooses a
  victim (usually the longest waiter).
- Release policy: exclusive/intent locks stay until commit; RR drops shared row locks at
  statement end, Serializable waits until commit.

---

## Interactions

- **DataFusion table provider** – scans and DML enter QuillSQL through
  `HoltScanExec`, `insert_into`, `delete_from`, and `update`.
- **Holt handles** – handles call `TxnContext` before mutating Holt rows/indexes; MVCC
  metadata lives in `TupleMeta`.
- **Holt** – committed/aborted statuses are persisted in the Holt `txn` tree and recovered
  before execution resumes.

---

## Teaching Ideas

- Change `DatabaseOptions::default_isolation_level` and compare SELECT behaviour under
  RC vs RR.
- Write a unit test that deadlocks two transactions and watch `LockManager` pick a victim.
- Implement statement-level snapshot refresh or Serializable Snapshot Isolation (SSI) as
  an advanced exercise.

## Lab Walkthrough (à la CMU 15-445)

1. **Warm-up** – Start two sessions, run `BEGIN; SELECT ...;` under RC vs RR, and trace
   which snapshot `TxnContext` installs by logging `txn.current_command_id()`.
2. **MVCC visibility** – Extend the `transaction_tests.rs` suite with a scenario where
   `txn1` updates a row while `txn2` reads it. Instrument `TupleMeta` printing so
   students see how `(insert_txn_id, delete_txn_id)` change as versions are linked.
3. **Undo tracing** – Force an abort after a multi-index UPDATE. Watch the undo stack
   entries unfold: `Insert` removes the new version + index entries, `Delete` restores
   the old version + keys.
4. **Restart drill** – Insert committed and rolled-back rows, reopen the database, and
   inspect which `TupleMeta` versions are visible after Holt transaction status recovery.

---

Further reading: [MVCC and 2PL](../transaction/mvcc_and_2pl.md)
