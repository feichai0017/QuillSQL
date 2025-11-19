# Transaction Module

`src/transaction/` enforces the Atomicity and Isolation parts of ACID. It combines MVCC
with strict two-phase locking so reads and writes can proceed concurrently without
violating correctness.

---

## Main Components

| Type | Role |
| ---- | ---- |
| `TransactionManager` | Creates/commits/aborts transactions, assigns txn & command ids, coordinates WAL. |
| `Transaction` | Stores state, held locks, undo chain, and cached snapshot. |
| `TxnContext` / `TxnRuntime` | Execution-time wrapper exposing MVCC + locking helpers. |
| `LockManager` | Multi-granularity locking (IS/IX/S/SIX/X) with deadlock detection. |
| `TransactionSnapshot` | Tracks `xmin/xmax/active_txns` for visibility checks. |

---

## Workflow

1. `SessionContext` calls `TransactionManager::begin` to create a transaction.
2. Each SQL statement builds a `TxnRuntime`, yielding a fresh command id and snapshot.
3. Operators call `TxnContext::lock_table/lock_row` to obey strict 2PL.
4. `TableHandle::insert/delete/update` records undo, acquires locks, and emits WAL via
   `TxnContext`.
5. Commit: write a `Commit` record → flush depending on `synchronous_commit` → release
   locks.
6. Abort: walk the undo list, write CLRs, restore heap/index state, release locks.

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

- **ExecutionContext** – all helpers (lock acquisition, visibility checks, undo logging)
  are exposed here, so physical operators never touch `LockManager` directly.
- **StorageEngine** – handles call `TxnContext` before mutating heaps/indexes; MVCC metadata
  lives in `TupleMeta`. Deletes and updates now push the affected index keys into the undo
  chain so heap/index WAL stay in lockstep.
- **Recovery** – Begin/Commit/Abort records emitted here drive ARIES undo/redo.
- **Background** – MVCC vacuum reads `TransactionManager::oldest_active_txn()` to compute
  `safe_xmin`.

---

## Teaching Ideas

- Change `DatabaseOptions::default_isolation_level` and compare SELECT behaviour under
  RC vs RR.
- Write a unit test that deadlocks two transactions and watch `LockManager` pick a victim.
- Implement statement-level snapshot refresh or Serializable Snapshot Isolation (SSI) as
  an advanced exercise.

## Lab Walkthrough (à la CMU 15-445)

1. **Warm-up** – Start two sessions, run `BEGIN; SELECT ...;` under RC vs RR, and trace
   which snapshot `TxnRuntime` installs by logging `txn.current_command_id()`.
2. **MVCC visibility** – Extend the `transaction_tests.rs` suite with a scenario where
   `txn1` updates a row while `txn2` reads it. Instrument `TupleMeta` printing so
   students see how `(insert_txn_id, delete_txn_id)` change as versions are linked.
3. **Undo tracing** – Force an abort after a multi-index UPDATE. Watch the undo stack
   entries unfold: `Insert` removes the new version + index entries, `Delete` restores
   the old version + keys. Map each step to the WAL records that are written.
4. **Crash drill** – Add `panic!()` right after `TransactionManager::commit` is called
   but before locks are released. Reboot, run recovery, and inspect the loser list;
   students can connect the dots between undo actions, CLRs, and ARIES theory.

---

Further reading: [MVCC and 2PL](../transaction/mvcc_and_2pl.md)
