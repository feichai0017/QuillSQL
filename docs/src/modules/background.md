# Background Services

`src/background/` hosts the asynchronous workers that keep a database healthy: WAL
writers, checkpoints, buffer flushers, and MVCC vacuum. A central registry makes it easy
to start/stop workers together—ideal for teaching how background maintenance supports
foreground queries.

---

## Responsibilities

- Start workers according to configuration (`WalOptions`, `MvccVacuumConfig`, etc.).
- Define lightweight traits (`CheckpointWal`, `BufferMaintenance`, `TxnSnapshotOps`) so
  workers can run without pulling in an async runtime.
- Provide `BackgroundWorkers`, a registry that tracks `WorkerHandle`s and shuts them down
  when `Database` drops.

---

## Built-in Workers

| Worker | Trigger | Behavior |
| ------ | ------- | -------- |
| WAL writer | `wal_writer_interval_ms` | Calls `WalManager::background_flush` to durably write log buffers. |
| Checkpoint | `checkpoint_interval_ms` | Captures dirty page / active txn tables and emits `Checkpoint` records to bound recovery. |
| Buffer writer | `bg_writer_interval` | Flushes dirty frames to reduce checkpoint pressure. |
| MVCC vacuum | `MvccVacuumConfig` | Removes obsolete tuple versions once `safe_xmin` advances. |

Every worker registers itself with `BackgroundWorkers`; `shutdown_all()` ensures threads
exit cleanly during tests or process teardown.

---

## Interactions

- **WalManager** – WAL writer and checkpoint workers operate on `Arc<dyn CheckpointWal>`.
- **BufferManager** – background flushers inspect dirty frames and help checkpoints
  capture consistent snapshots.
- **TransactionManager** – MVCC vacuum queries `TxnSnapshotOps` for `safe_xmin`.

---

## Teaching Ideas

- Tune `MvccVacuumConfig::batch_limit` and chart how quickly old tuple versions disappear.
- Disable a worker in tests to show why unflushed WAL or missing checkpoints lengthen
  recovery.
- Enable `RUST_LOG=background=info` to trace how these tasks complement foreground load.
