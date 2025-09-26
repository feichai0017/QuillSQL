# OLTP Architecture Roadmap

This document catalogs the components we must build to evolve QuillSQL from a single-user engine into a PostgreSQL-inspired OLTP database. The organization mirrors Postgres subsystems to highlight parity and gaps.

## 1. Transaction & Session Management
- **Transaction Manager**: Allocate `TransactionId`, track status (`in-progress`, `committed`, `aborted`), manage snapshots. Similar to PostgreSQL's `pg_xact` + `ProcArray`.
- **Session Context**: Per-connection state (current snapshot, subtransaction stack, GUCs). Needed once we expose concurrent clients.
- **Snapshot Interface**: Support `READ COMMITTED` and `REPEATABLE READ` by materializing visibility horizons (`xmin`, `xmax`, `active xids`). Mirrors `GetSnapshotData`.

## 2. MVCC Storage Layout
- **Tuple MVCC Metadata**: Extend heap tuple header with `xmin`, `xmax`, `command_id`, hint bits. Each new version points to previous via HOT-like chains.
- **Visibility Rules**: Implement `HeapTupleSatisfiesMVCC` equivalent for executor: determine if tuple is visible under current snapshot, handles committed/aborted xids.
- **Space Reuse**: After `xmax` becomes globally visible, tuples become dead and eligible for vacuum.

## 3. Write-Ahead Logging (WAL)
- **WAL Manager**: Append-only log buffer, LSN assignment, background walwriter, flush policy. Equivalent to `xlog.c`.
- **Page LSN Integration**: Every data page stores `page_lsn`. Buffer flush must ensure `page_lsn ≤ flushed_lsn` (Write-Ahead rule).
- **WAL Record Format**: Define record types (heap insert/update/delete, index changes, multi-insert, checkpoints). Store redo payload plus undo pointer.
- **Log Buffers & Sync**: Manage in-memory WAL buffers, synchronous commit settings, walwriter thread.

### Current status (2025-02)
- `WalManager` (`src/recovery/wal.rs`) 已经负责分配连续 LSN、缓存 WAL 记录，并在 `flush()` 时将批量记录写入磁盘段文件（默认 `wal_*.log`，16 MiB 滚动），可选 `fsync` 保证持久化。
- `BufferPoolManager::flush_page` (`src/buffer/buffer_pool.rs:447`) 会在刷脏页前检查 `page_lsn ≤ durable_lsn`，违背写前法则会被拒绝，从而要求调用方先通过 `WalManager::flush` 落盘日志。
- `Database::flush` (`src/database.rs:125`) 负责先调用 WAL 落盘，再刷新缓冲池；数据库在构造时会把 `WalManager` 注入缓冲池（`Database::new_on_disk_with_options`）。
- CLI 与 Server 入口提供配置项（命令行参数或 `QUILL_WAL_*` 环境变量）覆盖 WAL 目录、段大小、同步策略，便于部署时指定日志存储位置。
- 支持周期性后台 WAL 写线程：当配置 `writer_interval_ms` 时，`WalManager::start_background_flush` 会派生 walwriter 线程，以 Postgres 式周期刷写减轻前台刷盘压力。
- 当数据库持有 `DiskScheduler`、`IoUring` 时，WAL 写入也委托给调度器处理，通过新的 `DiskRequest::WriteWal` 路径统一 I/O 底层（线程池/io_uring），避免前台线程直接阻塞在文件写上。
- 新增 `WalReader`，可通过调度器从段文件顺序读取帧，为后续实现 WAL 重放/恢复流程打下基础。
- 表堆写路径（`src/storage/table_heap.rs`）会在插入/更新时生成 `PageWrite` WAL 记录，记录整页镜像并更新 `page_lsn`，确保崩溃后可按页重放。

## 4. Undo Logging & Rollback
- **Undo Records**: For UPDATE/DELETE store before images or logical diff to rollback uncommitted changes. Could live in per-transaction chains similar to Postgres `pg_clog` + undo.
- **Abort Handling**: Transaction manager replays undo chain to restore previous tuple versions and materialize aborted state.
- **Integration with WAL**: Undo records referenced in WAL for redo/undo (ARIES). When redoing, apply changes; during undo, use undo log to revert.

## 5. Locking & Concurrency Control
- **Lock Manager**: Table-level and row-level locks with modes (S, X, IS, IX). Backed by hash tables/quadratic tree like `LOCKTAG` in Postgres.
- **Deadlock Detection**: Wait-for graph builder, periodic detection (similar to `DeadLockCheck`).
- **Predicate/Index Locks**: Eventually needed for SERIALIZABLE isolation (predicate locking). For MVP focus on row locks.
- **Two-Phase Commit** (future): Infrastructure for distributed transactions (inspired by Postgres `twophase.c`).

## 6. Buffer Management Enhancements
- **Dirty Page Tracking**: Maintain dirty list with `page_lsn`. Required for checkpoints and WAL.
- **Checkpoint Coordination**: Force flush of dirty pages up to checkpoint LSN, store dirty page table.
- **Background Writer**: Periodically flush dirty buffers to smooth spikes.

## 7. Recovery Pipeline
- **Crash Recovery**: Implement ARIES phases: analysis (reconstruct active tx table), redo (reapply WAL), undo (rollback losers).
- **Checkpoints**: Create WAL records capturing dirty page list + active transactions. Determine checkpoint frequency.
- **Consistency Checks**: Sanity validation of WAL pages, CRC, torn page detection.

## 8. Vacuum & Maintenance
- **Autovacuum Worker**: Scan tables to reclaim tuples whose `xmax` is globally visible, freeze old `xmin`.
- **Freeze & Wraparound Handling**: Manage xid wraparound by freezing tuples (similar to Postgres `VACUUM FREEZE`).
- **Statistics Collector**: Track tuple counts, dead tuples for planner decisions.

## 9. Planner/Executor Integration
- **Visibility Hooks**: Executors must filter tuples using MVCC rules, follow HOT chains.
- **Plan Hints**: With MVCC + locking, update planner costing to account for multi-version scans.
- **Statement Execution Model**: Add `Portal`/cursor infrastructure for transaction-scoped iteration.

## 10. Testing & Tooling
- **Isolation Tests**: Borrow PostgreSQL isolation test framework (lit probe) for anomaly detection.
- **Crash Tests**: Automated harness to crash after WAL write but before data flush, ensure recovery.
- **Performance Benchmarks**: OLTP workloads (pgbench-like) to track progress.

## Implementation Roadmap

1. **Phase 0 – Infrastructure**
   - Extend buffer manager to track `page_lsn` and dirtypage list.
   - Build WAL manager (log buffer, writer thread).
   - Update DiskScheduler to support sequential log writes.

2. **Phase 1 – MVCC Core**
   - Augment tuple headers with `xmin/xmax` and implement visibility checks.
   - Create TransactionManager with xid allocation, status table, snapshots.
   - Introduce undo log chains per tuple/transaction; update INSERT/UPDATE/DELETE to write undo + WAL.

3. **Phase 2 – Locking & Isolation**
   - Implement table/row locks, deadlock detection.
   - Support READ COMMITTED snapshots (per-statement) and REPEATABLE READ (per txn).
   - Executor integrates MVCC visibility + locking for writes.

4. **Phase 3 – Recovery & Checkpoints**
   - WAL replay (analysis, redo, undo) on startup.
   - Periodic checkpoints writing dirty page table + active tx list.
   - Background writer to smooth flush.

5. **Phase 4 – Maintenance & Advanced Features**
   - Autovacuum to reclaim dead tuples and freeze.
   - Statistics collector for planner.
   - Optional: Two-phase commit, logical replication foundation.

6. **Phase 5 – Hardening & Benchmarks**
   - Comprehensive isolation tests, crash fault injection.
   - Benchmark vs PostgreSQL (pgbench) for throughput, latency.
   - Observability: metrics for WAL flush, lock wait, transaction latency.

This plan mirrors PostgreSQL’s architecture while adapting to QuillSQL’s existing modules (BufferPool, DiskScheduler, B+Tree). Each phase can be tackled incrementally with clear success criteria.
