# Disk I/O — Scheduler, io_uring Data Pages & WAL Handler

## 1. Architecture

- Dispatcher/Workers: A dispatcher thread receives `DiskRequest` and forwards them to N worker queues in round-robin. Workers execute I/O concurrently.
- APIs (stable): `schedule_read(page_id)`, `schedule_write(page_id, Bytes)`, `schedule_read_pages(Vec<PageId>)`, `schedule_allocate()`, `schedule_deallocate(page_id)`.
- Batch Reads: `ReadPages` submits a batch of single-page reads in-order; results preserve input order.

## 2. WAL Handler Backend (buffered I/O)

- Dedicated WAL handler threads perform sequential WAL writes/reads using buffered I/O (`write_all` / `read_exact`).
- Configurable worker count (default half of CPU count, min 1).
- Optional `sync` flag triggers `sync_data` for durability (used during checkpoints/group commit).
- Keeps WAL semantics simple (variable length records, no direct I/O). Data pages remain on io_uring + O_DIRECT.

## 3. io_uring Backend (Linux)

- Each worker owns an `IoUring` instance with configurable `queue_depth`; requests are submitted asynchronously and completions are drained in batches to keep the queue warm.
- Read batching: multi-page reads share a `BatchState` aggregator so pages can finish out of order while the caller still receives an ordered vector. Completion bookkeeping now uses `Rc<RefCell<BatchState>>`, replacing the earlier unsafe raw-pointer handoff.
- Writes hold their payload in-flight until the CQE arrives; optional `fdatasync` is tracked via a shared `WriteState`. Write and fsync CQEs update the same state object so the caller still observes a single result.
- Error Handling: CQEs convert errno/short-read conditions into `QuillSQLError` that flows back through the original result channel.

## 4. Configuration

- `config::IOSchedulerConfig` knobs:
  - `workers`: io_uring worker threads (default CPU cores).
  - `wal_workers`: WAL handler threads (default workers/2).
  - `iouring_queue_depth`, `iouring_fixed_buffers`, `iouring_sqpoll_idle_ms` (Linux).
  - `fsync_on_write`: controls whether data-page writes issue `fdatasync` (WAL sync is configured separately).

## 5. Concurrency & Safety

- Worker-local cloned file descriptors (Linux) plus positional I/O avoid shared mutable state.
- `DiskScheduler` now owns the dispatcher runtime: requests enter an MPSC channel, the dispatcher round-robins them across workers, and shutdown proceeds as `Shutdown` → join dispatcher → join workers.
- Buffer Pool integrates with DiskScheduler via channels; inflight-load guards serialize duplicate page loads.

## 6. Performance Notes

- Random small I/O: `io_uring` usually wins (fewer syscalls, better overlap).
- Today only the io_uring backend ships (Linux x86_64); adding a fallback backend is future work (see below).
- Sequential scans: Use `ReadPages` plus higher-level ring buffers in B+Tree/Table scan to minimize page guard holding and reduce cache pollution.

## 7. Future Work

- Queue-depth aware batching policies and CQE bulk harvesting.
- Optional group commit (accumulate writes, fsync once per group) behind a config flag.
- Metrics hooks (queue length, submit/complete QPS, p95/p99, error codes).
- Non-Linux fallback backend (thread pool or sync) and richer prioritisation/throttling policies.
