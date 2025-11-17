# Disk I/O — Scheduler, io_uring Data Pages & WAL Runtime

## 1. Architecture

- **Request Path**: foreground components enqueue `DiskRequest` objects via `DiskScheduler::{schedule_read, schedule_write, …}`. A dispatcher thread drains the global channel and distributes work round-robin to N io_uring workers. Each worker owns its own ring and file-descriptor cache, so once a request is forwarded, execution proceeds entirely off the foreground thread.
- **Stable APIs**: `schedule_read(page_id)`, `schedule_write(page_id, Bytes)`, `schedule_read_pages(Vec<PageId>)`, `schedule_allocate()`, `schedule_deallocate(page_id)` — every call returns a channel the caller can block on or poll.
- **Batch Reads**: `ReadPages` fans out per-page SQEs while a shared `BatchState` tracks completions. Even if the kernel completes I/O out of order, the caller receives a `Vec<BytesMut>` that preserves the original page order.

## 2. WAL Runtime (buffered I/O)

- Dedicated WAL runtime threads handle sequential WAL appends/reads using buffered I/O. They now keep a per-thread cache of open segment files, eliminating repeated `open()`/`close()` on every log record.
- Worker count defaults to `max(1, available_parallelism / 2)` but is tunable through `IOSchedulerConfig`.
- Optional `sync` on a request triggers `sync_data` / `fdatasync` so `WalManager` can honour synchronous commit or checkpoint barriers. Data pages stay on the io_uring dataplane; WAL always uses buffered writes.

## 3. io_uring Backend (Linux)

- Each worker owns an `IoUring` with configurable `queue_depth`, optional SQPOLL idle timeout, and a pool of registered fixed buffers sized to `PAGE_SIZE`. Workers submit SQEs asynchronously and drain CQEs in small batches to keep the ring warm.
- Read batching relies on shared `BatchState` instances (`Rc<RefCell<_>>`) so multi-page callers see ordered results without blocking the kernel on serialization.
- Writes keep their payload alive until completion; if a fixed buffer slot is available we reuse it, otherwise we fall back to heap buffers. A companion `WriteState` tracks an optional `fdatasync` so the caller still observes exactly one `Result<()>` once all CQEs land.
- Errors (short read/write, errno) are normalised into `QuillSQLError` values that flow back on the original channel.

## 4. Configuration

- `config::IOSchedulerConfig` controls:
  - `workers`: number of io_uring workers (default = available parallelism).
  - `wal_workers`: WAL runtime threads (default workers / 2).
  - `iouring_queue_depth`, `iouring_fixed_buffers`, `iouring_sqpoll_idle_ms`.
  - `fsync_on_write`: whether data-page writes also issue `fdatasync` (WAL sync is managed separately by `WalManager`).

## 5. Concurrency & Safety

- Worker-local file descriptors plus positional I/O remove shared mutable state on the hot path. The new per-worker handle cache further reduces syscall overhead.
- Shutdown sequence: enqueue `Shutdown`, dispatcher forwards it to every worker, each worker drains outstanding SQEs/CQEs, and finally dispatcher + workers are joined.
- BufferPool and TableHeap integrate via the same scheduler channels; inflight guards
  prevent duplicate page fetches even when multiple scans touch adjacent pages.

## 6. Performance Notes

- Random page access benefits from fewer syscalls and deeper outstanding queue depth than the blocking fallback.
- Only the io_uring backend currently ships (Linux x86_64). A portable fallback remains future work.
- For large sequential scans, rely on the buffer pool's sequential access pattern or add
  a custom iterator on top of `ReadPages` if you want to experiment with direct I/O.

## 7. Future Work

- Queue-depth aware scheduling and CQE bulk harvesting.
- Optional group commit (aggregate writes, single fsync) behind configuration.
- Metrics hooks (queue depth, submit/complete throughput, latency percentiles, error codes).
- Cross-platform fallback backend and richer prioritisation/throttling policies.
- Control-plane knobs for throttling individual background workers.
