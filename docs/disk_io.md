# Disk I/O — Scheduler, Thread Pool vs io_uring

## 1. Architecture

- Dispatcher/Workers: A dispatcher thread receives `DiskRequest` and forwards them to N worker queues in round-robin. Workers execute I/O concurrently.
- APIs (stable): `schedule_read(page_id)`, `schedule_write(page_id, Bytes)`, `schedule_read_pages(Vec<PageId>)`, `schedule_allocate()`, `schedule_deallocate(page_id)`.
- Batch Reads: `ReadPages` submits a batch of single-page reads in-order; results preserve input order.

## 2. Thread Pool Backend

- Each worker optionally holds a cloned `File` (Linux), using positional I/O (`read_at` / `write_at`) to avoid a global file mutex.
- Concurrency: Parallelism ≈ worker count; each call is a syscall, suitable for portability and simplicity.
- Durability: `write_page` flushes to the kernel buffer; fsync is not forced by default (lower latency, weaker durability). Higher-level components may call `flush_all_pages()` when needed.

## 3. io_uring Backend (Linux)

- Each worker owns an `IoUring` instance with configurable `queue_depth`; requests are submitted asynchronously and completions are drained in batches to keep the queue warm.
- Read batching: multi-page reads share a `BatchState` aggregator so pages can finish out of order while the caller still receives an ordered vector.
- Writes hold their payload in-flight until the CQE arrives; optional `fdatasync` is tracked via a shared `WriteState` so callers only see a single result when the link chain completes.
- Error Handling: CQEs convert errno/short-read conditions into `QuillSQLError` that flows back through the original result channel.

## 4. Configuration

- `config::IOStrategy` selects backend:
  - `ThreadPool { workers: Option<usize> }`
  - `IoUring { queue_depth: Option<usize> }`
- `config::IOSchedulerConfig` consolidates runtime knobs:
  - `workers`: default to CPU cores.
  - `iouring_queue_depth` (Linux-only): default 256.
  - `fsync_on_write`: `true` keeps durable writes, `false` skips `fdatasync` for latency-sensitive workloads.

## 5. Concurrency & Safety

- Worker-local cloned file descriptors (Linux) + positional I/O avoid shared mutable state.
- Message passing boundaries ensure shutdown safety: `Shutdown` → join dispatcher → join workers.
- Buffer Pool integrates with DiskScheduler via channels; inflight-load guards serialize duplicate page loads.

## 6. Performance Notes

- Random small I/O: `io_uring` usually wins (fewer syscalls, better overlap).
- Light workloads or non-Linux: Thread pool is simpler and portable.
- Sequential scans: Use `ReadPages` + higher-level ring buffers in B+Tree/Table scan to minimize page guard holding and reduce cache pollution.

## 7. Future Work

- Queue-depth aware batching policies and CQE bulk harvesting.
- Optional group commit (accumulate writes, fsync once per group) behind a config flag.
- Metrics hooks (queue length, submit/complete QPS, p95/p99, error codes).

