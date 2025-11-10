# Configuration & Runtime Options

`src/config/` centralizes tunables used by `DatabaseOptions`, the CLI/HTTP front-ends, and
background workers. Keeping knobs in one place makes it easy to demonstrate how WAL,
buffering, or vacuum behavior changes under different settings.

---

## Key Types

| Type | Description |
| ---- | ----------- |
| `DatabaseOptions` | Top-level options when constructing a database (WAL config, default isolation, etc.). |
| `WalOptions` | WAL directory, segment size, flush strategy, writer interval, sync mode. |
| `IndexVacuumConfig` / `MvccVacuumConfig` | Background worker intervals (buffer writer, MVCC vacuum). |
| `BufferPoolConfig` | Optional overrides for pool size, TinyLFU, and replacement policy details. |

---

## Usage

- CLI/HTTP front-ends parse env vars or config files into `DatabaseOptions` and pass them
  to `Database::new_*`.
- During `bootstrap_storage`, the database wires these options into `WalManager`,
  `DiskScheduler`, and `BackgroundWorkers`.
- Workers and execution components receive `Arc` references to the relevant configs so
  they can adapt at runtime without global state.

---

## Teaching Ideas

- Toggle `WalOptions::synchronous_commit` to discuss commit latency vs durability.
- Shrink the buffer pool to highlight eviction behavior under different replacement
  policies.
- Adjust `MvccVacuumConfig` intervals and measure how vacuum frequency affects foreground
  write throughput.
