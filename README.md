# QuillSQL

[![Crates.io](https://img.shields.io/crates/v/quill-sql.svg)](https://crates.io/crates/quill-sql)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

<div align="center">
  <img src="/public/rust-db.png" alt="QuillSQL Architecture" width="720"/>
  <p><em>A tiny yet serious SQL database in Rust — simple, modular, pragmatic.</em></p>
</div>

## ✨ Highlights

- **Clean architecture**: SQL → Logical Plan → Physical Plan → Volcano executor
- **Transaction control**: `BEGIN/COMMIT/ROLLBACK`, `SET TRANSACTION`, `SET SESSION TRANSACTION`, enforced `READ ONLY`, row/table locks
- **B+Tree index**: OLC readers, B-link pages, latch crabbing, range scan iterator
- **Buffer pool**: LRU-K, pin/unpin with RAII guards, flush-on-evict
- **Streaming scan**: Large sequential scans bypass buffer pool via a small direct I/O ring buffer to avoid cache pollution
- **WAL & Recovery (ARIES-inspired)**: FPW + PageDelta, DPT, chained CLR, per-transaction undo chains, idempotent replays
- **Information schema**: `information_schema.schemas`, `tables`, `columns`, `indexes`
- **Now supports**: `SHOW DATABASES`, `SHOW TABLES`, `EXPLAIN`, `DELETE`
- **Docs**: [Architecture](docs/architecture.md) · [Buffer Pool](docs/buffer_pool.md) · [B+ Tree Index](docs/btree_index.md) · [Disk I/O](docs/disk_io.md) · [WAL & Recovery](docs/wal.md) · [Transactions](docs/transactions.md)

---

## 🎓 Teaching & Research Friendly

- Clear module boundaries, suitable for classroom assignments and research prototypes. Inspired by CMU 15-445 BusTub with strengthened WAL/Recovery, observability, and centralized configuration.
- Pluggable pieces: buffer pool, index, WAL, and recovery are decoupled for side-by-side experiments.
- Readability-first: simple, pragmatic code with minimal hot-path allocations.

## 🚀 Quick Start

```bash
cargo run --bin client

# or open a persistent DB file
cargo run --bin client -- --file my.db

# start web server (http://127.0.0.1:8080)
cargo run --bin server

# specify data file and listening addr
QUILL_DB_FILE=my.db QUILL_HTTP_ADDR=0.0.0.0:8080 cargo run --bin server --release

# batch API (optional)
curl -XPOST http://127.0.0.1:8080/api/sql_batch -H 'content-type: application/json' \
     -d '{"sql": "SHOW TABLES; EXPLAIN SELECT 1;"}'
```

Sample session:
```sql
CREATE TABLE t(id INT, v INT DEFAULT 0);
INSERT INTO t(id, v) VALUES (1, 10), (2, 20), (3, 30);

SELECT id, v FROM t WHERE v > 10 ORDER BY id DESC LIMIT 1;

SHOW DATABASES;
SHOW TABLES;

EXPLAIN SELECT id, COUNT(*) FROM t GROUP BY id ORDER BY id;
```

## 🧱 Supported SQL

- **Data types**
  - `BOOLEAN`, `INT8/16/32/64`, `UINT8/16/32/64`, `FLOAT32/64`, `VARCHAR(n)`

- **CREATE TABLE**
  - Column options: `NOT NULL` | `DEFAULT <literal>`
  - Example:
    ```sql
    CREATE TABLE t(
      id INT64 NOT NULL,
      v  INT32 DEFAULT 0
    );
    ```

- **CREATE INDEX**
  - Example:
    ```sql
    CREATE INDEX idx_t_id ON t(id);
    ```

- **INSERT**
  - `INSERT INTO ... VALUES (...)` and `INSERT INTO ... SELECT ...`

- **SELECT**
- Projection: columns, literals, aliases
- FROM: table | subquery (`FROM (SELECT ...)`) — alias not yet supported
- WHERE: comparison/logical operators `= != > >= < <= AND OR`
- GROUP BY: aggregates `COUNT(expr|*)`, `AVG(expr)`
- ORDER BY: `ASC|DESC`, supports `NULLS FIRST|LAST`
- LIMIT/OFFSET
- JOIN: `INNER JOIN` (with `ON` condition), `CROSS JOIN`

- **UPDATE**
  - `UPDATE t SET col = expr [, ...] [WHERE predicate]`
- **DELETE**
  - `DELETE FROM t [WHERE predicate]`

- **SHOW**
- `SHOW DATABASES;` (rewritten to `SELECT schema FROM information_schema.schemas`)
- `SHOW TABLES;` (rewritten to `SELECT table_name FROM information_schema.tables`)

- **EXPLAIN**
  - `EXPLAIN <statement>` returns a single column named `plan` with multiple lines showing the logical plan tree

## ⚠️ Current Limitations

- Not yet supported: `DROP`, `ALTER`, MVCC, predicate locking.
- Not implemented: outer joins (Left/Right/Full), arithmetic expressions, table/subquery aliases
- `ORDER BY` `DESC` / `NULLS FIRST|LAST` currently affects sorting only (not storage layout)

## 🧪 Testing

```bash
cargo test -q
```

## ⚙️ Configuration

Programmatic configs (centralized)
- Build `DatabaseOptions { wal: WalOptions { .. } }` and pass it to `Database::new_*_with_options(..)`; WAL/scan are centrally managed by `crate::config`.
- Key structs: `IOStrategy`, `IOSchedulerConfig`, `BufferPoolConfig`, `BTreeConfig`, `TableScanConfig`, `WalConfig`/`WalOptions`.

Minimal environment variables (runtime only)
- PORT: bind port (overrides the port of `QUILL_HTTP_ADDR`)
- QUILL_HTTP_ADDR: listen address (default `0.0.0.0:8080`)
- QUILL_DB_FILE: path to database file (uses a temp DB if unset)
- QUILL_DEFAULT_ISOLATION: default session isolation (`read-uncommitted`, `read-committed`, `repeatable-read`, `serializable`)
- RUST_LOG: log level (e.g., info, debug)

Example (Rust)
```rust
use std::sync::Arc;
use quillsql::config::{IOStrategy, IOSchedulerConfig, BufferPoolConfig, BTreeConfig, TableScanConfig};
use quillsql::storage::disk_manager::DiskManager;
use quillsql::storage::disk_scheduler::DiskScheduler;
use quillsql::buffer::buffer_pool::BufferPoolManager;

// Disk I/O backend: ThreadPool or Linux io_uring
let dm = Arc::new(DiskManager::try_new("my.db").unwrap());
let scheduler = Arc::new(DiskScheduler::new_with_strategy(
    dm.clone(),
    IOStrategy::IoUring { queue_depth: Some(256) }, // or ThreadPool { workers: Some(n) }
));

// Buffer pool
let bpm = Arc::new(BufferPoolManager::new_with_config(
    BufferPoolConfig { buffer_pool_size: 5000, ..Default::default() },
    scheduler.clone(),
));

// B+Tree iterator tuning (batch window & prefetch)
let btree_cfg = BTreeConfig { seq_batch_enable: true, seq_window: 32, ..Default::default() };
// Table scan tuning (streaming readahead)
let table_scan_cfg = TableScanConfig { stream_scan_enable: true, readahead_pages: 4, ..Default::default() };

// WAL config (centralized)
use quillsql::config::WalConfig;
let wal_cfg = WalConfig { segment_size: 16 * 1024 * 1024, sync_on_flush: true, ..Default::default() };
```

Notes
- io_uring is Linux-only; non-Linux will fall back to the thread pool.
- Streaming/RingBuffer optimizations are controlled via `BTreeConfig` / `TableScanConfig` instead of env vars.

## 📦 Docker

```bash
# build
docker build -t quillsql:latest .

# run (ephemeral in-memory DB)
docker run --rm -p 8080:8080 quillsql:latest

# run with persistent file mounted
docker run --rm -p 8080:8080 -e QUILL_DB_FILE=/data/my.db -v $(pwd)/data:/data quillsql:latest
```


Includes sqllogictest-based cases:

- `src/tests/sql_example/create_table.slt`
- `src/tests/sql_example/create_index.slt`
- `src/tests/sql_example/insert.slt`
- `src/tests/sql_example/show_explain.slt`
- `src/tests/sql_example/delete.slt`

## 📚 Acknowledgements

- [BustubX](https://github.com/systemxlabs/bustubx)
- [CMU 15-445/645 Database Systems](https://15445.courses.cs.cmu.edu/)
