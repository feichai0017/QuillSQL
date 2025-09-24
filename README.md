# QuillSQL

<div align="center">
  <img src="/public/rust-db.png" alt="QuillSQL Cover" width="520"/>
  <p><em>A tiny yet serious SQL database in Rust — simple, modular, pragmatic.</em></p>
</div>

## ✨ Highlights

- **Clean architecture**: SQL → Logical Plan → Physical Plan → Volcano executor
- **B+Tree index**: OLC readers, B-link pages, latch crabbing, range scan iterator
- **Buffer pool**: LRU-K, pin/unpin with RAII guards, flush-on-evict
- **Streaming scan**: Large sequential scans bypass buffer pool via a small direct I/O ring buffer to avoid cache pollution
- **Information schema**: `information_schema.schemas`, `tables`, `columns`, `indexes`
- **Now supports**: `SHOW DATABASES`, `SHOW TABLES`, `EXPLAIN`
- **Docs**: [Architecture](docs/architecture.md) · [Buffer Pool](docs/buffer_pool.md) · [B+ Tree Index](docs/btree_index.md)

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

- **SHOW**
- `SHOW DATABASES;` (rewritten to `SELECT schema FROM information_schema.schemas`)
- `SHOW TABLES;` (rewritten to `SELECT table_name FROM information_schema.tables`)

- **EXPLAIN**
  - `EXPLAIN <statement>` returns a single column named `plan` with multiple lines showing the logical plan tree

## ⚠️ Current Limitations

- Not yet supported: `DELETE`, `DROP`, `ALTER`, transaction control (BEGIN/COMMIT/ROLLBACK)
- Not implemented: outer joins (Left/Right/Full), arithmetic expressions, table/subquery aliases
- `ORDER BY` `DESC` / `NULLS FIRST|LAST` currently affects sorting only (not storage layout)

## 🧪 Testing

```bash
cargo test -q
```

## ⚙️ Configuration (Environment Variables)

Service
- PORT: Bind port (takes precedence over QUILL_HTTP_ADDR’s port)
- QUILL_HTTP_ADDR: Listen address (default 0.0.0.0:8080 when PORT is not set)
- RUST_LOG: Log level (e.g., info, debug)

Database file
- QUILL_DB_FILE: Path to the database file; if unset, a temporary DB is used (data is cleaned up on process exit)

Streaming sequential scan (Ring Buffer / Bypass)
- QUILL_STREAM_SCAN: Force enable(1)/disable(0) streaming scan; defaults to auto
- QUILL_STREAM_THRESHOLD: Page threshold to trigger streaming; default BUFFER_POOL_SIZE/4
- QUILL_STREAM_READAHEAD: Readahead window size; default 2
- QUILL_STREAM_HINT: Per-query hint for SeqScan (1/0), overrides auto decision

Debugging
- QUILL_DEBUG_LOCK=1|2: Print lock/page acquisition debug logs

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

## 📚 Acknowledgements

- [BustubX](https://github.com/systemxlabs/bustubx)
- [CMU 15-445/645 Database Systems](https://15445.courses.cs.cmu.edu/)
