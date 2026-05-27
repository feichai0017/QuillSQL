# QuillSQL

[![Crates.io](https://img.shields.io/crates/v/quill-sql.svg)](https://crates.io/crates/quill-sql)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Mentioned in Awesome](https://awesome.re/mentioned-badge.svg)](https://github.com/rust-unofficial/awesome-rust#database)
[![Discord](https://img.shields.io/discord/1458041939587764247?label=Discord&logo=discord&logoColor=white)](https://discord.gg/dJqa4RYW65)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/feichai0017/QuillSQL)

<div align="center">
  <img src="/public/rust-db.png" alt="QuillSQL Architecture" width="720"/>
  <p><em>An educational Rust SQL engine backed by Holt storage</em></p>
</div>

## ✨ Highlights

- **Clean architecture**: SQL → Logical Plan → Physical Plan → Volcano executor
- **Transaction control**: `BEGIN/COMMIT/ROLLBACK`, `SET TRANSACTION`, `SET SESSION TRANSACTION`, enforced `READ ONLY`, row/table locks
- **Holt storage backend**: table rows, indexes, catalog descriptors, and transaction status are persisted through `holt::DB`
- **Holt ordered indexes**: point/range scans use order-preserving SQL key encoding with RID tie-breakers
- **Streaming scans**: executors consume storage through object-safe table/index handles, not page-heap internals
- **Virtual information schema**: `information_schema.schemas`, `tables`, `columns`, `indexes`
- **Docs**: 📖 **[Read the Book Online](https://feichai0017.github.io/QuillSQL/)** 

---

## Demo

<div align="center">
  <img src="/public/terminal-preview.svg" alt="QuillSQL Web Terminal" width="720"/>
  <p><em>Built-in web TTY — commands mirror our SQL test suite.</em></p>
</div>


- Run `cargo run --bin server` and open http://127.0.0.1:8080
- Commands: `help`, `docs`, `doc <name>`, `examples`, `example <name>`, `github`, `profile`
- Example scripts are pulled straight from `src/tests/sql_example/`

---

## 🎓 Teaching & Research Friendly

- Clear module boundaries, suitable for classroom assignments and research prototypes focused on SQL parsing, planning, optimization, execution, and transaction semantics.
- Storage is intentionally delegated to Holt. QuillSQL no longer ships its own page heap, buffer pool, B+Tree, or WAL implementation.
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
  - Storage: Holt is the only storage engine; `ENGINE=...` clauses are not supported.
  - Example:
    ```sql
    CREATE TABLE t(
      id INT64 NOT NULL,
      v  INT32 DEFAULT 0
    );
    ```

- **CREATE INDEX**
  - Index storage: Holt is implicit; `USING ...` clauses are not supported.
  - Example:
    ```sql
    CREATE INDEX idx_t_id ON t(id);
    ```

- **DROP**
  - `DROP TABLE [IF EXISTS] <name>`
  - `DROP INDEX [IF EXISTS] <name>`
  - Example:
    ```sql
    DROP INDEX IF EXISTS idx_orders_user_id;
    DROP TABLE orders;
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

- Not yet supported: `ALTER`, predicate locking.
- Not implemented: outer joins (Left/Right/Full), arithmetic expressions, table/subquery aliases
- `ORDER BY` `DESC` / `NULLS FIRST|LAST` currently affects sorting only (not storage layout)

## 🧪 Testing

```bash
cargo test -q
```

## ⚙️ Configuration

Minimal environment variables (runtime only)
- PORT: bind port (overrides the port of `QUILL_HTTP_ADDR`)
- QUILL_HTTP_ADDR: listen address (default `0.0.0.0:8080`)
- QUILL_DB_FILE: path to database file (uses a temp DB if unset)
- QUILL_DEFAULT_ISOLATION: default session isolation (`read-uncommitted`, `read-committed`, `repeatable-read`, `serializable`)
- RUST_LOG: log level (e.g., info, debug)

Programmatic options are exposed through `quill_sql::database::DatabaseOptions`. The only storage option is the Holt directory override; by default `Database::new_on_disk(path)` uses `path` as the Holt database directory and `Database::new_temp()` creates a temporary Holt database.

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

## <img src="https://cdn.simpleicons.org/discord/5865F2" height="20" /> Community

**Build. Break. Benchmark. Repeat.**  
QuillSQL has a Discord server for discussions, support, and *experimental* DB hacking.

**What you can do here:**
- 🧩 **Design discussions** — parser / executor / storage architecture
- 🧪 **Experiments & prototypes** — new operators, indexes, optimizer ideas, wild hacks
- 📈 **Benchmarks & profiling** — perf numbers, flamegraphs, regression hunts
- 📚 **Research notes** — papers, learning resources, implementation notes
- 🤝 **Contributing help** — “good first issue”, PR reviews, roadmap brainstorming

**Join:** https://discord.gg/dJqa4RYW65
