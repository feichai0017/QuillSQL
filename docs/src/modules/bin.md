# Front-Ends (CLI / HTTP)

The `bin/` directory contains the user-facing entry points. Both binaries embed the same
`Database` type, so they demonstrate how the core engine can power different UIs.

| Binary | Purpose |
| ------ | ------- |
| `client.rs` | Interactive CLI (REPL) that reads SQL, executes it, and prints tabular output. |
| `server.rs` | HTTP + JSON API for integration tests or web UIs. |

---

## CLI (`bin/client.rs`)

- Uses `rustyline` to provide history, multi-line editing, and familiar shell shortcuts.
- Each command awaits `database.run(sql)` and formats the resulting Arrow
  `RecordBatch`es as a table.
- Catalog introspection is regular SQL through DataFusion, for example
  `SELECT * FROM information_schema.tables`.

## HTTP (`bin/server.rs`)

- Built with `axum`/`hyper` (depending on the current `Cargo.toml`), exposing endpoints such as:
  - `POST /api/sql` – run one SQL string and return stringified rows or an error payload.
  - `POST /api/sql_batch` – split a semicolon-delimited script and return one result set
    per statement.
  - `/debug/...` endpoints expose lock, transaction, MVCC, and last-plan snapshots for
    local demos.
- Configuration comes from `QUILL_DB_FILE`, `QUILL_HTTP_ADDR`, `PORT`, etc., mirroring
  how production services inject settings.

---

## Teaching Ideas

- Extend the CLI with `\describe table` to practice catalog lookups.
- Add per-connection HTTP sessions if you want long-running browser transactions
  instead of the current single database mutex.
- Combine CLI interaction with `RUST_LOG` tracing to walk through the entire query
  lifecycle.
