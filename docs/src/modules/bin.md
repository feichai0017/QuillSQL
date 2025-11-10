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
- Each command calls `database.run(sql)` and formats the resulting `Vec<Tuple>`.
- Supports meta commands (e.g., `.tables`) that expose catalog metadata—great for
  teaching how logical objects map to physical handles.

## HTTP (`bin/server.rs`)

- Built with `axum`/`hyper` (depending on the current `Cargo.toml`), exposing endpoints such as:
  - `POST /query` – run arbitrary SQL and return rows or an error payload.
  - Health/metrics endpoints—which you can extend in labs to surface background worker
    status or buffer metrics.
- Configuration comes from `QUILL_DB_FILE`, `QUILL_HTTP_ADDR`, `PORT`, etc., mirroring
  how production services inject settings.

---

## Teaching Ideas

- Extend the CLI with `\describe table` to practice catalog lookups.
- Add transaction endpoints (BEGIN/COMMIT) to the HTTP server to demonstrate how
  `SessionContext` scopes transactions per connection.
- Combine CLI interaction with `RUST_LOG` tracing to walk through the entire query
  lifecycle.
