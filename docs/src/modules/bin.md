# Front-Ends (CLI / HTTP)

The `bin/` directory contains the user-facing entry points. Both binaries embed
the same `Database` type and therefore use the DataFusion frontend adapter.

| Binary | Purpose |
| ------ | ------- |
| `client.rs` | Interactive CLI that reads SQL, executes it, and prints tabular output. |
| `server.rs` | HTTP + JSON API for local demos, web UI experiments, and integration tests. |

## CLI

- Uses `rustyline` for history and multi-line editing.
- Executes each statement through `Database::run`.
- Prints Arrow `RecordBatch` output with `comfy_table`.

## HTTP

Important endpoints:

- `POST /api/sql`: run one SQL string.
- `POST /api/sql_batch`: split a semicolon-delimited script and return one result
  set per statement.
- `GET /debug/trace/last`: return the last captured logical/physical plan trace.
- `GET /debug/plan/last`: return a compact plan tree snapshot.

Configuration comes from `QUILL_DATA_DIR`, `QUILL_HTTP_ADDR`, `PORT`, and
`RUST_LOG`.
