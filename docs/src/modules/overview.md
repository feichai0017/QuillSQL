# Module Overview

The current codebase is intentionally small. DataFusion provides the SQL engine;
QuillSQL provides the embedding API, front ends, and MLIR research hooks.

## Database (`src/database.rs`)

`Database::run` is the only SQL entry point. It asks DataFusion to create the
logical plan, captures a debug snapshot of the logical/physical plan, executes
through DataFusion, and returns Arrow `RecordBatch` output.

`Database::register_parquet` exposes the durable storage path by registering a
Parquet dataset as a DataFusion table.

## JIT (`src/jit`)

| File | Role |
| ---- | ---- |
| `exec.rs` | DataFusion physical execution node for compiled filter/project islands. |
| `expr.rs` | Lowers supported DataFusion physical expressions into QuillSQL's small JIT IR. |
| `ir.rs` | Defines `KernelIR`, `PipelineIR`, and the initial filter/project fusion boundary. |
| `kernel.rs` | Defines the future Arrow kernel ABI and compiled-kernel descriptor. |
| `mlir/` | MLIR-first backend that emits and verifies `arith` scalar functions. |
| `rule.rs` | DataFusion physical optimizer rule that rewrites supported filter/project islands. |

The JIT package is not a storage adapter and not a second SQL engine. It is the
research boundary for replacing selected DataFusion physical operators with
compiled kernels.

## Front Ends (`src/bin`)

`client` is the interactive SQL shell. `server` exposes HTTP endpoints for SQL
execution plus lightweight debug endpoints for the last DataFusion plan.

## Removed Layers

The project no longer contains the old teaching database stack:

- custom SQL AST/planner/optimizer/executor
- custom system tables
- custom row heap, buffer manager, legacy index manager, WAL, or recovery manager
- external KV storage adapter
- SQL-layer transaction manager

Those topics are useful, but they are no longer the theme of this repository.
