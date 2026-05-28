# Module Overview

The current codebase is a small Cargo workspace. DataFusion provides the SQL
engine; QuillSQL provides the embedding API, front ends, and MLIR research
hooks.

## Workspace Packages

| Package | Role |
| ------- | ---- |
| `quill-sql` | Public facade crate plus CLI/server binaries and benchmarks. |
| `quill-core` | DataFusion-backed `Database` API, query execution, Parquet registration, and debug traces. |
| `quill-jit` | Pipeline extraction, Quill dialect skeleton, MLIR emission, compiled execution nodes, and Arrow kernel runtime. |

## Database (`crates/quill-core/src/database.rs`)

`Database::run` is the interactive SQL entry point. It asks DataFusion to create
the logical plan, captures a debug snapshot of the logical/physical plan,
executes through DataFusion, and returns Arrow `RecordBatch` output.

`Database::prepare` creates a reusable logical plan wrapped in `PreparedQuery`.
Benchmark code uses this path to reduce parsing and logical planning noise while
still giving DataFusion a fresh physical plan for each execution.

`Database::register_parquet` exposes the durable storage path by registering a
Parquet dataset as a DataFusion table.

## JIT (`crates/quill-jit/src`)

| File | Role |
| ---- | ---- |
| `compiler.rs` | Compiles recognized `PipelineIR` shapes into DataFusion execution nodes. |
| `dialect.rs` | Defines the Quill pipeline dialect skeleton used as the next lowering boundary. |
| `exec.rs` | DataFusion physical execution nodes for compiled filter/project and aggregate pipelines. |
| `expr.rs` | Lowers supported DataFusion physical expressions into QuillSQL's small JIT IR. |
| `ir.rs` | Defines the semantic `PipelineIR` shape extracted from DataFusion plans. |
| `kernel.rs` | Defines the future Arrow kernel ABI and compiled-kernel descriptor. |
| `lowering.rs` | Lowers exact `PipelineIR` shapes into executable record or aggregate kernels. |
| `mlir/` | MLIR emission, verification, and compiled ExecutionEngine invocation. |
| `options.rs` | Startup-time JIT execution options shared by the database and benchmark harnesses. |
| `pipeline.rs` | Extracts recognizable DataFusion physical-plan pipelines such as `filter -> projection` and `filter -> plain SUM`. |
| `rule.rs` | DataFusion physical optimizer rule that delegates supported pipeline rewrites to the compiler. |
| `runtime/` | Fixed-width Arrow batch kernel runtime for compiled execution nodes. |

The two JIT subdirectories have stricter internal boundaries:

- `mlir/mod.rs`: public backend surface and `KernelBackend` implementation.
- `mlir/emit.rs`: textual MLIR emission from QuillSQL JIT expressions.
- `mlir/verify.rs`: feature-gated MLIR parser/verifier setup.
- `mlir/compiled.rs`: feature-gated `ExecutionEngine` invocation artifacts.
- `runtime/mod.rs`: public fixed-width filter/project kernel surface.
- `runtime/array.rs`: Arrow array views and output builders.
- `runtime/eval.rs`: expression evaluation and SQL boolean/null semantics.
- `runtime/value.rs`: scalar value representation.

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
