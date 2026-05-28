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

| Directory | Role |
| --------- | ---- |
| `pipeline/` | Expression IR, `PipelineIR`, DataFusion physical-plan extraction, and the physical optimizer rule. |
| `dialect/` | Quill pipeline dialect skeleton used as the next lowering boundary. |
| `lower/` | Exact pipeline pattern lowering, compiled-plan construction, and JIT options. |
| `runtime/` | DataFusion physical execution nodes, compiled-kernel descriptors, and fixed-width Arrow batch kernels. |
| `mlir/` | MLIR emission, verification, and compiled ExecutionEngine invocation. |

The JIT subdirectories have stricter internal boundaries:

- `pipeline/expr.rs`: lowers supported DataFusion physical expressions into the
  small JIT expression IR.
- `pipeline/ir.rs`: defines the semantic `PipelineIR` shape extracted from
  DataFusion plans.
- `pipeline/extract.rs`: extracts recognizable physical-plan pipelines such as
  `filter -> projection` and `filter -> plain SUM`.
- `pipeline/rule.rs`: physical optimizer rule that delegates supported pipeline
  rewrites to the compiler.
- `lower/compiler.rs`: compiles recognized `PipelineIR` shapes into DataFusion
  execution nodes.
- `mlir/mod.rs`: public backend surface and `KernelBackend` implementation.
- `mlir/emit.rs`: textual MLIR emission from QuillSQL JIT expressions.
- `mlir/verify.rs`: feature-gated MLIR parser/verifier setup.
- `mlir/compiled.rs`: feature-gated `ExecutionEngine` invocation artifacts.
- `runtime/exec.rs`: DataFusion physical execution nodes for compiled record and
  aggregate pipelines.
- `runtime/kernel.rs`: future Arrow kernel ABI and compiled-kernel descriptor.
- `runtime/mod.rs`: public fixed-width filter/project runtime surface.
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
