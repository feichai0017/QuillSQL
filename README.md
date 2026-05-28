# QuillSQL

QuillSQL is a DataFusion, Arrow, Parquet, and MLIR SQL query compiler research
engine.

[![Crates.io](https://img.shields.io/crates/v/quill-sql.svg)](https://crates.io/crates/quill-sql)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Mentioned in Awesome](https://awesome.re/mentioned-badge.svg)](https://github.com/rust-unofficial/awesome-rust#database)
[![Discord](https://img.shields.io/discord/1458041939587764247?label=Discord&logo=discord&logoColor=white)](https://discord.gg/dJqa4RYW65)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/feichai0017/QuillSQL)

<div align="center">
  <img src="/public/rust-db.png" alt="QuillSQL Architecture" width="720"/>
  <p><em>DataFusion planning, Arrow/Parquet storage, and an MLIR-first JIT boundary.</em></p>
</div>

## Focus

- DataFusion is the only SQL path: parsing, binding, logical optimization,
  physical optimization, and physical execution all go through DataFusion.
- Storage is DataFusion-backed: in-memory tables for interactive work and
  Parquet/Arrow datasets for persistent analytical workloads.
- QuillSQL no longer ships a page store, buffer pool, legacy index manager, WAL,
  SQL transaction layer, external KV adapter, or compatibility storage engine.
- The research surface is query compilation: DataFusion physical plans and Arrow
  `RecordBatch`es feed an MLIR-first JIT module.
- The `jit-mlir` feature uses `melior` and the system MLIR/LLVM libraries to
  parse and verify generated MLIR.

## Quick Start

```bash
cargo run --bin client

# keep DataFusion scratch state under a chosen directory
cargo run --bin client -- --data-dir .quillsql-data

# start web server at http://127.0.0.1:8080
cargo run --bin server

QUILL_DATA_DIR=.quillsql-data QUILL_HTTP_ADDR=0.0.0.0:8080 cargo run --bin server --release
```

Sample session:

```sql
CREATE TABLE t AS SELECT 1 AS id, 10 AS v;
INSERT INTO t VALUES (2, 20), (3, 30);

SELECT id, v FROM t WHERE v > 10 ORDER BY id DESC LIMIT 1;
EXPLAIN SELECT id, COUNT(*) FROM t GROUP BY id ORDER BY id;
```

Parquet datasets can be registered through `Database::register_parquet`:

```rust
db.register_parquet("events", "/data/events.parquet").await?;
let out = db.run("SELECT count(*) FROM events WHERE user_id IS NOT NULL").await?;
```

## MLIR JIT

The workspace is split into focused packages:

- `quill-sql`: CLI/server binaries, benchmarks, and release metadata.
- `quill-core`: the DataFusion-backed `Database` API, query execution, Parquet
  registration, and debug trace capture.
- `quill-jit`: pipeline extraction, Quill pipeline dialect model, MLIR
  emission, compiled execution nodes, and fixed-width Arrow kernels.
- `quill-mlir`: optional C++/TableGen package that registers the formal
  Quill MLIR dialect and pass names when `jit-mlir` is enabled.

Inside `crates/quill-jit/src`:

- `pipeline/` owns expression lowering, `PipelineGraph`, DataFusion physical-plan
  extraction, and the physical optimizer rule.
- `dialect/` emits the formal Quill MLIR pipeline graph from `PipelineGraph`:
  source, exec, sink, row, column, and region-yield semantics.
- `lower/` owns exact pipeline pattern lowering, compiler construction, and JIT
  options.
- `runtime/` provides the DataFusion compiled pipeline node, pipeline specs, and
  fixed-width Arrow batch kernels.
- `mlir/` owns MLIR emission, verification, and compiled kernel
  invocation. The current compiled path covers narrow fixed-width
  ExecutionEngine kernels.

Current scope: MLIR is parsed and verified, the Quill dialect is defined with
TableGen under `crates/quill-mlir`, and the DataFusion optimizer rule
replaces supported filter/project and plain `SUM` pipelines with one
`CompiledPipelineExec` node. That node executes through QuillSQL's fixed-width
Arrow pipeline runtime while carrying structured MLIR pipeline specs. Compiled
scalar MLIR invocation is wired for the first `i64 -> bool` probe. The compiled
batch path now includes an `i64` filter kernel that emits a byte selection mask,
a multi-column fixed-width record pipeline for `filter -> project -> record_batch`,
and a unified Quill-dialect `filter -> plain SUM` lowering for both `f64` and
Q6-shaped `Date32`/`Decimal128` inputs. With `jit-mlir` and
`DatabaseOptions { jit: JitOptions::mlir_execution(), .. }`,
`CompiledPipelineExec` dispatches record and scalar-sum paths from
`PipelineSpec` through a thread-local MLIR execution cache when the input batch
has no nulls or slice offsets; other cases keep the safe Rust batch runtime.
CLI, server, and
benchmark binaries read the same option once at startup from `QUILL_JIT=mlir`.
Debug traces expose recognized `PipelineGraph` candidates for record and aggregate
pipelines. `PipelineGraph` is a small executable subgraph, not a second SQL
planner. The executable compiled kernels now share the same lowering shape:
`PipelineGraph -> Quill dialect -> scf/arith/llvm -> ExecutionEngine`. Current
coverage includes fixed-width record projection over `i64`, `f64`, `date32`,
and `decimal128`, `f64 filter/sum`, and the Q6-shaped decimal
`filter -> plain SUM` path. A grouped aggregate sink exists in the graph and
dialect as the next Q1-oriented extension point; it is not yet selected by the
DataFusion optimizer rule.

The formal Quill dialect no longer stores predicate or measure expressions as
string attributes. `quill.exec.filter`, `quill.exec.project`,
`quill.sink.plain_sum`, and `quill.sink.group_aggregate` use single-block
regions with `!quill.row` arguments, `quill.column` access, `arith` scalar operations, and `quill.yield`
terminators. The registered pass names `quill-canonicalize-pipeline` and
`convert-quill-to-loops` are registered as the C++ lowering extension points;
the covered record and plain-SUM kernels now lower through that pass.

Run the MLIR path with:

```bash
MLIR_SYS_220_PREFIX=/opt/homebrew/opt/llvm \
LLVM_SYS_220_PREFIX=/opt/homebrew/opt/llvm \
cargo test --features jit-mlir

QUILL_JIT=mlir \
MLIR_SYS_220_PREFIX=/opt/homebrew/opt/llvm \
LLVM_SYS_220_PREFIX=/opt/homebrew/opt/llvm \
cargo bench --features jit-mlir --bench tpch -- q6_scan_filter_aggregate
```

Adjust the prefixes for your local LLVM/MLIR installation.

## Benchmarks

QuillSQL keeps benchmark levels separate:

- `jit_micro`: isolates pipeline lowering, MLIR lowering, and small DataFusion
  filter/project and filter/sum SQL paths over in-memory Arrow tables.
- `tpch`: runs a small TPC-H ladder over external Parquet data. It currently
  includes Q6, Q1, and Q3 to cover scan/filter/plain aggregate, grouped
  aggregate/sort, and join-heavy execution. It reports both `sql/<query>`
  end-to-end timings and `prepared/<query>` timings over a reused logical plan.

```bash
# Compile all benchmark harnesses.
cargo bench --no-run

# Fast local smoke run.
cargo bench --bench jit_micro -- --sample-size 10
scripts/bench_jit_micro.sh

# Run TPC-H. By default this generates SF0.01 data under benchdata/tpch-sf0.01.
cargo bench --bench tpch -- --sample-size 10
scripts/bench_tpch.sh

# Or run TPC-H against an existing Parquet dataset.
QUILL_TPCH_DIR=/path/to/tpch-parquet cargo bench --bench tpch
```

Generated data is ignored by git. Set `QUILL_TPCH_SF=1` for SF1, and set
`QUILL_TPCH_REGENERATE=1` to rebuild the generated Parquet files. If
`QUILL_TPCH_DIR` is set, it should contain either `<table>.parquet` files or
table directories named like `lineitem`, `orders`, and `customer`. TPC-H timings
separate `Database::run` from `PreparedQuery::run`, so JIT execution changes can
be compared with less SQL parsing and logical planning noise.

## Testing

```bash
cargo test
cargo test --features jit-mlir
cargo clippy --all-targets -- -D warnings
cargo bench --no-run
```

The integration tests cover DataFusion memory tables, Parquet registration, and
`EXPLAIN` output over the current DataFusion physical plan.

## Runtime Configuration

- `PORT`: bind port, overriding `QUILL_HTTP_ADDR`.
- `QUILL_HTTP_ADDR`: listen address, default `0.0.0.0:8080`.
- `QUILL_DATA_DIR`: directory for DataFusion scratch state.
- `QUILL_JIT`: `off` keeps the pure DataFusion physical plan, `runtime` or unset
  enables Quill's fixed-width compiled pipeline runtime, and `mlir` requests
  executable MLIR kernels when built with `--features jit-mlir`.
- `RUST_LOG`: log level.

`DatabaseOptions` exposes the same data-directory setting for embedded use. It
also has `debug_trace` and `jit` switches: interactive and HTTP paths leave
trace enabled, while benchmark harnesses disable it so query timings do not
include trace-only physical planning.

## Docker

```bash
docker build -t quillsql:latest .
docker run --rm -p 8080:8080 quillsql:latest
docker run --rm -p 8080:8080 -e QUILL_DATA_DIR=/data -v $(pwd)/data:/data quillsql:latest
```

## Acknowledgements

- [Apache DataFusion](https://datafusion.apache.org/)
- [Apache Arrow](https://arrow.apache.org/)
- [MLIR](https://mlir.llvm.org/)
- [melior](https://github.com/mlir-rs/melior)

## Community

Discord: https://discord.gg/dJqa4RYW65
