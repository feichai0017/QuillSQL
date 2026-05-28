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

The JIT package is intentionally separate from the DataFusion wrapper:

- `src/jit/expr.rs` lowers supported DataFusion physical expressions to a small
  JIT expression IR.
- `src/jit/exec.rs` provides the DataFusion physical node for compiled
  filter/project islands.
- `src/jit/ir.rs` defines `KernelIR` and `PipelineIR`, including the first
  `FilterProject` fusion pattern.
- `src/jit/mlir/` owns MLIR emission, verification, and compiled kernel
  invocation. The current compiled path covers narrow fixed-width
  ExecutionEngine kernels.
- `src/jit/rule.rs` is the DataFusion physical optimizer rule that rewrites
  supported filter/project islands.
- `src/jit/runtime/` is the fixed-width Arrow batch kernel runtime. It includes
  the current DataFusion execution-node kernels and keeps specialized paths
  such as filter/sum separate from the generic expression evaluator.

Current scope: MLIR is parsed and verified, and the DataFusion optimizer rule
replaces supported filter/project islands with `CompiledFilterProjectExec` and
supported plain `SUM` pipelines with `CompiledFilterSumExec`. Those nodes
execute through QuillSQL's fixed-width Arrow kernel runtime while carrying MLIR
kernel descriptors. Compiled scalar MLIR invocation is wired for the first
`i64 -> bool` probe. The compiled batch path now includes an `i64` filter
kernel that emits a byte selection mask, an `i64` filter/project kernel that
compacts one projected column, and an `f64` filter/sum kernel for the first
scan/filter/plain-aggregate path. It also has a Q6-shaped
`Date32`/`Decimal128` filter/sum kernel that compiles and invokes through MLIR
over fixed-width column slices. With `jit-mlir` and
`QUILL_JIT_MLIR_DISPATCH=1`, `CompiledFilterSumExec` dispatches that decimal
path through a thread-local MLIR execution cache when the input batch has no
nulls or slice offsets; other cases keep the safe Arrow runtime fallback.

Run the MLIR path with:

```bash
MLIR_SYS_220_PREFIX=/opt/homebrew/opt/llvm \
LLVM_SYS_220_PREFIX=/opt/homebrew/opt/llvm \
cargo test --features jit-mlir

QUILL_JIT_MLIR_DISPATCH=1 \
MLIR_SYS_220_PREFIX=/opt/homebrew/opt/llvm \
LLVM_SYS_220_PREFIX=/opt/homebrew/opt/llvm \
cargo bench --features jit-mlir --bench tpch -- q6_scan_filter_aggregate
```

Adjust the prefixes for your local LLVM/MLIR installation.

## Benchmarks

QuillSQL keeps benchmark levels separate:

- `jit_micro`: isolates JIT IR fusion, MLIR lowering, and small DataFusion
  filter/project and filter/sum SQL paths over in-memory Arrow tables.
- `tpch`: runs a small TPC-H ladder over external Parquet data. It currently
  includes Q6, Q1, and Q3 to cover scan/filter/plain aggregate, grouped
  aggregate/sort, and join-heavy execution.

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
are DataFusion end-to-end timings through `Database::run`; they are not yet
compiled MLIR kernel timings.

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
- `RUST_LOG`: log level.

`DatabaseOptions` exposes the same data-directory setting for embedded use.

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
