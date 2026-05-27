# Benchmarking

QuillSQL uses benchmarks to separate three different claims:

- JIT lowering cost: how much time QuillSQL spends building JIT IR and MLIR.
- DataFusion execution cost: how the current DataFusion path behaves on Arrow
  batches or Parquet datasets.
- Compiled-node cost: how the rewritten filter/project physical island behaves
  before and after native MLIR function pointers are enabled.

The current code has a real `CompiledFilterProjectExec` in the DataFusion hot
path, and its execution body uses QuillSQL's fixed-width Arrow batch kernel.
Native MLIR kernel speedups are intentionally not claimed yet.

## Microbenchmarks

`benches/jit_micro.rs` isolates small, repeatable paths:

```bash
cargo bench --bench jit_micro -- --sample-size 10
```

Benchmarks:

| Name | Measures |
| ---- | -------- |
| `jit_ir/fuse_filter_project` | `PipelineIR` prefix fusion into `KernelIR::FilterProject`. |
| `mlir/compile_filter` | JIT expression to MLIR module generation for a filter. |
| `mlir/compile_filter_project` | Fused filter/project MLIR module generation. |
| `quill_kernel/filter_project_64k` | Direct fixed-width Arrow kernel execution outside DataFusion planning. |
| `datafusion/sql_filter_project_64k` | DataFusion SQL planning/execution over a 64K-row in-memory Arrow table, including the compiled filter/project physical node when the pattern matches. |

Without `jit-mlir`, MLIR verification is a no-op. With `jit-mlir`, the same
benchmark includes `melior` parse and verifier cost:

```bash
MLIR_SYS_220_PREFIX=/opt/homebrew/opt/llvm \
LLVM_SYS_220_PREFIX=/opt/homebrew/opt/llvm \
cargo bench --bench jit_micro --features jit-mlir -- --sample-size 10
```

## TPC-H

`benches/tpch.rs` is the end-to-end analytical benchmark harness. It expects
Parquet data. By default it uses the pure Rust `tpchgen-cli` generator to create
SF0.01 data inside the repository under `benchdata/tpch-sf0.01`.

```bash
cargo bench --bench tpch -- --sample-size 10
scripts/bench_tpch.sh
```

Generated data is outside version control. Useful knobs:

| Variable | Meaning |
| -------- | ------- |
| `QUILL_TPCH_SF` | Scale factor for generated data, default `0.01`. |
| `QUILL_TPCH_GEN_THREADS` | Number of generator threads. |
| `QUILL_TPCH_REGENERATE=1` | Delete and rebuild the generated data directory. |
| `QUILL_TPCH_DIR` | Use an existing Parquet dataset instead of generating one. |

When `QUILL_TPCH_DIR` is set, the directory can contain either
`<table>.parquet` files or table directories:

```text
tpch-parquet/
  lineitem.parquet
  orders.parquet
  customer.parquet
```

or:

```text
tpch-parquet/
  lineitem/
  orders/
  customer/
```

Current query ladder:

| Query | Family | Why it is included |
| ----- | ------ | ------------------ |
| Q6 | scan/filter/plain aggregate | First fixed-width baseline for filter and simple arithmetic. |
| Q1 | scan/filter/grouped aggregate/sort | Exercises aggregate state and result materialization. |
| Q3 | join-heavy aggregate | Adds multi-table build/probe pressure. |

## Reporting Rules

When reporting results, include:

- exact git commit
- command line and feature flags
- CPU, memory, OS, Rust version, and LLVM/MLIR version if `jit-mlir` is enabled
- TPC-H scale factor and data format
- whether file-system cache was warm
- whether the number is JIT lowering, DataFusion end-to-end, or future native
  kernel execution
