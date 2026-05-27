# Testing & Documentation

The active tests focus on DataFusion execution, Parquet registration, and the
MLIR JIT boundary.

## Test Suite

| Location | Purpose |
| -------- | ------- |
| `tests/df_arrow_parquet.rs` | End-to-end SQL over DataFusion memory tables and registered Parquet datasets. |
| `src/jit/*` unit tests | JIT expression lowering plus MLIR module generation and verification. |

Common commands:

```bash
cargo test
cargo test --features jit-mlir
cargo clippy --all-targets -- -D warnings
```

The `jit-mlir` feature requires local MLIR/LLVM libraries. On a Homebrew LLVM 22
installation, set:

```bash
MLIR_SYS_220_PREFIX=/opt/homebrew/opt/llvm \
LLVM_SYS_220_PREFIX=/opt/homebrew/opt/llvm \
cargo test --features jit-mlir
```

## Documentation

The `docs/` directory is an mdBook. It tracks the current DataFusion +
Arrow/Parquet + MLIR architecture and intentionally omits the removed teaching
database storage stack.
