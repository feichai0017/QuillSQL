# Testing & Documentation

The active tests focus on the current DataFusion + Holt architecture.

## Test Suite

| Location | Purpose |
| -------- | ------- |
| `tests/datafusion_holt.rs` | End-to-end SQL tests through DataFusion over Holt tables. |
| module unit tests | Storage codecs, ordered index encoding, MVCC visibility, and lock-manager behavior. |

Common commands:

```bash
cargo test
cargo test --features jit-mlir
cargo clippy --all-targets -- -D warnings
```

## Documentation

The `docs/` directory is an mdBook. It documents the current DataFusion-fronted
architecture rather than the removed teaching parser/planner/executor stack.
