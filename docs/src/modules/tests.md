# Testing & Documentation

QuillSQL is intended for teaching, so the repo invests heavily in examples and automated
verification. The `tests/` tree and this mdBook work together to illustrate every module.

---

## Test Suite

| Location | Purpose |
| -------- | ------- |
| `tests/sql_example/*.slt` | [sqllogictest](https://www.sqlite.org/sqllogictest.html) suites covering DDL, DML, transactions, and indexes. |
| `tests/transaction_tests.rs` | Rust unit tests that stress MVCC visibility, lock conflicts, and isolation semantics. |
| `tests/storage_*` | Component tests for heap/index/buffer internals—perfect references for lab exercises. |

Common commands:

```bash
cargo test -q
# focused run
cargo test tests::transaction_tests::repeatable_read_sees_consistent_snapshot_after_update -- --nocapture
```

For long-running suites, wrap with `timeout` to guard against hangs.

---

## Documentation (mdBook)

- The `docs/` directory is an mdBook; run `mdbook serve docs` to browse locally.
- Each module, including this page, has a dedicated chapter so instructors can teach
  subsystem by subsystem.
- Anchor chapters such as `architecture.md`, `transactions.md`, and `wal.md` walk through
  end-to-end flows and subsystem deep dives.

---

## Teaching Ideas

- Require sqllogictest additions alongside code changes to reinforce “tests as docs”.
- Use the mdBook site during lectures to connect diagrams with source files.
- Assign “doc walk-through” tasks where students extend diagrams or add experiment
  instructions to existing chapters.
