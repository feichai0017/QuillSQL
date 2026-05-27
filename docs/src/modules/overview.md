# Module Overview

QuillSQL is now a DataFusion-fronted SQL research engine backed by Holt. The
project keeps its own persistent storage adapter, MVCC metadata, transaction
status handling, CLI, and server, while DataFusion owns SQL parsing, planning,
optimization, and physical execution.

## Database (`src/database.rs`)

`Database::run` is the only SQL entry point. It creates a DataFusion session,
registers the Holt catalog provider, intercepts Holt-backed DDL, and returns
Arrow `RecordBatch` output.

## DataFusion Adapter (`src/df`)

This package contains the DataFusion integration:

| File | Role |
| ---- | ---- |
| `mod.rs` | Shared adapter state and transaction/session boundary. |
| `provider.rs` | DataFusion catalog, schema, and table providers. |
| `exec.rs` | `HoltScanExec` and DML result execution nodes. |
| `ddl.rs` | Holt-backed DDL handling. |
| `filter.rs` | Indexed filter recognition and residual filter evaluation. |
| `arrow.rs` | Arrow/Quill row conversion and result formatting. |

Simple single-column indexed equality/range predicates can narrow the Holt scan.
DataFusion still applies residual filters, so pushdown remains correctness-first.

## JIT (`src/jit`)

The JIT layer is attached as a DataFusion physical optimizer rule. The current
`jit-mlir` feature is intentionally toolchain-light: it enables the MLIR research
extension point without requiring system MLIR in the default build. The next step
is replacing eligible `FilterExec` and `ProjectionExec` nodes with compiled Arrow
batch kernels.

## Catalog (`src/catalog`)

The catalog is an in-memory projection of Holt descriptors. Holt is the source of
truth for table and index ids, schemas, and descriptor recovery. DataFusion
provides `information_schema` from the catalog provider; QuillSQL no longer stores
or scans system-table rows itself.

## Storage (`src/storage`)

The storage module is the Holt adapter and private row codec:

| File | Role |
| ---- | ---- |
| `engine.rs` | Object-safe table/index handles used by the Holt adapter. |
| `holt.rs` | Holt table/index handles, descriptor persistence, ordered index codec, and transaction status persistence. |
| `record.rs` | `RecordId` and `TupleMeta`, kept as SQL-layer MVCC metadata. |
| `tuple.rs`, `codec/` | Private tuple/scalar encoding utilities for Holt row values and index keys. |

There is no QuillSQL-owned page cache, heap file, B+Tree, or WAL path.

## Transactions (`src/transaction`)

`TransactionManager` assigns ids, tracks status, releases locks, and drives undo on
abort. `TxnContext` provides snapshot visibility and write checks. Holt stores the
recovered transaction status table; row versions keep MVCC state in `TupleMeta`.

## Front Ends (`src/bin`)

`client` is the interactive SQL shell. `server` exposes HTTP endpoints for SQL
execution and debug views over locks, transactions, plans, and MVCC samples.

## Tests (`tests`)

Integration tests cover DataFusion SQL over Holt tables, insert/update/delete,
secondary indexes, persistence/reopen, DataFusion `information_schema`, and
`EXPLAIN` output containing `HoltScanExec`.
