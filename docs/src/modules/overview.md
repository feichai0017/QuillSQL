# Module Overview

QuillSQL is organized around the SQL stack. Holt is the only persistent storage
backend, so the remaining modules focus on names, plans, operators, transactions, and
typed tuple handling.

## SQL Front-End (`src/sql`)

Parses SQL through `sqlparser` plus QuillSQL-specific DDL options. The parser keeps the
AST SQL-shaped so later stages can make binding and planning decisions explicitly.

## Planning (`src/plan`)

`LogicalPlanner` binds table and column names, checks types, and produces `LogicalPlan`.
`PhysicalPlanner` lowers optimized logical plans into Volcano operators. DDL supports
Holt-only storage: omitted `ENGINE`/`USING` defaults to Holt, while non-Holt choices are
rejected.

## Optimizer (`src/optimizer`)

The optimizer is a small rule pipeline. Rules are pure rewrites over `LogicalPlan`, which
makes them easy to test and reason about. The planner can choose Holt index scans for
simple equality/range predicates.

## Execution (`src/execution`)

Every physical operator implements `VolcanoExecutor` with `init` and `next`. Operators
receive an `ExecutionContext`, which provides catalog access, expression evaluation,
transaction helpers, and the `HoltStorageEngine` facade.

## Catalog (`src/catalog`)

The catalog keeps an in-memory projection of schemas, tables, indexes, and statistics.
Durable descriptors are stored in Holt. `information_schema` is also represented as Holt
system tables so SQL can query metadata using normal scans.

## Storage Facade (`src/storage`)

The storage module contains:

| File | Role |
| ---- | ---- |
| `engine.rs` | Object-safe table/index traits and the Holt-only storage engine. |
| `holt.rs` | Holt table/index handles, descriptor persistence, ordered index codec, and transaction status persistence. |
| `record.rs` | `RecordId` and `TupleMeta`, kept as SQL-layer MVCC metadata. |
| `tuple.rs`, `codec/` | Tuple and scalar encoding utilities. |

There is no QuillSQL-owned page cache, heap file, B+Tree, or WAL path.

## Transactions (`src/transaction`)

`TransactionManager` assigns ids, tracks statuses, releases locks, and drives undo on
abort. `TxnContext` gives operators snapshot visibility and locking helpers. Holt stores
the recovered transaction status table; QuillSQL keeps MVCC semantics in `TupleMeta`.

## Front-Ends (`src/bin`)

`client` is the interactive SQL shell. `server` exposes HTTP endpoints for SQL execution
and debug views over locks, transactions, plans, and MVCC samples.

## Tests (`src/tests`)

The integration tests cover SQL behavior, Holt persistence/reopen, Holt indexes,
transaction isolation, rollback, and metadata projection.
