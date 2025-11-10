# Execution Engine

`src/execution/` drives `PhysicalPlan` trees using the Volcano (iterator) model. Every
operator pulls tuples from its children, coordinating closely with transactions,
storage, and expression evaluation.

---

## Core Components

| Component | Role |
| --------- | ---- |
| `PhysicalPlan` | Enum covering all physical operators; each implements `VolcanoExecutor`. |
| `ExecutionContext` | Shared context carrying the catalog, `TxnContext`, storage engine, and expression helpers. |
| `TupleStream` | Unified scan interface returned by table/index handles. |

---

## Execution Flow

1. `ExecutionEngine::execute` calls `init` on the root plan (and recursively on children).
2. The engine loops calling `next`, with parents pulling tuples from children.
3. `ExecutionContext` supplies transaction snapshots, lock helpers, and expression
   evaluation per call.
4. Once `next` returns `None`, the accumulated results are returned to the caller (CLI,
   HTTP API, or tests).

---

## Operator Examples

- **PhysicalSeqScan** – acquires a `table_stream` from the storage engine, uses
  `ScanPrefetch` for batching, and relies on `TxnContext::read_visible_tuple` for MVCC.
- **PhysicalIndexScan** – uses `index_stream`, tracks `invisible_hits`, and notifies the
  catalog when garbage accumulates.
- **PhysicalUpdate/PhysicalDelete** – call `prepare_row_for_write` to re-validate locks
  and the latest tuple before invoking `apply_update/delete`.
- **PhysicalNestedLoopJoin** – showcases the parent/child pull loop and acts as a baseline
  for more advanced joins.

---

## Interactions

- **StorageEngine** – all data access goes through handles/streams, keeping execution
  storage-agnostic.
- **Transaction** – `TxnContext` enforces locking, snapshots, and undo logging; operators
  never talk to `LockManager` directly.
- **Expression** – `ExecutionContext::eval_expr` / `eval_predicate` evaluate expressions
  built by the planner.
- **Optimizer/Planner** – execution honours the plan as-is; all structural choices happen
  upstream.

---

## Teaching Ideas

- Implement a new operator (e.g., `PhysicalMergeJoin`) to see how `ExecutionContext`
  support generalises.
- Add adaptive prefetching inside `PhysicalSeqScan` to explore iterator hints.
- Enable `RUST_LOG=execution=trace` to watch the `init`/`next` call sequence during a
  query.

---

Further reading: [The Volcano Execution Model](../execution/volcano.md)
