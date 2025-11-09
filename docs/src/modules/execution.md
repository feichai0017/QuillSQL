# Execution Engine

Once a query has been parsed, planned, and optimized, the resulting `PhysicalPlan` is handed to the **Execution Engine**. This component is the workhorse of the database, responsible for actually running the plan and producing the final results.

## Core Concepts

- **Volcano (Iterator) Model**: QuillSQL uses a pull-based execution model. Each operator in the physical plan tree is an "iterator" that provides a `next()` method. The parent operator calls `next()` on its children to pull rows upwards through the tree, from the storage layer to the client.

- **Physical Operators**: Each logical operation (like a filter or a join) is mapped to a concrete physical operator that implements a specific algorithm (e.g., `PhysicalFilter`, `PhysicalNestedLoopJoin`).

- **Execution Context**: As the plan is executed, a shared `ExecutionContext` is passed between operators. It contains the current transaction, MVCC snapshot, and a pluggable `StorageEngine` so operators can call helpers like `read_visible_tuple`, `prepare_row_for_write`, `insert_tuple_with_indexes`, or `eval_predicate` without knowing anything about TableHeaps or indexes.

This section contains:

- **[The Volcano Execution Model](./../execution/volcano.md)**: A deep dive into the pull-based execution model and the lifecycle of a query.
