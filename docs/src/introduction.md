<div align="center">
  <img src="assets/rust-db.png" alt="QuillSQL Logo" width="500"/>
</div>

# QuillSQL Internals

Welcome to the technical documentation for QuillSQL.

This book documents QuillSQL as a SQL engine layered over Holt. It is intended for
developers and contributors who want to understand the parser, planner, optimizer,
Volcano executor, catalog, and transaction semantics without also maintaining a separate
page store.

---

## Table of Contents

*   [**Overall Architecture**](./architecture.md): A high-level overview of the entire system.

*   **Core Modules**
    *   [**Transaction Manager**](./modules/transaction.md): Concurrency control with MVCC and 2PL.
    *   [**Query Plan**](./modules/plan.md): The journey from SQL to an executable plan.
    *   [**Query Optimizer**](./modules/optimizer.md): Rule-based plan transformations.
    *   [**Execution Engine**](./modules/execution.md): The Volcano (iterator) execution model.
    *   [**Catalog**](./modules/catalog.md): SQL metadata projection backed by Holt descriptors.
