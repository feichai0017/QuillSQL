<div align="center">
  <img src="assets/rust-db.png" alt="QuillSQL Logo" width="500"/>
</div>

# QuillSQL Internals

Welcome to the technical documentation for QuillSQL.

This book documents QuillSQL as a DataFusion-fronted SQL research engine layered over
Holt. It is intended for developers and contributors who want to understand the Holt
storage adapter, DataFusion execution boundary, transaction semantics, and MLIR/JIT
extension point without also maintaining a separate page store.

---

## Table of Contents

*   [**Overall Architecture**](./architecture.md): A high-level overview of the entire system.

*   **Core Modules**
    *   [**Transaction Manager**](./modules/transaction.md): Concurrency control with MVCC and 2PL.
    *   [**Catalog**](./modules/catalog.md): SQL metadata projection backed by Holt descriptors.
