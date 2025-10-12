<div align="center">
  <img src="assets/rust-db.png" alt="QuillSQL Logo" width="500"/>
</div>

# QuillSQL Internals

Welcome to the technical documentation for QuillSQL.

This book provides a deep dive into the internal architecture and implementation details of the database. It is intended for developers, contributors, and anyone interested in understanding how a relational database is built from the ground up, referencing concepts from classic database courses like CMU 15-445.

---

## Table of Contents

*   [**Overall Architecture**](./architecture.md): A high-level overview of the entire system.

*   **Core Modules**
    *   [**Buffer Manager**](./modules/buffer.md): The in-memory page cache.
        *   [Page & Page Guards](./buffer/page.md)
        *   [The Buffer Pool](./buffer/buffer_pool.md)
    *   [**Storage Engine**](./modules/storage.md): How data is physically stored.
        *   [Disk I/O](./storage/disk_io.md)
        *   [Page & Tuple Layout](./storage/page_layouts.md)
        *   [Table Heap & MVCC](./storage/table_heap.md)
    *   [**Indexes**](./modules/index.md): The B+Tree implementation.
        *   [B+Tree Details](./index/btree_index.md)
    *   [**Recovery Manager (WAL)**](./modules/recovery.md): Crash recovery and the ARIES protocol.
    *   [**Transaction Manager**](./modules/transaction.md): Concurrency control with MVCC and 2PL.
    *   [**Query Plan**](./modules/plan.md): The journey from SQL to an executable plan.
    *   [**Query Optimizer**](./modules/optimizer.md): Rule-based plan transformations.
    *   [**Execution Engine**](./modules/execution.md): The Volcano (iterator) execution model.
