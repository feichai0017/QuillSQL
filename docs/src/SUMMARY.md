# Summary

- [Introduction](./introduction.md)
- [Overall Architecture](./architecture.md)

---

- [Contributor's Guide](./contributing.md)

---

- [Buffer Manager](./modules/buffer.md)
    - [Page & Page Guards](./buffer/page.md)
    - [The Buffer Pool](./buffer/buffer_pool.md)
- [Storage Engine](./modules/storage.md)
    - [Disk I/O](./storage/disk_io.md)
    - [Page & Tuple Layout](./storage/page_layouts.md)
    - [Table Heap & MVCC](./storage/table_heap.md)
- [Indexes](./modules/index.md)
    - [B+Tree](./index/btree_index.md)
- [Recovery Manager (WAL)](./modules/recovery.md)
    - [The ARIES Protocol](./recovery/aries.md)
- [Transaction Manager](./modules/transaction.md)
    - [MVCC and 2PL](./transaction/mvcc_and_2pl.md)
- [Query Plan](./modules/plan.md)
    - [The Lifecycle of a Query](./plan/lifecycle.md)
- [Query Optimizer](./modules/optimizer.md)
    - [Rule-Based Optimization](./optimizer/rules.md)
- [Execution Engine](./modules/execution.md)
    - [The Volcano Model](./execution/volcano.md)