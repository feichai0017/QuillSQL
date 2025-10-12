# Transaction Manager

The Transaction Manager is responsible for one of the most critical aspects of a database: guaranteeing the **ACID** properties, specifically **Isolation** and **Atomicity**.

It allows multiple clients to access the database concurrently without interfering with each other, and it ensures that transactions are treated as single, indivisible operations (all or nothing).

## Core Concepts

- **Concurrency Control**: To handle simultaneous operations, QuillSQL uses a hybrid approach that combines two powerful techniques:
    1.  **Multi-Version Concurrency Control (MVCC)**: Instead of overwriting data, writers create new *versions* of rows. Readers are given a consistent *snapshot* of the database, which allows them to proceed without being blocked by writers.
    2.  **Two-Phase Locking (2PL)**: To prevent two writers from modifying the same row at the same time, a strict two-phase locking protocol is used. Transactions must acquire locks on data before modifying it and hold those locks until the transaction ends.

- **Atomicity**: The transaction manager records all actions performed by a transaction. If the transaction needs to be aborted, it can use this record to perform the necessary undo operations, rolling the database back to its state before the transaction began.

This section contains:

- **[MVCC and 2PL](./../transaction/mvcc_and_2pl.md)**: A deep dive into QuillSQL's hybrid concurrency control model.