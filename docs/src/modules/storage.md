# Storage Engine

The storage engine is where the logical world of SQL tables and rows meets the physical world of disk blocks and bytes. Understanding how data is laid out on disk is fundamental to understanding database performance, concurrency control, and recovery.

This section is divided into the following parts:

- **[Disk I/O](../storage/disk_io.md)**: A look at the asynchronous I/O layer using `io_uring`.
- **[Page & Tuple Layout](../storage/page_layouts.md)**: A deep dive into how pages and tuples are physically structured on disk, including the slotted page layout.
- **[Table Heap](../storage/table_heap.md)**: Explains how tuple versions are managed for MVCC.
