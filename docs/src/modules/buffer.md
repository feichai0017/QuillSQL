# Buffer Manager

In any disk-based database, the speed difference between main memory (RAM) and persistent storage (SSD/HDD) is enormous. The **Buffer Manager** is the component designed to solve this problem. It acts as a cache, managing a region of main memory called the **Buffer Pool** and moving data pages between disk and memory as needed.

Its primary goal is to minimize disk I/O by keeping frequently accessed pages in memory.

This section is divided into the following parts:

- **[Page & Page Guards](../buffer/page.md)**: Explains the core concepts of pinning and the RAII guards used to safely access pages.
- **[The Buffer Pool](../buffer/buffer_pool.md)**: A deep dive into the architecture and lifecycle of a page request, including the page table, replacer, and concurrency.