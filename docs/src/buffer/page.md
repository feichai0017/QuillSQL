# Page & Page Guards

Before the Buffer Manager can hand out a reference to a page in memory, it must ensure that the page won't be evicted while it's being used by another thread. This is accomplished by **pinning**.

## Pinning

Pinning simply means incrementing a "pin count" associated with the page's frame in the buffer pool. A frame with a pin count greater than zero is forbidden from being chosen as a victim by the page replacer. 

- When a thread wants to use a page, it must first pin it.
- When the thread is finished with the page, it must **unpin** it (decrementing the count).

Manually managing pin counts is tedious and error-prone. Forgetting to unpin a page leads to a memory leak, as the frame can never be evicted. To solve this, QuillSQL uses a common and powerful C++ and Rust pattern: **Resource Acquisition Is Initialization (RAII)**.

## `ReadPageGuard` and `WritePageGuard`

Instead of returning a raw pointer to the page memory, the `BufferManager`'s `fetch_page_*` methods return a **guard** object: `ReadPageGuard` or `WritePageGuard`.

These guards are responsible for the lifetime of the pin and the lock on the page:

1.  **Acquisition**: When a `PageGuard` is created, its constructor acquires the appropriate lock (`RwLock`) on the page's frame and increments the frame's pin count.
    - `ReadPageGuard` takes a read lock, allowing multiple concurrent readers.
    - `WritePageGuard` takes an exclusive write lock.

2.  **Usage**: The calling code uses the guard object to access the page's data. The guard provides safe, locked access to the underlying byte array.

3.  **Release**: When the guard variable goes out of scope (e.g., at the end of a function), its `drop()` method is automatically called by the Rust compiler. This `drop()` implementation handles all the cleanup:
    - It decrements the pin count.
    - It releases the lock on the frame.
    - If it's a `WritePageGuard` and the data was modified, it informs the `BufferManager` that the page is now **dirty**.

This RAII pattern makes using the buffer pool much safer and more ergonomic, as it makes it impossible to forget to unpin a page or release a lock.
