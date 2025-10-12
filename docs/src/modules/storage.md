# Storage Engine

The storage engine is the foundational layer of QuillSQL, responsible for managing how data is physically stored, retrieved, and organized on disk. It acts as the bridge between the in-memory buffer pool and the on-disk database files. Its primary goal is to provide a reliable and efficient abstraction for higher-level components, like the execution engine and transaction manager, to work with data without needing to understand the intricacies of file I/O.

## Core Components

The storage engine is built around a few key abstractions:

- **`DiskManager`**: The lowest-level component that directly reads and writes raw bytes from the database file in fixed-size blocks called pages.
- **`TableHeap`**: Represents the collection of pages that hold the actual data for a single table. It manages the layout of tuples within these pages.
- **`Tuple`**: The internal representation of a single row of data, containing the values for each column.

---

## Tuple (`storage/tuple.rs`)

A `Tuple` is the most basic unit of data, representing a single record or row. 

### Structure

- **`data: Vec<ScalarValue>`**: A vector where each element is a `ScalarValue`, an enum that can hold any of the database's supported data types (e.g., `Int32`, `Varchar`, `Bool`).
- **`schema: SchemaRef`**: A shared reference (`Arc<Schema>`) to the tuple's schema. The schema describes the name and data type of each column, allowing the database to correctly interpret the `data` vector.

### Key Functionality

- **Creation & Merging**: Provides methods to create new tuples from a vector of values and to merge multiple tuples horizontally, which is essential for implementing database joins.
- **Projections**: Can create a new, smaller tuple containing only a subset of its original columns based on a target schema.
- **Comparison**: Implements `Ord` and `PartialOrd`, allowing tuples to be compared against each other. This is critical for sorting data and for evaluating filter conditions in `WHERE` clauses.

---

## Disk Manager (`storage/disk_manager.rs`)

The `DiskManager` is the exclusive interface to the physical database file on disk. It abstracts file I/O into page-oriented operations, hiding details like file handles and offsets.

### Responsibilities

- **Page I/O**: Its core responsibility is to provide `read_page(page_id)` and `write_page(page_id, data)` methods. It translates a logical `PageId` into a specific byte offset in the database file.
- **Page Allocation**: When a new page is needed, the `DiskManager` allocates space for it in the database file by incrementing an atomic `next_page_id` counter. 
- **Freelist Management**: To avoid letting the database file grow indefinitely, the `DiskManager` maintains a freelist of pages that have been deallocated. When a new page is requested, it first attempts to retrieve one from the freelist before allocating a new one.
- **Meta Page**: It manages a special `MetaPage` at the beginning of the database file (offset 0). This page stores critical database-wide metadata, such as the `PageId` of the first page in the freelist chain.

### Direct I/O

A key feature of the `DiskManager` is its attempt to use **Direct I/O** (`O_DIRECT` on Linux). 

- **Bypassing OS Cache**: Direct I/O allows the database to bypass the operating system's page cache and write directly to the disk hardware. This prevents a "double caching" problem where data exists once in the database's own buffer pool and again in the OS cache. Giving the database exclusive control over caching is crucial for predictable performance and implementing its own buffer replacement policies (like LRU-K).
- **Alignment**: Direct I/O imposes strict memory alignment requirements. The `DiskManager` handles this by using a special `AlignedPageBuf` to ensure that all data buffers passed to the OS for reading or writing are aligned to the page size boundary.
- **Fallback**: If Direct I/O is not supported by the filesystem, the `DiskManager` gracefully falls back to standard, buffered I/O.

---

## Table Heap (`storage/table_heap.rs`)

A `TableHeap` organizes the set of pages that belong to a single table. It's called a "heap" because tuples are not stored in any specific order within the pages; they are simply appended as they come.

### Structure & Page Management

- A `TableHeap` doesn't own the pages themselves but manages their `PageId`s. It keeps track of the `first_page_id` and `last_page_id` to maintain a linked list of pages for the table.
- It interacts with the `BufferManager` to fetch pages into memory for reading or modification. It does **not** call the `DiskManager` directly.

### Tuple & Version Management

- **Insertion (`insert_tuple`)**: When a new tuple is inserted, the `TableHeap` fetches the last page of the table. If there is enough free space, the tuple is inserted there. If the page is full, it requests a completely new page from the buffer pool, links it to the end of the page list, and inserts the tuple there.
- **MVCC Support**: The `TableHeap` is central to QuillSQL's Multi-Version Concurrency Control (MVCC) strategy. 
    - An `UPDATE` operation does not modify a tuple in-place. Instead, it calls `mvcc_update`, which creates a **new version** of the tuple (often in a different location) and uses transaction metadata to link the old and new versions together in a version chain.
    - A `DELETE` operation simply marks a tuple version as deleted by updating its metadata; it doesn't immediately remove the data.
- **Iteration**: It provides a `TableIterator` that allows the execution engine to perform a sequential scan over every tuple in the table. The iterator traverses the linked list of pages and iterates over the tuples within each page.

### Integration with Write-Ahead Logging (WAL)

- The `TableHeap` is a key participant in ensuring data durability. Before it modifies a page (e.g., by inserting a tuple), it first generates a corresponding WAL record (e.g., `HeapInsertPayload`).
- This record is passed to the `WalManager`, which writes it to the log. Only after the log record is secured can the actual page in the buffer pool be modified. This "log first" rule ensures that if the system crashes, the recovery process can replay the WAL to restore all committed changes.
