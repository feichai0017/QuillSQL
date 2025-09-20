# QuillSQL

<div align="center">
  <img src="/public/rust-db.png" alt="QuillSQL Cover" width="500"/>
</div>

A SQL database system implemented in Rust

## System Architecture

QuillSQL is a relational database system implemented in Rust, supporting standard SQL syntax and transaction processing. The system adopts a modular design, primarily comprising three main modules: SQL parser, query execution engine, and storage engine.


### B+ Tree Index

Implemented per textbook B+Tree with production-grade concurrency. Highlights:

- Structure & Concurrency
  - Latch crabbing on write path: hold parent only when the child is unsafe
  - Full B-link (high_key + right-sibling next pointer); readers can safely chase right during splits
  - OLC (optimistic version checks) on read: restart from root if a page changes
  - Sibling locking in PageId order for redistribute/merge to avoid ABBA deadlocks
- Correctness & Recovery
  - Header page atomically switches root to avoid intermediate states on root split/shrink
  - Parent-guided redirection + leaf-chain hop to avoid misplacing keys across parent ranges
  - Page header version/type checks; decode failure or version change triggers retry
- Buffer Pool
  - Read/WritePageGuard with RAII pin/unpin; drop makes the frame evictable again
  - LRU-K replacement, inflight_loads to prevent thundering herd, flush dirty before eviction
  - page_table <-> replacer consistency enforced to prevent accidental eviction
- Iterator
  - Leaf-chain based forward scan; compatible with B-link/OLC; no key loss under concurrent splits/merges
  - Supports range scans and full scans
- Benchmarks (optional)
  - Hot-read and range-scan benches (ignored by default). Examples:
    - Hot read (release):
      ```bash
      QUILL_BENCH_BPM=1024 QUILL_BENCH_N=50000 QUILL_BENCH_OPS=500000 \
      cargo test -p quill-sql --lib storage::index::btree_index::tests::bench_get_hot_read --release -- --nocapture --ignored
      ```
    - Full scan (release):
      ```bash
      QUILL_BENCH_BPM=1024 QUILL_BENCH_N=30000 QUILL_BENCH_PASSES=20 \
      cargo test -p quill-sql --lib storage::index::btree_index::tests::bench_range_scan --release -- --nocapture --ignored
      ```

## Supported SQL Syntax

### 1. Create/Drop Table

create table:

```sql
CREATE TABLE table_name (
    [ column_name data_type [index] [ column_constraint [...] ] ]
    [, ... ]
   );

   where data_type is:
    - BOOLEAN(BOOL): true | false
    - FLOAT(DOUBLE)
    - INTEGER(INT)
    - STRING(TEXT, VARCHAR)

   where column_constraint is:
   [ NOT NULL | NULL | DEFAULT expr ]
```

drop table:

```sql
DROP TABLE table_name;
```

### 2. Insert Into

```sql
INSERT INTO table_name
[ ( column_name [, ...] ) ]
values ( expr [, ...] );
```

### 3. Select

```sql
SELECT [* | col_name | function [ [ AS ] output_name [, ...] ]]
FROM from_item
[GROUP BY col_name]
[ORDER BY col_name [asc | desc] [, ...]]
[LIMIT count]
[OFFSET count]
```

where `function` is:

- count(col_name)
- min(col_name)
- max(col_name)
- sum(col_name)
- avg(col_name)

where `from_item` is:

- table_name
- table_name `join_type` table_name [`ON` predicate]

where `join_type` is:

- cross join
- join
- left join
- right join

where `on predicate` is:

- column_name = column_name

### 4. Update

```sql
UPDATE table_name
SET column_name = expr [, ...]
[WHERE condition];
```

where condition is: `column_name = expr`

### 5. Delete

```sql
DELETE FROM table_name
[WHERE condition];
```

where condition is: `column_name = expr`

### 6. Show Table

```sql
SHOW TABLES;
```

```sql
SHOW TABLE `table_name`;
```

### 7. Transaction

```
BEGIN;

COMMIT;

ROLLBACK;
```

## 8. Explain

```
explain sql;
```

## Getting Started

```bash
# Start the database server
cargo run --bin sqldb-server

# Start the command-line client
cargo run --bin sqldb-cli
```

## Acknowledgements and References

QuillSQL's design and implementation reference the following excellent projects and courses:

- [BustubX](https://github.com/systemxlabs/bustubx) - Educational relational database system implemented in Rust
- [CMU 15-445/645 Database Systems](https://15445.courses.cs.cmu.edu/) - Carnegie Mellon University's database systems course
