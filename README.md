# QuillSQL

<div align="center">
  <img src="/public/rust-db.png" alt="QuillSQL Cover" width="500"/>
</div>

A SQL database system implemented in Rust

## System Architecture

QuillSQL is a relational database system implemented in Rust, supporting standard SQL syntax and transaction processing. The system adopts a modular design, primarily comprising three main modules: SQL parser, query execution engine, and storage engine.

### Storage Engines

QuillSQL supports three storage engines, allowing you to choose the most suitable one for different scenarios:

1. **Memory Engine**
   - All data stored in memory
   - Suitable for temporary data and testing scenarios
   - Data not preserved after restart

2. **Bitcask Engine**
   - Log-structured key-value storage
   - High write performance, low read latency
   - Supports persistent storage
   - Utilizes LRU-K caching strategy to optimize read performance

3. **B+ Tree Engine**
   - Classic database index structure
   - Supports range queries and sorting
   - Implements buffer pool management
   - Suitable for scenarios requiring complex queries

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
* count(col_name)
* min(col_name)
* max(col_name)
* sum(col_name)
* avg(col_name)

where `from_item` is:
* table_name
* table_name `join_type` table_name [`ON` predicate]

where `join_type` is:
* cross join
* join
* left join
* right join

where `on predicate` is:
* column_name = column_name

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

- [Bitcask](https://github.com/dragonquest/bitcask) - High-performance Bitcask key-value storage implemented in Rust
- [BustubX](https://github.com/systemxlabs/bustubx) - Educational relational database system implemented in Rust
- [CMU 15-445/645 Database Systems](https://15445.courses.cs.cmu.edu/) - Carnegie Mellon University's database systems course
