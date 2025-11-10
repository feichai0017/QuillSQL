# Catalog Module

`src/catalog/` acts as QuillSQL’s data dictionary. It tracks schema/table/index metadata,
statistics, and the mapping between logical names and physical storage objects such as
`TableHeap` and `BPlusTreeIndex`. Every layer—planner, execution, background workers—uses
the catalog to discover structure.

---

## Responsibilities

- Persist definitions for schemas, tables, columns, indexes, and constraints.
- Map logical `TableReference`s to physical handles (heap files, index roots, file ids).
- Store table statistics (row counts, histograms) that drive ANALYZE and optimization.
- Manage the DDL lifecycle: creation and deletion update the in-memory registry and the
  on-disk metadata pages.

---

## Directory Layout

| Path | Description | Key Types |
| ---- | ----------- | --------- |
| `mod.rs` | Public API surface. | `Catalog`, `TableHandleRef` |
| `schema.rs` | Schema objects and table references. | `Schema`, `Column`, `TableReference` |
| `registry/` | Thread-safe registry for heaps (MVCC vacuum). | `TableRegistry` |
| `statistics.rs` | ANALYZE output and helpers. | `TableStatistics` |
| `loader.rs` | Boot-time metadata loader. | `load_catalog_data` |

---

## Core Concepts

### TableReference
Unified identifier (database, schema, table). Logical planner, execution, and transaction
code all use it when requesting handles from the catalog.

### Registries
`TableRegistry` maps internal IDs to `Arc<TableHeap>` plus logical names. It is used by
the MVCC vacuum worker to iterate user tables without poking directly into catalog data.

### Schema & Column
`Schema` stores column definitions (type, default, nullability). Execution uses it when
materialising tuples; the planner uses it to check expression types. `Schema::project`
helps physical operators build projected outputs.

### TableStatistics
`ANALYZE` writes row counts and histograms into the catalog. Optimizer rules and planner
heuristics can consult these stats when deciding whether to push filters or pick indexes.
Each column tracks null/non-null counts, min/max values, and a sample-based distinct
estimate, enabling DuckDB-style selectivity heuristics (`1/distinct`, uniform ranges).

---

## Interactions

- **SQL / Planner** – DDL planning calls `Catalog::create_table` / `create_index`; name
  binding relies on `Schema`.
- **Execution** – `ExecutionContext::table_handle` and `index_handle` fetch physical
  handles through the catalog, so scans never hard-code heap locations.
- **Background workers** – MVCC and index vacuum iterate the registries via `Arc` clones.
- **Recovery** – `load_catalog_data` rebuilds the in-memory catalog from control files and
  metadata pages during startup.

---

## Teaching Ideas

- Extend the schema system with hidden or computed columns and teach the catalog to store
  the extra metadata.
- Add histogram bins to `TableStatistics` and demonstrate how a simple cost heuristic can
  choose better plans.
- Turn on `RUST_LOG=catalog=debug` to observe how DDL mutates the registries.
