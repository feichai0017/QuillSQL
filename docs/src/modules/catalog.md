# Catalog Module

`src/catalog/` is QuillSQL's data dictionary. It maps SQL names to schemas, table ids,
index ids, and statistics. Durable descriptors are stored in Holt; the in-memory catalog
is rebuilt from those descriptors and from Holt-backed `information_schema` tables during
startup.

## Responsibilities

- Track schemas, tables, columns, indexes, and table statistics.
- Create and drop Holt table/index descriptors during DDL.
- Maintain SQL-visible `information_schema.schemas`, `tables`, `columns`, and `indexes`.
- Give the planner and executor stable `TableReference` and `Schema` lookups.

## Key Files

| File | Role |
| ---- | ---- |
| `catalog.rs` | Main catalog types and DDL metadata mutation. |
| `information.rs` | Startup loader and `information_schema` projection maintenance. |
| `schema.rs` | `Schema`, `SchemaRef`, and projection helpers. |
| `column.rs`, `data_type.rs` | Column metadata and SQL type mapping. |
| `stats.rs` | `ANALYZE` output and selectivity inputs. |

## Metadata Flow

1. DDL creates a Holt descriptor and updates the in-memory catalog.
2. The catalog writes projection rows into Holt-backed `information_schema` tables.
3. On reopen, QuillSQL loads `information_schema` rows and Holt descriptors, then
   refreshes the projection to remove stale entries.

This means Holt is the durable source of truth, while `information_schema` remains a
normal SQL view of metadata.
