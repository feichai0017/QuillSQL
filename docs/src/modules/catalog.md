# Catalog Module

`src/catalog/` is QuillSQL's data dictionary. It maps SQL names to schemas, table ids,
index ids, and statistics. Durable descriptors are stored in Holt; the in-memory catalog
is rebuilt from those descriptors during startup.

## Responsibilities

- Track schemas, tables, columns, indexes, and table statistics.
- Create and drop Holt table/index descriptors during DDL.
- Generate SQL-visible `information_schema.schemas`, `tables`, `columns`, and `indexes`.
- Give the planner and executor stable `TableReference` and `Schema` lookups.

## Key Files

| File | Role |
| ---- | ---- |
| `core.rs` | Main catalog types and DDL metadata mutation. |
| `information.rs` | Startup loader and virtual `information_schema` row generation. |
| `schema.rs` | `Schema`, `SchemaRef`, and projection helpers. |
| `column.rs`, `data_type.rs` | Column metadata and SQL type mapping. |
| `stats.rs` | `ANALYZE` output and selectivity inputs. |

## Metadata Flow

1. DDL creates or removes a Holt descriptor and updates the in-memory catalog.
2. On reopen, QuillSQL rebuilds the in-memory catalog from Holt descriptors.
3. Queries against `information_schema` scan virtual rows generated from that catalog.

This means Holt is the durable source of truth, while `information_schema` remains a
normal SQL-visible view of metadata without storing duplicate system rows.
