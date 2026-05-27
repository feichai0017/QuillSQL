# Catalog Module

`src/catalog/` is QuillSQL's in-memory projection of Holt descriptors. Holt is the
durable source of truth; the catalog is rebuilt from Holt on open and then exposed to
DataFusion through `HoltCatalogProvider` and `HoltSchemaProvider`.

## Responsibilities

- Track schemas, tables, columns, indexes, and Holt ids.
- Create and drop Holt table/index descriptors during DDL.
- Rebuild the in-memory catalog from Holt descriptors during startup.
- Provide table metadata to DataFusion's catalog and `information_schema` support.

## Key Files

| File | Role |
| ---- | ---- |
| `core.rs` | Main catalog types and DDL metadata mutation. |
| `information.rs` | Startup loader for Holt table/index descriptors. |
| `schema.rs` | `Schema`, `SchemaRef`, and projection helpers used by the private Holt row codec. |
| `column.rs`, `data_type.rs` | Column metadata and SQL type mapping. |

## Metadata Flow

1. DDL creates or removes a Holt descriptor and updates the in-memory catalog.
2. On reopen, QuillSQL rebuilds the in-memory catalog from Holt descriptors.
3. DataFusion reads catalog metadata through `HoltCatalogProvider`.
4. DataFusion generates SQL-visible `information_schema` rows from that provider.

There are no stored QuillSQL system-table rows.
