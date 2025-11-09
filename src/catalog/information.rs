use crate::buffer::{AtomicPageId, PageId, INVALID_PAGE_ID};
use crate::catalog::catalog::{CatalogSchema, CatalogTable};
use crate::catalog::{
    Catalog, Column, DataType, Schema, SchemaRef, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME,
};
use crate::database::Database;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::utils::scalar::ScalarValue;
use crate::utils::table_ref::TableReference;

use crate::storage::index::btree_index::BPlusTreeIndex;
use crate::storage::table_heap::{TableHeap, TableIterator};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

pub static INFORMATION_SCHEMA_NAME: &str = "information_schema";
pub static INFORMATION_SCHEMA_SCHEMAS: &str = "schemas";
pub static INFORMATION_SCHEMA_TABLES: &str = "tables";
pub static INFORMATION_SCHEMA_COLUMNS: &str = "columns";
pub static INFORMATION_SCHEMA_INDEXES: &str = "indexes";

pub static SCHEMAS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Column::new("catalog", DataType::Varchar(None), false),
        Column::new("schema", DataType::Varchar(None), false),
    ]))
});

pub static TABLES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Column::new("table_catalog", DataType::Varchar(None), false),
        Column::new("table_schema", DataType::Varchar(None), false),
        Column::new("table_name", DataType::Varchar(None), false),
        Column::new("first_page_id", DataType::UInt32, false),
    ]))
});

pub static COLUMNS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Column::new("table_catalog", DataType::Varchar(None), false),
        Column::new("table_schema", DataType::Varchar(None), false),
        Column::new("table_name", DataType::Varchar(None), false),
        Column::new("column_name", DataType::Varchar(None), false),
        Column::new("data_type", DataType::Varchar(None), false),
        Column::new("nullable", DataType::Boolean, false),
        Column::new("default", DataType::Varchar(None), false),
    ]))
});

pub static INDEXES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Column::new("table_catalog", DataType::Varchar(None), false),
        Column::new("table_schema", DataType::Varchar(None), false),
        Column::new("table_name", DataType::Varchar(None), false),
        Column::new("index_name", DataType::Varchar(None), false),
        Column::new("key_schema", DataType::Varchar(None), false),
        Column::new("internal_max_size", DataType::UInt32, false),
        Column::new("leaf_max_size", DataType::UInt32, false),
        Column::new("header_page_id", DataType::UInt32, false),
    ]))
});

pub fn load_catalog_data(db: &mut Database) -> QuillSQLResult<()> {
    load_information_schema(&mut db.catalog)?;
    load_schemas(db)?;
    create_default_schema_if_not_exists(&mut db.catalog)?;
    load_user_tables(db)?;
    load_user_indexes(db)?;
    Ok(())
}

fn create_default_schema_if_not_exists(catalog: &mut Catalog) -> QuillSQLResult<()> {
    if !catalog.schemas.contains_key(DEFAULT_SCHEMA_NAME) {
        catalog.create_schema(DEFAULT_SCHEMA_NAME)?;
    }
    Ok(())
}

fn load_information_schema(catalog: &mut Catalog) -> QuillSQLResult<()> {
    let meta = catalog.disk_manager.meta.read().unwrap();
    let information_schema_schemas_first_page_id = meta.information_schema_schemas_first_page_id;
    let information_schema_tables_first_page_id = meta.information_schema_tables_first_page_id;
    let information_schema_columns_first_page_id = meta.information_schema_columns_first_page_id;
    let information_schema_indexes_first_page_id = meta.information_schema_indexes_first_page_id;
    drop(meta);

    // load last page id
    let information_schema_schemas_last_page_id = load_table_last_page_id(
        catalog,
        information_schema_schemas_first_page_id,
        SCHEMAS_SCHEMA.clone(),
    )?;
    let information_schema_tables_last_page_id = load_table_last_page_id(
        catalog,
        information_schema_tables_first_page_id,
        TABLES_SCHEMA.clone(),
    )?;
    let information_schema_columns_last_page_id = load_table_last_page_id(
        catalog,
        information_schema_columns_first_page_id,
        COLUMNS_SCHEMA.clone(),
    )?;
    let information_schema_indexes_last_page_id = load_table_last_page_id(
        catalog,
        information_schema_indexes_first_page_id,
        INDEXES_SCHEMA.clone(),
    )?;

    let table_registry = catalog.table_registry();
    let mut information_schema = CatalogSchema::new(INFORMATION_SCHEMA_NAME);

    let schemas_table = TableHeap {
        schema: SCHEMAS_SCHEMA.clone(),
        buffer_pool: catalog.buffer_pool.clone(),
        first_page_id: AtomicPageId::new(information_schema_schemas_first_page_id),
        last_page_id: AtomicPageId::new(information_schema_schemas_last_page_id),
    };
    let schemas_heap = Arc::new(schemas_table);
    information_schema.tables.insert(
        INFORMATION_SCHEMA_SCHEMAS.to_string(),
        CatalogTable::new(INFORMATION_SCHEMA_SCHEMAS, schemas_heap.clone()),
    );
    table_registry.register(
        TableReference::Full {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: INFORMATION_SCHEMA_NAME.to_string(),
            table: INFORMATION_SCHEMA_SCHEMAS.to_string(),
        },
        schemas_heap,
    );

    let tables_table = TableHeap {
        schema: TABLES_SCHEMA.clone(),
        buffer_pool: catalog.buffer_pool.clone(),
        first_page_id: AtomicPageId::new(information_schema_tables_first_page_id),
        last_page_id: AtomicPageId::new(information_schema_tables_last_page_id),
    };
    let tables_heap = Arc::new(tables_table);
    information_schema.tables.insert(
        INFORMATION_SCHEMA_TABLES.to_string(),
        CatalogTable::new(INFORMATION_SCHEMA_TABLES, tables_heap.clone()),
    );
    table_registry.register(
        TableReference::Full {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: INFORMATION_SCHEMA_NAME.to_string(),
            table: INFORMATION_SCHEMA_TABLES.to_string(),
        },
        tables_heap,
    );

    let columns_table = TableHeap {
        schema: COLUMNS_SCHEMA.clone(),
        buffer_pool: catalog.buffer_pool.clone(),
        first_page_id: AtomicPageId::new(information_schema_columns_first_page_id),
        last_page_id: AtomicPageId::new(information_schema_columns_last_page_id),
    };
    let columns_heap = Arc::new(columns_table);
    information_schema.tables.insert(
        INFORMATION_SCHEMA_COLUMNS.to_string(),
        CatalogTable::new(INFORMATION_SCHEMA_COLUMNS, columns_heap.clone()),
    );
    table_registry.register(
        TableReference::Full {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: INFORMATION_SCHEMA_NAME.to_string(),
            table: INFORMATION_SCHEMA_COLUMNS.to_string(),
        },
        columns_heap,
    );

    let indexes_table = TableHeap {
        schema: INDEXES_SCHEMA.clone(),
        buffer_pool: catalog.buffer_pool.clone(),
        first_page_id: AtomicPageId::new(information_schema_indexes_first_page_id),
        last_page_id: AtomicPageId::new(information_schema_indexes_last_page_id),
    };
    let indexes_heap = Arc::new(indexes_table);
    information_schema.tables.insert(
        INFORMATION_SCHEMA_INDEXES.to_string(),
        CatalogTable::new(INFORMATION_SCHEMA_INDEXES, indexes_heap.clone()),
    );
    table_registry.register(
        TableReference::Full {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: INFORMATION_SCHEMA_NAME.to_string(),
            table: INFORMATION_SCHEMA_INDEXES.to_string(),
        },
        indexes_heap,
    );

    catalog.load_schema(INFORMATION_SCHEMA_NAME, information_schema);
    Ok(())
}

fn load_schemas(db: &mut Database) -> QuillSQLResult<()> {
    let schemas_table = {
        let information_schema =
            db.catalog
                .schemas
                .get(INFORMATION_SCHEMA_NAME)
                .ok_or_else(|| {
                    QuillSQLError::Internal("information_schema not initialized".to_string())
                })?;
        information_schema
            .tables
            .get(INFORMATION_SCHEMA_SCHEMAS)
            .ok_or_else(|| {
                QuillSQLError::Internal("information_schema.schemas missing".to_string())
            })?
            .table
            .clone()
    };

    let mut iterator = TableIterator::new(schemas_table, ..);
    while let Some((_rid, meta, tuple)) = iterator.next()? {
        if meta.is_deleted {
            continue;
        }
        let ScalarValue::Varchar(Some(_catalog)) = tuple.value(0)? else {
            return Err(QuillSQLError::Internal(
                "invalid catalog value in information_schema.schemas".to_string(),
            ));
        };
        let ScalarValue::Varchar(Some(schema_name)) = tuple.value(1)? else {
            return Err(QuillSQLError::Internal(
                "invalid schema value in information_schema.schemas".to_string(),
            ));
        };
        db.catalog
            .load_schema(schema_name.clone(), CatalogSchema::new(schema_name));
    }
    Ok(())
}

fn load_user_tables(db: &mut Database) -> QuillSQLResult<()> {
    let (columns_heap, tables_heap) = {
        let information_schema =
            db.catalog
                .schemas
                .get(INFORMATION_SCHEMA_NAME)
                .ok_or_else(|| {
                    QuillSQLError::Internal("information_schema not initialized".to_string())
                })?;
        let columns_heap = information_schema
            .tables
            .get(INFORMATION_SCHEMA_COLUMNS)
            .ok_or_else(|| {
                QuillSQLError::Internal("information_schema.columns missing".to_string())
            })?
            .table
            .clone();
        let tables_heap = information_schema
            .tables
            .get(INFORMATION_SCHEMA_TABLES)
            .ok_or_else(|| {
                QuillSQLError::Internal("information_schema.tables missing".to_string())
            })?
            .table
            .clone();
        (columns_heap, tables_heap)
    };

    let mut column_map: HashMap<(String, String, String), Vec<Column>> = HashMap::new();
    let mut column_iter = TableIterator::new(columns_heap, ..);
    while let Some((_rid, meta, tuple)) = column_iter.next()? {
        if meta.is_deleted {
            continue;
        }
        let ScalarValue::Varchar(Some(catalog)) = tuple.value(0)? else {
            return Err(QuillSQLError::Internal(
                "invalid catalog in information_schema.columns".to_string(),
            ));
        };
        let ScalarValue::Varchar(Some(schema)) = tuple.value(1)? else {
            return Err(QuillSQLError::Internal(
                "invalid schema in information_schema.columns".to_string(),
            ));
        };
        let ScalarValue::Varchar(Some(table)) = tuple.value(2)? else {
            return Err(QuillSQLError::Internal(
                "invalid table in information_schema.columns".to_string(),
            ));
        };
        let ScalarValue::Varchar(Some(column_name)) = tuple.value(3)? else {
            return Err(QuillSQLError::Internal(
                "invalid column name in information_schema.columns".to_string(),
            ));
        };
        let ScalarValue::Varchar(Some(data_type_str)) = tuple.value(4)? else {
            return Err(QuillSQLError::Internal(
                "invalid data type in information_schema.columns".to_string(),
            ));
        };
        let ScalarValue::Boolean(Some(nullable)) = tuple.value(5)? else {
            return Err(QuillSQLError::Internal(
                "invalid nullable flag in information_schema.columns".to_string(),
            ));
        };
        let ScalarValue::Varchar(Some(default)) = tuple.value(6)? else {
            return Err(QuillSQLError::Internal(
                "invalid default in information_schema.columns".to_string(),
            ));
        };

        let data_type: DataType = data_type_str.as_str().try_into()?;
        let default_value = ScalarValue::from_string(default, data_type)?;
        column_map
            .entry((catalog.clone(), schema.clone(), table.clone()))
            .or_default()
            .push(
                Column::new(column_name.clone(), data_type, *nullable).with_default(default_value),
            );
    }

    let mut table_iter = TableIterator::new(tables_heap, ..);
    while let Some((_rid, meta, tuple)) = table_iter.next()? {
        if meta.is_deleted {
            continue;
        }
        let ScalarValue::Varchar(Some(catalog)) = tuple.value(0)? else {
            return Err(QuillSQLError::Internal(
                "invalid catalog in information_schema.tables".to_string(),
            ));
        };
        let ScalarValue::Varchar(Some(schema)) = tuple.value(1)? else {
            return Err(QuillSQLError::Internal(
                "invalid schema in information_schema.tables".to_string(),
            ));
        };
        let ScalarValue::Varchar(Some(table)) = tuple.value(2)? else {
            return Err(QuillSQLError::Internal(
                "invalid table name in information_schema.tables".to_string(),
            ));
        };
        let ScalarValue::UInt32(Some(first_page_id)) = tuple.value(3)? else {
            return Err(QuillSQLError::Internal(
                "invalid first_page_id in information_schema.tables".to_string(),
            ));
        };

        let columns = column_map
            .remove(&(catalog.clone(), schema.clone(), table.clone()))
            .unwrap_or_default();
        let schema_ref = Arc::new(Schema::new(columns));

        let last_page_id =
            load_table_last_page_id(&mut db.catalog, *first_page_id, schema_ref.clone())?;
        let table_heap = TableHeap {
            schema: schema_ref.clone(),
            buffer_pool: db.buffer_pool.clone(),
            first_page_id: AtomicPageId::new(*first_page_id),
            last_page_id: AtomicPageId::new(last_page_id),
        };

        db.catalog.load_table(
            TableReference::Full {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table: table.to_string(),
            },
            CatalogTable::new(table, Arc::new(table_heap)),
        )?;
    }
    Ok(())
}

fn load_user_indexes(db: &mut Database) -> QuillSQLResult<()> {
    let indexes_heap = {
        let information_schema =
            db.catalog
                .schemas
                .get(INFORMATION_SCHEMA_NAME)
                .ok_or_else(|| {
                    QuillSQLError::Internal("information_schema not initialized".to_string())
                })?;

        information_schema
            .tables
            .get(INFORMATION_SCHEMA_INDEXES)
            .ok_or_else(|| {
                QuillSQLError::Internal("information_schema.indexes missing".to_string())
            })?
            .table
            .clone()
    };

    let mut iterator = TableIterator::new(indexes_heap, ..);
    while let Some((_rid, meta, tuple)) = iterator.next()? {
        if meta.is_deleted {
            continue;
        }
        let ScalarValue::Varchar(Some(catalog_name)) = tuple.value(0)? else {
            return Err(QuillSQLError::Internal(
                "invalid catalog in information_schema.indexes".to_string(),
            ));
        };
        let ScalarValue::Varchar(Some(table_schema_name)) = tuple.value(1)? else {
            return Err(QuillSQLError::Internal(
                "invalid schema in information_schema.indexes".to_string(),
            ));
        };
        let ScalarValue::Varchar(Some(table_name)) = tuple.value(2)? else {
            return Err(QuillSQLError::Internal(
                "invalid table in information_schema.indexes".to_string(),
            ));
        };
        let ScalarValue::Varchar(Some(index_name)) = tuple.value(3)? else {
            return Err(QuillSQLError::Internal(
                "invalid index name in information_schema.indexes".to_string(),
            ));
        };
        let ScalarValue::Varchar(Some(key_schema_str)) = tuple.value(4)? else {
            return Err(QuillSQLError::Internal(
                "invalid key schema in information_schema.indexes".to_string(),
            ));
        };
        let ScalarValue::UInt32(Some(internal_max_size)) = tuple.value(5)? else {
            return Err(QuillSQLError::Internal(
                "invalid internal max size in information_schema.indexes".to_string(),
            ));
        };
        let ScalarValue::UInt32(Some(leaf_max_size)) = tuple.value(6)? else {
            return Err(QuillSQLError::Internal(
                "invalid leaf max size in information_schema.indexes".to_string(),
            ));
        };
        let ScalarValue::UInt32(Some(header_page_id)) = tuple.value(7)? else {
            return Err(QuillSQLError::Internal(
                "invalid header page id in information_schema.indexes".to_string(),
            ));
        };

        let table_ref = TableReference::Full {
            catalog: catalog_name.clone(),
            schema: table_schema_name.clone(),
            table: table_name.clone(),
        };
        let table_schema = db.catalog.table_heap(&table_ref)?.schema.clone();
        let key_schema = Arc::new(parse_key_schema_from_varchar(
            key_schema_str.as_str(),
            table_schema,
        )?);

        let b_plus_tree_index = BPlusTreeIndex::open(
            key_schema,
            db.buffer_pool.clone(),
            *internal_max_size,
            *leaf_max_size,
            *header_page_id,
        );
        db.catalog
            .load_index(table_ref, index_name, Arc::new(b_plus_tree_index))?;
    }
    Ok(())
}

fn load_table_last_page_id(
    catalog: &mut Catalog,
    first_page_id: PageId,
    schema: SchemaRef,
) -> QuillSQLResult<PageId> {
    let mut page_id = first_page_id;
    loop {
        let (_, table_page) = catalog
            .buffer_pool
            .fetch_table_page(page_id, schema.clone())?;

        if table_page.header.next_page_id == INVALID_PAGE_ID {
            return Ok(page_id);
        } else {
            page_id = table_page.header.next_page_id;
        }
    }
}

pub fn key_schema_to_varchar(key_schema: &Schema) -> String {
    key_schema
        .columns
        .iter()
        .map(|col| col.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn parse_key_schema_from_varchar(varchar: &str, table_schema: SchemaRef) -> QuillSQLResult<Schema> {
    let column_names = varchar
        .split(",")
        .map(|name| name.trim())
        .collect::<Vec<&str>>();
    let indices = column_names
        .into_iter()
        .map(|name| table_schema.index_of(None, name))
        .collect::<QuillSQLResult<Vec<usize>>>()?;
    table_schema.project(&indices)
}
