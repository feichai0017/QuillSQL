use crate::buffer::{AtomicPageId, PageId, INVALID_PAGE_ID};
use crate::catalog::catalog::{CatalogSchema, CatalogTable};
use crate::catalog::{Catalog, Column, DataType, Schema, SchemaRef, DEFAULT_SCHEMA_NAME};
use crate::database::Database;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::utils::scalar::ScalarValue;
use crate::utils::table_ref::TableReference;

use crate::storage::index::btree_index::BPlusTreeIndex;
use crate::storage::table_heap::TableHeap;
use std::sync::{Arc, LazyLock};

pub static INFORMATION_SCHEMA_NAME: &str = "information_schema";
pub static INFORMATION_SCHEMA_SCHEMAS: &str = "schemas";
pub static INFORMATION_SCHEMA_TABLES: &str = "tables";
pub static INFORMATION_SCHEMA_COLUMNS: &str = "columns";
pub static INFORMATION_SCHEMA_INDEXES: &str = "indexes";

pub static SCHEMAS_SCHMEA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Column::new("catalog", DataType::Varchar(None), false),
        Column::new("schema", DataType::Varchar(None), false),
    ]))
});

pub static TABLES_SCHMEA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Column::new("table_catalog", DataType::Varchar(None), false),
        Column::new("table_schema", DataType::Varchar(None), false),
        Column::new("table_name", DataType::Varchar(None), false),
        Column::new("first_page_id", DataType::UInt32, false),
    ]))
});

pub static COLUMNS_SCHMEA: LazyLock<SchemaRef> = LazyLock::new(|| {
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

pub static INDEXES_SCHMEA: LazyLock<SchemaRef> = LazyLock::new(|| {
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
        SCHEMAS_SCHMEA.clone(),
    )?;
    let information_schema_tables_last_page_id = load_table_last_page_id(
        catalog,
        information_schema_tables_first_page_id,
        TABLES_SCHMEA.clone(),
    )?;
    let information_schema_columns_last_page_id = load_table_last_page_id(
        catalog,
        information_schema_columns_first_page_id,
        COLUMNS_SCHMEA.clone(),
    )?;
    let information_schema_indexes_last_page_id = load_table_last_page_id(
        catalog,
        information_schema_indexes_first_page_id,
        INDEXES_SCHMEA.clone(),
    )?;

    let mut information_schema = CatalogSchema::new(INFORMATION_SCHEMA_NAME);

    let schemas_table = TableHeap {
        schema: SCHEMAS_SCHMEA.clone(),
        buffer_pool: catalog.buffer_pool.clone(),
        first_page_id: AtomicPageId::new(information_schema_schemas_first_page_id),
        last_page_id: AtomicPageId::new(information_schema_schemas_last_page_id),
    };
    information_schema.tables.insert(
        INFORMATION_SCHEMA_SCHEMAS.to_string(),
        CatalogTable::new(INFORMATION_SCHEMA_SCHEMAS, Arc::new(schemas_table)),
    );

    let tables_table = TableHeap {
        schema: TABLES_SCHMEA.clone(),
        buffer_pool: catalog.buffer_pool.clone(),
        first_page_id: AtomicPageId::new(information_schema_tables_first_page_id),
        last_page_id: AtomicPageId::new(information_schema_tables_last_page_id),
    };
    information_schema.tables.insert(
        INFORMATION_SCHEMA_TABLES.to_string(),
        CatalogTable::new(INFORMATION_SCHEMA_TABLES, Arc::new(tables_table)),
    );

    let columns_table = TableHeap {
        schema: COLUMNS_SCHMEA.clone(),
        buffer_pool: catalog.buffer_pool.clone(),
        first_page_id: AtomicPageId::new(information_schema_columns_first_page_id),
        last_page_id: AtomicPageId::new(information_schema_columns_last_page_id),
    };
    information_schema.tables.insert(
        INFORMATION_SCHEMA_COLUMNS.to_string(),
        CatalogTable::new(INFORMATION_SCHEMA_COLUMNS, Arc::new(columns_table)),
    );

    let indexes_table = TableHeap {
        schema: INDEXES_SCHMEA.clone(),
        buffer_pool: catalog.buffer_pool.clone(),
        first_page_id: AtomicPageId::new(information_schema_indexes_first_page_id),
        last_page_id: AtomicPageId::new(information_schema_indexes_last_page_id),
    };
    information_schema.tables.insert(
        INFORMATION_SCHEMA_INDEXES.to_string(),
        CatalogTable::new(INFORMATION_SCHEMA_INDEXES, Arc::new(indexes_table)),
    );

    catalog.load_schema(INFORMATION_SCHEMA_NAME, information_schema);
    Ok(())
}

fn load_schemas(db: &mut Database) -> QuillSQLResult<()> {
    let schema_tuples = db.run(&format!(
        "select * from {}.{}",
        INFORMATION_SCHEMA_NAME, INFORMATION_SCHEMA_SCHEMAS
    ))?;
    for schema_tuple in schema_tuples.into_iter() {
        let error = Err(QuillSQLError::Internal(format!(
            "Failed to decode schema tuple: {:?}",
            schema_tuple,
        )));
        let ScalarValue::Varchar(Some(_catalog)) = schema_tuple.value(0)? else {
            return error;
        };
        let ScalarValue::Varchar(Some(schema_name)) = schema_tuple.value(1)? else {
            return error;
        };
        db.catalog
            .load_schema(schema_name, CatalogSchema::new(schema_name));
    }
    Ok(())
}

fn load_user_tables(db: &mut Database) -> QuillSQLResult<()> {
    let table_tuples = db.run(&format!(
        "select * from {}.{}",
        INFORMATION_SCHEMA_NAME, INFORMATION_SCHEMA_TABLES
    ))?;
    for table_tuple in table_tuples.into_iter() {
        let error = Err(QuillSQLError::Internal(format!(
            "Failed to decode table tuple: {:?}",
            table_tuple
        )));
        let ScalarValue::Varchar(Some(catalog)) = table_tuple.value(0)? else {
            return error;
        };
        let ScalarValue::Varchar(Some(table_schema)) = table_tuple.value(1)? else {
            return error;
        };
        let ScalarValue::Varchar(Some(table_name)) = table_tuple.value(2)? else {
            return error;
        };
        let ScalarValue::UInt32(Some(first_page_id)) = table_tuple.value(3)? else {
            return error;
        };

        let column_tuples = db.run(&format!("select * from {}.{} where table_catalog = '{}' and table_schema = '{}' and table_name = '{}'",
                                            INFORMATION_SCHEMA_NAME, INFORMATION_SCHEMA_COLUMNS, catalog, table_schema, table_name))?;
        let mut columns = vec![];
        for column_tuple in column_tuples.into_iter() {
            let error = Err(QuillSQLError::Internal(format!(
                "Failed to decode column tuple: {:?}",
                column_tuple
            )));
            let ScalarValue::Varchar(Some(column_name)) = column_tuple.value(3)? else {
                return error;
            };
            let ScalarValue::Varchar(Some(data_type_str)) = column_tuple.value(4)? else {
                return error;
            };
            let ScalarValue::Boolean(Some(nullable)) = column_tuple.value(5)? else {
                return error;
            };
            let ScalarValue::Varchar(Some(default)) = column_tuple.value(6)? else {
                return error;
            };
            let data_type: DataType = data_type_str.as_str().try_into()?;
            let default = ScalarValue::from_string(default, data_type)?;
            columns
                .push(Column::new(column_name.clone(), data_type, *nullable).with_default(default));
        }
        let schema = Arc::new(Schema::new(columns));

        // load last page id
        let last_page_id =
            load_table_last_page_id(&mut db.catalog, *first_page_id, schema.clone())?;
        let table_heap = TableHeap {
            schema: schema.clone(),
            buffer_pool: db.buffer_pool.clone(),
            first_page_id: AtomicPageId::new(*first_page_id),
            last_page_id: AtomicPageId::new(last_page_id),
        };
        db.catalog.load_table(
            TableReference::Full {
                catalog: catalog.to_string(),
                schema: table_schema.to_string(),
                table: table_name.to_string(),
            },
            CatalogTable::new(table_name, Arc::new(table_heap)),
        )?;
    }
    Ok(())
}

fn load_user_indexes(db: &mut Database) -> QuillSQLResult<()> {
    let index_tuples = db.run(&format!(
        "select * from {}.{}",
        INFORMATION_SCHEMA_NAME, INFORMATION_SCHEMA_INDEXES
    ))?;
    for index_tuple in index_tuples.into_iter() {
        let error = Err(QuillSQLError::Internal(format!(
            "Failed to decode index tuple: {:?}",
            index_tuple
        )));
        let ScalarValue::Varchar(Some(catalog_name)) = index_tuple.value(0)? else {
            return error;
        };
        let ScalarValue::Varchar(Some(table_schema_name)) = index_tuple.value(1)? else {
            return error;
        };
        let ScalarValue::Varchar(Some(table_name)) = index_tuple.value(2)? else {
            return error;
        };
        let ScalarValue::Varchar(Some(index_name)) = index_tuple.value(3)? else {
            return error;
        };
        let ScalarValue::Varchar(Some(key_schema_str)) = index_tuple.value(4)? else {
            return error;
        };
        let ScalarValue::UInt32(Some(internal_max_size)) = index_tuple.value(5)? else {
            return error;
        };
        let ScalarValue::UInt32(Some(leaf_max_size)) = index_tuple.value(6)? else {
            return error;
        };
        let ScalarValue::UInt32(Some(header_page_id)) = index_tuple.value(7)? else {
            return error;
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
