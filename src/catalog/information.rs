use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use crate::catalog::{
    Catalog, CatalogSchema, CatalogTable, Column, DataType, Schema, SchemaRef, TableBackend,
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME,
};
use crate::database::Database;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::engine::TableHandle;
use crate::storage::holt::HoltTableHandle;
use crate::storage::tuple::Tuple;
use crate::utils::scalar::ScalarValue;
use crate::utils::table_ref::TableReference;

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
    load_holt_catalog_tables(db)?;
    load_user_indexes(db)?;
    load_holt_catalog_indexes(db)?;
    db.catalog.refresh_information_schema_projection()?;
    Ok(())
}

fn create_default_schema_if_not_exists(catalog: &mut Catalog) -> QuillSQLResult<()> {
    if !catalog.schemas.contains_key(DEFAULT_SCHEMA_NAME) {
        catalog.create_schema(DEFAULT_SCHEMA_NAME)?;
    }
    Ok(())
}

fn load_information_schema(catalog: &mut Catalog) -> QuillSQLResult<()> {
    let holt = catalog.holt_store.clone();
    let mut information_schema = CatalogSchema::new(INFORMATION_SCHEMA_NAME);

    for (table_name, schema) in [
        (INFORMATION_SCHEMA_SCHEMAS, SCHEMAS_SCHEMA.clone()),
        (INFORMATION_SCHEMA_TABLES, TABLES_SCHEMA.clone()),
        (INFORMATION_SCHEMA_COLUMNS, COLUMNS_SCHEMA.clone()),
        (INFORMATION_SCHEMA_INDEXES, INDEXES_SCHEMA.clone()),
    ] {
        let table_ref = TableReference::Full {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: INFORMATION_SCHEMA_NAME.to_string(),
            table: table_name.to_string(),
        };
        let table_id = match holt.table_descriptor(&table_ref)? {
            Some(table_id) => table_id,
            None => holt.create_table_descriptor(&table_ref, schema.as_ref())?,
        };
        information_schema.tables.insert(
            table_name.to_string(),
            CatalogTable::new_holt(table_name, schema, table_id),
        );
    }

    catalog.load_schema(INFORMATION_SCHEMA_NAME, information_schema);
    Ok(())
}

fn load_schemas(db: &mut Database) -> QuillSQLResult<()> {
    for tuple in information_schema_rows(db, INFORMATION_SCHEMA_SCHEMAS)? {
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
    let mut column_map: HashMap<(String, String, String), Vec<Column>> = HashMap::new();
    for tuple in information_schema_rows(db, INFORMATION_SCHEMA_COLUMNS)? {
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

    for tuple in information_schema_rows(db, INFORMATION_SCHEMA_TABLES)? {
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
        let table_ref = TableReference::Full {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        };
        let Some(table_id) = db.catalog.holt_store.table_descriptor(&table_ref)? else {
            continue;
        };
        let columns = column_map
            .remove(&(catalog.clone(), schema.clone(), table.clone()))
            .unwrap_or_default();
        db.catalog.load_holt_table(
            table_ref,
            table.clone(),
            Arc::new(Schema::new(columns)),
            table_id,
        )?;
    }
    Ok(())
}

fn load_user_indexes(db: &mut Database) -> QuillSQLResult<()> {
    for tuple in information_schema_rows(db, INFORMATION_SCHEMA_INDEXES)? {
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

        let table_ref = TableReference::Full {
            catalog: catalog_name.clone(),
            schema: table_schema_name.clone(),
            table: table_name.clone(),
        };
        let Some(index_id) = db
            .catalog
            .holt_store
            .index_descriptor(&table_ref, index_name)?
        else {
            continue;
        };
        let table_schema = db.catalog.table_schema(&table_ref)?;
        let key_schema = Arc::new(parse_key_schema_from_varchar(
            key_schema_str.as_str(),
            table_schema,
        )?);
        db.catalog
            .load_holt_index(table_ref, index_name, key_schema, index_id)?;
    }
    Ok(())
}

fn load_holt_catalog_tables(db: &mut Database) -> QuillSQLResult<()> {
    for descriptor in db.catalog.holt_store.table_descriptors()? {
        if descriptor.table_ref.schema() == Some(INFORMATION_SCHEMA_NAME) {
            continue;
        }
        if db.catalog.try_table_schema(&descriptor.table_ref).is_some() {
            continue;
        }
        let schema_name = descriptor
            .table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        if !db.catalog.schemas.contains_key(&schema_name) {
            db.catalog
                .load_schema(schema_name.clone(), CatalogSchema::new(schema_name));
        }
        db.catalog.load_holt_table(
            descriptor.table_ref,
            descriptor.table_name,
            descriptor.schema,
            descriptor.table_id,
        )?;
    }
    Ok(())
}

fn load_holt_catalog_indexes(db: &mut Database) -> QuillSQLResult<()> {
    for descriptor in db.catalog.holt_store.index_descriptors()? {
        if descriptor.table_ref.schema() == Some(INFORMATION_SCHEMA_NAME) {
            continue;
        }
        if db.catalog.try_table_schema(&descriptor.table_ref).is_none() {
            continue;
        }
        if db
            .catalog
            .table_indexes(&descriptor.table_ref)
            .map(|indexes| {
                indexes
                    .iter()
                    .any(|index| index.name == descriptor.index_name)
            })
            .unwrap_or(false)
        {
            continue;
        }
        db.catalog.load_holt_index(
            descriptor.table_ref,
            descriptor.index_name,
            descriptor.key_schema,
            descriptor.index_id,
        )?;
    }
    Ok(())
}

fn information_schema_rows(db: &Database, table_name: &str) -> QuillSQLResult<Vec<Tuple>> {
    let information_schema = db
        .catalog
        .schemas
        .get(INFORMATION_SCHEMA_NAME)
        .ok_or_else(|| QuillSQLError::Internal("information_schema not initialized".to_string()))?;
    let table = information_schema.tables.get(table_name).ok_or_else(|| {
        QuillSQLError::Internal(format!("information_schema.{table_name} missing"))
    })?;
    let TableBackend::Holt { table_id } = table.backend;
    let table_ref = TableReference::Full {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: INFORMATION_SCHEMA_NAME.to_string(),
        table: table_name.to_string(),
    };
    let handle = HoltTableHandle::new(
        table_ref,
        table.schema.clone(),
        table_id,
        db.catalog.holt_store.clone(),
    );
    let mut stream = handle.full_scan()?;
    let mut rows = Vec::new();
    while let Some((_rid, meta, tuple)) = stream.next()? {
        if !meta.is_deleted {
            rows.push(tuple);
        }
    }
    Ok(rows)
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
        .split(',')
        .map(|name| name.trim())
        .collect::<Vec<&str>>();
    let indices = column_names
        .into_iter()
        .map(|name| table_schema.index_of(None, name))
        .collect::<QuillSQLResult<Vec<usize>>>()?;
    table_schema.project(&indices)
}
