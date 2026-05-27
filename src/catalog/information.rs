use std::sync::{Arc, LazyLock};

use crate::catalog::{
    Catalog, CatalogSchema, CatalogTable, Column, DataType, Schema, SchemaRef,
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME,
};
use crate::database::Database;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::tuple::Tuple;
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
        Column::new("table_id", DataType::UInt64, false),
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
        Column::new("index_id", DataType::UInt64, false),
    ]))
});

pub fn load_catalog_data(db: &mut Database) -> QuillSQLResult<()> {
    load_information_schema(&mut db.catalog)?;
    load_descriptor_tables(db)?;
    create_default_schema_if_not_exists(&mut db.catalog)?;
    load_descriptor_indexes(db)?;
    Ok(())
}

fn create_default_schema_if_not_exists(catalog: &mut Catalog) -> QuillSQLResult<()> {
    if !catalog.schemas.contains_key(DEFAULT_SCHEMA_NAME) {
        catalog.create_schema(DEFAULT_SCHEMA_NAME)?;
    }
    Ok(())
}

fn load_information_schema(catalog: &mut Catalog) -> QuillSQLResult<()> {
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
        catalog.holt_store.drop_table_descriptor(&table_ref)?;
        information_schema.tables.insert(
            table_name.to_string(),
            CatalogTable::virtual_table(table_name, schema),
        );
    }

    catalog.load_schema(INFORMATION_SCHEMA_NAME, information_schema);
    Ok(())
}

fn load_descriptor_tables(db: &mut Database) -> QuillSQLResult<()> {
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
        db.catalog.load_table(
            descriptor.table_ref,
            descriptor.table_name,
            descriptor.schema,
            descriptor.table_id,
        )?;
    }
    Ok(())
}

fn load_descriptor_indexes(db: &mut Database) -> QuillSQLResult<()> {
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
        db.catalog.load_index(
            descriptor.table_ref,
            descriptor.index_name,
            descriptor.key_schema,
            descriptor.index_id,
        )?;
    }
    Ok(())
}

impl Catalog {
    pub fn virtual_table_rows(
        &self,
        table_ref: &TableReference,
    ) -> QuillSQLResult<Option<Vec<Tuple>>> {
        if table_ref.schema() != Some(INFORMATION_SCHEMA_NAME) {
            return Ok(None);
        }

        let rows = match table_ref.table() {
            name if name == INFORMATION_SCHEMA_SCHEMAS => self.schema_rows(),
            name if name == INFORMATION_SCHEMA_TABLES => self.table_rows(),
            name if name == INFORMATION_SCHEMA_COLUMNS => self.column_rows(),
            name if name == INFORMATION_SCHEMA_INDEXES => self.index_rows(),
            table => {
                return Err(QuillSQLError::Internal(format!(
                    "unknown information_schema table {table}"
                )))
            }
        };
        Ok(Some(rows))
    }

    fn schema_rows(&self) -> Vec<Tuple> {
        let mut schema_names = self
            .schemas
            .keys()
            .filter(|name| name.as_str() != INFORMATION_SCHEMA_NAME)
            .cloned()
            .collect::<Vec<_>>();
        schema_names.sort();

        schema_names
            .into_iter()
            .map(|schema_name| {
                Tuple::new(
                    SCHEMAS_SCHEMA.clone(),
                    vec![DEFAULT_CATALOG_NAME.to_string().into(), schema_name.into()],
                )
            })
            .collect()
    }

    fn table_rows(&self) -> Vec<Tuple> {
        let mut rows = Vec::new();
        for (schema_name, table_name, table) in self.user_tables() {
            let table_id = table.table_id().unwrap_or(0);
            rows.push(Tuple::new(
                TABLES_SCHEMA.clone(),
                vec![
                    DEFAULT_CATALOG_NAME.to_string().into(),
                    schema_name.into(),
                    table_name.into(),
                    table_id.into(),
                ],
            ));
        }
        rows
    }

    fn column_rows(&self) -> Vec<Tuple> {
        let mut rows = Vec::new();
        for (schema_name, table_name, table) in self.user_tables() {
            for col in &table.schema.columns {
                let sql_type: sqlparser::ast::DataType = (&col.data_type).into();
                rows.push(Tuple::new(
                    COLUMNS_SCHEMA.clone(),
                    vec![
                        DEFAULT_CATALOG_NAME.to_string().into(),
                        schema_name.clone().into(),
                        table_name.clone().into(),
                        col.name.clone().into(),
                        format!("{sql_type}").into(),
                        col.nullable.into(),
                        format!("{}", col.default).into(),
                    ],
                ));
            }
        }
        rows
    }

    fn index_rows(&self) -> Vec<Tuple> {
        let mut rows = Vec::new();
        for (schema_name, table_name, table) in self.user_tables() {
            let mut indexes = table.indexes.iter().collect::<Vec<_>>();
            indexes.sort_by(|left, right| left.0.cmp(right.0));
            for (index_name, index) in indexes {
                rows.push(Tuple::new(
                    INDEXES_SCHEMA.clone(),
                    vec![
                        DEFAULT_CATALOG_NAME.to_string().into(),
                        schema_name.clone().into(),
                        table_name.clone().into(),
                        index_name.clone().into(),
                        key_schema_to_varchar(index.key_schema.as_ref()).into(),
                        index.index_id.into(),
                    ],
                ));
            }
        }
        rows
    }

    fn user_tables(&self) -> Vec<(String, String, &CatalogTable)> {
        let mut schema_names = self
            .schemas
            .keys()
            .filter(|name| name.as_str() != INFORMATION_SCHEMA_NAME)
            .cloned()
            .collect::<Vec<_>>();
        schema_names.sort();

        let mut tables = Vec::new();
        for schema_name in schema_names {
            let Some(schema) = self.schemas.get(&schema_name) else {
                continue;
            };
            let mut table_names = schema.tables.keys().cloned().collect::<Vec<_>>();
            table_names.sort();
            for table_name in table_names {
                if let Some(table) = schema.tables.get(&table_name) {
                    tables.push((schema_name.clone(), table_name, table));
                }
            }
        }
        tables
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
