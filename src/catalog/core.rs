use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog::{
    key_schema_to_varchar, Schema, SchemaRef, TableStatistics, COLUMNS_SCHEMA, INDEXES_SCHEMA,
    INFORMATION_SCHEMA_COLUMNS, INFORMATION_SCHEMA_INDEXES, INFORMATION_SCHEMA_NAME,
    INFORMATION_SCHEMA_SCHEMAS, INFORMATION_SCHEMA_TABLES, SCHEMAS_SCHEMA, TABLES_SCHEMA,
};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::engine::TableHandle;
use crate::storage::holt::{HoltStore, HoltTableHandle};
use crate::storage::tuple::Tuple;
use crate::utils::scalar::ScalarValue;
use crate::utils::table_ref::TableReference;

pub static DEFAULT_CATALOG_NAME: &str = "quillsql";
pub static DEFAULT_SCHEMA_NAME: &str = "public";

#[derive(Debug)]
pub struct Catalog {
    pub schemas: HashMap<String, CatalogSchema>,
    pub holt_store: Arc<HoltStore>,
}

#[derive(Debug)]
pub struct CatalogSchema {
    pub name: String,
    pub tables: HashMap<String, CatalogTable>,
}

impl CatalogSchema {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tables: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct CatalogTable {
    pub name: String,
    pub schema: SchemaRef,
    pub table_id: u64,
    pub indexes: HashMap<String, CatalogIndex>,
    pub stats: Option<TableStatistics>,
}

impl CatalogTable {
    pub fn new(name: impl Into<String>, schema: SchemaRef, table_id: u64) -> Self {
        Self {
            name: name.into(),
            schema,
            table_id,
            indexes: HashMap::new(),
            stats: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CatalogIndex {
    pub name: String,
    pub key_schema: SchemaRef,
    pub index_id: u64,
}

impl CatalogIndex {
    pub fn new(name: impl Into<String>, key_schema: SchemaRef, index_id: u64) -> Self {
        Self {
            name: name.into(),
            key_schema,
            index_id,
        }
    }
}

impl Catalog {
    pub fn new(holt_store: Arc<HoltStore>) -> Self {
        Self {
            schemas: HashMap::new(),
            holt_store,
        }
    }

    pub fn create_schema(&mut self, schema_name: impl Into<String>) -> QuillSQLResult<()> {
        let schema_name = schema_name.into();
        if self.schemas.contains_key(&schema_name) {
            return Err(QuillSQLError::Storage(
                "Cannot create duplicated schema".to_string(),
            ));
        }
        self.schemas
            .insert(schema_name.clone(), CatalogSchema::new(schema_name.clone()));

        let tuple = Tuple::new(
            SCHEMAS_SCHEMA.clone(),
            vec![
                DEFAULT_CATALOG_NAME.to_string().into(),
                schema_name.clone().into(),
            ],
        );
        self.insert_system_tuple(INFORMATION_SCHEMA_SCHEMAS, &tuple)?;
        Ok(())
    }

    pub fn create_table(
        &mut self,
        table_ref: TableReference,
        schema: SchemaRef,
    ) -> QuillSQLResult<u64> {
        let catalog_name = table_ref
            .catalog()
            .unwrap_or(DEFAULT_CATALOG_NAME)
            .to_string();
        let catalog_schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        let table_name = table_ref.table().to_string();

        {
            let Some(catalog_schema) = self.schemas.get_mut(&catalog_schema_name) else {
                return Err(QuillSQLError::Storage(format!(
                    "catalog schema {} not created yet",
                    catalog_schema_name
                )));
            };
            if catalog_schema.tables.contains_key(table_ref.table()) {
                return Err(QuillSQLError::Storage(
                    "Cannot create duplicated table".to_string(),
                ));
            }
        }

        let table_id = self
            .holt_store
            .create_table_descriptor(&table_ref, schema.as_ref())?;
        self.schemas
            .get_mut(&catalog_schema_name)
            .expect("schema checked above")
            .tables
            .insert(
                table_name.clone(),
                CatalogTable::new(table_name.clone(), schema.clone(), table_id),
            );

        self.insert_table_metadata(&catalog_name, &catalog_schema_name, &table_name, &schema)?;
        Ok(table_id)
    }

    pub fn drop_table(&mut self, table_ref: &TableReference) -> QuillSQLResult<bool> {
        let catalog_name = table_ref
            .catalog()
            .unwrap_or(DEFAULT_CATALOG_NAME)
            .to_string();
        let schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        let table_name = table_ref.table().to_string();

        if schema_name == INFORMATION_SCHEMA_NAME {
            return Err(QuillSQLError::Execution(
                "dropping information_schema tables is not allowed".to_string(),
            ));
        }

        let Some(catalog_table) = self
            .schemas
            .get(&schema_name)
            .and_then(|schema| schema.tables.get(&table_name))
        else {
            return Ok(false);
        };
        let index_names = catalog_table.indexes.keys().cloned().collect::<Vec<_>>();

        for index_name in &index_names {
            self.holt_store
                .drop_index_descriptor(table_ref, index_name)?;
        }
        self.holt_store.drop_table_descriptor(table_ref)?;

        self.schemas
            .get_mut(&schema_name)
            .expect("table existence checked above")
            .tables
            .remove(&table_name);

        for index_name in index_names {
            self.remove_index_metadata(&catalog_name, &schema_name, &table_name, &index_name)?;
        }
        self.remove_table_metadata(&catalog_name, &schema_name, &table_name)?;
        Ok(true)
    }

    pub fn table_schema(&self, table_ref: &TableReference) -> QuillSQLResult<SchemaRef> {
        Ok(self.catalog_table(table_ref)?.schema.clone())
    }

    pub fn table_id(&self, table_ref: &TableReference) -> QuillSQLResult<u64> {
        Ok(self.catalog_table(table_ref)?.table_id)
    }

    pub fn table_statistics(&self, table_ref: &TableReference) -> Option<&TableStatistics> {
        let schema_name = table_ref.schema().unwrap_or(DEFAULT_SCHEMA_NAME);
        self.schemas
            .get(schema_name)
            .and_then(|schema| schema.tables.get(table_ref.table()))
            .and_then(|table| table.stats.as_ref())
    }

    pub fn analyze_table(&mut self, table_ref: &TableReference) -> QuillSQLResult<TableStatistics> {
        let schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        let table_name = table_ref.table().to_string();
        let catalog_table = self.catalog_table(table_ref)?;
        let schema = catalog_table.schema.clone();
        let table = HoltTableHandle::new(
            table_ref.clone(),
            schema.clone(),
            catalog_table.table_id,
            self.holt_store.clone(),
        );
        let mut stats = TableStatistics::empty(schema.as_ref());
        let mut stream = table.full_scan()?;
        while let Some((_rid, meta, tuple)) = stream.next()? {
            if !meta.is_deleted {
                stats.record_tuple(&tuple);
            }
        }
        self.schemas
            .get_mut(&schema_name)
            .and_then(|schema| schema.tables.get_mut(&table_name))
            .expect("table existence checked above")
            .stats = Some(stats.clone());
        Ok(stats)
    }

    pub fn try_table_schema(&self, table_ref: &TableReference) -> Option<SchemaRef> {
        let schema_name = table_ref.schema().unwrap_or(DEFAULT_SCHEMA_NAME);
        self.schemas
            .get(schema_name)
            .and_then(|schema| schema.tables.get(table_ref.table()))
            .map(|table| table.schema.clone())
    }

    pub fn table_indexes(&self, table_ref: &TableReference) -> QuillSQLResult<Vec<CatalogIndex>> {
        Ok(self
            .catalog_table(table_ref)?
            .indexes
            .values()
            .cloned()
            .collect())
    }

    pub fn create_index(
        &mut self,
        index_name: String,
        table_ref: &TableReference,
        key_schema: SchemaRef,
    ) -> QuillSQLResult<u64> {
        let catalog_name = table_ref
            .catalog()
            .unwrap_or(DEFAULT_CATALOG_NAME)
            .to_string();
        let schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        let table_name = table_ref.table().to_string();

        {
            let table = self.catalog_table_mut(table_ref)?;
            if table.indexes.contains_key(&index_name) {
                return Err(QuillSQLError::Storage(
                    "Cannot create duplicated index".to_string(),
                ));
            }
        }

        let index_id =
            self.holt_store
                .create_index_descriptor(table_ref, &index_name, key_schema.as_ref())?;
        self.catalog_table_mut(table_ref)?.indexes.insert(
            index_name.clone(),
            CatalogIndex::new(index_name.clone(), key_schema.clone(), index_id),
        );
        self.insert_index_metadata(
            &catalog_name,
            &schema_name,
            &table_name,
            &index_name,
            key_schema.as_ref(),
        )?;
        Ok(index_id)
    }

    pub fn drop_index(
        &mut self,
        table_ref: &TableReference,
        index_name: &str,
    ) -> QuillSQLResult<bool> {
        let catalog_name = table_ref
            .catalog()
            .unwrap_or(DEFAULT_CATALOG_NAME)
            .to_string();
        let schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        let table_name = table_ref.table().to_string();

        if schema_name == INFORMATION_SCHEMA_NAME {
            return Err(QuillSQLError::Execution(
                "dropping indexes on information_schema tables is not allowed".to_string(),
            ));
        }

        if !self
            .catalog_table(table_ref)?
            .indexes
            .contains_key(index_name)
        {
            return Ok(false);
        }
        self.holt_store
            .drop_index_descriptor(table_ref, index_name)?;
        self.catalog_table_mut(table_ref)?
            .indexes
            .remove(index_name);
        self.remove_index_metadata(&catalog_name, &schema_name, &table_name, index_name)?;
        Ok(true)
    }

    pub fn find_index_owner(
        &self,
        catalog_hint: Option<&str>,
        schema_hint: Option<&str>,
        index_name: &str,
    ) -> Option<TableReference> {
        let catalog_name = catalog_hint.unwrap_or(DEFAULT_CATALOG_NAME);

        if let Some(schema_name) = schema_hint {
            return self.find_index_in_schema(catalog_name, schema_name, index_name);
        }

        for schema_name in self.schemas.keys() {
            if schema_name == INFORMATION_SCHEMA_NAME {
                continue;
            }
            if let Some(table_ref) =
                self.find_index_in_schema(catalog_name, schema_name, index_name)
            {
                return Some(table_ref);
            }
        }
        None
    }

    pub fn load_schema(&mut self, name: impl Into<String>, schema: CatalogSchema) {
        self.schemas.insert(name.into(), schema);
    }

    pub fn load_table(
        &mut self,
        table_ref: TableReference,
        table_name: impl Into<String>,
        schema: SchemaRef,
        table_id: u64,
    ) -> QuillSQLResult<()> {
        let schema_name = table_ref.schema().unwrap_or(DEFAULT_SCHEMA_NAME);
        let catalog_schema = self.schemas.get_mut(schema_name).ok_or_else(|| {
            QuillSQLError::Storage(format!("catalog schema {} not created yet", schema_name))
        })?;
        catalog_schema.tables.insert(
            table_ref.table().to_string(),
            CatalogTable::new(table_name, schema, table_id),
        );
        Ok(())
    }

    pub fn load_index(
        &mut self,
        table_ref: TableReference,
        index_name: impl Into<String>,
        key_schema: SchemaRef,
        index_id: u64,
    ) -> QuillSQLResult<()> {
        let idx_name = index_name.into();
        self.catalog_table_mut(&table_ref)?.indexes.insert(
            idx_name.clone(),
            CatalogIndex::new(idx_name, key_schema, index_id),
        );
        Ok(())
    }

    pub fn refresh_information_schema_projection(&self) -> QuillSQLResult<()> {
        self.delete_matching_system_rows(INFORMATION_SCHEMA_SCHEMAS, |tuple| {
            let ScalarValue::Varchar(Some(schema)) = tuple.value(1)? else {
                return Ok(false);
            };
            Ok(schema != INFORMATION_SCHEMA_NAME)
        })?;
        for table_name in [
            INFORMATION_SCHEMA_TABLES,
            INFORMATION_SCHEMA_COLUMNS,
            INFORMATION_SCHEMA_INDEXES,
        ] {
            self.delete_matching_system_rows(table_name, |tuple| {
                let ScalarValue::Varchar(Some(schema)) = tuple.value(1)? else {
                    return Ok(false);
                };
                Ok(schema != INFORMATION_SCHEMA_NAME)
            })?;
        }

        for (schema_name, schema) in &self.schemas {
            if schema_name == INFORMATION_SCHEMA_NAME {
                continue;
            }
            let tuple = Tuple::new(
                SCHEMAS_SCHEMA.clone(),
                vec![
                    DEFAULT_CATALOG_NAME.to_string().into(),
                    schema_name.clone().into(),
                ],
            );
            self.insert_system_tuple(INFORMATION_SCHEMA_SCHEMAS, &tuple)?;

            for (table_name, table) in &schema.tables {
                self.insert_table_metadata(
                    DEFAULT_CATALOG_NAME,
                    schema_name,
                    table_name,
                    table.schema.as_ref(),
                )?;
                for (index_name, index) in &table.indexes {
                    self.insert_index_metadata(
                        DEFAULT_CATALOG_NAME,
                        schema_name,
                        table_name,
                        index_name,
                        index.key_schema.as_ref(),
                    )?;
                }
            }
        }
        Ok(())
    }

    fn catalog_table(&self, table_ref: &TableReference) -> QuillSQLResult<&CatalogTable> {
        let schema_name = table_ref.schema().unwrap_or(DEFAULT_SCHEMA_NAME);
        self.schemas
            .get(schema_name)
            .ok_or_else(|| {
                QuillSQLError::Storage(format!("catalog schema {} not created yet", schema_name))
            })?
            .tables
            .get(table_ref.table())
            .ok_or_else(|| {
                QuillSQLError::Storage(format!("table {} not created yet", table_ref.table()))
            })
    }

    fn catalog_table_mut(
        &mut self,
        table_ref: &TableReference,
    ) -> QuillSQLResult<&mut CatalogTable> {
        let schema_name = table_ref.schema().unwrap_or(DEFAULT_SCHEMA_NAME);
        self.schemas
            .get_mut(schema_name)
            .ok_or_else(|| {
                QuillSQLError::Storage(format!("catalog schema {} not created yet", schema_name))
            })?
            .tables
            .get_mut(table_ref.table())
            .ok_or_else(|| {
                QuillSQLError::Storage(format!("table {} not created yet", table_ref.table()))
            })
    }

    fn information_schema_table(&self, table_name: &str) -> QuillSQLResult<&CatalogTable> {
        self.schemas
            .get(INFORMATION_SCHEMA_NAME)
            .ok_or_else(|| {
                QuillSQLError::Internal(
                    "catalog schema information_schema not created yet".to_string(),
                )
            })?
            .tables
            .get(table_name)
            .ok_or_else(|| {
                QuillSQLError::Internal(format!(
                    "table information_schema.{table_name} not created yet"
                ))
            })
    }

    fn insert_system_tuple(&self, table_name: &str, tuple: &Tuple) -> QuillSQLResult<()> {
        let table_id = self.information_schema_table(table_name)?.table_id;
        let _ = self.holt_store.insert_system_row(table_id, tuple)?;
        Ok(())
    }

    fn insert_table_metadata(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        schema: &Schema,
    ) -> QuillSQLResult<()> {
        let tuple = Tuple::new(
            TABLES_SCHEMA.clone(),
            vec![
                catalog_name.to_string().into(),
                schema_name.to_string().into(),
                table_name.to_string().into(),
                0u32.into(),
            ],
        );
        self.insert_system_tuple(INFORMATION_SCHEMA_TABLES, &tuple)?;

        for col in &schema.columns {
            let sql_type: sqlparser::ast::DataType = (&col.data_type).into();
            let tuple = Tuple::new(
                COLUMNS_SCHEMA.clone(),
                vec![
                    catalog_name.to_string().into(),
                    schema_name.to_string().into(),
                    table_name.to_string().into(),
                    col.name.clone().into(),
                    format!("{sql_type}").into(),
                    col.nullable.into(),
                    format!("{}", col.default).into(),
                ],
            );
            self.insert_system_tuple(INFORMATION_SCHEMA_COLUMNS, &tuple)?;
        }
        Ok(())
    }

    fn insert_index_metadata(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        index_name: &str,
        key_schema: &Schema,
    ) -> QuillSQLResult<()> {
        let tuple = Tuple::new(
            INDEXES_SCHEMA.clone(),
            vec![
                catalog_name.to_string().into(),
                schema_name.to_string().into(),
                table_name.to_string().into(),
                index_name.to_string().into(),
                key_schema_to_varchar(key_schema).into(),
                0u32.into(),
                0u32.into(),
                0u32.into(),
            ],
        );
        self.insert_system_tuple(INFORMATION_SCHEMA_INDEXES, &tuple)
    }

    fn remove_table_metadata(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> QuillSQLResult<()> {
        for table in [
            INFORMATION_SCHEMA_TABLES,
            INFORMATION_SCHEMA_COLUMNS,
            INFORMATION_SCHEMA_INDEXES,
        ] {
            self.delete_matching_system_rows(table, |tuple| {
                let ScalarValue::Varchar(Some(catalog)) = tuple.value(0)? else {
                    return Ok(false);
                };
                let ScalarValue::Varchar(Some(schema)) = tuple.value(1)? else {
                    return Ok(false);
                };
                let ScalarValue::Varchar(Some(row_table)) = tuple.value(2)? else {
                    return Ok(false);
                };
                Ok(catalog == catalog_name && schema == schema_name && row_table == table_name)
            })?;
        }
        Ok(())
    }

    fn remove_index_metadata(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        index_name: &str,
    ) -> QuillSQLResult<()> {
        self.delete_matching_system_rows(INFORMATION_SCHEMA_INDEXES, |tuple| {
            let ScalarValue::Varchar(Some(catalog)) = tuple.value(0)? else {
                return Ok(false);
            };
            let ScalarValue::Varchar(Some(schema)) = tuple.value(1)? else {
                return Ok(false);
            };
            let ScalarValue::Varchar(Some(row_table)) = tuple.value(2)? else {
                return Ok(false);
            };
            let ScalarValue::Varchar(Some(index)) = tuple.value(3)? else {
                return Ok(false);
            };
            Ok(catalog == catalog_name
                && schema == schema_name
                && row_table == table_name
                && index == index_name)
        })
    }

    fn delete_matching_system_rows<F>(
        &self,
        table_name: &str,
        mut predicate: F,
    ) -> QuillSQLResult<()>
    where
        F: FnMut(&Tuple) -> QuillSQLResult<bool>,
    {
        let table = self.information_schema_table(table_name)?;
        let table_id = table.table_id;
        let table_ref = TableReference::Full {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: INFORMATION_SCHEMA_NAME.to_string(),
            table: table_name.to_string(),
        };
        let handle = HoltTableHandle::new(
            table_ref,
            table.schema.clone(),
            table_id,
            self.holt_store.clone(),
        );
        let mut stream = handle.full_scan()?;
        let mut deleted = Vec::new();
        while let Some((rid, meta, tuple)) = stream.next()? {
            if !meta.is_deleted && predicate(&tuple)? {
                deleted.push(rid);
            }
        }
        for rid in deleted {
            self.holt_store
                .mark_system_row_deleted(table_id, table.schema.clone(), rid)?;
        }
        Ok(())
    }

    fn find_index_in_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
        index_name: &str,
    ) -> Option<TableReference> {
        let schema = self.schemas.get(schema_name)?;
        for (table_name, table) in &schema.tables {
            if table.indexes.contains_key(index_name) {
                if catalog_name == DEFAULT_CATALOG_NAME {
                    if schema_name == DEFAULT_SCHEMA_NAME {
                        return Some(TableReference::Bare {
                            table: table_name.clone(),
                        });
                    }
                    return Some(TableReference::Partial {
                        schema: schema_name.to_string(),
                        table: table_name.clone(),
                    });
                }
                return Some(TableReference::Full {
                    catalog: catalog_name.to_string(),
                    schema: schema_name.to_string(),
                    table: table_name.clone(),
                });
            }
        }
        None
    }
}
