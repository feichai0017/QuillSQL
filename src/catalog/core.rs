use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog::{SchemaRef, INFORMATION_SCHEMA_NAME};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::holt::HoltStore;
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
}

impl CatalogTable {
    pub fn persistent(name: impl Into<String>, schema: SchemaRef, table_id: u64) -> Self {
        Self {
            name: name.into(),
            schema,
            table_id,
            indexes: HashMap::new(),
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
        Ok(())
    }

    pub fn create_table(
        &mut self,
        table_ref: TableReference,
        schema: SchemaRef,
    ) -> QuillSQLResult<u64> {
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
                CatalogTable::persistent(table_name.clone(), schema.clone(), table_id),
            );
        Ok(table_id)
    }

    pub fn drop_table(&mut self, table_ref: &TableReference) -> QuillSQLResult<bool> {
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
        Ok(true)
    }

    pub fn table_schema(&self, table_ref: &TableReference) -> QuillSQLResult<SchemaRef> {
        Ok(self.catalog_table(table_ref)?.schema.clone())
    }

    pub fn table_id(&self, table_ref: &TableReference) -> QuillSQLResult<u64> {
        Ok(self.catalog_table(table_ref)?.table_id)
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
        Ok(index_id)
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
            CatalogTable::persistent(table_name, schema, table_id),
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
}
