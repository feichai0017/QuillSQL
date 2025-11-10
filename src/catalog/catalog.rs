use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::registry::TableRegistry;
use crate::catalog::{
    key_schema_to_varchar, SchemaRef, TableStatistics, COLUMNS_SCHEMA, INDEXES_SCHEMA,
    INFORMATION_SCHEMA_COLUMNS, INFORMATION_SCHEMA_INDEXES, INFORMATION_SCHEMA_NAME,
    INFORMATION_SCHEMA_SCHEMAS, INFORMATION_SCHEMA_TABLES, SCHEMAS_SCHEMA, TABLES_SCHEMA,
};
use crate::storage::disk_manager::DiskManager;
use crate::storage::page::{
    BPLUS_INTERNAL_PAGE_MAX_SIZE, BPLUS_LEAF_PAGE_MAX_SIZE, EMPTY_TUPLE_META,
};
use crate::storage::table_heap::{TableHeap, TableIterator};
use crate::storage::tuple::Tuple;
use crate::transaction::{CommandId, TransactionId};
use crate::utils::scalar::ScalarValue;
use crate::utils::table_ref::TableReference;
use crate::{
    buffer::BufferManager,
    error::{QuillSQLError, QuillSQLResult},
    storage::index::btree_index::BPlusTreeIndex,
};

pub static DEFAULT_CATALOG_NAME: &str = "quillsql";
pub static DEFAULT_SCHEMA_NAME: &str = "public";

#[derive(Debug)]
pub struct Catalog {
    pub schemas: HashMap<String, CatalogSchema>,
    pub buffer_pool: Arc<BufferManager>,
    pub disk_manager: Arc<DiskManager>,
    table_registry: Arc<TableRegistry>,
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
    pub table: Arc<TableHeap>,
    pub indexes: HashMap<String, Arc<BPlusTreeIndex>>,
    pub stats: Option<TableStatistics>,
}

impl CatalogTable {
    pub fn new(name: impl Into<String>, table: Arc<TableHeap>) -> Self {
        Self {
            name: name.into(),
            table,
            indexes: HashMap::new(),
            stats: None,
        }
    }
}

const SYSTEM_TXN_ID: TransactionId = 0;
const SYSTEM_COMMAND_ID: CommandId = 0;

impl Catalog {
    pub fn new(
        buffer_pool: Arc<BufferManager>,
        disk_manager: Arc<DiskManager>,
        table_registry: Arc<TableRegistry>,
    ) -> Self {
        Self {
            schemas: HashMap::new(),
            buffer_pool,
            disk_manager,
            table_registry,
        }
    }

    pub fn table_registry(&self) -> Arc<TableRegistry> {
        self.table_registry.clone()
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

        // update system table
        let Some(information_schema) = self.schemas.get_mut(INFORMATION_SCHEMA_NAME) else {
            return Err(QuillSQLError::Internal(
                "catalog schema information_schema not created yet".to_string(),
            ));
        };
        let Some(schemas_table) = information_schema
            .tables
            .get_mut(INFORMATION_SCHEMA_SCHEMAS)
        else {
            return Err(QuillSQLError::Internal(
                "table information_schema.schemas not created yet".to_string(),
            ));
        };

        let tuple = Tuple::new(
            SCHEMAS_SCHEMA.clone(),
            vec![
                DEFAULT_CATALOG_NAME.to_string().into(),
                schema_name.clone().into(),
            ],
        );
        schemas_table
            .table
            .insert_tuple(&EMPTY_TUPLE_META, &tuple)?;
        Ok(())
    }

    pub fn create_table(
        &mut self,
        table_ref: TableReference,
        schema: SchemaRef,
    ) -> QuillSQLResult<Arc<TableHeap>> {
        let catalog_name = table_ref
            .catalog()
            .unwrap_or(DEFAULT_CATALOG_NAME)
            .to_string();
        let catalog_schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        let table_name = table_ref.table().to_string();

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
        let table_heap = Arc::new(TableHeap::try_new(
            schema.clone(),
            self.buffer_pool.clone(),
        )?);
        let catalog_table = CatalogTable {
            name: table_name.clone(),
            table: table_heap.clone(),
            indexes: HashMap::new(),
            stats: None,
        };
        catalog_schema
            .tables
            .insert(table_name.clone(), catalog_table);
        self.table_registry
            .register(table_ref.clone(), table_heap.clone());

        // update system table
        let Some(information_schema) = self.schemas.get_mut(INFORMATION_SCHEMA_NAME) else {
            return Err(QuillSQLError::Internal(
                "catalog schema information_schema not created yet".to_string(),
            ));
        };
        let Some(tables_table) = information_schema.tables.get_mut(INFORMATION_SCHEMA_TABLES)
        else {
            return Err(QuillSQLError::Internal(
                "table information_schema.tables not created yet".to_string(),
            ));
        };

        let tuple = Tuple::new(
            TABLES_SCHEMA.clone(),
            vec![
                catalog_name.clone().into(),
                catalog_schema_name.clone().into(),
                table_name.clone().into(),
                (table_heap.first_page_id.load(Ordering::SeqCst)).into(),
            ],
        );
        tables_table.table.insert_tuple(&EMPTY_TUPLE_META, &tuple)?;

        let Some(columns_table) = information_schema
            .tables
            .get_mut(INFORMATION_SCHEMA_COLUMNS)
        else {
            return Err(QuillSQLError::Internal(
                "table information_schema.columns not created yet".to_string(),
            ));
        };
        for col in schema.columns.iter() {
            let sql_type: sqlparser::ast::DataType = (&col.data_type).into();
            let tuple = Tuple::new(
                COLUMNS_SCHEMA.clone(),
                vec![
                    catalog_name.clone().into(),
                    catalog_schema_name.clone().into(),
                    table_name.clone().into(),
                    col.name.clone().into(),
                    format!("{sql_type}").into(),
                    col.nullable.into(),
                    format!("{}", col.default).into(),
                ],
            );
            columns_table
                .table
                .insert_tuple(&EMPTY_TUPLE_META, &tuple)?;
        }

        Ok(table_heap)
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

        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Ok(false);
        };

        let Some(catalog_table) = schema.tables.remove(&table_name) else {
            return Ok(false);
        };

        self.table_registry.unregister(table_ref);

        for index_name in catalog_table.indexes.keys() {
            self.remove_index_metadata(&catalog_name, &schema_name, &table_name, index_name)?;
        }

        self.remove_table_metadata(&catalog_name, &schema_name, &table_name)?;
        Ok(true)
    }

    pub fn table_heap(&self, table_ref: &TableReference) -> QuillSQLResult<Arc<TableHeap>> {
        let catalog_schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        let table_name = table_ref.table().to_string();

        let Some(catalog_schema) = self.schemas.get(&catalog_schema_name) else {
            return Err(QuillSQLError::Storage(format!(
                "catalog schema {} not created yet",
                catalog_schema_name
            )));
        };
        let Some(catalog_table) = catalog_schema.tables.get(&table_name) else {
            return Err(QuillSQLError::Storage(format!(
                "table {} not created yet",
                table_name
            )));
        };
        Ok(catalog_table.table.clone())
    }

    pub fn table_statistics(&self, table_ref: &TableReference) -> Option<&TableStatistics> {
        let catalog_schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        self.schemas
            .get(&catalog_schema_name)
            .and_then(|schema| schema.tables.get(table_ref.table()))
            .and_then(|table| table.stats.as_ref())
    }

    pub fn analyze_table(&mut self, table_ref: &TableReference) -> QuillSQLResult<TableStatistics> {
        let catalog_schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        let table_name = table_ref.table().to_string();

        let Some(catalog_schema) = self.schemas.get_mut(&catalog_schema_name) else {
            return Err(QuillSQLError::Storage(format!(
                "catalog schema {} not created yet",
                catalog_schema_name
            )));
        };
        let Some(catalog_table) = catalog_schema.tables.get_mut(&table_name) else {
            return Err(QuillSQLError::Storage(format!(
                "table {} not created yet",
                table_name
            )));
        };

        let stats = TableStatistics::analyze(catalog_table.table.clone())?;
        catalog_table.stats = Some(stats.clone());
        Ok(stats)
    }

    pub fn try_table_heap(&self, table_ref: &TableReference) -> Option<Arc<TableHeap>> {
        let schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        self.schemas
            .get(&schema_name)
            .and_then(|schema| schema.tables.get(table_ref.table()))
            .map(|catalog_table| catalog_table.table.clone())
    }

    pub fn table_indexes(
        &self,
        table_ref: &TableReference,
    ) -> QuillSQLResult<Vec<(String, Arc<BPlusTreeIndex>)>> {
        let catalog_schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        let table_name = table_ref.table().to_string();

        let Some(catalog_schema) = self.schemas.get(&catalog_schema_name) else {
            return Err(QuillSQLError::Storage(format!(
                "catalog schema {} not created yet",
                catalog_schema_name
            )));
        };
        let Some(catalog_table) = catalog_schema.tables.get(&table_name) else {
            return Err(QuillSQLError::Storage(format!(
                "table {} not created yet",
                table_name
            )));
        };
        Ok(catalog_table
            .indexes
            .iter()
            .map(|(name, index)| (name.clone(), index.clone()))
            .collect())
    }

    pub fn create_index(
        &mut self,
        index_name: String,
        table_ref: &TableReference,
        key_schema: SchemaRef,
    ) -> QuillSQLResult<Arc<BPlusTreeIndex>> {
        let catalog_name = table_ref
            .catalog()
            .unwrap_or(DEFAULT_CATALOG_NAME)
            .to_string();
        let catalog_schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        let table_name = table_ref.table().to_string();

        let Some(catalog_schema) = self.schemas.get_mut(&catalog_schema_name) else {
            return Err(QuillSQLError::Storage(format!(
                "catalog schema {} not created yet",
                catalog_schema_name
            )));
        };
        let Some(catalog_table) = catalog_schema.tables.get_mut(&table_name) else {
            return Err(QuillSQLError::Storage(format!(
                "table {} not created yet",
                table_name
            )));
        };
        if catalog_table.indexes.contains_key(&index_name) {
            return Err(QuillSQLError::Storage(
                "Cannot create duplicated index".to_string(),
            ));
        }

        let b_plus_tree_index = Arc::new(BPlusTreeIndex::new(
            key_schema.clone(),
            self.buffer_pool.clone(),
            BPLUS_INTERNAL_PAGE_MAX_SIZE as u32,
            BPLUS_LEAF_PAGE_MAX_SIZE as u32,
        ));
        catalog_table
            .indexes
            .insert(index_name.clone(), b_plus_tree_index.clone());

        // update system table
        let Some(information_schema) = self.schemas.get_mut(INFORMATION_SCHEMA_NAME) else {
            return Err(QuillSQLError::Internal(
                "catalog schema information_schema not created yet".to_string(),
            ));
        };
        let Some(indexes_table) = information_schema
            .tables
            .get_mut(INFORMATION_SCHEMA_INDEXES)
        else {
            return Err(QuillSQLError::Internal(
                "table information_schema.indexes not created yet".to_string(),
            ));
        };

        let tuple = Tuple::new(
            INDEXES_SCHEMA.clone(),
            vec![
                catalog_name.clone().into(),
                catalog_schema_name.clone().into(),
                table_name.clone().into(),
                index_name.clone().into(),
                key_schema_to_varchar(&b_plus_tree_index.key_schema).into(),
                b_plus_tree_index.internal_max_size.into(),
                b_plus_tree_index.leaf_max_size.into(),
                b_plus_tree_index.header_page_id.into(),
            ],
        );
        indexes_table
            .table
            .insert_tuple(&EMPTY_TUPLE_META, &tuple)?;

        Ok(b_plus_tree_index)
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

        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Ok(false);
        };
        let Some(table) = schema.tables.get_mut(&table_name) else {
            return Ok(false);
        };

        if table.indexes.remove(index_name).is_none() {
            return Ok(false);
        }

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

    pub fn index(
        &self,
        table_ref: &TableReference,
        index_name: &str,
    ) -> QuillSQLResult<Option<Arc<BPlusTreeIndex>>> {
        let catalog_schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        let table_name = table_ref.table().to_string();

        let Some(catalog_schema) = self.schemas.get(&catalog_schema_name) else {
            return Err(QuillSQLError::Storage(format!(
                "catalog schema {} not created yet",
                catalog_schema_name
            )));
        };
        let Some(catalog_table) = catalog_schema.tables.get(&table_name) else {
            return Err(QuillSQLError::Storage(format!(
                "table {} not created yet",
                table_name
            )));
        };
        Ok(catalog_table.indexes.get(index_name).cloned())
    }

    pub fn load_schema(&mut self, name: impl Into<String>, schema: CatalogSchema) {
        self.schemas.insert(name.into(), schema);
    }

    pub fn load_table(
        &mut self,
        table_ref: TableReference,
        table: CatalogTable,
    ) -> QuillSQLResult<()> {
        let catalog_schema_name = table_ref.schema().unwrap_or(DEFAULT_SCHEMA_NAME);
        let table_name = table_ref.table().to_string();
        let Some(catalog_schema) = self.schemas.get_mut(catalog_schema_name) else {
            return Err(QuillSQLError::Storage(format!(
                "catalog schema {} not created yet",
                catalog_schema_name
            )));
        };
        self.table_registry
            .register(table_ref.clone(), table.table.clone());
        catalog_schema.tables.insert(table_name, table);
        Ok(())
    }

    pub fn load_index(
        &mut self,
        table_ref: TableReference,
        index_name: impl Into<String>,
        index: Arc<BPlusTreeIndex>,
    ) -> QuillSQLResult<()> {
        let catalog_schema_name = table_ref.schema().unwrap_or(DEFAULT_SCHEMA_NAME);
        let table_name = table_ref.table().to_string();
        let Some(catalog_schema) = self.schemas.get_mut(catalog_schema_name) else {
            return Err(QuillSQLError::Storage(format!(
                "catalog schema {} not created yet",
                catalog_schema_name
            )));
        };
        let Some(catalog_table) = catalog_schema.tables.get_mut(&table_name) else {
            return Err(QuillSQLError::Storage(format!(
                "catalog table {} not created yet",
                table_name
            )));
        };
        let idx_name: String = index_name.into();
        catalog_table.indexes.insert(idx_name.clone(), index);
        Ok(())
    }

    fn find_index_in_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
        index_name: &str,
    ) -> Option<TableReference> {
        if schema_name == INFORMATION_SCHEMA_NAME {
            return None;
        }
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

    fn remove_table_metadata(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> QuillSQLResult<()> {
        let (tables_heap, columns_heap, indexes_heap) = {
            let information_schema =
                self.schemas.get(INFORMATION_SCHEMA_NAME).ok_or_else(|| {
                    QuillSQLError::Internal(
                        "catalog schema information_schema not created yet".to_string(),
                    )
                })?;
            let tables_heap = information_schema
                .tables
                .get(INFORMATION_SCHEMA_TABLES)
                .ok_or_else(|| {
                    QuillSQLError::Internal(
                        "table information_schema.tables not created yet".to_string(),
                    )
                })?
                .table
                .clone();
            let columns_heap = information_schema
                .tables
                .get(INFORMATION_SCHEMA_COLUMNS)
                .ok_or_else(|| {
                    QuillSQLError::Internal(
                        "table information_schema.columns not created yet".to_string(),
                    )
                })?
                .table
                .clone();
            let indexes_heap = information_schema
                .tables
                .get(INFORMATION_SCHEMA_INDEXES)
                .ok_or_else(|| {
                    QuillSQLError::Internal(
                        "table information_schema.indexes not created yet".to_string(),
                    )
                })?
                .table
                .clone();
            (tables_heap, columns_heap, indexes_heap)
        };

        Self::delete_matching_rows(tables_heap, |tuple| {
            let ScalarValue::Varchar(Some(catalog)) = tuple.value(0)? else {
                return Ok(false);
            };
            let ScalarValue::Varchar(Some(schema)) = tuple.value(1)? else {
                return Ok(false);
            };
            let ScalarValue::Varchar(Some(table)) = tuple.value(2)? else {
                return Ok(false);
            };
            Ok(catalog == catalog_name && schema == schema_name && table == table_name)
        })?;

        Self::delete_matching_rows(columns_heap, |tuple| {
            let ScalarValue::Varchar(Some(catalog)) = tuple.value(0)? else {
                return Ok(false);
            };
            let ScalarValue::Varchar(Some(schema)) = tuple.value(1)? else {
                return Ok(false);
            };
            let ScalarValue::Varchar(Some(table)) = tuple.value(2)? else {
                return Ok(false);
            };
            Ok(catalog == catalog_name && schema == schema_name && table == table_name)
        })?;

        Self::delete_matching_rows(indexes_heap, |tuple| {
            let ScalarValue::Varchar(Some(catalog)) = tuple.value(0)? else {
                return Ok(false);
            };
            let ScalarValue::Varchar(Some(schema)) = tuple.value(1)? else {
                return Ok(false);
            };
            let ScalarValue::Varchar(Some(table)) = tuple.value(2)? else {
                return Ok(false);
            };
            Ok(catalog == catalog_name && schema == schema_name && table == table_name)
        })?;

        Ok(())
    }

    fn remove_index_metadata(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        index_name: &str,
    ) -> QuillSQLResult<()> {
        let indexes_heap = {
            let information_schema =
                self.schemas.get(INFORMATION_SCHEMA_NAME).ok_or_else(|| {
                    QuillSQLError::Internal(
                        "catalog schema information_schema not created yet".to_string(),
                    )
                })?;
            information_schema
                .tables
                .get(INFORMATION_SCHEMA_INDEXES)
                .ok_or_else(|| {
                    QuillSQLError::Internal(
                        "table information_schema.indexes not created yet".to_string(),
                    )
                })?
                .table
                .clone()
        };

        Self::delete_matching_rows(indexes_heap, |tuple| {
            let ScalarValue::Varchar(Some(catalog)) = tuple.value(0)? else {
                return Ok(false);
            };
            let ScalarValue::Varchar(Some(schema)) = tuple.value(1)? else {
                return Ok(false);
            };
            let ScalarValue::Varchar(Some(table)) = tuple.value(2)? else {
                return Ok(false);
            };
            let ScalarValue::Varchar(Some(index)) = tuple.value(3)? else {
                return Ok(false);
            };
            Ok(catalog == catalog_name
                && schema == schema_name
                && table == table_name
                && index == index_name)
        })?;

        Ok(())
    }

    fn delete_matching_rows<F>(heap: Arc<TableHeap>, mut predicate: F) -> QuillSQLResult<()>
    where
        F: FnMut(&Tuple) -> QuillSQLResult<bool>,
    {
        let mut iterator = TableIterator::new(heap.clone(), ..);
        while let Some((rid, _meta, tuple)) = iterator.next()? {
            if predicate(&tuple)? {
                heap.delete_tuple(rid, SYSTEM_TXN_ID, SYSTEM_COMMAND_ID, None)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::utils::table_ref::TableReference;
    use crate::{
        catalog::{Column, DataType, Schema},
        database::Database,
    };

    #[test]
    pub fn test_catalog_create_table() {
        let mut db = Database::new_temp().unwrap();

        let table_ref1 = TableReference::Bare {
            table: "test_table1".to_string(),
        };
        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, true),
            Column::new("b", DataType::Int16, true),
            Column::new("c", DataType::Int32, true),
        ]));
        let table_info = db
            .catalog
            .create_table(table_ref1.clone(), schema.clone())
            .unwrap();
        assert_eq!(table_info.schema, schema);

        let table_ref2 = TableReference::Bare {
            table: "test_table2".to_string(),
        };
        let schema = Arc::new(Schema::new(vec![
            Column::new("d", DataType::Int32, true),
            Column::new("e", DataType::Int16, true),
            Column::new("f", DataType::Int8, true),
        ]));
        let table_info = db
            .catalog
            .create_table(table_ref2.clone(), schema.clone())
            .unwrap();
        assert_eq!(table_info.schema, schema);

        let table_info = db.catalog.table_heap(&table_ref1).unwrap();
        assert_eq!(table_info.schema.column_count(), 3);

        let table_info = db.catalog.table_heap(&table_ref2).unwrap();
        assert_eq!(table_info.schema.column_count(), 3);
    }

    #[test]
    pub fn test_catalog_create_index() {
        let mut db = Database::new_temp().unwrap();

        let table_ref = TableReference::Bare {
            table: "test_table1".to_string(),
        };
        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, true),
            Column::new("b", DataType::Int16, true),
            Column::new("c", DataType::Int32, true),
        ]));
        let _ = db.catalog.create_table(table_ref.clone(), schema.clone());

        let index_name1 = "test_index1".to_string();
        let key_schema1 = Arc::new(schema.project(&[0, 2]).unwrap());
        let index1 = db
            .catalog
            .create_index(index_name1.clone(), &table_ref, key_schema1.clone())
            .unwrap();
        assert_eq!(index1.key_schema, key_schema1);

        let index_name2 = "test_index2".to_string();
        let key_schema2 = Arc::new(schema.project(&[1]).unwrap());
        let index2 = db
            .catalog
            .create_index(index_name2.clone(), &table_ref, key_schema2.clone())
            .unwrap();
        assert_eq!(index2.key_schema, key_schema2);

        let index3 = db
            .catalog
            .index(&table_ref, index_name1.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(index3.key_schema, key_schema1);
    }

    #[test]
    fn analyze_table_collects_basic_stats() {
        let mut db = Database::new_temp().unwrap();
        db.run("create table t_stats (a int, b int)").unwrap();
        db.run("insert into t_stats values (1, 10), (null, 20), (2, null)")
            .unwrap();

        let table_ref = TableReference::Bare {
            table: "t_stats".to_string(),
        };
        let stats = db.catalog.analyze_table(&table_ref).unwrap();
        assert_eq!(stats.row_count, 3);
        let col_a = stats.column_stats.get("a").unwrap();
        assert_eq!(col_a.null_count, 1);
        assert_eq!(col_a.non_null_count, 2);
        let stored = db.catalog.table_statistics(&table_ref).unwrap();
        assert_eq!(stored.row_count, 3);
    }
}
