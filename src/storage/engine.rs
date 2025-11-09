use std::sync::Arc;

use crate::{
    catalog::{Catalog, SchemaRef},
    error::QuillSQLResult,
    storage::{index::btree_index::BPlusTreeIndex, table_heap::TableHeap},
    utils::table_ref::TableReference,
};

pub trait TableHandle: Send + Sync {
    fn table_ref(&self) -> &TableReference;
    fn schema(&self) -> SchemaRef;
    fn table_heap(&self) -> Arc<TableHeap>;
}

pub trait IndexHandle: Send + Sync {
    fn name(&self) -> &str;
    fn key_schema(&self) -> SchemaRef;
    fn index(&self) -> Arc<BPlusTreeIndex>;
}

pub trait StorageEngine: Send + Sync {
    fn table(
        &self,
        catalog: &Catalog,
        table: &TableReference,
    ) -> QuillSQLResult<Arc<dyn TableHandle>>;

    fn indexes(
        &self,
        catalog: &Catalog,
        table: &TableReference,
    ) -> QuillSQLResult<Vec<Arc<dyn IndexHandle>>>;
}

#[derive(Default)]
pub struct DefaultStorageEngine;

struct HeapTableHandle {
    table_ref: TableReference,
    heap: Arc<TableHeap>,
}

impl HeapTableHandle {
    fn new(table_ref: TableReference, heap: Arc<TableHeap>) -> Self {
        Self { table_ref, heap }
    }
}

impl TableHandle for HeapTableHandle {
    fn table_ref(&self) -> &TableReference {
        &self.table_ref
    }

    fn schema(&self) -> SchemaRef {
        self.heap.schema.clone()
    }

    fn table_heap(&self) -> Arc<TableHeap> {
        self.heap.clone()
    }
}

struct BTreeIndexHandle {
    name: String,
    index: Arc<BPlusTreeIndex>,
}

impl BTreeIndexHandle {
    fn new(name: String, index: Arc<BPlusTreeIndex>) -> Self {
        Self { name, index }
    }
}

impl IndexHandle for BTreeIndexHandle {
    fn name(&self) -> &str {
        &self.name
    }

    fn key_schema(&self) -> SchemaRef {
        self.index.key_schema.clone()
    }

    fn index(&self) -> Arc<BPlusTreeIndex> {
        self.index.clone()
    }
}

impl StorageEngine for DefaultStorageEngine {
    fn table(
        &self,
        catalog: &Catalog,
        table: &TableReference,
    ) -> QuillSQLResult<Arc<dyn TableHandle>> {
        let heap = catalog.table_heap(table)?;
        Ok(Arc::new(HeapTableHandle::new(table.clone(), heap)))
    }

    fn indexes(
        &self,
        catalog: &Catalog,
        table: &TableReference,
    ) -> QuillSQLResult<Vec<Arc<dyn IndexHandle>>> {
        let indexes = catalog.table_indexes(table)?;
        Ok(indexes
            .into_iter()
            .map(|(name, index)| {
                Arc::new(BTreeIndexHandle::new(name, index)) as Arc<dyn IndexHandle>
            })
            .collect())
    }
}
