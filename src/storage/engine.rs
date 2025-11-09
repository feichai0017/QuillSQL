use std::sync::Arc;

use crate::{
    catalog::Catalog,
    error::QuillSQLResult,
    storage::{index::btree_index::BPlusTreeIndex, table_heap::TableHeap},
    utils::table_ref::TableReference,
};

pub trait StorageEngine: Send + Sync {
    fn table_heap(
        &self,
        catalog: &Catalog,
        table: &TableReference,
    ) -> QuillSQLResult<Arc<TableHeap>>;

    fn table_indexes(
        &self,
        catalog: &Catalog,
        table: &TableReference,
    ) -> QuillSQLResult<Vec<Arc<BPlusTreeIndex>>>;
}

#[derive(Default)]
pub struct DefaultStorageEngine;

impl StorageEngine for DefaultStorageEngine {
    fn table_heap(
        &self,
        catalog: &Catalog,
        table: &TableReference,
    ) -> QuillSQLResult<Arc<TableHeap>> {
        catalog.table_heap(table)
    }

    fn table_indexes(
        &self,
        catalog: &Catalog,
        table: &TableReference,
    ) -> QuillSQLResult<Vec<Arc<BPlusTreeIndex>>> {
        catalog.table_indexes(table)
    }
}
