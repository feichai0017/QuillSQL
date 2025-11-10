use std::sync::Arc;

use dashmap::DashMap;

use crate::storage::table_heap::TableHeap;
use crate::utils::table_ref::TableReference;

/// Registry of table heaps that may require background maintenance.
#[derive(Debug, Default)]
pub struct TableRegistry {
    inner: DashMap<TableReference, Arc<TableHeap>>,
}

impl TableRegistry {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    pub fn register(&self, table: TableReference, heap: Arc<TableHeap>) {
        self.inner.insert(table, heap);
    }

    pub fn unregister(&self, table: &TableReference) {
        self.inner.remove(table);
    }

    pub fn iter_tables(&self) -> impl Iterator<Item = (TableReference, Arc<TableHeap>)> + '_ {
        self.inner
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
    }
}
