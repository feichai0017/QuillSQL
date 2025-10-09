use std::sync::Arc;

use dashmap::DashMap;
// Use a simple static with lazy_init to avoid adding new dependencies.
use std::sync::OnceLock;

use crate::storage::index::btree_index::BPlusTreeIndex;
use crate::storage::table_heap::TableHeap;
use crate::utils::table_ref::TableReference;

/// Global registry of indexes for background maintenance.
/// Maps (table_ref, index_name) -> (Arc<BPlusTreeIndex>, Arc<TableHeap>)
#[derive(Debug, Default)]
pub struct IndexRegistry {
    inner: DashMap<(TableReference, String), (Arc<BPlusTreeIndex>, Arc<TableHeap>)>,
}

impl IndexRegistry {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    pub fn register(
        &self,
        table: TableReference,
        name: String,
        index: Arc<BPlusTreeIndex>,
        table_heap: Arc<TableHeap>,
    ) {
        self.inner.insert((table, name), (index, table_heap));
    }

    pub fn unregister(&self, table: &TableReference, name: &str) {
        self.inner.remove(&(table.clone(), name.to_string()));
    }

    pub fn all(&self) -> Vec<(Arc<BPlusTreeIndex>, Arc<TableHeap>)> {
        self.inner.iter().map(|e| e.value().clone()).collect()
    }

    /// Non-allocating iterator over registered indexes.
    pub fn iter(&self) -> impl Iterator<Item = (Arc<BPlusTreeIndex>, Arc<TableHeap>)> + '_ {
        self.inner.iter().map(|e| e.value().clone())
    }
}

/// Global singleton accessor (for now). In a larger system we would plumb this through Database.
static REGISTRY: OnceLock<IndexRegistry> = OnceLock::new();

pub fn global_index_registry() -> &'static IndexRegistry {
    REGISTRY.get_or_init(IndexRegistry::new)
}

/// Global registry of table heaps that may require background maintenance.
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

static TABLE_REGISTRY: OnceLock<TableRegistry> = OnceLock::new();

pub fn global_table_registry() -> &'static TableRegistry {
    TABLE_REGISTRY.get_or_init(TableRegistry::new)
}
