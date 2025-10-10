use std::any::TypeId;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use dashmap::DashMap;

use crate::buffer::{BufferEngine, StandardBufferManager};
use crate::storage::index::btree_index::BPlusTreeIndex;
use crate::storage::table_heap::TableHeap;
use crate::utils::table_ref::TableReference;

#[derive(Debug, Default)]
pub struct IndexRegistry<B: BufferEngine = StandardBufferManager> {
    inner: DashMap<(TableReference, String), (Arc<BPlusTreeIndex<B>>, Arc<TableHeap<B>>)>,
}

impl<B: BufferEngine> IndexRegistry<B> {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    pub fn register(
        &self,
        table: TableReference,
        name: String,
        index: Arc<BPlusTreeIndex<B>>,
        table_heap: Arc<TableHeap<B>>,
    ) {
        self.inner.insert((table, name), (index, table_heap));
    }

    pub fn unregister(&self, table: &TableReference, name: &str) {
        self.inner.remove(&(table.clone(), name.to_string()));
    }

    pub fn all(&self) -> Vec<(Arc<BPlusTreeIndex<B>>, Arc<TableHeap<B>>)> {
        self.inner
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (Arc<BPlusTreeIndex<B>>, Arc<TableHeap<B>>)> + '_ {
        self.inner.iter().map(|entry| entry.value().clone())
    }
}

#[derive(Debug, Default)]
pub struct TableRegistry<B: BufferEngine = StandardBufferManager> {
    inner: DashMap<TableReference, Arc<TableHeap<B>>>,
}

impl<B: BufferEngine> TableRegistry<B> {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    pub fn register(&self, table: TableReference, heap: Arc<TableHeap<B>>) {
        self.inner.insert(table, heap);
    }

    pub fn unregister(&self, table: &TableReference) {
        self.inner.remove(table);
    }

    pub fn iter_tables(&self) -> impl Iterator<Item = (TableReference, Arc<TableHeap<B>>)> + '_ {
        self.inner
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
    }
}

struct RegistryStore {
    index: HashMap<TypeId, Arc<dyn std::any::Any + Send + Sync>>,
    table: HashMap<TypeId, Arc<dyn std::any::Any + Send + Sync>>,
}

impl RegistryStore {
    fn new() -> Self {
        Self {
            index: HashMap::new(),
            table: HashMap::new(),
        }
    }
}

fn registry_store() -> &'static RwLock<RegistryStore> {
    static STORE: OnceLock<RwLock<RegistryStore>> = OnceLock::new();
    STORE.get_or_init(|| RwLock::new(RegistryStore::new()))
}

pub fn global_index_registry<B: BufferEngine + 'static>() -> Arc<IndexRegistry<B>> {
    let store = registry_store();
    {
        let guard = store.read().unwrap();
        if let Some(entry) = guard.index.get(&TypeId::of::<B>()) {
            if let Ok(registry) = entry.clone().downcast::<IndexRegistry<B>>() {
                return registry;
            }
        }
    }
    let mut guard = store.write().unwrap();
    guard
        .index
        .entry(TypeId::of::<B>())
        .or_insert_with(|| Arc::new(IndexRegistry::<B>::new()));
    guard
        .index
        .get(&TypeId::of::<B>())
        .and_then(|entry| entry.clone().downcast::<IndexRegistry<B>>().ok())
        .expect("failed to initialize IndexRegistry")
}

pub fn global_table_registry<B: BufferEngine + 'static>() -> Arc<TableRegistry<B>> {
    let store = registry_store();
    {
        let guard = store.read().unwrap();
        if let Some(entry) = guard.table.get(&TypeId::of::<B>()) {
            if let Ok(registry) = entry.clone().downcast::<TableRegistry<B>>() {
                return registry;
            }
        }
    }
    let mut guard = store.write().unwrap();
    guard
        .table
        .entry(TypeId::of::<B>())
        .or_insert_with(|| Arc::new(TableRegistry::<B>::new()));
    guard
        .table
        .get(&TypeId::of::<B>())
        .and_then(|entry| entry.clone().downcast::<TableRegistry<B>>().ok())
        .expect("failed to initialize TableRegistry")
}
