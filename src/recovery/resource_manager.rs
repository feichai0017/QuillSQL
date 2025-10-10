use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock, RwLock};

use dashmap::DashMap;

use crate::buffer::{BufferEngine, StandardBufferManager};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::wal::codec::{
    decode_page_delta, decode_page_write, ResourceManagerId, WalFrame,
};
use crate::recovery::Lsn;
use crate::storage::disk_scheduler::DiskScheduler;

#[derive(Clone)]
pub struct RedoContext<B: BufferEngine = StandardBufferManager> {
    pub disk_scheduler: Arc<DiskScheduler>,
    pub buffer_pool: Option<Arc<B>>,
}

#[derive(Clone)]
pub struct UndoContext<B: BufferEngine = StandardBufferManager> {
    pub disk_scheduler: Arc<DiskScheduler>,
    pub buffer_pool: Option<Arc<B>>,
}

pub trait ResourceManager<B: BufferEngine>: Send + Sync {
    fn redo(&self, frame: &WalFrame, ctx: &RedoContext<B>) -> QuillSQLResult<usize>;
    fn undo(&self, frame: &WalFrame, ctx: &UndoContext<B>) -> QuillSQLResult<()>;

    fn transaction_id(&self, _frame: &WalFrame) -> Option<u64> {
        None
    }
}

struct ResourceRegistry<B: BufferEngine> {
    managers: DashMap<ResourceManagerId, Arc<dyn ResourceManager<B>>>,
}

impl<B: BufferEngine> Default for ResourceRegistry<B> {
    fn default() -> Self {
        Self {
            managers: DashMap::new(),
        }
    }
}

impl<B: BufferEngine> ResourceRegistry<B> {
    fn register(&self, id: ResourceManagerId, manager: Arc<dyn ResourceManager<B>>) {
        if self.managers.contains_key(&id) {
            return;
        }
        self.managers.insert(id, manager);
    }

    fn get(&self, id: ResourceManagerId) -> Option<Arc<dyn ResourceManager<B>>> {
        self.managers.get(&id).map(|entry| entry.value().clone())
    }
}

struct RegistryStore {
    registries: HashMap<TypeId, Arc<dyn std::any::Any + Send + Sync>>,
}

impl RegistryStore {
    fn new() -> Self {
        Self {
            registries: HashMap::new(),
        }
    }
}

fn registry_store() -> &'static RwLock<RegistryStore> {
    static STORE: OnceLock<RwLock<RegistryStore>> = OnceLock::new();
    STORE.get_or_init(|| RwLock::new(RegistryStore::new()))
}

fn resource_registry<B: BufferEngine + 'static>() -> Arc<ResourceRegistry<B>> {
    let store = registry_store();
    {
        let guard = store.read().unwrap();
        if let Some(entry) = guard.registries.get(&TypeId::of::<B>()) {
            if let Ok(registry) = entry.clone().downcast::<ResourceRegistry<B>>() {
                return registry;
            }
        }
    }
    let mut guard = store.write().unwrap();
    guard
        .registries
        .entry(TypeId::of::<B>())
        .or_insert_with(|| Arc::new(ResourceRegistry::<B>::default()));
    guard
        .registries
        .get(&TypeId::of::<B>())
        .and_then(|entry| entry.clone().downcast::<ResourceRegistry<B>>().ok())
        .expect("resource registry initialization failed")
}

fn default_tracker() -> &'static RwLock<HashSet<TypeId>> {
    static TRACKER: OnceLock<RwLock<HashSet<TypeId>>> = OnceLock::new();
    TRACKER.get_or_init(|| RwLock::new(HashSet::new()))
}

pub fn register_resource_manager<B: BufferEngine + 'static>(
    id: ResourceManagerId,
    manager: Arc<dyn ResourceManager<B>>,
) {
    let registry = resource_registry::<B>();
    registry.register(id, manager);
}

pub fn get_resource_manager<B: BufferEngine + 'static>(
    id: ResourceManagerId,
) -> Option<Arc<dyn ResourceManager<B>>> {
    let registry = resource_registry::<B>();
    registry.get(id)
}

pub fn ensure_default_resource_managers_registered<B: BufferEngine + 'static>() {
    let tracker = default_tracker();
    {
        let guard = tracker.read().unwrap();
        if guard.contains(&TypeId::of::<B>()) {
            return;
        }
    }
    let mut guard = tracker.write().unwrap();
    if guard.insert(TypeId::of::<B>()) {
        register_resource_manager::<B>(
            ResourceManagerId::Page,
            Arc::new(PageResourceManager::default()),
        );
        crate::storage::heap_recovery::ensure_heap_resource_manager_registered::<B>();
    }
}

#[derive(Default)]
struct PageResourceManager;

impl PageResourceManager {
    fn page_requires_redo<B: BufferEngine>(
        &self,
        ctx: &RedoContext<B>,
        page_id: u32,
        record_lsn: Lsn,
    ) -> QuillSQLResult<bool> {
        if let Some(bpm) = &ctx.buffer_pool {
            match bpm.fetch_page_read(page_id) {
                Ok(guard) => Ok(guard.lsn() < record_lsn),
                Err(_) => Ok(true),
            }
        } else {
            Ok(true)
        }
    }

    fn redo_page_write(
        &self,
        ctx: &RedoContext<impl BufferEngine>,
        payload: crate::recovery::wal::codec::PageWritePayload,
    ) -> QuillSQLResult<()> {
        debug_assert_eq!(payload.page_image.len(), crate::buffer::PAGE_SIZE);
        let bytes = bytes::Bytes::from(payload.page_image);
        let rx = ctx.disk_scheduler.schedule_write(payload.page_id, bytes)?;
        rx.recv().map_err(|e| {
            QuillSQLError::Internal(format!("WAL recovery write recv failed: {}", e))
        })??;
        Ok(())
    }

    fn redo_page_delta(
        &self,
        ctx: &RedoContext<impl BufferEngine>,
        payload: crate::recovery::wal::codec::PageDeltaPayload,
    ) -> QuillSQLResult<()> {
        let rx = ctx.disk_scheduler.schedule_read(payload.page_id)?;
        let buf: bytes::BytesMut = rx.recv().map_err(|e| {
            QuillSQLError::Internal(format!("WAL recovery read recv failed: {}", e))
        })??;
        if buf.len() != crate::buffer::PAGE_SIZE {
            return Err(QuillSQLError::Internal(format!(
                "Unexpected page size {} while applying delta",
                buf.len()
            )));
        }
        let mut page_bytes = buf.to_vec();
        let start = payload.offset as usize;
        if start >= crate::buffer::PAGE_SIZE {
            return Err(QuillSQLError::Internal(format!(
                "PageDelta start out of bounds: offset={} page_size={}",
                start,
                crate::buffer::PAGE_SIZE
            )));
        }
        let end = match start.checked_add(payload.data.len()) {
            Some(e) if e <= crate::buffer::PAGE_SIZE => e,
            _ => {
                return Err(QuillSQLError::Internal(format!(
                    "PageDelta out of bounds: offset={} len={} page_size={}",
                    start,
                    payload.data.len(),
                    crate::buffer::PAGE_SIZE
                )))
            }
        };
        page_bytes[start..end].copy_from_slice(&payload.data);
        let rxw = ctx
            .disk_scheduler
            .schedule_write(payload.page_id, bytes::Bytes::from(page_bytes))?;
        rxw.recv().map_err(|e| {
            QuillSQLError::Internal(format!("WAL recovery write recv failed: {}", e))
        })??;
        Ok(())
    }
}

impl<B: BufferEngine> ResourceManager<B> for PageResourceManager {
    fn redo(&self, frame: &WalFrame, ctx: &RedoContext<B>) -> QuillSQLResult<usize> {
        match frame.info {
            0 => {
                let payload = decode_page_write(&frame.body)?;
                if !self.page_requires_redo(ctx, payload.page_id, frame.lsn)? {
                    return Ok(0);
                }
                self.redo_page_write(ctx, payload)?;
                Ok(1)
            }
            1 => {
                let payload = decode_page_delta(&frame.body)?;
                if !self.page_requires_redo(ctx, payload.page_id, frame.lsn)? {
                    return Ok(0);
                }
                self.redo_page_delta(ctx, payload)?;
                Ok(1)
            }
            other => Err(QuillSQLError::Internal(format!(
                "Unknown Page info kind: {}",
                other
            ))),
        }
    }

    fn undo(&self, _frame: &WalFrame, _ctx: &UndoContext<B>) -> QuillSQLResult<()> {
        Ok(())
    }
}
