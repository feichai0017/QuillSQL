use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use once_cell::sync::Lazy;
use std::sync::OnceLock;

use crate::buffer::BufferManager;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::wal::codec::{
    decode_page_delta, decode_page_write, ResourceManagerId, WalFrame,
};
use crate::recovery::Lsn;
use crate::storage::disk_scheduler::DiskScheduler;

#[derive(Clone)]
pub struct RedoContext {
    pub disk_scheduler: Arc<DiskScheduler>,
    pub buffer_pool: Option<Arc<BufferManager>>,
}

#[derive(Clone)]
pub struct UndoContext {
    pub disk_scheduler: Arc<DiskScheduler>,
    pub buffer_pool: Option<Arc<BufferManager>>,
}

pub trait ResourceManager: Send + Sync {
    fn redo(&self, frame: &WalFrame, ctx: &RedoContext) -> QuillSQLResult<usize>;
    fn undo(&self, frame: &WalFrame, ctx: &UndoContext) -> QuillSQLResult<()>;

    fn transaction_id(&self, _frame: &WalFrame) -> Option<u64> {
        None
    }
}

static REGISTRY: Lazy<RwLock<HashMap<ResourceManagerId, Arc<dyn ResourceManager>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub fn register_resource_manager(id: ResourceManagerId, manager: Arc<dyn ResourceManager>) {
    let mut guard = REGISTRY
        .write()
        .expect("resource manager registry poisoned");
    guard.insert(id, manager);
}

pub fn get_resource_manager(id: ResourceManagerId) -> Option<Arc<dyn ResourceManager>> {
    let guard = REGISTRY.read().expect("resource manager registry poisoned");
    guard.get(&id).cloned()
}

#[derive(Default)]
struct PageResourceManager;

impl PageResourceManager {
    fn page_requires_redo(
        &self,
        ctx: &RedoContext,
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
        ctx: &RedoContext,
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
        ctx: &RedoContext,
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

impl ResourceManager for PageResourceManager {
    fn redo(&self, frame: &WalFrame, ctx: &RedoContext) -> QuillSQLResult<usize> {
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

    fn undo(&self, _frame: &WalFrame, _ctx: &UndoContext) -> QuillSQLResult<()> {
        Ok(())
    }
}

static DEFAULT_RESOURCE_MANAGERS: OnceLock<()> = OnceLock::new();

pub fn ensure_default_resource_managers_registered() {
    DEFAULT_RESOURCE_MANAGERS.get_or_init(|| {
        register_resource_manager(
            ResourceManagerId::Page,
            Arc::new(PageResourceManager::default()),
        );
        crate::storage::heap_recovery::ensure_heap_resource_manager_registered();
        crate::storage::index::index_recovery::ensure_index_resource_manager_registered();
    });
}
