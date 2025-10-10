use std::sync::Arc;

use crate::buffer::{BufferEngine, StandardBufferManager};
use crate::error::QuillSQLResult;
use crate::recovery::resource_manager::{
    ensure_default_resource_managers_registered, get_resource_manager, RedoContext,
};
use crate::recovery::wal::codec::WalFrame;
use crate::storage::disk_scheduler::DiskScheduler;

pub struct RedoExecutor<B: BufferEngine = StandardBufferManager> {
    disk_scheduler: Arc<DiskScheduler>,
    buffer_pool: Option<Arc<B>>,
}

impl<B: BufferEngine + 'static> RedoExecutor<B> {
    pub fn new(disk_scheduler: Arc<DiskScheduler>, buffer_pool: Option<Arc<B>>) -> Self {
        ensure_default_resource_managers_registered::<B>();
        Self {
            disk_scheduler,
            buffer_pool,
        }
    }

    pub fn apply(&self, frame: &WalFrame) -> QuillSQLResult<usize> {
        if let Some(manager) = get_resource_manager::<B>(frame.rmid) {
            let ctx = RedoContext {
                disk_scheduler: self.disk_scheduler.clone(),
                buffer_pool: self.buffer_pool.clone(),
            };
            manager.redo(frame, &ctx)
        } else {
            Ok(0)
        }
    }
}
