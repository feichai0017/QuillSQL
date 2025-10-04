use std::sync::Arc;

use crate::buffer::BufferManager;
use crate::error::QuillSQLResult;
use crate::recovery::resource_manager::{
    ensure_default_resource_managers_registered, get_resource_manager, RedoContext,
};
use crate::recovery::wal_record::WalFrame;
use crate::storage::disk_scheduler::DiskScheduler;

pub struct RedoExecutor {
    disk_scheduler: Arc<DiskScheduler>,
    buffer_pool: Option<Arc<BufferManager>>,
}

impl RedoExecutor {
    pub fn new(
        disk_scheduler: Arc<DiskScheduler>,
        buffer_pool: Option<Arc<BufferManager>>,
    ) -> Self {
        ensure_default_resource_managers_registered();
        Self {
            disk_scheduler,
            buffer_pool,
        }
    }

    pub fn apply(&self, frame: &WalFrame) -> QuillSQLResult<usize> {
        if let Some(manager) = get_resource_manager(frame.rmid) {
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
