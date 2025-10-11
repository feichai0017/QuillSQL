use std::sync::Arc;

use crate::buffer::PageId;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::disk_scheduler::DiskScheduler;

#[derive(Debug)]
pub struct SegmentAllocator {
    scheduler: Arc<DiskScheduler>,
    segment_size: u32,
}

impl SegmentAllocator {
    pub fn new(scheduler: Arc<DiskScheduler>, segment_size: u32) -> Self {
        Self {
            scheduler,
            segment_size: segment_size.max(1),
        }
    }

    pub fn allocate_page(&self) -> QuillSQLResult<PageId> {
        let rx = self.scheduler.schedule_allocate()?;
        let page_id = rx
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("Channel disconnected: {}", e)))??;
        let _ = self.segment_size;
        Ok(page_id)
    }

    pub fn deallocate_page(&self, page_id: PageId) -> QuillSQLResult<()> {
        self.scheduler
            .schedule_deallocate(page_id)?
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("Channel disconnected: {}", e)))??;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn scheduler(&self) -> Arc<DiskScheduler> {
        self.scheduler.clone()
    }

    #[allow(dead_code)]
    pub fn segment_size(&self) -> u32 {
        self.segment_size
    }
}
