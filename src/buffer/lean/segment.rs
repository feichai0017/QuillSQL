use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::buffer::PageId;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::disk_scheduler::DiskScheduler;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SegmentAllocationState {
    pub last_allocated_page: PageId,
}

#[derive(Debug)]
pub struct SegmentAllocator {
    scheduler: Arc<DiskScheduler>,
    segment_size: u32,
    state: Mutex<SegmentAllocationState>,
}

impl SegmentAllocator {
    #[allow(dead_code)]
    pub fn new(scheduler: Arc<DiskScheduler>, segment_size: u32) -> Self {
        Self::with_state(scheduler, segment_size, SegmentAllocationState::default())
    }

    pub fn with_state(
        scheduler: Arc<DiskScheduler>,
        segment_size: u32,
        state: SegmentAllocationState,
    ) -> Self {
        Self {
            scheduler,
            segment_size: segment_size.max(1),
            state: Mutex::new(state),
        }
    }

    pub fn allocate_page(&self) -> QuillSQLResult<PageId> {
        let rx = self.scheduler.schedule_allocate()?;
        let page_id = rx
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("Channel disconnected: {}", e)))??;
        {
            let mut state = self.state.lock().unwrap();
            if page_id > state.last_allocated_page {
                state.last_allocated_page = page_id;
            }
        }
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

    pub fn snapshot(&self) -> SegmentAllocationState {
        self.state.lock().unwrap().clone()
    }
}
