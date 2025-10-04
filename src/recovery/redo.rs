use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::buffer::{BufferManager, PAGE_SIZE};
use crate::error::QuillSQLResult;
use crate::recovery::wal_record::{PageDeltaPayload, PageWritePayload, WalFrame, WalRecordPayload};
use crate::recovery::Lsn;
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
        Self {
            disk_scheduler,
            buffer_pool,
        }
    }

    pub fn apply(&self, frame: &WalFrame) -> QuillSQLResult<usize> {
        match &frame.payload {
            WalRecordPayload::PageWrite(payload) => {
                if !self.page_requires_redo(payload.page_id, frame.lsn)? {
                    return Ok(0);
                }
                self.redo_page_write(payload.clone())?;
                Ok(1)
            }
            WalRecordPayload::PageDelta(payload) => {
                if !self.page_requires_redo(payload.page_id, frame.lsn)? {
                    return Ok(0);
                }
                self.redo_page_delta(payload.clone())?;
                Ok(1)
            }
            _ => Ok(0),
        }
    }

    fn page_requires_redo(&self, page_id: u32, record_lsn: Lsn) -> QuillSQLResult<bool> {
        if let Some(bpm) = &self.buffer_pool {
            match bpm.fetch_page_read(page_id) {
                Ok(guard) => Ok(guard.lsn() < record_lsn),
                Err(_) => Ok(true),
            }
        } else {
            Ok(true)
        }
    }

    fn redo_page_write(&self, payload: PageWritePayload) -> QuillSQLResult<()> {
        debug_assert_eq!(payload.page_image.len(), PAGE_SIZE);
        let bytes = Bytes::from(payload.page_image);
        let rx = self.disk_scheduler.schedule_write(payload.page_id, bytes)?;
        rx.recv().map_err(|e| {
            crate::error::QuillSQLError::Internal(format!("WAL recovery write recv failed: {}", e))
        })??;
        Ok(())
    }

    fn redo_page_delta(&self, payload: PageDeltaPayload) -> QuillSQLResult<()> {
        // Read current page image
        let rx = self.disk_scheduler.schedule_read(payload.page_id)?;
        let buf: BytesMut = rx.recv().map_err(|e| {
            crate::error::QuillSQLError::Internal(format!("WAL recovery read recv failed: {}", e))
        })??;
        if buf.len() != PAGE_SIZE {
            return Err(crate::error::QuillSQLError::Internal(format!(
                "Unexpected page size {} while applying delta",
                buf.len()
            )));
        }
        let mut page_bytes = buf.to_vec();
        let start = payload.offset as usize;
        if start >= PAGE_SIZE {
            return Err(crate::error::QuillSQLError::Internal(format!(
                "PageDelta start out of bounds: offset={} page_size={}",
                start, PAGE_SIZE
            )));
        }
        let end = match start.checked_add(payload.data.len()) {
            Some(e) if e <= PAGE_SIZE => e,
            _ => {
                return Err(crate::error::QuillSQLError::Internal(format!(
                    "PageDelta out of bounds: offset={} len={} page_size={}",
                    start,
                    payload.data.len(),
                    PAGE_SIZE
                )))
            }
        };
        page_bytes[start..end].copy_from_slice(&payload.data);
        let rxw = self
            .disk_scheduler
            .schedule_write(payload.page_id, Bytes::from(page_bytes))?;
        rxw.recv().map_err(|e| {
            crate::error::QuillSQLError::Internal(format!("WAL recovery write recv failed: {}", e))
        })??;
        Ok(())
    }
}
