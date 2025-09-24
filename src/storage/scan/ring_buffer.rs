use std::collections::VecDeque;
use std::sync::Arc;

use bytes::BytesMut;

use crate::buffer::{PageId, INVALID_PAGE_ID};
use crate::catalog::SchemaRef;
use crate::error::QuillSQLResult;
use crate::storage::codec::TablePageCodec;
use crate::storage::disk_scheduler::DiskScheduler;
use crate::storage::page::TablePage;

#[derive(Debug)]
pub struct DirectRingBuffer {
    pub readahead: usize,
    window: VecDeque<(PageId, BytesMut)>,
    pub next_pid: PageId,
    schema: SchemaRef,
    disk: Arc<DiskScheduler>,
}

impl DirectRingBuffer {
    pub fn new(schema: SchemaRef, disk: Arc<DiskScheduler>, readahead: usize) -> Self {
        Self {
            readahead,
            window: VecDeque::new(),
            next_pid: INVALID_PAGE_ID,
            schema,
            disk,
        }
    }

    pub fn prime(&mut self, start_pid: PageId) -> QuillSQLResult<()> {
        self.next_pid = start_pid;
        self.prefetch(self.readahead)
    }

    pub fn prefetch(&mut self, n: usize) -> QuillSQLResult<()> {
        let limit = n.min(self.readahead);
        while self.window.len() < limit && self.next_pid != INVALID_PAGE_ID {
            let rx = self.disk.schedule_read(self.next_pid)?;
            let data = rx.recv().map_err(|e| {
                crate::error::QuillSQLError::Internal(format!("Channel disconnected: {}", e))
            })??;
            // decode to learn the next pid
            let (page, _) = TablePageCodec::decode(&data, self.schema.clone())?;
            let cur = self.next_pid;
            self.next_pid = page.header.next_page_id;
            self.window.push_back((cur, data));
        }
        Ok(())
    }

    pub fn next_page(&mut self) -> QuillSQLResult<Option<(PageId, TablePage)>> {
        if let Some((pid, data)) = self.window.pop_front() {
            let (page, _) = TablePageCodec::decode(&data, self.schema.clone())?;
            // keep the window filled
            let _ = self.prefetch(1);
            return Ok(Some((pid, page)));
        }
        if self.next_pid == INVALID_PAGE_ID {
            return Ok(None);
        }
        // no window filled yet, read one
        let rx = self.disk.schedule_read(self.next_pid)?;
        let data = rx.recv().map_err(|e| {
            crate::error::QuillSQLError::Internal(format!("Channel disconnected: {}", e))
        })??;
        let (page, _) = TablePageCodec::decode(&data, self.schema.clone())?;
        let pid = self.next_pid;
        self.next_pid = page.header.next_page_id;
        Ok(Some((pid, page)))
    }
}
