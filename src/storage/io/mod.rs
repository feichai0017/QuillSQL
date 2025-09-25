use bytes::{Bytes, BytesMut};
use std::sync::mpsc::Receiver;

use crate::buffer::PageId;
use crate::error::QuillSQLResult;

pub type DiskCommandResultReceiver<T> = Receiver<QuillSQLResult<T>>;

/// IO backend abstraction for DiskScheduler.
/// Implementations are responsible for request routing and graceful shutdown.
pub trait IOBackend: Send + Sync {
    /// Schedule single-page read.
    fn schedule_read(&self, page_id: PageId)
        -> QuillSQLResult<DiskCommandResultReceiver<BytesMut>>;
    /// Schedule batch reads, preserving order of input page_ids in the output.
    fn schedule_read_pages(
        &self,
        page_ids: Vec<PageId>,
    ) -> QuillSQLResult<DiskCommandResultReceiver<Vec<BytesMut>>>;
    /// Schedule single-page write (exact PAGE_SIZE bytes).
    fn schedule_write(
        &self,
        page_id: PageId,
        data: Bytes,
    ) -> QuillSQLResult<DiskCommandResultReceiver<()>>;
    /// Allocate a new page id.
    fn schedule_allocate(&self) -> QuillSQLResult<DiskCommandResultReceiver<PageId>>;
    /// Deallocate a page id (zeroing on disk, then freelist push).
    fn schedule_deallocate(&self, page_id: PageId)
        -> QuillSQLResult<DiskCommandResultReceiver<()>>;
}

#[cfg_attr(not(target_os = "linux"), allow(unused))]
pub mod thread_pool;

#[cfg(target_os = "linux")]
pub mod iouring;
