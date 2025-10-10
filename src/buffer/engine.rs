use crate::buffer::PageId;
use crate::error::QuillSQLResult;
use crate::recovery::{Lsn, WalManager};
use std::any::Any;
use std::sync::Arc;

/// Trait that abstracts the read access surface of a buffered page guard.
pub trait BufferReadGuard {
    fn page_id(&self) -> PageId;
    fn data(&self) -> &[u8];
    fn lsn(&self) -> Lsn;
    fn is_dirty(&self) -> bool;
    fn pin_count(&self) -> u32;
}

/// Trait that abstracts the write access surface of a buffered page guard.
pub trait BufferWriteGuard: BufferReadGuard {
    fn data_mut(&mut self) -> &mut [u8];
    fn mark_dirty(&mut self);
    fn set_lsn(&mut self, lsn: Lsn);
    fn overwrite(&mut self, data: &[u8], new_lsn: Option<Lsn>);
    fn first_dirty_lsn(&self) -> Option<Lsn> {
        None
    }
}

/// Boxed read guard alias used across the crate.
pub type ReadGuardRef = Box<dyn BufferReadGuard>;

/// Boxed write guard alias used across the crate.
pub type WriteGuardRef = Box<dyn BufferWriteGuard>;

/// Trait that captures the buffer manager surface consumed by the rest of the system.
pub trait BufferEngine: Send + Sync + Sized + 'static {
    fn new_page(self: &Arc<Self>) -> QuillSQLResult<WriteGuardRef>;
    fn fetch_page_read(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<ReadGuardRef>;
    fn fetch_page_write(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<WriteGuardRef>;
    fn prefetch_page(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<()>;
    fn flush_page(&self, page_id: PageId) -> QuillSQLResult<bool>;
    fn flush_all_pages(&self) -> QuillSQLResult<()>;
    fn delete_page(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<bool>;
    fn dirty_page_ids(&self) -> Vec<PageId>;
    fn dirty_page_table_snapshot(&self) -> Vec<(PageId, Lsn)>;
    fn set_wal_manager(&self, wal_manager: Arc<WalManager>);
    fn wal_manager(&self) -> Option<Arc<WalManager>>;
    fn as_any(&self) -> &dyn Any;
}
