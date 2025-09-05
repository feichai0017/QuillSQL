mod buffer_pool;
mod page;

pub use buffer_pool::{BufferPoolManager, FrameId, BUFFER_POOL_SIZE};
pub use page::{
    AtomicPageId, Page, PageId, PageRef, INVALID_PAGE_ID, PAGE_SIZE,
};
