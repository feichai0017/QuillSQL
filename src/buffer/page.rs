use crate::buffer::buffer_pool::FrameId;
use crate::utils::cache::lru_k::LRUKReplacer;
use crate::utils::cache::Replacer;
use dashmap::DashMap;
use derive_with::With;
use log::error;
use std::ops::Deref;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

pub type PageId = u32;
pub type AtomicPageId = AtomicU32;

pub const INVALID_PAGE_ID: PageId = 0;
pub const PAGE_SIZE: usize = 4096;

#[derive(Debug, With)]
pub struct Page {
    pub page_id: PageId,
    data: [u8; PAGE_SIZE],
    // reference count number
    pub pin_count: AtomicU32,
    // whether it has been written
    pub is_dirty: bool,
}

impl Page {
    pub fn empty() -> Self {
        Self::new(INVALID_PAGE_ID)
    }
    pub fn new(page_id: PageId) -> Self {
        Self {
            page_id,
            data: [0; PAGE_SIZE],
            pin_count: AtomicU32::new(0),
            is_dirty: false,
        }
    }
    pub fn destroy(&mut self) {
        self.page_id = 0;
        self.data = [0; PAGE_SIZE];
        self.pin_count.store(0, std::sync::atomic::Ordering::SeqCst);
        self.is_dirty = false;
    }

    pub fn set_data(&mut self, data: [u8; PAGE_SIZE]) {
        self.data = data;
        self.is_dirty = true;
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn replace(&mut self, other: Page) {
        self.page_id = other.page_id;
        self.data = other.data;
        self.pin_count = other.pin_count;
        self.is_dirty = other.is_dirty;
    }
}

#[derive(Clone, Debug)]
pub struct PageRef {
    pub page: Arc<RwLock<Page>>,
    pub page_table: Arc<DashMap<PageId, FrameId>>,
    pub replacer: Arc<RwLock<LRUKReplacer>>,
}

impl Deref for PageRef {
    type Target = Arc<RwLock<Page>>;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl Drop for PageRef {
    fn drop(&mut self) {
        let page_guard = self.page.read().unwrap();
        let page_id = page_guard.page_id;

        // 使用原子减法，并检查操作前的值
        if page_guard.pin_count.fetch_sub(1, Ordering::SeqCst) == 1 {
            // 如果减之前是 1，说明现在是 0 了
            if let Some(frame_id) = self.page_table.get(&page_id) {
                if let Err(e) = self
                    .replacer
                    .write()
                    .unwrap()
                    .set_evictable(*frame_id, true)
                {
                    // 在 drop 中 panic 是不好的，最好是 log an error
                    error!(
                        "Failed to set evictable to frame {}, err: {:?}",
                        *frame_id, e
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::{Page, PageRef};
    use crate::utils::cache::lru_k::LRUKReplacer;
    use dashmap::DashMap;
    use std::sync::{Arc, RwLock};

    #[test]
    fn page_ref() {
        let page = Arc::new(RwLock::new(Page::new(1)));
        let page_table = Arc::new(DashMap::new());
        let replacer = Arc::new(RwLock::new(LRUKReplacer::with_k(10, 2)));

        let page_ref = PageRef {
            page: page.clone(),
            page_table,
            replacer,
        };
        assert_eq!(Arc::strong_count(&page), 2);
        assert_eq!(page_ref.read().unwrap().page_id, 1);
        drop(page_ref);
        assert_eq!(Arc::strong_count(&page), 1);
    }
}
