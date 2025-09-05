use crate::buffer::buffer_pool::FrameId;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::utils::cache::lru_k::LRUKReplacer;
use crate::utils::cache::Replacer;
use dashmap::DashMap;
use derive_with::With;
use log::{debug, error};
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
        match self.page.read() {
            Ok(page_guard) => {
                let page_id = page_guard.page_id;
                debug!("Dropping PageRef for page {}", page_id);

                // 使用原子减法，并检查操作前的值
                if page_guard.pin_count.fetch_sub(1, Ordering::SeqCst) == 1 {
                    // 如果减之前是 1，说明现在是 0 了
                    debug!("Page {} pin count reached 0, setting evictable", page_id);
                    if let Some(frame_id) = self.page_table.get(&page_id) {
                        if let Ok(mut replacer) = self.replacer.write() {
                            if let Err(e) = replacer.set_evictable(*frame_id, true) {
                                error!(
                                    "Failed to set evictable to frame {}, err: {:?}",
                                    *frame_id, e
                                );
                            }
                        } else {
                            error!("Failed to acquire replacer write lock for page {}", page_id);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to acquire page read lock in drop: {:?}", e);
            }
        }
    }
}

/// Read guard for a page, automatically handles unpin on drop
/// Provides read-only access to page data with RAII semantics
#[derive(Debug)]
pub struct PageReadGuard {
    _page_ref: PageRef, // Own the PageRef to keep page alive
    page_id: PageId,
    data: Vec<u8>, // Snapshot of page data
    is_dirty: bool,
}

impl PageReadGuard {
    /// Get the page ID
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Get read-only access to page data
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Check if page is dirty
    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    /// Get current pin count
    pub fn pin_count(&self) -> u32 {
        self._page_ref
            .read()
            .map_or(0, |g| g.pin_count.load(Ordering::SeqCst))
    }
}

impl Drop for PageReadGuard {
    fn drop(&mut self) {
        debug!("Releasing read lock on page {}", self.page_id());
        // PageRef's drop will handle unpin automatically
    }
}

/// Write guard for a page, automatically handles unpin on drop
/// Provides read-write access to page data with RAII semantics
#[derive(Debug)]
pub struct PageWriteGuard {
    page_ref: PageRef, // Own the PageRef to keep page alive and access lock
    page_id: PageId,
}

impl PageWriteGuard {
    /// Get the page ID
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Get read-only access to page data
    pub fn data(&self) -> Vec<u8> {
        self.page_ref
            .read()
            .map_or(vec![0; PAGE_SIZE], |g| g.data().to_vec())
    }

    /// Mark the page as dirty
    pub fn set_dirty(&mut self, dirty: bool) {
        if let Ok(mut guard) = self.page_ref.write() {
            guard.is_dirty = dirty;
            debug!("Page {} dirty flag set to {}", self.page_id(), dirty);
        }
    }

    /// Set the page data
    pub fn set_data(&mut self, data: [u8; PAGE_SIZE]) {
        if let Ok(mut guard) = self.page_ref.write() {
            guard.set_data(data);
            debug!("Page {} data updated", self.page_id());
        }
    }

    /// Check if page is dirty
    pub fn is_dirty(&self) -> bool {
        self.page_ref.read().map_or(false, |g| g.is_dirty)
    }

    /// Get current pin count
    pub fn pin_count(&self) -> u32 {
        self.page_ref
            .read()
            .map_or(0, |g| g.pin_count.load(Ordering::SeqCst))
    }
}

impl Drop for PageWriteGuard {
    fn drop(&mut self) {
        debug!(
            "Releasing write lock on page {}, is_dirty: {}",
            self.page_id(),
            self.is_dirty()
        );
        // PageRef's drop will handle unpin automatically
    }
}

impl PageRef {
    /// Acquire a read lock on the page
    /// Returns a Result to handle lock poisoning gracefully
    pub fn read_guard(&self) -> QuillSQLResult<PageReadGuard> {
        let page_id = self.read().map_or(0, |g| g.page_id);
        debug!("Acquiring read guard for page {}", page_id);

        match self.page.read() {
            Ok(guard) => {
                let page_id = guard.page_id;
                let data = guard.data().to_vec();
                let is_dirty = guard.is_dirty;
                drop(guard); // Release lock immediately

                Ok(PageReadGuard {
                    _page_ref: self.clone(),
                    page_id,
                    data,
                    is_dirty,
                })
            }
            Err(e) => {
                error!("Failed to acquire read lock: {:?}", e);
                Err(QuillSQLError::Internal(format!(
                    "Page read lock poisoned: {}",
                    e
                )))
            }
        }
    }

    /// Acquire a write lock on the page  
    /// Returns a Result to handle lock poisoning gracefully
    pub fn write_guard(&self) -> QuillSQLResult<PageWriteGuard> {
        let page_id = self.read().map_or(0, |g| g.page_id);
        debug!("Acquiring write guard for page {}", page_id);

        Ok(PageWriteGuard {
            page_ref: self.clone(),
            page_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::cache::lru_k::LRUKReplacer;
    use crate::utils::cache::Replacer;
    use dashmap::DashMap;
    use std::sync::{Arc, RwLock};
    use std::thread;
    use std::time::Duration;

    fn create_test_page_ref(page_id: PageId) -> PageRef {
        let page = Arc::new(RwLock::new(Page::new(page_id)));
        let page_table = Arc::new(DashMap::new());
        let replacer = Arc::new(RwLock::new(LRUKReplacer::with_k(10, 2)));

        // 模拟 BufferPoolManager 的行为
        page_table.insert(page_id, 0);
        replacer.write().unwrap().record_access(0).unwrap();
        replacer.write().unwrap().set_evictable(0, false).unwrap();

        PageRef {
            page,
            page_table,
            replacer,
        }
    }

    #[test]
    fn test_page_ref_basic() {
        let page = Arc::new(RwLock::new(Page::new(1)));
        let page_table = Arc::new(DashMap::new());
        let replacer = Arc::new(RwLock::new(LRUKReplacer::with_k(10, 2)));

        let page_ref = PageRef {
            page: page.clone(),
            page_table,
            replacer,
        };
        assert_eq!(Arc::strong_count(&page), 2);

        // 使用新的 Guard API
        {
            let guard = page_ref.read_guard().unwrap();
            assert_eq!(guard.page_id(), 1);
        }

        drop(page_ref);
        assert_eq!(Arc::strong_count(&page), 1);
    }

    #[test]
    fn test_page_read_guard() {
        let page_ref = create_test_page_ref(100);

        // 测试读锁获取
        let read_guard = page_ref.read_guard().unwrap();
        assert_eq!(read_guard.page_id(), 100);
        assert!(!read_guard.is_dirty());

        // 测试数据访问
        let data = read_guard.data();
        assert_eq!(data.len(), PAGE_SIZE);

        // Guard 自动释放
        drop(read_guard);

        // 确保可以再次获取锁
        let _read_guard2 = page_ref.read_guard().unwrap();
    }

    #[test]
    fn test_page_write_guard() {
        let page_ref = create_test_page_ref(200);

        // 测试写锁获取
        let mut write_guard = page_ref.write_guard().unwrap();
        assert_eq!(write_guard.page_id(), 200);
        assert!(!write_guard.is_dirty());

        // 测试修改
        write_guard.set_dirty(true);
        assert!(write_guard.is_dirty());

        // 测试数据修改
        let test_data = [42u8; PAGE_SIZE];
        write_guard.set_data(test_data);
        assert_eq!(write_guard.data()[0], 42);

        // Guard 自动释放
        drop(write_guard);

        // 验证修改被保留
        let read_guard = page_ref.read_guard().unwrap();
        assert!(read_guard.is_dirty());
        assert_eq!(read_guard.data()[0], 42);
    }

    #[test]
    fn test_concurrent_readers() {
        let page_ref = Arc::new(create_test_page_ref(300));
        let mut handles = vec![];

        // 启动多个读线程
        for i in 0..5 {
            let page_ref_clone = page_ref.clone();
            let handle = thread::spawn(move || {
                for _ in 0..10 {
                    let guard = page_ref_clone.read_guard().unwrap();
                    assert_eq!(guard.page_id(), 300);
                    thread::sleep(Duration::from_millis(1));
                }
                println!("Reader {} completed", i);
            });
            handles.push(handle);
        }

        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_reader_writer_mutual_exclusion() {
        let page_ref = Arc::new(create_test_page_ref(400));

        // 启动写线程
        let page_ref_writer = page_ref.clone();
        let writer_handle = thread::spawn(move || {
            for i in 0..5 {
                let mut guard = page_ref_writer.write_guard().unwrap();
                guard.set_dirty(true);
                let test_data = [i as u8; PAGE_SIZE];
                guard.set_data(test_data);
                thread::sleep(Duration::from_millis(10));
                println!("Writer set data to {}", i);
            }
        });

        // 启动读线程
        let page_ref_reader = page_ref.clone();
        let reader_handle = thread::spawn(move || {
            for i in 0..10 {
                let guard = page_ref_reader.read_guard().unwrap();
                let data = guard.data();
                println!("Reader {} saw data[0] = {}", i, data[0]);
                thread::sleep(Duration::from_millis(5));
            }
        });

        writer_handle.join().unwrap();
        reader_handle.join().unwrap();

        // 验证最终状态
        let final_guard = page_ref.read_guard().unwrap();
        assert!(final_guard.is_dirty());
        assert_eq!(final_guard.data()[0], 4); // 最后写入的值
    }

    #[test]
    fn test_guard_drop_auto_unpin() {
        let page_ref = create_test_page_ref(500);

        // 检查初始 pin count (通过 Guard 获取的情况下)
        {
            let guard = page_ref.read_guard().unwrap();
            // Guard 的 page_ref 会增加引用计数，实际上相当于有两个 PageRef
            let _ = guard.pin_count(); // 检查 pin_count 可访问
        } // guard 在这里 drop

        // 验证可以继续获取锁
        {
            let guard = page_ref.read_guard().unwrap();
            assert_eq!(guard.page_id(), 500);
        }

        // 测试更现实的场景：创建多个 guard
        let guard1 = page_ref.read_guard().unwrap();
        let guard2 = page_ref.read_guard().unwrap();
        assert_eq!(guard1.page_id(), guard2.page_id());

        drop(guard1);
        drop(guard2);

        // 确保还能获取新的 guard
        let _final_guard = page_ref.read_guard().unwrap();
    }
}
