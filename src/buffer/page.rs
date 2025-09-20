use crate::buffer::BufferPoolManager;
use derive_with::With;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::mem::{self, ManuallyDrop};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

pub type PageId = u32;
pub type AtomicPageId = AtomicU32;

pub const INVALID_PAGE_ID: PageId = 0;
pub const PAGE_SIZE: usize = 4096;

#[derive(Debug, With)]
pub struct Page {
    pub page_id: PageId,
    pub data: [u8; PAGE_SIZE],
    pub pin_count: AtomicU32, // 使用原子操作，避免锁竞争
    pub is_dirty: bool,
}

impl Page {
    pub fn empty() -> Self {
        Self {
            page_id: INVALID_PAGE_ID,
            data: [0; PAGE_SIZE],
            pin_count: AtomicU32::new(0), // set by caller with proper ordering
            is_dirty: false,
        }
    }

    pub fn new(page_id: PageId) -> Self {
        Self {
            page_id,
            data: [0; PAGE_SIZE],
            pin_count: AtomicU32::new(0),
            is_dirty: false,
        }
    }

    // Public getter for page_id
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    // Public getter for data
    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    // Public method to reset the page memory, used in delete_page.
    pub fn destroy(&mut self) {
        self.page_id = INVALID_PAGE_ID;
        self.data = [0; PAGE_SIZE];
        self.pin_count.store(0, Ordering::Relaxed);
        self.is_dirty = false;
    }

    // 原子操作：增加pin_count（Acquire 以发布可见性给随后读）
    pub fn pin(&self) -> u32 {
        self.pin_count.fetch_add(1, Ordering::AcqRel) + 1
    }

    // 原子操作：减少pin_count（Release 以与获取/驱逐建立次序）
    pub fn unpin(&self) -> u32 {
        self.pin_count.fetch_sub(1, Ordering::AcqRel)
    }

    // 原子操作：获取当前pin_count（Acquire 保守读取）
    pub fn get_pin_count(&self) -> u32 {
        self.pin_count.load(Ordering::Acquire)
    }
}

/// 一个只读的页面保护器。
/// 它的存在就意味着持有了页面的读锁和 pin。
/// 实现了 Deref，可以像 &Page 一样直接使用。
#[derive(Debug)]
pub struct ReadPageGuard {
    bpm: Arc<BufferPoolManager>,
    // 这个 Arc 必须持有，以确保 guard 指向的数据是有效的。
    // 我们用 _ 前缀表示它只是为了维持生命周期。
    _page: Arc<RwLock<Page>>,
    guard: ManuallyDrop<RwLockReadGuard<'static, Page>>,
}

impl Deref for ReadPageGuard {
    type Target = Page;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

// 为ReadPageGuard添加pin_count的便捷访问
impl ReadPageGuard {
    pub fn pin_count(&self) -> u32 {
        self.guard.get_pin_count()
    }
}

impl Drop for ReadPageGuard {
    fn drop(&mut self) {
        // Unpin under latch (safe because evictor requires write lock to evict)
        let old_pin = self.guard.unpin();
        let page_id = self.guard.page_id;
        let is_dirty = self.guard.is_dirty;
        unsafe {
            ManuallyDrop::drop(&mut self.guard);
        }
        if let Err(e) = self.bpm.complete_unpin(page_id, is_dirty, old_pin) {
            eprintln!("Warning: Failed to complete_unpin page {}: {}", page_id, e);
        }
    }
}

/// 一个可写的页面保护器。
/// 它的存在就意味着持有了页面的写锁和 pin。
/// 实现了 Deref 和 DerefMut，可以像 &mut Page 一样直接使用。
#[derive(Debug)]
pub struct WritePageGuard {
    bpm: Arc<BufferPoolManager>,
    _page: Arc<RwLock<Page>>,
    guard: ManuallyDrop<RwLockWriteGuard<'static, Page>>,
}

impl Deref for WritePageGuard {
    type Target = Page;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for WritePageGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // 任何可变访问都应该将页面标记为脏页。
        self.guard.is_dirty = true;
        &mut self.guard
    }
}

// 为WritePageGuard添加pin_count的便捷访问
impl WritePageGuard {
    pub fn pin_count(&self) -> u32 {
        self.guard.get_pin_count()
    }
}

impl Drop for WritePageGuard {
    fn drop(&mut self) {
        // Unpin under latch (safe with our eviction protocol)
        let old_pin = self.guard.unpin();
        let page_id = self.guard.page_id;
        let is_dirty = self.guard.is_dirty;
        unsafe {
            ManuallyDrop::drop(&mut self.guard);
        }
        if let Err(e) = self.bpm.complete_unpin(page_id, is_dirty, old_pin) {
            eprintln!("Warning: Failed to complete_unpin page {}: {}", page_id, e);
        }
    }
}

// 工厂函数，隐藏 `unsafe` 实现细节
pub(crate) fn new_read_guard(
    bpm: Arc<BufferPoolManager>,
    page: Arc<RwLock<Page>>,
) -> ReadPageGuard {
    let guard = page.read();
    // SAFETY: 这是安全的核心。我们将 guard 的生命周期从 page 的借用中分离出来。
    // 这是可行的，因为 ReadPageGuard 结构体本身持有一个 page 的 Arc 引用，
    // 保证了在 guard 的生命周期内，底层的 RwLock 不会被销毁。
    let static_guard = unsafe { mem::transmute::<_, RwLockReadGuard<'static, Page>>(guard) };
    ReadPageGuard {
        bpm,
        _page: page,
        guard: ManuallyDrop::new(static_guard),
    }
}

pub(crate) fn new_write_guard(
    bpm: Arc<BufferPoolManager>,
    page: Arc<RwLock<Page>>,
) -> WritePageGuard {
    let guard = page.write();
    // SAFETY: 同上，WritePageGuard 持有 page 的 Arc 引用，保证了安全性。
    let static_guard = unsafe { mem::transmute::<_, RwLockWriteGuard<'static, Page>>(guard) };
    WritePageGuard {
        bpm,
        _page: page,
        guard: ManuallyDrop::new(static_guard),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::TempDir;

    // 引入真实的 BufferPoolManager 及其依赖
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;
    use crate::{buffer::buffer_pool::BufferPoolManager, utils::cache::Replacer};

    // 引入需要被测试的 Page 结构体
    use super::{Page, INVALID_PAGE_ID};

    /// 辅助函数，用于为 page 模块的测试设置一个真实的环境。
    fn setup_real_bpm_environment(num_pages: usize) -> (TempDir, Arc<BufferPoolManager>) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let disk_manager = Arc::new(DiskManager::try_new(db_path).unwrap());
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let buffer_pool_manager = Arc::new(BufferPoolManager::new(num_pages, disk_scheduler));

        (temp_dir, buffer_pool_manager)
    }

    #[test]
    fn test_page_struct_creation() {
        let page = Page::new(1);
        assert_eq!(page.page_id(), 1);
        assert!(!page.is_dirty);
        assert_eq!(page.get_pin_count(), 0);
        assert!(page.data().iter().all(|&b| b == 0));

        let empty_page = Page::empty();
        assert_eq!(empty_page.page_id(), INVALID_PAGE_ID);
    }

    #[test]
    fn test_read_guard_deref_and_drop() {
        let (_temp_dir, bpm) = setup_real_bpm_environment(10);

        let page_id = {
            let guard = bpm.new_page().unwrap();
            guard.page_id()
        };

        // 确认在创建后，pin_count 为 0，页面在 replacer 中
        assert_eq!(bpm.replacer.read().size(), 1);

        {
            let read_guard = bpm.fetch_page_read(page_id).unwrap();

            // 1. 测试 Deref - 可以直接访问页面字段
            assert_eq!(read_guard.page_id(), page_id);
            assert_eq!(read_guard.pin_count(), 1);

            // 页面被 pin 住，应该从 replacer 中移除
            assert_eq!(bpm.replacer.read().size(), 0);

            // read_guard 在此 drop
        }

        // 2. 测试 Drop - 确认页面被 unpin，并回到 replacer
        assert_eq!(bpm.replacer.read().size(), 1);

        // 验证 pin count 已经降回 0
        let final_check_guard = bpm.fetch_page_read(page_id).unwrap();
        // pin_count 再次变为 1，因为我们又 fetch 了一次
        assert_eq!(final_check_guard.pin_count(), 1);
    }

    #[test]
    fn test_write_guard_deref_mut_and_drop() {
        let (_temp_dir, bpm) = setup_real_bpm_environment(10);
        let page_id;

        {
            let mut write_guard = bpm.new_page().unwrap();
            page_id = write_guard.page_id();

            // 1. 测试 DerefMut - 写入数据
            write_guard.data[0] = 123;

            // 2. 确认写入操作将 is_dirty 标记为 true
            assert!(write_guard.is_dirty);

            // 页面被 pin 住，replacer 应为空
            assert_eq!(bpm.replacer.read().size(), 0);

            // write_guard 在此 drop
        }

        // 3. 测试 Drop - 页面被 unpin，回到 replacer
        assert_eq!(bpm.replacer.read().size(), 1);

        // 4. 重新获取页面，验证数据和脏位
        let read_guard = bpm.fetch_page_read(page_id).unwrap();
        assert_eq!(read_guard.data()[0], 123);
        assert!(read_guard.is_dirty); // 脏位应该被保留
    }

    #[test]
    fn test_write_guard_without_mutation_is_not_dirty() {
        let (_temp_dir, bpm) = setup_real_bpm_environment(10);
        let page_id = bpm.new_page().unwrap().page_id(); // 创建并立即 unpin

        {
            // 获取一个 WriteGuard 但不修改它
            let write_guard = bpm.fetch_page_write(page_id).unwrap();
            // 只通过 Deref 读取
            assert_eq!(write_guard.page_id(), page_id);
            // write_guard 在此 drop
        }

        // 重新获取页面，验证它没有被标记为脏页
        let read_guard = bpm.fetch_page_read(page_id).unwrap();
        assert!(!read_guard.is_dirty);
    }

    #[test]
    fn test_guards_hold_lock() {
        let (_temp_dir, bpm) = setup_real_bpm_environment(10);
        let page_arc = bpm.pool[0].clone(); // 直接访问内部 pool 以便测试锁

        // 为了测试，我们手动在 frame 0 上创建一个页面
        let page_id = 100;
        bpm.page_table.insert(page_id, 0);
        *page_arc.write() = Page::new(page_id);

        // 1. 获取一个写保护器
        let mut write_guard = bpm.fetch_page_write(page_id).unwrap();

        // 2. 尝试在持有写锁的同时获取读锁会失败
        assert!(page_arc.try_read().is_none());
        assert!(page_arc.try_write().is_none());

        // 3. 修改数据
        write_guard.data[0] = 123;

        // 4. 释放写保护器
        drop(write_guard);

        // 5. 现在应该可以成功获取读锁了
        let read_guard = bpm.fetch_page_read(page_id).unwrap();
        assert!(page_arc.try_write().is_none()); // 但仍然不能获取写锁
        assert_eq!(read_guard.data[0], 123);
    }
}
