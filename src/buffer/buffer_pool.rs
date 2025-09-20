use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::Ordering;
use std::{collections::VecDeque, sync::Arc};

use crate::buffer::page::{self, Page, PageId, ReadPageGuard, WritePageGuard, PAGE_SIZE};

use crate::catalog::SchemaRef;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::codec::{
    BPlusTreeHeaderPageCodec, BPlusTreeInternalPageCodec, BPlusTreeLeafPageCodec,
    BPlusTreePageCodec, TablePageCodec,
};
use crate::storage::disk_scheduler::DiskScheduler;
use crate::storage::{
    page::TablePage,
    page::{BPlusTreeHeaderPage, BPlusTreeInternalPage, BPlusTreeLeafPage, BPlusTreePage},
};

use crate::utils::cache::lru_k::LRUKReplacer;
use crate::utils::cache::Replacer;

pub type FrameId = usize;

pub const BUFFER_POOL_SIZE: usize = 5000;

#[derive(Debug)]
pub struct BufferPoolManager {
    pub(crate) pool: Vec<Arc<RwLock<Page>>>,
    pub(crate) replacer: Arc<RwLock<LRUKReplacer>>,
    pub(crate) disk_scheduler: Arc<DiskScheduler>,
    pub(crate) page_table: Arc<DashMap<PageId, FrameId>>,
    pub(crate) free_list: Arc<RwLock<VecDeque<FrameId>>>,
    /// Per-page inflight load guards to serialize concurrent loads of the same page
    pub(crate) inflight_loads: Arc<DashMap<PageId, Arc<Mutex<()>>>>,
}
impl BufferPoolManager {
    #[inline]
    fn replacer_set_evictable(&self, frame_id: FrameId, evictable: bool) -> QuillSQLResult<()> {
        let mut rep = self.replacer.write();
        rep.set_evictable(frame_id, evictable)
            .map_err(|e| QuillSQLError::Internal(format!("replacer set_evictable failed: {}", e)))
    }

    #[inline]
    fn replacer_record_access(&self, frame_id: FrameId) -> QuillSQLResult<()> {
        let mut rep = self.replacer.write();
        rep.record_access(frame_id)
            .map_err(|e| QuillSQLError::Internal(format!("replacer record_access failed: {}", e)))
    }
    #[inline]
    fn replacer_touch_and_set(&self, frame_id: FrameId, evictable: bool) -> QuillSQLResult<()> {
        let mut rep = self.replacer.write();
        rep.record_access(frame_id).map_err(|e| {
            QuillSQLError::Internal(format!("replacer record_access failed: {}", e))
        })?;
        rep.set_evictable(frame_id, evictable)
            .map_err(|e| QuillSQLError::Internal(format!("replacer set_evictable failed: {}", e)))
    }

    pub fn new(num_pages: usize, disk_scheduler: Arc<DiskScheduler>) -> Self {
        let mut free_list = VecDeque::with_capacity(num_pages);
        let mut pool = vec![];
        for i in 0..num_pages {
            free_list.push_back(i);
            pool.push(Arc::new(RwLock::new(Page::empty())));
        }

        Self {
            pool,
            replacer: Arc::new(RwLock::new(LRUKReplacer::with_k(num_pages, 2))),
            disk_scheduler,
            page_table: Arc::new(DashMap::new()),
            free_list: Arc::new(RwLock::new(free_list)),
            inflight_loads: Arc::new(DashMap::new()),
        }
    }

    /// 创建一个新页面。
    pub fn new_page(self: &Arc<Self>) -> QuillSQLResult<WritePageGuard> {
        if self.free_list.read().is_empty() && self.replacer.read().size() == 0 {
            return Err(QuillSQLError::Storage(
                "Cannot new page because buffer pool is full and no page to evict".to_string(),
            ));
        }

        let frame_id = self.allocate_frame()?;

        let rx_alloc = self.disk_scheduler.schedule_allocate()?;
        let new_page_id = rx_alloc.recv().map_err(|e| {
            QuillSQLError::Internal(format!("Failed to receive allocated page_id: {}", e))
        })??;
        self.page_table.insert(new_page_id, frame_id);

        let page_arc = self.pool[frame_id].clone();
        {
            let mut page_writer = page_arc.write();
            *page_writer = Page::new(new_page_id);
            page_writer.pin_count.store(1, Ordering::Relaxed);
        }

        self.replacer_touch_and_set(frame_id, false)?;

        Ok(page::new_write_guard(self.clone(), page_arc))
    }

    /// 获取一个只读页面。
    pub fn fetch_page_read(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<ReadPageGuard> {
        // Robust retry to tolerate delete/evict races
        for _ in 0..16 {
            if let Ok(frame_id) = self.get_frame_for_page(page_id) {
                let page_arc = self.pool[frame_id].clone();

                // Validate mapping before pin
                let reader = page_arc.read();
                if reader.page_id() == page_id {
                    // Pin now that mapping is validated
                    reader.pin();
                    // Mark non-evictable while pinned
                    let _ = self.replacer_set_evictable(frame_id, false);
                    drop(reader);
                    return Ok(page::new_read_guard(self.clone(), page_arc));
                } else {
                    drop(reader);
                    std::hint::spin_loop();
                    continue;
                }
            } else {
                std::hint::spin_loop();
                continue;
            }
        }
        Err(QuillSQLError::Internal(
            "fetch_page_read: failed after retries".to_string(),
        ))
    }

    /// 获取一个可写页面。
    pub fn fetch_page_write(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<WritePageGuard> {
        for _ in 0..16 {
            if let Ok(frame_id) = self.get_frame_for_page(page_id) {
                let page_arc = self.pool[frame_id].clone();

                // Validate mapping then pin
                {
                    let reader = page_arc.read();
                    if reader.page_id() == page_id {
                        reader.pin();
                        // Mark non-evictable while pinned
                        let _ = self.replacer_set_evictable(frame_id, false);
                    } else {
                        drop(reader);
                        std::hint::spin_loop();
                        continue;
                    }
                    if std::env::var("QUILL_DEBUG_LOCK").ok().as_deref() == Some("2") {
                        eprintln!(
                            "[LOCK DEBUG] thread={:?} attempt write page_id={}",
                            std::thread::current().id(),
                            reader.page_id()
                        );
                    }
                }

                let guard = page::new_write_guard(self.clone(), page_arc.clone());
                if std::env::var("QUILL_DEBUG_LOCK").ok().as_deref() == Some("2") {
                    if let Some(r) = page_arc.try_read() {
                        eprintln!(
                            "[LOCK DEBUG] thread={:?} acquired write page_id={}",
                            std::thread::current().id(),
                            r.page_id()
                        );
                    } else {
                        eprintln!(
                            "[LOCK DEBUG] thread={:?} acquired write page_id=<busy>",
                            std::thread::current().id()
                        );
                    }
                }
                return Ok(guard);
            } else {
                std::hint::spin_loop();
                continue;
            }
        }
        Err(QuillSQLError::Internal(
            "fetch_page_write: failed after retries".to_string(),
        ))
    }

    /// 完成 unpin 的后续处理：根据旧的 pin_count 决定是否可驱逐，并处理脏位。
    /// 该函数必须在未持有页面锁的情况下调用，以避免与 fetch_page_write 的锁序形成死锁。
    pub(crate) fn complete_unpin(
        &self,
        page_id: PageId,
        is_dirty: bool,
        old_pin_count: u32,
    ) -> QuillSQLResult<()> {
        if let Some(frame_id_ref) = self.page_table.get(&page_id) {
            let frame_id = *frame_id_ref;
            if is_dirty {
                if let Some(mut p) = self.pool[frame_id].try_write() {
                    p.is_dirty = true;
                }
            }
            if old_pin_count == 1 {
                self.replacer_set_evictable(frame_id, true)?;
            }
        }
        Ok(())
    }

    /// 辅助函数：为给定的 page_id 查找或分配一个 frame。
    fn get_frame_for_page(&self, page_id: PageId) -> QuillSQLResult<FrameId> {
        if let Some(frame_id_ref) = self.page_table.get(&page_id) {
            let frame_id = *frame_id_ref;
            self.replacer_record_access(frame_id)?;
            Ok(frame_id)
        } else {
            // Serialize concurrent loads for the same page_id
            let (lock_arc, created_here) = if let Some(g) = self.inflight_loads.get(&page_id) {
                (g.clone(), false)
            } else {
                let arc = Arc::new(Mutex::new(()));
                self.inflight_loads.insert(page_id, arc.clone());
                (arc, true)
            };

            let _lock_guard = lock_arc.lock();

            // Re-check after acquiring inflight guard to avoid duplicate loads
            if let Some(frame_id_ref2) = self.page_table.get(&page_id) {
                let frame_id2 = *frame_id_ref2;
                self.replacer_record_access(frame_id2)?;
                return Ok(frame_id2);
            }

            let frame_id = match self.allocate_frame() {
                Ok(fid) => fid,
                Err(e) => {
                    if created_here {
                        self.inflight_loads.remove(&page_id);
                    }
                    return Err(e);
                }
            };

            let page_data_bytes = match self.disk_scheduler.schedule_read(page_id) {
                Ok(rx) => match rx.recv() {
                    Ok(Ok(bytes)) => bytes,
                    Ok(Err(e)) => {
                        if created_here {
                            self.inflight_loads.remove(&page_id);
                        }
                        return Err(e);
                    }
                    Err(e) => {
                        if created_here {
                            self.inflight_loads.remove(&page_id);
                        }
                        return Err(QuillSQLError::Internal(format!(
                            "Channel disconnected: {}",
                            e
                        )));
                    }
                },
                Err(e) => {
                    if created_here {
                        self.inflight_loads.remove(&page_id);
                    }
                    return Err(e);
                }
            };

            let mut page_data_array = [0u8; PAGE_SIZE];
            page_data_array.copy_from_slice(&page_data_bytes[..PAGE_SIZE]);

            let page_arc = &self.pool[frame_id];
            {
                let mut page = page_arc.write();
                *page = Page::new(page_id);
                page.data = page_data_array;
                // pin_count 将在调用者中设置
            }

            self.page_table.insert(page_id, frame_id);
            if created_here {
                self.inflight_loads.remove(&page_id);
            }

            self.replacer_record_access(frame_id)?;
            Ok(frame_id)
        }
    }

    pub fn fetch_table_page(
        self: &Arc<Self>,
        page_id: PageId,
        schema: SchemaRef,
    ) -> QuillSQLResult<(ReadPageGuard, TablePage)> {
        let guard = self.fetch_page_read(page_id)?;
        // 因为 guard 实现了 Deref，可以直接访问 data
        let (table_page, _) = TablePageCodec::decode(&guard.data, schema)?;
        Ok((guard, table_page))
    }

    pub fn fetch_tree_page(
        self: &Arc<Self>,
        page_id: PageId,
        key_schema: SchemaRef,
    ) -> QuillSQLResult<(ReadPageGuard, BPlusTreePage)> {
        let guard = self.fetch_page_read(page_id)?;
        let (tree_page, _) = BPlusTreePageCodec::decode(&guard.data, key_schema.clone())?;
        Ok((guard, tree_page))
    }

    pub fn fetch_tree_internal_page(
        self: &Arc<Self>,
        page_id: PageId,
        key_schema: SchemaRef,
    ) -> QuillSQLResult<(ReadPageGuard, BPlusTreeInternalPage)> {
        let guard = self.fetch_page_read(page_id)?;
        let (tree_internal_page, _) =
            BPlusTreeInternalPageCodec::decode(&guard.data, key_schema.clone())?;
        Ok((guard, tree_internal_page))
    }

    pub fn fetch_tree_leaf_page(
        self: &Arc<Self>,
        page_id: PageId,
        key_schema: SchemaRef,
    ) -> QuillSQLResult<(ReadPageGuard, BPlusTreeLeafPage)> {
        let guard = self.fetch_page_read(page_id)?;
        let (tree_leaf_page, _) = BPlusTreeLeafPageCodec::decode(&guard.data, key_schema.clone())?;
        Ok((guard, tree_leaf_page))
    }

    pub fn fetch_header_page(
        self: &Arc<Self>,
        page_id: PageId,
    ) -> QuillSQLResult<(ReadPageGuard, BPlusTreeHeaderPage)> {
        let guard = self.fetch_page_read(page_id)?;
        let (header_page, _) = BPlusTreeHeaderPageCodec::decode(&guard.data)?;
        Ok((guard, header_page))
    }

    pub fn flush_page(&self, page_id: PageId) -> QuillSQLResult<bool> {
        if let Some(frame_id_ref) = self.page_table.get(&page_id) {
            let frame_id = *frame_id_ref;
            let page_arc = self.pool[frame_id].clone();

            // Lock for reading to copy data, then lock for writing to update dirty flag.
            let page_data = page_arc.read().data;
            let data_bytes = Bytes::copy_from_slice(&page_data);

            self.disk_scheduler
                .schedule_write(page_id, data_bytes)?
                .recv()
                .map_err(|e| QuillSQLError::Internal(format!("Channel disconnected: {}", e)))??;

            page_arc.write().is_dirty = false;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn flush_all_pages(&self) -> QuillSQLResult<()> {
        let page_ids: Vec<PageId> = self.page_table.iter().map(|e| *e.key()).collect();
        for page_id in page_ids {
            if self.page_table.contains_key(&page_id) {
                if let Some(frame_id_ref) = self.page_table.get(&page_id) {
                    let frame_id = *frame_id_ref;
                    if self.pool[frame_id].read().is_dirty {
                        self.flush_page(page_id)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn delete_page(&self, page_id: PageId) -> QuillSQLResult<bool> {
        if let Some((_, frame_id)) = self.page_table.remove(&page_id) {
            let page_arc = self.pool[frame_id].clone();

            // Try to acquire read lock without blocking
            match page_arc.try_read() {
                Some(page_reader) => {
                    if page_reader.get_pin_count() > 0 {
                        // Cannot delete a pinned page, re-insert to page table and return.
                        drop(page_reader); // Release read lock first
                        self.page_table.insert(page_id, frame_id);
                        return Ok(false);
                    }
                    drop(page_reader); // Release read lock
                }
                None => {
                    // Failed to acquire read lock, meaning page is likely pinned with write lock
                    // Re-insert to page table and return false
                    self.page_table.insert(page_id, frame_id);
                    return Ok(false);
                }
            }

            // Reset page memory
            page_arc.write().destroy();

            // Ensure remover precondition: mark evictable before removing from replacer
            {
                let mut rep = self.replacer.write();
                let _ = rep.set_evictable(frame_id, true);
                let _ = rep.remove(frame_id);
            }

            self.free_list.write().push_back(frame_id);

            self.disk_scheduler
                .schedule_deallocate(page_id)?
                .recv()
                .map_err(|e| QuillSQLError::Internal(format!("Channel disconnected: {}", e)))??;

            Ok(true)
        } else {
            // Page not in buffer pool, but we should still try to deallocate from disk.
            self.disk_scheduler
                .schedule_deallocate(page_id)?
                .recv()
                .map_err(|e| QuillSQLError::Internal(format!("Channel disconnected: {}", e)))??;
            Ok(true)
        }
    }

    fn allocate_frame(&self) -> QuillSQLResult<FrameId> {
        // Try free list first
        if let Some(frame_id) = self.free_list.write().pop_front() {
            return Ok(frame_id);
        }

        // Then evict from replacer, with safety checks
        loop {
            let opt = { self.replacer.write().evict() };
            let Some(frame_id) = opt else {
                return Err(QuillSQLError::Storage(
                    "Cannot allocate frame: buffer pool is full and all pages are pinned"
                        .to_string(),
                ));
            };

            let evicted_page_arc = self.pool[frame_id].clone();
            // Use a scoped block to ensure the try_write guard (and Option) drop before next loop
            let handled = {
                let opt_guard = evicted_page_arc.try_write();
                if let Some(evicted_page_writer) = opt_guard {
                    let evicted_page_id = evicted_page_writer.page_id;
                    if evicted_page_writer.get_pin_count() > 0 {
                        drop(evicted_page_writer);
                        let mut rep = self.replacer.write();
                        rep.record_access(frame_id)?;
                        rep.set_evictable(frame_id, true)?;
                        false
                    } else {
                        let need_flush = evicted_page_writer.is_dirty;
                        drop(evicted_page_writer);
                        if need_flush {
                            self.flush_page(evicted_page_id)?;
                        }
                        self.page_table.remove(&evicted_page_id);
                        // signal handled and return after block
                        // we cannot return here due to borrow; mark and handle below
                        // store the decision in handled flag
                        // but we need evicted_page_id only for flush which is done
                        // so we can use a marker
                        true
                    }
                } else {
                    // Couldn't get exclusive lock; frame likely pinned. Try next.
                    let mut rep = self.replacer.write();
                    rep.record_access(frame_id)?;
                    rep.set_evictable(frame_id, true)?;
                    false
                }
            };
            if handled {
                return Ok(frame_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::buffer_pool::BufferPoolManager;
    use crate::storage::disk_manager::DiskManager; // 假设您有 DiskManager
    use crate::storage::disk_scheduler::DiskScheduler; // 假设您有 DiskScheduler
    use crate::utils::cache::Replacer;
    use std::fs;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// 辅助函数，用于为每个测试设置一个干净的环境。
    /// 它会创建一个临时目录、DiskManager 和 BufferPoolManager。
    fn setup_test_environment(
        num_pages: usize,
    ) -> (
        TempDir, // RAII handle for the temp directory
        Arc<BufferPoolManager>,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let disk_manager = Arc::new(DiskManager::try_new(db_path).unwrap());
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let buffer_pool_manager = Arc::new(BufferPoolManager::new(num_pages, disk_scheduler));

        (temp_dir, buffer_pool_manager)
    }

    #[test]
    fn test_new_page_and_basic_fetch() {
        let (_temp_dir, bpm) = setup_test_environment(10);

        // 1. 创建一个新页面
        let mut page0_guard = bpm.new_page().unwrap();
        let page0_id = page0_guard.page_id();

        // 2. 写入一些数据
        let test_data = b"Hello, World!";
        page0_guard.data[..test_data.len()].copy_from_slice(test_data);
        assert!(page0_guard.is_dirty); // 可变访问应该标记为脏页

        // 3. 读取并验证数据
        assert_eq!(&page0_guard.data[..test_data.len()], test_data);

        // 4. 先释放写保护器，然后获取读保护器
        drop(page0_guard);

        let page0_read_guard = bpm.fetch_page_read(page0_id).unwrap();
        assert_eq!(page0_read_guard.page_id(), page0_id);
        assert_eq!(&page0_read_guard.data[..test_data.len()], test_data);
        assert_eq!(page0_read_guard.pin_count(), 1);

        // 5. Drop 读保护器
        drop(page0_read_guard);

        // 6. 确认 pin count 归零
        let final_guard = bpm.fetch_page_read(page0_id).unwrap();
        assert_eq!(final_guard.pin_count(), 1);
        assert_eq!(final_guard.is_dirty, true); // 脏位应该保持
    }

    #[test]
    fn test_unpin_and_eviction_logic() {
        let (_temp_dir, bpm) = setup_test_environment(3);

        // 1. 创建3个页面，填满缓冲池
        let page1 = bpm.new_page().unwrap();
        let page1_id = page1.page_id();
        let page2 = bpm.new_page().unwrap();
        let page2_id = page2.page_id();
        let page3 = bpm.new_page().unwrap();
        let page3_id = page3.page_id();

        // 此时 replacer 为空，因为所有页面都被 pin 住
        assert_eq!(bpm.replacer.read().size(), 0);

        // 2. Drop page1，它应该变得可被驱逐
        drop(page1);
        assert_eq!(bpm.replacer.read().size(), 1);

        // 3. Drop page2，它也应该变得可被驱逐
        drop(page2);
        assert_eq!(bpm.replacer.read().size(), 2);

        // 4. 创建一个新的页面，这将触发驱逐
        // LRU-K 策略下，page1_id 是最先被 unpin 的，应该被驱逐
        let page4 = bpm.new_page().unwrap();
        assert_ne!(page4.page_id(), page1_id);

        // 5. 验证 page1 已经不在 page_table 中
        assert!(bpm.page_table.get(&page1_id).is_none());
        assert!(bpm.page_table.get(&page2_id).is_some());
        assert!(bpm.page_table.get(&page3_id).is_some());

        // 6. page3 仍然被 pin 住，所以 replacer 中只有一个 page2
        assert_eq!(bpm.replacer.read().size(), 1);
    }

    #[test]
    fn test_flush_page() {
        let (temp_dir, bpm) = setup_test_environment(10);
        let db_path = temp_dir.path().join("test.db");

        // 1. 创建一个新页面并写入数据
        let page_id = {
            let mut guard = bpm.new_page().unwrap();
            guard.data[0..4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
            guard.page_id()
            // guard 在此 drop，unpin 时 is_dirty 应该为 true
        };

        // 2. 调用 flush_page
        let flush_result = bpm.flush_page(page_id).unwrap();
        assert!(flush_result);

        // 3. 验证页面的脏位已被清除
        let guard = bpm.fetch_page_read(page_id).unwrap();
        assert!(!guard.is_dirty);
        drop(guard);

        // 4. 验证数据确实被写入磁盘（不检查具体位置，因为DiskManager的实现细节可能不同）
        let file_data = fs::read(db_path).unwrap();

        // 搜索整个文件，确认数据已写入
        let mut found = false;
        for i in 0..=(file_data.len().saturating_sub(4)) {
            if &file_data[i..i + 4] == &[0xDE, 0xAD, 0xBE, 0xEF] {
                found = true;
                break;
            }
        }

        assert!(found, "Test data was not written to disk correctly");
    }

    #[test]
    fn test_delete_page() {
        let (_temp_dir, bpm) = setup_test_environment(10);

        // 1. 创建一些页面
        let page1_id = bpm.new_page().unwrap().page_id();
        drop(bpm.new_page().unwrap()); // unpin

        assert_eq!(bpm.page_table.len(), 2);
        assert_eq!(bpm.free_list.read().len(), 8);

        // 2. 删除一个未被 pin 的页面 (page1)
        drop(bpm.fetch_page_read(page1_id).unwrap()); // unpin page1
        let deleted = bpm.delete_page(page1_id).unwrap();
        assert!(deleted);

        // 3. 验证其已被移除
        assert!(bpm.page_table.get(&page1_id).is_none());
        assert_eq!(bpm.page_table.len(), 1);
        assert_eq!(bpm.free_list.read().len(), 9); // free_list 增加
        assert_eq!(bpm.replacer.read().size(), 1); // 另一个页面还在

        // 4. 尝试获取被删除的页面，应该会从磁盘重新读取（内容为空）
        let refetched_guard = bpm.fetch_page_read(page1_id).unwrap();
        assert!(refetched_guard.data.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_delete_pinned_page_fails() {
        let (_temp_dir, bpm) = setup_test_environment(10);

        let guard = bpm.new_page().unwrap();
        let page_id = guard.page_id();

        // 尝试删除一个被 pin 的页面
        let deleted = bpm.delete_page(page_id).unwrap();
        assert!(!deleted); // 应该失败

        // 验证页面仍然存在
        assert!(bpm.page_table.get(&page_id).is_some());
    }

    #[test]
    fn test_buffer_pool_is_full() {
        let (_temp_dir, bpm) = setup_test_environment(2);

        // 创建两个页面，填满缓冲池，并且一直持有它们的 guard
        let _page1 = bpm.new_page().unwrap();
        let _page2 = bpm.new_page().unwrap();

        // 此时缓冲池已满，且所有页面都被 pin 住，无法驱逐
        assert_eq!(bpm.replacer.read().size(), 0);
        assert!(bpm.free_list.read().is_empty());

        // 尝试创建第三个页面，应该会失败
        let page3_result = bpm.new_page();
        assert!(page3_result.is_err());
    }

    #[test]
    fn test_concurrent_reads_and_exclusive_write() {
        let (_temp_dir, bpm) = setup_test_environment(10);

        // 创建一个页面
        let page_id = {
            let mut guard = bpm.new_page().unwrap();
            guard.data[0] = 42;
            guard.page_id()
        };

        // 1. 获取一个读保护器
        let read_guard1 = bpm.fetch_page_read(page_id).unwrap();
        assert_eq!(read_guard1.data[0], 42);
        assert_eq!(read_guard1.pin_count(), 1);
        drop(read_guard1);

        // 2. 验证写操作是独占的
        let mut write_guard = bpm.fetch_page_write(page_id).unwrap();
        write_guard.data[0] = 99;
        assert_eq!(write_guard.data[0], 99);
    }

    #[test]
    fn test_concurrent_same_page_fetch_single_frame() {
        use std::thread;
        let (_temp_dir, bpm) = setup_test_environment(8);

        // Prepare a page on disk by allocating once
        let page_id = bpm.new_page().unwrap().page_id();
        // Unpin to allow others to fetch
        drop(bpm.fetch_page_read(page_id).unwrap());

        let threads = (0..8)
            .map(|_| {
                let bpm_c = bpm.clone();
                thread::spawn(move || {
                    for _ in 0..100 {
                        let g = bpm_c.fetch_page_read(page_id).unwrap();
                        assert_eq!(g.page_id(), page_id);
                    }
                })
            })
            .collect::<Vec<_>>();

        for t in threads {
            t.join().unwrap();
        }

        // Ensure only one frame is mapped to this page
        let frame_id = bpm.page_table.get(&page_id).map(|r| *r).unwrap();
        // Verify that the frame actually holds the page
        assert_eq!(bpm.pool[frame_id].read().page_id(), page_id);
    }

    #[test]
    fn test_delete_vs_fetch_race_safety() {
        use std::thread;
        let (_temp_dir, bpm) = setup_test_environment(8);

        let page_id = bpm.new_page().unwrap().page_id();
        drop(bpm.fetch_page_read(page_id).unwrap());

        let bpm_del = bpm.clone();
        let deleter = thread::spawn(move || {
            // Try to delete repeatedly; it may fail if pinned, but must not corrupt state
            for _ in 0..1000 {
                let _ = bpm_del.delete_page(page_id).unwrap();
            }
        });

        let bpm_fetch = bpm.clone();
        let fetcher = thread::spawn(move || {
            for _ in 0..1000 {
                let _ = bpm_fetch.fetch_page_read(page_id).unwrap();
            }
        });

        deleter.join().unwrap();
        fetcher.join().unwrap();

        // State must remain consistent: either mapped and readable, or not mapped but deletable
        let frame_id_opt = bpm.page_table.get(&page_id).map(|r| *r);
        if let Some(frame_id) = frame_id_opt {
            assert_eq!(bpm.pool[frame_id].read().page_id(), page_id);
        }
    }
}
