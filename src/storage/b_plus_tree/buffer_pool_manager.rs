use crate::error::{Error, Result};
use crate::storage::codec::{
    index_page::{BPlusTreeInternalPageCodec, BPlusTreeLeafPageCodec, BPlusTreePageCodec},
    table_page::TablePageCodec,
};
use crate::storage::b_plus_tree::disk::disk_scheduler::DiskScheduler;
use crate::storage::b_plus_tree::page::index_page::{
    BPlusTreeInternalPage, BPlusTreeLeafPage, BPlusTreePage,
};
use crate::storage::b_plus_tree::page::table_page::TablePage;
use crate::utils::cache::lru_k::LRUKReplacer;
use crate::utils::cache::Replacer;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use derive_with::With;
use log::error;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub type PageId = u32;
pub type FrameId = usize;
pub type AtomicPageId = AtomicU32;

pub const BUFFER_POOL_SIZE: usize = 1000;
// Define a constant for invalid page ID
pub const INVALID_PAGE_ID: PageId = 0;
pub const PAGE_SIZE: usize = 4096;

// Frame corresponds to the frame (page) in memory.
#[derive(Debug, With)]
pub struct Page {
    pub page_id: PageId,
    data: [u8; PAGE_SIZE],
    // 被引用次数
    pub pin_count: AtomicU32,
    // 是否被写过
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
        self.pin_count.store(0, Ordering::SeqCst);
        self.is_dirty = false;
    }

    pub fn set_data_from_bytes_mut(&mut self, data_mut: BytesMut) -> Result<()> {
        if data_mut.len() > PAGE_SIZE {
            return Err(Error::Internal(
                "Received data larger than page size".to_string(),
            ));
        }
        if data_mut.len() != PAGE_SIZE {
            return Err(Error::Internal(format!(
                "Read incorrect amount of data: {} bytes",
                data_mut.len()
            )));
        }
        self.data.copy_from_slice(&data_mut);
        self.is_dirty = true;
        Ok(())
    }

    pub fn set_data(&mut self, data: [u8; PAGE_SIZE]) {
        self.data = data;
        self.is_dirty = true;
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn data_as_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self.data)
    }

    pub fn replace(&mut self, other: Page) {
        self.page_id = other.page_id;
        self.data = other.data;
        self.pin_count
            .store(other.pin_count.load(Ordering::SeqCst), Ordering::SeqCst);
        self.is_dirty = other.is_dirty;
    }
}

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
        let guard = self.page.read();
        let page_id = guard.page_id;
        let old_pin_count = guard.pin_count.fetch_sub(1, Ordering::Release);

        if old_pin_count == 0 {
            error!(
                "WARN: PageRef dropped for page {} with pin_count already 0!",
                page_id
            );
            return;
        }

        let new_pin_count = old_pin_count - 1;
        drop(guard);

        if let Some(frame_id_entry) = self.page_table.get(&page_id) {
            let frame_id = *frame_id_entry;
            drop(frame_id_entry);

            if new_pin_count == 0 {
                let mut replacer_guard = self.replacer.write();
                if let Err(e) = replacer_guard.set_evictable(frame_id, true) {
                    error!(
                        "ERROR: Failed to set evictable=true for frame {} (page {}): {:?}",
                        frame_id, page_id, e
                    );
                } else {
                    println!(
                        "DEBUG: Page {} (frame {}) unpinned and set evictable.",
                        page_id, frame_id
                    );
                }
            }
        } else {
            error!(
                "ERROR: Cannot unpin page id {} as it is not in the page_table during PageRef drop",
                page_id
            );
        }
    }
}

#[derive(Debug)]
pub struct BufferPoolManager {
    pool: Vec<Arc<RwLock<Page>>>,
    // LRU-K置换算法
    pub replacer: Arc<RwLock<LRUKReplacer>>,
    pub disk_scheduler: Arc<DiskScheduler>,
    // 缓冲池中的页号与frame号的映射
    page_table: Arc<DashMap<PageId, FrameId>>,
    // 缓冲池中空闲的frame
    free_list: Arc<RwLock<VecDeque<FrameId>>>,
    _pool_size: usize,
}
impl BufferPoolManager {
    pub fn new(pool_size: usize, k: usize, disk_scheduler: Arc<DiskScheduler>) -> Result<Self> {
        let mut free_list = VecDeque::with_capacity(pool_size);
        let mut pool = vec![];
        for i in 0..pool_size {
            free_list.push_back(i);
            pool.push(Arc::new(RwLock::new(Page::new(INVALID_PAGE_ID))));
        }

        Ok(Self {
            pool,
            replacer: Arc::new(RwLock::new(LRUKReplacer::with_k(pool_size, k))),
            disk_scheduler,
            page_table: Arc::new(DashMap::new()),
            free_list: Arc::new(RwLock::new(free_list)),
            _pool_size: pool_size,
        })
    }

    pub fn new_page(&self) -> Result<PageRef> {
        let frame_id = self.allocate_frame()?;

        let rx = self.disk_scheduler.schedule_allocate()?;
        let new_page_id = match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(id)) => id,
            Ok(Err(e)) => {
                self.free_list.write().push_back(frame_id);
                return Err(e);
            }
            Err(e) => {
                self.free_list.write().push_back(frame_id);
                return Err(Error::Internal(format!("Timeout allocating page: {}", e)));
            }
        };

        self.page_table.insert(new_page_id, frame_id);
        let page_arc = self.pool[frame_id].clone();
        {
            let mut page_guard = page_arc.write();
            page_guard.page_id = new_page_id;
            page_guard.is_dirty = true;
            page_guard.pin_count.store(1, Ordering::SeqCst);
            page_guard.data.fill(0);
        }

        {
            let mut replacer_guard = self.replacer.write();
            replacer_guard.record_access(frame_id)?;
            replacer_guard.set_evictable(frame_id, false)?;
        }

        println!(
            "DEBUG: Created new page {} in frame {}",
            new_page_id, frame_id
        );
        Ok(PageRef {
            page: page_arc,
            page_table: self.page_table.clone(),
            replacer: self.replacer.clone(),
        })
    }

    pub fn fetch_page(&self, page_id: PageId) -> Result<PageRef> {
        if page_id == INVALID_PAGE_ID {
            return Err(Error::Internal(
                "Cannot fetch invalid page ID 0".to_string(),
            ));
        }

        if let Some(frame_id_entry) = self.page_table.get(&page_id) {
            let frame_id = *frame_id_entry;
            drop(frame_id_entry);

            println!(
                "DEBUG: fetch_page cache hit for page {} in frame {}",
                page_id, frame_id
            );
            let page_arc = self.pool[frame_id].clone();

            page_arc.read().pin_count.fetch_add(1, Ordering::Acquire);

            {
                let mut replacer_guard = self.replacer.write();
                replacer_guard.record_access(frame_id)?;
                replacer_guard.set_evictable(frame_id, false)?;
            }

            return Ok(PageRef {
                page: page_arc,
                page_table: self.page_table.clone(),
                replacer: self.replacer.clone(),
            });
        }

        let frame_id = self.allocate_frame()?;

        let rx = self.disk_scheduler.schedule_read(page_id)?;
        let page_data_res = rx.recv_timeout(Duration::from_secs(5));

        match page_data_res {
            Ok(Ok(page_data_bytes)) => {
                self.page_table.insert(page_id, frame_id);
                let page_arc = self.pool[frame_id].clone();
                {
                    let mut page_guard = page_arc.write();
                    page_guard.page_id = page_id;
                    page_guard.is_dirty = false;
                    page_guard.pin_count.store(1, Ordering::SeqCst);
                    page_guard.set_data_from_bytes_mut(page_data_bytes)?;
                }

                {
                    let mut replacer_guard = self.replacer.write();
                    replacer_guard.record_access(frame_id)?;
                    replacer_guard.set_evictable(frame_id, false)?;
                }

                println!("DEBUG: Fetched page {} into frame {}", page_id, frame_id);
                Ok(PageRef {
                    page: page_arc,
                    page_table: self.page_table.clone(),
                    replacer: self.replacer.clone(),
                })
            }
            Ok(Err(e)) => {
                self.free_list.write().push_back(frame_id);
                Err(Error::Internal(format!(
                    "Disk read failed for page {}: {:?}",
                    page_id, e
                )))
            }
            Err(e) => {
                self.free_list.write().push_back(frame_id);
                Err(Error::Internal(format!(
                    "Timeout/channel error fetching page {}: {}",
                    page_id, e
                )))
            }
        }
    }

    pub fn fetch_table_page(&self, page_id: PageId) -> Result<(PageRef, TablePage)> {
        let page = self.fetch_page(page_id)?;
        let (table_page, _) = TablePageCodec::decode(page.read().data())?;
        Ok((page, table_page))
    }

    pub fn fetch_tree_page(&self, page_id: PageId) -> Result<(PageRef, BPlusTreePage)> {
        let page = self.fetch_page(page_id)?;
        let (tree_page, _) = BPlusTreePageCodec::decode(page.read().data())?;
        Ok((page, tree_page))
    }

    pub fn fetch_tree_internal_page(
        &self,
        page_id: PageId,
    ) -> Result<(PageRef, BPlusTreeInternalPage)> {
        let page = self.fetch_page(page_id)?;
        let (tree_internal_page, _) = BPlusTreeInternalPageCodec::decode(page.read().data())?;
        Ok((page, tree_internal_page))
    }

    pub fn fetch_tree_leaf_page(&self, page_id: PageId) -> Result<(PageRef, BPlusTreeLeafPage)> {
        let page = self.fetch_page(page_id)?;
        let (tree_leaf_page, _) = BPlusTreeLeafPageCodec::decode(page.read().data())?;
        Ok((page, tree_leaf_page))
    }

    pub fn flush_page(&self, page_id: PageId) -> Result<bool> {
        if let Some(frame_id_entry) = self.page_table.get(&page_id) {
            let frame_id = *frame_id_entry;
            let page_arc = self.pool[frame_id].clone();
            drop(frame_id_entry);

            self.flush_page_internal(page_id, frame_id, page_arc)?;
            Ok(true)
        } else {
            println!(
                "WARN: flush_page called for page {} not in buffer pool",
                page_id
            );
            Ok(false)
        }
    }

    pub fn flush_all_pages(&self) -> Result<()> {
        println!("DEBUG: Flushing all pages.");
        let pages_to_flush: Vec<(PageId, FrameId, Arc<RwLock<Page>>)> = self
            .page_table
            .iter()
            .map(|entry| {
                (
                    *entry.key(),
                    *entry.value(),
                    self.pool[*entry.value()].clone(),
                )
            })
            .collect();

        let mut first_error: Option<Error> = None;

        for (page_id, frame_id, page_arc) in pages_to_flush {
            let should_flush = page_arc.read().is_dirty;
            if should_flush {
                if let Err(e) = self.flush_page_internal(page_id, frame_id, page_arc) {
                    eprintln!(
                        "ERROR: Failed to flush page {} (frame {}): {:?}",
                        page_id, frame_id, e
                    );
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }
        println!("DEBUG: flush_all_pages finished.");
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    pub fn delete_page(&self, page_id: PageId) -> Result<bool> {
        if let Some((_, frame_id)) = self.page_table.remove(&page_id) {
            let page_arc = self.pool[frame_id].clone();

            let pin_count = page_arc.read().pin_count.load(Ordering::Acquire);
            if pin_count > 0 {
                eprintln!(
                    "ERROR: Cannot delete pinned page {} (pin_count={})",
                    page_id, pin_count
                );
                self.page_table.insert(page_id, frame_id);
                return Err(Error::Internal(format!(
                    "Cannot delete pinned page {}",
                    page_id
                )));
            }

            {
                let mut page_guard = page_arc.write();
                page_guard.destroy();
            }

            self.replacer.write().remove(frame_id);
            self.free_list.write().push_back(frame_id);

            println!(
                "DEBUG: Deleted page {} from buffer pool (frame {})",
                page_id, frame_id
            );

            let rx = self.disk_scheduler.schedule_deallocate(page_id)?;
            match rx.recv_timeout(Duration::from_secs(5)) {
                Ok(Ok(())) => Ok(true),
                Ok(Err(e)) => {
                    eprintln!(
                        "ERROR: Failed to deallocate page {} on disk: {:?}",
                        page_id, e
                    );
                    Ok(true)
                }
                Err(e) => {
                    eprintln!("ERROR: Timeout deallocating page {}: {}", page_id, e);
                    Ok(true)
                }
            }
        } else {
            Ok(false)
        }
    }

    fn allocate_frame(&self) -> Result<FrameId> {
        // 1. Check free list
        if let Some(frame_id) = self.free_list.write().pop_front() {
            println!("DEBUG: Allocated frame {} from free list.", frame_id);
            return Ok(frame_id);
        }

        // 2. Free list is empty, try to evict using replacer
        let frame_id_to_evict;
        let page_id_to_evict;
        let page_arc_to_evict;
        let is_dirty;

        {
            let mut replacer_guard = self.replacer.write(); // Acquire REPLACER WRITE LOCK
            if let Some(frame_id) = replacer_guard.evict() {
                frame_id_to_evict = frame_id;
                page_arc_to_evict = self.pool[frame_id].clone();

                // Temporarily lock page to read metadata safely
                {
                    let page_guard = page_arc_to_evict.read();
                    page_id_to_evict = page_guard.page_id;
                    is_dirty = page_guard.is_dirty;
                }

                // IMPORTANT: Remove from page table *before* releasing replacer lock
                if page_id_to_evict != INVALID_PAGE_ID {
                    if self.page_table.remove(&page_id_to_evict).is_none() {
                        // This shouldn't happen if replacer and page_table are consistent
                        error!(
                            "ERROR: Page {} evicted by replacer but not found in page table!",
                            page_id_to_evict
                        );
                        // Decide how to handle this inconsistency - maybe panic? For now, log and continue.
                    } else {
                        println!(
                            "DEBUG: Removed page {} from page table during eviction.",
                            page_id_to_evict
                        );
                    }
                } else {
                    println!("WARN: Evicted frame {} had INVALID_PAGE_ID", frame_id);
                }

                // Replacer lock is released when replacer_guard goes out of scope here
            } else {
                return Err(Error::Internal(
                    "Cannot allocate frame: buffer pool full and no page is evictable".to_string(),
                ));
            }
        } // Release REPLACER WRITE LOCK

        // 3. Flush the evicted page *if* it was dirty (Replacer lock is NOT held now)
        if is_dirty {
            println!(
                "DEBUG: Evicting dirty page {} from frame {}. Flushing.",
                page_id_to_evict, frame_id_to_evict
            );
            // Now acquire PAGE WRITE LOCK (no Replacer lock held)
            {
                let mut page_guard = page_arc_to_evict.write();
                // flush_page_internal needs mutable access to reset dirty flag
                // Or maybe flush_page_internal should take Bytes and not mutate?
                // Let's assume flush_page_internal takes the guard or necessary data
                // Simplified call: pass page_id and Arc, let internal handle locking if needed
                // Actually, let's fetch the data and reset dirty flag here.
                if page_guard.is_dirty {
                    // Double check dirty flag just before flush
                    let data_to_flush = page_guard.data_as_bytes();
                    page_guard.is_dirty = false;
                    // Drop guard before potential blocking I/O
                    drop(page_guard);

                    println!("DEBUG: Flushing page {}", page_id_to_evict);
                    let rx = self
                        .disk_scheduler
                        .schedule_write(page_id_to_evict, data_to_flush)?;
                    match rx.recv_timeout(Duration::from_secs(5)) {
                        Ok(Ok(())) => {
                            println!(
                                "DEBUG: Successfully flushed page {} during eviction",
                                page_id_to_evict
                            );
                        }
                        Ok(Err(e)) => {
                            eprintln!("ERROR: Disk write failed for page {} during eviction: {:?}. Data loss possible.", page_id_to_evict, e);
                            // What to do here? The frame is being reused...
                            // Maybe try to mark it dirty again? Risky.
                            // For now, just log the error.
                        }
                        Err(e) => {
                            eprintln!("ERROR: Timeout flushing page {} during eviction: {}. Data loss possible.", page_id_to_evict, e);
                            // Same problem as above.
                        }
                    }
                } else {
                    // Dirty flag was reset by another thread? Unlikely but possible.
                    drop(page_guard);
                }
            }
        } else {
            println!(
                "DEBUG: Evicting clean page {} from frame {}.",
                page_id_to_evict, frame_id_to_evict
            );
        }

        // 4. Return the evicted frame_id, ready for reuse
        println!("DEBUG: Allocated frame {} via eviction.", frame_id_to_evict);
        Ok(frame_id_to_evict)
    }

    fn flush_page_internal(
        &self,
        page_id: PageId,
        _frame_id: FrameId, // No longer needed?
        page_arc: Arc<RwLock<Page>>,
    ) -> Result<()> {
        let (data_to_flush, _was_dirty) = {
            let mut page_guard = page_arc.write(); // Acquire lock here
            if !page_guard.is_dirty {
                return Ok(()); // Return early if not dirty
            }
            let data = page_guard.data_as_bytes();
            page_guard.is_dirty = false; // Reset dirty flag
            (data, true)
        };

        // was_dirty is always true if we reach here due to the early return
        println!("DEBUG: Flushing page {}", page_id);
        let rx = self.disk_scheduler.schedule_write(page_id, data_to_flush)?;
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => {
                println!("DEBUG: Successfully flushed page {}", page_id);
                Ok(())
            }
            Ok(Err(e)) => {
                eprintln!("ERROR: Disk write failed for page {}: {:?}", page_id, e);
                // Should we mark the page dirty again? Maybe.
                // page_arc.write().is_dirty = true;
                Err(Error::Internal(format!("Disk write failed: {:?}", e)))
            }
            Err(e) => {
                eprintln!("ERROR: Timeout flushing page {}: {}", page_id, e);
                // Should we mark the page dirty again?
                // page_arc.write().is_dirty = true;
                Err(Error::Internal(format!("Disk write timeout: {}", e)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::b_plus_tree::buffer_pool_manager::BufferPoolManager;
    use crate::storage::b_plus_tree::disk::disk_manager::DiskManager;
    use crate::storage::b_plus_tree::disk::disk_scheduler::DiskScheduler;
    use crate::utils::cache::Replacer;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    pub fn test_buffer_pool_manager_new_page() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let disk_manager = Arc::new(DiskManager::try_new(temp_path).unwrap());
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let buffer_pool = BufferPoolManager::new(3, 2, disk_scheduler).unwrap();
        let page1 = buffer_pool.new_page().unwrap();
        let page1_id = page1.read().page_id;
        assert_eq!(buffer_pool.pool[0].read().page_id, page1_id,);
        assert_eq!(
            *buffer_pool.page_table.get(&page1.read().page_id).unwrap(),
            0
        );
        assert_eq!(buffer_pool.free_list.read().len(), 2);
        assert_eq!(buffer_pool.replacer.read().size(), 0);

        let page2 = buffer_pool.new_page().unwrap();
        let page2_id = page2.read().page_id;
        assert_eq!(buffer_pool.pool[1].read().page_id, page2_id,);

        let page3 = buffer_pool.new_page().unwrap();
        let page3_id = page3.read().page_id;
        assert_eq!(buffer_pool.pool[2].read().page_id, page3_id,);

        let page4 = buffer_pool.new_page();
        assert!(page4.is_err());

        drop(page1);

        let page5 = buffer_pool.new_page().unwrap();
        let page5_id = page5.read().page_id;
        assert_eq!(buffer_pool.pool[0].read().page_id, page5_id,);
    }

    #[test]
    pub fn test_buffer_pool_manager_unpin_page() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let disk_manager = Arc::new(DiskManager::try_new(temp_path).unwrap());
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let buffer_pool = BufferPoolManager::new(3, 2, disk_scheduler).unwrap();

        let page1 = buffer_pool.new_page().unwrap();
        let _page2 = buffer_pool.new_page().unwrap();
        let _page3 = buffer_pool.new_page().unwrap();
        let page4 = buffer_pool.new_page();
        assert!(page4.is_err());

        drop(page1);
        let page5 = buffer_pool.new_page();
        assert!(page5.is_ok());
    }

    #[test]
    pub fn test_buffer_pool_manager_fetch_page() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let disk_manager = Arc::new(DiskManager::try_new(temp_path).unwrap());
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let buffer_pool = BufferPoolManager::new(3, 2, disk_scheduler).unwrap();

        let page1 = buffer_pool.new_page().unwrap();
        let page1_id = page1.read().page_id;
        drop(page1);

        let page2 = buffer_pool.new_page().unwrap();
        let page2_id = page2.read().page_id;
        drop(page2);

        let page3 = buffer_pool.new_page().unwrap();
        drop(page3);

        let page = buffer_pool.fetch_page(page1_id).unwrap();
        assert_eq!(page.read().page_id, page1_id);
        drop(page);

        let page = buffer_pool.fetch_page(page2_id).unwrap();
        assert_eq!(page.read().page_id, page2_id);
        drop(page);

        assert_eq!(buffer_pool.replacer.read().size(), 3);
    }

    #[test]
    pub fn test_buffer_pool_manager_delete_page() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let disk_manager = Arc::new(DiskManager::try_new(temp_path).unwrap());
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let buffer_pool = BufferPoolManager::new(3, 2, disk_scheduler).unwrap();

        let page1 = buffer_pool.new_page().unwrap();
        let page1_id = page1.read().page_id;
        drop(page1);

        let page2 = buffer_pool.new_page().unwrap();
        drop(page2);

        let page3 = buffer_pool.new_page().unwrap();
        drop(page3);

        let res = buffer_pool.delete_page(page1_id).unwrap();
        assert!(res);
        assert_eq!(buffer_pool.pool.len(), 3);
        assert_eq!(buffer_pool.free_list.read().len(), 1);
        assert_eq!(buffer_pool.replacer.read().size(), 2);
        assert_eq!(buffer_pool.page_table.len(), 2);

        let page = buffer_pool.fetch_page(page1_id).unwrap();
        assert_eq!(page.read().page_id, page1_id);
    }
}
