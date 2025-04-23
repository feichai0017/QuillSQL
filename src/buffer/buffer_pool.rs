use bytes::Bytes;
use dashmap::DashMap;
use std::sync::RwLock;
use std::{collections::VecDeque, sync::Arc};

use crate::buffer::page::{Page, PageId, PAGE_SIZE};

use crate::buffer::PageRef;
use crate::catalog::SchemaRef;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::b_plus_tree::disk::disk_scheduler::DiskScheduler;
use crate::storage::codec::{
    BPlusTreeInternalPageCodec, BPlusTreeLeafPageCodec, BPlusTreePageCodec, TablePageCodec,
};
use crate::storage::{
    b_plus_tree::page::TablePage,
    b_plus_tree::page::{BPlusTreeInternalPage, BPlusTreeLeafPage, BPlusTreePage},
};

use crate::utils::cache::lru_k::LRUKReplacer;
use crate::utils::cache::Replacer;

pub type FrameId = usize;

pub const BUFFER_POOL_SIZE: usize = 1000;

#[derive(Debug)]
pub struct BufferPoolManager {
    pool: Vec<Arc<RwLock<Page>>>,
    pub replacer: Arc<RwLock<LRUKReplacer>>,
    pub disk_scheduler: Arc<DiskScheduler>,
    page_table: Arc<DashMap<PageId, FrameId>>,
    free_list: Arc<RwLock<VecDeque<FrameId>>>,
}
impl BufferPoolManager {
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
        }
    }

    pub fn new_page(&self) -> QuillSQLResult<PageRef> {
        if self.free_list.read().unwrap().is_empty() && self.replacer.read().unwrap().size() == 0 {
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
        let new_page = Page::new(new_page_id).with_pin_count(1u32);
        self.pool[frame_id].write().unwrap().replace(new_page);

        self.replacer.write().unwrap().record_access(frame_id)?;
        self.replacer
            .write()
            .unwrap()
            .set_evictable(frame_id, false)?;

        Ok(PageRef {
            page: self.pool[frame_id].clone(),
            page_table: self.page_table.clone(),
            replacer: self.replacer.clone(),
        })
    }

    pub fn fetch_page(&self, page_id: PageId) -> QuillSQLResult<PageRef> {
        if let Some(frame_id_ref) = self.page_table.get(&page_id) {
            let frame_id = *frame_id_ref;
            let page = self.pool[frame_id].clone();
            page.write().unwrap().pin_count += 1;
            self.replacer
                .write()
                .unwrap()
                .set_evictable(frame_id, false)?;
            Ok(PageRef {
                page,
                page_table: self.page_table.clone(),
                replacer: self.replacer.clone(),
            })
        } else {
            let frame_id = self.allocate_frame()?;

            let rx_read = self.disk_scheduler.schedule_read(page_id)?;
            let page_data_bytes = rx_read.recv().map_err(|e| {
                QuillSQLError::Internal(format!("Failed to receive page data: {}", e))
            })??;

            let mut page_data_array = [0u8; PAGE_SIZE];
            if page_data_bytes.len() == PAGE_SIZE {
                page_data_array.copy_from_slice(&page_data_bytes[..PAGE_SIZE]);
            } else {
                let copy_len = std::cmp::min(page_data_bytes.len(), PAGE_SIZE);
                page_data_array[..copy_len].copy_from_slice(&page_data_bytes[..copy_len]);
            }

            self.page_table.insert(page_id, frame_id);
            let new_page = Page::new(page_id)
                .with_pin_count(1u32)
                .with_data(page_data_array);
            self.pool[frame_id].write().unwrap().replace(new_page);

            self.replacer.write().unwrap().record_access(frame_id)?;
            self.replacer
                .write()
                .unwrap()
                .set_evictable(frame_id, false)?;

            Ok(PageRef {
                page: self.pool[frame_id].clone(),
                page_table: self.page_table.clone(),
                replacer: self.replacer.clone(),
            })
        }
    }

    pub fn fetch_table_page(
        &self,
        page_id: PageId,
        schema: SchemaRef,
    ) -> QuillSQLResult<(PageRef, TablePage)> {
        let page = self.fetch_page(page_id)?;
        let (table_page, _) = TablePageCodec::decode(page.read().unwrap().data(), schema.clone())?;
        Ok((page, table_page))
    }

    pub fn fetch_tree_page(
        &self,
        page_id: PageId,
        key_schema: SchemaRef,
    ) -> QuillSQLResult<(PageRef, BPlusTreePage)> {
        let page = self.fetch_page(page_id)?;
        let (tree_page, _) =
            BPlusTreePageCodec::decode(page.read().unwrap().data(), key_schema.clone())?;
        Ok((page, tree_page))
    }

    pub fn fetch_tree_internal_page(
        &self,
        page_id: PageId,
        key_schema: SchemaRef,
    ) -> QuillSQLResult<(PageRef, BPlusTreeInternalPage)> {
        let page = self.fetch_page(page_id)?;
        let (tree_internal_page, _) =
            BPlusTreeInternalPageCodec::decode(page.read().unwrap().data(), key_schema.clone())?;
        Ok((page, tree_internal_page))
    }

    pub fn fetch_tree_leaf_page(
        &self,
        page_id: PageId,
        key_schema: SchemaRef,
    ) -> QuillSQLResult<(PageRef, BPlusTreeLeafPage)> {
        let page = self.fetch_page(page_id)?;
        let (tree_leaf_page, _) =
            BPlusTreeLeafPageCodec::decode(page.read().unwrap().data(), key_schema.clone())?;
        Ok((page, tree_leaf_page))
    }

    pub fn flush_page(&self, page_id: PageId) -> QuillSQLResult<bool> {
        if let Some(frame_id_ref) = self.page_table.get(&page_id) {
            let frame_id = *frame_id_ref;
            let page_lock = self.pool[frame_id].clone();
            let page = page_lock.read().unwrap();

            let data_array = page.data();
            let data_bytes = Bytes::copy_from_slice(&data_array[..]);
            drop(page);

            let rx_write = self.disk_scheduler.schedule_write(page_id, data_bytes)?;
            rx_write.recv().map_err(|e| {
                QuillSQLError::Internal(format!("Failed to receive flush result: {}", e))
            })??;

            self.pool[frame_id].write().unwrap().is_dirty = false;
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
                    if self.pool[frame_id].read().unwrap().is_dirty {
                        self.flush_page(page_id)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn delete_page(&self, page_id: PageId) -> QuillSQLResult<bool> {
        if let Some(frame_id_lock) = self.page_table.get(&page_id) {
            let frame_id = *frame_id_lock;
            drop(frame_id_lock);

            let page = self.pool[frame_id].clone();
            if page.read().unwrap().pin_count > 0 {
                return Ok(false);
            }

            page.write().unwrap().destroy();
            self.page_table.remove(&page_id);
            self.free_list.write().unwrap().push_back(frame_id);
            self.replacer.write().unwrap().remove(frame_id);

            let rx_dealloc = self.disk_scheduler.schedule_deallocate(page_id)?;
            rx_dealloc.recv().map_err(|e| {
                QuillSQLError::Internal(format!("Failed to receive deallocate result: {}", e))
            })??;

            Ok(true)
        } else {
            let rx_dealloc = self.disk_scheduler.schedule_deallocate(page_id)?;
            rx_dealloc.recv().map_err(|e| {
                QuillSQLError::Internal(format!(
                    "Failed to receive deallocate result (page not in pool): {}",
                    e
                ))
            })??;
            Ok(true)
        }
    }

    fn allocate_frame(&self) -> QuillSQLResult<FrameId> {
        if let Some(frame_id) = self.free_list.write().unwrap().pop_front() {
            Ok(frame_id)
        } else if let Some(frame_id) = self.replacer.write().unwrap().evict() {
            let evicted_page_lock = self.pool[frame_id].clone();
            let evicted_page_read_guard = evicted_page_lock.read().unwrap();
            let evicted_page_id = evicted_page_read_guard.page_id;
            let is_dirty = evicted_page_read_guard.is_dirty;

            if is_dirty {
                let data_array = evicted_page_read_guard.data();
                let data_bytes = Bytes::copy_from_slice(&data_array[..]);
                drop(evicted_page_read_guard);

                let rx_write = self
                    .disk_scheduler
                    .schedule_write(evicted_page_id, data_bytes)?;
                rx_write.recv().map_err(|e| {
                    QuillSQLError::Internal(format!(
                        "Failed to receive evicted page flush result: {}",
                        e
                    ))
                })??;
            } else {
                drop(evicted_page_read_guard);
            }

            self.page_table.remove(&evicted_page_id);
            Ok(frame_id)
        } else {
            Err(QuillSQLError::Storage(
                "Cannot allocate free frame".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::b_plus_tree::disk::disk_scheduler::DiskScheduler;
    use crate::utils::cache::Replacer;
    use crate::{buffer::BufferPoolManager, storage::b_plus_tree::disk::disk_manager::DiskManager};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn setup_test_environment(
        num_pages: usize,
    ) -> (TempDir, Arc<BufferPoolManager>, Arc<DiskScheduler>) {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");
        let disk_manager = Arc::new(DiskManager::try_new(temp_path).unwrap());
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let buffer_pool = Arc::new(BufferPoolManager::new(num_pages, disk_scheduler.clone()));
        (temp_dir, buffer_pool, disk_scheduler)
    }

    #[test]
    pub fn test_buffer_pool_manager_new_page() {
        let (_temp_dir, buffer_pool, _ds) = setup_test_environment(3);

        let page1 = buffer_pool.new_page().unwrap();
        let page1_id = page1.read().unwrap().page_id;
        assert_eq!(buffer_pool.pool[0].read().unwrap().page_id, page1_id,);
        assert_eq!(
            *buffer_pool
                .page_table
                .get(&page1.read().unwrap().page_id)
                .unwrap(),
            0
        );
        assert_eq!(buffer_pool.free_list.read().unwrap().len(), 2);
        assert_eq!(buffer_pool.replacer.read().unwrap().size(), 0);

        let page2 = buffer_pool.new_page().unwrap();
        let page2_id = page2.read().unwrap().page_id;
        assert_eq!(buffer_pool.pool[1].read().unwrap().page_id, page2_id,);

        let page3 = buffer_pool.new_page().unwrap();
        let page3_id = page3.read().unwrap().page_id;
        assert_eq!(buffer_pool.pool[2].read().unwrap().page_id, page3_id,);

        let page4 = buffer_pool.new_page();
        assert!(page4.is_err());

        drop(page1);

        let page5 = buffer_pool.new_page().unwrap();
        let page5_id = page5.read().unwrap().page_id;
        assert_eq!(buffer_pool.pool[0].read().unwrap().page_id, page5_id,);
        assert!(buffer_pool.page_table.get(&page1_id).is_none());
    }

    #[test]
    pub fn test_buffer_pool_manager_unpin_page() {
        let (_temp_dir, buffer_pool, _ds) = setup_test_environment(3);

        let page1 = buffer_pool.new_page().unwrap();
        let _page2 = buffer_pool.new_page().unwrap();
        let _page3 = buffer_pool.new_page().unwrap();
        let page4 = buffer_pool.new_page();
        assert!(page4.is_err());

        drop(page1);
        assert_eq!(buffer_pool.replacer.read().unwrap().size(), 1);

        let page5 = buffer_pool.new_page();
        assert!(page5.is_ok());
        assert_eq!(buffer_pool.replacer.read().unwrap().size(), 0);
    }

    #[test]
    pub fn test_buffer_pool_manager_fetch_page() {
        let (_temp_dir, buffer_pool, _ds) = setup_test_environment(3);

        let page1 = buffer_pool.new_page().unwrap();
        let page1_id = page1.read().unwrap().page_id;
        drop(page1);

        let page2 = buffer_pool.new_page().unwrap();
        let page2_id = page2.read().unwrap().page_id;
        drop(page2);

        let page3 = buffer_pool.new_page().unwrap();
        let _page3_id = page3.read().unwrap().page_id;
        drop(page3);

        assert_eq!(buffer_pool.replacer.read().unwrap().size(), 3);

        let page = buffer_pool.fetch_page(page1_id).unwrap();
        assert_eq!(page.read().unwrap().page_id, page1_id);
        assert_eq!(page.read().unwrap().pin_count, 1);
        assert_eq!(buffer_pool.replacer.read().unwrap().size(), 2);
        drop(page);
        assert_eq!(buffer_pool.replacer.read().unwrap().size(), 3);

        let page = buffer_pool.fetch_page(page2_id).unwrap();
        assert_eq!(page.read().unwrap().page_id, page2_id);
        assert_eq!(page.read().unwrap().pin_count, 1);
        assert_eq!(buffer_pool.replacer.read().unwrap().size(), 2);
        drop(page);
        assert_eq!(buffer_pool.replacer.read().unwrap().size(), 3);
    }

    #[test]
    pub fn test_buffer_pool_manager_delete_page() {
        let (_temp_dir, buffer_pool, _ds) = setup_test_environment(3);

        let page1 = buffer_pool.new_page().unwrap();
        let page1_id = page1.read().unwrap().page_id;
        drop(page1);

        let page2 = buffer_pool.new_page().unwrap();
        let _ = page2.read().unwrap().page_id;
        drop(page2);

        let page3 = buffer_pool.new_page().unwrap();
        let _ = page3.read().unwrap().page_id;
        drop(page3);

        assert_eq!(buffer_pool.replacer.read().unwrap().size(), 3);
        assert_eq!(buffer_pool.page_table.len(), 3);
        assert_eq!(buffer_pool.free_list.read().unwrap().len(), 0);

        let res = buffer_pool.delete_page(page1_id).unwrap();
        assert!(res);
        assert_eq!(buffer_pool.pool.len(), 3);
        assert_eq!(buffer_pool.free_list.read().unwrap().len(), 1);
        assert_eq!(buffer_pool.replacer.read().unwrap().size(), 2);
        assert_eq!(buffer_pool.page_table.len(), 2);
        assert!(buffer_pool.page_table.get(&page1_id).is_none());

        let res_non_exist = buffer_pool.delete_page(page1_id).unwrap();
        assert!(res_non_exist);

        let page = buffer_pool.fetch_page(page1_id).unwrap();
        assert_eq!(page.read().unwrap().page_id, page1_id);
        assert!(page.read().unwrap().data().iter().all(|&b| b == 0));
    }
}
