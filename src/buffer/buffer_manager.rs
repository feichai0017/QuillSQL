//! BufferManager coordinates replacement, dirty tracking, and WAL with a shared BufferPool.

use std::sync::Arc;

use bytes::Bytes;
use dashmap::{DashMap, DashSet};
use parking_lot::{Mutex, RwLock};

use crate::buffer::buffer_pool::{BufferPool, FrameId, FrameMeta};
use crate::buffer::page::{self, PageId, ReadPageGuard, WritePageGuard, INVALID_PAGE_ID};
use crate::catalog::SchemaRef;
use crate::config::BufferPoolConfig;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::{Lsn, WalManager};
use crate::storage::codec::{
    BPlusTreeHeaderPageCodec, BPlusTreeInternalPageCodec, BPlusTreeLeafPageCodec,
    BPlusTreePageCodec, TablePageCodec,
};
use crate::storage::disk_scheduler::DiskScheduler;
use crate::storage::page::{
    BPlusTreeHeaderPage, BPlusTreeInternalPage, BPlusTreeLeafPage, BPlusTreePage, TablePage,
};
use crate::utils::cache::lru_k::LRUKReplacer;
use crate::utils::cache::tiny_lfu::TinyLFU;
use crate::utils::cache::Replacer;

#[derive(Debug)]
pub struct BufferManager {
    pool: Arc<BufferPool>,
    replacer: Arc<RwLock<LRUKReplacer>>,
    inflight_loads: DashMap<PageId, Arc<Mutex<()>>>,
    tiny_lfu: Option<Arc<RwLock<TinyLFU>>>,
    dirty_pages: DashSet<PageId>,
    dirty_page_table: DashMap<PageId, Lsn>,
    wal_manager: Arc<RwLock<Option<Arc<WalManager>>>>,
}

impl BufferManager {
    pub fn new(num_pages: usize, disk_scheduler: Arc<DiskScheduler>) -> Self {
        Self::new_with_config(
            BufferPoolConfig {
                buffer_pool_size: num_pages,
                ..Default::default()
            },
            disk_scheduler,
        )
    }

    pub fn new_with_config(config: BufferPoolConfig, disk_scheduler: Arc<DiskScheduler>) -> Self {
        let pool = Arc::new(BufferPool::new_with_config(config, disk_scheduler));
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(pool.capacity())));
        let tiny_lfu = if config.tiny_lfu_enable {
            Some(Arc::new(RwLock::new(TinyLFU::new(
                pool.capacity().next_power_of_two(),
                config.tiny_lfu_counters,
            ))))
        } else {
            None
        };

        Self {
            pool,
            replacer,
            inflight_loads: DashMap::new(),
            tiny_lfu,
            dirty_pages: DashSet::new(),
            dirty_page_table: DashMap::new(),
            wal_manager: Arc::new(RwLock::new(None)),
        }
    }

    pub fn buffer_pool(&self) -> Arc<BufferPool> {
        self.pool.clone()
    }

    pub fn replacer_arc(&self) -> Arc<RwLock<LRUKReplacer>> {
        self.replacer.clone()
    }

    pub fn set_wal_manager(&self, wal_manager: Arc<WalManager>) {
        *self.wal_manager.write() = Some(wal_manager);
    }

    pub fn wal_manager(&self) -> Option<Arc<WalManager>> {
        self.wal_manager.read().clone()
    }

    pub fn dirty_page_ids(&self) -> Vec<PageId> {
        self.dirty_pages.iter().map(|entry| *entry.key()).collect()
    }

    pub fn dirty_page_table_snapshot(&self) -> Vec<(PageId, Lsn)> {
        self.dirty_page_table
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect()
    }

    fn note_dirty_page(&self, page_id: PageId, rec_lsn: Lsn) {
        self.dirty_pages.insert(page_id);
        self.dirty_page_table.entry(page_id).or_insert(rec_lsn);
    }

    pub fn new_page(self: &Arc<Self>) -> QuillSQLResult<WritePageGuard> {
        if !self.pool.has_free_frame() && self.replacer.read().size() == 0 {
            return Err(QuillSQLError::Storage(
                "Cannot new page because buffer pool is full and no page to evict".to_string(),
            ));
        }

        let frame_id = self.allocate_frame()?;
        let page_id = self.pool.allocate_page_id()?;
        self.pool.insert_mapping(page_id, frame_id);

        {
            let mut meta = self.pool.frame_meta(frame_id);
            meta.page_id = page_id;
            meta.pin_count = 1;
            meta.is_dirty = false;
            meta.lsn = 0;
        }

        self.pool.reset_frame(frame_id);
        self.replacer_record_access(frame_id)?;
        self.mark_non_evictable(frame_id)?;
        Ok(page::new_write_guard(Arc::clone(self), frame_id))
    }

    pub fn fetch_page_read(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<ReadPageGuard> {
        if page_id == INVALID_PAGE_ID {
            return Err(QuillSQLError::Storage(
                "fetch_page_read: invalid page id".to_string(),
            ));
        }

        let frame_id = self.ensure_frame(page_id)?;
        {
            let mut meta = self.pool.frame_meta(frame_id);
            meta.pin_count += 1;
        }
        self.replacer_record_access(frame_id)?;
        self.mark_non_evictable(frame_id)?;
        Ok(page::new_read_guard(Arc::clone(self), frame_id))
    }

    pub fn fetch_page_write(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<WritePageGuard> {
        if page_id == INVALID_PAGE_ID {
            return Err(QuillSQLError::Storage(
                "fetch_page_write: invalid page id".to_string(),
            ));
        }

        let frame_id = self.ensure_frame(page_id)?;
        {
            let mut meta = self.pool.frame_meta(frame_id);
            meta.pin_count += 1;
        }
        self.replacer_record_access(frame_id)?;
        self.mark_non_evictable(frame_id)?;
        Ok(page::new_write_guard(Arc::clone(self), frame_id))
    }

    pub fn complete_unpin(
        &self,
        page_id: PageId,
        is_dirty: bool,
        rec_lsn_hint: Option<Lsn>,
    ) -> QuillSQLResult<()> {
        if let Some(frame_id) = self.pool.lookup_frame(page_id) {
            let mut meta = self.pool.frame_meta(frame_id);
            if meta.pin_count > 0 {
                meta.pin_count -= 1;
            }
            if is_dirty {
                meta.is_dirty = true;
                if let Some(lsn) = rec_lsn_hint {
                    meta.lsn = lsn;
                }
                self.note_dirty_page(page_id, rec_lsn_hint.unwrap_or(meta.lsn));
            }
            if meta.pin_count == 0 {
                self.mark_evictable(frame_id)?;
            }
        }
        Ok(())
    }

    pub fn fetch_table_page(
        self: &Arc<Self>,
        page_id: PageId,
        schema: SchemaRef,
    ) -> QuillSQLResult<(ReadPageGuard, TablePage)> {
        let guard = self.fetch_page_read(page_id)?;
        let (page, _) = TablePageCodec::decode(guard.data(), schema)?;
        Ok((guard, page))
    }

    pub fn fetch_tree_page(
        self: &Arc<Self>,
        page_id: PageId,
        key_schema: SchemaRef,
    ) -> QuillSQLResult<(ReadPageGuard, BPlusTreePage)> {
        let guard = self.fetch_page_read(page_id)?;
        let (page, _) = BPlusTreePageCodec::decode(guard.data(), key_schema.clone())?;
        Ok((guard, page))
    }

    pub fn fetch_tree_internal_page(
        self: &Arc<Self>,
        page_id: PageId,
        key_schema: SchemaRef,
    ) -> QuillSQLResult<(ReadPageGuard, BPlusTreeInternalPage)> {
        let guard = self.fetch_page_read(page_id)?;
        let (page, _) = BPlusTreeInternalPageCodec::decode(guard.data(), key_schema.clone())?;
        Ok((guard, page))
    }

    pub fn fetch_tree_leaf_page(
        self: &Arc<Self>,
        page_id: PageId,
        key_schema: SchemaRef,
    ) -> QuillSQLResult<(ReadPageGuard, BPlusTreeLeafPage)> {
        let guard = self.fetch_page_read(page_id)?;
        let (page, _) = BPlusTreeLeafPageCodec::decode(guard.data(), key_schema.clone())?;
        Ok((guard, page))
    }

    pub fn fetch_header_page(
        self: &Arc<Self>,
        page_id: PageId,
    ) -> QuillSQLResult<(ReadPageGuard, BPlusTreeHeaderPage)> {
        let guard = self.fetch_page_read(page_id)?;
        let (header, _) = BPlusTreeHeaderPageCodec::decode(guard.data())?;
        Ok((guard, header))
    }

    pub fn prefetch_page(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<()> {
        if let Ok(g) = self.fetch_page_read(page_id) {
            drop(g);
        }
        Ok(())
    }

    pub fn flush_page(&self, page_id: PageId) -> QuillSQLResult<bool> {
        let Some(frame_id) = self.pool.lookup_frame(page_id) else {
            return Ok(false);
        };
        let meta = self.pool.frame_meta(frame_id);
        if !meta.is_dirty {
            self.dirty_pages.remove(&page_id);
            self.dirty_page_table.remove(&page_id);
            return Ok(false);
        }
        let lsn = meta.lsn;
        drop(meta);
        self.ensure_wal_durable(lsn)?;
        let bytes = {
            let _lock = self.pool.frame_lock(frame_id).read();
            let slice = unsafe { self.pool.frame_slice(frame_id) };
            Bytes::copy_from_slice(slice)
        };
        self.pool.write_page_to_disk(page_id, bytes)?;
        let mut meta = self.pool.frame_meta(frame_id);
        meta.is_dirty = false;
        self.dirty_pages.remove(&page_id);
        self.dirty_page_table.remove(&page_id);
        Ok(true)
    }

    pub fn flush_all_pages(&self) -> QuillSQLResult<()> {
        if let Some(wal) = self.wal_manager.read().clone() {
            wal.flush(None)?;
        }
        let dirty_ids: Vec<PageId> = self.dirty_pages.iter().map(|entry| *entry.key()).collect();
        for page_id in dirty_ids {
            let _ = self.flush_page(page_id)?;
        }
        Ok(())
    }

    pub fn delete_page(&self, page_id: PageId) -> QuillSQLResult<bool> {
        let (guard, created_here) = if let Some(existing) = self.inflight_loads.get(&page_id) {
            (existing.clone(), false)
        } else {
            let arc = Arc::new(Mutex::new(()));
            self.inflight_loads.insert(page_id, arc.clone());
            (arc, true)
        };
        let lock = guard.lock();
        let result = self.delete_page_inner(page_id);
        drop(lock);
        if created_here {
            self.inflight_loads.remove(&page_id);
        }
        result
    }

    fn delete_page_inner(&self, page_id: PageId) -> QuillSQLResult<bool> {
        if let Some(frame_id) = self.pool.lookup_frame(page_id) {
            let lock = self.pool.frame_lock(frame_id);
            let guard = match lock.try_write() {
                Some(g) => g,
                None => return Ok(false),
            };
            drop(guard);
            let meta = self.pool.frame_meta(frame_id);
            if meta.page_id != page_id {
                drop(meta);
                self.pool.remove_mapping_if(page_id, frame_id);
                return self.delete_page_inner(page_id);
            }
            if meta.pin_count > 0 {
                drop(meta);
                return Ok(false);
            }
            if !self.pool.remove_mapping_if(page_id, frame_id) {
                drop(meta);
                return self.delete_page_inner(page_id);
            }
            drop(meta);
            self.pool.reset_frame(frame_id);
            self.dirty_pages.remove(&page_id);
            self.dirty_page_table.remove(&page_id);
            {
                let mut meta = self.pool.frame_meta(frame_id);
                *meta = FrameMeta::default();
            }
            {
                let mut rep = self.replacer.write();
                let _ = rep.set_evictable(frame_id, true);
                rep.remove(frame_id);
            }
            self.pool.push_free_frame(frame_id);
            self.pool
                .disk_scheduler()
                .schedule_deallocate(page_id)?
                .recv()
                .map_err(|e| QuillSQLError::Internal(format!("Channel disconnected: {}", e)))??;
            Ok(true)
        } else {
            self.pool
                .disk_scheduler()
                .schedule_deallocate(page_id)?
                .recv()
                .map_err(|e| QuillSQLError::Internal(format!("Channel disconnected: {}", e)))??;
            Ok(true)
        }
    }

    fn ensure_frame(&self, page_id: PageId) -> QuillSQLResult<FrameId> {
        if let Some(frame_id) = self.pool.lookup_frame(page_id) {
            self.replacer_record_access(frame_id)?;
            if let Some(lfu) = &self.tiny_lfu {
                lfu.write().admit_record(page_id as u64);
            }
            return Ok(frame_id);
        }

        let (guard, created_here) = if let Some(existing) = self.inflight_loads.get(&page_id) {
            (existing.clone(), false)
        } else {
            let arc = Arc::new(Mutex::new(()));
            self.inflight_loads.insert(page_id, arc.clone());
            (arc, true)
        };
        let _lock = guard.lock();

        if let Some(frame_id) = self.pool.lookup_frame(page_id) {
            if created_here {
                self.inflight_loads.remove(&page_id);
            }
            self.replacer_record_access(frame_id)?;
            if let Some(lfu) = &self.tiny_lfu {
                lfu.write().admit_record(page_id as u64);
            }
            return Ok(frame_id);
        }

        if let Some(lfu) = &self.tiny_lfu {
            let estimate = lfu.read().estimate(page_id as u64);
            if estimate == 0 && !self.pool.has_free_frame() && self.replacer.read().size() == 0 {
                if created_here {
                    self.inflight_loads.remove(&page_id);
                }
                return Err(QuillSQLError::Storage(
                    "Cannot allocate frame: admission denied and no space".to_string(),
                ));
            }
        }

        let frame_id = self.allocate_frame()?;
        self.pool.load_page_into_frame(page_id, frame_id)?;
        self.pool.insert_mapping(page_id, frame_id);
        {
            let mut meta = self.pool.frame_meta(frame_id);
            meta.page_id = page_id;
            meta.pin_count = 0;
            meta.is_dirty = false;
            meta.lsn = 0;
        }
        if let Some(lfu) = &self.tiny_lfu {
            lfu.write().admit_record(page_id as u64);
        }
        if created_here {
            self.inflight_loads.remove(&page_id);
        }
        self.replacer_record_access(frame_id)?;
        Ok(frame_id)
    }

    fn allocate_frame(&self) -> QuillSQLResult<FrameId> {
        if let Some(frame_id) = self.pool.pop_free_frame() {
            return Ok(frame_id);
        }
        self.evict_victim_frame()
    }

    fn replacer_record_access(&self, frame_id: FrameId) -> QuillSQLResult<()> {
        let mut rep = self.replacer.write();
        let _ = rep.record_access(frame_id);
        Ok(())
    }

    fn evict_victim_frame(&self) -> QuillSQLResult<FrameId> {
        loop {
            let victim = {
                let mut rep = self.replacer.write();
                match rep.evict() {
                    Some(frame_id) => frame_id,
                    None => {
                        return Err(QuillSQLError::Storage(
                            "Cannot allocate frame: buffer pool is full".to_string(),
                        ))
                    }
                }
            };

            let (page_id, pin_count, is_dirty, lsn) = {
                let meta = self.pool.frame_meta(victim);
                (meta.page_id, meta.pin_count, meta.is_dirty, meta.lsn)
            };

            if pin_count > 0 {
                let mut rep = self.replacer.write();
                let _ = rep.record_access(victim);
                let _ = rep.set_evictable(victim, false);
                continue;
            }

            if page_id != INVALID_PAGE_ID {
                if is_dirty {
                    self.ensure_wal_durable(lsn)?;
                    let bytes = Bytes::copy_from_slice(unsafe { self.pool.frame_slice(victim) });
                    self.pool.write_page_to_disk(page_id, bytes)?;
                    self.dirty_pages.remove(&page_id);
                    self.dirty_page_table.remove(&page_id);
                }
                self.pool.remove_mapping(page_id);
            }

            self.pool.clear_frame_meta(victim);
            self.pool.reset_frame(victim);
            return Ok(victim);
        }
    }

    fn mark_evictable(&self, frame_id: FrameId) -> QuillSQLResult<()> {
        let mut rep = self.replacer.write();
        let _ = rep.set_evictable(frame_id, true);
        Ok(())
    }

    fn mark_non_evictable(&self, frame_id: FrameId) -> QuillSQLResult<()> {
        let mut rep = self.replacer.write();
        let _ = rep.set_evictable(frame_id, false);
        Ok(())
    }

    fn ensure_wal_durable(&self, lsn: Lsn) -> QuillSQLResult<()> {
        if lsn == 0 {
            return Ok(());
        }
        if let Some(wal) = self.wal_manager.read().clone() {
            if lsn > wal.durable_lsn() {
                wal.flush(Some(lsn))?;
                if wal.durable_lsn() < lsn {
                    return Err(QuillSQLError::Internal(format!(
                        "Flush blocked: page_lsn={} > durable_lsn={}",
                        lsn,
                        wal.durable_lsn()
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn clone_arc(self: &Arc<Self>) -> Arc<Self> {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tempfile::TempDir;

    fn setup_manager(num_pages: usize) -> (TempDir, Arc<BufferManager>) {
        let temp_dir = TempDir::new().unwrap();
        let db_file = temp_dir.path().join("test.db");
        let disk_manager = Arc::new(DiskManager::try_new(db_file).unwrap());
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let manager = Arc::new(BufferManager::new(num_pages, disk_scheduler));
        (temp_dir, manager)
    }

    #[test]
    fn new_page_initializes_frame() {
        let (_tmp, manager) = setup_manager(2);
        let guard = manager.new_page().unwrap();
        let page_id = guard.page_id();
        let frame_id = guard.frame_id();

        // Page should be zeroed
        assert!(guard.data().iter().all(|b| *b == 0));
        assert!(!guard.is_dirty());
        assert_eq!(guard.lsn(), 0);

        drop(guard);

        let meta = manager.buffer_pool().frame_meta(frame_id).clone();
        assert_eq!(meta.page_id, page_id);
        assert_eq!(meta.pin_count, 0);
        assert!(!meta.is_dirty);
    }

    #[test]
    fn fetch_page_read_increments_pin_and_resets_on_drop() {
        let (_tmp, manager) = setup_manager(2);
        let guard = manager.new_page().unwrap();
        let page_id = guard.page_id();
        let frame_id = guard.frame_id();
        drop(guard);

        {
            let read_guard = manager.fetch_page_read(page_id).unwrap();
            assert_eq!(read_guard.pin_count(), 1);
            assert_eq!(read_guard.frame_id(), frame_id);
        }

        let meta = manager.buffer_pool().frame_meta(frame_id).clone();
        assert_eq!(meta.pin_count, 0);
    }

    #[test]
    fn fetch_page_write_marks_dirty_and_tracks_lsn() {
        let (_tmp, manager) = setup_manager(2);
        let mut guard = manager.new_page().unwrap();
        let page_id = guard.page_id();
        guard.data_mut()[0] = 7;
        guard.set_lsn(99);
        guard.mark_dirty();
        drop(guard);

        let mut write_guard = manager.fetch_page_write(page_id).unwrap();
        assert!(write_guard.is_dirty());
        assert_eq!(write_guard.lsn(), 99);
        write_guard.data_mut()[1] = 8;
        drop(write_guard);

        let meta = manager
            .buffer_pool()
            .frame_meta(manager.buffer_pool().lookup_frame(page_id).unwrap())
            .clone();
        assert!(meta.is_dirty);
        assert_eq!(meta.lsn, 99);
        assert_eq!(meta.pin_count, 0);
    }

    #[test]
    fn flush_page_writes_back_and_clears_dirty_flag() {
        let (_tmp, manager) = setup_manager(2);
        let mut guard = manager.new_page().unwrap();
        let page_id = guard.page_id();
        guard.data_mut()[0] = 42;
        guard.set_lsn(123);
        guard.mark_dirty();
        drop(guard);

        assert!(manager.flush_page(page_id).unwrap());

        let meta = manager
            .buffer_pool()
            .frame_meta(manager.buffer_pool().lookup_frame(page_id).unwrap())
            .clone();
        assert!(!meta.is_dirty);
    }

    #[test]
    fn delete_page_releases_frame() {
        let (_tmp, manager) = setup_manager(2);
        let page_id = {
            let guard = manager.new_page().unwrap();
            guard.page_id()
        };

        assert!(manager.delete_page(page_id).unwrap());
        assert!(manager.buffer_pool().lookup_frame(page_id).is_none());
        assert!(manager.buffer_pool().has_free_frame());

        // Ensure subsequent allocation succeeds and pool remains operational
        let new_guard = manager.new_page().unwrap();
        assert!(new_guard.frame_id() < manager.buffer_pool().capacity());
    }

    #[test]
    fn concurrent_reads_do_not_leak_pins() {
        const THREADS: usize = 8;
        let (_tmp, manager) = setup_manager(4);
        let (page_id, frame_id) = {
            let mut guard = manager.new_page().unwrap();
            guard.data_mut()[0] = 42;
            (guard.page_id(), guard.frame_id())
        };

        let barrier = Arc::new(Barrier::new(THREADS));
        let mut handles = Vec::with_capacity(THREADS);
        for _ in 0..THREADS {
            let mgr = manager.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..50 {
                    let guard = mgr.fetch_page_read(page_id).expect("read page");
                    assert_eq!(guard.data()[0], 42);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let pool = manager.buffer_pool();
        let meta = pool.frame_meta(frame_id);
        assert_eq!(meta.pin_count, 0);
        assert_eq!(meta.page_id, page_id);
    }

    #[test]
    fn concurrent_writes_mark_dirty_and_flush_once() {
        const THREADS: usize = 4;
        let (_tmp, manager) = setup_manager(4);
        let (page_id, frame_id) = {
            let guard = manager.new_page().unwrap();
            (guard.page_id(), guard.frame_id())
        };

        let barrier = Arc::new(Barrier::new(THREADS));
        let mut handles = Vec::with_capacity(THREADS);
        for tid in 0..THREADS {
            let mgr = manager.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                let lsn = (tid as Lsn) + 1;
                barrier.wait();
                for _ in 0..25 {
                    let mut guard = mgr.fetch_page_write(page_id).expect("write guard");
                    guard.data_mut()[tid] = (tid as u8) + 1;
                    guard.set_lsn(lsn);
                    guard.mark_dirty();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        {
            let pool = manager.buffer_pool();
            let meta = pool.frame_meta(frame_id);
            assert!(meta.is_dirty);
            assert_eq!(meta.pin_count, 0);
            assert_eq!(meta.page_id, page_id);
        }

        assert!(manager.flush_page(page_id).unwrap());
        {
            let pool = manager.buffer_pool();
            let meta = pool.frame_meta(frame_id);
            assert!(!meta.is_dirty);
            assert_eq!(meta.pin_count, 0);
        }

        let read_back = manager
            .buffer_pool()
            .disk_scheduler()
            .schedule_read(page_id)
            .unwrap()
            .recv()
            .unwrap()
            .unwrap();
        for tid in 0..THREADS {
            assert_eq!(read_back[tid], (tid as u8) + 1);
        }
    }
}
