//! Low-level buffer pool responsible for frame storage, page table, and disk I/O.

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use parking_lot::{Mutex, MutexGuard, RwLock};
use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::buffer::page::{PageId, INVALID_PAGE_ID, PAGE_SIZE};
use crate::config::BufferPoolConfig;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::Lsn;
use crate::storage::disk_scheduler::DiskScheduler;

pub type FrameId = usize;

pub const BUFFER_POOL_SIZE: usize = 5000;

#[derive(Debug, Default, Clone)]
pub struct FrameMeta {
    pub page_id: PageId,
    pub pin_count: u32,
    pub is_dirty: bool,
    pub lsn: Lsn,
}

#[derive(Debug)]
pub struct BufferPool {
    arena: Box<[UnsafeCell<u8>]>,
    locks: Vec<RwLock<()>>,
    meta: Vec<Mutex<FrameMeta>>,
    page_table: DashMap<PageId, FrameId>,
    free_list: Mutex<VecDeque<FrameId>>,
    disk_scheduler: Arc<DiskScheduler>,
}

unsafe impl Sync for BufferPool {}

impl BufferPool {
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
        let num_pages = config.buffer_pool_size;
        let mut free_list = VecDeque::with_capacity(num_pages);
        let mut meta = Vec::with_capacity(num_pages);
        let mut locks = Vec::with_capacity(num_pages);
        for frame_id in 0..num_pages {
            free_list.push_back(frame_id);
            meta.push(Mutex::new(FrameMeta::default()));
            locks.push(RwLock::new(()));
        }
        let mut arena_vec: Vec<UnsafeCell<u8>> = Vec::with_capacity(num_pages * PAGE_SIZE);
        arena_vec.resize_with(num_pages * PAGE_SIZE, || UnsafeCell::new(0u8));
        let arena = arena_vec.into_boxed_slice();

        Self {
            arena,
            locks,
            meta,
            page_table: DashMap::new(),
            free_list: Mutex::new(free_list),
            disk_scheduler,
        }
    }

    pub fn capacity(&self) -> usize {
        self.locks.len()
    }

    pub fn frame_lock(&self, frame_id: FrameId) -> &RwLock<()> {
        &self.locks[frame_id]
    }

    /// Returns an immutable view over the page bytes stored in `frame_id`.
    ///
    /// # Safety
    /// Caller must guarantee the frame lock is held for read (or write) and that the
    /// frame remains pinned for the duration of the returned slice. Violating the
    /// locking protocol allows concurrent mutation and results in undefined behavior.
    pub unsafe fn frame_slice(&self, frame_id: FrameId) -> &[u8] {
        let ptr = self.frame_ptr(frame_id) as *const u8;
        std::slice::from_raw_parts(ptr, PAGE_SIZE)
    }

    /// Returns a mutable view over the page bytes stored in `frame_id`.
    ///
    /// # Safety
    /// Caller must hold the frame's write lock and ensure no other references to the
    /// underlying slice exist. The buffer manager enforces this by acquiring the
    /// corresponding `RwLock` write guard before invoking this function. Misuse causes
    /// aliasing mutable references and leads to undefined behavior.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn frame_slice_mut(&self, frame_id: FrameId) -> &mut [u8] {
        let ptr = self.frame_ptr(frame_id);
        std::slice::from_raw_parts_mut(ptr, PAGE_SIZE)
    }

    /// Computes the raw pointer to the page bytes backing `frame_id`.
    ///
    /// # Safety
    /// Equivalent to `frame_slice`/`frame_slice_mut`; caller must uphold the same
    /// locking and lifetime guarantees before dereferencing the pointer.
    unsafe fn frame_ptr(&self, frame_id: FrameId) -> *mut u8 {
        self.arena.as_ptr().add(frame_id * PAGE_SIZE) as *mut u8
    }

    pub fn frame_meta(&self, frame_id: FrameId) -> MutexGuard<'_, FrameMeta> {
        self.meta[frame_id].lock()
    }

    pub fn clear_frame_meta(&self, frame_id: FrameId) {
        let mut meta = self.meta[frame_id].lock();
        *meta = FrameMeta::default();
    }

    pub fn pop_free_frame(&self) -> Option<FrameId> {
        self.free_list.lock().pop_front()
    }

    pub fn has_free_frame(&self) -> bool {
        !self.free_list.lock().is_empty()
    }

    pub fn push_free_frame(&self, frame_id: FrameId) {
        self.free_list.lock().push_back(frame_id);
    }

    pub fn insert_mapping(&self, page_id: PageId, frame_id: FrameId) {
        self.page_table.insert(page_id, frame_id);
    }

    pub fn remove_mapping_if(&self, page_id: PageId, frame_id: FrameId) -> bool {
        self.page_table
            .remove_if(&page_id, |_, current| *current == frame_id)
            .is_some()
    }

    pub fn remove_mapping(&self, page_id: PageId) {
        self.page_table.remove(&page_id);
    }

    pub fn lookup_frame(&self, page_id: PageId) -> Option<FrameId> {
        self.page_table.get(&page_id).map(|entry| *entry.value())
    }

    pub fn read_page_from_disk(&self, page_id: PageId) -> QuillSQLResult<BytesMut> {
        let rx = self.disk_scheduler.schedule_read(page_id)?;
        let data = rx
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("Channel disconnected: {}", e)))??;
        Ok(data)
    }

    pub fn load_page_into_frame(&self, page_id: PageId, frame_id: FrameId) -> QuillSQLResult<()> {
        let page_bytes = self.read_page_from_disk(page_id)?;
        let slice = unsafe { self.frame_slice_mut(frame_id) };
        let len = PAGE_SIZE.min(page_bytes.len());
        slice[..len].copy_from_slice(&page_bytes[..len]);
        if len < PAGE_SIZE {
            slice[len..].fill(0);
        }
        let mut meta = self.meta[frame_id].lock();
        meta.page_id = page_id;
        meta.is_dirty = false;
        meta.pin_count = 0;
        meta.lsn = 0;
        Ok(())
    }

    pub fn write_page_to_disk(&self, page_id: PageId, bytes: Bytes) -> QuillSQLResult<()> {
        self.disk_scheduler
            .schedule_write(page_id, bytes)?
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("Channel disconnected: {}", e)))??;
        Ok(())
    }

    pub fn allocate_page_id(&self) -> QuillSQLResult<PageId> {
        let rx = self.disk_scheduler.schedule_allocate()?;
        let page_id = rx
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("Channel disconnected: {}", e)))??;
        if page_id == INVALID_PAGE_ID {
            return Err(QuillSQLError::Internal(
                "DiskScheduler returned INVALID_PAGE_ID".to_string(),
            ));
        }
        Ok(page_id)
    }

    pub fn disk_scheduler(&self) -> Arc<DiskScheduler> {
        self.disk_scheduler.clone()
    }

    pub fn reset_frame(&self, frame_id: FrameId) {
        unsafe {
            self.frame_slice_mut(frame_id).fill(0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk_manager::DiskManager;
    use tempfile::TempDir;

    fn setup_pool(num_pages: usize) -> (TempDir, Arc<DiskScheduler>, BufferPool) {
        let temp_dir = TempDir::new().unwrap();
        let disk_manager = Arc::new(DiskManager::try_new(temp_dir.path().join("pool.db")).unwrap());
        let scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let mut config = BufferPoolConfig::default();
        config.buffer_pool_size = num_pages;
        let pool = BufferPool::new_with_config(config, scheduler.clone());
        (temp_dir, scheduler, pool)
    }

    #[test]
    fn load_page_into_frame_populates_arena_and_meta() {
        let (_tmp, scheduler, pool) = setup_pool(4);
        let rx_alloc = scheduler.schedule_allocate().unwrap();
        let page_id = rx_alloc.recv().unwrap().unwrap();

        let pattern = Bytes::copy_from_slice(&vec![0xAA; PAGE_SIZE]);
        scheduler
            .schedule_write(page_id, pattern.clone())
            .unwrap()
            .recv()
            .unwrap()
            .unwrap();

        let frame_id = pool.pop_free_frame().expect("free frame");
        pool.load_page_into_frame(page_id, frame_id).unwrap();

        {
            let meta = pool.frame_meta(frame_id);
            assert_eq!(meta.page_id, page_id);
            assert_eq!(meta.pin_count, 0);
            assert!(!meta.is_dirty);
            assert_eq!(meta.lsn, 0);
        }

        let data = unsafe { pool.frame_slice(frame_id) };
        assert_eq!(data, pattern.as_ref());
    }

    #[test]
    fn write_page_to_disk_persists_arena_bytes() {
        let (_tmp, scheduler, pool) = setup_pool(4);
        let page_id = scheduler
            .schedule_allocate()
            .unwrap()
            .recv()
            .unwrap()
            .unwrap();

        let frame_id = pool.pop_free_frame().expect("free frame");
        unsafe {
            pool.frame_slice_mut(frame_id).fill(0x3C);
        }
        let payload = Bytes::copy_from_slice(unsafe { pool.frame_slice(frame_id) });
        pool.write_page_to_disk(page_id, payload.clone()).unwrap();

        let read_back = scheduler
            .schedule_read(page_id)
            .unwrap()
            .recv()
            .unwrap()
            .unwrap();
        assert!(read_back.iter().all(|b| *b == 0x3C));
    }

    #[test]
    fn reset_frame_clears_data_and_meta() {
        let (_tmp, scheduler, pool) = setup_pool(2);
        let page_id = scheduler
            .schedule_allocate()
            .unwrap()
            .recv()
            .unwrap()
            .unwrap();
        let frame_id = pool.pop_free_frame().expect("free frame");

        {
            let mut meta = pool.frame_meta(frame_id);
            meta.page_id = page_id;
            meta.pin_count = 5;
            meta.is_dirty = true;
            meta.lsn = 99;
        }
        unsafe {
            pool.frame_slice_mut(frame_id).fill(0x55);
        }

        pool.reset_frame(frame_id);
        pool.clear_frame_meta(frame_id);

        let meta = pool.frame_meta(frame_id);
        assert_eq!(meta.page_id, INVALID_PAGE_ID);
        assert_eq!(meta.pin_count, 0);
        assert!(!meta.is_dirty);
        assert_eq!(meta.lsn, 0);
        drop(meta);

        assert!(unsafe { pool.frame_slice(frame_id) }
            .iter()
            .all(|b| *b == 0));
    }

    #[test]
    fn page_table_insert_lookup_and_remove() {
        let (_tmp, scheduler, pool) = setup_pool(2);
        let page_id = scheduler
            .schedule_allocate()
            .unwrap()
            .recv()
            .unwrap()
            .unwrap();
        let frame_id = 0;

        pool.insert_mapping(page_id, frame_id);
        assert_eq!(pool.lookup_frame(page_id), Some(frame_id));
        assert!(pool.remove_mapping_if(page_id, frame_id));
        assert!(pool.lookup_frame(page_id).is_none());

        pool.insert_mapping(page_id, frame_id);
        pool.remove_mapping(page_id);
        assert!(pool.lookup_frame(page_id).is_none());
    }
}
