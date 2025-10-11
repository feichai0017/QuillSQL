use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use parking_lot::{Mutex, RwLock};

use super::super::standard::page::{INVALID_PAGE_ID, PAGE_SIZE};
use crate::buffer::PageId;
use crate::config::BufferPoolConfig;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::Lsn;
use crate::storage::disk_scheduler::DiskScheduler;

pub type FrameId = usize;

#[derive(Debug, Clone)]
pub struct LeanFrameMeta {
    pub page_id: PageId,
    pub pin_count: u32,
    pub is_dirty: bool,
    pub lsn: Lsn,
}

impl Default for LeanFrameMeta {
    fn default() -> Self {
        Self {
            page_id: INVALID_PAGE_ID,
            pin_count: 0,
            is_dirty: false,
            lsn: 0,
        }
    }
}

#[derive(Debug)]
pub struct LeanFrameMetaAtomic {
    page_id: AtomicU32,
    pin_count: AtomicU32,
    dirty: AtomicBool,
    lsn: AtomicU64,
}

impl LeanFrameMetaAtomic {
    pub fn new() -> Self {
        Self {
            page_id: AtomicU32::new(INVALID_PAGE_ID),
            pin_count: AtomicU32::new(0),
            dirty: AtomicBool::new(false),
            lsn: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> LeanFrameMeta {
        LeanFrameMeta {
            page_id: self.page_id.load(Ordering::Acquire),
            pin_count: self.pin_count.load(Ordering::Acquire),
            is_dirty: self.dirty.load(Ordering::Acquire),
            lsn: self.lsn.load(Ordering::Acquire),
        }
    }

    pub fn reset(&self) {
        self.page_id.store(INVALID_PAGE_ID, Ordering::Release);
        self.pin_count.store(0, Ordering::Release);
        self.dirty.store(false, Ordering::Release);
        self.lsn.store(0, Ordering::Release);
    }

    pub fn set_page_id(&self, page_id: PageId) {
        self.page_id.store(page_id, Ordering::Release);
    }

    pub fn page_id(&self) -> PageId {
        self.page_id.load(Ordering::Acquire)
    }

    pub fn set_lsn(&self, lsn: Lsn) {
        self.lsn.store(lsn, Ordering::Release);
    }

    pub fn lsn(&self) -> Lsn {
        self.lsn.load(Ordering::Acquire)
    }

    pub fn pin(&self) -> u32 {
        self.pin_count.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn set_pin_count(&self, count: u32) {
        self.pin_count.store(count, Ordering::Release);
    }

    pub fn unpin(&self) -> u32 {
        let previous = self.pin_count.fetch_sub(1, Ordering::AcqRel);
        if previous == 0 {
            // Restore count and return zero; caller will handle.
            self.pin_count.fetch_add(1, Ordering::AcqRel);
            0
        } else {
            previous - 1
        }
    }

    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    pub fn clear_dirty(&self) {
        self.dirty.store(false, Ordering::Release);
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub struct LeanBufferPool {
    arena: Box<[UnsafeCell<u8>]>,
    locks: Vec<RwLock<()>>,
    meta: Vec<LeanFrameMetaAtomic>,
    free_list: Mutex<VecDeque<FrameId>>,
    disk_scheduler: Arc<DiskScheduler>,
}

unsafe impl Sync for LeanBufferPool {}

impl LeanBufferPool {
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
            meta.push(LeanFrameMetaAtomic::new());
            locks.push(RwLock::new(()));
        }

        let mut arena_vec: Vec<UnsafeCell<u8>> = Vec::with_capacity(num_pages * PAGE_SIZE);
        arena_vec.resize_with(num_pages * PAGE_SIZE, || UnsafeCell::new(0u8));
        let arena = arena_vec.into_boxed_slice();

        Self {
            arena,
            locks,
            meta,
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

    pub fn frame_meta(&self, frame_id: FrameId) -> &LeanFrameMetaAtomic {
        &self.meta[frame_id]
    }

    pub fn frame_meta_snapshot(&self, frame_id: FrameId) -> LeanFrameMeta {
        self.meta[frame_id].snapshot()
    }

    pub unsafe fn frame_slice(&self, frame_id: FrameId) -> &[u8] {
        let ptr = self.frame_ptr(frame_id) as *const u8;
        std::slice::from_raw_parts(ptr, PAGE_SIZE)
    }

    pub unsafe fn frame_slice_mut(&self, frame_id: FrameId) -> &mut [u8] {
        let ptr = self.frame_ptr(frame_id);
        std::slice::from_raw_parts_mut(ptr, PAGE_SIZE)
    }

    unsafe fn frame_ptr(&self, frame_id: FrameId) -> *mut u8 {
        self.arena.as_ptr().add(frame_id * PAGE_SIZE) as *mut u8
    }

    pub fn clear_frame_meta(&self, frame_id: FrameId) {
        self.meta[frame_id].reset();
    }

    pub fn pop_free_frame(&self) -> Option<FrameId> {
        self.free_list.lock().pop_front()
    }

    pub fn push_free_frame(&self, frame_id: FrameId) {
        self.free_list.lock().push_back(frame_id);
    }

    pub fn load_page_into_frame(&self, page_id: PageId, frame_id: FrameId) -> QuillSQLResult<()> {
        let page_bytes = self.read_page_from_disk(page_id)?;
        let slice = unsafe { self.frame_slice_mut(frame_id) };
        let len = PAGE_SIZE.min(page_bytes.len());
        slice[..len].copy_from_slice(&page_bytes[..len]);
        if len < PAGE_SIZE {
            slice[len..].fill(0);
        }
        let meta = self.frame_meta(frame_id);
        meta.set_page_id(page_id);
        meta.set_pin_count(0);
        meta.clear_dirty();
        meta.set_lsn(0);
        Ok(())
    }

    pub fn write_page_to_disk(&self, page_id: PageId, bytes: Bytes) -> QuillSQLResult<()> {
        self.disk_scheduler
            .schedule_write(page_id, bytes)?
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("Channel disconnected: {}", e)))??;
        Ok(())
    }

    pub fn read_page_from_disk(&self, page_id: PageId) -> QuillSQLResult<BytesMut> {
        let rx = self.disk_scheduler.schedule_read(page_id)?;
        let data = rx
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("Channel disconnected: {}", e)))??;
        Ok(data)
    }

    pub fn reset_frame(&self, frame_id: FrameId) {
        unsafe {
            self.frame_slice_mut(frame_id).fill(0);
        }
        self.meta[frame_id].reset();
    }

    pub fn disk_scheduler(&self) -> Arc<DiskScheduler> {
        self.disk_scheduler.clone()
    }
}
