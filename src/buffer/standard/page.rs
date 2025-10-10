use crate::buffer::engine::{BufferReadGuard, BufferWriteGuard};
use crate::recovery::Lsn;
use derive_with::With;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use std::mem::{self, ManuallyDrop};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use super::buffer_pool::{BufferPool, FrameId, FrameMeta};
use super::StandardBufferManager;

pub type PageId = u32;
pub type AtomicPageId = AtomicU32;

pub const INVALID_PAGE_ID: PageId = 0;
pub const PAGE_SIZE: usize = 4096;

#[derive(Debug, With)]
pub struct PageMeta {
    pub page_id: PageId,
    pub pin_count: AtomicU32,
    pub is_dirty: bool,
    pub page_lsn: Lsn,
}

impl PageMeta {
    pub fn empty() -> Self {
        Self {
            page_id: INVALID_PAGE_ID,
            pin_count: AtomicU32::new(0),
            is_dirty: false,
            page_lsn: 0,
        }
    }

    pub fn new(page_id: PageId) -> Self {
        Self {
            page_id,
            pin_count: AtomicU32::new(0),
            is_dirty: false,
            page_lsn: 0,
        }
    }

    pub fn destroy(&mut self) {
        self.page_id = INVALID_PAGE_ID;
        self.pin_count.store(0, Ordering::Relaxed);
        self.is_dirty = false;
        self.page_lsn = 0;
    }
}

#[derive(Debug)]
pub struct ReadPageGuard {
    bpm: Arc<StandardBufferManager>,
    pool: Arc<BufferPool>,
    frame_id: FrameId,
    guard: ManuallyDrop<RwLockReadGuard<'static, ()>>,
}

impl ReadPageGuard {
    pub fn pin_count(&self) -> u32 {
        self.meta_snapshot().pin_count
    }

    pub fn data(&self) -> &[u8] {
        unsafe { self.pool.frame_slice(self.frame_id) }
    }

    pub fn is_dirty(&self) -> bool {
        self.meta_snapshot().is_dirty
    }

    pub fn page_id(&self) -> PageId {
        self.meta_snapshot().page_id
    }

    pub fn lsn(&self) -> Lsn {
        self.meta_snapshot().lsn
    }

    pub fn meta_snapshot(&self) -> FrameMeta {
        self.pool.frame_meta(self.frame_id).clone()
    }

    pub fn frame_id(&self) -> FrameId {
        self.frame_id
    }
}

impl Drop for ReadPageGuard {
    fn drop(&mut self) {
        let snapshot = self.meta_snapshot();
        let page_id = snapshot.page_id;
        let is_dirty = snapshot.is_dirty;
        unsafe {
            ManuallyDrop::drop(&mut self.guard);
        }
        if let Err(e) = self.bpm.complete_unpin(page_id, is_dirty, None) {
            eprintln!("Warning: Failed to complete_unpin page {}: {}", page_id, e);
        }
    }
}

#[derive(Debug)]
pub struct WritePageGuard {
    bpm: Arc<StandardBufferManager>,
    pool: Arc<BufferPool>,
    frame_id: FrameId,
    guard: ManuallyDrop<RwLockWriteGuard<'static, ()>>,
    first_dirty_lsn: Option<Lsn>,
}

impl WritePageGuard {
    pub fn pin_count(&self) -> u32 {
        self.meta_snapshot().pin_count
    }

    pub fn data(&self) -> &[u8] {
        unsafe { self.pool.frame_slice(self.frame_id) }
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        unsafe { self.pool.frame_slice_mut(self.frame_id) }
    }

    pub fn is_dirty(&self) -> bool {
        self.meta_snapshot().is_dirty
    }

    pub fn page_id(&self) -> PageId {
        self.meta_snapshot().page_id
    }

    pub fn lsn(&self) -> Lsn {
        self.meta_snapshot().lsn
    }

    pub fn set_lsn(&mut self, lsn: Lsn) {
        let mut meta = self.pool.frame_meta(self.frame_id);
        meta.lsn = lsn;
        if self.first_dirty_lsn.is_none() {
            self.first_dirty_lsn = Some(lsn);
        }
    }

    pub fn mark_dirty(&mut self) {
        let mut meta = self.pool.frame_meta(self.frame_id);
        meta.is_dirty = true;
        if self.first_dirty_lsn.is_none() {
            self.first_dirty_lsn = Some(meta.lsn);
        }
    }

    pub fn overwrite(&mut self, data: &[u8], new_lsn: Option<Lsn>) {
        debug_assert_eq!(data.len(), PAGE_SIZE);
        let slice = unsafe { self.pool.frame_slice_mut(self.frame_id) };
        slice.copy_from_slice(data);
        if let Some(lsn) = new_lsn {
            self.set_lsn(lsn);
        }
        self.mark_dirty();
    }

    pub fn meta_snapshot(&self) -> FrameMeta {
        self.pool.frame_meta(self.frame_id).clone()
    }

    pub fn frame_id(&self) -> FrameId {
        self.frame_id
    }
}

impl Drop for WritePageGuard {
    fn drop(&mut self) {
        let snapshot = self.meta_snapshot();
        let page_id = snapshot.page_id;
        let is_dirty = snapshot.is_dirty;
        let lsn = snapshot.lsn;
        unsafe {
            ManuallyDrop::drop(&mut self.guard);
        }
        let rec_lsn_hint = if let Some(first) = self.first_dirty_lsn {
            Some(first)
        } else if is_dirty {
            Some(lsn)
        } else {
            None
        };
        if let Err(e) = self.bpm.complete_unpin(page_id, is_dirty, rec_lsn_hint) {
            eprintln!("Warning: Failed to complete_unpin page {}: {}", page_id, e);
        }
    }
}

impl BufferReadGuard for ReadPageGuard {
    fn page_id(&self) -> PageId {
        self.meta_snapshot().page_id
    }

    fn data(&self) -> &[u8] {
        ReadPageGuard::data(self)
    }

    fn lsn(&self) -> Lsn {
        self.meta_snapshot().lsn
    }

    fn is_dirty(&self) -> bool {
        self.meta_snapshot().is_dirty
    }

    fn pin_count(&self) -> u32 {
        self.meta_snapshot().pin_count
    }
}

impl BufferReadGuard for WritePageGuard {
    fn page_id(&self) -> PageId {
        self.meta_snapshot().page_id
    }

    fn data(&self) -> &[u8] {
        WritePageGuard::data(self)
    }

    fn lsn(&self) -> Lsn {
        self.meta_snapshot().lsn
    }

    fn is_dirty(&self) -> bool {
        self.meta_snapshot().is_dirty
    }

    fn pin_count(&self) -> u32 {
        self.meta_snapshot().pin_count
    }
}

impl BufferWriteGuard for WritePageGuard {
    fn data_mut(&mut self) -> &mut [u8] {
        WritePageGuard::data_mut(self)
    }

    fn mark_dirty(&mut self) {
        WritePageGuard::mark_dirty(self)
    }

    fn set_lsn(&mut self, lsn: Lsn) {
        WritePageGuard::set_lsn(self, lsn)
    }

    fn overwrite(&mut self, data: &[u8], new_lsn: Option<Lsn>) {
        WritePageGuard::overwrite(self, data, new_lsn)
    }

    fn first_dirty_lsn(&self) -> Option<Lsn> {
        self.first_dirty_lsn
    }
}

pub(crate) fn new_read_guard(bpm: Arc<StandardBufferManager>, frame_id: FrameId) -> ReadPageGuard {
    let pool = bpm.buffer_pool();
    let lock = pool.frame_lock(frame_id);
    let guard = lock.read();
    let guard_static: RwLockReadGuard<'static, ()> =
        unsafe { mem::transmute::<RwLockReadGuard<'_, ()>, RwLockReadGuard<'static, ()>>(guard) };
    ReadPageGuard {
        bpm,
        pool,
        frame_id,
        guard: ManuallyDrop::new(guard_static),
    }
}

pub(crate) fn new_write_guard(
    bpm: Arc<StandardBufferManager>,
    frame_id: FrameId,
) -> WritePageGuard {
    let pool = bpm.buffer_pool();
    let lock = pool.frame_lock(frame_id);
    let guard = lock.write();
    let guard_static: RwLockWriteGuard<'static, ()> =
        unsafe { mem::transmute::<RwLockWriteGuard<'_, ()>, RwLockWriteGuard<'static, ()>>(guard) };
    WritePageGuard {
        bpm,
        pool,
        frame_id,
        guard: ManuallyDrop::new(guard_static),
        first_dirty_lsn: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk_scheduler::DiskScheduler;
    use tempfile::TempDir;

    fn setup_real_bpm_environment(num_pages: usize) -> (TempDir, Arc<StandardBufferManager>) {
        let temp_dir = TempDir::new().unwrap();
        let disk_manager = Arc::new(
            crate::storage::disk_manager::DiskManager::try_new(temp_dir.path().join("bpm.db"))
                .unwrap(),
        );
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let buffer_pool_manager = Arc::new(StandardBufferManager::new(num_pages, disk_scheduler));
        (temp_dir, buffer_pool_manager)
    }

    #[test]
    fn read_guard_pins_and_unpins_frame() {
        let (_tmp, bpm) = setup_real_bpm_environment(4);
        let guard = bpm.new_page().unwrap();
        let page_id = guard.page_id();
        drop(guard);

        let read_guard = bpm.fetch_page_read(page_id).unwrap();
        assert_eq!(read_guard.pin_count(), 1);
        drop(read_guard);
    }
}
