use crate::buffer::buffer_pool::{BufferPool, FrameMeta};
use crate::buffer::{BufferManager, FrameId};
use crate::recovery::Lsn;
use derive_with::With;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use std::mem::{self, ManuallyDrop};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

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
    bpm: Arc<BufferManager>,
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
    bpm: Arc<BufferManager>,
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

pub(crate) fn new_read_guard(bpm: Arc<BufferManager>, frame_id: FrameId) -> ReadPageGuard {
    let pool = bpm.buffer_pool();
    let lock = pool.frame_lock(frame_id).read();
    let static_guard =
        unsafe { mem::transmute::<RwLockReadGuard<'_, ()>, RwLockReadGuard<'static, ()>>(lock) };
    ReadPageGuard {
        bpm,
        pool,
        frame_id,
        guard: ManuallyDrop::new(static_guard),
    }
}

pub(crate) fn new_write_guard(bpm: Arc<BufferManager>, frame_id: FrameId) -> WritePageGuard {
    let pool = bpm.buffer_pool();
    let lock = pool.frame_lock(frame_id).write();
    let static_guard =
        unsafe { mem::transmute::<RwLockWriteGuard<'_, ()>, RwLockWriteGuard<'static, ()>>(lock) };
    WritePageGuard {
        bpm,
        pool,
        frame_id,
        guard: ManuallyDrop::new(static_guard),
        first_dirty_lsn: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::buffer::BufferManager;
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;

    use super::{PageMeta, INVALID_PAGE_ID, PAGE_SIZE};

    fn setup_real_bpm_environment(num_pages: usize) -> (TempDir, Arc<BufferManager>) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let disk_manager = Arc::new(DiskManager::try_new(db_path).unwrap());
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let buffer_pool_manager = Arc::new(BufferManager::new(num_pages, disk_scheduler));

        (temp_dir, buffer_pool_manager)
    }

    #[test]
    fn test_page_struct_creation() {
        let page = PageMeta::new(1);
        assert_eq!(page.page_id, 1);
        assert!(!page.is_dirty);
        assert_eq!(page.pin_count.load(Ordering::Acquire), 0);

        let empty_page = PageMeta::empty();
        assert_eq!(empty_page.page_id, INVALID_PAGE_ID);
    }

    #[test]
    fn test_read_guard_deref_and_drop() {
        let (_temp_dir, bpm) = setup_real_bpm_environment(10);

        let (page_id, frame_id) = {
            let guard = bpm.new_page().unwrap();
            let frame_id = guard.frame_id();
            (guard.page_id(), frame_id)
        };

        {
            let meta = bpm.buffer_pool().frame_meta(frame_id).clone();
            assert_eq!(meta.pin_count, 0);
        }

        let read_guard = bpm.fetch_page_read(page_id).unwrap();
        assert_eq!(read_guard.page_id(), page_id);
        assert_eq!(read_guard.pin_count(), 1);
        assert_eq!(read_guard.data().len(), PAGE_SIZE);
        let snapshot = read_guard.meta_snapshot();
        assert_eq!(snapshot.pin_count, 1);
        drop(read_guard);

        let meta = bpm.buffer_pool().frame_meta(frame_id).clone();
        assert_eq!(meta.pin_count, 0);
    }

    #[test]
    fn test_write_guard_deref_mut_and_drop() {
        let (_temp_dir, bpm) = setup_real_bpm_environment(10);
        let (page_id, frame_id) = {
            let mut write_guard = bpm.new_page().unwrap();
            write_guard.data_mut()[0] = 123;
            write_guard.set_lsn(42);
            write_guard.mark_dirty();
            (write_guard.page_id(), write_guard.frame_id())
        };

        let meta = bpm.buffer_pool().frame_meta(frame_id).clone();
        assert!(meta.is_dirty);
        assert_eq!(meta.lsn, 42);
        assert_eq!(meta.pin_count, 0);

        let read_guard = bpm.fetch_page_read(page_id).unwrap();
        assert_eq!(read_guard.data()[0], 123);
        assert!(read_guard.is_dirty());
        assert_eq!(read_guard.lsn(), 42);
        let snapshot = read_guard.meta_snapshot();
        assert_eq!(snapshot.lsn, 42);
        assert!(snapshot.is_dirty);
        assert_eq!(snapshot.pin_count, 1);
        drop(read_guard);

        let meta = bpm.buffer_pool().frame_meta(frame_id).clone();
        assert!(meta.is_dirty);
        assert_eq!(meta.lsn, 42);
        assert_eq!(meta.pin_count, 0);
    }

    #[test]
    fn test_write_guard_without_mutation_is_not_dirty() {
        let (_temp_dir, bpm) = setup_real_bpm_environment(10);
        let (page_id, frame_id) = {
            let guard = bpm.new_page().unwrap();
            (guard.page_id(), guard.frame_id())
        };

        {
            let _write_guard = bpm.fetch_page_write(page_id).unwrap();
        }

        let read_guard = bpm.fetch_page_read(page_id).unwrap();
        let snapshot = read_guard.meta_snapshot();
        assert!(!snapshot.is_dirty);
        assert_eq!(snapshot.lsn, 0);
        assert_eq!(snapshot.pin_count, 1);
        drop(read_guard);

        let meta = bpm.buffer_pool().frame_meta(frame_id).clone();
        assert!(!meta.is_dirty);
        assert_eq!(meta.pin_count, 0);
    }
}
