use std::mem::{self, ManuallyDrop};
use std::sync::Arc;

use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

use crate::buffer::engine::{BufferReadGuard, BufferWriteGuard};
use crate::buffer::standard::buffer_pool::{BufferPool, FrameId, FrameMeta};
use crate::buffer::standard::page::PAGE_SIZE;
use crate::buffer::PageId;
use crate::recovery::Lsn;

use super::manager::LeanBufferManager;
use super::metadata::{AccessKind, LeanPageDescriptor};

#[derive(Debug)]
pub struct LeanReadGuard {
    manager: Arc<LeanBufferManager>,
    pool: Arc<BufferPool>,
    frame_id: FrameId,
    guard: ManuallyDrop<RwLockReadGuard<'static, ()>>,
    descriptor: Arc<LeanPageDescriptor>,
}

impl LeanReadGuard {
    pub(crate) fn meta_snapshot(&self) -> FrameMeta {
        self.pool.frame_meta(self.frame_id).clone()
    }

    pub(crate) fn data(&self) -> &[u8] {
        unsafe { self.pool.frame_slice(self.frame_id) }
    }
}

impl Drop for LeanReadGuard {
    fn drop(&mut self) {
        let page_id = self.descriptor.page_id();
        let transition = self.descriptor.record_release(AccessKind::Read);
        self.manager.on_descriptor_transition(page_id, transition);
        unsafe {
            ManuallyDrop::drop(&mut self.guard);
        }
        if let Err(err) = self
            .manager
            .complete_unpin(page_id, self.frame_id, false, None)
        {
            eprintln!(
                "Warning: LeanReadGuard drop failed to complete_unpin page {}: {}",
                page_id, err
            );
        }
    }
}

impl BufferReadGuard for LeanReadGuard {
    fn page_id(&self) -> PageId {
        self.descriptor.page_id()
    }

    fn data(&self) -> &[u8] {
        self.data()
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

#[derive(Debug)]
pub struct LeanWriteGuard {
    manager: Arc<LeanBufferManager>,
    pool: Arc<BufferPool>,
    frame_id: FrameId,
    guard: ManuallyDrop<RwLockWriteGuard<'static, ()>>,
    descriptor: Arc<LeanPageDescriptor>,
    first_dirty_lsn: Option<Lsn>,
}

impl LeanWriteGuard {
    pub(crate) fn meta_snapshot(&self) -> FrameMeta {
        self.pool.frame_meta(self.frame_id).clone()
    }

    pub(crate) fn data(&self) -> &[u8] {
        unsafe { self.pool.frame_slice(self.frame_id) }
    }

    pub(crate) fn data_mut(&mut self) -> &mut [u8] {
        unsafe { self.pool.frame_slice_mut(self.frame_id) }
    }
}

impl Drop for LeanWriteGuard {
    fn drop(&mut self) {
        let page_id = self.descriptor.page_id();
        let meta_snapshot = self.meta_snapshot();
        let is_dirty = meta_snapshot.is_dirty;
        let lsn = meta_snapshot.lsn;
        let rec_lsn_hint = if let Some(first) = self.first_dirty_lsn {
            Some(first)
        } else if is_dirty {
            Some(lsn)
        } else {
            None
        };

        let transition = self.descriptor.record_release(AccessKind::Write);
        self.manager.on_descriptor_transition(page_id, transition);

        unsafe {
            ManuallyDrop::drop(&mut self.guard);
        }

        if let Err(err) =
            self.manager
                .complete_unpin(page_id, self.frame_id, is_dirty, rec_lsn_hint)
        {
            eprintln!(
                "Warning: LeanWriteGuard drop failed to complete_unpin page {}: {}",
                page_id, err
            );
        }
    }
}

impl BufferReadGuard for LeanWriteGuard {
    fn page_id(&self) -> PageId {
        self.descriptor.page_id()
    }

    fn data(&self) -> &[u8] {
        self.data()
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

impl BufferWriteGuard for LeanWriteGuard {
    fn data_mut(&mut self) -> &mut [u8] {
        self.data_mut()
    }

    fn mark_dirty(&mut self) {
        let mut meta = self.pool.frame_meta(self.frame_id);
        meta.is_dirty = true;
        if self.first_dirty_lsn.is_none() {
            self.first_dirty_lsn = Some(meta.lsn);
        }
    }

    fn set_lsn(&mut self, lsn: Lsn) {
        let mut meta = self.pool.frame_meta(self.frame_id);
        meta.lsn = lsn;
        if self.first_dirty_lsn.is_none() {
            self.first_dirty_lsn = Some(lsn);
        }
    }

    fn overwrite(&mut self, data: &[u8], new_lsn: Option<Lsn>) {
        debug_assert_eq!(data.len(), PAGE_SIZE);
        let slice = unsafe { self.pool.frame_slice_mut(self.frame_id) };
        slice.copy_from_slice(data);
        if let Some(lsn) = new_lsn {
            self.set_lsn(lsn);
        }
        self.mark_dirty();
    }

    fn first_dirty_lsn(&self) -> Option<Lsn> {
        self.first_dirty_lsn
    }
}

pub(crate) fn new_read_guard(
    manager: Arc<LeanBufferManager>,
    descriptor: Arc<LeanPageDescriptor>,
    frame_id: FrameId,
) -> LeanReadGuard {
    let pool = manager.buffer_pool();
    let lock = pool.frame_lock(frame_id);
    let guard = lock.read();
    let guard_static: RwLockReadGuard<'static, ()> =
        unsafe { mem::transmute::<RwLockReadGuard<'_, ()>, RwLockReadGuard<'static, ()>>(guard) };
    LeanReadGuard {
        manager,
        pool,
        frame_id,
        guard: ManuallyDrop::new(guard_static),
        descriptor,
    }
}

pub(crate) fn new_write_guard(
    manager: Arc<LeanBufferManager>,
    descriptor: Arc<LeanPageDescriptor>,
    frame_id: FrameId,
) -> LeanWriteGuard {
    let pool = manager.buffer_pool();
    let lock = pool.frame_lock(frame_id);
    let guard = lock.write();
    let guard_static: RwLockWriteGuard<'static, ()> =
        unsafe { mem::transmute::<RwLockWriteGuard<'_, ()>, RwLockWriteGuard<'static, ()>>(guard) };
    LeanWriteGuard {
        manager,
        pool,
        frame_id,
        guard: ManuallyDrop::new(guard_static),
        descriptor,
        first_dirty_lsn: None,
    }
}
