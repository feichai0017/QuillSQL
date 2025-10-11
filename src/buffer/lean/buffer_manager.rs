use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::mapref::entry::Entry;
use dashmap::{DashMap, DashSet};
use log::warn;
use parking_lot::{Mutex, RwLock};

use super::buffer_pool::{FrameId, LeanBufferPool};
use super::metadata::{
    AccessKind, LeanBufferStats, LeanBufferStatsSnapshot, LeanPageDescriptor, LeanPageSnapshot,
    LeanPageState, StateTransition,
};
use super::page::{new_read_guard, new_write_guard, LeanReadGuard, LeanWriteGuard};
use super::page_table::LeanPageTable;
use super::replacer::{LeanReplacer, LeanReplacerSnapshot};
use super::segment::{SegmentAllocationState, SegmentAllocator};
use crate::background::{self, WorkerHandle};
use crate::buffer::engine::{BufferEngine, ReadGuardRef, WriteGuardRef};
use crate::buffer::standard::page::INVALID_PAGE_ID;
use crate::buffer::PageId;
use crate::catalog::SchemaRef;
use crate::config::BufferPoolConfig;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::{ControlFileManager, Lsn, WalManager};
use crate::storage::codec::{
    BPlusTreeHeaderPageCodec, BPlusTreeInternalPageCodec, BPlusTreeLeafPageCodec,
    BPlusTreePageCodec, TablePageCodec,
};
use crate::storage::disk_scheduler::DiskScheduler;
use crate::storage::page::{
    BPlusTreeHeaderPage, BPlusTreeInternalPage, BPlusTreeLeafPage, BPlusTreePage, TablePage,
};

#[derive(Debug, Clone)]
pub struct LeanBufferOptions {
    pub hot_partition_fraction: f64,
    pub cooling_window: usize,
    pub temperature_decay: f64,
    pub cooling_batch_size: usize,
    pub page_table_partitions: usize,
    pub segment_size: u32,
}

impl Default for LeanBufferOptions {
    fn default() -> Self {
        Self {
            hot_partition_fraction: 0.20,
            cooling_window: 32,
            temperature_decay: 0.5,
            cooling_batch_size: 64,
            page_table_partitions: 64,
            segment_size: 16 * 1024,
        }
    }
}

#[derive(Debug)]
pub struct LeanBufferManager {
    pool: Arc<LeanBufferPool>,
    options: Arc<LeanBufferOptions>,
    page_table: Arc<LeanPageTable>,
    inflight_loads: DashMap<PageId, Arc<Mutex<()>>>,
    descriptors: DashMap<PageId, Arc<LeanPageDescriptor>>,
    stats: Arc<LeanBufferStats>,
    replacer: Arc<LeanReplacer>,
    segment_allocator: Arc<SegmentAllocator>,
    wal_manager: Arc<RwLock<Option<Arc<WalManager>>>>,
    control_file: Arc<RwLock<Option<Arc<crate::recovery::ControlFileManager>>>>,
    dirty_pages: DashSet<PageId>,
    dirty_page_table: DashMap<PageId, Lsn>,
}

impl LeanBufferManager {
    pub fn new(num_pages: usize, disk_scheduler: Arc<DiskScheduler>) -> Self {
        Self::with_buffer_config_and_state(
            BufferPoolConfig {
                buffer_pool_size: num_pages,
                ..Default::default()
            },
            disk_scheduler,
            LeanBufferOptions::default(),
            None,
        )
    }

    pub fn with_buffer_config(
        buffer_config: BufferPoolConfig,
        disk_scheduler: Arc<DiskScheduler>,
        options: LeanBufferOptions,
    ) -> Self {
        Self::with_buffer_config_and_state(buffer_config, disk_scheduler, options, None)
    }

    pub fn with_buffer_config_and_state(
        buffer_config: BufferPoolConfig,
        disk_scheduler: Arc<DiskScheduler>,
        options: LeanBufferOptions,
        segment_state: Option<SegmentAllocationState>,
    ) -> Self {
        let pool = Arc::new(LeanBufferPool::new_with_config(
            buffer_config,
            disk_scheduler,
        ));
        let page_table = Arc::new(LeanPageTable::new(options.page_table_partitions));
        let segment_allocator = Arc::new(SegmentAllocator::with_state(
            pool.disk_scheduler(),
            options.segment_size,
            segment_state.unwrap_or_default(),
        ));
        Self {
            pool,
            options: Arc::new(options),
            page_table,
            inflight_loads: DashMap::new(),
            descriptors: DashMap::new(),
            stats: Arc::new(LeanBufferStats::default()),
            replacer: Arc::new(LeanReplacer::new()),
            segment_allocator,
            wal_manager: Arc::new(RwLock::new(None)),
            control_file: Arc::new(RwLock::new(None)),
            dirty_pages: DashSet::new(),
            dirty_page_table: DashMap::new(),
        }
    }

    pub fn options(&self) -> &LeanBufferOptions {
        self.options.as_ref()
    }

    pub fn stats_snapshot(&self) -> LeanBufferStatsSnapshot {
        self.stats.snapshot()
    }

    pub fn replacer_snapshot(&self) -> LeanReplacerSnapshot {
        self.replacer.snapshot()
    }

    pub fn segment_state(&self) -> SegmentAllocationState {
        self.segment_allocator.snapshot()
    }

    pub fn page_snapshot(&self, page_id: PageId) -> Option<LeanPageSnapshot> {
        self.descriptors
            .get(&page_id)
            .map(|descriptor| descriptor.snapshot())
    }

    pub fn buffer_pool(&self) -> Arc<LeanBufferPool> {
        self.pool.clone()
    }

    pub fn set_wal_manager(&self, wal_manager: Arc<WalManager>) {
        *self.wal_manager.write() = Some(wal_manager);
    }

    pub fn wal_manager(&self) -> Option<Arc<WalManager>> {
        self.wal_manager.read().clone()
    }

    pub fn set_control_file(&self, control_file: Arc<ControlFileManager>) {
        *self.control_file.write() = Some(control_file);
    }

    pub fn control_file(&self) -> Option<Arc<ControlFileManager>> {
        self.control_file.read().clone()
    }

    pub fn collect_dirty_page_ids(&self) -> Vec<PageId> {
        self.dirty_pages.iter().map(|entry| *entry.key()).collect()
    }

    pub fn collect_dirty_page_table_snapshot(&self) -> Vec<(PageId, Lsn)> {
        self.dirty_page_table
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect()
    }

    pub fn cooling_pass(&self, budget: usize) -> usize {
        let budget = if budget == 0 {
            self.options.cooling_batch_size
        } else {
            budget
        };
        if budget == 0 {
            return 0;
        }
        let decay = self.options.temperature_decay.clamp(0.0, 1.0);
        if decay >= 1.0 {
            return 0;
        }

        let mut visited = 0usize;
        for entry in self.descriptors.iter().take(budget) {
            let page_id = *entry.key();
            let descriptor = entry.value();
            if let Some(transition) = descriptor.cool_down(decay) {
                self.on_descriptor_transition(page_id, transition);
            }
            visited += 1;
        }
        self.enforce_hot_fraction();
        visited
    }

    fn enforce_hot_fraction(&self) {
        let snapshot = self.stats_snapshot();
        let total = snapshot.hot_pages + snapshot.cooling_pages + snapshot.cool_pages;
        if total == 0 {
            return;
        }
        let mut target = (self.options.hot_partition_fraction * total as f64).ceil() as usize;
        target = target.max(1);
        if snapshot.hot_pages <= target {
            return;
        }
        let mut remaining = snapshot.hot_pages - target;
        for entry in self.descriptors.iter() {
            if remaining == 0 {
                break;
            }
            let page_id = *entry.key();
            let descriptor = entry.value();
            if descriptor.state() != LeanPageState::Hot {
                continue;
            }
            if let Some(transition) = descriptor.force_state(LeanPageState::Cooling) {
                self.on_descriptor_transition(page_id, transition);
                remaining = remaining.saturating_sub(1);
            }
        }
    }

    fn ensure_descriptor(&self, page_id: PageId) -> Arc<LeanPageDescriptor> {
        match self.descriptors.entry(page_id) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let descriptor =
                    Arc::new(LeanPageDescriptor::new(page_id, Arc::clone(&self.options)));
                self.stats.on_page_created(descriptor.state());
                self.replacer.on_page_created(page_id, descriptor.state());
                entry.insert(descriptor.clone());
                descriptor
            }
        }
    }

    pub(crate) fn on_descriptor_transition(&self, page_id: PageId, transition: StateTransition) {
        self.stats.apply_transition(transition);
        self.replacer.apply_transition(page_id, transition);
    }

    fn record_access(&self, page_id: PageId, access: AccessKind) -> Arc<LeanPageDescriptor> {
        let descriptor = self.ensure_descriptor(page_id);
        self.stats.record_access(access);
        let transition = descriptor.record_access(access);
        self.on_descriptor_transition(page_id, transition);
        descriptor
    }

    fn note_dirty_page(&self, page_id: PageId, rec_lsn: Lsn) {
        self.dirty_pages.insert(page_id);
        self.dirty_page_table.entry(page_id).or_insert(rec_lsn);
    }

    fn mark_non_evictable(&self, page_id: PageId) {
        self.replacer.mark_non_evictable(page_id);
    }

    fn mark_evictable(&self, page_id: PageId) {
        if let Some(descriptor) = self.descriptors.get(&page_id) {
            self.replacer
                .mark_evictable(page_id, descriptor.value().state());
        }
    }

    fn ensure_frame(&self, page_id: PageId) -> QuillSQLResult<FrameId> {
        if let Some(frame_id) = self.page_table.lookup(page_id) {
            return Ok(frame_id);
        }

        let (guard, created_here) = if let Some(existing) = self.inflight_loads.get(&page_id) {
            (existing.clone(), false)
        } else {
            let arc = Arc::new(Mutex::new(()));
            self.inflight_loads.insert(page_id, arc.clone());
            (arc, true)
        };
        let lock = guard.lock();

        if let Some(frame_id) = self.page_table.lookup(page_id) {
            drop(lock);
            if created_here {
                self.inflight_loads.remove(&page_id);
            }
            return Ok(frame_id);
        }

        let frame_id = self.allocate_frame()?;
        self.pool.load_page_into_frame(page_id, frame_id)?;
        self.page_table.insert(page_id, frame_id);
        let meta = self.pool.frame_meta(frame_id);
        meta.set_page_id(page_id);
        meta.set_pin_count(1);
        meta.clear_dirty();
        meta.set_lsn(0);
        drop(lock);
        if created_here {
            self.inflight_loads.remove(&page_id);
        }
        Ok(frame_id)
    }

    fn allocate_frame(&self) -> QuillSQLResult<FrameId> {
        if let Some(frame_id) = self.pool.pop_free_frame() {
            return Ok(frame_id);
        }
        self.evict_victim_frame()
    }

    fn evict_victim_frame(&self) -> QuillSQLResult<FrameId> {
        loop {
            let Some(page_id) = self.replacer.pop_victim() else {
                return Err(QuillSQLError::Storage(
                    "Cannot allocate frame: buffer pool is full".to_string(),
                ));
            };

            let Some(frame_id) = self.page_table.lookup(page_id) else {
                continue;
            };

            let meta_snapshot = self.pool.frame_meta_snapshot(frame_id);
            if meta_snapshot.pin_count > 0 {
                self.mark_non_evictable(page_id);
                continue;
            }

            if meta_snapshot.page_id != page_id {
                self.page_table.remove_if(page_id, frame_id);
                continue;
            }

            if meta_snapshot.is_dirty {
                self.ensure_wal_durable(meta_snapshot.lsn)?;
                let bytes = Bytes::copy_from_slice(unsafe { self.pool.frame_slice(frame_id) });
                self.pool.write_page_to_disk(page_id, bytes)?;
                self.dirty_pages.remove(&page_id);
                self.dirty_page_table.remove(&page_id);
            }

            self.page_table.remove(page_id);
            self.pool.clear_frame_meta(frame_id);
            self.pool.reset_frame(frame_id);
            self.mark_non_evictable(page_id);
            return Ok(frame_id);
        }
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

    pub(crate) fn complete_unpin(
        &self,
        page_id: PageId,
        frame_id: FrameId,
        is_dirty: bool,
        rec_lsn_hint: Option<Lsn>,
    ) -> QuillSQLResult<()> {
        let meta = self.pool.frame_meta(frame_id);
        let remaining = meta.unpin();
        if is_dirty {
            meta.mark_dirty();
            if let Some(lsn) = rec_lsn_hint {
                meta.set_lsn(lsn);
            }
            let lsn = rec_lsn_hint.unwrap_or_else(|| meta.lsn());
            self.note_dirty_page(page_id, lsn);
        }
        if remaining == 0 {
            self.mark_evictable(page_id);
        } else {
            self.mark_non_evictable(page_id);
        }
        Ok(())
    }

    fn persist_segment_state(&self) {
        if let Some(control) = self.control_file.read().clone() {
            let state = self.segment_allocator.snapshot();
            if let Err(err) = control.update_lean_segment_state(state.last_allocated_page) {
                warn!("Failed to persist lean segment state: {}", err);
            }
        }
    }

    fn new_page_guard(self: &Arc<Self>) -> QuillSQLResult<LeanWriteGuard> {
        let frame_id = self.allocate_frame()?;
        let page_id = self.segment_allocator.allocate_page()?;
        self.page_table.insert(page_id, frame_id);
        self.pool.reset_frame(frame_id);
        let meta = self.pool.frame_meta(frame_id);
        meta.set_page_id(page_id);
        meta.set_pin_count(1);
        meta.clear_dirty();
        meta.set_lsn(0);
        let descriptor = self.record_access(page_id, AccessKind::New);
        self.mark_non_evictable(page_id);
        self.persist_segment_state();
        Ok(new_write_guard(self.clone(), descriptor, frame_id))
    }

    fn fetch_page_read_guard(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<LeanReadGuard> {
        if page_id == INVALID_PAGE_ID {
            return Err(QuillSQLError::Storage(
                "fetch_page_read: invalid page id".to_string(),
            ));
        }
        let frame_id = self.ensure_frame(page_id)?;
        self.pool.frame_meta(frame_id).pin();
        self.mark_non_evictable(page_id);
        let descriptor = self.record_access(page_id, AccessKind::Read);
        Ok(new_read_guard(self.clone(), descriptor, frame_id))
    }

    fn fetch_page_write_guard(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<LeanWriteGuard> {
        if page_id == INVALID_PAGE_ID {
            return Err(QuillSQLError::Storage(
                "fetch_page_write: invalid page id".to_string(),
            ));
        }
        let frame_id = self.ensure_frame(page_id)?;
        self.pool.frame_meta(frame_id).pin();
        self.mark_non_evictable(page_id);
        let descriptor = self.record_access(page_id, AccessKind::Write);
        Ok(new_write_guard(self.clone(), descriptor, frame_id))
    }

    fn prefetch_page_internal(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<()> {
        let descriptor = self.record_access(page_id, AccessKind::Prefetch);
        let frame_id = self.ensure_frame(page_id)?;
        let snapshot = self.pool.frame_meta_snapshot(frame_id);
        if snapshot.pin_count == 0 {
            self.mark_evictable(page_id);
        }
        drop(descriptor);
        Ok(())
    }

    fn flush_page_internal(&self, page_id: PageId) -> QuillSQLResult<bool> {
        let Some(frame_id) = self.page_table.lookup(page_id) else {
            return Ok(false);
        };
        let meta = self.pool.frame_meta(frame_id);
        if !meta.is_dirty() {
            self.dirty_pages.remove(&page_id);
            self.dirty_page_table.remove(&page_id);
            return Ok(false);
        }
        let lsn = meta.lsn();
        self.ensure_wal_durable(lsn)?;
        let bytes = {
            let _lock = self.pool.frame_lock(frame_id).read();
            let slice = unsafe { self.pool.frame_slice(frame_id) };
            Bytes::copy_from_slice(slice)
        };
        self.pool.write_page_to_disk(page_id, bytes)?;
        meta.clear_dirty();
        self.dirty_pages.remove(&page_id);
        self.dirty_page_table.remove(&page_id);
        Ok(true)
    }

    fn delete_page_internal(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<bool> {
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
        if let Some(frame_id) = self.page_table.lookup(page_id) {
            let lock = self.pool.frame_lock(frame_id);
            let guard = match lock.try_write() {
                Some(g) => g,
                None => return Ok(false),
            };
            drop(guard);
            let meta = self.pool.frame_meta(frame_id);
            let snapshot = meta.snapshot();
            if snapshot.page_id != page_id {
                self.page_table.remove_if(page_id, frame_id);
                return self.delete_page_inner(page_id);
            }
            if snapshot.pin_count > 0 {
                return Ok(false);
            }
            if !self.page_table.remove_if(page_id, frame_id) {
                return self.delete_page_inner(page_id);
            }
            self.pool.reset_frame(frame_id);
            self.pool.push_free_frame(frame_id);
            self.dirty_pages.remove(&page_id);
            self.dirty_page_table.remove(&page_id);
            if let Some((_, descriptor)) = self.descriptors.remove(&page_id) {
                self.stats.on_page_removed(descriptor.state());
                self.replacer.on_page_removed(page_id, descriptor.state());
            } else {
                self.replacer.on_page_removed(page_id, LeanPageState::Cool);
            }
            self.segment_allocator.deallocate_page(page_id)?;
            Ok(true)
        } else {
            if let Some((_, descriptor)) = self.descriptors.remove(&page_id) {
                self.stats.on_page_removed(descriptor.state());
                self.replacer.on_page_removed(page_id, descriptor.state());
            }
            self.page_table.remove(page_id);
            self.segment_allocator.deallocate_page(page_id)?;
            Ok(true)
        }
    }

    pub fn fetch_table_page(
        self: &Arc<Self>,
        page_id: PageId,
        schema: SchemaRef,
    ) -> QuillSQLResult<(LeanReadGuard, TablePage)> {
        let guard = self.fetch_page_read_guard(page_id)?;
        let (page, _) = TablePageCodec::decode(guard.data(), schema)?;
        Ok((guard, page))
    }

    pub fn fetch_tree_page(
        self: &Arc<Self>,
        page_id: PageId,
        key_schema: SchemaRef,
    ) -> QuillSQLResult<(LeanReadGuard, BPlusTreePage)> {
        let guard = self.fetch_page_read_guard(page_id)?;
        let (page, _) = BPlusTreePageCodec::decode(guard.data(), key_schema)?;
        Ok((guard, page))
    }

    pub fn fetch_tree_internal_page(
        self: &Arc<Self>,
        page_id: PageId,
        key_schema: SchemaRef,
    ) -> QuillSQLResult<(LeanReadGuard, BPlusTreeInternalPage)> {
        let guard = self.fetch_page_read_guard(page_id)?;
        let (page, _) = BPlusTreeInternalPageCodec::decode(guard.data(), key_schema)?;
        Ok((guard, page))
    }

    pub fn fetch_tree_leaf_page(
        self: &Arc<Self>,
        page_id: PageId,
        key_schema: SchemaRef,
    ) -> QuillSQLResult<(LeanReadGuard, BPlusTreeLeafPage)> {
        let guard = self.fetch_page_read_guard(page_id)?;
        let (page, _) = BPlusTreeLeafPageCodec::decode(guard.data(), key_schema)?;
        Ok((guard, page))
    }

    pub fn fetch_header_page(
        self: &Arc<Self>,
        page_id: PageId,
    ) -> QuillSQLResult<(LeanReadGuard, BPlusTreeHeaderPage)> {
        let guard = self.fetch_page_read_guard(page_id)?;
        let (page, _) = BPlusTreeHeaderPageCodec::decode(guard.data())?;
        Ok((guard, page))
    }

    pub fn spawn_cooler_worker(
        self: &Arc<Self>,
        interval: Option<Duration>,
        batch_limit: usize,
    ) -> Option<WorkerHandle> {
        background::spawn_lean_cooler_worker(self.clone(), interval, batch_limit)
    }
}

impl BufferEngine for LeanBufferManager {
    fn new_page(self: &Arc<Self>) -> QuillSQLResult<WriteGuardRef> {
        self.new_page_guard()
            .map(|guard| Box::new(guard) as WriteGuardRef)
    }

    fn fetch_page_read(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<ReadGuardRef> {
        self.fetch_page_read_guard(page_id)
            .map(|guard| Box::new(guard) as ReadGuardRef)
    }

    fn fetch_page_write(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<WriteGuardRef> {
        self.fetch_page_write_guard(page_id)
            .map(|guard| Box::new(guard) as WriteGuardRef)
    }

    fn prefetch_page(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<()> {
        self.prefetch_page_internal(page_id)
    }

    fn flush_page(&self, page_id: PageId) -> QuillSQLResult<bool> {
        self.flush_page_internal(page_id)
    }

    fn flush_all_pages(&self) -> QuillSQLResult<()> {
        if let Some(wal) = self.wal_manager.read().clone() {
            wal.flush(None)?;
        }
        for page_id in self.collect_dirty_page_ids() {
            let _ = self.flush_page_internal(page_id)?;
        }
        Ok(())
    }

    fn delete_page(self: &Arc<Self>, page_id: PageId) -> QuillSQLResult<bool> {
        self.delete_page_internal(page_id)
    }

    fn dirty_page_ids(&self) -> Vec<PageId> {
        self.collect_dirty_page_ids()
    }

    fn dirty_page_table_snapshot(&self) -> Vec<(PageId, Lsn)> {
        self.collect_dirty_page_table_snapshot()
    }

    fn set_wal_manager(&self, wal_manager: Arc<WalManager>) {
        LeanBufferManager::set_wal_manager(self, wal_manager);
    }

    fn wal_manager(&self) -> Option<Arc<WalManager>> {
        LeanBufferManager::wal_manager(self)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::engine::BufferEngine;
    use crate::storage::disk_manager::DiskManager;
    use tempfile::TempDir;

    #[test]
    fn lean_manager_allocates_and_tracks_stats() {
        let temp = TempDir::new().unwrap();
        let disk_manager = Arc::new(DiskManager::try_new(temp.path().join("lean.db")).unwrap());
        let scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let manager = Arc::new(LeanBufferManager::new(8, scheduler));

        let guard = <LeanBufferManager as BufferEngine>::new_page(&manager).unwrap();
        assert_eq!(guard.pin_count(), 1);
        let stats = manager.stats_snapshot();
        assert_eq!(stats.total_accesses, 1);
        assert_eq!(stats.hot_pages, 1);

        drop(guard);
        let stats_after = manager.stats_snapshot();
        assert_eq!(
            stats_after.hot_pages + stats_after.cooling_pages + stats_after.cool_pages,
            1
        );
    }

    #[test]
    fn cooling_pass_rebalances_hot_fraction() {
        let temp = TempDir::new().unwrap();
        let disk_manager =
            Arc::new(DiskManager::try_new(temp.path().join("rebalance.db")).unwrap());
        let scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let manager = Arc::new(LeanBufferManager::new(16, scheduler));

        let mut guards = Vec::new();
        for _ in 0..6 {
            guards.push(<LeanBufferManager as BufferEngine>::new_page(&manager).unwrap());
        }
        guards.clear();

        manager.cooling_pass(manager.options().cooling_batch_size);
        let stats = manager.stats_snapshot();
        let total = stats.hot_pages + stats.cooling_pages + stats.cool_pages;
        assert_eq!(total, 6);
        let allowed_hot = (manager.options().hot_partition_fraction * total as f64).ceil() as usize;
        assert!(stats.hot_pages <= allowed_hot);
    }
}
