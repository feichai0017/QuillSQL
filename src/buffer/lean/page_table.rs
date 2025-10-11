use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::buffer::standard::buffer_pool::FrameId;
use crate::buffer::PageId;

#[derive(Debug)]
pub struct PageTableEntry {
    frame_id: FrameId,
    #[allow(dead_code)]
    swizzled: AtomicUsize,
}

impl PageTableEntry {
    fn new(frame_id: FrameId) -> Self {
        Self {
            frame_id,
            swizzled: AtomicUsize::new(0),
        }
    }

    pub fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    #[allow(dead_code)]
    pub fn set_swizzled(&self, ptr: Option<NonNull<u8>>) {
        let raw = ptr.map_or(0, |p| p.as_ptr() as usize);
        self.swizzled.store(raw, Ordering::Release);
    }

    #[allow(dead_code)]
    pub fn take_swizzled(&self) -> Option<NonNull<u8>> {
        let raw = self.swizzled.swap(0, Ordering::AcqRel);
        NonNull::new(raw as *mut u8)
    }

    #[allow(dead_code)]
    pub fn swizzled_ptr(&self) -> Option<NonNull<u8>> {
        NonNull::new(self.swizzled.load(Ordering::Acquire) as *mut u8)
    }
}

#[derive(Debug)]
struct Partition {
    map: RwLock<HashMap<PageId, Arc<PageTableEntry>>>,
}

impl Partition {
    fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
        }
    }
}

#[derive(Debug)]
pub struct LeanPageTable {
    partitions: Vec<Partition>,
    mask: usize,
}

impl LeanPageTable {
    pub fn new(partitions: usize) -> Self {
        let partition_count = partitions.max(1).next_power_of_two();
        let mask = partition_count - 1;
        let partitions = (0..partition_count).map(|_| Partition::new()).collect();
        Self { partitions, mask }
    }

    fn partition(&self, page_id: PageId) -> &Partition {
        &self.partitions[(page_id as usize) & self.mask]
    }

    pub fn lookup(&self, page_id: PageId) -> Option<FrameId> {
        self.entry(page_id).map(|entry| entry.frame_id())
    }

    pub fn entry(&self, page_id: PageId) -> Option<Arc<PageTableEntry>> {
        let partition = self.partition(page_id);
        partition.map.read().get(&page_id).cloned()
    }

    pub fn insert(&self, page_id: PageId, frame_id: FrameId) {
        let entry = Arc::new(PageTableEntry::new(frame_id));
        let partition = self.partition(page_id);
        partition.map.write().insert(page_id, entry);
    }

    pub fn remove(&self, page_id: PageId) -> Option<Arc<PageTableEntry>> {
        let partition = self.partition(page_id);
        partition.map.write().remove(&page_id)
    }

    pub fn remove_if(&self, page_id: PageId, frame_id: FrameId) -> bool {
        let partition = self.partition(page_id);
        let mut guard = partition.map.write();
        match guard.get(&page_id) {
            Some(current) if current.frame_id() == frame_id => {
                guard.remove(&page_id);
                true
            }
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub fn set_swizzled(&self, page_id: PageId, ptr: Option<NonNull<u8>>) {
        if let Some(entry) = self.entry(page_id) {
            entry.set_swizzled(ptr);
        }
    }

    #[allow(dead_code)]
    pub fn take_swizzled(&self, page_id: PageId) -> Option<NonNull<u8>> {
        self.entry(page_id).and_then(|entry| entry.take_swizzled())
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.partitions
            .iter()
            .map(|partition| partition.map.read().len())
            .sum()
    }
}
