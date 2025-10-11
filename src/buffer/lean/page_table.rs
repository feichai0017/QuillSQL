use std::collections::HashMap;

use parking_lot::RwLock;

use crate::buffer::standard::buffer_pool::FrameId;
use crate::buffer::PageId;

#[derive(Debug)]
struct Partition {
    map: RwLock<HashMap<PageId, FrameId>>,
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
        let idx = (page_id as usize) & self.mask;
        &self.partitions[idx]
    }

    pub fn lookup(&self, page_id: PageId) -> Option<FrameId> {
        let partition = self.partition(page_id);
        partition.map.read().get(&page_id).copied()
    }

    pub fn insert(&self, page_id: PageId, frame_id: FrameId) {
        let partition = self.partition(page_id);
        partition.map.write().insert(page_id, frame_id);
    }

    pub fn remove(&self, page_id: PageId) -> Option<FrameId> {
        let partition = self.partition(page_id);
        partition.map.write().remove(&page_id)
    }

    pub fn remove_if(&self, page_id: PageId, frame_id: FrameId) -> bool {
        let partition = self.partition(page_id);
        let mut guard = partition.map.write();
        match guard.get(&page_id) {
            Some(current) if *current == frame_id => {
                guard.remove(&page_id);
                true
            }
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.partitions
            .iter()
            .map(|partition| partition.map.read().len())
            .sum()
    }
}
