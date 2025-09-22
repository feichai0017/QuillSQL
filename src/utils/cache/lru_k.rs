use super::Replacer;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::buffer::FrameId;
use std::collections::{HashMap, LinkedList};

#[derive(Debug)]
struct LRUKNode {
    k: usize,
    // 该frame最近k次被访问的时间
    history: LinkedList<u64>,
    // 是否可被置换
    is_evictable: bool,
}
impl LRUKNode {
    pub fn new(k: usize) -> Self {
        Self {
            k,
            history: LinkedList::new(),
            is_evictable: false,
        }
    }
    pub fn record_access(&mut self, timestamp: u64) {
        self.history.push_back(timestamp);
        if self.history.len() > self.k {
            self.history.pop_front();
        }
    }
}

#[derive(Debug)]
pub struct LRUKReplacer {
    // 当前可置换的frame数
    current_size: usize,
    // 可置换的frame数上限
    replacer_size: usize,
    k: usize,
    node_store: HashMap<FrameId, LRUKNode>,
    // 当前时间戳（从0递增）
    current_timestamp: u64,
}

impl Replacer for LRUKReplacer {
    fn new(capacity: usize) -> Self {
        // Use a default K value or make K configurable via another mechanism if needed
        // For now, let's assume a reasonable default K, e.g., 2
        const DEFAULT_K: usize = 2;
        Self {
            current_size: 0,
            replacer_size: capacity, // Use capacity from trait
            k: DEFAULT_K,
            node_store: HashMap::with_capacity(capacity),
            current_timestamp: 0,
        }
    }

    // 驱逐 evictable 且具有最大 k-distance 的 frame
    fn evict(&mut self) -> Option<FrameId> {
        let mut max_k_distance = 0;
        let mut result = None;
        for (frame_id, node) in self.node_store.iter() {
            if !node.is_evictable {
                continue;
            }
            let k_distance = if node.history.len() < self.k {
                u64::MAX - node.history.front().unwrap()
            } else {
                self.current_timestamp - node.history.front().unwrap()
            };
            if k_distance > max_k_distance {
                max_k_distance = k_distance;
                result = Some(*frame_id);
            }
        }
        if let Some(frame_id) = result {
            self.remove(frame_id);
        }
        result
    }

    // 记录frame的访问
    fn record_access(&mut self, frame_id: FrameId) -> QuillSQLResult<()> {
        if let Some(node) = self.node_store.get_mut(&frame_id) {
            node.record_access(self.current_timestamp);
            self.current_timestamp += 1;
        } else {
            // 创建新node
            if self.node_store.len() >= self.replacer_size {
                return Err(QuillSQLError::Internal(
                    "frame size exceeds the limit".to_string(),
                ));
            }
            let mut node = LRUKNode::new(self.k);
            node.record_access(self.current_timestamp);
            self.current_timestamp += 1;
            self.node_store.insert(frame_id, node);
        }
        Ok(())
    }

    // 设置frame是否可被置换
    fn set_evictable(&mut self, frame_id: FrameId, set_evictable: bool) -> QuillSQLResult<()> {
        if let Some(node) = self.node_store.get_mut(&frame_id) {
            let evictable = node.is_evictable;
            node.is_evictable = set_evictable;
            if set_evictable && !evictable {
                self.current_size += 1;
            } else if !set_evictable && evictable {
                self.current_size -= 1;
            }
            Ok(())
        } else {
            Err(QuillSQLError::Internal("frame not found".to_string()))
        }
    }

    // 移除frame
    fn remove(&mut self, frame_id: FrameId) {
        if let Some(node) = self.node_store.get(&frame_id) {
            assert!(node.is_evictable, "frame is not evictable");
            self.node_store.remove(&frame_id);
            self.current_size -= 1;
        }
    }

    // 获取当前可置换的frame数
    fn size(&self) -> usize {
        self.current_size
    }
}

impl LRUKReplacer {
    pub fn with_k(num_frames: usize, k: usize) -> Self {
        Self {
            current_size: 0,
            replacer_size: num_frames,
            k,
            node_store: HashMap::new(),
            current_timestamp: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::cache::Replacer; // Import the trait

    #[test]
    pub fn test_lru_k_set_evictable() {
        let mut replacer = LRUKReplacer::with_k(3, 2); // Use specific constructor
        replacer.record_access(1).unwrap();
        replacer.set_evictable(1, true).unwrap();
        assert_eq!(replacer.size(), 1);
        replacer.set_evictable(1, false).unwrap();
        assert_eq!(replacer.size(), 0);
    }

    #[test]
    pub fn test_lru_k_evict_all_pages_at_least_k() {
        let mut replacer = LRUKReplacer::with_k(2, 3);
        replacer.record_access(1).unwrap();
        replacer.record_access(2).unwrap();
        replacer.record_access(2).unwrap();
        replacer.record_access(1).unwrap(); // ts=3
        replacer.record_access(2).unwrap(); // ts=4
        replacer.set_evictable(1, true).unwrap();
        replacer.set_evictable(2, true).unwrap();
        // Frame 1 history: [0, 3], k-dist = 5 - 0 = 5
        // Frame 2 history: [1, 2, 4], k-dist = 5 - 1 = 4
        let frame_id = replacer.evict();
        assert_eq!(frame_id, Some(1)); // Frame 1 has larger k-distance
    }

    #[test]
    pub fn test_lru_k_evict_some_page_less_than_k() {
        let mut replacer = LRUKReplacer::with_k(3, 3);
        replacer.record_access(1).unwrap(); // ts=0
        replacer.record_access(2).unwrap(); // ts=1, history < k
        replacer.record_access(3).unwrap(); // ts=2, history < k
        replacer.record_access(1).unwrap(); // ts=3
        replacer.record_access(1).unwrap(); // ts=4, history = [0, 3, 4]
        replacer.record_access(3).unwrap(); // ts=5, history = [2, 5]
                                            // Frame 1: history=[0,3,4], k=3, k-dist = 6-0 = 6
                                            // Frame 2: history=[1], k=3, infinite k-dist, oldest_ts=1
                                            // Frame 3: history=[2,5], k=3, infinite k-dist, oldest_ts=2
        replacer.set_evictable(1, true).unwrap();
        replacer.set_evictable(2, true).unwrap();
        replacer.set_evictable(3, true).unwrap();
        // Evict should prioritize finite k-dist (frame 1), BUT the logic was updated
        // to prioritize infinite k-dist with oldest timestamp. Frame 2 (ts=1) is older than Frame 3 (ts=2).
        let frame_id = replacer.evict();
        assert_eq!(frame_id, Some(2));
    }

    #[test]
    pub fn test_lru_k_test_case() {
        let mut lru_replacer = LRUKReplacer::with_k(7, 2); // k=2

        // Scenario: add six elements
        lru_replacer.record_access(1).unwrap(); // ts=0
        lru_replacer.record_access(2).unwrap(); // ts=1
        lru_replacer.record_access(3).unwrap(); // ts=2
        lru_replacer.record_access(4).unwrap(); // ts=3
        lru_replacer.record_access(5).unwrap(); // ts=4
        lru_replacer.record_access(6).unwrap(); // ts=5
        lru_replacer.set_evictable(1, true).unwrap();
        lru_replacer.set_evictable(2, true).unwrap();
        lru_replacer.set_evictable(3, true).unwrap();
        lru_replacer.set_evictable(4, true).unwrap();
        lru_replacer.set_evictable(5, true).unwrap();
        lru_replacer.set_evictable(6, false).unwrap(); // 6 not evictable initially
        assert_eq!(5, lru_replacer.size());

        // Scenario: access frame 1 again
        lru_replacer.record_access(1).unwrap(); // ts=6, history=[0, 6]
                                                // Frame 1: k=2, k-dist = 7-0 = 7
                                                // Frame 2: history=[1], k=2, inf k-dist, oldest=1
                                                // Frame 3: history=[2], k=2, inf k-dist, oldest=2
                                                // Frame 4: history=[3], k=2, inf k-dist, oldest=3
                                                // Frame 5: history=[4], k=2, inf k-dist, oldest=4

        // Scenario: Evict three pages.
        // Infinite distance frames first, ordered by oldest timestamp.
        let value = lru_replacer.evict(); // Evict frame 2 (oldest inf dist)
        assert_eq!(Some(2), value);
        let value = lru_replacer.evict(); // Evict frame 3
        assert_eq!(Some(3), value);
        let value = lru_replacer.evict(); // Evict frame 4
        assert_eq!(Some(4), value);
        assert_eq!(lru_replacer.size(), 2); // Remaining: [1, 5] (6 is non-evictable)

        // Scenario: Insert new frames 3, 4, update 5
        lru_replacer.record_access(3).unwrap(); // ts=7, history=[7], inf k-dist, oldest=7
        lru_replacer.record_access(4).unwrap(); // ts=8, history=[8], inf k-dist, oldest=8
        lru_replacer.record_access(5).unwrap(); // ts=9, history=[4, 9], k-dist=10-4=6
        lru_replacer.record_access(4).unwrap(); // ts=10, history=[8, 10], k-dist=11-8=3
        lru_replacer.set_evictable(3, true).unwrap();
        lru_replacer.set_evictable(4, true).unwrap();
        assert_eq!(4, lru_replacer.size()); // Now [1, 5, 3, 4] are evictable
                                            // Frame 1: hist=[0, 6], k=2, k-dist = 11-0 = 11
                                            // Frame 5: hist=[4, 9], k=2, k-dist = 11-4 = 7
                                            // Frame 3: hist=[7], k=2, inf k-dist, oldest=7
                                            // Frame 4: hist=[8, 10], k=2, k-dist = 11-8 = 3

        // Scenario: Evict next.
        // Should be infinite dist frame 3 (oldest_ts=7)
        let value = lru_replacer.evict();
        assert_eq!(Some(3), value);
        assert_eq!(3, lru_replacer.size()); // Remaining: [1, 5, 4]

        // Set 6 to be evictable.
        lru_replacer.set_evictable(6, true).unwrap();
        assert_eq!(4, lru_replacer.size()); // Now [1, 5, 4, 6]
                                            // Frame 6: hist=[5], k=2, inf k-dist, oldest=5
                                            // Evict should be frame 6 (oldest inf dist)
        let value = lru_replacer.evict();
        assert_eq!(Some(6), value);
        assert_eq!(3, lru_replacer.size()); // Remaining: [1, 5, 4]

        // Set 1 to non-evictable
        lru_replacer.set_evictable(1, false).unwrap();
        assert_eq!(2, lru_replacer.size()); // Remaining evictable: [5, 4]
                                            // Frame 5: k-dist=7
                                            // Frame 4: k-dist=3
                                            // Evict should be frame 5 (max k-dist)
        let value = lru_replacer.evict();
        assert_eq!(Some(5), value);
        assert_eq!(1, lru_replacer.size()); // Remaining evictable: [4]

        // Update access history for 1
        lru_replacer.record_access(1).unwrap(); // ts=11, hist=[6, 11]
        lru_replacer.record_access(1).unwrap(); // ts=12, hist=[11, 12]
        lru_replacer.set_evictable(1, true).unwrap();
        assert_eq!(2, lru_replacer.size()); // Evictable: [4, 1]
                                            // Frame 4: k-dist=3
                                            // Frame 1: k-dist=13-11=2
                                            // Evict should be frame 4 (max k-dist)
        let value = lru_replacer.evict();
        assert_eq!(Some(4), value);

        assert_eq!(1, lru_replacer.size()); // Remaining: [1]
        let value = lru_replacer.evict();
        assert_eq!(Some(1), value);
        assert_eq!(0, lru_replacer.size());

        // Empty replacer
        assert_eq!(None, lru_replacer.evict());
        assert_eq!(0, lru_replacer.size());
        // Remove on non-existent frame should do nothing silently in remove(), size unchanged
        // lru_replacer.remove(1); // Let's skip this, remove expects node to exist
        assert_eq!(0, lru_replacer.size());
    }
}
