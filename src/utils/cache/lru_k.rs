use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use crate::utils::cache::FrameId;
#[derive(Debug)]
pub struct LruKReplacer {
    k: usize,
    // Stores the *timestamps* of the last k accesses for *all* frames currently
    // being tracked (pinned or unpinned).
    access_history: HashMap<FrameId, VecDeque<Instant>>,
    // Stores the set of frames that are candidates for eviction (pin_count == 0).
    evictable_frames: HashSet<FrameId>,
    current_timestamp: Instant, // Use a monotonic clock source
}

impl LruKReplacer {
    pub fn new(k: usize) -> Self {
        assert!(k > 0, "k must be greater than 0");
        LruKReplacer {
            k,
            access_history: HashMap::new(),
            evictable_frames: HashSet::new(),
            current_timestamp: Instant::now(), // Initialize clock
        }
    }

    /// Records an access to a frame. Called when a page is pinned.
    /// This function should be called *before* the frame is pinned,
    /// or at least before set_evictable(frame_id, false) is called.
    pub fn record_access(&mut self, frame_id: FrameId) {
        let history = self
            .access_history
            .entry(frame_id)
            .or_insert_with(VecDeque::new);
        history.push_back(self.current_timestamp.clone()); // Record current time
        if history.len() > self.k {
            history.pop_front(); // Keep only the last K accesses
        }
    }

    /// Marks a frame's eviction status.
    /// - `set_evictable(.., true)`: Called when a frame's pin count becomes 0.
    /// - `set_evictable(.., false)`: Called when a frame's pin count becomes > 0.
    pub fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        if evictable {
            // Frame becomes evictable, add to set if it has history
            if self.access_history.contains_key(&frame_id) {
                self.evictable_frames.insert(frame_id);
            } else {
                // Should not happen: cannot make a frame evictable if it was never accessed
                eprintln!(
                    "WARN: set_evictable(true) called for frame {} with no access history.",
                    frame_id
                );
            }
        } else {
            // Frame becomes non-evictable (pinned), remove from set
            self.evictable_frames.remove(&frame_id);
        }
    }

    /// Removes frame state entirely (e.g., when page is deleted).
    /// Called *after* the frame is unpinned (if necessary) and removed
    /// from the evictable set by set_evictable(frame_id, false).
    pub fn remove(&mut self, frame_id: FrameId) {
        // Ensure it's not marked as evictable anymore
        self.evictable_frames.remove(&frame_id);
        // Remove its access history
        self.access_history.remove(&frame_id);
    }

    /// Finds the frame to evict based on LRU-K logic among evictable frames.
    /// Returns the FrameId of the victim frame if found.
    pub fn evict(&mut self) -> Option<FrameId> {
        self.current_timestamp = Instant::now(); // Update time for distance calculation

        let mut victim: Option<FrameId> = None;
        let mut max_k_distance = Duration::ZERO; // Max duration = oldest Kth access
        let mut oldest_first_access = Instant::now(); // Track oldest for frames with < k history
        let mut oldest_inf_dist_frame: Option<FrameId> = None;

        for frame_id in self.evictable_frames.iter() {
            if let Some(history) = self.access_history.get(frame_id) {
                if history.len() < self.k {
                    // Treat as having infinite K-distance.
                    // Among these, choose the one with the oldest *most recent* access (LRU).
                    // Or, following the paper more closely: choose the one whose first recorded
                    // access is oldest. Let's use the first access time.
                    if let Some(first_ts) = history.front() {
                        if oldest_inf_dist_frame.is_none() || *first_ts < oldest_first_access {
                            oldest_first_access = *first_ts;
                            oldest_inf_dist_frame = Some(*frame_id);
                        }
                    } else {
                        // Should not happen if set_evictable logic is correct (history exists)
                        eprintln!(
                            "WARN: Evictable frame {} has empty access history.",
                            frame_id
                        );
                        // Consider this frame immediately if found (as it has no history)
                        if oldest_inf_dist_frame.is_none() {
                            oldest_inf_dist_frame = Some(*frame_id);
                        }
                    }
                } else {
                    // Has K accesses, calculate K-th distance (time since K-th oldest access)
                    // The K-th oldest is the first element in the deque
                    if let Some(kth_oldest_ts) = history.front() {
                        let k_distance = self.current_timestamp.duration_since(*kth_oldest_ts);
                        if k_distance >= max_k_distance {
                            // >= favors evicting older ones in case of tie
                            max_k_distance = k_distance;
                            victim = Some(*frame_id);
                        }
                    } else {
                        // Should not happen if history.len() == k
                        eprintln!(
                            "WARN: Frame {} reported K accesses but history is empty.",
                            frame_id
                        );
                    }
                }
            } else {
                // Should not happen if set_evictable logic is correct
                eprintln!(
                    "WARN: Evictable frame {} not found in access history.",
                    frame_id
                );
            }
        }

        // Decide final victim
        let chosen_victim = if victim.is_some() {
            victim // Prefer evicting frames with K accesses based on oldest Kth access
        } else {
            oldest_inf_dist_frame // Otherwise, evict the frame with < K accesses that was accessed earliest
        };

        // Remove the chosen victim from the replacer state before returning
        if let Some(victim_id) = chosen_victim {
            self.remove(victim_id);
        }

        chosen_victim
    }

    /// Returns the number of evictable frames being tracked.
    pub fn size(&self) -> usize {
        self.evictable_frames.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread, time::Duration};

    #[test]
    fn test_lru_k_basic() {
        let k = 2;
        let mut replacer = LruKReplacer::new(k);

        // Access frames 1, 2, 3 once
        replacer.record_access(1);
        replacer.record_access(2);
        replacer.record_access(3);

        // Mark all as evictable
        replacer.set_evictable(1, true);
        replacer.set_evictable(2, true);
        replacer.set_evictable(3, true);
        assert_eq!(replacer.size(), 3);

        // Evict - should be 1 (oldest first access, < K accesses)
        assert_eq!(replacer.evict(), Some(1));
        assert_eq!(replacer.size(), 2);

        // Access 2 again (now has 2 accesses)
        replacer.record_access(2);
        thread::sleep(Duration::from_millis(5)); // Ensure time passes

        // Access 3 again (now has 2 accesses)
        replacer.record_access(3);
        thread::sleep(Duration::from_millis(5));

        // Mark 2 and 3 as evictable again (evict removes them)
        replacer.set_evictable(2, true);
        replacer.set_evictable(3, true);
        assert_eq!(replacer.size(), 2);

        // Access 4 once
        replacer.record_access(4);
        replacer.set_evictable(4, true);
        assert_eq!(replacer.size(), 3);

        // Evict - should be 4 (infinite K-dist, accessed later than 2's/3's first access)
        // Correction: Infinite K-dist frames are evicted based on *oldest* first access.
        // 2 and 3 have K accesses now. 4 has < K.
        // Compare Kth distance for 2 and 3. 2's Kth (first) access is older.
        assert_eq!(replacer.evict(), Some(2));
        assert_eq!(replacer.size(), 2); // 3 and 4 remain

        // Evict again - should be 3 (its Kth access is older than 4's "infinite")
        assert_eq!(replacer.evict(), Some(3));
        assert_eq!(replacer.size(), 1); // 4 remains

        // Evict again - should be 4
        assert_eq!(replacer.evict(), Some(4));
        assert_eq!(replacer.size(), 0);

        // Evict empty
        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_lru_k_pinning() {
        let k = 2;
        let mut replacer = LruKReplacer::new(k);

        replacer.record_access(1);
        replacer.record_access(2);
        replacer.set_evictable(1, true);
        replacer.set_evictable(2, true);
        assert_eq!(replacer.size(), 2);

        // Pin frame 1
        replacer.set_evictable(1, false);
        assert_eq!(replacer.size(), 1);

        // Evict should return 2
        assert_eq!(replacer.evict(), Some(2));
        assert_eq!(replacer.size(), 0);
        assert_eq!(replacer.evict(), None); // Nothing left to evict

        // Unpin 1, it becomes evictable
        replacer.set_evictable(1, true);
        assert_eq!(replacer.size(), 1);

        // Evict should return 1
        assert_eq!(replacer.evict(), Some(1));
        assert_eq!(replacer.size(), 0);
    }

    #[test]
    fn test_lru_k_remove() {
        let k = 1;
        let mut replacer = LruKReplacer::new(k);
        replacer.record_access(1);
        replacer.set_evictable(1, true);
        assert_eq!(replacer.size(), 1);

        replacer.remove(1); // Remove frame 1 completely
        assert_eq!(replacer.size(), 0);
        assert!(!replacer.evictable_frames.contains(&1));
        assert!(!replacer.access_history.contains_key(&1));
        assert_eq!(replacer.evict(), None);
    }
}
