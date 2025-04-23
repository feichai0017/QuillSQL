use super::Replacer; // 引入定义的 trait
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::buffer::FrameId;
use std::collections::HashMap;
use std::time::{Duration, Instant}; // 使用真实时间

const DEFAULT_WINDOW_DURATION: Duration = Duration::from_secs(10); // 默认窗口10秒

#[derive(Debug, Clone)]
struct WindowLFUNode {
    frequency: u64,
    last_access_time: Instant,
    is_evictable: bool,
}

impl WindowLFUNode {
    fn new() -> Self {
        Self {
            frequency: 1, // 初始频率为1
            last_access_time: Instant::now(),
            is_evictable: false, // 初始不可淘汰
        }
    }
}

#[derive(Debug)]
pub struct WindowLFUReplacer {
    node_store: HashMap<FrameId, WindowLFUNode>,
    capacity: usize,
    current_size: usize, // 可淘汰 frame 的数量
    window_duration: Duration,
}

// --- 实现 Replacer Trait ---

impl Replacer for WindowLFUReplacer {
    fn new(capacity: usize) -> Self {
        Self::with_window(capacity, DEFAULT_WINDOW_DURATION)
    }

    fn record_access(&mut self, frame_id: FrameId) -> QuillSQLResult<()> {
        let now = Instant::now();
        if let Some(node) = self.node_store.get_mut(&frame_id) {
            node.frequency += 1;
            node.last_access_time = now;
        } else {
            // 如果缓存已满，不能直接添加，需要先驱逐
            // BufferPoolManager 应该保证在调用 record_access 前已确保有空间
            if self.node_store.len() >= self.capacity {
                 // 通常不应在这里失败，BPM 应该先调用 evict
                return Err(QuillSQLError::Internal(format!(
                    "WindowLFU capacity {} reached when accessing new frame {}",
                    self.capacity, frame_id
                )));
            }
            self.node_store.insert(frame_id, WindowLFUNode::new());
            // 新加入的 frame 初始 pin_count > 0，所以不可淘汰，current_size 不变
        }
        Ok(())
    }

    fn evict(&mut self) -> Option<FrameId> {
        if self.current_size == 0 {
            return None;
        }

        let now = Instant::now();
        let mut victim_frame = None;
        // 使用 Option<(u64, Instant)> 来存储优先级 (有效频率, 最后访问时间)
        // 有效频率越小越优先，时间越早越优先
        let mut min_priority: Option<(u64, Instant)> = None;

        for (&frame_id, node) in self.node_store.iter() {
            if !node.is_evictable {
                continue;
            }

            let effective_frequency = if now.duration_since(node.last_access_time) <= self.window_duration {
                // 在窗口内，使用原始频率
                node.frequency
            } else {
                // 超出窗口，频率视为最低 (0)
                0
            };

            let current_priority = (effective_frequency, node.last_access_time);

            match min_priority {
                None => {
                    min_priority = Some(current_priority);
                    victim_frame = Some(frame_id);
                }
                Some(min_p) => {
                    // 比较优先级：先比频率（越小越优先），再比时间（越早越优先）
                    if effective_frequency < min_p.0 || (effective_frequency == min_p.0 && node.last_access_time < min_p.1) {
                        min_priority = Some(current_priority);
                        victim_frame = Some(frame_id);
                    }
                }
            }
        }

        if let Some(victim_id) = victim_frame {
            self.remove(victim_id); // remove 会处理 node_store 和 current_size
            Some(victim_id)
        } else {
            None // 没有找到可驱逐的 frame
        }
    }

    fn set_evictable(&mut self, frame_id: FrameId, set_evictable: bool) -> QuillSQLResult<()> {
        if let Some(node) = self.node_store.get_mut(&frame_id) {
            let was_evictable = node.is_evictable;
            node.is_evictable = set_evictable;
            if set_evictable && !was_evictable {
                self.current_size += 1;
            } else if !set_evictable && was_evictable {
                // 检查确保 current_size 不会小于 0
                if self.current_size > 0 {
                     self.current_size -= 1;
                } else {
                    // Log warning or handle error: trying to decrease size below zero
                    eprintln!("Warning: Attempted to decrease WindowLFUReplacer size below zero for frame {}", frame_id);
                }
            }
            Ok(())
        } else {
            Err(QuillSQLError::Internal(format!(
                "Frame {} not found in WindowLFUReplacer::set_evictable",
                frame_id
            )))
        }
    }

    fn remove(&mut self, frame_id: FrameId) {
        if let Some(node) = self.node_store.remove(&frame_id) {
            if node.is_evictable {
                 if self.current_size > 0 {
                     self.current_size -= 1;
                 } else {
                     eprintln!("Warning: Attempted to decrease WindowLFUReplacer size below zero during remove for frame {}", frame_id);
                 }
            }
        }
    }

    fn size(&self) -> usize {
        self.current_size
    }
}

// --- Window-LFU 特定方法 ---
impl WindowLFUReplacer {
    /// 创建带有指定窗口大小的 Replacer
    pub fn with_window(capacity: usize, window_duration: Duration) -> Self {
        Self {
            node_store: HashMap::with_capacity(capacity),
            capacity,
            current_size: 0,
            window_duration,
        }
    }
}


// --- 测试用例 ---
#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_window_lfu_basic_eviction() {
        let mut replacer = WindowLFUReplacer::new(3);

        replacer.record_access(1).unwrap(); // freq=1, time=t0
        replacer.record_access(2).unwrap(); // freq=1, time=t1
        replacer.record_access(3).unwrap(); // freq=1, time=t2

        replacer.set_evictable(1, true).unwrap();
        replacer.set_evictable(2, true).unwrap();
        replacer.set_evictable(3, true).unwrap();
        assert_eq!(replacer.size(), 3);

        // Frame 1 最先访问，频率相同，应首先被驱逐
        assert_eq!(replacer.evict(), Some(1));
        assert_eq!(replacer.size(), 2);

        // Frame 2 其次
        assert_eq!(replacer.evict(), Some(2));
        assert_eq!(replacer.size(), 1);

        // Frame 3 最后
        assert_eq!(replacer.evict(), Some(3));
        assert_eq!(replacer.size(), 0);

        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_window_lfu_frequency_effect() {
        let mut replacer = WindowLFUReplacer::new(3);

        replacer.record_access(1).unwrap(); // freq=1
        replacer.record_access(2).unwrap(); // freq=1
        replacer.record_access(3).unwrap(); // freq=1

        replacer.record_access(1).unwrap(); // freq=2
        replacer.record_access(1).unwrap(); // freq=3
        replacer.record_access(2).unwrap(); // freq=2

        replacer.set_evictable(1, true).unwrap();
        replacer.set_evictable(2, true).unwrap();
        replacer.set_evictable(3, true).unwrap();

        // Frame 3 频率最低 (1)，应首先被驱逐
        assert_eq!(replacer.evict(), Some(3));
        // Frame 2 频率其次 (2)
        assert_eq!(replacer.evict(), Some(2));
        // Frame 1 频率最高 (3)
        assert_eq!(replacer.evict(), Some(1));
    }

    #[test]
    fn test_window_lfu_window_effect() {
        // 使用较小的窗口时间进行测试
        let window = Duration::from_millis(50);
        let mut replacer = WindowLFUReplacer::with_window(3, window);

        replacer.record_access(1).unwrap(); // freq=1, time=t0
        sleep(window / 2);
        replacer.record_access(2).unwrap(); // freq=1, time=t1 (in window)
        replacer.record_access(2).unwrap(); // freq=2, time=t2 (in window)

        replacer.set_evictable(1, true).unwrap();
        replacer.set_evictable(2, true).unwrap();

        // 等待超过窗口时间
        sleep(window * 2);

        replacer.record_access(3).unwrap(); // freq=1, time=t3 (now frame 1 and 2 are out of window)
        replacer.set_evictable(3, true).unwrap();


        // Frame 1 和 2 都超出了窗口，频率视为 0
        // Frame 1 的 last_access_time 最早，应该被驱逐
        assert_eq!(replacer.evict(), Some(1));

        // Frame 2 其次 (超出窗口)
        assert_eq!(replacer.evict(), Some(2));

        // Frame 3 在窗口内
        assert_eq!(replacer.evict(), Some(3));
    }

     #[test]
    fn test_window_lfu_set_evictable() {
        let mut replacer = WindowLFUReplacer::new(3);

        replacer.record_access(1).unwrap();
        replacer.record_access(2).unwrap();
        replacer.record_access(3).unwrap();

        replacer.set_evictable(1, true).unwrap();
        replacer.set_evictable(3, true).unwrap();
        assert_eq!(replacer.size(), 2);

        // Frame 2 不可淘汰，Frame 1 先访问，被驱逐
        assert_eq!(replacer.evict(), Some(1));
        assert_eq!(replacer.size(), 1);

        // 使 Frame 2 可淘汰，Frame 3 不可淘汰
        replacer.set_evictable(2, true).unwrap();
        replacer.set_evictable(3, false).unwrap();
        assert_eq!(replacer.size(), 1);

        // 只能驱逐 Frame 2
        assert_eq!(replacer.evict(), Some(2));
        assert_eq!(replacer.size(), 0);
    }

     #[test]
    fn test_window_lfu_remove() {
        let mut replacer = WindowLFUReplacer::new(3);
        replacer.record_access(1).unwrap();
        replacer.record_access(2).unwrap();
        replacer.set_evictable(1, true).unwrap();
        replacer.set_evictable(2, true).unwrap();
        assert_eq!(replacer.size(), 2);

        replacer.remove(1); // 移除可淘汰的 frame 1
        assert_eq!(replacer.size(), 1);
        assert!(replacer.node_store.get(&1).is_none());

        replacer.record_access(3).unwrap(); // 添加 frame 3，初始不可淘汰
        assert_eq!(replacer.size(), 1); // size 不变

        replacer.remove(3); // 移除不可淘汰的 frame 3
        assert_eq!(replacer.size(), 1); // size 不变
        assert!(replacer.node_store.get(&3).is_none());

        assert_eq!(replacer.evict(), Some(2)); // 只能淘汰 frame 2
    }

    #[test]
    fn test_window_lfu_capacity() {
        let mut replacer = WindowLFUReplacer::new(2);
        replacer.record_access(1).unwrap();
        replacer.record_access(2).unwrap();
        replacer.set_evictable(1, true).unwrap();
        replacer.set_evictable(2, true).unwrap();

        // 尝试访问超出容量的 frame，应该失败（或由 BPM 处理）
        // 这里我们测试 record_access 在容量满时的行为 (假设它会报错)
        assert!(replacer.record_access(3).is_err());

        // 驱逐一个
        assert_eq!(replacer.evict(), Some(1));

        // 现在可以访问了 (假设 BPM 调用 record_access)
        replacer.record_access(3).unwrap();
        replacer.set_evictable(3, true).unwrap();
        assert_eq!(replacer.size(), 2);
        assert!(replacer.node_store.contains_key(&3));
    }
}
