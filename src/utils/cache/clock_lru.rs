use crate::error::{QuillSQLError, QuillSQLResult};
use crate::buffer::FrameId;
use crate::utils::cache::Replacer;
use std::collections::HashMap;

/// Clock LRU缓存替换算法实现
/// 使用时钟算法（Clock Algorithm）实现近似LRU
#[derive(Debug)]
pub struct ClockLRUReplacer {
    // 当前可替换的frame数量
    current_size: usize,
    // 最大容量
    capacity: usize,
    // 时钟指针，指向下一个要检查的位置
    clock_hand: usize,
    // 存储frame信息的向量
    frames: Vec<Option<FrameEntry>>,
    // FrameId到frames索引的映射
    frame_map: HashMap<FrameId, usize>,
}

/// Frame条目，存储每个frame的状态
#[derive(Debug, Clone)]
struct FrameEntry {
    // frame ID
    frame_id: FrameId,
    // 引用位，表示最近是否被访问过
    referenced: bool,
    // 是否可被替换
    is_evictable: bool,
}

impl FrameEntry {
    fn new(frame_id: FrameId) -> Self {
        Self {
            frame_id,
            referenced: false,  // 初始化时引用位为false
            is_evictable: false,
        }
    }
}

impl Replacer for ClockLRUReplacer {
    /// 创建一个新的ClockLRUReplacer实例
    fn new(capacity: usize) -> Self {
        Self {
            current_size: 0,
            capacity,
            clock_hand: 0,
            frames: vec![None; capacity],
            frame_map: HashMap::with_capacity(capacity),
        }
    }

    /// 记录frame的访问
    /// 如果frame已存在，将其引用位设为true
    /// 如果frame不存在，添加到缓存中
    fn record_access(&mut self, frame_id: FrameId) -> QuillSQLResult<()> {
        if let Some(&index) = self.frame_map.get(&frame_id) {
            // frame已存在，将引用位设为true
            if let Some(entry) = &mut self.frames[index] {
                entry.referenced = true;
            }
        } else {
            // frame不存在，需要添加
            if self.frame_map.len() >= self.capacity {
                return Err(QuillSQLError::Internal("frame size exceeds capacity".to_string()));
            }
            
            // 寻找一个空位
            let mut slot_index = None;
            for (i, slot) in self.frames.iter().enumerate() {
                if slot.is_none() {
                    slot_index = Some(i);
                    break;
                }
            }
            
            let index = slot_index.unwrap_or_else(|| {
                // 如果没有空位，选择第一个未占用的位置
                // 这种情况实际上不应该发生，因为我们已经检查了容量
                panic!("No available slot found, this should not happen");
            });
            
            // 创建新条目并添加到frames中
            let entry = FrameEntry::new(frame_id);  // 初始化引用位为false
            self.frames[index] = Some(entry);
            self.frame_map.insert(frame_id, index);
        }
        
        Ok(())
    }

    /// 驱逐一个frame
    /// 使用时钟算法选择要替换的frame
    fn evict(&mut self) -> Option<FrameId> {
        // 如果没有可驱逐的frame，直接返回None
        if self.current_size == 0 {
            return None;
        }
        
        // 记录起始位置，确保我们不会无限循环
        let start_clock_hand = self.clock_hand;
        let mut first_evictable = None;
        let mut first_evictable_pos = None;
        
        // 第一轮：寻找第一个引用位为false的可驱逐frame
        loop {
            if let Some(entry) = &mut self.frames[self.clock_hand] {
                if entry.is_evictable {
                    if first_evictable.is_none() {
                        // 记录第一个找到的可驱逐frame
                        first_evictable = Some(entry.frame_id);
                        first_evictable_pos = Some(self.clock_hand);
                    }
                    
                    if !entry.referenced {
                        // 找到引用位为false的可驱逐frame
                        let frame_id = entry.frame_id;
                        let current_pos = self.clock_hand;
                        
                        // 移动时钟指针
                        self.clock_hand = (self.clock_hand + 1) % self.capacity;
                        
                        // 移除frame
                        self.frames[current_pos] = None;
                        self.frame_map.remove(&frame_id);
                        self.current_size -= 1;
                        
                        return Some(frame_id);
                    } else {
                        // 引用位为true，设置为false
                        entry.referenced = false;
                    }
                }
            }
            
            // 移动时钟指针
            self.clock_hand = (self.clock_hand + 1) % self.capacity;
            
            // 如果已经遍历了一圈，退出循环
            if self.clock_hand == start_clock_hand {
                break;
            }
        }
        
        // 第二轮：如果所有可驱逐frame的引用位都是true，选择第一个找到的可驱逐frame
        if let Some(frame_id) = first_evictable {
            if let Some(pos) = first_evictable_pos {
                // 移除frame
                self.frames[pos] = None;
                self.frame_map.remove(&frame_id);
                self.current_size -= 1;
                
                // 移动时钟指针到下一个位置
                self.clock_hand = (pos + 1) % self.capacity;
                
                return Some(frame_id);
            }
        }
        
        None
    }

    /// 设置frame是否可被驱逐
    fn set_evictable(&mut self, frame_id: FrameId, set_evictable: bool) -> QuillSQLResult<()> {
        if let Some(&index) = self.frame_map.get(&frame_id) {
            if let Some(entry) = &mut self.frames[index] {
                // 更新可驱逐状态
                let old_evictable = entry.is_evictable;
                entry.is_evictable = set_evictable;
                
                // 更新可驱逐frame计数
                if !old_evictable && set_evictable {
                    self.current_size += 1;
                } else if old_evictable && !set_evictable {
                    self.current_size -= 1;
                }
                
                return Ok(());
            }
        }
        
        Err(QuillSQLError::Internal(format!("frame {} not found", frame_id)))
    }

    /// 从缓存中移除指定frame
    fn remove(&mut self, frame_id: FrameId) {
        if let Some(index) = self.frame_map.remove(&frame_id) {
            if let Some(entry) = &self.frames[index] {
                if entry.is_evictable {
                    self.current_size -= 1;
                }
            }
            self.frames[index] = None;
        }
    }

    /// 获取当前可驱逐的frame数量
    fn size(&self) -> usize {
        self.current_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_clock_lru_basic() {
        let mut replacer = ClockLRUReplacer::new(3);
        
        // 添加三个frame
        replacer.record_access(1).unwrap();
        replacer.record_access(2).unwrap();
        replacer.record_access(3).unwrap();
        
        // 设置为可驱逐
        replacer.set_evictable(1, true).unwrap();
        replacer.set_evictable(2, true).unwrap();
        replacer.set_evictable(3, true).unwrap();
        
        assert_eq!(replacer.size(), 3);
        
        // 第一个被驱逐的应该是frame 1，因为时钟指针从0开始
        let evicted = replacer.evict();
        assert_eq!(evicted, Some(1));
        assert_eq!(replacer.size(), 2);
    }
    
    #[test]
    fn test_clock_lru_reference_bit() {
        let mut replacer = ClockLRUReplacer::new(3);
        
        // 添加三个frame
        replacer.record_access(1).unwrap();
        replacer.record_access(2).unwrap();
        replacer.record_access(3).unwrap();
        
        // 设置为可驱逐
        replacer.set_evictable(1, true).unwrap();
        replacer.set_evictable(2, true).unwrap();
        replacer.set_evictable(3, true).unwrap();
        
        // 访问frame 1，设置引用位
        replacer.record_access(1).unwrap();
        
        // 现在frame 1的引用位为true，其他frame的引用位为false
        // 时钟指针从0开始，首先检查frame 1，发现引用位为true，设为false并移动到frame 2
        // 然后检查frame 2，引用位为false，应该淘汰frame 2
        let evicted = replacer.evict();
        assert_eq!(evicted, Some(2));
        
        // 时钟指针现在应该指向frame 3的位置
        // frame 1的引用位已被重置为false
        // 下一个被淘汰的应该是frame 3
        let evicted = replacer.evict();
        assert_eq!(evicted, Some(3));
        
        // 只剩下frame 1
        assert_eq!(replacer.size(), 1);
    }
    
    
    #[test]
    fn test_clock_lru_remove() {
        let mut replacer = ClockLRUReplacer::new(3);
        
        // 添加三个frame
        replacer.record_access(1).unwrap();
        replacer.record_access(2).unwrap();
        replacer.record_access(3).unwrap();
        
        // 设置为可驱逐
        replacer.set_evictable(1, true).unwrap();
        replacer.set_evictable(2, true).unwrap();
        replacer.set_evictable(3, true).unwrap();
        
        assert_eq!(replacer.size(), 3);
        
        // 移除frame 2
        replacer.remove(2);
        
        assert_eq!(replacer.size(), 2);
        
        // 应该驱逐frame 1
        let evicted = replacer.evict();
        assert_eq!(evicted, Some(1));
        
        // 只剩下frame 3
        assert_eq!(replacer.size(), 1);
    }
    
    #[test]
    fn test_clock_lru_complex() {
        let mut replacer = ClockLRUReplacer::new(5);
        
        // 添加五个frame
        for i in 1..=5 {
            replacer.record_access(i).unwrap();
            replacer.set_evictable(i, true).unwrap();
        }
        
        assert_eq!(replacer.size(), 5);
        
        // 访问frame 2和4，设置它们的引用位
        replacer.record_access(2).unwrap();
        replacer.record_access(4).unwrap();
        
        // 第一轮驱逐应该是frame 1，因为时钟指针从0开始
        let evicted = replacer.evict();
        assert_eq!(evicted, Some(1));
        
        // 第二轮应该是frame 3，因为frame 2的引用位被设置
        // frame 2的引用位会被重置为false
        let evicted = replacer.evict();
        assert_eq!(evicted, Some(3));
        
        // 第三轮应该是frame 5，因为frame 4的引用位被设置
        // frame 4的引用位会被重置为false
        let evicted = replacer.evict();
        assert_eq!(evicted, Some(5));
        
        // 最后剩下frame 2和4，但引用位都已被重置为false
        assert_eq!(replacer.size(), 2);
        
        // 再访问一次frame 2，设置引用位
        replacer.record_access(2).unwrap();
        
        // 下一个驱逐应该是frame 4
        let evicted = replacer.evict();
        assert_eq!(evicted, Some(4));
        
        // 只剩下frame 2
        assert_eq!(replacer.size(), 1);
    }
}