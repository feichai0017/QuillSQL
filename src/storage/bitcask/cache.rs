// src/storage/bitcask/cache.rs
use crate::error::Result;
use crate::storage::bitcask::datafile::Entry;
use crate::utils::cache::lru_k::LRUKReplacer;
use crate::utils::cache::Replacer;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

const DEFAULT_K: usize = 2;

pub struct BitcaskCache<K, V>
where
    K: Eq + Hash + Clone + Debug,
{
    cache: HashMap<K, CacheEntry<V>>,
    replacer: LRUKReplacer,
    capacity: usize,
}

struct CacheEntry<V> {
    value: V,
    frame_id: usize,
}

impl<K, V> BitcaskCache<K, V>
where
    K: Eq + Hash + Clone + Debug,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(capacity),
            replacer: LRUKReplacer::with_k(capacity, DEFAULT_K),
            capacity,
        }
    }

    pub fn new_with_k(capacity: usize, k: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(capacity),
            replacer: LRUKReplacer::with_k(capacity, k),
            capacity,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        let frame_id = self.key_to_frame_id(key);

        // 记录访问
        if self.cache.contains_key(key) {
            self.replacer.record_access(frame_id).ok()?;
            return Some(&self.cache.get(key)?.value);
        }

        None
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let frame_id = self.key_to_frame_id(key);

        // 记录访问
        if self.cache.contains_key(key) {
            self.replacer.record_access(frame_id).ok()?;
            return Some(&mut self.cache.get_mut(key)?.value);
        }

        None
    }

    pub fn put(&mut self, key: K, value: V) -> Result<()> {
        let frame_id = self.key_to_frame_id(&key);

        // 如果已存在，直接更新
        if self.cache.contains_key(&key) {
            self.cache.insert(key, CacheEntry { value, frame_id });
            self.replacer.record_access(frame_id)?;
            return Ok(());
        }

        // 如果缓存已满，需要淘汰
        if self.cache.len() >= self.capacity {
            if let Some(victim_frame_id) = self.replacer.evict() {
                // 找到对应的key并删除
                let victim_key = self
                    .cache
                    .iter()
                    .find(|(_, v)| v.frame_id == victim_frame_id)
                    .map(|(k, _)| k.clone());

                if let Some(victim_key) = victim_key {
                    self.cache.remove(&victim_key);
                }
            }
        }

        // 插入新条目
        self.cache.insert(key, CacheEntry { value, frame_id });
        self.replacer.record_access(frame_id)?;
        self.replacer.set_evictable(frame_id, true)?;

        Ok(())
    }

    pub fn remove(&mut self, key: &K) {
        if let Some(entry) = self.cache.remove(key) {
            self.replacer.remove(entry.frame_id);
        }
    }

    // 将key转换为frame_id，这里使用简单hash
    fn key_to_frame_id(&self, key: &K) -> usize {
        let mut hash: usize = 0;
        for byte in format!("{:?}", key).bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as usize);
        }
        hash % self.capacity
    }
}

// 为原来的Entry缓存提供向后兼容性的类型别名
pub type EntryCache = BitcaskCache<Vec<u8>, Entry>;
