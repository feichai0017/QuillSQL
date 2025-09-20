use super::lru_k::LRUKReplacer;
use super::Replacer;
use crate::buffer::FrameId;
use crate::error::QuillSQLResult;

/// A thin wrapper that shards LRUKReplacer to reduce lock contention.
/// It exposes the same minimal API used by BufferPoolManager.
#[derive(Debug)]
pub struct ShardedLRUKReplacer {
    shards: Vec<LRUKReplacer>,
    mask: usize,
}

impl ShardedLRUKReplacer {
    pub fn new(capacity: usize) -> Self {
        // choose shard count as power-of-two up to 8
        let mut shards = 1usize;
        while shards < 8 && shards * 64 < capacity {
            shards <<= 1;
        }
        Self::with_shards(capacity, shards)
    }

    pub fn with_shards(capacity: usize, shards: usize) -> Self {
        let shards_pow2 = shards.next_power_of_two().max(1);
        let per = (capacity + shards_pow2 - 1) / shards_pow2;
        let v = (0..shards_pow2).map(|_| LRUKReplacer::new(per)).collect();
        Self {
            shards: v,
            mask: shards_pow2 - 1,
        }
    }

    #[inline]
    fn shard_id(&self, frame_id: FrameId) -> usize {
        frame_id & self.mask
    }

    pub fn record_access(&mut self, frame_id: FrameId) -> QuillSQLResult<()> {
        let sid = self.shard_id(frame_id);
        self.shards[sid].record_access(frame_id)
    }

    pub fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) -> QuillSQLResult<()> {
        let sid = self.shard_id(frame_id);
        self.shards[sid].set_evictable(frame_id, evictable)
    }

    pub fn remove(&mut self, frame_id: FrameId) {
        let sid = self.shard_id(frame_id);
        self.shards[sid].remove(frame_id)
    }

    pub fn evict(&mut self) -> Option<FrameId> {
        // simple round scan; favor earlier shard
        for s in 0..self.shards.len() {
            if let Some(fid) = self.shards[s].evict() {
                return Some(fid);
            }
        }
        None
    }

    pub fn size(&self) -> usize {
        self.shards.iter().map(|s| s.size()).sum()
    }
}
