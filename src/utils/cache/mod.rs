use crate::error::Result;
use crate::storage::b_plus_tree::buffer_pool_manager::FrameId;

pub mod clock_lru;
pub mod lru_k;
pub mod window_lfu;

pub trait Replacer {
    fn new(capacity: usize) -> Self where Self: Sized;

    fn record_access(&mut self, frame_id: FrameId) -> Result<()>;

    fn evict(&mut self) -> Option<FrameId>;

    fn set_evictable(&mut self, frame_id: FrameId, set_evictable: bool) -> Result<()>;

    fn remove(&mut self, frame_id: FrameId);

    fn size(&self) -> usize;
}
