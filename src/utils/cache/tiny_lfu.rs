use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// TinyLFU-style admission filter: approximate frequency via 4-bit counters in a simple
/// Count-Min Sketch. This is a minimal, lockless (external locking) and CPU-cheap version
/// intended to bias admission decisions before the main replacer.
#[derive(Debug)]
pub struct TinyLFU {
    width: usize,
    depth: usize,
    tables: Vec<Vec<u8>>, // 4-bit per counter packed into u8 (2 counters per byte)
}

impl TinyLFU {
    pub fn new(width: usize, depth: usize) -> Self {
        let width = width.next_power_of_two();
        let depth = depth.max(1).min(4);
        let tables = (0..depth).map(|_| vec![0u8; (width + 1) / 2]).collect();
        Self {
            width,
            depth,
            tables,
        }
    }

    #[inline]
    fn hash_i(&self, key: u64, i: usize) -> usize {
        let mut h = DefaultHasher::new();
        (key.wrapping_add((i as u64) << 32)).hash(&mut h);
        (h.finish() as usize) & (self.width - 1)
    }

    #[inline]
    fn load_counter(slot: &mut [u8], idx: usize) -> u8 {
        let byte = &mut slot[idx / 2];
        if idx % 2 == 0 {
            *byte & 0x0F
        } else {
            (*byte >> 4) & 0x0F
        }
    }

    #[inline]
    fn store_counter(slot: &mut [u8], idx: usize, val: u8) {
        let b = &mut slot[idx / 2];
        if idx % 2 == 0 {
            *b = (*b & 0xF0) | (val & 0x0F);
        } else {
            *b = (*b & 0x0F) | ((val & 0x0F) << 4);
        }
    }

    /// Record an access for the 64-bit key.
    pub fn admit_record(&mut self, key: u64) {
        for i in 0..self.depth {
            let idx = self.hash_i(key, i);
            let slot = &mut self.tables[i];
            let mut c = Self::load_counter(slot, idx);
            if c < 15 {
                c += 1;
            }
            Self::store_counter(slot, idx, c);
        }
    }

    /// Estimate frequency for the key (min of counters).
    pub fn estimate(&self, key: u64) -> u8 {
        let mut minv = 15u8;
        for i in 0..self.depth {
            let idx = self.hash_i(key, i);
            let slot = &self.tables[i];
            let c = if idx / 2 < slot.len() {
                if idx % 2 == 0 {
                    slot[idx / 2] & 0x0F
                } else {
                    (slot[idx / 2] >> 4) & 0x0F
                }
            } else {
                0
            };
            if c < minv {
                minv = c;
            }
        }
        minv
    }

    /// Periodic aging to prevent counter saturation. Halves all counters.
    pub fn age(&mut self) {
        for t in self.tables.iter_mut() {
            for b in t.iter_mut() {
                let lo = (*b & 0x0F) >> 1;
                let hi = ((*b >> 4) & 0x0F) >> 1;
                *b = (hi << 4) | lo;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiny_lfu_basic() {
        let mut f = TinyLFU::new(1024, 4);
        let k1 = 123u64;
        let k2 = 456u64;
        for _ in 0..8 {
            f.admit_record(k1);
        }
        for _ in 0..2 {
            f.admit_record(k2);
        }
        assert!(f.estimate(k1) >= f.estimate(k2));
        f.age();
        assert!(f.estimate(k1) >= f.estimate(k2));
    }
}
