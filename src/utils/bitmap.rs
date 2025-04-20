#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DynamicBitmap {
    map: Vec<u8>,
}

impl DynamicBitmap {
    pub fn new() -> Self {
        Self { map: Vec::new() }
    }

    pub fn set(&mut self, index: usize, value: bool) {
        let byte_idx = index >> 3; // idx / 8
        if byte_idx >= self.map.len() {
            self.map.extend(vec![0; byte_idx - self.map.len() + 1])
        }
        let offset = index & 0b111; // idx % 8
        let mut byte = self.map[byte_idx];

        let curval = (byte >> (7 - offset)) & 1;
        let mask = if value { 1 ^ curval } else { curval };
        byte ^= mask << (7 - offset); // Bit flipping
        self.map[byte_idx] = byte;
    }

    pub fn get(&self, index: usize) -> Option<bool> {
        if index >= self.map.len() << 8 {
            return None;
        }
        let byte_idx = index >> 3; // idx / 8
        let offset = index & 0b111; // idx % 8
        let byte = self.map[byte_idx];
        Some((byte >> (7 - offset)) & 1 == 1)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.map.clone()
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            map: bytes.to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::bitmap::DynamicBitmap;

    #[test]
    fn dynamic_bitmap() {
        let mut bitmap = DynamicBitmap::new();
        assert_eq!(bitmap.get(0), None);

        bitmap.set(3, true);
        assert_eq!(bitmap.map.len(), 1);

        bitmap.set(10, true);
        assert_eq!(bitmap.map.len(), 2);

        assert_eq!(bitmap.get(0), Some(false));
        assert_eq!(bitmap.get(3), Some(true));
        assert_eq!(bitmap.get(10), Some(true));

        let new_bitmap = DynamicBitmap::from_bytes(&bitmap.to_bytes());
        assert_eq!(new_bitmap, bitmap);
    }
}