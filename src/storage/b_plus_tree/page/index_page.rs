use crate::storage::b_plus_tree::buffer_pool_manager::PageId;
use crate::storage::b_plus_tree::buffer_pool_manager::INVALID_PAGE_ID;
use crate::storage::b_plus_tree::comparator::{default_comparator, KeyComparator};
use crate::storage::b_plus_tree::page::table_page::RecordId;
use std::cmp::Ordering;

pub const BPLUS_INTERNAL_PAGE_MAX_SIZE: usize = 10;
pub const BPLUS_LEAF_PAGE_MAX_SIZE: usize = 10;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BPlusTreePage {
    Internal(BPlusTreeInternalPage),
    Leaf(BPlusTreeLeafPage),
}

impl BPlusTreePage {
    pub fn is_full(&self) -> bool {
        match self {
            Self::Internal(page) => page.is_full(),
            Self::Leaf(page) => page.is_full(),
        }
    }
    pub fn is_underflow(&self, is_root: bool) -> bool {
        if is_root {
            return false;
        }
        match self {
            Self::Internal(page) => page.header.current_size < page.min_size(),
            Self::Leaf(page) => page.header.current_size < page.min_size(),
        }
    }
    pub fn insert_internalkv(&mut self, internalkv: InternalKV) {
        match self {
            Self::Internal(page) => page.insert(internalkv.0, internalkv.1),
            Self::Leaf(_) => panic!("Leaf page cannot insert InternalKV"),
        }
    }
    pub fn can_borrow(&self) -> bool {
        match self {
            Self::Internal(page) => page.header.current_size > page.min_size(),
            Self::Leaf(page) => page.header.current_size > page.min_size(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BPlusTreePageType {
    LeafPage,
    InternalPage,
}

pub type InternalKV = (Vec<u8>, PageId);
pub type LeafKV = (Vec<u8>, RecordId);

/**
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 *
 * Header format (size in byte, 12 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 */
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BPlusTreeInternalPage {
    pub header: BPlusTreeInternalPageHeader,
    // 第一个key为空，n个key对应n+1个value
    pub array: Vec<InternalKV>,
    pub comparator: KeyComparator,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BPlusTreeInternalPageHeader {
    pub page_type: BPlusTreePageType,
    pub current_size: u32,
    // max kv size can be stored
    pub max_size: u32,
}

impl BPlusTreeInternalPage {
    pub fn new(max_size: u32) -> Self {
        Self {
            header: BPlusTreeInternalPageHeader {
                page_type: BPlusTreePageType::InternalPage,
                current_size: 0,
                max_size,
            },
            array: Vec::with_capacity(max_size as usize),
            comparator: default_comparator,
        }
    }
    pub fn min_size(&self) -> u32 {
        self.header.max_size / 2
    }
    pub fn key_at(&self, index: usize) -> &Vec<u8> {
        &self.array[index].0
    }
    pub fn value_at(&self, index: usize) -> PageId {
        self.array[index].1
    }
    pub fn values(&self) -> Vec<PageId> {
        self.array.iter().map(|kv| kv.1).collect()
    }

    pub fn sibling_page_ids(&self, page_id: PageId) -> (Option<PageId>, Option<PageId>) {
        let index = self.array.iter().position(|x| x.1 == page_id);
        if let Some(index) = index {
            return (
                if index == 0 {
                    None
                } else {
                    Some(self.array[index - 1].1)
                },
                if index == self.header.current_size as usize - 1 {
                    None
                } else {
                    Some(self.array[index + 1].1)
                },
            );
        }
        (None, None)
    }

    // 使用自定义比较器比较键
    pub fn compare_keys(&self, a: &[u8], b: &[u8]) -> Ordering {
        (self.comparator)(a, b)
    }

    // Insert key-value pair using binary search to find insertion position
    pub fn insert(&mut self, key: Vec<u8>, page_id: PageId) {
        // Skip first empty key if exists
        let start_idx = if self.array.is_empty() { 0 } else { 1 };
        let mut pos = start_idx;
        
        // Binary search to find insertion position
        let mut left = start_idx;
        let mut right = self.array.len();
        
        while left < right {
            let mid = (left + right) / 2;
            match self.compare_keys(&key, &self.array[mid].0) {
                Ordering::Less => right = mid,
                Ordering::Greater => left = mid + 1,
                Ordering::Equal => {
                    pos = mid;
                    break;
                }
            }
            pos = left;
        }

        // Insert at found position
        self.array.insert(pos, (key, page_id));
        self.header.current_size += 1;
    }
    pub fn batch_insert(&mut self, kvs: Vec<InternalKV>) {
        let kvs_len = kvs.len();
        self.array.extend(kvs);
        self.header.current_size += kvs_len as u32;
        let cmp = self.comparator;
        self.array.sort_by(|a, b| cmp(a.0.as_slice(), b.0.as_slice()));
    }

    pub fn delete(&mut self, key: &Vec<u8>) {
        if self.header.current_size == 0 {
            return;
        }
        // 第一个key为空，所以从1开始
        let mut start: i32 = 1;
        let mut end: i32 = self.header.current_size as i32 - 1;
        while start < end {
            let mid = (start + end) / 2;
            let compare_res = self.compare_keys(key.as_slice(), self.array[mid as usize].0.as_slice());
            if compare_res == Ordering::Equal {
                self.array.remove(mid as usize);
                self.header.current_size -= 1;
                // 删除后，如果只剩下一个空key，那么删除
                if self.header.current_size == 1 {
                    self.array.remove(0);
                    self.header.current_size -= 1;
                }
                return;
            } else if compare_res == Ordering::Less {
                end = mid - 1;
            } else {
                start = mid + 1;
            }
        }
        if self.compare_keys(key.as_slice(), self.array[start as usize].0.as_slice()) == Ordering::Equal {
            self.array.remove(start as usize);
            self.header.current_size -= 1;
            // 删除后，如果只剩下一个空key，那么删除
            if self.header.current_size == 1 {
                self.array.remove(0);
                self.header.current_size -= 1;
            }
        }
    }

    pub fn delete_page_id(&mut self, page_id: PageId) {
        for i in 0..self.header.current_size {
            if self.array[i as usize].1 == page_id {
                self.array.remove(i as usize);
                self.header.current_size -= 1;
                return;
            }
        }
    }

    pub fn is_full(&self) -> bool {
        self.header.current_size > self.header.max_size
    }

    pub fn split_off(&mut self, at: usize) -> Vec<InternalKV> {
        let new_array = self.array.split_off(at);
        self.header.current_size -= new_array.len() as u32;
        new_array
    }

    pub fn reverse_split_off(&mut self, at: usize) -> Vec<InternalKV> {
        let mut new_array = Vec::new();
        for _ in 0..=at {
            new_array.push(self.array.remove(0));
        }
        self.header.current_size -= new_array.len() as u32;
        new_array
    }

    pub fn replace_key(&mut self, old_key: &Vec<u8>, new_key: &Vec<u8>) {
        let key_index = self.key_index(old_key.clone());
        if let Some(index) = key_index {
            self.array[index].0 = new_key.clone();
        }
    }

    pub fn key_index(&self, key: Vec<u8>) -> Option<usize> {
        if self.header.current_size == 0 {
            return None;
        }
        // 第一个key为空，所以从1开始
        let mut start: i32 = 1;
        let mut end: i32 = self.header.current_size as i32 - 1;
        while start < end {
            let mid = (start + end) / 2;
            let compare_res = self.compare_keys(key.as_slice(), self.array[mid as usize].0.as_slice());
            if compare_res == Ordering::Equal {
                return Some(mid as usize);
            } else if compare_res == Ordering::Less {
                end = mid - 1;
            } else {
                start = mid + 1;
            }
        }
        if self.compare_keys(key.as_slice(), self.array[start as usize].0.as_slice()) == Ordering::Equal {
            return Some(start as usize);
        }
        None
    }

    // 查找key对应的page_id
    pub fn look_up(&self, key: &[u8]) -> PageId {
        // 第一个key为空，所以从1开始
        let mut start = 1;
        if self.header.current_size == 0 {
            println!("look_up empty page");
        }
        let mut end = self.header.current_size - 1;
        while start < end {
            let mid = (start + end) / 2;
            let compare_res = self.compare_keys(key, self.array[mid as usize].0.as_slice());
            if compare_res == Ordering::Equal {
                return self.array[mid as usize].1;
            } else if compare_res == Ordering::Less {
                end = mid - 1;
            } else {
                start = mid + 1;
            }
        }
        let compare_res = self.compare_keys(key, self.array[start as usize].0.as_slice());
        if compare_res == Ordering::Less {
            self.array[start as usize - 1].1
        } else {
            self.array[start as usize].1
        }
    }
}

/**
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) | NextPageId (4)
 *  ---------------------------------------------------------------------
 */
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BPlusTreeLeafPage {
    pub header: BPlusTreeLeafPageHeader,
    pub array: Vec<LeafKV>,
    pub comparator: KeyComparator,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BPlusTreeLeafPageHeader {
    pub page_type: BPlusTreePageType,
    pub current_size: u32,
    // max kv size can be stored
    pub max_size: u32,
    pub next_page_id: PageId,
}

impl BPlusTreeLeafPage {
    pub fn new(max_size: u32) -> Self {
        Self {
            header: BPlusTreeLeafPageHeader {
                page_type: BPlusTreePageType::LeafPage,
                current_size: 0,
                max_size,
                next_page_id: INVALID_PAGE_ID,
            },
            array: Vec::with_capacity(max_size as usize),
            comparator: default_comparator,
        }
    }

    pub fn empty() -> Self {
        Self {
            header: BPlusTreeLeafPageHeader {
                page_type: BPlusTreePageType::LeafPage,
                current_size: 0,
                max_size: 0,
                next_page_id: INVALID_PAGE_ID,
            },
            array: Vec::new(),
            comparator: default_comparator,
        }
    }

    pub fn min_size(&self) -> u32 {
        self.header.max_size / 2
    }

    pub fn key_at(&self, index: usize) -> &Vec<u8> {
        &self.array[index].0
    }

    pub fn kv_at(&self, index: usize) -> &LeafKV {
        &self.array[index]
    }

    pub fn is_full(&self) -> bool {
        self.header.current_size > self.header.max_size
    }

    // 使用自定义比较器比较键
    pub fn compare_keys(&self, a: &[u8], b: &[u8]) -> Ordering {
        (self.comparator)(a, b)
    }

    // Insert key-value pair using binary search to find insertion position
    pub fn insert(&mut self, key: &[u8], rid: RecordId) {
        let mut pos = 0;
        let mut left = 0;
        let mut right = self.array.len();

        // Binary search to find insertion position
        while left < right {
            let mid = (left + right) / 2;
            match self.compare_keys(key, &self.array[mid].0) {
                Ordering::Less => right = mid,
                Ordering::Greater => left = mid + 1,
                Ordering::Equal => {
                    pos = mid;
                    break;
                }
            }
            pos = left;
        }

        // Insert at found position
        self.array.insert(pos, (key.to_vec(), rid));
        self.header.current_size += 1;
    }

    pub fn batch_insert(&mut self, kvs: Vec<LeafKV>) {
        let kvs_len = kvs.len();
        self.array.extend(kvs);
        self.header.current_size += kvs_len as u32;
        let cmp = self.comparator;
        self.array.sort_by(|a, b| cmp(a.0.as_slice(), b.0.as_slice()));
    }

    pub fn split_off(&mut self, at: usize) -> Vec<LeafKV> {
        let new_array = self.array.split_off(at);
        self.header.current_size -= new_array.len() as u32;
        new_array
    }

    pub fn reverse_split_off(&mut self, at: usize) -> Vec<LeafKV> {
        let mut new_array = Vec::new();
        for _ in 0..=at {
            new_array.push(self.array.remove(0));
        }
        self.header.current_size -= new_array.len() as u32;
        new_array
    }

    pub fn delete(&mut self, key: &[u8]) {
        let key_index = self.key_index(key.to_vec());
        if let Some(index) = key_index {
            self.array.remove(index);
            self.header.current_size -= 1;
        }
    }

    // 查找key对应的rid
    pub fn look_up(&self, key: &[u8]) -> Option<RecordId> {
        let key_index = self.key_index(key.to_vec());
        key_index.map(|index| self.array[index].1)
    }

    fn key_index(&self, key: Vec<u8>) -> Option<usize> {
        if self.header.current_size == 0 {
            return None;
        }
        let mut start: i32 = 0;
        let mut end: i32 = self.header.current_size as i32 - 1;
        while start < end {
            let mid = (start + end) / 2;
            let compare_res = self.compare_keys(key.as_slice(), self.array[mid as usize].0.as_slice());
            if compare_res == Ordering::Equal {
                return Some(mid as usize);
            } else if compare_res == Ordering::Less {
                end = mid - 1;
            } else {
                start = mid + 1;
            }
        }
        if self.compare_keys(key.as_slice(), self.array[start as usize].0.as_slice()) == Ordering::Equal {
            return Some(start as usize);
        }
        None
    }

    pub fn next_closest(&self, key: &Vec<u8>, included: bool) -> Option<usize> {
        for (idx, (k, _)) in self.array.iter().enumerate() {
            let cmp = self.compare_keys(key.as_slice(), k.as_slice());
            if cmp == Ordering::Equal && included {
                return Some(idx);
            }
            if cmp == Ordering::Less {
                return Some(idx);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::b_plus_tree::page::index_page::{BPlusTreeInternalPage, BPlusTreeLeafPage};
    use crate::storage::b_plus_tree::page::table_page::RecordId;

    // 辅助函数将整数转换为字节数组
    fn int_to_bytes(i: i32) -> Vec<u8> {
        i.to_be_bytes().to_vec()
    }

    // 创建空字节数组
    fn empty_bytes() -> Vec<u8> {
        Vec::new()
    }

    #[test]
    pub fn test_internal_page_insert() {
        let mut internal_page = BPlusTreeInternalPage::new(3);

        // 插入空键
        internal_page.insert(empty_bytes(), 0);

        // 插入两个键值对
        internal_page.insert(int_to_bytes(2), 2);
        internal_page.insert(int_to_bytes(1), 1);

        assert_eq!(internal_page.header.current_size, 3);
        assert_eq!(internal_page.array[0].0, empty_bytes());
        assert_eq!(internal_page.array[0].1, 0);
        assert_eq!(internal_page.array[1].0, int_to_bytes(1));
        assert_eq!(internal_page.array[1].1, 1);
        assert_eq!(internal_page.array[2].0, int_to_bytes(2));
        assert_eq!(internal_page.array[2].1, 2);
    }

    #[test]
    pub fn test_leaf_page_insert() {
        
        let mut leaf_page = BPlusTreeLeafPage::new(3);

        leaf_page.insert(&int_to_bytes(2), RecordId::new(2, 2));
        leaf_page.insert(&int_to_bytes(1), RecordId::new(1, 1));
        leaf_page.insert(&int_to_bytes(3), RecordId::new(3, 3));

        assert_eq!(leaf_page.header.current_size, 3);
        assert_eq!(leaf_page.array[0].0, int_to_bytes(1));
        assert_eq!(leaf_page.array[0].1, RecordId::new(1, 1));
        assert_eq!(leaf_page.array[1].0, int_to_bytes(2));
        assert_eq!(leaf_page.array[1].1, RecordId::new(2, 2));
        assert_eq!(leaf_page.array[2].0, int_to_bytes(3));
        assert_eq!(leaf_page.array[2].1, RecordId::new(3, 3));
    }

    #[test]
    pub fn test_internal_page_look_up() {

        let mut internal_page = BPlusTreeInternalPage::new(5);

        internal_page.insert(empty_bytes(), 0);
        internal_page.insert(int_to_bytes(2), 2);
        internal_page.insert(int_to_bytes(1), 1);
        internal_page.insert(int_to_bytes(3), 3);
        internal_page.insert(int_to_bytes(4), 4);

        assert_eq!(internal_page.look_up(&int_to_bytes(0)), 0);
        assert_eq!(internal_page.look_up(&int_to_bytes(3)), 3);
        assert_eq!(internal_page.look_up(&int_to_bytes(5)), 4);

        let mut internal_page = BPlusTreeInternalPage::new(2);
        internal_page.insert(empty_bytes(), 0);
        internal_page.insert(int_to_bytes(1), 1);

        assert_eq!(internal_page.look_up(&int_to_bytes(0)), 0);
        assert_eq!(internal_page.look_up(&int_to_bytes(1)), 1);
        assert_eq!(internal_page.look_up(&int_to_bytes(2)), 1);
    }

    #[test]
    pub fn test_leaf_page_look_up() {

        let mut leaf_page = BPlusTreeLeafPage::new(5);

        leaf_page.insert(&int_to_bytes(2), RecordId::new(2, 2));
        leaf_page.insert(&int_to_bytes(1), RecordId::new(1, 1));
        leaf_page.insert(&int_to_bytes(3), RecordId::new(3, 3));
        leaf_page.insert(&int_to_bytes(5), RecordId::new(5, 5));
        leaf_page.insert(&int_to_bytes(4), RecordId::new(4, 4));

        assert_eq!(leaf_page.look_up(&int_to_bytes(0)), None);
        assert_eq!(
            leaf_page.look_up(&int_to_bytes(2)),
            Some(RecordId::new(2, 2))
        );
        assert_eq!(
            leaf_page.look_up(&int_to_bytes(3)),
            Some(RecordId::new(3, 3))
        );
        assert_eq!(leaf_page.look_up(&int_to_bytes(6)), None);

        let mut leaf_page = BPlusTreeLeafPage::new(2);
        leaf_page.insert(&int_to_bytes(2), RecordId::new(2, 2));
        leaf_page.insert(&int_to_bytes(1), RecordId::new(1, 1));

        assert_eq!(leaf_page.look_up(&int_to_bytes(0)), None);
        assert_eq!(
            leaf_page.look_up(&int_to_bytes(1)),
            Some(RecordId::new(1, 1))
        );
        assert_eq!(
            leaf_page.look_up(&int_to_bytes(2)),
            Some(RecordId::new(2, 2))
        );
        assert_eq!(leaf_page.look_up(&int_to_bytes(3)), None);
    }

    #[test]
    pub fn test_internal_page_delete() {

        let mut internal_page = BPlusTreeInternalPage::new(5);

        internal_page.insert(empty_bytes(), 0);
        internal_page.insert(int_to_bytes(2), 2);
        internal_page.insert(int_to_bytes(1), 1);
        internal_page.insert(int_to_bytes(3), 3);
        internal_page.insert(int_to_bytes(4), 4);

        internal_page.delete(&int_to_bytes(2));
        assert_eq!(internal_page.header.current_size, 4);
        assert_eq!(internal_page.array[0].0, empty_bytes());
        assert_eq!(internal_page.array[0].1, 0);
        assert_eq!(internal_page.array[1].0, int_to_bytes(1));
        assert_eq!(internal_page.array[1].1, 1);
        assert_eq!(internal_page.array[2].0, int_to_bytes(3));
        assert_eq!(internal_page.array[2].1, 3);
        assert_eq!(internal_page.array[3].0, int_to_bytes(4));
        assert_eq!(internal_page.array[3].1, 4);

        internal_page.delete(&int_to_bytes(4));
        assert_eq!(internal_page.header.current_size, 3);

        internal_page.delete(&int_to_bytes(3));
        assert_eq!(internal_page.header.current_size, 2);

        internal_page.delete(&int_to_bytes(1));
        assert_eq!(internal_page.header.current_size, 0);

        internal_page.delete(&int_to_bytes(1));
        assert_eq!(internal_page.header.current_size, 0);
    }

    #[test]
    pub fn test_leaf_page_delete() {

        let mut leaf_page = BPlusTreeLeafPage::new(5);

        leaf_page.insert(&int_to_bytes(2), RecordId::new(2, 2));
        leaf_page.insert(&int_to_bytes(1), RecordId::new(1, 1));
        leaf_page.insert(&int_to_bytes(3), RecordId::new(3, 3));
        leaf_page.insert(&int_to_bytes(5), RecordId::new(5, 5));
        leaf_page.insert(&int_to_bytes(4), RecordId::new(4, 4));

        leaf_page.delete(&int_to_bytes(2));
        assert_eq!(leaf_page.header.current_size, 4);
        assert_eq!(leaf_page.array[0].0, int_to_bytes(1));
        assert_eq!(leaf_page.array[0].1, RecordId::new(1, 1));
        assert_eq!(leaf_page.array[1].0, int_to_bytes(3));
        assert_eq!(leaf_page.array[1].1, RecordId::new(3, 3));
        assert_eq!(leaf_page.array[2].0, int_to_bytes(4));
        assert_eq!(leaf_page.array[2].1, RecordId::new(4, 4));
        assert_eq!(leaf_page.array[3].0, int_to_bytes(5));
        assert_eq!(leaf_page.array[3].1, RecordId::new(5, 5));

        leaf_page.delete(&int_to_bytes(3));
        assert_eq!(leaf_page.header.current_size, 3);

        leaf_page.delete(&int_to_bytes(5));
        assert_eq!(leaf_page.header.current_size, 2);

        leaf_page.delete(&int_to_bytes(1));
        assert_eq!(leaf_page.header.current_size, 1);
        assert_eq!(leaf_page.array[0].0, int_to_bytes(4));
        assert_eq!(leaf_page.array[0].1, RecordId::new(4, 4));

        leaf_page.delete(&int_to_bytes(4));
        assert_eq!(leaf_page.header.current_size, 0);

        leaf_page.delete(&int_to_bytes(4));
        assert_eq!(leaf_page.header.current_size, 0);
    }
}
