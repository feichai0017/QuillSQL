use crate::buffer::{PageId, INVALID_PAGE_ID};
use crate::catalog::{Schema, SchemaRef};
use crate::storage::page::RecordId;
use crate::storage::tuple::Tuple;
use std::sync::Arc;

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
    pub fn min_size(&self) -> u32 {
        match self {
            Self::Internal(page) => page.min_size(),
            Self::Leaf(page) => page.min_size(),
        }
    }
    pub fn current_size(&self) -> u32 {
        match self {
            Self::Internal(page) => page.header.current_size,
            Self::Leaf(page) => page.header.current_size,
        }
    }
    pub fn max_size(&self) -> u32 {
        match self {
            Self::Internal(page) => page.header.max_size,
            Self::Leaf(page) => page.header.max_size,
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

pub type InternalKV = (Tuple, PageId);
pub type LeafKV = (Tuple, RecordId);

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
    pub schema: SchemaRef,
    pub header: BPlusTreeInternalPageHeader,
    // 第一个key为空，n个key对应n+1个value
    pub array: Vec<InternalKV>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BPlusTreeInternalPageHeader {
    pub page_type: BPlusTreePageType,
    pub current_size: u32,
    // max kv size can be stored
    pub max_size: u32,
}

impl BPlusTreeInternalPage {
    pub fn new(schema: SchemaRef, max_size: u32) -> Self {
        Self {
            schema,
            header: BPlusTreeInternalPageHeader {
                page_type: BPlusTreePageType::InternalPage,
                current_size: 0,
                max_size,
            },
            array: Vec::with_capacity(max_size as usize),
        }
    }
    pub fn min_size(&self) -> u32 {
        (self.header.max_size + 1) / 2
    }
    pub fn key_at(&self, index: usize) -> &Tuple {
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

    /// Insert a key-value pair into the internal page using binary search
    /// Works whether the page has a leading NULL sentinel or not.
    pub fn insert(&mut self, key: Tuple, page_id: PageId) {
        let size = self.header.current_size as usize;
        let pos = self.array[..size]
            .binary_search_by(|(k, _)| k.cmp(&key))
            .unwrap_or_else(|pos| pos);
        self.array.insert(pos, (key, page_id));
        self.header.current_size += 1;
    }
    pub fn batch_insert(&mut self, kvs: Vec<InternalKV>) {
        let kvs_len = kvs.len();
        self.array.extend(kvs);
        self.header.current_size += kvs_len as u32;
        self.array.sort_by(|a, b| a.0.cmp(&b.0));
    }

    pub fn remove(&mut self, page_id: PageId) -> Option<(Tuple, PageId)> {
        if let Some(pos) = self.array.iter().position(|&(_, pid)| pid == page_id) {
            let removed = self.array.remove(pos);
            self.header.current_size -= 1;
            Some(removed)
        } else {
            None
        }
    }

    pub fn delete(&mut self, key: &Tuple) {
        if self.header.current_size == 0 {
            return;
        }
        // 第一个key为空，所以从1开始
        let mut start: i32 = 1;
        let mut end: i32 = self.header.current_size as i32 - 1;
        while start < end {
            let mid = (start + end) / 2;
            let compare_res = key.partial_cmp(&self.array[mid as usize].0).unwrap();
            if compare_res == std::cmp::Ordering::Equal {
                self.array.remove(mid as usize);
                self.header.current_size -= 1;
                // 删除后，如果只剩下一个空key，那么删除
                if self.header.current_size == 1 {
                    self.array.remove(0);
                    self.header.current_size -= 1;
                }
                return;
            } else if compare_res == std::cmp::Ordering::Less {
                end = mid - 1;
            } else {
                start = mid + 1;
            }
        }
        if key.partial_cmp(&self.array[start as usize].0).unwrap() == std::cmp::Ordering::Equal {
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
        // Split happens only after overflow so full means strictly greater
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

    pub fn replace_key(&mut self, old_key: &Tuple, new_key: Tuple) {
        let key_index = self.key_index(old_key);
        if let Some(index) = key_index {
            self.array[index].0 = new_key;
        }
    }

    pub fn key_index(&self, key: &Tuple) -> Option<usize> {
        if self.header.current_size == 0 {
            return None;
        }
        let size = self.header.current_size as usize;
        self.array[..size]
            .binary_search_by(|(k, _)| k.cmp(key))
            .ok()
    }

    // 查找page_id对应的索引位置
    pub fn value_index(&self, page_id: PageId) -> Option<usize> {
        if self.header.current_size == 0 {
            return None;
        }
        let size = self.header.current_size as usize;
        self.array[..size]
            .iter()
            .position(|(_, pid)| *pid == page_id)
    }

    // 查找key对应的page_id（返回最后一个 <= key 的指针）。
    pub fn look_up(&self, key: &Tuple) -> PageId {
        let size = self.header.current_size as usize;

        // Manual binary search that skips the sentinel at index 0
        let mut low = 1;
        // high can be size - 1, which is fine since key_at will be checked against size
        let mut high = size;
        let mut result_idx = 0; // Will point to P0 if key is smaller than all keys

        while low < high {
            let mid = low + (high - low) / 2;
            if key >= self.key_at(mid) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        // low is the insertion point. We need the element to the left of it.
        result_idx = low - 1;

        self.value_at(result_idx)
    }

    // 查找key对应的page_id的可变引用（返回最后一个 <= key 的指针）。
    pub fn look_up_mut(&mut self, key: &Tuple) -> Option<&mut PageId> {
        let size = self.header.current_size as usize;

        // Manual binary search that skips the sentinel at index 0
        let mut low = 1;
        let mut high = size;
        let mut result_idx = 0;

        while low < high {
            let mid = low + (high - low) / 2;
            if key >= self.key_at(mid) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        result_idx = low - 1;

        Some(&mut self.array[result_idx].1)
    }

    /// Removes the first key-value pair (not the sentinel) and returns it.
    /// Also returns the key that will be promoted up to the parent.
    pub fn remove_first_kv(&mut self) -> (Tuple, PageId) {
        // 检查页面状态是否一致
        if self.array.len() != self.header.current_size as usize {
            panic!(
                "Page state inconsistent: array len {} != current_size {}",
                self.array.len(),
                self.header.current_size
            );
        }

        // The first "real" key is at index 1, after the sentinel.
        if self.array.len() <= 1 {
            panic!(
                "remove_first_kv called on internal page with array len {} and current_size {}",
                self.array.len(),
                self.header.current_size
            );
        }
        let result = self.array.remove(1);
        self.header.current_size -= 1;
        result
    }
    pub fn remove_last_kv(&mut self) -> (Tuple, PageId) {
        // 检查页面状态是否一致
        if self.array.len() != self.header.current_size as usize {
            panic!(
                "Page state inconsistent: array len {} != current_size {}",
                self.array.len(),
                self.header.current_size
            );
        }

        if self.array.is_empty() {
            panic!(
                "remove_last_kv called on empty internal page with current_size {}",
                self.header.current_size
            );
        }

        let result = self.array.pop().unwrap();
        self.header.current_size -= 1;
        assert_eq!(self.header.current_size as usize, self.array.len());
        result
    }
    pub fn merge(&mut self, middle_key: Tuple, other: &mut BPlusTreeInternalPage) {
        // The middle key from the parent is pushed as a new separator,
        // pointing to the first child of the 'other' (right) page.
        self.array
            .push((middle_key, other.array.first().unwrap().1));

        // The remaining key-pointer pairs from the 'other' page are appended.
        // We drain from index 1 to skip 'other' page's sentinel entry.
        self.array.extend(other.array.drain(1..));

        self.header.current_size = self.array.len() as u32;
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
    pub schema: SchemaRef,
    pub header: BPlusTreeLeafPageHeader,
    pub array: Vec<LeafKV>,
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
    pub fn new(schema: SchemaRef, max_size: u32) -> Self {
        Self {
            schema,
            header: BPlusTreeLeafPageHeader {
                page_type: BPlusTreePageType::LeafPage,
                current_size: 0,
                max_size,
                next_page_id: INVALID_PAGE_ID,
            },
            array: Vec::with_capacity(max_size as usize),
        }
    }

    pub fn empty() -> Self {
        Self {
            schema: Arc::new(Schema::empty()),
            header: BPlusTreeLeafPageHeader {
                page_type: BPlusTreePageType::LeafPage,
                current_size: 0,
                max_size: 0,
                next_page_id: INVALID_PAGE_ID,
            },
            array: Vec::new(),
        }
    }

    pub fn min_size(&self) -> u32 {
        (self.header.max_size + 1) / 2
    }

    pub fn key_at(&self, index: usize) -> &Tuple {
        &self.array[index].0
    }

    pub fn kv_at(&self, index: usize) -> &LeafKV {
        &self.array[index]
    }

    pub fn is_full(&self) -> bool {
        // Split happens only after overflow so full means strictly greater
        self.header.current_size > self.header.max_size
    }

    pub fn insert(&mut self, key: Tuple, rid: RecordId) {
        let size = self.header.current_size as usize;
        let pos = self.array[..size]
            .binary_search_by(|(k, _)| k.cmp(&key))
            .unwrap_or_else(|pos| pos);
        self.array.insert(pos, (key, rid));
        self.header.current_size += 1;
    }

    pub fn batch_insert(&mut self, kvs: Vec<LeafKV>) {
        let kvs_len = kvs.len();
        self.array.extend(kvs);
        self.header.current_size += kvs_len as u32;
        self.array.sort_by(|a, b| a.0.cmp(&b.0));
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

    pub fn delete(&mut self, key: &Tuple) {
        let key_index = self.key_index(key);
        if let Some(index) = key_index {
            self.array.remove(index);
            self.header.current_size -= 1;
        }
    }

    // 查找key对应的rid
    pub fn look_up(&self, key: &Tuple) -> Option<RecordId> {
        let key_index = self.key_index(key);
        key_index.map(|index| self.array[index].1)
    }

    // 查找key对应的rid的可变引用
    pub fn look_up_mut(&mut self, key: &Tuple) -> Option<&mut RecordId> {
        let key_index = self.key_index(key);
        key_index.map(|index| &mut self.array[index].1)
    }

    fn key_index(&self, key: &Tuple) -> Option<usize> {
        if self.header.current_size == 0 {
            return None;
        }
        let size = self.header.current_size as usize;
        self.array[..size]
            .binary_search_by(|(k, _)| k.cmp(key))
            .ok()
    }

    /// Removes and returns the first key-value pair.
    pub fn remove_first_kv(&mut self) -> (Tuple, RecordId) {
        // 检查页面状态是否一致
        if self.array.len() != self.header.current_size as usize {
            panic!(
                "Page state inconsistent: array len {} != current_size {}",
                self.array.len(),
                self.header.current_size
            );
        }

        if self.array.is_empty() {
            panic!(
                "remove_first_kv called on empty leaf page with current_size {}",
                self.header.current_size
            );
        }

        let result = self.array.remove(0);
        self.header.current_size -= 1;
        result
    }
    pub fn remove_last_kv(&mut self) -> (Tuple, RecordId) {
        // 检查页面状态是否一致
        if self.array.len() != self.header.current_size as usize {
            panic!(
                "Page state inconsistent: array len {} != current_size {}",
                self.array.len(),
                self.header.current_size
            );
        }

        if self.array.is_empty() {
            panic!(
                "remove_last_kv called on empty leaf page with current_size {}",
                self.header.current_size
            );
        }

        let result = self.array.pop().unwrap();
        self.header.current_size -= 1;
        result
    }
    pub fn merge(&mut self, other: &mut BPlusTreeLeafPage) {
        self.array.extend(other.array.drain(..));
        self.header.current_size = self.array.len() as u32;
        self.header.next_page_id = other.header.next_page_id;
    }

    pub fn next_closest(&self, tuple: &Tuple, included: bool) -> Option<usize> {
        for (idx, (key, _)) in self.array.iter().enumerate() {
            if tuple == key && included {
                return Some(idx);
            }
            if key > tuple {
                return Some(idx);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::page::{BPlusTreeInternalPage, BPlusTreeLeafPage};
    use crate::storage::tuple::Tuple;
    use crate::utils::scalar::ScalarValue;
    use crate::{
        catalog::{Column, DataType, Schema},
        storage::page::RecordId,
    };
    use std::sync::Arc;

    #[test]
    pub fn test_internal_page_insert() {
        let key_schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let mut internal_page = BPlusTreeInternalPage::new(key_schema.clone(), 3);
        internal_page.insert(Tuple::empty(key_schema.clone()), 0);
        internal_page.insert(
            Tuple::new(key_schema.clone(), vec![2i8.into(), 2i16.into()]),
            2,
        );
        internal_page.insert(
            Tuple::new(key_schema.clone(), vec![1i8.into(), 1i16.into()]),
            1,
        );
        assert_eq!(internal_page.header.current_size, 3);
        assert_eq!(
            internal_page.array[0].0.data,
            Tuple::empty(key_schema.clone()).data
        );
        assert_eq!(internal_page.array[0].1, 0);
        assert_eq!(internal_page.array[1].0.data, vec![1i8.into(), 1i16.into()]);
        assert_eq!(internal_page.array[1].1, 1);
        assert_eq!(internal_page.array[2].0.data, vec![2i8.into(), 2i16.into()]);
        assert_eq!(internal_page.array[2].1, 2);
    }

    #[test]
    pub fn test_leaf_page_insert() {
        let key_schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let mut leaf_page = BPlusTreeLeafPage::new(key_schema.clone(), 3);
        leaf_page.insert(
            Tuple::new(key_schema.clone(), vec![2i8.into(), 2i16.into()]),
            RecordId::new(2, 2),
        );
        leaf_page.insert(
            Tuple::new(key_schema.clone(), vec![1i8.into(), 1i16.into()]),
            RecordId::new(1, 1),
        );
        leaf_page.insert(
            Tuple::new(key_schema.clone(), vec![3i8.into(), 3i16.into()]),
            RecordId::new(3, 3),
        );
        assert_eq!(leaf_page.header.current_size, 3);
        assert_eq!(leaf_page.array[0].0.data, vec![1i8.into(), 1i16.into()]);
        assert_eq!(leaf_page.array[0].1, RecordId::new(1, 1));
        assert_eq!(leaf_page.array[1].0.data, vec![2i8.into(), 2i16.into()]);
        assert_eq!(leaf_page.array[1].1, RecordId::new(2, 2));
        assert_eq!(leaf_page.array[2].0.data, vec![3i8.into(), 3i16.into()]);
        assert_eq!(leaf_page.array[2].1, RecordId::new(3, 3));
    }

    #[test]
    pub fn test_internal_page_look_up() {
        let key_schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let mut internal_page = BPlusTreeInternalPage::new(key_schema.clone(), 5);
        internal_page.insert(Tuple::empty(key_schema.clone()), 0);
        internal_page.insert(
            Tuple::new(key_schema.clone(), vec![2i8.into(), 2i16.into()]),
            2,
        );
        internal_page.insert(
            Tuple::new(key_schema.clone(), vec![1i8.into(), 1i16.into()]),
            1,
        );
        internal_page.insert(
            Tuple::new(key_schema.clone(), vec![3i8.into(), 3i16.into()]),
            3,
        );
        internal_page.insert(
            Tuple::new(key_schema.clone(), vec![4i8.into(), 4i16.into()]),
            4,
        );

        assert_eq!(
            internal_page.look_up(&Tuple::new(
                key_schema.clone(),
                vec![0i8.into(), 0i16.into()]
            ),),
            0
        );
        assert_eq!(
            internal_page.look_up(&Tuple::new(
                key_schema.clone(),
                vec![3i8.into(), 3i16.into()]
            ),),
            3
        );
        assert_eq!(
            internal_page.look_up(&Tuple::new(
                key_schema.clone(),
                vec![5i8.into(), 5i16.into()]
            ),),
            4
        );

        let mut internal_page = BPlusTreeInternalPage::new(key_schema.clone(), 2);
        internal_page.insert(Tuple::empty(key_schema.clone()), 0);
        internal_page.insert(
            Tuple::new(key_schema.clone(), vec![1i8.into(), 1i16.into()]),
            1,
        );

        assert_eq!(
            internal_page.look_up(&Tuple::new(
                key_schema.clone(),
                vec![0i8.into(), 0i16.into()]
            ),),
            0
        );
        assert_eq!(
            internal_page.look_up(&Tuple::new(
                key_schema.clone(),
                vec![1i8.into(), 1i16.into()]
            ),),
            1
        );
        assert_eq!(
            internal_page.look_up(&Tuple::new(
                key_schema.clone(),
                vec![2i8.into(), 2i16.into()]
            ),),
            1
        );
    }

    #[test]
    pub fn test_leaf_page_look_up() {
        let key_schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let mut leaf_page = BPlusTreeLeafPage::new(key_schema.clone(), 5);
        leaf_page.insert(
            Tuple::new(key_schema.clone(), vec![2i8.into(), 2i16.into()]),
            RecordId::new(2, 2),
        );
        leaf_page.insert(
            Tuple::new(key_schema.clone(), vec![1i8.into(), 1i16.into()]),
            RecordId::new(1, 1),
        );
        leaf_page.insert(
            Tuple::new(key_schema.clone(), vec![3i8.into(), 3i16.into()]),
            RecordId::new(3, 3),
        );
        leaf_page.insert(
            Tuple::new(key_schema.clone(), vec![5i8.into(), 5i16.into()]),
            RecordId::new(5, 5),
        );
        leaf_page.insert(
            Tuple::new(key_schema.clone(), vec![4i8.into(), 4i16.into()]),
            RecordId::new(4, 4),
        );
        assert_eq!(
            leaf_page.look_up(&Tuple::new(
                key_schema.clone(),
                vec![0i8.into(), 0i16.into()]
            ),),
            None
        );
        assert_eq!(
            leaf_page.look_up(&Tuple::new(
                key_schema.clone(),
                vec![2i8.into(), 2i16.into()]
            ),),
            Some(RecordId::new(2, 2))
        );
        assert_eq!(
            leaf_page.look_up(&Tuple::new(
                key_schema.clone(),
                vec![3i8.into(), 3i16.into()]
            ),),
            Some(RecordId::new(3, 3))
        );
        assert_eq!(
            leaf_page.look_up(&Tuple::new(
                key_schema.clone(),
                vec![6i8.into(), 6i16.into()]
            ),),
            None
        );

        let mut leaf_page = BPlusTreeLeafPage::new(key_schema.clone(), 2);
        leaf_page.insert(
            Tuple::new(key_schema.clone(), vec![2i8.into(), 2i16.into()]),
            RecordId::new(2, 2),
        );
        leaf_page.insert(
            Tuple::new(key_schema.clone(), vec![1i8.into(), 1i16.into()]),
            RecordId::new(1, 1),
        );
        assert_eq!(
            leaf_page.look_up(&Tuple::new(
                key_schema.clone(),
                vec![ScalarValue::Int8(None), ScalarValue::Int16(None)]
            ),),
            None
        );
        assert_eq!(
            leaf_page.look_up(&Tuple::new(
                key_schema.clone(),
                vec![1i8.into(), 1i16.into()]
            ),),
            Some(RecordId::new(1, 1))
        );
        assert_eq!(
            leaf_page.look_up(&Tuple::new(
                key_schema.clone(),
                vec![2i8.into(), 2i16.into()]
            ),),
            Some(RecordId::new(2, 2))
        );
        assert_eq!(
            leaf_page.look_up(&Tuple::new(
                key_schema.clone(),
                vec![3i8.into(), 3i16.into()]
            ),),
            None
        );
    }

    #[test]
    pub fn test_internal_page_delete() {
        let key_schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let mut internal_page = BPlusTreeInternalPage::new(key_schema.clone(), 5);
        internal_page.insert(Tuple::empty(key_schema.clone()), 0);
        internal_page.insert(
            Tuple::new(key_schema.clone(), vec![2i8.into(), 2i16.into()]),
            2,
        );
        internal_page.insert(
            Tuple::new(key_schema.clone(), vec![1i8.into(), 1i16.into()]),
            1,
        );
        internal_page.insert(
            Tuple::new(key_schema.clone(), vec![3i8.into(), 3i16.into()]),
            3,
        );
        internal_page.insert(
            Tuple::new(key_schema.clone(), vec![4i8.into(), 4i16.into()]),
            4,
        );

        internal_page.delete(&Tuple::new(
            key_schema.clone(),
            vec![2i8.into(), 2i16.into()],
        ));
        assert_eq!(internal_page.header.current_size, 4);
        assert_eq!(
            internal_page.array[0].0.data,
            vec![ScalarValue::Int8(None), ScalarValue::Int16(None)]
        );
        assert_eq!(internal_page.array[0].1, 0);
        assert_eq!(internal_page.array[1].0.data, vec![1i8.into(), 1i16.into()]);
        assert_eq!(internal_page.array[1].1, 1);
        assert_eq!(internal_page.array[2].0.data, vec![3i8.into(), 3i16.into()]);
        assert_eq!(internal_page.array[2].1, 3);
        assert_eq!(internal_page.array[3].0.data, vec![4i8.into(), 4i16.into()]);
        assert_eq!(internal_page.array[3].1, 4);
        internal_page.delete(&Tuple::new(
            key_schema.clone(),
            vec![4i8.into(), 4i16.into()],
        ));
        assert_eq!(internal_page.header.current_size, 3);
        internal_page.delete(&Tuple::new(
            key_schema.clone(),
            vec![3i8.into(), 3i16.into()],
        ));
        assert_eq!(internal_page.header.current_size, 2);
        internal_page.delete(&Tuple::new(
            key_schema.clone(),
            vec![1i8.into(), 1i16.into()],
        ));
        assert_eq!(internal_page.header.current_size, 0);
        internal_page.delete(&Tuple::new(
            key_schema.clone(),
            vec![1i8.into(), 1i16.into()],
        ));
        assert_eq!(internal_page.header.current_size, 0);
    }

    #[test]
    pub fn test_leaf_page_delete() {
        let key_schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let mut leaf_page = BPlusTreeLeafPage::new(key_schema.clone(), 5);
        leaf_page.insert(
            Tuple::new(key_schema.clone(), vec![2i8.into(), 2i16.into()]),
            RecordId::new(2, 2),
        );
        leaf_page.insert(
            Tuple::new(key_schema.clone(), vec![1i8.into(), 1i16.into()]),
            RecordId::new(1, 1),
        );
        leaf_page.insert(
            Tuple::new(key_schema.clone(), vec![3i8.into(), 3i16.into()]),
            RecordId::new(3, 3),
        );
        leaf_page.insert(
            Tuple::new(key_schema.clone(), vec![5i8.into(), 5i16.into()]),
            RecordId::new(5, 5),
        );
        leaf_page.insert(
            Tuple::new(key_schema.clone(), vec![4i8.into(), 4i16.into()]),
            RecordId::new(4, 4),
        );

        leaf_page.delete(&Tuple::new(
            key_schema.clone(),
            vec![2i8.into(), 2i16.into()],
        ));
        assert_eq!(leaf_page.header.current_size, 4);
        assert_eq!(leaf_page.array[0].0.data, vec![1i8.into(), 1i16.into()]);
        assert_eq!(leaf_page.array[0].1, RecordId::new(1, 1));
        assert_eq!(leaf_page.array[1].0.data, vec![3i8.into(), 3i16.into()]);
        assert_eq!(leaf_page.array[1].1, RecordId::new(3, 3));
        assert_eq!(leaf_page.array[2].0.data, vec![4i8.into(), 4i16.into()]);
        assert_eq!(leaf_page.array[2].1, RecordId::new(4, 4));
        assert_eq!(leaf_page.array[3].0.data, vec![5i8.into(), 5i16.into()]);
        assert_eq!(leaf_page.array[3].1, RecordId::new(5, 5));
        leaf_page.delete(&Tuple::new(
            key_schema.clone(),
            vec![3i8.into(), 3i16.into()],
        ));
        assert_eq!(leaf_page.header.current_size, 3);
        leaf_page.delete(&Tuple::new(
            key_schema.clone(),
            vec![5i8.into(), 5i16.into()],
        ));
        assert_eq!(leaf_page.header.current_size, 2);
        leaf_page.delete(&Tuple::new(
            key_schema.clone(),
            vec![1i8.into(), 1i16.into()],
        ));
        assert_eq!(leaf_page.header.current_size, 1);
        assert_eq!(leaf_page.array[0].0.data, vec![4i8.into(), 4i16.into()]);
        assert_eq!(leaf_page.array[0].1, RecordId::new(4, 4));
        leaf_page.delete(&Tuple::new(
            key_schema.clone(),
            vec![4i8.into(), 4i16.into()],
        ));
        assert_eq!(leaf_page.header.current_size, 0);
        leaf_page.delete(&Tuple::new(
            key_schema.clone(),
            vec![4i8.into(), 4i16.into()],
        ));
        assert_eq!(leaf_page.header.current_size, 0);
    }
}
