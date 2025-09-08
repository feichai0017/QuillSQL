use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::buffer::{PageId, ReadPageGuard, WritePageGuard, INVALID_PAGE_ID};
use crate::catalog::SchemaRef;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::codec::{
    BPlusTreeInternalPageCodec, BPlusTreeLeafPageCodec, BPlusTreePageCodec,
};
use crate::storage::index::Index;
use crate::storage::page::BPlusTreeInternalPage;
use crate::{
    buffer::BufferPoolManager,
    storage::page::{BPlusTreeLeafPage, BPlusTreePage, RecordId},
};

use crate::storage::tuple::Tuple;
use rand::seq::SliceRandom;

#[derive(Debug)]
pub struct Context {
    /// 存储从根节点到当前节点路径上所有被持有的写保护器。
    /// 在向下遍历时，如果遇到“安全”的节点，这个队列会被清空，
    /// 从而释放所有祖先节点的锁。
    pub write_set: VecDeque<WritePageGuard>,

    /// (可选，用于读操作) 存储读保护器。
    pub read_set: VecDeque<ReadPageGuard>,
}

impl Context {
    pub fn new() -> Self {
        Self {
            write_set: VecDeque::new(),
            read_set: VecDeque::new(),
        }
    }

    /// 方便地将一个写保护器添加到路径中。
    pub fn push_write_guard(&mut self, guard: WritePageGuard) {
        self.write_set.push_back(guard);
    }

    pub fn push_read_guard(&mut self, guard: ReadPageGuard) {
        self.read_set.push_back(guard);
    }

    /// 当遇到安全节点时，清空路径并释放所有持有的锁。
    pub fn release_all_write_locks(&mut self) {
        self.write_set.clear();
    }
}
// B+树索引
#[derive(Debug)]
pub struct BPlusTreeIndex {
    pub key_schema: SchemaRef,
    pub buffer_pool: Arc<BufferPoolManager>,
    pub internal_max_size: u32,
    pub leaf_max_size: u32,
    pub root_page_id: AtomicU32,
}

impl Index for BPlusTreeIndex {
    fn key_schema(&self) -> &SchemaRef {
        &self.key_schema
    }
    fn insert(&self, key: &Tuple, value: RecordId) -> QuillSQLResult<()> {
        self.insert(key, value)
    }
    fn get(&self, key: &Tuple) -> QuillSQLResult<Option<RecordId>> {
        self.get(key)
    }
    fn delete(&self, key: &Tuple) -> QuillSQLResult<()> {
        self.delete(key)
    }
}

impl BPlusTreeIndex {
    pub fn new(
        key_schema: SchemaRef,
        buffer_pool: Arc<BufferPoolManager>,
        internal_max_size: u32,
        leaf_max_size: u32,
    ) -> Self {
        Self {
            key_schema,
            buffer_pool,
            internal_max_size,
            leaf_max_size,
            root_page_id: AtomicU32::new(INVALID_PAGE_ID),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.root_page_id.load(Ordering::SeqCst) == INVALID_PAGE_ID
    }

    pub fn get(&self, key: &Tuple) -> QuillSQLResult<Option<RecordId>> {
        if self.is_empty() {
            return Ok(None);
        }

        // 1. 原子地读取根页面ID，获取一个稳定的遍历起点。
        let root_page_id = self.root_page_id.load(Ordering::SeqCst);
        if root_page_id == INVALID_PAGE_ID {
            return Ok(None);
        }

        // 2. 从根节点开始，通过闩锁耦合找到叶子页面。
        let leaf_guard = self.find_leaf_page(key, root_page_id)?;

        // 3. 从叶子页面的 Guard 中解码出页面内容。
        let (leaf_page, _) =
            BPlusTreeLeafPageCodec::decode(&leaf_guard.data, self.key_schema.clone())?;

        // 4. 在解码后的页面中查找 key。
        //    `leaf_guard` 在函数返回时被 drop，自动释放所有资源。
        Ok(leaf_page.look_up(key))
    }

    /// 辅助函数：以写模式查找叶子页面，并沿途执行闩锁耦合。
    fn find_leaf_page_for_write(
        &self,
        key: &Tuple,
        is_insert: bool,
    ) -> QuillSQLResult<(WritePageGuard, Context)> {
        let mut context = Context::new();
        let root_page_id = self.root_page_id.load(Ordering::SeqCst);
        let mut current_guard = self.buffer_pool.fetch_page_write(root_page_id)?;

        loop {
            let (page, _) =
                BPlusTreePageCodec::decode(&current_guard.data, self.key_schema.clone())?;

            match page {
                BPlusTreePage::Internal(internal) => {
                    let child_page_id = internal.look_up(key);
                    context.push_write_guard(current_guard);
                    current_guard = self.buffer_pool.fetch_page_write(child_page_id)?;
                    let (child_page, _) =
                        BPlusTreePageCodec::decode(&current_guard.data, self.key_schema.clone())?;

                    // 安全节点判断
                    let is_safe = if is_insert {
                        child_page.current_size() < child_page.max_size()
                    } else {
                        child_page.current_size() > child_page.min_size()
                    };

                    if is_safe {
                        context.release_all_write_locks();
                    }
                }
                BPlusTreePage::Leaf(_) => {
                    return Ok((current_guard, context));
                }
            }
        }
    }

    /// 公共 API: 插入一个键值对，使用闩锁耦合实现高并发。
    pub fn insert(&self, key: &Tuple, rid: RecordId) -> QuillSQLResult<()> {
        if self.is_empty() {
            // 如果树为空，尝试原子地创建新树。
            // 使用 compare_and_swap 防止多个线程同时创建新树。
            match self.root_page_id.compare_exchange(
                INVALID_PAGE_ID,
                0, // 临时值，马上会被真实 page_id 替换
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // 只有第一个线程会成功
                    return self.start_new_tree(key, rid);
                }
                Err(_) => {
                    // 其他线程等待第一个线程完成
                    // 自旋等待，直到 root_page_id 不再是 INVALID_PAGE_ID
                    while self.is_empty() {
                        std::thread::yield_now();
                    }
                }
            }
        }

        let (mut leaf_guard, mut context) = self.find_leaf_page_for_write(key, true)?;
        let (mut leaf_page, _) =
            BPlusTreeLeafPageCodec::decode(&leaf_guard.data, self.key_schema.clone())?;

        if let Some(existing_rid) = leaf_page.look_up_mut(key) {
            *existing_rid = rid;
            let encoded = BPlusTreeLeafPageCodec::encode(&leaf_page);
            leaf_guard.data.copy_from_slice(&encoded);
            return Ok(());
        }

        leaf_page.insert(key.clone(), rid);
        let encoded = BPlusTreeLeafPageCodec::encode(&leaf_page);
        leaf_guard.data.copy_from_slice(&encoded);

        if leaf_page.is_full() {
            self.split(leaf_guard, &mut context)?;
        }

        Ok(())
    }

    /// 公共 API: 删除一个键，使用闩锁耦合实现并发安全。
    pub fn delete(&self, key: &Tuple) -> QuillSQLResult<()> {
        if self.is_empty() {
            return Ok(());
        }

        let (mut leaf_guard, mut context) = self.find_leaf_page_for_write(key, false)?;
        let (mut leaf_page, _) =
            BPlusTreeLeafPageCodec::decode(&leaf_guard.data, self.key_schema.clone())?;

        if leaf_page.look_up(key).is_none() {
            return Ok(());
        }

        leaf_page.delete(key);
        leaf_guard
            .data
            .copy_from_slice(&BPlusTreeLeafPageCodec::encode(&leaf_page));

        let is_root = leaf_guard.page_id() == self.root_page_id.load(Ordering::SeqCst);
        if !is_root && leaf_page.header.current_size < leaf_page.min_size() {
            self.handle_underflow(leaf_guard, &mut context)?;
        }
        Ok(())
    }

    fn handle_underflow(
        &self,
        node_guard: WritePageGuard,
        context: &mut Context,
    ) -> QuillSQLResult<()> {
        let is_root = context.write_set.is_empty();
        if is_root {
            self.adjust_root(node_guard)?;
            return Ok(());
        }

        let mut parent_guard = context.write_set.pop_back().unwrap();
        let (mut parent_page, _) =
            BPlusTreeInternalPageCodec::decode(&parent_guard.data, self.key_schema.clone())?;
        let node_idx = parent_page.value_index(node_guard.page_id()).unwrap();

        // Attempt to redistribute from left sibling first
        if node_idx > 0 {
            let left_sibling_guard = self
                .buffer_pool
                .fetch_page_write(parent_page.value_at(node_idx - 1))?;
            let (left_sibling_page, _) =
                BPlusTreePageCodec::decode(&left_sibling_guard.data, self.key_schema.clone())?;
            if left_sibling_page.current_size() > left_sibling_page.min_size() {
                self.redistribute(left_sibling_guard, node_guard, parent_guard, node_idx, true)?;
                return Ok(());
            }
        }

        // Attempt to redistribute from right sibling
        if node_idx < parent_page.header.current_size as usize - 1 {
            let right_sibling_guard = self
                .buffer_pool
                .fetch_page_write(parent_page.value_at(node_idx + 1))?;
            let (right_sibling_page, _) =
                BPlusTreePageCodec::decode(&right_sibling_guard.data, self.key_schema.clone())?;
            if right_sibling_page.current_size() > right_sibling_page.min_size() {
                self.redistribute(
                    right_sibling_guard,
                    node_guard,
                    parent_guard,
                    node_idx,
                    false,
                )?;
                return Ok(());
            }
        }

        // Must coalesce
        if node_idx > 0 {
            // Coalesce with left sibling
            let left_sibling_guard = self
                .buffer_pool
                .fetch_page_write(parent_page.value_at(node_idx - 1))?;
            self.coalesce(
                left_sibling_guard,
                node_guard,
                parent_guard,
                node_idx,
                context,
            )?;
        } else {
            // Coalesce with right sibling
            let right_sibling_guard = self
                .buffer_pool
                .fetch_page_write(parent_page.value_at(node_idx + 1))?;
            self.coalesce(
                node_guard,
                right_sibling_guard,
                parent_guard,
                node_idx + 1,
                context,
            )?;
        }

        Ok(())
    }

    fn redistribute(
        &self,
        mut from_guard: WritePageGuard,
        mut to_guard: WritePageGuard,
        mut parent_guard: WritePageGuard,
        parent_idx_of_to_node: usize,
        from_is_left_sibling: bool,
    ) -> QuillSQLResult<()> {
        let (mut from_page, _) =
            BPlusTreePageCodec::decode(&from_guard.data, self.key_schema.clone())?;
        let (mut to_page, _) = BPlusTreePageCodec::decode(&to_guard.data, self.key_schema.clone())?;
        let (mut parent_page, _) =
            BPlusTreeInternalPageCodec::decode(&parent_guard.data, self.key_schema.clone())?;

        if from_is_left_sibling {
            // from=left, to=right. Separator key is at parent_idx_of_to_node.
            let separator_idx = parent_idx_of_to_node;
            match (&mut to_page, &mut from_page) {
                (BPlusTreePage::Leaf(to_leaf), BPlusTreePage::Leaf(from_leaf)) => {
                    let item_to_move = from_leaf.remove_last_kv();
                    // The new separator is the new last key of the from_leaf.
                    parent_page.array[separator_idx].0 = from_leaf
                        .key_at(from_leaf.header.current_size as usize - 1)
                        .clone();
                    to_leaf.array.insert(0, item_to_move);
                    to_leaf.header.current_size += 1;
                }
                (BPlusTreePage::Internal(to_internal), BPlusTreePage::Internal(from_internal)) => {
                    let item_to_move = from_internal.remove_last_kv(); // (K_last, P_last)
                    let separator_key = parent_page.key_at(separator_idx).clone();

                    // The new separator in the parent is K_last
                    parent_page.array[separator_idx].0 = item_to_move.0.clone();

                    // The old separator from the parent moves down to become the first key in the 'to' node.
                    // Its pointer should be the original first pointer of the 'to' node.
                    to_internal
                        .array
                        .insert(1, (separator_key, to_internal.array[0].1));

                    // The sentinel pointer of the 'to' node is updated to P_last.
                    to_internal.array[0].1 = item_to_move.1;

                    to_internal.header.current_size += 1;
                }
                _ => return Err(QuillSQLError::Internal("Mismatched page types".to_string())),
            }
        } else {
            // Borrow from RIGHT
            // from=right, to=left. Separator key is at parent_idx_of_to_node + 1.
            let separator_idx = parent_idx_of_to_node + 1;
            match (&mut to_page, &mut from_page) {
                (BPlusTreePage::Leaf(to_leaf), BPlusTreePage::Leaf(from_leaf)) => {
                    let item_to_move = from_leaf.remove_first_kv();
                    // New separator is the new first key of from_leaf
                    parent_page.array[separator_idx].0 = from_leaf.key_at(0).clone();
                    to_leaf.array.push(item_to_move);
                    to_leaf.header.current_size += 1;
                }
                (BPlusTreePage::Internal(to_internal), BPlusTreePage::Internal(from_internal)) => {
                    let item_to_move = from_internal.remove_first_kv(); // (K_first, P_first)
                    let separator_key = parent_page.key_at(separator_idx).clone();

                    // The new separator in the parent is the NEW first key of the 'from' (right) node.
                    parent_page.array[separator_idx].0 = from_internal.key_at(1).clone();

                    // The old separator from parent is moved down to the 'to' (left) node.
                    // It is appended along with the pointer from the moved item.
                    to_internal.array.push((separator_key, item_to_move.1));
                    to_internal.header.current_size += 1;
                }
                _ => return Err(QuillSQLError::Internal("Mismatched page types".to_string())),
            }
        }

        from_guard
            .data
            .copy_from_slice(&BPlusTreePageCodec::encode(&from_page));
        to_guard
            .data
            .copy_from_slice(&BPlusTreePageCodec::encode(&to_page));
        parent_guard
            .data
            .copy_from_slice(&BPlusTreeInternalPageCodec::encode(&parent_page));

        Ok(())
    }

    fn coalesce(
        &self,
        mut left_guard: WritePageGuard,
        mut right_guard: WritePageGuard,
        mut parent_guard: WritePageGuard,
        parent_idx_of_right_node: usize,
        context: &mut Context,
    ) -> QuillSQLResult<()> {
        let right_page_id = right_guard.page_id();
        let (mut left_page, _) =
            BPlusTreePageCodec::decode(&left_guard.data, self.key_schema.clone())?;
        let (mut right_page, _) =
            BPlusTreePageCodec::decode(&right_guard.data, self.key_schema.clone())?;
        let (mut parent_page, _) =
            BPlusTreeInternalPageCodec::decode(&parent_guard.data, self.key_schema.clone())?;

        let middle_key = parent_page
            .remove(right_guard.page_id())
            .map(|(key, _)| key)
            .unwrap();

        match (&mut left_page, &mut right_page) {
            (BPlusTreePage::Leaf(left_leaf), BPlusTreePage::Leaf(right_leaf)) => {
                left_leaf.merge(right_leaf);
            }
            (BPlusTreePage::Internal(left_internal), BPlusTreePage::Internal(right_internal)) => {
                left_internal.merge(middle_key, right_internal);
            }
            _ => return Err(QuillSQLError::Internal("Mismatched page types".to_string())),
        }

        left_guard
            .data
            .copy_from_slice(&BPlusTreePageCodec::encode(&left_page));
        parent_guard
            .data
            .copy_from_slice(&BPlusTreeInternalPageCodec::encode(&parent_page));

        drop(left_guard);
        drop(right_guard);
        self.buffer_pool.delete_page(right_page_id)?;

        if parent_page.header.current_size < parent_page.min_size() {
            self.handle_underflow(parent_guard, context)?;
        }
        Ok(())
    }

    fn adjust_root(&self, mut root_guard: WritePageGuard) -> QuillSQLResult<()> {
        let (root_page, _) = BPlusTreePageCodec::decode(&root_guard.data, self.key_schema.clone())?;
        let root_id = root_guard.page_id();

        if let BPlusTreePage::Internal(root_internal) = root_page {
            if root_internal.header.current_size == 1 {
                let new_root_id = root_internal.value_at(0);
                self.root_page_id.store(new_root_id, Ordering::SeqCst);
                drop(root_guard);
                self.buffer_pool.delete_page(root_id)?;
            }
        } else if let BPlusTreePage::Leaf(root_leaf) = root_page {
            if root_leaf.header.current_size == 0 {
                self.root_page_id.store(INVALID_PAGE_ID, Ordering::SeqCst);
                drop(root_guard);
                self.buffer_pool.delete_page(root_id)?;
            }
        }
        Ok(())
    }

    /// 内部方法：当树为空时，创建第一个节点。
    fn start_new_tree(&self, key: &Tuple, rid: RecordId) -> QuillSQLResult<()> {
        let mut root_guard = self.buffer_pool.new_page()?;
        let root_page_id = root_guard.page_id();
        let mut leaf_page = BPlusTreeLeafPage::new(self.key_schema.clone(), self.leaf_max_size);
        leaf_page.insert(key.clone(), rid);
        let encoded_data = BPlusTreeLeafPageCodec::encode(&leaf_page);
        root_guard.data.copy_from_slice(&encoded_data);
        // 更新根节点 ID，替换掉之前的临时值
        self.root_page_id.store(root_page_id, Ordering::SeqCst);
        Ok(())
    }

    /// 内部辅助函数：从根节点开始遍历，找到并返回包含目标 key 的
    /// 叶子节点的只读保护器 (ReadPageGuard)。
    ///
    /// 这个函数通过 ReadPageGuard 的 RAII 特性，在遍历时实现了闩锁耦合。
    /// 内部辅助函数：从一个给定的 page_id 开始遍历，找到并返回包含目标 key 的
    /// 叶子节点的只读保护器 (ReadPageGuard)。
    ///
    /// 这个函数通过 ReadPageGuard 的 RAII 特性，在遍历时实现了闩锁耦合。
    fn find_leaf_page(&self, key: &Tuple, start_page_id: PageId) -> QuillSQLResult<ReadPageGuard> {
        let mut current_page_id = start_page_id;

        loop {
            // a. 为当前页面获取一个 ReadPageGuard。
            //    这会自动 pin 住页面并加上读锁。
            let current_guard = self.buffer_pool.fetch_page_read(current_page_id)?;

            // b. 解码页面内容以判断其类型。
            let (page_content, _) =
                BPlusTreePageCodec::decode(&current_guard.data, self.key_schema.clone())?;

            match page_content {
                // c. 如果是内部节点...
                BPlusTreePage::Internal(internal_page) => {
                    if internal_page.header.current_size > internal_page.min_size() {
                        // context.release_all_write_locks(); // This line is removed
                    }
                    // 找到下一个要遍历的子节点的 page_id。
                    current_page_id = internal_page.look_up(key);
                    // 【闩锁耦合的核心】: `current_guard` 在这里离开作用域，
                    // 它的 Drop 实现会被调用，自动释放当前页面的读锁和 pin。
                    // 然后循环会用 `current_page_id` 去锁住下一层的节点。
                }
                // d. 如果是叶子节点...
                BPlusTreePage::Leaf(_) => {
                    // 我们已经到达了树的最底层。返回这个页面的 Guard，
                    // 它的所有权会被转移给调用者 (get 方法)，从而延长锁的生命周期。
                    return Ok(current_guard);
                }
            }
        }
    }

    /// 内部方法：分裂一个节点，并可能递归地向上传播分裂。
    fn split(&self, mut page_guard: WritePageGuard, context: &mut Context) -> QuillSQLResult<()> {
        loop {
            let page_id = page_guard.page_id();
            let (mut page, _) =
                BPlusTreePageCodec::decode(&page_guard.data, self.key_schema.clone())?;

            let mut new_page_guard = self.buffer_pool.new_page()?;
            let new_page_id = new_page_guard.page_id();

            let middle_key = match &mut page {
                BPlusTreePage::Leaf(leaf_page) => {
                    let mut new_leaf =
                        BPlusTreeLeafPage::new(self.key_schema.clone(), self.leaf_max_size);
                    new_leaf.batch_insert(
                        leaf_page.split_off(leaf_page.header.current_size as usize / 2),
                    );
                    new_leaf.header.next_page_id = leaf_page.header.next_page_id;
                    leaf_page.header.next_page_id = new_page_id;
                    let new_data = BPlusTreeLeafPageCodec::encode(&new_leaf);
                    new_page_guard.data.copy_from_slice(&new_data);
                    new_leaf.key_at(0).clone()
                }
                BPlusTreePage::Internal(internal_page) => {
                    let mut new_internal =
                        BPlusTreeInternalPage::new(self.key_schema.clone(), self.internal_max_size);
                    let middle_idx = internal_page.header.current_size as usize / 2;
                    let middle_key = internal_page.key_at(middle_idx).clone();
                    // 分裂 internal page 时，中间的 key 要向上提，而不是留在新页面
                    new_internal.batch_insert(internal_page.split_off(middle_idx));
                    let new_data = BPlusTreeInternalPageCodec::encode(&new_internal);
                    new_page_guard.data.copy_from_slice(&new_data);
                    middle_key
                }
            };

            // 写回修改后的旧页面和新页面
            let old_page_data = BPlusTreePageCodec::encode(&page);
            page_guard.data.copy_from_slice(&old_page_data);
            drop(page_guard);
            drop(new_page_guard);

            // 如果 context 为空，说明根节点分裂了，需要创建新的根
            if context.write_set.is_empty() {
                let mut new_root_guard = self.buffer_pool.new_page()?;
                let new_root_id = new_root_guard.page_id();
                let mut new_root_page =
                    BPlusTreeInternalPage::new(self.key_schema.clone(), self.internal_max_size);

                new_root_page.insert(Tuple::empty(self.key_schema.clone()), page_id);
                new_root_page.insert(middle_key, new_page_id);

                let encoded = BPlusTreeInternalPageCodec::encode(&new_root_page);
                new_root_guard.data.copy_from_slice(&encoded);
                self.root_page_id.store(new_root_id, Ordering::SeqCst);
                return Ok(());
            }

            // 否则，更新父节点
            let mut parent_guard = context.write_set.pop_back().unwrap();
            let (mut parent_page, _) =
                BPlusTreeInternalPageCodec::decode(&parent_guard.data, self.key_schema.clone())?;
            parent_page.insert(middle_key, new_page_id);

            let encoded = BPlusTreeInternalPageCodec::encode(&parent_page);
            parent_guard.data.copy_from_slice(&encoded);

            if parent_page.is_full() {
                // 如果父节点也满了，继续循环分裂
                page_guard = parent_guard;
                // 在分裂父节点之前，释放所有更上层的锁
                context.release_all_write_locks();
            } else {
                // 父节点未满，分裂过程结束
                return Ok(());
            }
        }
    }
}

#[derive(Debug)]
pub struct TreeIndexIterator {
    index: Arc<BPlusTreeIndex>,
    start_bound: Bound<Tuple>,
    end_bound: Bound<Tuple>,
    current_guard: Option<ReadPageGuard>,
    cursor: usize,
    started: bool,
}

impl TreeIndexIterator {
    pub fn new<R: RangeBounds<Tuple>>(index: Arc<BPlusTreeIndex>, range: R) -> Self {
        Self {
            index,
            start_bound: range.start_bound().cloned(),
            end_bound: range.end_bound().cloned(),
            current_guard: None,
            cursor: 0,
            started: false,
        }
    }

    /// 迭代器的核心逻辑
    pub fn next(&mut self) -> QuillSQLResult<Option<RecordId>> {
        if !self.started {
            let root_page_id = self.index.root_page_id.load(Ordering::SeqCst);
            if root_page_id == INVALID_PAGE_ID {
                return Ok(None);
            }

            let start_key = match &self.start_bound {
                Bound::Included(k) | Bound::Excluded(k) => k.clone(),
                Bound::Unbounded => {
                    let mut guard = self.index.buffer_pool.fetch_page_read(root_page_id)?;
                    loop {
                        let (page, _) =
                            BPlusTreePageCodec::decode(&guard.data, self.index.key_schema.clone())?;
                        match page {
                            BPlusTreePage::Internal(internal) => {
                                let first_child_id = internal.value_at(0);
                                guard = self.index.buffer_pool.fetch_page_read(first_child_id)?;
                            }
                            BPlusTreePage::Leaf(_leaf) => {
                                self.current_guard = Some(guard);
                                self.cursor = 0;
                                break;
                            }
                        }
                    }
                    Tuple::empty(self.index.key_schema.clone())
                }
            };

            if self.current_guard.is_none() {
                let guard = self.index.find_leaf_page(&start_key, root_page_id)?;
                let (leaf, _) =
                    BPlusTreeLeafPageCodec::decode(&guard.data, self.index.key_schema.clone())?;
                self.cursor = leaf
                    .next_closest(&start_key, matches!(self.start_bound, Bound::Included(_)))
                    .unwrap_or(leaf.header.current_size as usize);
                self.current_guard = Some(guard);
            }
            self.started = true;
        }

        if let Some(guard) = self.current_guard.as_ref() {
            let (leaf, _) =
                BPlusTreeLeafPageCodec::decode(&guard.data, self.index.key_schema.clone())?;
            if self.cursor >= leaf.header.current_size as usize {
                let next_page_id = leaf.header.next_page_id;
                if next_page_id == INVALID_PAGE_ID {
                    self.current_guard = None;
                    return Ok(None);
                }
                self.current_guard = Some(self.index.buffer_pool.fetch_page_read(next_page_id)?);
                self.cursor = 0;
                return self.next();
            }

            let (key, rid) = leaf.kv_at(self.cursor);

            let in_range = match &self.end_bound {
                Bound::Included(end_key) => key <= end_key,
                Bound::Excluded(end_key) => key < end_key,
                Bound::Unbounded => true,
            };

            if in_range {
                self.cursor += 1;
                return Ok(Some(*rid));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::catalog::SchemaRef;
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;
    use crate::storage::index::btree_index::TreeIndexIterator;
    use crate::storage::page::RecordId;
    use crate::storage::tuple::Tuple;
    use crate::{
        buffer::BufferPoolManager,
        catalog::{Column, DataType, Schema},
        storage::codec::BPlusTreePageCodec,
    };
    use rand::seq::SliceRandom;

    use super::BPlusTreeIndex;

    fn build_index() -> (BPlusTreeIndex, SchemaRef) {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let key_schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferPoolManager::new(1000, disk_scheduler));
        let index = BPlusTreeIndex::new(key_schema.clone(), buffer_pool, 4, 4);

        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![1i8.into(), 1i16.into()]),
                RecordId::new(1, 1),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![2i8.into(), 2i16.into()]),
                RecordId::new(2, 2),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![3i8.into(), 3i16.into()]),
                RecordId::new(3, 3),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![4i8.into(), 4i16.into()]),
                RecordId::new(4, 4),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![5i8.into(), 5i16.into()]),
                RecordId::new(5, 5),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![6i8.into(), 6i16.into()]),
                RecordId::new(6, 6),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![7i8.into(), 7i16.into()]),
                RecordId::new(7, 7),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![8i8.into(), 8i16.into()]),
                RecordId::new(8, 8),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![9i8.into(), 9i16.into()]),
                RecordId::new(9, 9),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![10i8.into(), 10i16.into()]),
                RecordId::new(10, 10),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![11i8.into(), 11i16.into()]),
                RecordId::new(11, 11),
            )
            .unwrap();
        (index, key_schema)
    }

    fn build_simple_index() -> (BPlusTreeIndex, SchemaRef) {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let key_schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferPoolManager::new(1000, disk_scheduler));
        let index = BPlusTreeIndex::new(key_schema.clone(), buffer_pool, 4, 4);

        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![1i8.into(), 1i16.into()]),
                RecordId::new(1, 1),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![2i8.into(), 2i16.into()]),
                RecordId::new(2, 2),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![3i8.into(), 3i16.into()]),
                RecordId::new(3, 3),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![4i8.into(), 4i16.into()]),
                RecordId::new(4, 4),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![5i8.into(), 5i16.into()]),
                RecordId::new(5, 5),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![6i8.into(), 6i16.into()]),
                RecordId::new(6, 6),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![7i8.into(), 7i16.into()]),
                RecordId::new(7, 7),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![8i8.into(), 8i16.into()]),
                RecordId::new(8, 8),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![9i8.into(), 9i16.into()]),
                RecordId::new(9, 9),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![10i8.into(), 10i16.into()]),
                RecordId::new(10, 10),
            )
            .unwrap();
        index
            .insert(
                &Tuple::new(key_schema.clone(), vec![11i8.into(), 11i16.into()]),
                RecordId::new(11, 11),
            )
            .unwrap();
        (index, key_schema)
    }

    #[test]
    pub fn test_index_get() {
        let (index, key_schema) = build_index();
        assert_eq!(
            index
                .get(&Tuple::new(
                    key_schema.clone(),
                    vec![3i8.into(), 3i16.into()],
                ))
                .unwrap(),
            Some(RecordId::new(3, 3))
        );
        assert_eq!(
            index
                .get(&Tuple::new(
                    key_schema.clone(),
                    vec![10i8.into(), 10i16.into()],
                ))
                .unwrap(),
            Some(RecordId::new(10, 10))
        );
    }

    #[test]
    pub fn test_delete1() {
        let (index, key_schema) = build_simple_index();

        let keys: Vec<i64> = vec![1, 2, 3, 4, 5];
        for &key in &keys {
            let tuple = Tuple::new(
                key_schema.clone(),
                vec![(key as i8).into(), (key as i16).into()],
            );
            assert!(index.get(&tuple).unwrap().is_some());
        }

        let remove_keys: Vec<i64> = vec![1, 5];
        for &key in &remove_keys {
            let tuple = Tuple::new(
                key_schema.clone(),
                vec![(key as i8).into(), (key as i16).into()],
            );
            index.delete(&tuple).unwrap();
        }

        let mut size = 0;
        for &key in &keys {
            let tuple = Tuple::new(
                key_schema.clone(),
                vec![(key as i8).into(), (key as i16).into()],
            );
            let is_present = index.get(&tuple).unwrap().is_some();
            if is_present {
                assert!(!remove_keys.contains(&key));
                size += 1;
            } else {
                assert!(remove_keys.contains(&key));
            }
        }
        assert_eq!(size, 3);
    }

    #[test]
    pub fn test_delete2() {
        let (index, key_schema) = build_simple_index();

        let keys: Vec<i64> = vec![1, 2, 3, 4, 5];
        for &key in &keys {
            let tuple = Tuple::new(
                key_schema.clone(),
                vec![(key as i8).into(), (key as i16).into()],
            );
            assert!(index.get(&tuple).unwrap().is_some());
        }

        let remove_keys: Vec<i64> = vec![1, 5, 3, 4];
        for &key in &remove_keys {
            let tuple = Tuple::new(
                key_schema.clone(),
                vec![(key as i8).into(), (key as i16).into()],
            );
            index.delete(&tuple).unwrap();
        }

        let mut size = 0;
        for &key in &keys {
            let tuple = Tuple::new(
                key_schema.clone(),
                vec![(key as i8).into(), (key as i16).into()],
            );
            let is_present = index.get(&tuple).unwrap().is_some();
            if is_present {
                assert!(!remove_keys.contains(&key));
                size += 1;
            } else {
                assert!(remove_keys.contains(&key));
            }
        }
        assert_eq!(size, 1);
    }

    #[test]
    pub fn test_scale() {
        let key_schema = Arc::new(Schema::new(vec![Column::new("a", DataType::Int64, false)]));
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferPoolManager::new(30, disk_scheduler));
        let index = BPlusTreeIndex::new(key_schema.clone(), buffer_pool.clone(), 3, 3);

        let scale = 1000;
        let mut keys: Vec<i64> = (1..=scale).collect();
        keys.shuffle(&mut rand::thread_rng());
        println!("\n--- Shuffled keys for insertion ---\n{:?}", keys);

        for &key in &keys {
            let tuple = Tuple::new(key_schema.clone(), vec![key.into()]);
            let rid = RecordId::new(key as u32, key as u32);
            index.insert(&tuple, rid).unwrap();
        }

        let mut remove_keys = Vec::new();
        for i in 0..scale / 2 {
            remove_keys.push(keys[i as usize]);
        }
        println!("\n--- Keys to be removed ---\n{:?}", remove_keys);

        for &key in &remove_keys {
            let tuple = Tuple::new(key_schema.clone(), vec![key.into()]);
            index.delete(&tuple).unwrap();
        }

        println!("\n--- Verifying final state ---");
        for &key in &keys {
            let tuple = Tuple::new(key_schema.clone(), vec![key.into()]);
            let is_present = index.get(&tuple).unwrap().is_some();
            if !is_present {
                assert!(
                    remove_keys.contains(&key),
                    "key {} was not found but was not in remove_keys",
                    key
                );
            } else {
                assert!(
                    !remove_keys.contains(&key),
                    "key {} was found but was in remove_keys",
                    key
                );
            }
        }

        let mut iter = TreeIndexIterator::new(Arc::new(index), ..);
        let mut remaining_keys: Vec<i64> = keys
            .into_iter()
            .filter(|k| !remove_keys.contains(k))
            .collect();
        remaining_keys.sort();

        let mut iter_result = Vec::new();
        while let Some(rid) = iter.next().unwrap() {
            iter_result.push(rid.slot_num as i64);
        }

        assert_eq!(remaining_keys, iter_result);
    }

    // #[test]
    // pub fn test_concurrent_insert() {
    //     let runs = 3;
    //     let scale_factor = 1000;
    //     let thread_nums = 4;

    //     for _ in 0..runs {
    //         let key_schema = Arc::new(Schema::new(vec![Column::new("a", DataType::Int64, false)]));
    //         let temp_dir = TempDir::new().unwrap();
    //         let temp_path = temp_dir.path().join("test.db");
    //         let disk_manager = DiskManager::try_new(temp_path).unwrap();
    //         let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
    //         let buffer_pool = Arc::new(BufferPoolManager::new(50, disk_scheduler));
    //         let index = Arc::new(BPlusTreeIndex::new(
    //             key_schema.clone(),
    //             buffer_pool.clone(),
    //             4,
    //             4,
    //         ));

    //         let mut keys: Vec<i64> = (1..scale_factor).collect();
    //         keys.shuffle(&mut rand::thread_rng());

    //         let mut threads = vec![];
    //         for i in 0..thread_nums {
    //             let index_clone = index.clone();
    //             let key_schema_clone = key_schema.clone();
    //             let keys_clone = keys.clone();
    //             threads.push(std::thread::spawn(move || {
    //                 for key in &keys_clone {
    //                     if key % thread_nums as i64 == i as i64 {
    //                         let tuple = Tuple::new(key_schema_clone.clone(), vec![(*key).into()]);
    //                         let rid = RecordId::new(*key as u32, *key as u32);
    //                         index_clone.insert(&tuple, rid).unwrap();
    //                     }
    //                 }
    //             }));
    //         }

    //         for t in threads {
    //             t.join().unwrap();
    //         }

    //         for key in &keys {
    //             let tuple = Tuple::new(key_schema.clone(), vec![(*key).into()]);
    //             let result = index.get(&tuple).unwrap();
    //             assert!(result.is_some());
    //             assert_eq!(result.unwrap().slot_num, *key as u32);
    //         }
    //     }
    // }

    #[test]
    pub fn test_index_insert1() {
        // create KeyComparator and index schema
        let key_schema = Arc::new(Schema::new(vec![Column::new("a", DataType::Int64, false)]));

        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferPoolManager::new(50, disk_scheduler));

        // create b+ tree
        let index = BPlusTreeIndex::new(key_schema.clone(), buffer_pool.clone(), 2, 3);
        let key = 42i64;
        let tuple = Tuple::new(key_schema.clone(), vec![key.into()]);
        let rid = RecordId::new(key as u32, key as u32);
        index.insert(&tuple, rid).unwrap();

        let root_page_id = index.root_page_id.load(std::sync::atomic::Ordering::SeqCst);
        let root_guard = buffer_pool.fetch_page_read(root_page_id).unwrap();
        let (root_page, _) = BPlusTreePageCodec::decode(&root_guard.data, key_schema).unwrap();

        assert!(matches!(
            root_page,
            crate::storage::page::BPlusTreePage::Leaf(_)
        ));

        if let crate::storage::page::BPlusTreePage::Leaf(root_as_leaf) = root_page {
            assert_eq!(root_as_leaf.header.current_size, 1);
            assert_eq!(root_as_leaf.array[0].0, tuple);
            assert_eq!(root_as_leaf.array[0].1, rid);
        }
    }

    #[test]
    pub fn test_index_insert2() {
        let key_schema = Arc::new(Schema::new(vec![Column::new("a", DataType::Int64, false)]));

        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferPoolManager::new(50, disk_scheduler));
        let index = BPlusTreeIndex::new(key_schema.clone(), buffer_pool, 2, 3);

        let keys: Vec<i64> = vec![1, 2, 3, 4, 5, 6, 7];
        for key in &keys {
            let tuple = Tuple::new(key_schema.clone(), vec![(*key).into()]);
            let rid = RecordId::new(*key as u32, *key as u32);
            index.insert(&tuple, rid).unwrap();
        }

        let mut rids = Vec::new();
        for key in &keys {
            let tuple = Tuple::new(key_schema.clone(), vec![(*key).into()]);
            let result = index.get(&tuple).unwrap();
            assert!(result.is_some());
            rids.push(result.unwrap());
        }

        for (i, key) in keys.iter().enumerate() {
            assert_eq!(rids[i].slot_num, *key as u32);
        }

        let mut size = 0;
        for key in &keys {
            let tuple = Tuple::new(key_schema.clone(), vec![(*key).into()]);
            let is_present = index.get(&tuple).unwrap().is_some();
            assert!(is_present);
            size += 1;
        }
        assert_eq!(size, keys.len());
    }

    #[test]
    pub fn test_index_insert3() {
        let key_schema = Arc::new(Schema::new(vec![Column::new("a", DataType::Int64, false)]));
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferPoolManager::new(50, disk_scheduler));
        let index = BPlusTreeIndex::new(key_schema.clone(), buffer_pool.clone(), 3, 4);

        let keys: Vec<i64> = vec![5, 4, 3, 2, 1];
        for key in &keys {
            let tuple = Tuple::new(key_schema.clone(), vec![(*key).into()]);
            let rid = RecordId::new(*key as u32, *key as u32);
            index.insert(&tuple, rid).unwrap();
        }

        let mut rids = Vec::new();
        for key in &keys {
            let tuple = Tuple::new(key_schema.clone(), vec![(*key).into()]);
            let result = index.get(&tuple).unwrap();
            assert!(result.is_some());
            rids.push(result.unwrap());
        }

        let mut iter = TreeIndexIterator::new(Arc::new(index), ..);
        let mut current_key = 1;
        while let Some(rid) = iter.next().unwrap() {
            assert_eq!(rid.slot_num, current_key);
            current_key += 1;
        }
        assert_eq!(current_key, keys.len() as u32 + 1);
    }

    #[test]
    pub fn test_index_iterator() {
        let (index, key_schema) = build_index();
        let index = Arc::new(index);

        let end_tuple1 = Tuple::new(key_schema.clone(), vec![3i8.into(), 3i16.into()]);
        let mut iterator1 = TreeIndexIterator::new(index.clone(), ..end_tuple1);
        assert_eq!(iterator1.next().unwrap(), Some(RecordId::new(1, 1)));
        assert_eq!(iterator1.next().unwrap(), Some(RecordId::new(2, 2)));
        assert_eq!(iterator1.next().unwrap(), None);

        let start_tuple2 = Tuple::new(key_schema.clone(), vec![3i8.into(), 3i16.into()]);
        let end_tuple2 = Tuple::new(key_schema.clone(), vec![5i8.into(), 5i16.into()]);
        let mut iterator2 = TreeIndexIterator::new(index.clone(), start_tuple2..=end_tuple2);
        assert_eq!(iterator2.next().unwrap(), Some(RecordId::new(3, 3)));
        assert_eq!(iterator2.next().unwrap(), Some(RecordId::new(4, 4)));
        assert_eq!(iterator2.next().unwrap(), Some(RecordId::new(5, 5)));
        assert_eq!(iterator2.next().unwrap(), None);

        let start_tuple3 = Tuple::new(key_schema.clone(), vec![6i8.into(), 6i16.into()]);
        let end_tuple3 = Tuple::new(key_schema.clone(), vec![8i8.into(), 8i16.into()]);
        let mut iterator3 = TreeIndexIterator::new(
            index.clone(),
            (Bound::Excluded(start_tuple3), Bound::Excluded(end_tuple3)),
        );
        assert_eq!(iterator3.next().unwrap(), Some(RecordId::new(7, 7)));

        let start_tuple4 = Tuple::new(key_schema.clone(), vec![9i8.into(), 9i16.into()]);
        let mut iterator4 = TreeIndexIterator::new(index.clone(), start_tuple4..);
        assert_eq!(iterator4.next().unwrap(), Some(RecordId::new(9, 9)));
        assert_eq!(iterator4.next().unwrap(), Some(RecordId::new(10, 10)));
        assert_eq!(iterator4.next().unwrap(), Some(RecordId::new(11, 11)));
        assert_eq!(iterator4.next().unwrap(), None);
        assert_eq!(iterator4.next().unwrap(), None);
    }
}
