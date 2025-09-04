use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

use log::debug;

use crate::buffer::{AtomicPageId, PageId, PageRef, INVALID_PAGE_ID};
use crate::catalog::SchemaRef;
use crate::error::QuillSQLResult;
use crate::storage::index::Index;
use crate::utils::util::page_bytes_to_array;
use crate::storage::codec::{
    BPlusTreeInternalPageCodec, BPlusTreeLeafPageCodec, BPlusTreePageCodec,
};
use crate::storage::page::{self, InternalKV, LeafKV};
use crate::{
    buffer::BufferPoolManager,
    storage::page::{BPlusTreeInternalPage, BPlusTreeLeafPage, BPlusTreePage, RecordId},
    error::QuillSQLError,
};

use crate::storage::tuple::Tuple;

struct Context {
    pub root_page_id: PageId,
    pub write_set: VecDeque<PageId>,
    pub read_set: VecDeque<PageId>,
}
impl Context {
    pub fn new(root_page_id: PageId) -> Self {
        Self {
            root_page_id,
            write_set: VecDeque::new(),
            read_set: VecDeque::new(),
        }
    }
}

// B+树索引
#[derive(Debug)]
pub struct BPlusTreeIndex {
    pub key_schema: SchemaRef,
    pub buffer_pool: Arc<BufferPoolManager>,
    pub internal_max_size: u32,
    pub leaf_max_size: u32,
    pub root_page_id: AtomicPageId,
}

impl Index for BPlusTreeIndex {
    fn insert(&self, key: &Tuple, value: RecordId) -> QuillSQLResult<()> {
        self.insert(key, value)
    }

    fn get(&self, key: &Tuple) -> QuillSQLResult<Option<RecordId>> {
        self.get(key)
    }

    fn delete(&self, key: &Tuple) -> QuillSQLResult<()> {
        self.delete(key)
    }

    fn key_schema(&self) -> &SchemaRef {
        &self.key_schema
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
            root_page_id: AtomicPageId::new(INVALID_PAGE_ID),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.root_page_id.load(Ordering::SeqCst) == INVALID_PAGE_ID
    }

    pub fn insert(&self, key: &Tuple, rid: RecordId) -> QuillSQLResult<()> {
        let tid = thread::current().id();
        debug!("[{:?}] Insert: Starting insert for key {:?}", tid, key);
        if self.is_empty() {
            debug!("[{:?}] Insert: Tree is empty, attempting to start new tree.", tid);
            match self.start_new_tree(key, rid) {
                Ok(_) => {
                    debug!("[{:?}] Insert: Successfully started new tree.", tid);
                    return Ok(());
                },
                Err(e) => {
                    if e.to_string().contains("Race condition detected") {
                         debug!("[{:?}] Insert: Lost race to start new tree, falling back to conservative insert.", tid);
                         return self.insert_conservative(key, rid);
                    }
                    return Err(e);
                }
            }
        }
        self.insert_optimistic(key, rid)
    }
    
    pub fn insert_optimistic(&self, key: &Tuple, rid: RecordId) -> QuillSQLResult<()> {
        let tid = thread::current().id();
        debug!("[{:?}] Optimistic Insert: Starting for key {:?}", tid, key);
        
        let leaf_page_ref = match self.find_leaf_page_concurrent(key)? {
            Some(page) => page,
            None => {
                debug!("[{:?}] Optimistic Insert: Tree seems empty, falling back to conservative.", tid);
                return self.insert_conservative(key, rid);
            }
        };

        let page_id = leaf_page_ref.read().unwrap().page_id;
        debug!("[{:?}] Optimistic Insert: Found leaf page {}, acquiring write lock.", tid, page_id);
        
        let mut leaf_page_write_guard = leaf_page_ref.write().unwrap();
        let (mut leaf_page, _) = BPlusTreeLeafPageCodec::decode(&leaf_page_write_guard.data(), self.key_schema.clone())?;

        if !leaf_page.is_full() {
            debug!("[{:?}] Optimistic Insert: Page {} is not full. Inserting.", tid, page_id);
            leaf_page.insert(key.clone(), rid);
            leaf_page_write_guard.set_data(page_bytes_to_array(&BPlusTreeLeafPageCodec::encode(&leaf_page)));
            debug!("[{:?}] Optimistic Insert: Success on page {}. Releasing write lock.", tid, page_id);
            return Ok(());
        } else {
            debug!("[{:?}] Optimistic Insert: Page {} is full. Releasing lock and falling back to conservative.", tid, page_id);
            drop(leaf_page_write_guard);
            return self.insert_conservative(key, rid);
        }
    }
    
    fn insert_conservative(&self, key: &Tuple, rid: RecordId) -> QuillSQLResult<()> {
        let tid = thread::current().id();
        debug!("[{:?}] Conservative Insert: Starting for key {:?}", tid, key);

        let root_page_id = self.root_page_id.load(Ordering::SeqCst);
        let mut locked_path: Vec<PageRef> = Vec::new();
        let root_page_ref = self.buffer_pool.fetch_page(root_page_id)?;
        locked_path.push(root_page_ref);

        'traversal: loop {
            let current_page_ref = locked_path.last().unwrap();
            let (is_leaf, next_page_id_opt) = {
                let current_page_guard = current_page_ref.read().unwrap();
                let page_id = current_page_guard.page_id;
                debug!("[{:?}] Conservative Traverse: Reading page {}", tid, page_id);
                
                let (current_tree_page, _) =
                    BPlusTreePageCodec::decode(&current_page_guard.data(), self.key_schema.clone())?;
                
                match &current_tree_page {
                    BPlusTreePage::Internal(internal_page) => {
                        let next_id = internal_page.look_up(key);
                        debug!("[{:?}] Conservative Traverse: Page {} is internal, next page is {}", tid, page_id, next_id);
                        (false, Some(next_id))
                    }
                    BPlusTreePage::Leaf(_) => {
                        debug!("[{:?}] Conservative Traverse: Page {} is leaf. Traversal done.", tid, page_id);
                        (true, None)
                    }
                }
            };

            if is_leaf {
                break 'traversal;
            }

            let next_page_id = next_page_id_opt.unwrap();
            let next_page_ref = self.buffer_pool.fetch_page(next_page_id)?;

            let child_is_safe = {
                let next_page_guard = next_page_ref.read().unwrap();
                let (next_tree_page, _) =
                    BPlusTreePageCodec::decode(&next_page_guard.data(), self.key_schema.clone())?;
                !next_tree_page.is_full()
            };

            // ★★★ 核心修复 ★★★
            // 使用 drain 来安全地释放祖先节点的锁，同时保留父节点的原始 PageRef
            if child_is_safe {
                debug!("[{:?}] Conservative Traverse: Child page {} is safe. Releasing ancestor locks.", tid, next_page_id);
                if locked_path.len() > 1 {
                    // 移除从 0 到倒数第二个元素，只留下最后一个（即父节点）
                    // 这样就释放了所有祖先节点的锁
                    locked_path.drain(0..locked_path.len() - 1);
                }
            } else {
                debug!("[{:?}] Conservative Traverse: Child page {} is NOT safe. Keeping parent lock.", tid, next_page_id);
            }

            locked_path.push(next_page_ref);
    
        }

        debug!("[{:?}] Conservative Insert: Path locked {:?}. Starting split/update phase.", tid, locked_path.iter().map(|p| p.read().unwrap().page_id).collect::<Vec<_>>());

        let mut new_child_entry: Option<(Tuple, u32)> = None;

        while let Some(page_to_process_ref) = locked_path.pop() {
            let mut page_guard = page_to_process_ref.write().unwrap();
            let page_id = page_guard.page_id;
            let (mut tree_page, _) =
                BPlusTreePageCodec::decode(&page_guard.data(), self.key_schema.clone())?;
            
            debug!("[{:?}] Conservative Update: Processing page {} with write lock.", tid, page_id);

            match new_child_entry {
                Some((ref new_key, new_page_id)) => {
                    if let BPlusTreePage::Internal(internal) = &mut tree_page {
                        debug!("[{:?}] Conservative Update: Inserting split result ({:?}, {}) into internal page {}", tid, new_key, new_page_id, page_id);
                        internal.insert(new_key.clone(), new_page_id);
                    }
                }
                None => {
                    if let BPlusTreePage::Leaf(leaf) = &mut tree_page {
                        debug!("[{:?}] Conservative Update: Inserting original key {:?} into leaf page {}", tid, key, page_id);
                        leaf.insert(key.clone(), rid);
                    }
                }
            }

            if tree_page.is_full() {
                debug!("[{:?}] Conservative Update: Page {} is full. Splitting.", tid, page_id);
                new_child_entry = Some(self.split(&mut tree_page)?);
                
                if locked_path.is_empty() {
                    debug!("[{:?}] Conservative Update: Root page {} split. Creating new root.", tid, page_id);
                    let old_root_page_id = page_id;
                    let new_root_page_ref = self.buffer_pool.new_page()?;
                    let new_root_page_id = new_root_page_ref.read().unwrap().page_id;
                    let (new_key, new_child_page_id) = new_child_entry.take().unwrap();
                    
                    {
                        let mut new_root_guard = new_root_page_ref.write().unwrap();
                        let mut new_root_page = BPlusTreeInternalPage::new(self.key_schema.clone(), self.internal_max_size);
                        new_root_page.insert(Tuple::empty(self.key_schema.clone()), old_root_page_id);
                        new_root_page.insert(new_key, new_child_page_id);
                        new_root_guard.set_data(page_bytes_to_array(&BPlusTreeInternalPageCodec::encode(&new_root_page)));
                    }
                    
                    self.root_page_id.store(new_root_page_id, Ordering::SeqCst);
                    debug!("[{:?}] Conservative Update: New root is page {}. Atomically updated root pointer.", tid, new_root_page_id);
                    new_child_entry = None;
                }
            } else {
                new_child_entry = None;
            }
            
            page_guard.set_data(page_bytes_to_array(&BPlusTreePageCodec::encode(&tree_page)));
            debug!("[{:?}] Conservative Update: Wrote back page {}. Releasing write lock.", tid, page_id);

            if new_child_entry.is_none() {
                break;
            }
        }
        Ok(())
    }

    pub fn delete(&self, key: &Tuple) -> QuillSQLResult<()> {
        if self.is_empty() {
            return Ok(());
        }
        let mut context = Context::new(self.root_page_id.load(Ordering::SeqCst));
        // 找到leaf page
        let Some(leaf_page) = self.find_leaf_page(key, &mut context)? else {
            return Err(QuillSQLError::Storage(
                "Cannot find leaf page to delete".to_string(),
            ));
        };
        let (mut leaf_tree_page, _) = BPlusTreeLeafPageCodec::decode(
            leaf_page.read().unwrap().data(),
            self.key_schema.clone(),
        )?;
        leaf_tree_page.delete(key);
        leaf_page
            .write()
            .unwrap()
            .set_data(page_bytes_to_array(&BPlusTreeLeafPageCodec::encode(
                &leaf_tree_page,
            )));

        let mut curr_tree_page = BPlusTreePage::Leaf(leaf_tree_page);
        let mut curr_page_id = leaf_page.read().unwrap().page_id;

        // leaf page未达到半满则从兄弟节点借一个或合并
        while curr_tree_page.is_underflow(self.root_page_id.load(Ordering::SeqCst) == curr_page_id)
        {
            let Some(parent_page_id) = context.read_set.pop_back() else {
                return Err(QuillSQLError::Storage("Cannot find parent page".to_string()));
            };
            let (left_sibling_page_id, right_sibling_page_id) =
                self.find_sibling_pages(parent_page_id, curr_page_id)?;

            // 尝试从左兄弟借一个
            if let Some(left_sibling_page_id) = left_sibling_page_id {
                if self.borrow_max_kv(parent_page_id, curr_page_id, left_sibling_page_id)? {
                    break;
                }
            }

            // 尝试从右兄弟借一个
            if let Some(right_sibling_page_id) = right_sibling_page_id {
                if self.borrow_min_kv(parent_page_id, curr_page_id, right_sibling_page_id)? {
                    break;
                }
            }

            let new_parent_page_id = if let Some(left_sibling_page_id) = left_sibling_page_id {
                // 跟左兄弟合并
                self.merge(parent_page_id, left_sibling_page_id, curr_page_id)?
            } else if let Some(right_sibling_page_id) = right_sibling_page_id {
                // 跟右兄弟合并
                self.merge(parent_page_id, curr_page_id, right_sibling_page_id)?
            } else {
                return Err(QuillSQLError::Storage(
                    "Cannot process index page borrow or merge".to_string(),
                ));
            };
            let (_, new_parent_tree_page) = self
                .buffer_pool
                .fetch_tree_page(new_parent_page_id, self.key_schema.clone())?;

            curr_page_id = new_parent_page_id;
            curr_tree_page = new_parent_tree_page;
        }

        Ok(())
    }

    fn start_new_tree(&self, key: &Tuple, rid: RecordId) -> QuillSQLResult<()> {
        let tid = thread::current().id();
        debug!("[{:?}] Start New Tree: Attempting for key {:?}", tid, key);
        let new_page = self.buffer_pool.new_page()?;
        let new_page_id = new_page.read().unwrap().page_id;
        
        let mut leaf_page = BPlusTreeLeafPage::new(self.key_schema.clone(), self.leaf_max_size);
        leaf_page.insert(key.clone(), rid);
        new_page.write().unwrap().set_data(page_bytes_to_array(&BPlusTreeLeafPageCodec::encode(&leaf_page)));

        let result = self.root_page_id.compare_exchange(
            INVALID_PAGE_ID,
            new_page_id,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );

        match result {
            Ok(_) => {
                debug!("[{:?}] Start New Tree: Success! New root is page {}", tid, new_page_id);
                Ok(())
            },
            Err(_) => {
                debug!("[{:?}] Start New Tree: Lost race! Deleting orphaned page {}", tid, new_page_id);
                self.buffer_pool.delete_page(new_page_id)?;
                Err(QuillSQLError::Internal("Race condition detected: Tree already initialized".to_string()))
            }
        }
    }

    // 找到叶子节点上对应的Value
    pub fn get(&self, key: &Tuple) -> QuillSQLResult<Option<RecordId>> {
        if self.is_empty() {
            return Ok(None);
        }

        // 找到leaf page
        let Some(leaf_page) = self.find_leaf_page_concurrent(key)? else {
            return Ok(None);
        };
        let (leaf_tree_page, _) = BPlusTreeLeafPageCodec::decode(
            leaf_page.read().unwrap().data(),
            self.key_schema.clone(),
        )?;
        let result = leaf_tree_page.look_up(key);
        Ok(result)
    }

    fn find_leaf_page(&self, key: &Tuple, context: &mut Context) -> QuillSQLResult<Option<PageRef>> {
        if self.is_empty() {
            return Ok(None);
        }
        let (mut curr_page, mut curr_tree_page) = self.buffer_pool.fetch_tree_page(
            self.root_page_id.load(Ordering::SeqCst),
            self.key_schema.clone(),
        )?;

        // 找到leaf page
        loop {
            match curr_tree_page {
                BPlusTreePage::Internal(internal_page) => {
                    context
                        .read_set
                        .push_back(curr_page.read().unwrap().page_id);
                    // 查找下一页
                    let next_page_id = internal_page.look_up(key);
                    let (next_page, next_tree_page) = self
                        .buffer_pool
                        .fetch_tree_page(next_page_id, self.key_schema.clone())?;
                    curr_page = next_page;
                    curr_tree_page = next_tree_page;
                }
                BPlusTreePage::Leaf(_leaf_page) => {
                    return Ok(Some(curr_page));
                }
            }
        }
    }

    fn find_leaf_page_concurrent(&self, key: &Tuple) -> QuillSQLResult<Option<PageRef>> {
        if self.is_empty() {
            return Ok(None);
        }

        let mut current_page_id = self.root_page_id.load(Ordering::SeqCst);

        // 循环直到找到叶子节点
        loop {
            // 1. 获取并持有当前页面的 PageRef（这会 pin 住页面）
            let current_page_ref = self.buffer_pool.fetch_page(current_page_id)?;
            
            // 2. 获取当前页面的读锁，锁守卫的生命周期将持续到循环的末尾或显式 drop
            let page_read_guard = current_page_ref.read().unwrap();

            // 3. 解码页面内容
            let (tree_page, _) =
                BPlusTreePageCodec::decode(page_read_guard.data(), self.key_schema.clone())?;

            match tree_page {
                BPlusTreePage::Internal(internal_page) => {
                    // a. 如果是内部节点，查找下一个子页面的 ID
                    let next_page_id = internal_page.look_up(key);
                    
                    // b. 关键：在释放当前节点的锁之前，我们不需要做任何事。
                    //    因为下一次循环开始时，会 fetch 新的页面，
                    //    而旧的 `current_page_ref` 和 `page_read_guard` 会在
                    //    下一次循环赋值时被自动 drop，从而安全地 unpin 和 unlock。
                    //    这种隐式的锁耦合是 Rust RAII 的强大之处。
                    current_page_id = next_page_id;
                }
                BPlusTreePage::Leaf(_) => {
                    // b. 如果是叶子节点，我们已经找到了目标。
                    //    我们不再需要持有读锁，所以显式 drop 它。
                    drop(page_read_guard); 
                    //    返回持有的 PageRef，调用者可以用它来获取写锁或读取内容。
                    return Ok(Some(current_page_ref));
                }
            }
            // `page_read_guard` 在这里离开作用域，自动释放读锁
            // `current_page_ref` 在下一次循环开始时被新的 ref 覆盖，旧的 ref 被 drop
        }
    }

    // 分裂page
    fn split(&self, tree_page: &mut BPlusTreePage) -> QuillSQLResult<InternalKV> {
        let new_page = self.buffer_pool.new_page()?;
        let new_page_id = new_page.read().unwrap().page_id;

        match tree_page {
            BPlusTreePage::Leaf(leaf_page) => {
                // 叶子节点的分裂逻辑是正确的，保持不变。
                let mut new_leaf_page =
                    BPlusTreeLeafPage::new(self.key_schema.clone(), self.leaf_max_size);
                new_leaf_page
                    .batch_insert(leaf_page.split_off(leaf_page.header.current_size as usize / 2));

                new_leaf_page.header.next_page_id = leaf_page.header.next_page_id;
                leaf_page.header.next_page_id = new_page.read().unwrap().page_id;

                new_page.write().unwrap().set_data(page_bytes_to_array(
                    &BPlusTreeLeafPageCodec::encode(&new_leaf_page),
                ));

                Ok((new_leaf_page.key_at(0).clone(), new_page_id))
            }
            BPlusTreePage::Internal(internal_page) => {
                // ★★★ 最终修正的内部节点分裂逻辑 ★★★

                // 1. 找到分裂点
                let mid_idx = internal_page.header.current_size as usize / 2;

                // 2. 将后半部分的元素移动到一个临时的 Vec 中，准备创建新页面。
                let new_kvs = internal_page.array.split_off(mid_idx);
                
                // 3. 更新旧（左）节点的 size。
                internal_page.header.current_size = internal_page.array.len() as u32;

                // 4. 从刚移出的后半部分元素中，取出第一个元素。它的 key 就是要上推的 middle_key。
                let (middle_key, first_child_of_new_page) = new_kvs.get(0).unwrap().clone();
                
                // 5. 创建新的右侧兄弟节点。
                let mut new_internal_page =
                    BPlusTreeInternalPage::new(self.key_schema.clone(), self.internal_max_size);

                // 6. 新的右侧节点的第一个指针，就是刚取出的那个 middle_key 原本关联的指针。
                //    并且它的 key 应该是哨兵（null）。
                new_internal_page.insert(Tuple::empty(self.key_schema.clone()), first_child_of_new_page);
                
                // 7. 将 new_kvs 中剩余的元素（从第二个开始）插入到新页面。
                new_internal_page.batch_insert(new_kvs[1..].to_vec());
                
                // 8. 将新页面写回缓冲池。
                new_page.write().unwrap().set_data(page_bytes_to_array(
                    &BPlusTreeInternalPageCodec::encode(&new_internal_page),
                ));

                // 9. 返回被上推的 middle_key 和新页面的 ID。
                Ok((middle_key, new_page_id))
            }
        }
    }

    fn borrow_min_kv(
        &self,
        parent_page_id: PageId,
        page_id: PageId,
        borrowed_page_id: PageId,
    ) -> QuillSQLResult<bool> {
        self.borrow(parent_page_id, page_id, borrowed_page_id, true)
    }

    fn borrow_max_kv(
        &self,
        parent_page_id: PageId,
        page_id: PageId,
        borrowed_page_id: PageId,
    ) -> QuillSQLResult<bool> {
        self.borrow(parent_page_id, page_id, borrowed_page_id, false)
    }

    fn borrow(
        &self,
        parent_page_id: PageId,
        page_id: PageId,
        borrowed_page_id: PageId,
        min_max: bool,
    ) -> QuillSQLResult<bool> {
        let (borrowed_page, mut borrowed_tree_page) = self
            .buffer_pool
            .fetch_tree_page(borrowed_page_id, self.key_schema.clone())?;
        if !borrowed_tree_page.can_borrow() {
            return Ok(false);
        }

        let (page, mut tree_page) = self
            .buffer_pool
            .fetch_tree_page(page_id, self.key_schema.clone())?;

        let (old_internal_key, new_internal_key) = match borrowed_tree_page {
            BPlusTreePage::Internal(ref mut borrowed_internal_page) => {
                let BPlusTreePage::Internal(ref mut internal_page) = tree_page else {
                    return Err(QuillSQLError::Storage(
                        "Leaf page can not borrow from internal page".to_string(),
                    ));
                };
                if min_max {
                    let kv = borrowed_internal_page.reverse_split_off(0).remove(0);
                    internal_page.insert(kv.0.clone(), kv.1);
                    (
                        kv.0,
                        self.find_subtree_min_leafkv(borrowed_internal_page.value_at(0))?
                            .0,
                    )
                } else {
                    let kv = borrowed_internal_page
                        .split_off(borrowed_internal_page.header.current_size as usize - 1)
                        .remove(0);
                    let min_key = internal_page.key_at(0).clone();
                    internal_page.insert(kv.0.clone(), kv.1);
                    (
                        min_key,
                        self.find_subtree_min_leafkv(borrowed_internal_page.value_at(0))?
                            .0,
                    )
                }
            }
            BPlusTreePage::Leaf(ref mut borrowed_leaf_page) => {
                let BPlusTreePage::Leaf(ref mut leaf_page) = tree_page else {
                    return Err(QuillSQLError::Storage(
                        "Internal page can not borrow from leaf page".to_string(),
                    ));
                };
                if min_max {
                    let kv = borrowed_leaf_page.reverse_split_off(0).remove(0);
                    leaf_page.insert(kv.0.clone(), kv.1);
                    (kv.0, borrowed_leaf_page.key_at(0).clone())
                } else {
                    let kv = borrowed_leaf_page
                        .split_off(borrowed_leaf_page.header.current_size as usize - 1)
                        .remove(0);
                    leaf_page.insert(kv.0.clone(), kv.1);
                    (leaf_page.key_at(1).clone(), leaf_page.key_at(0).clone())
                }
            }
        };

        page.write()
            .unwrap()
            .set_data(page_bytes_to_array(&BPlusTreePageCodec::encode(&tree_page)));

        borrowed_page
            .write()
            .unwrap()
            .set_data(page_bytes_to_array(&BPlusTreePageCodec::encode(
                &borrowed_tree_page,
            )));

        // 更新父节点
        let (parent_page, mut parent_internal_page) = self
            .buffer_pool
            .fetch_tree_internal_page(parent_page_id, self.key_schema.clone())?;
        parent_internal_page.replace_key(&old_internal_key, new_internal_key);

        parent_page.write().unwrap().set_data(page_bytes_to_array(
            &BPlusTreeInternalPageCodec::encode(&parent_internal_page),
        ));
        Ok(true)
    }

    fn find_sibling_pages(
        &self,
        parent_page_id: PageId,
        child_page_id: PageId,
    ) -> QuillSQLResult<(Option<PageId>, Option<PageId>)> {
        let (_, parent_internal_page) = self
            .buffer_pool
            .fetch_tree_internal_page(parent_page_id, self.key_schema.clone())?;
        Ok(parent_internal_page.sibling_page_ids(child_page_id))
    }

    fn merge(
        &self,
        parent_page_id: PageId,
        left_page_id: PageId,
        right_page_id: PageId,
    ) -> QuillSQLResult<PageId> {
        let (left_page, mut left_tree_page) = self
            .buffer_pool
            .fetch_tree_page(left_page_id, self.key_schema.clone())?;
        let (_, mut right_tree_page) = self
            .buffer_pool
            .fetch_tree_page(right_page_id, self.key_schema.clone())?;

        // 向左合入
        match left_tree_page {
            BPlusTreePage::Internal(ref mut left_internal_page) => {
                if let BPlusTreePage::Internal(ref mut right_internal_page) = right_tree_page {
                    // 空key处理
                    let mut kvs = right_internal_page.array.clone();
                    let min_leaf_kv =
                        self.find_subtree_min_leafkv(right_internal_page.value_at(0))?;
                    kvs[0].0 = min_leaf_kv.0;
                    left_internal_page.batch_insert(kvs);
                } else {
                    return Err(QuillSQLError::Storage(
                        "Leaf page can not merge from internal page".to_string(),
                    ));
                }
            }
            BPlusTreePage::Leaf(ref mut left_leaf_page) => {
                if let BPlusTreePage::Leaf(ref mut right_leaf_page) = right_tree_page {
                    left_leaf_page.batch_insert(right_leaf_page.array.clone());
                    // 更新next page id
                    left_leaf_page.header.next_page_id = right_leaf_page.header.next_page_id;
                } else {
                    return Err(QuillSQLError::Storage(
                        "Internal page can not merge from leaf page".to_string(),
                    ));
                }
            }
        };

        left_page
            .write()
            .unwrap()
            .set_data(page_bytes_to_array(&BPlusTreePageCodec::encode(
                &left_tree_page,
            )));

        // 删除右边页
        self.buffer_pool.delete_page(right_page_id)?;

        // 更新父节点
        let (parent_page, mut parent_internal_page) = self
            .buffer_pool
            .fetch_tree_internal_page(parent_page_id, self.key_schema.clone())?;
        parent_internal_page.delete_page_id(right_page_id);

        // 根节点只有一个子节点（叶子）时，则叶子节点成为新的根节点
        if parent_page_id == self.root_page_id.load(Ordering::SeqCst)
            && parent_internal_page.header.current_size == 1
        {
            self.root_page_id.store(left_page_id, Ordering::SeqCst);
            // 删除旧的根节点
            self.buffer_pool.delete_page(parent_page_id)?;
            Ok(left_page_id)
        } else {
            parent_page.write().unwrap().set_data(page_bytes_to_array(
                &BPlusTreeInternalPageCodec::encode(&parent_internal_page),
            ));
            Ok(parent_page_id)
        }
    }

    // 查找子树最小的leafKV
    fn find_subtree_min_leafkv(&self, page_id: PageId) -> QuillSQLResult<LeafKV> {
        self.find_subtree_leafkv(page_id, true)
    }

    // 查找子树最大的leafKV
    fn find_subtree_max_leafkv(&self, page_id: PageId) -> QuillSQLResult<LeafKV> {
        self.find_subtree_leafkv(page_id, false)
    }

    fn find_subtree_leafkv(&self, page_id: PageId, min_or_max: bool) -> QuillSQLResult<LeafKV> {
        let (_, mut curr_tree_page) = self
            .buffer_pool
            .fetch_tree_page(page_id, self.key_schema.clone())?;
        loop {
            match curr_tree_page {
                BPlusTreePage::Internal(internal_page) => {
                    let index = if min_or_max {
                        0
                    } else {
                        internal_page.header.current_size as usize - 1
                    };
                    let next_page_id = internal_page.value_at(index);
                    let (_, next_tree_page) = self
                        .buffer_pool
                        .fetch_tree_page(next_page_id, self.key_schema.clone())?;
                    curr_tree_page = next_tree_page;
                }
                BPlusTreePage::Leaf(leaf_page) => {
                    let index = if min_or_max {
                        0
                    } else {
                        leaf_page.header.current_size as usize - 1
                    };
                    return Ok(leaf_page.kv_at(index).clone());
                }
            }
        }
    }

    pub fn get_first_leaf_page(&self) -> QuillSQLResult<BPlusTreeLeafPage> {
        let (_, mut curr_tree_page) = self.buffer_pool.fetch_tree_page(
            self.root_page_id.load(Ordering::SeqCst),
            self.key_schema.clone(),
        )?;
        loop {
            match curr_tree_page {
                BPlusTreePage::Internal(internal_page) => {
                    let next_page_id = internal_page.value_at(0);
                    let (_, next_tree_page) = self
                        .buffer_pool
                        .fetch_tree_page(next_page_id, self.key_schema.clone())?;
                    curr_tree_page = next_tree_page;
                }
                BPlusTreePage::Leaf(leaf_page) => {
                    return Ok(leaf_page);
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct TreeIndexIterator {
    index: Arc<BPlusTreeIndex>,
    start_bound: Bound<Tuple>,
    end_bound: Bound<Tuple>,
    leaf_page: BPlusTreeLeafPage,
    cursor: usize,
    started: bool,
}

impl TreeIndexIterator {
    pub fn new<R: RangeBounds<Tuple>>(index: Arc<BPlusTreeIndex>, range: R) -> Self {
        Self {
            index,
            start_bound: range.start_bound().cloned(),
            end_bound: range.end_bound().cloned(),
            leaf_page: BPlusTreeLeafPage::empty(),
            cursor: 0,
            started: false,
        }
    }

    pub fn load_next_leaf_page(&mut self) -> QuillSQLResult<bool> {
        let next_page_id = self.leaf_page.header.next_page_id;
        if next_page_id == INVALID_PAGE_ID {
            Ok(false)
        } else {
            let (_, next_leaf_page) = self
                .index
                .buffer_pool
                .fetch_tree_leaf_page(next_page_id, self.index.key_schema.clone())?;
            self.leaf_page = next_leaf_page;
            Ok(true)
        }
    }

    pub fn next(&mut self) -> QuillSQLResult<Option<RecordId>> {
        if self.started {
            match self.end_bound.as_ref() {
                Bound::Included(end_tuple) => {
                    self.cursor += 1;
                    let end_tuple = end_tuple.clone();
                    let kv = if self.cursor >= self.leaf_page.header.current_size as usize {
                        if self.load_next_leaf_page()? {
                            self.cursor = 0;
                            self.leaf_page.array[self.cursor].clone()
                        } else {
                            return Ok(None);
                        }
                    } else {
                        self.leaf_page.array[self.cursor].clone()
                    };
                    if kv.0 <= end_tuple {
                        Ok(Some(kv.1))
                    } else {
                        Ok(None)
                    }
                }
                Bound::Excluded(end_tuple) => {
                    self.cursor += 1;
                    let end_tuple = end_tuple.clone();
                    let kv = if self.cursor >= self.leaf_page.header.current_size as usize {
                        if self.load_next_leaf_page()? {
                            self.cursor = 0;
                            self.leaf_page.array[self.cursor].clone()
                        } else {
                            return Ok(None);
                        }
                    } else {
                        self.leaf_page.array[self.cursor].clone()
                    };
                    if kv.0 < end_tuple {
                        Ok(Some(kv.1))
                    } else {
                        Ok(None)
                    }
                }
                Bound::Unbounded => {
                    self.cursor += 1;
                    if self.cursor >= self.leaf_page.header.current_size as usize {
                        if self.load_next_leaf_page()? {
                            self.cursor = 0;
                            Ok(Some(self.leaf_page.array[self.cursor].1))
                        } else {
                            Ok(None)
                        }
                    } else {
                        Ok(Some(self.leaf_page.array[self.cursor].1))
                    }
                }
            }
        } else {
            self.started = true;
            match self.start_bound.as_ref() {
                Bound::Included(start_tuple) => {
                    let mut context = Context::new(self.index.root_page_id.load(Ordering::SeqCst));
                    let Some(leaf_page) = self.index.find_leaf_page(start_tuple, &mut context)?
                    else {
                        return Ok(None);
                    };
                    self.leaf_page = BPlusTreeLeafPageCodec::decode(
                        leaf_page.read().unwrap().data(),
                        self.index.key_schema.clone(),
                    )?
                    .0;
                    if let Some(idx) = self.leaf_page.next_closest(start_tuple, true) {
                        self.cursor = idx;
                        Ok(Some(self.leaf_page.array[self.cursor].1))
                    } else if self.load_next_leaf_page()? {
                        self.cursor = 0;
                        Ok(Some(self.leaf_page.array[self.cursor].1))
                    } else {
                        Ok(None)
                    }
                }
                Bound::Excluded(start_tuple) => {
                    let mut context = Context::new(self.index.root_page_id.load(Ordering::SeqCst));
                    let Some(leaf_page) = self.index.find_leaf_page(start_tuple, &mut context)?
                    else {
                        return Ok(None);
                    };
                    self.leaf_page = BPlusTreeLeafPageCodec::decode(
                        leaf_page.read().unwrap().data(),
                        self.index.key_schema.clone(),
                    )?
                    .0;
                    if let Some(idx) = self.leaf_page.next_closest(start_tuple, false) {
                        self.cursor = idx;
                        Ok(Some(self.leaf_page.array[self.cursor].1))
                    } else if self.load_next_leaf_page()? {
                        self.cursor = 0;
                        Ok(Some(self.leaf_page.array[self.cursor].1))
                    } else {
                        Ok(None)
                    }
                }
                Bound::Unbounded => {
                    self.leaf_page = self.index.get_first_leaf_page()?;
                    self.cursor = 0;
                    Ok(Some(self.leaf_page.array[self.cursor].1))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use rand::seq::SliceRandom;
    use rand::Rng;
    use tempfile::TempDir;

    use crate::catalog::SchemaRef;
    use crate::storage::index::bplus_index::TreeIndexIterator;
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;
    use crate::storage::page::RecordId;
    use crate::storage::tuple::Tuple;
    use crate::utils::util::pretty_format_index_tree;
    use crate::{
        buffer::BufferPoolManager,
        catalog::{Column, DataType, Schema},
    };

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

    #[test]
    pub fn test_index_insert() {
        let (index, _) = build_index();
        let display = pretty_format_index_tree(&index).unwrap();
        println!("{display}");
        assert_eq!(display, "B+ Tree Level No.1:
+-----------------------+
| page_id=13, size: 2/4 |
+-----------------------+
| +------------+------+ |
| | NULL, NULL | 5, 5 | |
| +------------+------+ |
| | 8          | 12   | |
| +------------+------+ |
+-----------------------+
B+ Tree Level No.2:
+-----------------------+------------------------+
| page_id=8, size: 2/4  | page_id=12, size: 3/4  |
+-----------------------+------------------------+
| +------------+------+ | +------+------+------+ |
| | NULL, NULL | 3, 3 | | | 5, 5 | 7, 7 | 9, 9 | |
| +------------+------+ | +------+------+------+ |
| | 6          | 7    | | | 9    | 10   | 11   | |
| +------------+------+ | +------+------+------+ |
+-----------------------+------------------------+
B+ Tree Level No.3:
+--------------------------------------+--------------------------------------+---------------------------------------+----------------------------------------+---------------------------------------+
| page_id=6, size: 2/4, next_page_id=7 | page_id=7, size: 2/4, next_page_id=9 | page_id=9, size: 2/4, next_page_id=10 | page_id=10, size: 2/4, next_page_id=11 | page_id=11, size: 3/4, next_page_id=0 |
+--------------------------------------+--------------------------------------+---------------------------------------+----------------------------------------+---------------------------------------+
| +------+------+                      | +------+------+                      | +------+------+                       | +------+------+                        | +------+--------+--------+            |
| | 1, 1 | 2, 2 |                      | | 3, 3 | 4, 4 |                      | | 5, 5 | 6, 6 |                       | | 7, 7 | 8, 8 |                        | | 9, 9 | 10, 10 | 11, 11 |            |
| +------+------+                      | +------+------+                      | +------+------+                       | +------+------+                        | +------+--------+--------+            |
| | 1-1  | 2-2  |                      | | 3-3  | 4-4  |                      | | 5-5  | 6-6  |                       | | 7-7  | 8-8  |                        | | 9-9  | 10-10  | 11-11  |            |
| +------+------+                      | +------+------+                      | +------+------+                       | +------+------+                        | +------+--------+--------+            |
+--------------------------------------+--------------------------------------+---------------------------------------+----------------------------------------+---------------------------------------+
");
    }

    #[test]
    pub fn test_index_delete() {
        let (index, key_schema) = build_index();

        index
            .delete(&Tuple::new(
                key_schema.clone(),
                vec![3i8.into(), 3i16.into()],
            ))
            .unwrap();
        println!("{}", pretty_format_index_tree(&index).unwrap());
        index
            .delete(&Tuple::new(
                key_schema.clone(),
                vec![10i8.into(), 10i16.into()],
            ))
            .unwrap();
        println!("{}", pretty_format_index_tree(&index).unwrap());
        index
            .delete(&Tuple::new(
                key_schema.clone(),
                vec![8i8.into(), 8i16.into()],
            ))
            .unwrap();
        println!("{}", pretty_format_index_tree(&index).unwrap());

        assert_eq!(pretty_format_index_tree(&index).unwrap(),
                   "B+ Tree Level No.1:
+------------------------------+
| page_id=8, size: 3/4         |
+------------------------------+
| +------------+------+------+ |
| | NULL, NULL | 5, 5 | 7, 7 | |
| +------------+------+------+ |
| | 6          | 9    | 10   | |
| +------------+------+------+ |
+------------------------------+
B+ Tree Level No.2:
+--------------------------------------+---------------------------------------+---------------------------------------+
| page_id=6, size: 3/4, next_page_id=9 | page_id=9, size: 2/4, next_page_id=10 | page_id=10, size: 3/4, next_page_id=0 |
+--------------------------------------+---------------------------------------+---------------------------------------+
| +------+------+------+               | +------+------+                       | +------+------+--------+              |
| | 1, 1 | 2, 2 | 4, 4 |               | | 5, 5 | 6, 6 |                       | | 7, 7 | 9, 9 | 11, 11 |              |
| +------+------+------+               | +------+------+                       | +------+------+--------+              |
| | 1-1  | 2-2  | 4-4  |               | | 5-5  | 6-6  |                       | | 7-7  | 9-9  | 11-11  |              |
| +------+------+------+               | +------+------+                       | +------+------+--------+              |
+--------------------------------------+---------------------------------------+---------------------------------------+
");
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

    // Helper function to build a larger B+ Tree with a non-overflowing data type.
    fn build_large_index() -> (BPlusTreeIndex, SchemaRef) {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test_concurrent.db");

        // Use i32 to avoid overflow, as we're inserting 1000 keys.
        let key_schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int32, false),
            Column::new("b", DataType::Int32, false),
        ]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferPoolManager::new(1000, disk_scheduler));
        // It's okay to have smaller page sizes for the test to force more splits and depth.
        let index = BPlusTreeIndex::new(key_schema.clone(), buffer_pool, 10, 10);

        // Insert 1000 keys.
        for i in 0..1000 {
            let key = Tuple::new(
                key_schema.clone(),
                // Both values are now i32
                vec![(i as i32).into(), (i as i32).into()],
            );
            // Use a consistent RID for easy verification.
            let rid = RecordId::new(i as u32, i as u32);
            index.insert(&key, rid).unwrap();
        }

        (index, key_schema)
    }

    #[test]
    pub fn test_concurrent_get() {
        let (index, key_schema) = build_large_index();
        let index = Arc::new(index);

        let mut handles = vec![];
        let num_threads = 10;
        let num_reads_per_thread = 1000;
        let num_total_keys = 1000;

        for _ in 0..num_threads {
            let index_clone = Arc::clone(&index);
            let key_schema_clone = Arc::clone(&key_schema);

            let handle = std::thread::spawn(move || {
                // Create a random number generator for each thread.
                let mut rng = rand::rng();
                for _ in 0..num_reads_per_thread {
                    // Generate a key within the valid range of inserted keys (0-999).
                    let key_val = rng.random_range(0..num_total_keys);

                    let key = Tuple::new(
                        key_schema_clone.clone(),
                        vec![(key_val as i32).into(), (key_val as i32).into()],
                    );
                    
                    let result = index_clone.get(&key).unwrap();

                    // The expected RID should match how we created it in `build_large_index`.
                    let expected_rid = RecordId::new(key_val as u32, key_val as u32);
                    
                    // This assertion should now pass.
                    assert_eq!(result, Some(expected_rid));
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_bplus_tree_concurrent_high_contention_cmu_style() {
        const NUM_THREADS: usize = 8;
        const KEYS_PER_THREAD: i32 = 500;
        const TOTAL_KEYS: i32 = NUM_THREADS as i32 * KEYS_PER_THREAD;

        // --- 1. 设置环境 ---
        let (index, key_schema, _temp_dir) = { // _temp_dir 的生命周期需要保持到测试结束
            let temp_dir = TempDir::new().unwrap();
            let temp_path = temp_dir.path().join("test_concurrent_cmu.db");
            let key_schema = Arc::new(Schema::new(vec![Column::new("id", DataType::Int32, false)]));
            let disk_manager = DiskManager::try_new(temp_path).unwrap();
            let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
            let buffer_pool = Arc::new(BufferPoolManager::new(1000, disk_scheduler));
            let index = Arc::new(BPlusTreeIndex::new(key_schema.clone(), buffer_pool, 10, 10));
            (index, key_schema, temp_dir)
        };
        
        // --- 2. 阶段一：并发插入 ---
        println!("\n--- Phase 1: Concurrent Insert ---");
        let mut handles = vec![];
        // 使用 Barrier 来确保所有线程尽可能同时开始，增加冲突概率
        let barrier = Arc::new(Barrier::new(NUM_THREADS));

        for i in 0..NUM_THREADS {
            let index_clone = Arc::clone(&index);
            let key_schema_clone = Arc::clone(&key_schema);
            let barrier_clone = Arc::clone(&barrier);

            let handle = thread::spawn(move || {
                // 每个线程负责插入一段连续的 key
                let start_key = i as i32 * KEYS_PER_THREAD;
                let end_key = start_key + KEYS_PER_THREAD;
                
                barrier_clone.wait(); // 等待所有线程就绪

                for key_val in start_key..end_key {
                    let key_tuple = Tuple::new(key_schema_clone.clone(), vec![key_val.into()]);
                    let rid = RecordId::new(key_val as u32, key_val as u32);
                    index_clone.insert(&key_tuple, rid).unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
        
        // --- 中间验证：检查所有 key 是否都已插入 ---
        for i in 0..TOTAL_KEYS {
            let key_tuple = Tuple::new(key_schema.clone(), vec![i.into()]);
            let expected_rid = RecordId::new(i as u32, i as u32);
            match index.get(&key_tuple) {
                Ok(Some(rid)) => assert_eq!(rid, expected_rid, "Key {} should exist after insert phase", i),
                _ => panic!("Key {} not found after insert phase", i),
            }
        }
        println!("--- Phase 1: All keys inserted and verified successfully. ---");

        // --- 3. 阶段二：并发删除 ---
        println!("\n--- Phase 2: Concurrent Delete ---");
        let mut handles = vec![];
        let barrier = Arc::new(Barrier::new(NUM_THREADS));
        
        // 将所有 key 随机打乱，让不同线程删除随机的 key，而不是连续的块
        let mut keys_to_delete: Vec<i32> = (0..TOTAL_KEYS).collect();
        let mut rng = rand::rng();
        keys_to_delete.shuffle(&mut rng);
        let keys_arc = Arc::new(keys_to_delete);

        for i in 0..NUM_THREADS {
            let index_clone = Arc::clone(&index);
            let key_schema_clone = Arc::clone(&key_schema);
            let barrier_clone = Arc::clone(&barrier);
            let keys_to_delete_clone = Arc::clone(&keys_arc);

            let handle = thread::spawn(move || {
                // 每个线程负责删除总 key 集合的一部分
                let start_index = i * (TOTAL_KEYS as usize / NUM_THREADS);
                let end_index = start_index + (TOTAL_KEYS as usize / NUM_THREADS);
                
                barrier_clone.wait(); // 等待所有线程就绪

                for key_idx in start_index..end_index {
                    let key_val = keys_to_delete_clone[key_idx];
                    let key_tuple = Tuple::new(key_schema_clone.clone(), vec![key_val.into()]);
                    index_clone.delete(&key_tuple).unwrap();
                }
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        println!("--- Phase 2: Concurrent delete phase finished. ---");

        // --- 4. 阶段三：最终状态验证 ---
        println!("\n--- Phase 3: Final Verification ---");
        let mut not_found_count = 0;
        for i in 0..TOTAL_KEYS {
            let key_tuple = Tuple::new(key_schema.clone(), vec![i.into()]);
            if let Ok(None) = index.get(&key_tuple) {
                // Key 确实被删除了，符合预期
                not_found_count += 1;
            } else {
                panic!("Key {} should have been deleted but was found!", i);
            }
        }
        
        assert_eq!(not_found_count, TOTAL_KEYS, "Not all keys were deleted correctly.");
        println!("--- Phase 3: Verification successful. All keys are deleted. ---");
        
        // 最终检查树是否为空（如果你的实现支持）
        // 这里我们假设 is_empty() 方法是线程安全的
        assert!(index.is_empty(), "The tree should be empty after deleting all keys.");
        println!("\nCMU-style high contention test passed!");
    }

}