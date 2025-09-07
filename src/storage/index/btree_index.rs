use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use crate::buffer::{AtomicPageId, INVALID_PAGE_ID, PageId, ReadPageGuard};
use crate::catalog::SchemaRef;
use crate::error::QuillSQLResult;
use crate::storage::index::Index;
use crate::utils::util::page_bytes_to_array;
use crate::storage::codec::{
    BPlusTreeInternalPageCodec, BPlusTreeLeafPageCodec, BPlusTreePageCodec,
};
use crate::storage::page::{InternalKV, LeafKV};
use crate::{
    buffer::BufferPoolManager,
    storage::page::{BPlusTreeInternalPage, BPlusTreeLeafPage, BPlusTreePage, RecordId},
    error::QuillSQLError,
};

use crate::storage::tuple::Tuple;

/// Context 用于在树的遍历过程中跟踪父节点的 page_id，
/// 以便在需要分裂或合并时可以向上回溯。
struct Context {
    pub path: VecDeque<PageId>,
}
impl Context {
    pub fn new() -> Self {
        Self {
            path: VecDeque::new(),
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
    pub root_page_id: AtomicU32,
    /// 用于写操作（如 insert/delete）的粗粒度锁，保证树结构的整体一致性。
    pub index_lock: RwLock<()>,
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
            index_lock: RwLock::new(()),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.root_page_id.load(Ordering::SeqCst) == INVALID_PAGE_ID
    }

    /// 公共 API: 查找一个键对应的 RecordId。
    /// 这个方法会获取一个读锁，允许多个并发的 get 操作。
    pub fn get(&self, key: &Tuple) -> QuillSQLResult<Option<RecordId>> {
        // 获取索引级别的读锁
        let _guard = self.index_lock.read().unwrap();
        if self.is_empty() {
            return Ok(None);
        }

        // 1. 找到叶子页面的 ID
        let leaf_page_id = self.find_leaf_page_id(key, &mut Context::new())?;

        // 2. 获取该页面的读保护器
        let leaf_guard = self.buffer_pool.fetch_page_read(leaf_page_id)?;

        // 3. 解码页面内容并查找 key
        let (leaf_page, _) =
            BPlusTreeLeafPageCodec::decode(&leaf_guard.data, self.key_schema.clone())?;
        
        // 当 leaf_guard 离开作用域时，读锁和 pin 会自动释放
        Ok(leaf_page.look_up(key))
    }

    /// 向 B+ 树中插入一个新的键值对。
    /// （简化版实现：处理了新树和叶子节点未满的情况）
    pub fn insert(&self, key: &Tuple, rid: RecordId) -> QuillSQLResult<()> {
        // 获取索引级别的写锁，防止其他写操作并发
        let _guard = self.index_lock.write().unwrap();
        
        // 如果树是空的，创建一个新的根节点（叶子节点）
        if self.is_empty() {
            return self.start_new_tree(key, rid);
        }
        
        // 省略了完整的分裂逻辑，仅为演示 Guard 的使用
        let mut context = Context::new();
        let leaf_page_id = self.find_leaf_page_id(key, &mut context)?;
        
        let mut leaf_guard = self.buffer_pool.fetch_page_write(leaf_page_id)?;
        let (mut leaf_page, _) = BPlusTreeLeafPageCodec::decode(&leaf_guard.data, self.key_schema.clone())?;
        
        leaf_page.insert(key.clone(), rid);
        
        // 如果页面满了，需要分裂，这里暂时省略
        if leaf_page.is_full() {
            // TODO: 实现分裂逻辑
            // 1. 创建新页面 (new_page)
            // 2. 移动一半数据到新页面
            // 3. 更新旧页面的 next_page_id
            // 4. 将新页面的第一个 key 和 page_id 插入到父节点
            //    这可能需要递归地向上分裂
            println!("WARN: Page split is not fully implemented.");
        }
        
        // 将修改后的页面内容编码并写回
        let encoded_data = BPlusTreeLeafPageCodec::encode(&leaf_page);
        leaf_guard.data.copy_from_slice(&encoded_data);
        
        Ok(())
    }

    pub fn delete(&self, _key: &Tuple) -> QuillSQLResult<()> {
        unimplemented!("Delete operation is not yet refactored");
    }

    /// 内部方法：当树为空时，创建第一个节点。
    fn start_new_tree(&self, key: &Tuple, rid: RecordId) -> QuillSQLResult<()> {
        let mut root_guard = self.buffer_pool.new_page()?;
        let root_page_id = root_guard.page_id();

        let mut leaf_page = BPlusTreeLeafPage::new(self.key_schema.clone(), self.leaf_max_size);
        leaf_page.insert(key.clone(), rid);

        let encoded_data = BPlusTreeLeafPageCodec::encode(&leaf_page);
        root_guard.data.copy_from_slice(&encoded_data);

        self.root_page_id.store(root_page_id, Ordering::SeqCst);
        Ok(())
    }

    fn find_leaf_page(
        &self,
        key: &Tuple,
        context: &mut Context,
    ) -> QuillSQLResult<ReadPageGuard> {
        let mut current_page_id = self.root_page_id.load(Ordering::SeqCst);
        loop {
            let current_guard = self.buffer_pool.fetch_page_read(current_page_id)?;
            let (page_data, _) =
                BPlusTreePageCodec::decode(&current_guard.data, self.key_schema.clone())?;

            match page_data {
                BPlusTreePage::Internal(internal) => {
                    context.path.push_back(current_page_id);
                    current_page_id = internal.look_up(key);
                }
                BPlusTreePage::Leaf(_) => return Ok(current_guard),
            }
        }
    }

    /// 内部方法：从根节点开始遍历，找到包含目标 key 的叶子节点的 page_id。
    /// 此方法只使用读操作，因此是安全的。
    fn find_leaf_page_id(&self, key: &Tuple, context: &mut Context) -> QuillSQLResult<PageId> {
        let mut current_page_id = self.root_page_id.load(Ordering::SeqCst);

        loop {
            // 获取当前页面的只读保护器
            let current_guard = self.buffer_pool.fetch_page_read(current_page_id)?;
            
            // 解码页面
            let (current_tree_page, _) =
                BPlusTreePageCodec::decode(&current_guard.data, self.key_schema.clone())?;
            
            // guard 在解码后就可以释放了，我们只需要 page_id
            drop(current_guard);

            match current_tree_page {
                BPlusTreePage::Internal(internal_page) => {
                    // 如果是内部节点，记录路径并继续向下查找
                    context.path.push_back(current_page_id);
                    current_page_id = internal_page.look_up(key);
                }
                BPlusTreePage::Leaf(_) => {
                    // 如果是叶子节点，我们找到了目标，返回它的 ID
                    return Ok(current_page_id);
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
        todo!("TreeIndexIterator::next is not yet implemented");
    }
}

// #[cfg(test)]
// mod tests {
//     use std::ops::Bound;
//     use std::sync::Arc;
//     use tempfile::TempDir;

//     use crate::catalog::SchemaRef;
//     use crate::storage::index::btree_index::TreeIndexIterator;
//     use crate::storage::disk_manager::DiskManager;
//     use crate::storage::disk_scheduler::DiskScheduler;
//     use crate::storage::page::RecordId;
//     use crate::storage::tuple::Tuple;
//     use crate::utils::util::pretty_format_index_tree;
//     use crate::{
//         buffer::BufferPoolManager,
//         catalog::{Column, DataType, Schema},
//     };

//     use super::BPlusTreeIndex;

//     fn build_index() -> (BPlusTreeIndex, SchemaRef) {
//         let temp_dir = TempDir::new().unwrap();
//         let temp_path = temp_dir.path().join("test.db");

//         let key_schema = Arc::new(Schema::new(vec![
//             Column::new("a", DataType::Int8, false),
//             Column::new("b", DataType::Int16, false),
//         ]));
//         let disk_manager = DiskManager::try_new(temp_path).unwrap();
//         let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
//         let buffer_pool = Arc::new(BufferPoolManager::new(1000, disk_scheduler));
//         let index = BPlusTreeIndex::new(key_schema.clone(), buffer_pool, 4, 4);

//         index
//             .insert(
//                 &Tuple::new(key_schema.clone(), vec![1i8.into(), 1i16.into()]),
//                 RecordId::new(1, 1),
//             )
//             .unwrap();
//         index
//             .insert(
//                 &Tuple::new(key_schema.clone(), vec![2i8.into(), 2i16.into()]),
//                 RecordId::new(2, 2),
//             )
//             .unwrap();
//         index
//             .insert(
//                 &Tuple::new(key_schema.clone(), vec![3i8.into(), 3i16.into()]),
//                 RecordId::new(3, 3),
//             )
//             .unwrap();
//         index
//             .insert(
//                 &Tuple::new(key_schema.clone(), vec![4i8.into(), 4i16.into()]),
//                 RecordId::new(4, 4),
//             )
//             .unwrap();
//         index
//             .insert(
//                 &Tuple::new(key_schema.clone(), vec![5i8.into(), 5i16.into()]),
//                 RecordId::new(5, 5),
//             )
//             .unwrap();
//         index
//             .insert(
//                 &Tuple::new(key_schema.clone(), vec![6i8.into(), 6i16.into()]),
//                 RecordId::new(6, 6),
//             )
//             .unwrap();
//         index
//             .insert(
//                 &Tuple::new(key_schema.clone(), vec![7i8.into(), 7i16.into()]),
//                 RecordId::new(7, 7),
//             )
//             .unwrap();
//         index
//             .insert(
//                 &Tuple::new(key_schema.clone(), vec![8i8.into(), 8i16.into()]),
//                 RecordId::new(8, 8),
//             )
//             .unwrap();
//         index
//             .insert(
//                 &Tuple::new(key_schema.clone(), vec![9i8.into(), 9i16.into()]),
//                 RecordId::new(9, 9),
//             )
//             .unwrap();
//         index
//             .insert(
//                 &Tuple::new(key_schema.clone(), vec![10i8.into(), 10i16.into()]),
//                 RecordId::new(10, 10),
//             )
//             .unwrap();
//         index
//             .insert(
//                 &Tuple::new(key_schema.clone(), vec![11i8.into(), 11i16.into()]),
//                 RecordId::new(11, 11),
//             )
//             .unwrap();
//         (index, key_schema)
//     }

//     #[test]
//     pub fn test_index_insert() {
//         let (index, _) = build_index();
//         let display = pretty_format_index_tree(&index).unwrap();
//         println!("{display}");
//         assert_eq!(display, "B+ Tree Level No.1:
// +-----------------------+
// | page_id=13, size: 2/4 |
// +-----------------------+
// | +------------+------+ |
// | | NULL, NULL | 5, 5 | |
// | +------------+------+ |
// | | 8          | 12   | |
// | +------------+------+ |
// +-----------------------+
// B+ Tree Level No.2:
// +-----------------------+------------------------+
// | page_id=8, size: 2/4  | page_id=12, size: 3/4  |
// +-----------------------+------------------------+
// | +------------+------+ | +------+------+------+ |
// | | NULL, NULL | 3, 3 | | | 5, 5 | 7, 7 | 9, 9 | |
// | +------------+------+ | +------+------+------+ |
// | | 6          | 7    | | | 9    | 10   | 11   | |
// | +------------+------+ | +------+------+------+ |
// +-----------------------+------------------------+
// B+ Tree Level No.3:
// +--------------------------------------+--------------------------------------+---------------------------------------+----------------------------------------+---------------------------------------+
// | page_id=6, size: 2/4, next_page_id=7 | page_id=7, size: 2/4, next_page_id=9 | page_id=9, size: 2/4, next_page_id=10 | page_id=10, size: 2/4, next_page_id=11 | page_id=11, size: 3/4, next_page_id=0 |
// +--------------------------------------+--------------------------------------+---------------------------------------+----------------------------------------+---------------------------------------+
// | +------+------+                      | +------+------+                      | +------+------+                       | +------+------+                        | +------+--------+--------+            |
// | | 1, 1 | 2, 2 |                      | | 3, 3 | 4, 4 |                      | | 5, 5 | 6, 6 |                       | | 7, 7 | 8, 8 |                        | | 9, 9 | 10, 10 | 11, 11 |            |
// | +------+------+                      | +------+------+                      | +------+------+                       | +------+------+                        | +------+--------+--------+            |
// | | 1-1  | 2-2  |                      | | 3-3  | 4-4  |                      | | 5-5  | 6-6  |                       | | 7-7  | 8-8  |                        | | 9-9  | 10-10  | 11-11  |            |
// | +------+------+                      | +------+------+                      | +------+------+                       | +------+------+                        | +------+--------+--------+            |
// +--------------------------------------+--------------------------------------+---------------------------------------+----------------------------------------+---------------------------------------+
// ");
//     }

//     #[test]
//     pub fn test_index_delete() {
//         let (index, key_schema) = build_index();

//         index
//             .delete(&Tuple::new(
//                 key_schema.clone(),
//                 vec![3i8.into(), 3i16.into()],
//             ))
//             .unwrap();
//         println!("{}", pretty_format_index_tree(&index).unwrap());
//         index
//             .delete(&Tuple::new(
//                 key_schema.clone(),
//                 vec![10i8.into(), 10i16.into()],
//             ))
//             .unwrap();
//         println!("{}", pretty_format_index_tree(&index).unwrap());
//         index
//             .delete(&Tuple::new(
//                 key_schema.clone(),
//                 vec![8i8.into(), 8i16.into()],
//             ))
//             .unwrap();
//         println!("{}", pretty_format_index_tree(&index).unwrap());

//         assert_eq!(pretty_format_index_tree(&index).unwrap(),
//                    "B+ Tree Level No.1:
// +------------------------------+
// | page_id=8, size: 3/4         |
// +------------------------------+
// | +------------+------+------+ |
// | | NULL, NULL | 5, 5 | 7, 7 | |
// | +------------+------+------+ |
// | | 6          | 9    | 10   | |
// | +------------+------+------+ |
// +------------------------------+
// B+ Tree Level No.2:
// +--------------------------------------+---------------------------------------+---------------------------------------+
// | page_id=6, size: 3/4, next_page_id=9 | page_id=9, size: 2/4, next_page_id=10 | page_id=10, size: 3/4, next_page_id=0 |
// +--------------------------------------+---------------------------------------+---------------------------------------+
// | +------+------+------+               | +------+------+                       | +------+------+--------+              |
// | | 1, 1 | 2, 2 | 4, 4 |               | | 5, 5 | 6, 6 |                       | | 7, 7 | 9, 9 | 11, 11 |              |
// | +------+------+------+               | +------+------+                       | +------+------+--------+              |
// | | 1-1  | 2-2  | 4-4  |               | | 5-5  | 6-6  |                       | | 7-7  | 9-9  | 11-11  |              |
// | +------+------+------+               | +------+------+                       | +------+------+--------+              |
// +--------------------------------------+---------------------------------------+---------------------------------------+
// ");
//     }

//     #[test]
//     pub fn test_index_get() {
//         let (index, key_schema) = build_index();
//         assert_eq!(
//             index
//                 .get(&Tuple::new(
//                     key_schema.clone(),
//                     vec![3i8.into(), 3i16.into()],
//                 ))
//                 .unwrap(),
//             Some(RecordId::new(3, 3))
//         );
//         assert_eq!(
//             index
//                 .get(&Tuple::new(
//                     key_schema.clone(),
//                     vec![10i8.into(), 10i16.into()],
//                 ))
//                 .unwrap(),
//             Some(RecordId::new(10, 10))
//         );
//     }

    // #[test]
    // pub fn test_index_iterator() {
    //     let (index, key_schema) = build_index();
    //     let index = Arc::new(index);

    //     let end_tuple1 = Tuple::new(key_schema.clone(), vec![3i8.into(), 3i16.into()]);
    //     let mut iterator1 = TreeIndexIterator::new(index.clone(), ..end_tuple1);
    //     assert_eq!(iterator1.next().unwrap(), Some(RecordId::new(1, 1)));
    //     assert_eq!(iterator1.next().unwrap(), Some(RecordId::new(2, 2)));
    //     assert_eq!(iterator1.next().unwrap(), None);

    //     let start_tuple2 = Tuple::new(key_schema.clone(), vec![3i8.into(), 3i16.into()]);
    //     let end_tuple2 = Tuple::new(key_schema.clone(), vec![5i8.into(), 5i16.into()]);
    //     let mut iterator2 = TreeIndexIterator::new(index.clone(), start_tuple2..=end_tuple2);
    //     assert_eq!(iterator2.next().unwrap(), Some(RecordId::new(3, 3)));
    //     assert_eq!(iterator2.next().unwrap(), Some(RecordId::new(4, 4)));
    //     assert_eq!(iterator2.next().unwrap(), Some(RecordId::new(5, 5)));
    //     assert_eq!(iterator2.next().unwrap(), None);

    //     let start_tuple3 = Tuple::new(key_schema.clone(), vec![6i8.into(), 6i16.into()]);
    //     let end_tuple3 = Tuple::new(key_schema.clone(), vec![8i8.into(), 8i16.into()]);
    //     let mut iterator3 = TreeIndexIterator::new(
    //         index.clone(),
    //         (Bound::Excluded(start_tuple3), Bound::Excluded(end_tuple3)),
    //     );
    //     assert_eq!(iterator3.next().unwrap(), Some(RecordId::new(7, 7)));

    //     let start_tuple4 = Tuple::new(key_schema.clone(), vec![9i8.into(), 9i16.into()]);
    //     let mut iterator4 = TreeIndexIterator::new(index.clone(), start_tuple4..);
    //     assert_eq!(iterator4.next().unwrap(), Some(RecordId::new(9, 9)));
    //     assert_eq!(iterator4.next().unwrap(), Some(RecordId::new(10, 10)));
    //     assert_eq!(iterator4.next().unwrap(), Some(RecordId::new(11, 11)));
    //     assert_eq!(iterator4.next().unwrap(), None);
    //     assert_eq!(iterator4.next().unwrap(), None);
    // }
// }
