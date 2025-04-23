use crate::buffer::{AtomicPageId, INVALID_PAGE_ID};
use crate::catalog::SchemaRef;
use crate::utils::util::page_bytes_to_array;
use crate::storage::codec::TablePageCodec;
use crate::storage::b_plus_tree::page::{RecordId, TablePage, TupleMeta, INVALID_RID};
use crate::{buffer::BufferPoolManager, error::QuillSQLResult};
use std::collections::Bound;
use std::ops::RangeBounds;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::storage::tuple::Tuple;

#[derive(Debug)]
pub struct TableHeap {
    pub schema: SchemaRef,
    pub buffer_pool: Arc<BufferPoolManager>,
    pub first_page_id: AtomicPageId,
    pub last_page_id: AtomicPageId,
}

impl TableHeap {
    pub fn try_new(schema: SchemaRef, buffer_pool: Arc<BufferPoolManager>) -> QuillSQLResult<Self> {
        // new a page and initialize
        let first_page = buffer_pool.new_page()?;
        let first_page_id = first_page.read().unwrap().page_id;
        let table_page = TablePage::new(schema.clone(), INVALID_PAGE_ID);
        first_page
            .write()
            .unwrap()
            .set_data(page_bytes_to_array(&TablePageCodec::encode(&table_page)));

        Ok(Self {
            schema,
            buffer_pool,
            first_page_id: AtomicPageId::new(first_page_id),
            last_page_id: AtomicPageId::new(first_page_id),
        })
    }

    /// Inserts a tuple into the table.
    ///
    /// This function inserts the given tuple into the table. If the last page in the table
    /// has enough space for the tuple, it is inserted there. Otherwise, a new page is allocated
    /// and the tuple is inserted there.
    ///
    /// Parameters:
    /// - `meta`: The metadata associated with the tuple.
    /// - `tuple`: The tuple to be inserted.
    ///
    /// Returns:
    /// An `Option` containing the `Rid` of the inserted tuple if successful, otherwise `None`.
    pub fn insert_tuple(&self, meta: &TupleMeta, tuple: &Tuple) -> QuillSQLResult<RecordId> {
        let mut last_page_id = self.last_page_id.load(Ordering::SeqCst);
        let (last_page, mut last_table_page) = self
            .buffer_pool
            .fetch_table_page(last_page_id, self.schema.clone())?;

        // Loop until a suitable page is found for inserting the tuple
        loop {
            if last_table_page.next_tuple_offset(tuple).is_ok() {
                break;
            }

            // if there's no tuple in the page, and we can't insert the tuple,
            // then this tuple is too large.
            assert!(
                last_table_page.header.num_tuples > 0,
                "tuple is too large, cannot insert"
            );

            // Allocate a new page if no more table pages are available.
            let next_page = self.buffer_pool.new_page()?;
            let next_page_id = next_page.read().unwrap().page_id;
            let next_table_page = TablePage::new(self.schema.clone(), INVALID_PAGE_ID);
            next_page
                .write()
                .unwrap()
                .set_data(page_bytes_to_array(&TablePageCodec::encode(
                    &next_table_page,
                )));

            // Update and release the previous page
            last_table_page.header.next_page_id = next_page_id;
            last_page
                .write()
                .unwrap()
                .set_data(page_bytes_to_array(&TablePageCodec::encode(
                    &last_table_page,
                )));

            // Update last_page_id.
            last_page_id = next_page_id;
            last_table_page = next_table_page;
            self.last_page_id.store(last_page_id, Ordering::SeqCst);
        }

        // Insert the tuple into the chosen page
        let slot_id = last_table_page.insert_tuple(meta, tuple)?;

        last_page
            .write()
            .unwrap()
            .set_data(page_bytes_to_array(&TablePageCodec::encode(
                &last_table_page,
            )));

        // Map the slot_id to a Rid and return
        Ok(RecordId::new(last_page_id, slot_id as u32))
    }

    pub fn update_tuple(&self, rid: RecordId, tuple: Tuple) -> QuillSQLResult<()> {
        let (page, mut table_page) = self
            .buffer_pool
            .fetch_table_page(rid.page_id, self.schema.clone())?;
        table_page.update_tuple(tuple, rid.slot_num as u16)?;

        page.write()
            .unwrap()
            .set_data(page_bytes_to_array(&TablePageCodec::encode(&table_page)));
        Ok(())
    }

    pub fn update_tuple_meta(&self, meta: TupleMeta, rid: RecordId) -> QuillSQLResult<()> {
        let (page, mut table_page) = self
            .buffer_pool
            .fetch_table_page(rid.page_id, self.schema.clone())?;
        table_page.update_tuple_meta(meta, rid.slot_num as u16)?;

        page.write()
            .unwrap()
            .set_data(page_bytes_to_array(&TablePageCodec::encode(&table_page)));
        Ok(())
    }

    pub fn full_tuple(&self, rid: RecordId) -> QuillSQLResult<(TupleMeta, Tuple)> {
        let (_, table_page) = self
            .buffer_pool
            .fetch_table_page(rid.page_id, self.schema.clone())?;
        let result = table_page.tuple(rid.slot_num as u16)?;
        Ok(result)
    }

    pub fn tuple(&self, rid: RecordId) -> QuillSQLResult<Tuple> {
        let (_meta, tuple) = self.full_tuple(rid)?;
        Ok(tuple)
    }

    pub fn tuple_meta(&self, rid: RecordId) -> QuillSQLResult<TupleMeta> {
        let (meta, _tuple) = self.full_tuple(rid)?;
        Ok(meta)
    }

    pub fn get_first_rid(&self) -> QuillSQLResult<Option<RecordId>> {
        let first_page_id = self.first_page_id.load(Ordering::SeqCst);
        let (_, table_page) = self
            .buffer_pool
            .fetch_table_page(first_page_id, self.schema.clone())?;
        
        // Find first non-deleted tuple
        for slot_num in 0..table_page.header.num_tuples {
            if !table_page.header.tuple_infos[slot_num as usize].meta.is_deleted {
                return Ok(Some(RecordId::new(first_page_id, slot_num as u32)));
            }
        }
        
        Ok(None)
    }

    pub fn get_next_rid(&self, rid: RecordId) -> QuillSQLResult<Option<RecordId>> {
        let (_, table_page) = self
            .buffer_pool
            .fetch_table_page(rid.page_id, self.schema.clone())?;
        let next_rid = table_page.get_next_rid(&rid);
        if next_rid.is_some() {
            return Ok(next_rid);
        }

        if table_page.header.next_page_id == INVALID_PAGE_ID {
            return Ok(None);
        }
        let (_, next_table_page) = self
            .buffer_pool
            .fetch_table_page(table_page.header.next_page_id, self.schema.clone())?;
        
        // Find first non-deleted tuple in next page
        for slot_num in 0..next_table_page.header.num_tuples {
            if !next_table_page.header.tuple_infos[slot_num as usize].meta.is_deleted {
                return Ok(Some(RecordId::new(table_page.header.next_page_id, slot_num as u32)));
            }
        }
        Ok(None)
    }
}

#[derive(Debug)]
pub struct TableIterator {
    heap: Arc<TableHeap>,
    start_bound: Bound<RecordId>,
    end_bound: Bound<RecordId>,
    cursor: RecordId,
    started: bool,
    ended: bool,
}

impl TableIterator {
    pub fn new<R: RangeBounds<RecordId>>(heap: Arc<TableHeap>, range: R) -> Self {
        Self {
            heap,
            start_bound: range.start_bound().cloned(),
            end_bound: range.end_bound().cloned(),
            cursor: INVALID_RID,
            started: false,
            ended: false,
        }
    }

    pub fn next(&mut self) -> QuillSQLResult<Option<(RecordId, Tuple)>> {
        if self.ended {
            return Ok(None);
        }

        if self.started {
            match self.end_bound {
                Bound::Included(rid) => {
                    if let Some(next_rid) = self.heap.get_next_rid(self.cursor)? {
                        if next_rid == rid {
                            self.ended = true;
                        }
                        self.cursor = next_rid;
                        Ok(self
                            .heap
                            .tuple(self.cursor)
                            .ok()
                            .map(|tuple| (self.cursor, tuple)))
                    } else {
                        Ok(None)
                    }
                }
                Bound::Excluded(rid) => {
                    if let Some(next_rid) = self.heap.get_next_rid(self.cursor)? {
                        if next_rid == rid {
                            Ok(None)
                        } else {
                            self.cursor = next_rid;
                            Ok(self
                                .heap
                                .tuple(self.cursor)
                                .ok()
                                .map(|tuple| (self.cursor, tuple)))
                        }
                    } else {
                        Ok(None)
                    }
                }
                Bound::Unbounded => {
                    if let Some(next_rid) = self.heap.get_next_rid(self.cursor)? {
                        self.cursor = next_rid;
                        Ok(self
                            .heap
                            .tuple(self.cursor)
                            .ok()
                            .map(|tuple| (self.cursor, tuple)))
                    } else {
                        Ok(None)
                    }
                }
            }
        } else {
            self.started = true;
            match self.start_bound {
                Bound::Included(rid) => {
                    self.cursor = rid;
                    Ok(self
                        .heap
                        .tuple(self.cursor)
                        .ok()
                        .map(|tuple| (self.cursor, tuple)))
                }
                Bound::Excluded(rid) => {
                    if let Some(next_rid) = self.heap.get_next_rid(rid)? {
                        self.cursor = next_rid;
                        Ok(self
                            .heap
                            .tuple(self.cursor)
                            .ok()
                            .map(|tuple| (self.cursor, tuple)))
                    } else {
                        self.ended = true;
                        Ok(None)
                    }
                }
                Bound::Unbounded => {
                    if let Some(first_rid) = self.heap.get_first_rid()? {
                        self.cursor = first_rid;
                        Ok(self
                            .heap
                            .tuple(self.cursor)
                            .ok()
                            .map(|tuple| (self.cursor, tuple)))
                    } else {
                        self.ended = true;
                        Ok(None)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::catalog::{Column, DataType, Schema};
    use crate::storage::b_plus_tree::disk::disk_manager::DiskManager;
    use crate::storage::b_plus_tree::disk::disk_scheduler::DiskScheduler;
    use crate::storage::b_plus_tree::page::EMPTY_TUPLE_META;
    use crate::storage::b_plus_tree::table_heap::TableIterator;
    use crate::storage::{b_plus_tree::table_heap::TableHeap, tuple::Tuple};
    use crate::buffer::BufferPoolManager;

    #[test]
    pub fn test_table_heap_update_tuple_meta() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferPoolManager::new(1000, disk_scheduler));
        let table_heap = TableHeap::try_new(schema.clone(), buffer_pool).unwrap();

        let _rid1 = table_heap
            .insert_tuple(
                &EMPTY_TUPLE_META,
                &Tuple::new(schema.clone(), vec![1i8.into(), 1i16.into()]),
            )
            .unwrap();
        let rid2 = table_heap
            .insert_tuple(
                &EMPTY_TUPLE_META,
                &Tuple::new(schema.clone(), vec![2i8.into(), 2i16.into()]),
            )
            .unwrap();
        let _rid3 = table_heap
            .insert_tuple(
                &EMPTY_TUPLE_META,
                &Tuple::new(schema.clone(), vec![3i8.into(), 3i16.into()]),
            )
            .unwrap();

        let mut meta = table_heap.tuple_meta(rid2).unwrap();
        meta.insert_txn_id = 1;
        meta.delete_txn_id = 2;
        meta.is_deleted = true;
        table_heap.update_tuple_meta(meta, rid2).unwrap();

        let meta = table_heap.tuple_meta(rid2).unwrap();
        assert_eq!(meta.insert_txn_id, 1);
        assert_eq!(meta.delete_txn_id, 2);
        assert!(meta.is_deleted);
    }

    #[test]
    pub fn test_table_heap_insert_tuple() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferPoolManager::new(1000, disk_scheduler));
        let table_heap = TableHeap::try_new(schema.clone(), buffer_pool).unwrap();

        let meta1 = super::TupleMeta {
            insert_txn_id: 1,
            delete_txn_id: 1,
            is_deleted: false,
        };
        let rid1 = table_heap
            .insert_tuple(
                &meta1,
                &Tuple::new(schema.clone(), vec![1i8.into(), 1i16.into()]),
            )
            .unwrap();
        let meta2 = super::TupleMeta {
            insert_txn_id: 2,
            delete_txn_id: 2,
            is_deleted: false,
        };
        let rid2 = table_heap
            .insert_tuple(
                &meta2,
                &Tuple::new(schema.clone(), vec![2i8.into(), 2i16.into()]),
            )
            .unwrap();
        let meta3 = super::TupleMeta {
            insert_txn_id: 3,
            delete_txn_id: 3,
            is_deleted: false,
        };
        let rid3 = table_heap
            .insert_tuple(
                &meta3,
                &Tuple::new(schema.clone(), vec![3i8.into(), 3i16.into()]),
            )
            .unwrap();

        let (meta, tuple) = table_heap.full_tuple(rid1).unwrap();
        assert_eq!(meta, meta1);
        assert_eq!(tuple.data, vec![1i8.into(), 1i16.into()]);

        let (meta, tuple) = table_heap.full_tuple(rid2).unwrap();
        assert_eq!(meta, meta2);
        assert_eq!(tuple.data, vec![2i8.into(), 2i16.into()]);

        let (meta, tuple) = table_heap.full_tuple(rid3).unwrap();
        assert_eq!(meta, meta3);
        assert_eq!(tuple.data, vec![3i8.into(), 3i16.into()]);
    }

    #[test]
    pub fn test_table_heap_iterator() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));

        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferPoolManager::new(1000, disk_scheduler));
        let table_heap = Arc::new(TableHeap::try_new(schema.clone(), buffer_pool).unwrap());

        let meta1 = super::TupleMeta {
            insert_txn_id: 1,
            delete_txn_id: 1,
            is_deleted: false,
        };
        let rid1 = table_heap
            .insert_tuple(
                &meta1,
                &Tuple::new(schema.clone(), vec![1i8.into(), 1i16.into()]),
            )
            .unwrap();
        let meta2 = super::TupleMeta {
            insert_txn_id: 2,
            delete_txn_id: 2,
            is_deleted: false,
        };
        let rid2 = table_heap
            .insert_tuple(
                &meta2,
                &Tuple::new(schema.clone(), vec![2i8.into(), 2i16.into()]),
            )
            .unwrap();
        let meta3 = super::TupleMeta {
            insert_txn_id: 3,
            delete_txn_id: 3,
            is_deleted: false,
        };
        let rid3 = table_heap
            .insert_tuple(
                &meta3,
                &Tuple::new(schema.clone(), vec![3i8.into(), 3i16.into()]),
            )
            .unwrap();

        let mut iterator = TableIterator::new(table_heap.clone(), ..);

        let (rid, tuple) = iterator.next().unwrap().unwrap();
        assert_eq!(rid, rid1);
        assert_eq!(tuple.data, vec![1i8.into(), 1i16.into()]);

        let (rid, tuple) = iterator.next().unwrap().unwrap();
        assert_eq!(rid, rid2);
        assert_eq!(tuple.data, vec![2i8.into(), 2i16.into()]);

        let (rid, tuple) = iterator.next().unwrap().unwrap();
        assert_eq!(rid, rid3);
        assert_eq!(tuple.data, vec![3i8.into(), 3i16.into()]);

        assert!(iterator.next().unwrap().is_none());
    }
}
