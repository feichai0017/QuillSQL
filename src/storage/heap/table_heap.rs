use crate::buffer::{AtomicPageId, WritePageGuard, INVALID_PAGE_ID};
use crate::catalog::SchemaRef;
use crate::storage::codec::TablePageCodec;
use crate::storage::page::{RecordId, TablePage, TupleMeta, INVALID_RID};
use crate::{buffer::BufferManager, error::QuillSQLResult};
use std::collections::Bound;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::storage::heap::wal_codec::RelationIdent;
use crate::storage::tuple::Tuple;

#[derive(Debug)]
pub struct TableHeap {
    pub schema: SchemaRef,
    pub buffer_pool: Arc<BufferManager>,
    pub first_page_id: AtomicPageId,
    pub last_page_id: AtomicPageId,
}

impl TableHeap {
    /// Creates a new table heap. This involves allocating an initial page.
    pub fn try_new(schema: SchemaRef, buffer_pool: Arc<BufferManager>) -> QuillSQLResult<Self> {
        // new_page() returns a WritePageGuard.
        let mut first_page_guard = buffer_pool.new_page()?;
        let first_page_id = first_page_guard.page_id();

        // Initialize the first page as an empty TablePage.
        let table_page = TablePage::new(schema.clone(), INVALID_PAGE_ID);
        let encoded_data = TablePageCodec::encode(&table_page);

        // Use DerefMut to get a mutable reference and update the page data.
        // This also marks the page as dirty automatically.
        first_page_guard.data_mut().copy_from_slice(&encoded_data);
        first_page_guard.set_lsn(table_page.lsn());

        // The first_page_guard is dropped here, automatically unpinning the page.
        Ok(Self {
            schema,
            buffer_pool,
            first_page_id: AtomicU32::new(first_page_id),
            last_page_id: AtomicU32::new(first_page_id),
        })
    }

    fn write_back_page(
        &self,
        guard: &mut WritePageGuard,
        table_page: &mut TablePage,
    ) -> QuillSQLResult<()> {
        let new_image = TablePageCodec::encode(table_page);
        guard.apply_page_image(&new_image)?;
        Ok(())
    }

    pub(crate) fn relation_ident(&self) -> RelationIdent {
        RelationIdent {
            root_page_id: self.first_page_id.load(Ordering::SeqCst),
        }
    }

    /// Inserts `tuple` with MVCC metadata `meta`, allocating a new page if the
    /// current tail page runs out of capacity.
    pub fn insert_tuple(&self, meta: &TupleMeta, tuple: &Tuple) -> QuillSQLResult<RecordId> {
        let mut current_page_id = self.last_page_id.load(Ordering::SeqCst);

        loop {
            let mut current_page_guard = self.buffer_pool.fetch_page_write(current_page_id)?;
            let mut table_page =
                TablePageCodec::decode(current_page_guard.data(), self.schema.clone())?.0;
            table_page.set_lsn(current_page_guard.lsn());

            if table_page.next_tuple_offset(tuple).is_ok() {
                let slot_id = table_page.insert_tuple(meta, tuple)?;
                self.write_back_page(&mut current_page_guard, &mut table_page)?;
                return Ok(RecordId::new(current_page_id, slot_id as u32));
            }

            let mut new_page_guard = self.buffer_pool.new_page()?;
            let new_page_id = new_page_guard.page_id();
            let mut new_table_page = TablePage::new(self.schema.clone(), INVALID_PAGE_ID);
            self.write_back_page(&mut new_page_guard, &mut new_table_page)?;

            table_page.header.next_page_id = new_page_id;
            self.write_back_page(&mut current_page_guard, &mut table_page)?;
            drop(current_page_guard);

            self.last_page_id.store(new_page_id, Ordering::SeqCst);
            current_page_id = new_page_id;
        }
    }

    pub fn full_tuple(&self, rid: RecordId) -> QuillSQLResult<(TupleMeta, Tuple)> {
        let (_, table_page) = self
            .buffer_pool
            .fetch_table_page(rid.page_id, self.schema.clone())?;
        let result = table_page.tuple(rid.slot_num as u16)?;
        Ok(result)
    }

    /// Overwrite the tuple metadata at `rid` without emitting WAL.
    pub fn write_tuple_meta(&self, rid: RecordId, meta: TupleMeta) -> QuillSQLResult<()> {
        let mut page_guard = self.buffer_pool.fetch_page_write(rid.page_id)?;
        let mut table_page = TablePageCodec::decode(page_guard.data(), self.schema.clone())?.0;
        table_page.set_lsn(page_guard.lsn());

        let slot = rid.slot_num as u16;
        table_page.update_tuple_meta(meta, slot)?;
        self.write_back_page(&mut page_guard, &mut table_page)
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

        if table_page.header.num_tuples == 0 {
            Ok(None)
        } else {
            Ok(Some(RecordId::new(first_page_id, 0)))
        }
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

        if next_table_page.header.num_tuples == 0 {
            return Ok(None);
        }
        Ok(Some(RecordId::new(table_page.header.next_page_id, 0)))
    }

    /// Attempt to reclaim the tuple at `rid` if `predicate` returns true for the current metadata.
    /// Returns true when the tuple was removed.
    pub fn vacuum_slot_if<F>(&self, rid: RecordId, predicate: F) -> QuillSQLResult<bool>
    where
        F: FnOnce(&TupleMeta) -> bool,
    {
        let mut page_guard = self.buffer_pool.fetch_page_write(rid.page_id)?;
        let mut table_page = TablePageCodec::decode(page_guard.data(), self.schema.clone())?.0;
        table_page.set_lsn(page_guard.lsn());

        let slot = rid.slot_num as u16;
        if slot >= table_page.header.num_tuples {
            return Ok(false);
        }
        let meta = table_page.header.tuple_infos[slot as usize].meta;
        if !predicate(&meta) {
            return Ok(false);
        }

        table_page.reclaim_tuple(slot)?;
        self.write_back_page(&mut page_guard, &mut table_page)?;
        Ok(true)
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
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        Self {
            heap,
            start_bound: start,
            end_bound: end,
            cursor: INVALID_RID,
            started: false,
            ended: false,
        }
    }

    pub fn next(&mut self) -> QuillSQLResult<Option<(RecordId, TupleMeta, Tuple)>> {
        if self.ended {
            return Ok(None);
        }

        if self.started {
            match self.end_bound {
                Bound::Included(rid) => {
                    if let Some(next_rid) = self.heap.get_next_rid(self.cursor)? {
                        self.cursor = next_rid;
                        if self.cursor == rid {
                            self.ended = true;
                        }
                        let (meta, tuple) = self.heap.full_tuple(self.cursor)?;
                        Ok(Some((self.cursor, meta, tuple)))
                    } else {
                        Ok(None)
                    }
                }
                Bound::Excluded(rid) => {
                    if let Some(next_rid) = self.heap.get_next_rid(self.cursor)? {
                        if next_rid == rid {
                            self.ended = true;
                            Ok(None)
                        } else {
                            self.cursor = next_rid;
                            let (meta, tuple) = self.heap.full_tuple(self.cursor)?;
                            Ok(Some((self.cursor, meta, tuple)))
                        }
                    } else {
                        Ok(None)
                    }
                }
                Bound::Unbounded => {
                    if let Some(next_rid) = self.heap.get_next_rid(self.cursor)? {
                        self.cursor = next_rid;
                        let (meta, tuple) = self.heap.full_tuple(self.cursor)?;
                        Ok(Some((self.cursor, meta, tuple)))
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
                    let (meta, tuple) = self.heap.full_tuple(self.cursor)?;
                    Ok(Some((self.cursor, meta, tuple)))
                }
                Bound::Excluded(rid) => {
                    if let Some(next_rid) = self.heap.get_next_rid(rid)? {
                        self.cursor = next_rid;
                        let (meta, tuple) = self.heap.full_tuple(self.cursor)?;
                        Ok(Some((self.cursor, meta, tuple)))
                    } else {
                        self.ended = true;
                        Ok(None)
                    }
                }
                Bound::Unbounded => {
                    if let Some(first_rid) = self.heap.get_first_rid()? {
                        self.cursor = first_rid;
                        let (meta, tuple) = self.heap.full_tuple(self.cursor)?;
                        Ok(Some((self.cursor, meta, tuple)))
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

    use crate::buffer::BufferManager;
    use crate::catalog::{Column, DataType, Schema};
    use crate::storage::codec::TupleCodec;
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;
    use crate::storage::page::EMPTY_TUPLE_META;
    use crate::storage::table_heap::TableIterator;
    use crate::storage::{table_heap::TableHeap, tuple::Tuple};
    use crate::utils::scalar::ScalarValue;

    #[test]
    pub fn test_table_heap_write_tuple_meta() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferManager::new(1000, disk_scheduler));
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
        table_heap.write_tuple_meta(rid2, meta).unwrap();

        let meta = table_heap.tuple_meta(rid2).unwrap();
        assert_eq!(meta.insert_txn_id, 1);
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
        let buffer_pool = Arc::new(BufferManager::new(1000, disk_scheduler));
        let table_heap = TableHeap::try_new(schema.clone(), buffer_pool).unwrap();

        let meta1 = super::TupleMeta::new(1, 0);
        let rid1 = table_heap
            .insert_tuple(
                &meta1,
                &Tuple::new(schema.clone(), vec![1i8.into(), 1i16.into()]),
            )
            .unwrap();
        let meta2 = super::TupleMeta::new(2, 0);
        let rid2 = table_heap
            .insert_tuple(
                &meta2,
                &Tuple::new(schema.clone(), vec![2i8.into(), 2i16.into()]),
            )
            .unwrap();
        let meta3 = super::TupleMeta::new(3, 0);
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
        let buffer_pool = Arc::new(BufferManager::new(1000, disk_scheduler));
        let table_heap = Arc::new(TableHeap::try_new(schema.clone(), buffer_pool).unwrap());

        let meta1 = super::TupleMeta::new(1, 0);
        let rid1 = table_heap
            .insert_tuple(
                &meta1,
                &Tuple::new(schema.clone(), vec![1i8.into(), 1i16.into()]),
            )
            .unwrap();
        let meta2 = super::TupleMeta::new(2, 0);
        let rid2 = table_heap
            .insert_tuple(
                &meta2,
                &Tuple::new(schema.clone(), vec![2i8.into(), 2i16.into()]),
            )
            .unwrap();
        let meta3 = super::TupleMeta::new(3, 0);
        let rid3 = table_heap
            .insert_tuple(
                &meta3,
                &Tuple::new(schema.clone(), vec![3i8.into(), 3i16.into()]),
            )
            .unwrap();

        let mut iterator = TableIterator::new(table_heap.clone(), ..);

        let (rid, meta, tuple) = iterator.next().unwrap().unwrap();
        assert_eq!(rid, rid1);
        assert_eq!(meta, meta1);
        assert_eq!(tuple.data, vec![1i8.into(), 1i16.into()]);

        let (rid, meta, tuple) = iterator.next().unwrap().unwrap();
        assert_eq!(rid, rid2);
        assert_eq!(meta, meta2);
        assert_eq!(tuple.data, vec![2i8.into(), 2i16.into()]);

        let (rid, meta, tuple) = iterator.next().unwrap().unwrap();
        assert_eq!(rid, rid3);
        assert_eq!(meta, meta3);
        assert_eq!(tuple.data, vec![3i8.into(), 3i16.into()]);

        assert!(iterator.next().unwrap().is_none());
    }

    #[test]
    pub fn test_recover_set_tuple_meta_and_bytes() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferManager::new(128, disk_scheduler));
        let table_heap = TableHeap::try_new(schema.clone(), buffer_pool.clone()).unwrap();

        // Insert a row
        let rid = table_heap
            .insert_tuple(
                &EMPTY_TUPLE_META,
                &Tuple::new(schema.clone(), vec![1i8.into(), 10i16.into()]),
            )
            .unwrap();

        // Change bytes via recovery API
        let new_tuple = Tuple::new(schema.clone(), vec![2i8.into(), 20i16.into()]);
        let new_bytes = TupleCodec::encode(&new_tuple);
        table_heap
            .recover_set_tuple_bytes(rid, &new_bytes)
            .expect("recover bytes");

        // Verify tuple data changed
        let (_m, t) = table_heap.full_tuple(rid).unwrap();
        assert_eq!(t.data, vec![2i8.into(), 20i16.into()]);

        // Mark deleted via recovery API and verify
        let mut meta = table_heap.tuple_meta(rid).unwrap();
        meta.is_deleted = true;
        table_heap
            .recover_set_tuple_meta(rid, meta)
            .expect("recover meta");
        let m2 = table_heap.tuple_meta(rid).unwrap();
        assert!(m2.is_deleted);
    }

    #[test]
    pub fn test_recover_repack_on_size_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferManager::new(128, disk_scheduler));
        let table_heap = TableHeap::try_new(schema.clone(), buffer_pool.clone()).unwrap();

        let rid = table_heap
            .insert_tuple(
                &EMPTY_TUPLE_META,
                &Tuple::new(schema.clone(), vec![1i8.into(), 10i16.into()]),
            )
            .unwrap();

        // Create a tuple with different encoded length and recover-set it
        let larger_tuple = Tuple::new(schema.clone(), vec![99i8.into(), 300i16.into()]);
        let larger_bytes = TupleCodec::encode(&larger_tuple);
        table_heap
            .recover_set_tuple_bytes(rid, &larger_bytes)
            .expect("recover larger bytes");

        let (_m, t2) = table_heap.full_tuple(rid).unwrap();
        assert_eq!(t2.data, vec![99i8.into(), 300i16.into()]);
    }

    #[test]
    fn vacuum_slot_if_reclaims_tuple() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("vacuum.db");
        let schema = Arc::new(Schema::new(vec![Column::new("v", DataType::Int32, false)]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferManager::new(32, disk_scheduler));
        let heap = TableHeap::try_new(schema.clone(), buffer_pool).unwrap();

        let meta = super::TupleMeta::new(1, 0);
        let tuple = Tuple::new(schema.clone(), vec![ScalarValue::Int32(Some(5))]);
        let rid = heap.insert_tuple(&meta, &tuple).unwrap();

        assert!(heap.full_tuple(rid).is_ok());
        assert!(heap.vacuum_slot_if(rid, |_| true).unwrap());
        assert!(heap.full_tuple(rid).is_err());
        assert!(heap.get_first_rid().unwrap().is_none());
    }

    #[test]
    fn vacuum_slot_if_respects_predicate() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("vacuum_predicate.db");
        let schema = Arc::new(Schema::new(vec![Column::new("v", DataType::Int32, false)]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferManager::new(32, disk_scheduler));
        let heap = TableHeap::try_new(schema.clone(), buffer_pool).unwrap();

        let meta = super::TupleMeta::new(42, 0);
        let tuple = Tuple::new(schema.clone(), vec![ScalarValue::Int32(Some(9))]);
        let rid = heap.insert_tuple(&meta, &tuple).unwrap();

        assert!(!heap
            .vacuum_slot_if(rid, |current| current.insert_txn_id == 0)
            .unwrap());
        let (meta_after, tuple_after) = heap.full_tuple(rid).unwrap();
        assert_eq!(meta_after.insert_txn_id, 42);
        assert_eq!(tuple_after, tuple);
    }
}
