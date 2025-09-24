use crate::buffer::{AtomicPageId, INVALID_PAGE_ID};
use crate::catalog::SchemaRef;
use crate::storage::codec::TablePageCodec;
use crate::storage::page::{RecordId, TablePage, TupleMeta, INVALID_RID};
use crate::{buffer::BufferPoolManager, error::QuillSQLResult};
use std::collections::Bound;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::storage::scan::DirectRingBuffer;
use crate::storage::tuple::Tuple;

#[derive(Debug)]
pub struct TableHeap {
    pub schema: SchemaRef,
    pub buffer_pool: Arc<BufferPoolManager>,
    pub first_page_id: AtomicPageId,
    pub last_page_id: AtomicPageId,
}

impl TableHeap {
    /// Creates a new table heap. This involves allocating an initial page.
    pub fn try_new(schema: SchemaRef, buffer_pool: Arc<BufferPoolManager>) -> QuillSQLResult<Self> {
        // new_page() returns a WritePageGuard.
        let mut first_page_guard = buffer_pool.new_page()?;
        let first_page_id = first_page_guard.page_id();

        // Initialize the first page as an empty TablePage.
        let table_page = TablePage::new(schema.clone(), INVALID_PAGE_ID);
        let encoded_data = TablePageCodec::encode(&table_page);

        // Use DerefMut to get a mutable reference and update the page data.
        // This also marks the page as dirty automatically.
        first_page_guard.data.copy_from_slice(&encoded_data);

        // The first_page_guard is dropped here, automatically unpinning the page.
        Ok(Self {
            schema,
            buffer_pool,
            first_page_id: AtomicU32::new(first_page_id),
            last_page_id: AtomicU32::new(first_page_id),
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
    /// Inserts a tuple into the table.
    pub fn insert_tuple(&self, meta: &TupleMeta, tuple: &Tuple) -> QuillSQLResult<RecordId> {
        let mut current_page_id = self.last_page_id.load(Ordering::SeqCst);

        loop {
            let mut current_page_guard = self.buffer_pool.fetch_page_write(current_page_id)?;
            let mut table_page =
                TablePageCodec::decode(&current_page_guard.data, self.schema.clone())?.0;

            // If there's space, insert the tuple.
            if table_page.next_tuple_offset(tuple).is_ok() {
                let slot_id = table_page.insert_tuple(meta, tuple)?;

                // Encode the modified page back into the guard's buffer.
                let encoded_data = TablePageCodec::encode(&table_page);
                current_page_guard.data.copy_from_slice(&encoded_data);

                return Ok(RecordId::new(current_page_id, slot_id as u32));
            }

            // If the page is full, allocate a new one and link it.
            let new_page_guard = self.buffer_pool.new_page()?;
            let new_page_id = new_page_guard.page_id();

            // Initialize the new page.
            let new_table_page = TablePage::new(self.schema.clone(), INVALID_PAGE_ID);
            let _ = TablePageCodec::encode(&new_table_page);
            // new_page_guard is already a write guard, so we can modify its data.
            // We'll let it drop at the end of the *next* loop iteration (or when the function returns).
            // But for now, we must write our changes to it.
            // We don't need to do this here because the page is already blank.

            // Update the old page to point to the new one.
            table_page.header.next_page_id = new_page_id;
            let encoded_old_page = TablePageCodec::encode(&table_page);
            current_page_guard.data.copy_from_slice(&encoded_old_page);

            // The `current_page_guard` will be dropped here, saving the changes.
            drop(current_page_guard);

            // Update the atomic last_page_id and continue the loop.
            self.last_page_id.store(new_page_id, Ordering::SeqCst);
            current_page_id = new_page_id;
        }
    }

    pub fn update_tuple(&self, rid: RecordId, tuple: Tuple) -> QuillSQLResult<()> {
        let mut page_guard = self.buffer_pool.fetch_page_write(rid.page_id)?;
        let mut table_page = TablePageCodec::decode(&page_guard.data, self.schema.clone())?.0;

        table_page.update_tuple(tuple, rid.slot_num as u16)?;

        let encoded_data = TablePageCodec::encode(&table_page);
        page_guard.data.copy_from_slice(&encoded_data);
        Ok(())
    }

    pub fn update_tuple_meta(&self, meta: TupleMeta, rid: RecordId) -> QuillSQLResult<()> {
        let mut page_guard = self.buffer_pool.fetch_page_write(rid.page_id)?;
        let mut table_page = TablePageCodec::decode(&page_guard.data, self.schema.clone())?.0;

        table_page.update_tuple_meta(meta, rid.slot_num as u16)?;

        let encoded_data = TablePageCodec::encode(&table_page);
        page_guard.data.copy_from_slice(&encoded_data);
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
            if !table_page.header.tuple_infos[slot_num as usize]
                .meta
                .is_deleted
            {
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
            if !next_table_page.header.tuple_infos[slot_num as usize]
                .meta
                .is_deleted
            {
                return Ok(Some(RecordId::new(
                    table_page.header.next_page_id,
                    slot_num as u32,
                )));
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
    strategy: ScanStrategy,
}

#[derive(Debug)]
enum ScanStrategy {
    /// Existing behavior: go through buffer pool (page_table/LRU-K)
    Cached,
    /// Streaming with direct disk ring buffer
    Streaming {
        ring: DirectRingBuffer,
        current_page: Option<(u32, TablePage)>,
        current_slot: u16,
    },
}

impl TableIterator {
    pub fn new<R: RangeBounds<RecordId>>(heap: Arc<TableHeap>, range: R) -> Self {
        Self::new_with_hint(heap, range, None)
    }

    pub fn new_with_hint<R: RangeBounds<RecordId>>(
        heap: Arc<TableHeap>,
        range: R,
        streaming_hint: Option<bool>,
    ) -> Self {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();

        // Decide strategy: enable streaming if (env=1) or approx pages >= threshold and full scan
        let env_stream = std::env::var("QUILL_STREAM_SCAN").ok().as_deref() == Some("1");
        let pool_quarter = (heap.buffer_pool.pool.len().max(1) / 4) as u32;
        let threshold: u32 = std::env::var("QUILL_STREAM_THRESHOLD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(pool_quarter.max(1));
        let readahead: usize = std::env::var("QUILL_STREAM_READAHEAD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2);

        let approx_pages = heap
            .last_page_id
            .load(Ordering::SeqCst)
            .saturating_sub(heap.first_page_id.load(Ordering::SeqCst))
            + 1;

        let is_full_scan = matches!(start, Bound::Unbounded) && matches!(end, Bound::Unbounded);

        let use_streaming = match streaming_hint {
            Some(true) => true,
            Some(false) => false,
            None => is_full_scan && (env_stream || approx_pages >= threshold),
        };

        let strategy = if use_streaming {
            let ring = DirectRingBuffer::new(
                heap.schema.clone(),
                heap.buffer_pool.disk_scheduler.clone(),
                readahead,
            );
            ScanStrategy::Streaming {
                ring,
                current_page: None,
                current_slot: 0,
            }
        } else {
            ScanStrategy::Cached
        };

        Self {
            heap,
            start_bound: start,
            end_bound: end,
            cursor: INVALID_RID,
            started: false,
            ended: false,
            strategy,
        }
    }

    pub fn next(&mut self) -> QuillSQLResult<Option<(RecordId, Tuple)>> {
        if self.ended {
            return Ok(None);
        }

        // Streaming strategy (only supports full scan). For other ranges fallback to Cached.
        if let ScanStrategy::Streaming {
            ring,
            current_page,
            current_slot,
        } = &mut self.strategy
        {
            // Initialize on first call
            if !self.started {
                self.started = true;
                let first = self.heap.first_page_id.load(Ordering::SeqCst);
                if first == INVALID_PAGE_ID {
                    self.ended = true;
                    return Ok(None);
                }
                // Ensure disk visibility for streaming reads
                self.heap.buffer_pool.flush_all_pages()?;
                ring.prime(first)?;
                let (_pid, page) = match ring.next_page()? {
                    Some(v) => v,
                    None => {
                        self.ended = true;
                        return Ok(None);
                    }
                };
                *current_page = Some((first, page));
                *current_slot = 0;
            }

            loop {
                let (pid, page) = match current_page.as_mut() {
                    Some(v) => v,
                    None => {
                        self.ended = true;
                        return Ok(None);
                    }
                };

                // find next visible tuple from current_slot
                while (*current_slot as usize) < page.header.num_tuples as usize {
                    let slot = *current_slot;
                    *current_slot += 1;
                    if !page.header.tuple_infos[slot as usize].meta.is_deleted {
                        let rid = RecordId::new(*pid, slot as u32);
                        let (_m, t) = page.tuple(slot as u16)?;
                        return Ok(Some((rid, t)));
                    }
                }

                // move to next page
                if let Some((next_pid, next_page)) = ring.next_page()? {
                    *current_page = Some((next_pid, next_page));
                } else {
                    self.ended = true;
                    return Ok(None);
                }
                *current_slot = 0;
            }
        }

        // Cached strategy (original)
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

    use crate::buffer::BufferPoolManager;
    use crate::catalog::{Column, DataType, Schema};
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;
    use crate::storage::page::EMPTY_TUPLE_META;
    use crate::storage::table_heap::TableIterator;
    use crate::storage::{table_heap::TableHeap, tuple::Tuple};

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

    #[test]
    pub fn test_streaming_seq_scan_ring() {
        // Force streaming mode regardless of table size
        std::env::set_var("QUILL_STREAM_SCAN", "1");
        std::env::set_var("QUILL_STREAM_READAHEAD", "2");

        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));

        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferPoolManager::new(128, disk_scheduler));
        let table_heap = Arc::new(TableHeap::try_new(schema.clone(), buffer_pool).unwrap());

        // Insert many rows to span multiple pages
        let rows = 1000;
        for i in 0..rows {
            let _rid = table_heap
                .insert_tuple(
                    &super::TupleMeta {
                        insert_txn_id: 1,
                        delete_txn_id: 1,
                        is_deleted: false,
                    },
                    &Tuple::new(schema.clone(), vec![(i as i8).into(), (i as i16).into()]),
                )
                .unwrap();
        }

        // Ensure data is persisted before direct I/O streaming
        table_heap.buffer_pool.flush_all_pages().unwrap();

        // Iterate full range; should go through streaming ring buffer
        let mut it = TableIterator::new(table_heap.clone(), ..);
        let mut cnt = 0usize;
        while let Some((_rid, _t)) = it.next().unwrap() {
            cnt += 1;
        }
        assert_eq!(cnt, rows);
    }
}
