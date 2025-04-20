use crate::error::Result;
use crate::storage::b_plus_tree::buffer_pool_manager::BufferPoolManager;
use crate::storage::b_plus_tree::buffer_pool_manager::{AtomicPageId, INVALID_PAGE_ID};
use crate::storage::b_plus_tree::codec::TablePageCodec;
use crate::storage::b_plus_tree::page::table_page::{RecordId, TablePage, TupleMeta};
use crate::utils::util::page_bytes_to_array;
use std::collections::Bound;
use std::ops::RangeBounds;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Debug)]
pub struct TableHeap {
    pub buffer_pool: Arc<BufferPoolManager>,
    pub first_page_id: AtomicPageId,
    pub last_page_id: AtomicPageId,
}

impl TableHeap {
    pub fn try_new(buffer_pool: Arc<BufferPoolManager>) -> Result<Self> {
        // new a page and initialize
        let first_page = buffer_pool.new_page()?;
        let first_page_id = first_page.read().page_id;
        let table_page = TablePage::new(INVALID_PAGE_ID);
        first_page
            .write()
            .set_data(page_bytes_to_array(&TablePageCodec::encode(&table_page)));

        Ok(Self {
            buffer_pool,
            first_page_id: AtomicPageId::new(first_page_id),
            last_page_id: AtomicPageId::new(first_page_id),
        })
    }

    /// Inserts raw byte data into the table.
    ///
    /// Parameters:
    /// - `meta`: The metadata associated with the data.
    /// - `data`: The raw byte data (`Vec<u8>`) to be inserted.
    ///
    /// Returns:
    /// A `Result` containing the `RecordId` of the inserted data if successful.
    pub fn insert_data(&self, meta: &TupleMeta, data: &[u8]) -> Result<RecordId> {
        let mut last_page_id = self.last_page_id.load(Ordering::SeqCst);

        // Need to loop until we find or create a page with enough space
        loop {
            let (page_ref, mut table_page) = self.buffer_pool.fetch_table_page(last_page_id)?; // Fetch the current candidate page

            // Check if the current page has enough space using next_data_offset
            if table_page.next_data_offset(data).is_ok() {
                // Yes, insert into this page
                let slot_id = table_page.insert_data(meta, data)?;

                // Write the modified page back
                page_ref
                    .write()
                    .set_data(page_bytes_to_array(&TablePageCodec::encode(&table_page)));

                // Return the new RecordId
                return Ok(RecordId::new(last_page_id, slot_id as u32));
            }

            // Current page is full, need to allocate a new one
            // if there's no tuple in the page, and we can't insert the tuple,
            // then this tuple is too large.
            assert!(
                table_page.header.num_tuples > 0 || table_page.header.num_deleted_tuples > 0, // Check if page was ever used
                "Data is too large to fit in an empty page, cannot insert"
            );

            // Allocate a new page
            let new_page = self.buffer_pool.new_page()?;
            let new_page_id = new_page.read().page_id;
            let new_table_page = TablePage::new(INVALID_PAGE_ID);
            new_page
                .write()
                .set_data(page_bytes_to_array(&TablePageCodec::encode(
                    &new_table_page,
                )));

            // Update and release the *previous* page to point to the new page
            table_page.header.next_page_id = new_page_id;
            let prev_page_ref = self.buffer_pool.fetch_page(last_page_id)?; // Re-fetch previous page ref to write
            prev_page_ref
                .write()
                .set_data(page_bytes_to_array(&TablePageCodec::encode(&table_page)));

            // Update last_page_id tracker and loop to try inserting into the new page
            self.last_page_id.store(new_page_id, Ordering::SeqCst);
            last_page_id = new_page_id;
            // The loop will now fetch and check the new page
        }
    }

    // Update existing data identified by RecordId
    pub fn update_data(&self, rid: RecordId, data: &[u8]) -> Result<()> {
        let (page, mut table_page) = self.buffer_pool.fetch_table_page(rid.page_id)?;
        table_page.update_data(data, rid.slot_num as u16)?; // Use update_data

        page.write()
            .set_data(page_bytes_to_array(&TablePageCodec::encode(&table_page)));
        Ok(())
    }

    // Update only the metadata
    pub fn update_tuple_meta(&self, meta: TupleMeta, rid: RecordId) -> Result<()> {
        let (page, mut table_page) = self.buffer_pool.fetch_table_page(rid.page_id)?;
        table_page.update_tuple_meta(meta, rid.slot_num as u16)?;

        page.write()
            .set_data(page_bytes_to_array(&TablePageCodec::encode(&table_page)));
        Ok(())
    }

    // Get both metadata and data
    pub fn get_full_data(&self, rid: RecordId) -> Result<(TupleMeta, Vec<u8>)> {
        let (_, table_page) = self.buffer_pool.fetch_table_page(rid.page_id)?;
        let result = table_page.get_data(rid.slot_num as u16)?; // Use get_data
        Ok(result)
    }

    // Get only the data
    pub fn get_data(&self, rid: RecordId) -> Result<Vec<u8>> {
        let (_meta, data) = self.get_full_data(rid)?;
        Ok(data)
    }

    // Get only the metadata
    pub fn tuple_meta(&self, rid: RecordId) -> Result<TupleMeta> {
        let (meta, _data) = self.get_full_data(rid)?;
        Ok(meta)
    }

    // Find the first valid (not deleted) RecordId
    pub fn get_first_rid(&self) -> Result<Option<RecordId>> {
        let mut page_id_option = Some(self.first_page_id.load(Ordering::SeqCst));

        while let Some(page_id) = page_id_option {
            if page_id == INVALID_PAGE_ID {
                break;
            }
            let (_, table_page) = self.buffer_pool.fetch_table_page(page_id)?;
            for slot_num in 0..table_page.header.num_tuples {
                if let Ok(meta) = table_page.tuple_meta(slot_num) {
                    if !meta.is_deleted {
                        return Ok(Some(RecordId::new(page_id, slot_num as u32)));
                    }
                }
            }
            page_id_option = Some(table_page.header.next_page_id);
        }
        Ok(None)
    }

    // Find the next valid (not deleted) RecordId after the given one
    pub fn get_next_rid(&self, rid: RecordId) -> Result<Option<RecordId>> {
        let (_, current_table_page) = self.buffer_pool.fetch_table_page(rid.page_id)?;
        let mut current_slot = rid.slot_num as u16;

        // Try next slots in the current page
        current_slot += 1;
        while current_slot < current_table_page.header.num_tuples {
            if let Ok(meta) = current_table_page.tuple_meta(current_slot) {
                if !meta.is_deleted {
                    return Ok(Some(RecordId::new(rid.page_id, current_slot as u32)));
                }
            }
            current_slot += 1;
        }

        // Move to the next page(s) if necessary
        let mut next_page_id_option = Some(current_table_page.header.next_page_id);
        while let Some(page_id) = next_page_id_option {
            if page_id == INVALID_PAGE_ID {
                break;
            }
            let (_, table_page) = self.buffer_pool.fetch_table_page(page_id)?;
            for slot_num in 0..table_page.header.num_tuples {
                if let Ok(meta) = table_page.tuple_meta(slot_num) {
                    if !meta.is_deleted {
                        return Ok(Some(RecordId::new(page_id, slot_num as u32)));
                    }
                }
            }
            next_page_id_option = Some(table_page.header.next_page_id);
        }

        Ok(None)
    }
}

#[derive(Debug)]
pub struct TableIterator {
    heap: Arc<TableHeap>,
    start_bound: Bound<RecordId>,
    end_bound: Bound<RecordId>,
    cursor: Option<RecordId>, // Use Option to represent initial state / end
                              // Remove started/ended flags, cursor Option handles this
}

impl TableIterator {
    pub fn new<R: RangeBounds<RecordId>>(heap: Arc<TableHeap>, range: R) -> Self {
        Self {
            heap,
            start_bound: range.start_bound().cloned(),
            end_bound: range.end_bound().cloned(),
            cursor: None, // Start with no cursor position
        }
    }

    // Helper to check if a rid is within the end bound
    fn is_within_end_bound(&self, rid: RecordId) -> bool {
        match self.end_bound {
            Bound::Included(bound_rid) => {
                rid.page_id <= bound_rid.page_id && rid.slot_num <= bound_rid.slot_num
            }
            Bound::Excluded(bound_rid) => {
                rid.page_id < bound_rid.page_id
                    || (rid.page_id == bound_rid.page_id && rid.slot_num < bound_rid.slot_num)
            }
            Bound::Unbounded => true,
        }
    }

    pub fn next(&mut self) -> Result<Option<(RecordId, Vec<u8>)>> {
        let next_rid = if self.cursor.is_none() {
            // First call to next()
            // Determine the starting RecordId based on start_bound
            match self.start_bound {
                Bound::Included(rid) => Some(rid),
                Bound::Excluded(rid) => self.heap.get_next_rid(rid)?,
                Bound::Unbounded => self.heap.get_first_rid()?,
            }
        } else {
            // Subsequent calls: get the next RID after the current cursor
            self.heap.get_next_rid(self.cursor.unwrap())?
        };

        if let Some(current_rid) = next_rid {
            // Check if the determined/next RID is within the end bound
            if self.is_within_end_bound(current_rid) {
                self.cursor = Some(current_rid); // Update cursor
                                                 // Fetch the data for the current valid RID
                match self.heap.get_data(current_rid) {
                    Ok(data) => Ok(Some((current_rid, data))),
                    Err(e) => Err(e), // Propagate error fetching data
                }
            } else {
                // Reached or exceeded the end bound
                self.cursor = None; // Mark iterator as finished
                Ok(None)
            }
        } else {
            // No more RIDs found
            self.cursor = None; // Mark iterator as finished
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;
    use tempfile::TempDir;

    // Correct imports based on assumed project structure
    use crate::error::Result;
    use crate::storage::b_plus_tree::buffer_pool_manager::BufferPoolManager;
    use crate::storage::b_plus_tree::disk::disk_manager::DiskManager;
    use crate::storage::b_plus_tree::disk::disk_scheduler::DiskScheduler;
    use crate::storage::b_plus_tree::page::table_page::{
        TupleMeta, EMPTY_TUPLE_META,
    };
    use crate::storage::b_plus_tree::table_heap::{TableHeap, TableIterator};
    // Helper to create test data
    fn create_test_data(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    pub fn test_table_heap_insert_and_get_data() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test_heap.db");
        let disk_manager = Arc::new(DiskManager::try_new(temp_path)?);
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let buffer_pool = Arc::new(BufferPoolManager::new(1000, 2, disk_scheduler)?);
        let table_heap = TableHeap::try_new(buffer_pool)?;

        let data1 = create_test_data("record1");
        let data2 = create_test_data("record2");
        let data3 = create_test_data("record3");

        let meta1 = TupleMeta {
            insert_txn_id: 1,
            delete_txn_id: 0,
            is_deleted: false,
        };
        let meta2 = TupleMeta {
            insert_txn_id: 2,
            delete_txn_id: 0,
            is_deleted: false,
        };
        let meta3 = TupleMeta {
            insert_txn_id: 3,
            delete_txn_id: 0,
            is_deleted: false,
        };

        let rid1 = table_heap.insert_data(&meta1, &data1)?;
        let rid2 = table_heap.insert_data(&meta2, &data2)?;
        let rid3 = table_heap.insert_data(&meta3, &data3)?;

        let (ret_meta1, ret_data1) = table_heap.get_full_data(rid1)?;
        assert_eq!(ret_meta1, meta1);
        assert_eq!(ret_data1, data1);

        let (ret_meta2, ret_data2) = table_heap.get_full_data(rid2)?;
        assert_eq!(ret_meta2, meta2);
        assert_eq!(ret_data2, data2);

        let (ret_meta3, ret_data3) = table_heap.get_full_data(rid3)?;
        assert_eq!(ret_meta3, meta3);
        assert_eq!(ret_data3, data3);

        Ok(())
    }

    #[test]
    pub fn test_table_heap_update_tuple_meta() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test_heap_meta.db");
        let disk_manager = Arc::new(DiskManager::try_new(temp_path)?);
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let buffer_pool = Arc::new(BufferPoolManager::new(1000, 2, disk_scheduler)?);
        let table_heap = TableHeap::try_new(buffer_pool)?;

        let rid1 = table_heap.insert_data(&EMPTY_TUPLE_META, &create_test_data("data1"))?;
        let rid2 = table_heap.insert_data(&EMPTY_TUPLE_META, &create_test_data("data2"))?;
        let rid3 = table_heap.insert_data(&EMPTY_TUPLE_META, &create_test_data("data3"))?;

        let mut meta = table_heap.tuple_meta(rid2)?;
        assert!(!meta.is_deleted);
        meta.insert_txn_id = 10;
        meta.delete_txn_id = 20;
        meta.is_deleted = true;
        table_heap.update_tuple_meta(meta, rid2)?;

        let updated_meta = table_heap.tuple_meta(rid2)?;
        assert_eq!(updated_meta.insert_txn_id, 10);
        assert_eq!(updated_meta.delete_txn_id, 20);
        assert!(updated_meta.is_deleted);

        // Check others are unaffected
        assert!(!table_heap.tuple_meta(rid1)?.is_deleted);
        assert!(!table_heap.tuple_meta(rid3)?.is_deleted);

        Ok(())
    }

    #[test]
    pub fn test_table_heap_iterator() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test_heap_iter.db");
        let disk_manager = Arc::new(DiskManager::try_new(temp_path)?);
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let buffer_pool = Arc::new(BufferPoolManager::new(1000, 2, disk_scheduler)?);
        let table_heap = Arc::new(TableHeap::try_new(buffer_pool)?);

        let data1 = create_test_data("iter_data1");
        let data2 = create_test_data("iter_data2");
        let data3 = create_test_data("iter_data3");
        let data4_deleted = create_test_data("iter_data4_deleted");
        let data5 = create_test_data("iter_data5");

        let rid1 = table_heap.insert_data(&EMPTY_TUPLE_META, &data1)?;
        let rid2 = table_heap.insert_data(&EMPTY_TUPLE_META, &data2)?;
        let rid3 = table_heap.insert_data(&EMPTY_TUPLE_META, &data3)?;
        let rid4 = table_heap.insert_data(&EMPTY_TUPLE_META, &data4_deleted)?; // Insert data to be deleted
        let rid5 = table_heap.insert_data(&EMPTY_TUPLE_META, &data5)?;

        // Mark rid4 as deleted
        let mut meta4 = table_heap.tuple_meta(rid4)?;
        meta4.is_deleted = true;
        table_heap.update_tuple_meta(meta4, rid4)?;

        // Iterate over all non-deleted items
        let mut iterator = TableIterator::new(table_heap.clone(), ..);
        let mut results = Vec::new();
        while let Some(item) = iterator.next()? {
            results.push(item);
        }

        // Should skip the deleted record (rid4)
        assert_eq!(results.len(), 4);
        assert_eq!(results[0], (rid1, data1));
        assert_eq!(results[1], (rid2, data2));
        assert_eq!(results[2], (rid3, data3));
        assert_eq!(results[3], (rid5, data5));

        // Test bounded iteration (e.g., from rid2 inclusive to rid5 exclusive)
        let mut iterator_bounded = TableIterator::new(table_heap.clone(), rid2..rid5);
        let mut results_bounded = Vec::new();
        while let Some(item) = iterator_bounded.next()? {
            results_bounded.push(item);
        }

        // Should include rid2, rid3 (skip deleted rid4), and stop before rid5
        assert_eq!(results_bounded.len(), 2);
        assert_eq!(results_bounded[0].0, rid2);
        assert_eq!(results_bounded[1].0, rid3);
        Ok(())
    }
}
