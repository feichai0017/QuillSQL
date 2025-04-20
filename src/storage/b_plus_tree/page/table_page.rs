use crate::error::{Error, Result};
use crate::storage::b_plus_tree::buffer_pool_manager::{PageId, INVALID_PAGE_ID, PAGE_SIZE};
use crate::storage::b_plus_tree::codec::{TablePageHeaderCodec, TablePageHeaderTupleInfoCodec};
use derive_new;
use std::sync::LazyLock;

pub type TransactionId = u64;

pub static EMPTY_TUPLE_META: TupleMeta = TupleMeta {
    insert_txn_id: 0,
    delete_txn_id: 0,
    is_deleted: false,
};

pub static EMPTY_TUPLE_INFO: LazyLock<TupleInfo> = LazyLock::new(|| TupleInfo {
    offset: 0,
    size: 0,
    meta: EMPTY_TUPLE_META,
});

/**
 * Slotted page format:
 *  ---------------------------------------------------------
 *  | HEADER | ... FREE SPACE ... | ... INSERTED DATA ... |
 *  ---------------------------------------------------------
 *                                ^
 *                                free space pointer
 *
 *  Header format (size in bytes):
 *  ----------------------------------------------------------------------------
 *  | NextPageId (4)| NumItems(2) | NumDeletedItems(2) |
 *  ----------------------------------------------------------------------------
 *  ----------------------------------------------------------------
 *  | Item_1 offset+size + ItemMeta | Item_2 offset+size + ItemMeta | ... |
 *  ----------------------------------------------------------------
 *
 */
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TablePage {
    pub header: TablePageHeader,
    // 整个页原始数据
    pub data: [u8; PAGE_SIZE],
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TablePageHeader {
    pub next_page_id: PageId,
    pub num_tuples: u16,
    pub num_deleted_tuples: u16,
    pub tuple_infos: Vec<TupleInfo>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TupleInfo {
    pub offset: u16,
    pub size: u16,
    pub meta: TupleMeta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleMeta {
    pub insert_txn_id: TransactionId,
    pub delete_txn_id: TransactionId,
    pub is_deleted: bool,
}

pub const INVALID_RID: RecordId = RecordId {
    page_id: INVALID_PAGE_ID,
    slot_num: 0,
};

#[derive(derive_new::new, Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordId {
    pub page_id: PageId,
    pub slot_num: u32,
}

impl TablePage {
    pub fn new(next_page_id: PageId) -> Self {
        Self {
            header: TablePageHeader {
                next_page_id,
                num_tuples: 0,
                num_deleted_tuples: 0,
                tuple_infos: Vec::new(),
            },
            data: [0; PAGE_SIZE],
        }
    }

    // Get the offset for the next data insertion.
    pub fn next_data_offset(&self, data: &[u8]) -> Result<usize> {
        // Get the ending offset of the current slot. If there are inserted items,
        // get the offset of the previous inserted item; otherwise, set it to the size of the page.
        let slot_end_offset = if self.header.num_tuples > 0 {
            self.header.tuple_infos[self.header.num_tuples as usize - 1].offset as usize
        } else {
            PAGE_SIZE
        };

        // Check if the current slot has enough space for the new data. Return an error if not.
        if slot_end_offset < data.len() {
            return Err(Error::Internal("No enough space to store data".to_string()));
        }

        // Calculate the insertion offset for the new data by subtracting its length
        // from the ending offset of the current slot.
        let data_offset = slot_end_offset - data.len();

        // Calculate the minimum valid data insertion offset, including the table page header size,
        // the total size of each tuple info (existing tuple infos and newly added tuple info).
        let min_data_offset = TablePageHeaderCodec::encode(&self.header).len()
            + TablePageHeaderTupleInfoCodec::encode(&EMPTY_TUPLE_INFO).len();
        if data_offset < min_data_offset {
            return Err(Error::Internal("No enough space to store data".to_string()));
        }

        // Return the calculated insertion offset for the new data.
        Ok(data_offset)
    }

    pub fn insert_data(&mut self, meta: &TupleMeta, data: &[u8]) -> Result<u16> {
        // Get the offset for the next data insertion.
        let data_offset = self.next_data_offset(data)?;
        let tuple_id = self.header.num_tuples;
        debug_assert!(data.len() < u16::MAX as usize);

        // Store data information including offset, length, and metadata.
        self.header.tuple_infos.push(TupleInfo {
            offset: data_offset as u16,
            size: data.len() as u16,
            meta: *meta,
        });

        // only check
        assert_eq!(tuple_id, self.header.tuple_infos.len() as u16 - 1);

        self.header.num_tuples += 1;
        if meta.is_deleted {
            self.header.num_deleted_tuples += 1;
        }

        // Copy the data into the appropriate position within the page's data buffer.
        self.data[data_offset..data_offset + data.len()].copy_from_slice(data);
        Ok(tuple_id)
    }

    pub fn update_tuple_meta(&mut self, meta: TupleMeta, slot_num: u16) -> Result<()> {
        if slot_num >= self.header.num_tuples {
            return Err(Error::Internal(format!(
                "tuple_id {} out of range",
                slot_num
            )));
        }
        if meta.is_deleted && !self.header.tuple_infos[slot_num as usize].meta.is_deleted {
            self.header.num_deleted_tuples += 1;
        }

        self.header.tuple_infos[slot_num as usize].meta = meta;
        Ok(())
    }

    pub fn update_data(&mut self, data: &[u8], slot_num: u16) -> Result<()> {
        if slot_num >= self.header.num_tuples {
            return Err(Error::Internal(format!(
                "tuple_id {} out of range",
                slot_num
            )));
        }
        let offset = self.header.tuple_infos[slot_num as usize].offset as usize;
        let size = self.header.tuple_infos[slot_num as usize].size as usize;
        if data.len() == size {
            self.data[offset..(offset + size)].copy_from_slice(data);
        } else {
            // need to move other data
            let mut full_items = vec![];
            for info in self.header.tuple_infos.iter() {
                full_items.push((
                    info.meta,
                    self.data[info.offset as usize..(info.offset + info.size) as usize].to_vec(),
                ));
            }
            full_items[slot_num as usize].1 = data.to_vec();

            let mut new_page = TablePage::new(self.header.next_page_id);
            for (meta, item_data) in full_items.iter() {
                new_page.insert_data(meta, item_data)?;
            }
            self.header = new_page.header;
            self.data = new_page.data;
        }
        Ok(())
    }

    pub fn get_data(&self, slot_num: u16) -> Result<(TupleMeta, Vec<u8>)> {
        if slot_num >= self.header.num_tuples {
            return Err(Error::Internal(format!(
                "tuple_id {} out of range",
                slot_num
            )));
        }

        let offset = self.header.tuple_infos[slot_num as usize].offset;
        let size = self.header.tuple_infos[slot_num as usize].size;
        let meta = self.header.tuple_infos[slot_num as usize].meta;
        let data = self.data[offset as usize..(offset + size) as usize].to_vec();

        Ok((meta, data))
    }

    pub fn tuple_meta(&self, slot_num: u16) -> Result<TupleMeta> {
        if slot_num >= self.header.num_tuples {
            return Err(Error::Internal(format!(
                "tuple_id {} out of range",
                slot_num
            )));
        }

        Ok(self.header.tuple_infos[slot_num as usize].meta)
    }

    pub fn get_next_rid(&self, rid: &RecordId) -> Option<RecordId> {
        // TODO 忽略删除的tuple
        let tuple_id = rid.slot_num;
        if tuple_id + 1 >= self.header.num_tuples as u32 {
            return None;
        }

        Some(RecordId::new(rid.page_id, tuple_id + 1))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::b_plus_tree::page::table_page::EMPTY_TUPLE_META;

    // 辅助函数，创建测试用的字节数组
    fn create_test_data(value: u32) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    #[test]
    pub fn test_table_page_get_data() {
        let mut table_page = super::TablePage::new(0);
        let data1 = create_test_data(1);
        let data2 = create_test_data(2);
        let data3 = create_test_data(3);

        let tuple_id = table_page.insert_data(&EMPTY_TUPLE_META, &data1).unwrap();
        assert_eq!(tuple_id, 0);

        let tuple_id = table_page.insert_data(&EMPTY_TUPLE_META, &data2).unwrap();
        assert_eq!(tuple_id, 1);

        let tuple_id = table_page.insert_data(&EMPTY_TUPLE_META, &data3).unwrap();
        assert_eq!(tuple_id, 2);

        let (tuple_meta, retrieved_data) = table_page.get_data(0).unwrap();
        assert_eq!(tuple_meta, EMPTY_TUPLE_META);
        assert_eq!(retrieved_data, data1);

        let (_tuple_meta, retrieved_data) = table_page.get_data(1).unwrap();
        assert_eq!(retrieved_data, data2);

        let (_tuple_meta, retrieved_data) = table_page.get_data(2).unwrap();
        assert_eq!(retrieved_data, data3);
    }

    #[test]
    pub fn test_table_page_update_tuple_meta() {
        let mut table_page = super::TablePage::new(0);
        let data1 = create_test_data(1);
        let data2 = create_test_data(2);
        let data3 = create_test_data(3);

        let _tuple_id = table_page.insert_data(&EMPTY_TUPLE_META, &data1).unwrap();
        let _tuple_id = table_page.insert_data(&EMPTY_TUPLE_META, &data2).unwrap();
        let _tuple_id = table_page.insert_data(&EMPTY_TUPLE_META, &data3).unwrap();

        let mut tuple_meta = table_page.tuple_meta(0).unwrap();
        tuple_meta.is_deleted = true;
        tuple_meta.delete_txn_id = 1;
        tuple_meta.insert_txn_id = 2;

        table_page.update_tuple_meta(tuple_meta, 0).unwrap();
        let tuple_meta = table_page.tuple_meta(0).unwrap();
        assert!(tuple_meta.is_deleted);
        assert_eq!(tuple_meta.delete_txn_id, 1);
        assert_eq!(tuple_meta.insert_txn_id, 2);
    }
}
