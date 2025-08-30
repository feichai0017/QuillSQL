use log::debug;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::RwLock;
use std::{
    io::{Read, Seek, Write},
    sync::{atomic::AtomicU32, Mutex, MutexGuard},
};

use crate::error::{QuillSQLError, QuillSQLResult};

use crate::buffer::{PageId, PAGE_SIZE, INVALID_PAGE_ID};
use crate::storage::codec::{FreelistPageCodec, MetaPageCodec};
use crate::storage::page::FreelistPage;
use crate::storage::page::MetaPage;
use crate::storage::page::META_PAGE_SIZE;

static EMPTY_PAGE: [u8; PAGE_SIZE] = [0; PAGE_SIZE];

#[derive(Debug)]
pub struct DiskManager {
    next_page_id: AtomicU32,
    db_file: Mutex<File>,
    pub meta: RwLock<MetaPage>,
}

impl DiskManager {
    pub fn try_new(db_path: impl AsRef<Path>) -> QuillSQLResult<Self> {
        let mut is_new_file = false;
        let (db_file, meta) = if db_path.as_ref().exists() {
            let mut db_file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(db_path)?;
            let mut buf = vec![0; *META_PAGE_SIZE];
            db_file.read_exact(&mut buf)?;
            let (meta_page, _) = MetaPageCodec::decode(&buf)?;
            (db_file, meta_page)
        } else {
            is_new_file = true;
            let mut db_file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(db_path)?;
            let meta_page = MetaPage::try_new()?;
            #[allow(clippy::unused_io_amount)]
            db_file.write(&MetaPageCodec::encode(&meta_page))?;
            (db_file, meta_page)
        };

        // calculate next page id
        let db_file_len = db_file.metadata()?.len();
        if (db_file_len - *META_PAGE_SIZE as u64) % PAGE_SIZE as u64 != 0 {
            return Err(QuillSQLError::Internal(format!(
                "db file size not a multiple of {} + meta page size {}",
                PAGE_SIZE, *META_PAGE_SIZE,
            )));
        }
        let next_page_id =
            (((db_file_len - *META_PAGE_SIZE as u64) / PAGE_SIZE as u64) + 1) as PageId;
        debug!("Initialized disk_manager next_page_id: {}", next_page_id);

        let disk_manager = Self {
            next_page_id: AtomicU32::new(next_page_id),
            // Use a mutex to wrap the file handle to ensure that only one thread
            // can access the file at the same time among multiple threads.
            db_file: Mutex::new(db_file),
            meta: RwLock::new(meta),
        };

        // new pages
        if is_new_file {
            let freelist_page_id = disk_manager.allocate_freelist_page()?;
            let information_schema_schemas_first_page_id = disk_manager.allocate_page()?;
            let information_schema_tables_first_page_id = disk_manager.allocate_page()?;
            let information_schema_columns_first_page_id = disk_manager.allocate_page()?;
            let information_schema_indexes_first_page_id = disk_manager.allocate_page()?;

            let mut meta = disk_manager.meta.write().unwrap();
            meta.freelist_page_id = freelist_page_id;
            meta.information_schema_schemas_first_page_id =
                information_schema_schemas_first_page_id;
            meta.information_schema_tables_first_page_id = information_schema_tables_first_page_id;
            meta.information_schema_columns_first_page_id =
                information_schema_columns_first_page_id;
            meta.information_schema_indexes_first_page_id =
                information_schema_indexes_first_page_id;
            drop(meta);
            disk_manager.write_meta_page()?;
        }
        debug!(
            "disk_manager meta page: {:?}",
            disk_manager.meta.read().unwrap()
        );

        Ok(disk_manager)
    }

    pub fn read_page(&self, page_id: PageId) -> QuillSQLResult<[u8; PAGE_SIZE]> {
        let mut guard = self.db_file.lock().unwrap();
        let mut buf = [0; PAGE_SIZE];

        // set offset and read page data
        guard.seek(std::io::SeekFrom::Start(
            (*META_PAGE_SIZE + (page_id - 1) as usize * PAGE_SIZE) as u64,
        ))?;
        // Read buf.len() bytes of data from the file, and store the data in the buf array.
        guard.read_exact(&mut buf)?;

        Ok(buf)
    }

    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> QuillSQLResult<()> {
        if data.len() != PAGE_SIZE {
            return Err(QuillSQLError::Internal(format!(
                "Page size is not {}",
                PAGE_SIZE
            )));
        }
        let mut guard = self.db_file.lock().unwrap();
        Self::write_page_internal(&mut guard, page_id, data)
    }

    pub fn allocate_page(&self) -> QuillSQLResult<PageId> {
        if let Some(page_id) = self.freelist_pop()? {
            Ok(page_id)
        } else {
            let mut guard = self.db_file.lock().unwrap();

            // fetch current value and increment page id
            let page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);

            // Write an empty page (all zeros) to the allocated page.
            Self::write_page_internal(&mut guard, page_id, &EMPTY_PAGE)?;

            Ok(page_id)
        }
    }

    pub fn allocate_freelist_page(&self) -> QuillSQLResult<PageId> {
        let page_id = self.allocate_page()?;
        let freelist_page = FreelistPage::new();
        self.write_page(page_id, &FreelistPageCodec::encode(&freelist_page))?;
        Ok(page_id)
    }

    pub fn deallocate_page(&self, page_id: PageId) -> QuillSQLResult<()> {
        // Write an empty page (all zeros) to the deallocated page.
        // But this page is not deallocated, only data will be written with null or zeros.
        let mut guard = self.db_file.lock().unwrap();
        Self::write_page_internal(&mut guard, page_id, &EMPTY_PAGE)?;
        drop(guard);

        self.freelist_push(page_id)?;
        Ok(())
    }

    fn freelist_push(&self, page_id: PageId) -> QuillSQLResult<()> {
        let mut curr_page_id = INVALID_PAGE_ID;
        let mut next_page_id = self.meta.read().unwrap().freelist_page_id;
        loop {
            let mut freelist_page = if next_page_id == INVALID_PAGE_ID {
                next_page_id = self.allocate_freelist_page()?;
                if curr_page_id != INVALID_PAGE_ID {
                    let (mut curr_freelist_page, _) =
                        FreelistPageCodec::decode(&self.read_page(curr_page_id)?)?;
                    curr_freelist_page.header.next_page_id = next_page_id;
                    self.write_page(
                        curr_page_id,
                        &FreelistPageCodec::encode(&curr_freelist_page),
                    )?;
                }

                FreelistPage::new()
            } else {
                let (freelist_page, _) = FreelistPageCodec::decode(&self.read_page(next_page_id)?)?;
                freelist_page
            };

            if freelist_page.is_full() {
                curr_page_id = next_page_id;
                next_page_id = freelist_page.header.next_page_id;
            } else {
                freelist_page.push(page_id);
                // persist page data
                self.write_page(next_page_id, &FreelistPageCodec::encode(&freelist_page))?;
                break;
            }
        }
        Ok(())
    }

    fn freelist_pop(&self) -> QuillSQLResult<Option<PageId>> {
        let mut freelist_page_id = self.meta.read().unwrap().freelist_page_id;
        loop {
            if freelist_page_id != INVALID_PAGE_ID {
                let (mut freelist_page, _) =
                    FreelistPageCodec::decode(&self.read_page(freelist_page_id)?)?;
                if let Some(page_id) = freelist_page.pop() {
                    self.write_page(freelist_page_id, &FreelistPageCodec::encode(&freelist_page))?;
                    return Ok(Some(page_id));
                } else {
                    freelist_page_id = freelist_page.header.next_page_id;
                }
            } else {
                return Ok(None);
            }
        }
    }

    fn write_meta_page(&self) -> QuillSQLResult<()> {
        let mut guard = self.db_file.lock().unwrap();
        guard.seek(std::io::SeekFrom::Start(0))?;
        guard.write_all(&MetaPageCodec::encode(&self.meta.read().unwrap()))?;
        guard.flush()?;
        Ok(())
    }

    fn write_page_internal(
        guard: &mut MutexGuard<File>,
        page_id: PageId,
        data: &[u8],
    ) -> QuillSQLResult<()> {
        // Seek to the start of the page in the database file and write the data.
        guard.seek(std::io::SeekFrom::Start(
            (*META_PAGE_SIZE + (page_id - 1) as usize * PAGE_SIZE) as u64,
        ))?;
        guard.write_all(data)?;
        guard.flush()?;
        Ok(())
    }

    pub fn db_file_len(&self) -> QuillSQLResult<u64> {
        let guard = self.db_file.lock().unwrap();
        let meta = guard.metadata()?;
        Ok(meta.len())
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::PAGE_SIZE;
    use crate::storage::codec::MetaPageCodec;
    use crate::storage::page::EMPTY_META_PAGE;
    use tempfile::TempDir;

    #[test]
    pub fn test_disk_manager_write_read_page() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let disk_manager = super::DiskManager::try_new(temp_path).unwrap();

        let page_id1 = disk_manager.allocate_page().unwrap();
        assert_eq!(page_id1, 6);
        let mut page1 = vec![1, 2, 3];
        page1.extend(vec![0; PAGE_SIZE - 3]);
        disk_manager.write_page(page_id1, &page1).unwrap();
        let page = disk_manager.read_page(page_id1).unwrap();
        assert_eq!(page, page1.as_slice());

        let page_id2 = disk_manager.allocate_page().unwrap();
        assert_eq!(page_id2, 7);
        let mut page2 = vec![0; PAGE_SIZE - 3];
        page2.extend(vec![4, 5, 6]);
        disk_manager.write_page(page_id2, &page2).unwrap();
        let page = disk_manager.read_page(page_id2).unwrap();
        assert_eq!(page, page2.as_slice());

        let db_file_len = disk_manager.db_file_len().unwrap();
        assert_eq!(
            db_file_len as usize,
            PAGE_SIZE * 7 + MetaPageCodec::encode(&EMPTY_META_PAGE).len()
        );
    }

    #[test]
    pub fn test_disk_manager_freelist() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let disk_manager = super::DiskManager::try_new(temp_path).unwrap();

        let page_id1 = disk_manager.allocate_page().unwrap();
        let _page_id2 = disk_manager.allocate_page().unwrap();
        let _page_id3 = disk_manager.allocate_page().unwrap();

        disk_manager.deallocate_page(page_id1).unwrap();

        let page_id4 = disk_manager.allocate_page().unwrap();
        assert_eq!(page_id1, page_id4);
    }
}