use log::{debug, warn};
use std::alloc::{alloc, alloc_zeroed, dealloc, Layout};
use std::fs::File;
use std::path::Path;
use std::ptr::NonNull;
use std::slice;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::RwLock;
use std::{
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    sync::{Mutex, MutexGuard},
};

#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;

use crate::error::{QuillSQLError, QuillSQLResult};

use crate::buffer::{PageId, INVALID_PAGE_ID, PAGE_SIZE};
use crate::storage::codec::FreelistPageCodec;
use crate::storage::page::FreelistPage;
use crate::storage::page::{decode_meta_page, encode_meta_page, MetaPage, META_PAGE_SIZE};

static EMPTY_PAGE: [u8; PAGE_SIZE] = [0; PAGE_SIZE];

/// Page-aligned buffer suitable for O_DIRECT transfers.
pub(crate) struct AlignedPageBuf {
    ptr: NonNull<u8>,
    layout: Layout,
}

impl AlignedPageBuf {
    pub(crate) fn new_zeroed() -> QuillSQLResult<Self> {
        Self::allocate(true)
    }

    pub(crate) fn new_uninit() -> QuillSQLResult<Self> {
        Self::allocate(false)
    }

    fn allocate(zeroed: bool) -> QuillSQLResult<Self> {
        let layout = Layout::from_size_align(PAGE_SIZE, PAGE_SIZE)
            .map_err(|_| QuillSQLError::Internal("Invalid PAGE_SIZE layout".into()))?;
        let ptr = unsafe {
            if zeroed {
                alloc_zeroed(layout)
            } else {
                alloc(layout)
            }
        };
        let Some(non_null) = NonNull::new(ptr) else {
            return Err(QuillSQLError::Internal("Aligned allocation failed".into()));
        };
        Ok(Self {
            ptr: non_null,
            layout,
        })
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), PAGE_SIZE) }
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), PAGE_SIZE) }
    }

    pub(crate) fn ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Drop for AlignedPageBuf {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

#[derive(Debug)]
pub struct DiskManager {
    next_page_id: AtomicU32,
    db_file: Mutex<File>,
    pub meta: RwLock<MetaPage>,
}

impl DiskManager {
    fn open_raw_file(db_path: &Path, create: bool, direct: bool) -> std::io::Result<(File, bool)> {
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true);
        if create {
            options.create(true);
        }
        #[cfg(target_os = "linux")]
        if direct {
            options.custom_flags(libc::O_DIRECT | libc::O_NOATIME);
        }

        match options.open(db_path) {
            Ok(file) => {
                #[cfg(target_os = "linux")]
                {
                    // direct indicates whether the opened handle actually uses O_DIRECT.
                    return Ok((file, direct));
                }
                #[cfg(not(target_os = "linux"))]
                {
                    return Ok((file, false));
                }
            }
            Err(err) => {
                #[cfg(target_os = "linux")]
                {
                    // Some filesystems (tmpfs, overlays) reject O_DIRECT with EINVAL. In that
                    // case we retry without custom flags instead of failing the entire startup.
                    if direct && err.raw_os_error() == Some(libc::EINVAL) {
                        warn!(
                            "O_DIRECT unsupported for {:?}, falling back to buffered I/O",
                            db_path
                        );
                        return Self::open_raw_file(db_path, create, false);
                    }
                }
                #[cfg(target_os = "linux")]
                warn!(
                    "O_DIRECT unavailable ({}), falling back to buffered I/O",
                    err
                );
                let mut fallback = std::fs::OpenOptions::new();
                fallback.read(true).write(true);
                if create {
                    fallback.create(true);
                }
                fallback.open(db_path).map(|f| (f, false))
            }
        }
    }

    pub fn try_new(db_path: impl AsRef<Path>) -> QuillSQLResult<Self> {
        let mut is_new_file = false;
        let db_path_ref = db_path.as_ref();
        let (db_file, meta, direct_enabled) = if db_path_ref.exists() {
            let (mut db_file, direct_ok) = Self::open_raw_file(db_path_ref, false, true)?;
            let mut buf = vec![0; *META_PAGE_SIZE];
            match db_file.read_exact(&mut buf) {
                Ok(()) => {
                    let (meta_page, _) = decode_meta_page(&buf)?;
                    (db_file, meta_page, direct_ok)
                }
                Err(err) => {
                    if let Some(raw) = err.raw_os_error() {
                        if raw == libc::EINVAL {
                            // Some platforms allow opening with O_DIRECT but fail when we actually
                            // perform unaligned I/O (e.g. tmpfs). Retry the read with a buffered
                            // handle so temp DBs can still function.
                            let (mut fallback_file, _) =
                                Self::open_raw_file(db_path_ref, false, false)?;
                            fallback_file.read_exact(&mut buf)?;
                            let (meta_page, _) = decode_meta_page(&buf)?;
                            (fallback_file, meta_page, false)
                        } else {
                            return Err(err.into());
                        }
                    } else {
                        return Err(err.into());
                    }
                }
            }
        } else {
            is_new_file = true;
            let (mut db_file, mut direct_ok) = Self::open_raw_file(db_path_ref, true, true)?;
            let meta_page = MetaPage::try_new()?;
            let meta_bytes = encode_meta_page(&meta_page);

            if direct_ok {
                let mut aligned = AlignedPageBuf::new_zeroed()?;
                aligned.as_mut_slice().copy_from_slice(&meta_bytes);

                if let Err(err) = db_file.write_all(aligned.as_slice()) {
                    let mut need_fallback = err.kind() == ErrorKind::InvalidInput;
                    #[cfg(target_os = "linux")]
                    {
                        if !need_fallback && err.raw_os_error() == Some(libc::EINVAL) {
                            need_fallback = true;
                        }
                    }

                    if need_fallback {
                        let (mut fallback_file, _) =
                            Self::open_raw_file(db_path_ref, false, false)?;
                        fallback_file.write_all(&meta_bytes)?;
                        db_file = fallback_file;
                        direct_ok = false;
                    } else {
                        return Err(err.into());
                    }
                }
            } else {
                db_file.write_all(&meta_bytes)?;
            }

            (db_file, meta_page, direct_ok)
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

        #[cfg(target_os = "linux")]
        if !direct_enabled {
            warn!("DiskManager running without O_DIRECT; expect OS page cache usage");
        }

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
        if page_id == crate::buffer::INVALID_PAGE_ID {
            return Err(QuillSQLError::Storage(
                "read_page: invalid page id".to_string(),
            ));
        }
        let mut guard = self.db_file.lock().unwrap();
        let mut aligned = AlignedPageBuf::new_zeroed()?;

        guard.seek(SeekFrom::Start(
            (*META_PAGE_SIZE + (page_id - 1) as usize * PAGE_SIZE) as u64,
        ))?;
        guard.read_exact(aligned.as_mut_slice())?;

        let mut page = [0u8; PAGE_SIZE];
        page.copy_from_slice(aligned.as_slice());
        Ok(page)
    }

    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> QuillSQLResult<()> {
        if page_id == crate::buffer::INVALID_PAGE_ID {
            return Err(QuillSQLError::Storage(
                "write_page: invalid page id".to_string(),
            ));
        }
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
        if page_id == crate::buffer::INVALID_PAGE_ID {
            return Err(QuillSQLError::Storage(
                "deallocate_page: invalid page id".to_string(),
            ));
        }
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
        let encoded = encode_meta_page(&self.meta.read().unwrap());
        guard.write_all(&encoded)?;
        guard.flush()?;
        Ok(())
    }

    fn write_page_internal(
        guard: &mut MutexGuard<File>,
        page_id: PageId,
        data: &[u8],
    ) -> QuillSQLResult<()> {
        // Seek to the start of the page in the database file and write the data.
        guard.seek(SeekFrom::Start(
            (*META_PAGE_SIZE + (page_id - 1) as usize * PAGE_SIZE) as u64,
        ))?;

        if data.as_ptr() as usize % PAGE_SIZE == 0 {
            guard.write_all(data)?;
        } else {
            let mut aligned = AlignedPageBuf::new_zeroed()?;
            aligned.as_mut_slice().copy_from_slice(data);
            guard.write_all(aligned.as_slice())?;
        }
        guard.flush()?;
        Ok(())
    }

    pub fn db_file_len(&self) -> QuillSQLResult<u64> {
        let guard = self.db_file.lock().unwrap();
        let meta = guard.metadata()?;
        Ok(meta.len())
    }

    #[cfg(target_os = "linux")]
    pub fn try_clone_db_file(&self) -> QuillSQLResult<File> {
        let guard = self.db_file.lock().unwrap();
        let cloned = guard.try_clone()?;
        Ok(cloned)
    }

    /// Linux only: page read without taking the global file mutex, using positional I/O on a cloned fd.
    #[cfg(target_os = "linux")]
    pub fn read_page_at_unlocked(
        &self,
        file: &File,
        page_id: PageId,
    ) -> QuillSQLResult<[u8; PAGE_SIZE]> {
        use std::os::unix::fs::FileExt;
        if page_id == crate::buffer::INVALID_PAGE_ID {
            return Err(QuillSQLError::Storage(
                "read_page: invalid page id".to_string(),
            ));
        }
        let mut buf = [0u8; PAGE_SIZE];
        let offset = (*META_PAGE_SIZE + (page_id - 1) as usize * PAGE_SIZE) as u64;
        file.read_at(&mut buf, offset)?;
        Ok(buf)
    }

    /// Linux only: page write without taking the global file mutex, using positional I/O on a cloned fd.
    #[cfg(target_os = "linux")]
    pub fn write_page_at_unlocked(
        &self,
        file: &File,
        page_id: PageId,
        data: &[u8],
    ) -> QuillSQLResult<()> {
        use std::os::unix::fs::FileExt;
        if page_id == crate::buffer::INVALID_PAGE_ID {
            return Err(QuillSQLError::Storage(
                "write_page: invalid page id".to_string(),
            ));
        }
        if data.len() != PAGE_SIZE {
            return Err(QuillSQLError::Internal(format!(
                "Page size is not {}",
                PAGE_SIZE
            )));
        }
        let offset = (*META_PAGE_SIZE + (page_id - 1) as usize * PAGE_SIZE) as u64;
        file.write_at(data, offset)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::PAGE_SIZE;
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
        assert_eq!(db_file_len as usize, PAGE_SIZE * 7 + PAGE_SIZE);
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
