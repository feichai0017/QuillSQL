use crate::error::{Error, Result};
use crate::storage::bptree::page_layout::PAGE_SIZE;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Manages reading and writing pages to the database file on disk.
#[derive(Debug)]
pub struct DiskManager {
    file: File,
    num_pages: usize,
}

impl DiskManager {
    /// Creates a new DiskManager or opens an existing database file.
    pub fn new(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let num_pages = (file_size as usize + PAGE_SIZE - 1) / PAGE_SIZE;
        if file_size > 0 && file_size % PAGE_SIZE as u64 != 0 {
            println!(
                "WARN: DiskManager file size {} is not a multiple of PAGE_SIZE {}. Num pages calculated as {}",
                file_size,
                PAGE_SIZE,
                num_pages
            );
        }

        Ok(DiskManager { file, num_pages })
    }

    /// Reads the contents of the specified page from disk.
    pub fn read_page(&mut self, page_id: usize) -> Result<[u8; PAGE_SIZE]> {
        let offset = (page_id * PAGE_SIZE) as u64;
        let mut page_data: [u8; PAGE_SIZE] = [0u8; PAGE_SIZE];
        let file_len = self.file.metadata()?.len();

        // Ensure we don't read beyond the actual file length,
        // but allow reading the last (potentially partial) page if file size isn't page-aligned.
        if offset >= file_len {
            // If offset is exactly file_len and file_len is page-aligned, it's a new page (return zeros).
            // If offset > file_len, or if file_len is not page-aligned and offset is at the start of the last partial page,
            // it's likely an error or uninitialized space.
            // For simplicity, return zeros if reading at or beyond EOF for now.
            // A stricter implementation might error for offset > file_len.
            println!(
                "WARN: Reading page_id {} at offset {} at/beyond EOF ({}). Returning zeroed page.",
                page_id, offset, file_len
            );
            return Ok(page_data); // Return zeroed page data array
        }

        self.file.seek(SeekFrom::Start(offset))?;
        match self.file.read_exact(&mut page_data) {
            Ok(_) => Ok(page_data),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // This can happen if the file ends exactly at the offset or mid-page.
                // Return the partially read data (or zeros for the unread part).
                eprintln!("WARN: read_exact encountered EOF for page_id {}. Returning potentially partial/zeroed page.", page_id);
                Ok(page_data) // Return potentially partial data array
            }
            Err(e) => Err(Error::Io(e.to_string())),
        }
    }

    /// Writes the contents of the page data to the specified page ID on disk.
    pub fn write_page(&mut self, page_id: usize, page_data: &[u8; PAGE_SIZE]) -> Result<()> {
        let offset = (page_id * PAGE_SIZE) as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(page_data)?;
        // Ensure num_pages reflects the write if it went beyond the previous count
        self.num_pages = std::cmp::max(self.num_pages, page_id + 1);
        Ok(())
    }

    /// Allocates a new page ID by incrementing the internal counter.
    /// Does not perform any disk I/O.
    pub fn allocate_page(&mut self) -> usize {
        let new_page_id = self.num_pages;
        self.num_pages += 1;
        new_page_id
    }

    /// Returns the total number of pages known to the DiskManager.
    pub fn get_num_pages(&self) -> usize {
        self.num_pages
    }
}
