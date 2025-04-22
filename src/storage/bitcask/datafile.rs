use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use memmap2::MmapOptions;

use serde::{Deserialize, Serialize};

use crate::storage::bitcask::REMOVE_TOMBSTONE;
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct DataFileMetadata {
    pub id: u128,
    pub path: std::path::PathBuf,
}

/// CleanFile is a wrapper for File which deletes the file on close
/// if the file's size is 0:
#[derive(Debug)]
struct CleanFile {
    file: Option<std::fs::File>,
    path: std::path::PathBuf,
}

impl std::ops::Deref for CleanFile {
    type Target = std::fs::File;

    fn deref(&self) -> &Self::Target {
        self.file.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for CleanFile {
    fn deref_mut(&mut self) -> &mut std::fs::File {
        self.file.as_mut().unwrap()
    }
}

impl Drop for CleanFile {
    fn drop(&mut self) {
        self.file.take();

        let path = &self.path.as_path();
        let file_metadata = std::fs::metadata(path);
        if let Ok(metadata) = file_metadata {
            if metadata.len() == 0 {
                log::trace!(
                    "Datafile.drop: removing file since its empty {}",
                    path.display()
                );
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

#[derive(Debug)]
pub struct DataFile {
    pub id: u128,
    pub is_readonly: bool,

    file: CleanFile,
    pub path: std::path::PathBuf,
}

impl DataFile {
    pub fn create(path: &std::path::Path, is_readonly: bool) -> Result<DataFile> {
        let datafile = if is_readonly {
            OpenOptions::new().read(true).open(&path)?
        } else {
            OpenOptions::new()
                .read(true)
                .append(true)
                .write(true)
                .create(true)
                .open(&path)?
        };

        let id = crate::utils::util::extract_id_from_filename(&path.to_path_buf())?;

        let df = DataFile {
            id,
            file: CleanFile {
                file: Some(datafile),
                path: path.to_path_buf(),
            },
            is_readonly,
            path: path.to_path_buf(),
        };

        Ok(df)
    }

    pub fn get_id(&self) -> u128 {
        self.id
    }

    pub fn write(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> Result<u64> {
        let entry = Entry {
            timestamp,
            key: key.to_vec(),
            value: value.to_vec(),
        };
        use std::io::Write as _;

        let offset = self.file.seek(SeekFrom::Current(0))?;
        // serialize_into is vastly slower than serializing to avec then doing 1 big write
        let encoded: Vec<u8> = bincode::serialize(&entry)?;
        self.file.write_all(&encoded)?;
        Ok(offset)
    }

    pub fn remove(&mut self, key: &[u8], timestamp: u128) -> Result<u64> {
        self.write(key, REMOVE_TOMBSTONE, timestamp)
    }

    pub fn read(&mut self, offset: u64) -> Result<Entry> {
        let mmap = unsafe { MmapOptions::new().map(&*self.file)? };
        let decoded: Entry = bincode::deserialize(&mmap[(offset as usize)..])?;
        Ok(decoded)
    }

    pub fn iter(&mut self) -> DataFileIterator {
        let file = std::fs::File::open(&self.path).unwrap();

        DataFileIterator { file }
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all().map_err(Into::into)
    }

    pub fn inspect(&mut self, with_header: bool) -> String {
        let mut list = String::new();

        if with_header {
            list.push_str(format!("Datafile {}:\n", self.id).as_str());
        }

        for (offset, entry) in self.iter() {
            let mut op = "S"; // Set

            if entry.value == crate::storage::bitcask::REMOVE_TOMBSTONE {
                op = "D" // Delete
            }

            let line = format!(
                "{:0>8} | {: >1} | {} | {}\n",
                offset,
                op,
                std::str::from_utf8(&entry.key).unwrap(),
                std::str::from_utf8(&entry.value).unwrap()
            );
            list.push_str(&line);
        }

        list.trim_end().to_string()
    }
}

pub struct DataFileIterator {
    file: std::fs::File,
}

impl Iterator for DataFileIterator {
    type Item = (u64, Entry);

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.file.seek(SeekFrom::Current(0)).unwrap();
        let decoded_maybe = bincode::deserialize_from(&self.file);
        Some((offset, decoded_maybe.ok()?))
    }
}

impl Drop for DataFile {
    fn drop(&mut self) {
        self.file.sync_all().unwrap_or_default();
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Entry {
    // TODO: crc: impl later
    pub timestamp: u128,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}