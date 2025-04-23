use log::*;

use std::fs::create_dir_all;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use glob::glob;
use rayon::prelude::*;

use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::bitcask::cache::BitcaskCache;
use crate::storage::bitcask::datafile::DataFile;
use crate::storage::bitcask::datafile::DataFileMetadata;
use crate::storage::bitcask::indexfile::IndexFile;
use crate::storage::bitcask::keydir::KeyDir;
use crate::storage::bitcask::keydir::KeyDirEntry;
use crate::storage::bitcask::{data_file_format, DATA_FILE_GLOB_FORMAT, REMOVE_TOMBSTONE};
use crate::utils::util;

#[derive(Clone, Debug)]
pub struct Options {
    pub base_dir: std::path::PathBuf,
    pub data_file_limit: u64,
}

pub struct Bitcask {
    options: Options,

    keydir: KeyDir,

    // active file:
    current_data_file: DataFile,

    // datafiles
    data_files: Vec<DataFileMetadata>,
    data_files_cache: BitcaskCache<u128, DataFile>,

    // once the active DataFile has reached the threshold
    // defined in data_file_limit, it will open a new data_file:
    data_file_limit: u64,
}

pub fn new(options: Options) -> QuillSQLResult<Bitcask> {
    // best effort:
    let _ = env_logger::try_init();

    create_dir_all(&options.base_dir).map_err(|source| {
        QuillSQLError::Storage(format!("Failed to create database directory: {}", source))
    })?;

    let created_dir = create_dir_all(&options.base_dir);
    if let Err(err_msg) = created_dir {
        return Err(QuillSQLError::Storage(format!(
            "Failed to create '{}': {}",
            options.base_dir.display(),
            err_msg
        )));
    }

    let path = std::path::Path::new(&options.base_dir);

    let filename = data_file_format(util::time());
    let data_file = DataFile::create(&path.join(filename), false)?;

    let mut db = Bitcask {
        options: options.clone(),
        keydir: KeyDir::new(),
        current_data_file: data_file,
        data_files: Vec::new(),
        data_files_cache: BitcaskCache::new(128),
        data_file_limit: options.data_file_limit,
    };

    db.startup(&path)?;

    Ok(db)
}

pub struct Stats {
    pub num_immutable_datafiles: u64,
    pub num_keys: u64,
}

impl Bitcask {
    pub fn stats(&self) -> Stats {
        trace!("Stats called number of data files: {:?}", self.data_files);

        Stats {
            num_immutable_datafiles: (self.data_files.len() as u64),
            num_keys: (self.keydir.iter().count() as u64),
        }
    }

    // Startup Jobs:
    pub fn startup(&mut self, base_dir: &Path) -> QuillSQLResult<()> {
        let mut data_files_sorted = self.get_data_files_except_current(&base_dir)?;

        self.build_keydir(&mut data_files_sorted)
            .map_err(|source| QuillSQLError::Internal(format!("Failed to build keydir: {}", source)))?;

        self.cleanup()?;
        //self.merge()?;

        Ok(())
    }

    /// call merge to reclaim some disk space
    pub fn merge(&mut self) -> QuillSQLResult<()> {
        let base_dir = &self.options.base_dir;

        let data_files: Vec<PathBuf> = self
            .get_data_files_except_current(base_dir)?
            .iter()
            .rev()
            .cloned()
            .collect();
        trace!(
            "merge: found Data Files before merge operation: {:?}",
            data_files
        );

        if data_files.len() < 2 {
            // Nothing to merge, it does not make sense
            return Ok(());
        }

        // first removing all the startup indices:
        let indices_paths = self.glob_files(&base_dir, "index.*")?;
        for index_path in indices_paths {
            let _ = std::fs::remove_file(&index_path);
        }

        let now = util::time();
        let merged_path = base_dir.join(format!("merge.{}", now));
        let mut temp_datastore = DataFile::create(&merged_path, false)?;

        let index_path = self.options.base_dir.join(format!("index.{}", now));
        let mut index = IndexFile::create(&index_path, false)?;

        let keydir = &self.keydir;

        let mut num_entries_written = 0;
        for (key, entry) in keydir.iter() {
            let value = self.read(&key)?;

            // Keys that are in the 'mutable' datafile don't need to be
            // written again, as it is just wasting time:
            if self.current_data_file.id == entry.file_id {
                continue;
            }

            let new_offset = temp_datastore.write(key, &value, entry.timestamp)?;
            index.write(key, now, new_offset, entry.timestamp)?;

            num_entries_written += 1;
        }

        drop(temp_datastore);
        drop(index);

        if num_entries_written == 0 {
            // The data_files only contains duplicate entries, which already exists in the
            // "main keyfile" therefore lets delete these duplicates/old entries:
            for path in data_files {
                std::fs::remove_file(path)?;
            }

            self.data_files = Vec::new();
            return Ok(());
        }

        let new_datafile_path = &base_dir.join(data_file_format(now));
        trace!(
            "trying to rename data file '{}' to '{}'",
            merged_path.display(),
            new_datafile_path.display()
        );
        std::fs::rename(&merged_path, new_datafile_path)?;

        // glob all data files except for the ones we have merged. We cannot delete them yet because the keydir is not rebuilt yet:
        let mut new_data_files: Vec<PathBuf> = self
            .glob_files(&base_dir, DATA_FILE_GLOB_FORMAT)?
            .iter()
            .cloned()
            .filter(|item| !data_files.contains(&item))
            .collect();

        self.build_keydir(&mut new_data_files)?;

        for path in data_files {
            std::fs::remove_file(path)?;
        }

        Ok(())
    }

    fn get_data_files_except_current(&self, base_dir: &Path) -> QuillSQLResult<Vec<PathBuf>> {
        let mut entries = self.glob_files(&base_dir, DATA_FILE_GLOB_FORMAT)?;

        entries.sort_by(|a, b| natord::compare(a.to_str().unwrap(), b.to_str().unwrap()));

        // Remove current data file since the current data file is mutable:
        entries.retain(|x| {
            x.file_name().unwrap().to_str().unwrap()
                != self
                    .current_data_file
                    .path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
        });

        Ok(entries)
    }

    fn glob_files(&self, base_dir: &Path, pattern: &'static str) -> QuillSQLResult<Vec<PathBuf>> {
        let glob_path = base_dir.join(pattern);
        let glob_path_str = glob_path
            .to_str()
            .ok_or_else(|| QuillSQLError::Internal(format!("Invalid path: {}", glob_path.display())))?;
        let glob_result =
            glob(glob_path_str).map_err(|e| QuillSQLError::Internal(format!("Glob error: {}", e)))?;
        let mut entries: Vec<PathBuf> = glob_result.filter_map(|entry| entry.ok()).collect();
        entries.sort_by(|a, b| {
            let a_str = a.to_str().unwrap_or("");
            let b_str = b.to_str().unwrap_or("");
            natord::compare(a_str, b_str)
        });
        Ok(entries)
    }

    fn build_keydir(&mut self, datafiles_paths: &mut Vec<PathBuf>) -> QuillSQLResult<()> {
        trace!(
            "rebuilding keydir now based on the files: {:?}",
            datafiles_paths
        );

        let base_dir = self.options.base_dir.to_owned();

        // take ownership of these
        let keydir = Arc::new(Mutex::new(KeyDir::new()));
        let data_files = Arc::new(Mutex::new(Vec::new()));

        trace!("Database.build_keydir: Starting to rebuild keydir now...");
        datafiles_paths.par_iter_mut().for_each({
            let keydir = Arc::clone(&keydir);
            let data_files = Arc::clone(&data_files);

            move |entry| {
                let mut counter = 0;

                let file_id = util::extract_id_from_filename(&entry).unwrap();

                let index_path = base_dir.join(format!("index.{}", file_id));
                trace!("Database.build_keydir: check if index exist '{}'", index_path.display());

                if index_path.exists() {
                    trace!("Database.build_keydir: index found 'index.{}'. Importing data file No={} Path={} ...", file_id, file_id, &entry.display());

                    let mut index = IndexFile::create(&index_path, true).unwrap();

                    for (_, entry) in index.iter() {
                        {
                            let mut keydir = keydir.lock().unwrap();
                            let set_result = keydir.set(&entry.key, entry.file_id, entry.offset, entry.timestamp);
                            if let Err(err) = set_result {
                                trace!("Setting value into keydir has failed: {}", err);
                            }
                        }

                        counter += 1;
                    }

                    trace!("Database.build_keydir: index 'index.{}' fully read. Imported index file No={} Path={} NumRecords={}", file_id, file_id, entry.display(), counter);

                } else {
                    trace!("Database.build_keydir: start loading datafile No={} Path={} NumRecords={}", file_id, entry.display(), counter);
                    let mut df = DataFile::create(&entry, true).unwrap();

                    for (offset, record) in df.iter() {
                        let mut keydir = keydir.lock().unwrap();

                        if record.value == REMOVE_TOMBSTONE {
                            trace!("Database.build_keydir: loading datafile No={} Path={} NumRecords={}: Removing key", file_id, entry.display(), counter);
                            keydir.remove(&record.key).unwrap_or_default();
                            continue;
                        }

                        {
                            let maybe_current_entry = keydir.get(&record.key);

                            if let Ok(current_entry) = maybe_current_entry {
                                if record.timestamp > current_entry.timestamp {
                                    keydir.set(record.key.as_slice(), file_id, offset, record.timestamp).unwrap();
                                }
                            } else {
                                keydir.set(record.key.as_slice(), file_id, offset, record.timestamp).unwrap();
                            }
                        }

                        counter += 1;
                    }

                    trace!("Database.build_keydir: loading datafile No={} Path={} NumRecords={}", file_id, entry.display(), counter);
                }


                let mut data_files = data_files.lock().unwrap();
                data_files.push(DataFileMetadata {
                    id: file_id,
                    path: entry.to_path_buf(),
                })
            }
        });

        trace!("Database.build_keydir: Finished rebuilding keydir ...");

        // take ownership of the arc wrapped value
        // this shouldn't fail because rayon blocks until its done
        // try_unwrap gets the value out of the Arc if there aren't any extra refs to it
        let mut data_files = Arc::try_unwrap(data_files)
            .ok() // ignore error value
            // get the value out of the mutex
            // into_inner gets the value out of the mutex if its not locked
            .and_then(|mutex| mutex.into_inner().ok())
            .expect("rayon to finish");

        let keydir = Arc::try_unwrap(keydir)
            .ok() // ignore error value
            // get the value out of the mutex
            .and_then(|mutex| mutex.into_inner().ok())
            .expect("rayon to finish");

        // Removing the current file as the current one is not an immutable data file yet:
        data_files.retain(|df| df.id != self.current_data_file.id);

        trace!(
            "Assigning new data files to internal struct: {:?} => {:?}",
            self.data_files,
            data_files
        );
        self.data_files = data_files;
        self.keydir = keydir;

        self.cleanup()?;

        Ok(())
    }

    fn cleanup(&mut self) -> QuillSQLResult<()> {
        let entries = self.glob_files(&self.options.base_dir, DATA_FILE_GLOB_FORMAT)?;

        for entry in entries {
            let file_id = util::extract_id_from_filename(&entry)?;
            if self.current_data_file.get_id() == file_id {
                continue;
            }

            // cleaning up old files with 0 bytes size:
            let info = std::fs::metadata(&entry)?;
            if info.len() == 0 && std::fs::remove_file(&entry).is_ok() {
                trace!(
                    "... removing {} since it is zero bytes and its not the current data file id (this: {}, current: {})",
                    entry.display(),
                    file_id,
                    self.current_data_file.get_id()
                );
            }
        }

        Ok(())
    }

    fn switch_to_new_data_file(&mut self) -> QuillSQLResult<()> {
        let data_file_id = util::time();

        let new_path =
            std::path::Path::new(&self.options.base_dir).join(data_file_format(data_file_id));

        trace!(
            "Database.switch_to_new_data_file: New data file is {} (file_id={})",
            new_path.display(),
            data_file_id
        );

        let new_data_file = DataFile::create(new_path.as_path(), false)?;
        let old_data_file = std::mem::replace(&mut self.current_data_file, new_data_file);

        let data_file_id = self.current_data_file.get_id();
        trace!(
            "Database.switch_to_new_data_file: Switched data file. Old_Id={} New_Id={}",
            old_data_file.get_id(),
            data_file_id
        );

        self.data_files.push(DataFileMetadata {
            id: old_data_file.id,
            path: std::path::Path::new(&self.options.base_dir)
                .join(data_file_format(old_data_file.id)),
        });

        Ok(())
    }

    pub fn write(&mut self, key: &[u8], value: &[u8]) -> QuillSQLResult<()> {
        let data_file_id = self.current_data_file.get_id();

        let timestamp = util::time();

        let offset = self.current_data_file.write(key, value, timestamp)?;
        self.keydir.set(&key, data_file_id, offset, timestamp)?;

        if offset >= self.data_file_limit {
            trace!(
                "Database.write: Offset threshold reached for data file id '{}', key '{}':  {} < {}. Switching to new data file",
                data_file_id,
                std::str::from_utf8(&key).unwrap(),
                offset,
                self.data_file_limit
            );
            return self.switch_to_new_data_file();
        }

        Ok(())
    }

    pub fn read(&self, key: &[u8]) -> QuillSQLResult<Vec<u8>> {
        let entry = self.keydir.get(key)?;

        let data_filename = data_file_format(entry.file_id);
        let path = std::path::Path::new(&self.options.base_dir).join(data_filename);

        let mut data_file = DataFile::create(&path, true)?;
        trace!(
            "Database.read: Trying to read from offset {} from file {}",
            entry.offset,
            path.display()
        );

        let found_entry = data_file.read(entry.offset)?;
        Ok(found_entry.value)
    }

    pub fn read_cache(&mut self, key: &[u8]) -> QuillSQLResult<Vec<u8>> {
        let entry = self.keydir.get(key)?;

        if let Some(df) = self.data_files_cache.get_mut(&entry.file_id) {
            let found_entry = df.read(entry.offset)?;
            return Ok(found_entry.value);
        }

        let data_filename = data_file_format(entry.file_id);
        let path = std::path::Path::new(&self.options.base_dir).join(data_filename);

        let mut data_file = DataFile::create(&path, true)?;
        trace!(
            "Database.read: Trying to read from offset {} from file {}",
            entry.offset,
            path.display()
        );
        let found_entry = data_file.read(entry.offset)?;
        let _ = self.data_files_cache.put(entry.file_id, data_file);
        Ok(found_entry.value)
    }

    pub fn remove(&mut self, key: &[u8]) -> QuillSQLResult<()> {
        let timestamp = util::time();
        self.current_data_file.remove(key, timestamp)?;
        self.keydir.remove(key)
    }

    // get_datafile_at should only be used for debugging:
    pub fn get_datafile_at(&mut self, index: u32) -> DataFile {
        let df = self.data_files.get_mut(index as usize).unwrap();
        DataFile::create(&df.path, true).unwrap()
    }

    pub fn get_current_datafile(&mut self) -> DataFile {
        let path = self.current_data_file.path.as_path();
        DataFile::create(&path, true).unwrap()
    }

    pub fn keys(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.keydir.keys()
    }

    pub fn keys_range(
        &self,
        min: &[u8],
        max: &[u8],
    ) -> impl Iterator<Item = (&Vec<u8>, &KeyDirEntry)> {
        self.keydir.keys_range(min, max)
    }

    pub fn keys_range_min(&self, min: &[u8]) -> impl Iterator<Item = (&Vec<u8>, &KeyDirEntry)> {
        self.keydir.keys_range_min(min)
    }

    pub fn keys_range_max(&self, max: &[u8]) -> impl Iterator<Item = (&Vec<u8>, &KeyDirEntry)> {
        self.keydir.keys_range_max(max)
    }

    pub fn sync(&mut self) -> QuillSQLResult<()> {
        self.current_data_file.sync()
    }

    pub fn close(&mut self) -> QuillSQLResult<()> {
        self.sync()
    }

    // 添加迭代器方法
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &KeyDirEntry)> {
        self.keydir.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // 基本读写测试
    #[test]
    fn test_basic_read_write() -> QuillSQLResult<()> {
        let dir = tempdir()?;
        let path = dir.path().to_path_buf();
        let mut bitcask = new(Options {
            base_dir: path.clone(),
            data_file_limit: 1024 * 1024,
        })?;

        // 写入一些数据
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        bitcask.write(&key1, &value1)?;

        // 读取数据
        let result1 = bitcask.read(&key1)?;
        assert_eq!(value1, result1);

        bitcask.close()?;
        Ok(())
    }

    // 缓存读取测试
    #[test]
    fn test_cache_read() -> QuillSQLResult<()> {
        let dir = tempdir()?;
        let path = dir.path().to_path_buf();
        let mut bitcask = new(Options {
            base_dir: path.clone(),
            data_file_limit: 1024 * 1024,
        })?;

        // 写入多条数据
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            bitcask.write(&key, &value)?;
        }

        // 使用缓存读取，应该会缓存DataFile
        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            let expected = format!("value{}", i).into_bytes();
            let value = bitcask.read_cache(&key)?;
            assert_eq!(expected, value);
        }

        // 再次读取，应该从缓存获取
        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            let expected = format!("value{}", i).into_bytes();
            let value = bitcask.read_cache(&key)?;
            assert_eq!(expected, value);
        }

        bitcask.close()?;
        Ok(())
    }

    // 删除测试
    #[test]
    fn test_remove() -> QuillSQLResult<()> {
        let dir = tempdir()?;
        let path = dir.path().to_path_buf();
        let mut bitcask = new(Options {
            base_dir: path.clone(),
            data_file_limit: 1024 * 1024,
        })?;

        // 写入数据
        let key = b"key_to_remove".to_vec();
        let value = b"value".to_vec();
        bitcask.write(&key, &value)?;

        // 确认可以读取
        let result = bitcask.read(&key)?;
        assert_eq!(value, result);

        // 删除数据
        bitcask.remove(&key)?;

        // 尝试读取，应该失败
        let read_result = bitcask.read(&key);
        assert!(read_result.is_err());

        bitcask.close()?;
        Ok(())
    }

    // 数据持久性测试
    #[test]
    fn test_persistence() -> QuillSQLResult<()> {
        let dir = tempdir()?;
        let path = dir.path().to_path_buf();

        // 第一个实例写入数据
        {
            let mut bitcask = new(Options {
                base_dir: path.clone(),
                data_file_limit: 1024 * 1024,
            })?;

            let key = b"persist_key".to_vec();
            let value = b"persist_value".to_vec();
            bitcask.write(&key, &value)?;
            bitcask.sync()?;
            bitcask.close()?;
        }

        // 第二个实例读取数据
        {
            let mut bitcask = new(Options {
                base_dir: path.clone(),
                data_file_limit: 1024 * 1024,
            })?;

            let key = b"persist_key".to_vec();
            let expected = b"persist_value".to_vec();
            let value = bitcask.read(&key)?;
            assert_eq!(expected, value);
            bitcask.close()?;
        }

        Ok(())
    }

    // 大数据测试
    #[test]
    fn test_large_values() -> QuillSQLResult<()> {
        let dir = tempdir()?;
        let path = dir.path().to_path_buf();
        let mut bitcask = new(Options {
            base_dir: path.clone(),
            data_file_limit: 1024 * 1024,
        })?;

        // 写入一个大值
        let key = b"large_key".to_vec();
        let value = vec![42u8; 100 * 1024]; // 100KB
        bitcask.write(&key, &value)?;

        // 读取并验证
        let result = bitcask.read(&key)?;
        assert_eq!(value, result);

        bitcask.close()?;
        Ok(())
    }

    // 缓存替换策略测试
    #[test]
    fn test_cache_replacement() -> QuillSQLResult<()> {
        let dir = tempdir()?;
        let path = dir.path().to_path_buf();

        // 创建一个缓存较小的Bitcask实例，方便触发替换
        let options = Options {
            base_dir: path.clone(),
            data_file_limit: 4096, // 使用较小的文件限制
        };

        let mut bitcask = new(options)?;

        // 写入足够多的数据，触发多个数据文件创建
        for i in 0..20 {
            // 每个写入都是1KB左右，这样应该会创建多个数据文件
            let key = format!("key{}", i).into_bytes();
            let value = vec![i as u8; 1000]; // 1KB左右的数据
            bitcask.write(&key, &value)?;

            // 强制切换到新数据文件
            if i % 4 == 3 {
                bitcask.switch_to_new_data_file()?;
            }
        }

        // 首先访问前5个key
        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            let _ = bitcask.read_cache(&key)?;
        }

        // 然后访问后10个key - 这应该触发缓存替换
        for i in 10..20 {
            let key = format!("key{}", i).into_bytes();
            let expected = vec![i as u8; 1000];
            let value = bitcask.read_cache(&key)?;
            assert_eq!(expected, value);
        }

        // 再次访问前5个key - 应该仍能正确读取
        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            let expected = vec![i as u8; 1000];
            let value = bitcask.read_cache(&key)?;
            assert_eq!(expected, value);
        }

        bitcask.close()?;
        Ok(())
    }


}
