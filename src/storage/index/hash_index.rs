use crate::buffer::{BufferManager, PageId, WritePageGuard};
use crate::catalog::SchemaRef;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::index::Index;
use crate::storage::page::hash_page::HashBucketPage;
use crate::storage::page::RecordId;
use crate::storage::tuple::Tuple;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::RwLock;

/// A minimal extendible-hash-like index skeleton backed by bucket pages.
/// Directory is implicit (power-of-two split), for parity with BusTub style.
#[derive(Debug)]
pub struct HashIndex {
    pub key_schema: SchemaRef,
    pub buffer_pool: Arc<BufferManager>,
    pub bucket_max_size: u32,
    pub dir_depth: RwLock<u32>,
    pub directory: RwLock<Vec<PageId>>,
}

impl HashIndex {
    pub fn new(
        key_schema: SchemaRef,
        buffer_pool: Arc<BufferManager>,
        bucket_max_size: u32,
    ) -> Self {
        // create one initial bucket
        let initial_bucket_id = {
            let mut guard = buffer_pool.new_page().expect("alloc bucket");
            let page = HashBucketPage::new(key_schema.clone(), bucket_max_size, 0);
            let bytes = crate::storage::codec::HashBucketPageCodec::encode(&page);
            guard.data_mut().copy_from_slice(&bytes);
            guard.page_id()
        };
        Self {
            key_schema,
            buffer_pool,
            bucket_max_size,
            dir_depth: RwLock::new(0),
            directory: RwLock::new(vec![initial_bucket_id]),
        }
    }

    fn bucket_index(&self, key: &Tuple) -> usize {
        // Production-grade stable hashing: SipHash13 over encoded tuple bytes
        let mut hasher = DefaultHasher::new();
        let bytes = crate::storage::codec::TupleCodec::encode(key);
        hasher.write(&bytes);
        let h = hasher.finish();
        let depth = *self.dir_depth.read().unwrap();
        let dir = self.directory.read().unwrap();
        if depth == 0 {
            return 0;
        }
        (h as usize) & ((1usize << depth) - 1).min(dir.len().saturating_sub(1))
    }

    fn load_bucket(&self, page_id: PageId) -> QuillSQLResult<(WritePageGuard, HashBucketPage)> {
        let guard = self.buffer_pool.fetch_page_write(page_id)?;
        let (bucket, _) = crate::storage::codec::HashBucketPageCodec::decode(
            guard.data(),
            self.key_schema.clone(),
        )?;
        Ok((guard, bucket))
    }

    fn write_bucket(&self, mut guard: WritePageGuard, bucket: &HashBucketPage) {
        let bytes = crate::storage::codec::HashBucketPageCodec::encode(bucket);
        guard.data_mut().copy_from_slice(&bytes);
    }

    fn split_bucket(
        &self,
        _bucket_idx: usize,
        page_id: PageId,
        old_bucket: &mut HashBucketPage,
    ) -> QuillSQLResult<()> {
        let old_local = old_bucket.header.local_depth;
        // 1) allocate new page WITHOUT holding directory locks
        let new_guard = self.buffer_pool.new_page()?;
        let new_pid = new_guard.page_id();

        // 2) maybe double directory, and collect indices to redirect to new_pid
        let (depth_now, indices_to_new): (u32, Vec<usize>) = {
            let mut depth_guard = self.dir_depth.write().unwrap();
            let dir_len;
            {
                let mut dir_guard = self.directory.write().unwrap();
                if old_local == *depth_guard {
                    let clone = dir_guard.clone();
                    dir_guard.extend_from_slice(&clone);
                    *depth_guard += 1;
                }
                dir_len = dir_guard.len();
            }
            let depth_now = *depth_guard;
            drop(depth_guard);
            // compute indices needing redirection
            let dir_read = self.directory.read().unwrap();
            let mut indices = Vec::new();
            for i in 0..dir_len {
                if dir_read[i] == page_id {
                    if ((i >> old_local) & 1) == 1 {
                        indices.push(i);
                    }
                }
            }
            (depth_now, indices)
        };

        // 3) build new bucket contents and update old bucket (no dir locks)
        let mut new_bucket =
            HashBucketPage::new(self.key_schema.clone(), self.bucket_max_size, old_local + 1);
        old_bucket.header.local_depth = old_local + 1;

        let mut stay = Vec::new();
        let mut move_vec = Vec::new();
        for (k, v) in old_bucket.array.drain(..) {
            let mut hasher = DefaultHasher::new();
            let enc = crate::storage::codec::TupleCodec::encode(&k);
            hasher.write(&enc);
            let idx = (hasher.finish() as usize) & ((1usize << depth_now) - 1);
            if ((idx >> old_local) & 1) == 1 {
                move_vec.push((k, v));
            } else {
                stay.push((k, v));
            }
        }
        old_bucket.array = stay;
        old_bucket.header.current_size = old_bucket.array.len() as u32;
        new_bucket.array = move_vec;
        new_bucket.header.current_size = new_bucket.array.len() as u32;

        // 4) write back both buckets (no dir locks)
        self.write_bucket(self.buffer_pool.fetch_page_write(page_id)?, old_bucket);
        self.write_bucket(new_guard, &new_bucket);

        // 5) apply directory redirections QUICKLY
        {
            let mut dir_guard = self.directory.write().unwrap();
            for i in indices_to_new {
                dir_guard[i] = new_pid;
            }
        }
        Ok(())
    }

    fn find_dir_index_of_pid(&self, pid: PageId) -> Option<usize> {
        let dir = self.directory.read().unwrap();
        dir.iter().position(|p| *p == pid)
    }

    fn try_shrink_directory(&self) -> QuillSQLResult<()> {
        let dir = self.directory.read().unwrap();
        let mut seen = std::collections::HashSet::new();
        let mut max_local = 0u32;
        for &pid in dir.iter() {
            if seen.insert(pid) {
                let (_g, b) = self.load_bucket(pid)?;
                if b.header.local_depth > max_local {
                    max_local = b.header.local_depth;
                }
            }
        }
        drop(dir);
        let mut depth_guard = self.dir_depth.write().unwrap();
        if *depth_guard == 0 {
            return Ok(());
        }
        if max_local < *depth_guard {
            let mut dir_guard = self.directory.write().unwrap();
            let new_len = dir_guard.len() / 2;
            dir_guard.truncate(new_len);
            *depth_guard -= 1;
        }
        Ok(())
    }

    fn try_merge_bucket(&self, page_id: PageId) -> QuillSQLResult<()> {
        // snapshot directory to find some_idx and buddy
        let some_idx = if let Some(i) = self.find_dir_index_of_pid(page_id) {
            i
        } else {
            return Ok(());
        };
        // read local depth via IO (no dir locks)
        let (_tg, tb) = self.load_bucket(page_id)?;
        let local = tb.header.local_depth;
        if local == 0 {
            return Ok(());
        }
        // compute buddy and survivor pids using short directory read locks
        let buddy_pid = {
            *self
                .directory
                .read()
                .unwrap()
                .get(some_idx ^ (1usize << (local as usize - 1)))
                .unwrap()
        };
        if buddy_pid == page_id {
            return Ok(());
        }
        let survivor_pid = {
            *self
                .directory
                .read()
                .unwrap()
                .get(some_idx & !(1usize << (local as usize - 1)))
                .unwrap()
        };
        let other_pid = if survivor_pid == page_id {
            buddy_pid
        } else {
            page_id
        };

        // load buckets (IO)
        let (_bg, buddy_bucket) = self.load_bucket(buddy_pid)?;
        if buddy_bucket.header.local_depth != local {
            return Ok(());
        }
        let (_tg2, target_bucket) = self.load_bucket(page_id)?;
        if (target_bucket.header.current_size + buddy_bucket.header.current_size)
            > self.bucket_max_size
        {
            return Ok(());
        }

        // merge into survivor
        let (s_guard, mut s_bucket) = self.load_bucket(survivor_pid)?;
        let (_o_guard, o_bucket) = self.load_bucket(other_pid)?;
        s_bucket.array.extend(o_bucket.array.into_iter());
        s_bucket.header.current_size = s_bucket.array.len() as u32;
        s_bucket.header.local_depth = local - 1;
        self.write_bucket(s_guard, &s_bucket);

        // short critical section: redirect directory entries of other_pid
        {
            let mut dir_guard = self.directory.write().unwrap();
            for i in 0..dir_guard.len() {
                if dir_guard[i] == other_pid {
                    dir_guard[i] = survivor_pid;
                }
            }
        }
        let _ = self.buffer_pool.delete_page(other_pid);
        self.try_shrink_directory()?;
        Ok(())
    }
}

impl Index for HashIndex {
    fn insert(&self, key: &Tuple, value: RecordId) -> QuillSQLResult<()> {
        if self.directory.read().unwrap().is_empty() {
            return Err(QuillSQLError::Internal("hash directory empty".to_string()));
        }
        // Non-recursive retry loop to avoid potential infinite recursion during split
        for _ in 0..64 {
            let idx = self.bucket_index(key);
            let page_id = { *self.directory.read().unwrap().get(idx).unwrap() };
            let (guard, mut bucket) = self.load_bucket(page_id)?;

            if let Some(pos) = bucket.array.iter().position(|(k, _)| k == key) {
                bucket.array[pos].1 = value;
                self.write_bucket(guard, &bucket);
                return Ok(());
            }

            if bucket.is_full() {
                // drop guard before split to avoid holding page lock across new_page/IO
                drop(guard);
                self.split_bucket(idx, page_id, &mut bucket)?;
                // continue to retry with updated directory/buckets
                continue;
            }

            bucket.array.push((key.clone(), value));
            bucket.header.current_size += 1;
            self.write_bucket(guard, &bucket);
            return Ok(());
        }
        Err(QuillSQLError::Internal(
            "hash insert retry exceeded".to_string(),
        ))
    }

    fn get(&self, key: &Tuple) -> QuillSQLResult<Option<RecordId>> {
        if self.directory.read().unwrap().is_empty() {
            return Ok(None);
        }
        // Weakly consistent loop in case directory changes during split
        for _ in 0..8 {
            let idx = self.bucket_index(key);
            let page_id = { *self.directory.read().unwrap().get(idx).unwrap() };
            let (_guard, bucket) = self.load_bucket(page_id)?;
            if let Some((_, v)) = bucket.array.iter().find(|(k, _)| k == key) {
                return Ok(Some(*v));
            }
        }
        Ok(None)
    }

    fn delete(&self, key: &Tuple) -> QuillSQLResult<()> {
        if self.directory.read().unwrap().is_empty() {
            return Ok(());
        }
        // Weakly consistent retry to survive concurrent split/redirects
        let mut last_pid: Option<PageId> = None;
        for _ in 0..8 {
            let idx = self.bucket_index(key);
            let page_id = { *self.directory.read().unwrap().get(idx).unwrap() };
            last_pid = Some(page_id);
            let (guard, mut bucket) = self.load_bucket(page_id)?;
            if let Some(pos) = bucket.array.iter().position(|(k, _)| k == key) {
                bucket.array.remove(pos);
                bucket.header.current_size -= 1;
                self.write_bucket(guard, &bucket);
                break;
            } else {
                drop(guard);
                continue;
            }
        }
        // attempt merge & shrink (best effort)
        let _ = (|| -> QuillSQLResult<()> {
            if let Some(pid) = last_pid {
                self.try_merge_bucket(pid)?;
            }
            Ok(())
        })();
        Ok(())
    }

    fn key_schema(&self) -> &SchemaRef {
        &self.key_schema
    }
}

#[cfg(test)]
mod tests {
    use super::HashIndex;
    use crate::buffer::BufferManager;
    use crate::catalog::{Column, DataType, Schema};
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;
    use crate::storage::index::Index;
    use crate::storage::page::RecordId;
    use crate::storage::tuple::Tuple;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn basic_hash_index() {
        let key_schema = Arc::new(Schema::new(vec![Column::new("a", DataType::Int32, false)]));
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");
        let disk_manager = Arc::new(DiskManager::try_new(temp_path).unwrap());
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let buffer_pool = Arc::new(BufferManager::new(512, disk_scheduler));
        let index = HashIndex::new(key_schema.clone(), buffer_pool, 8);

        // insert 5 keys
        for i in 1..=5 {
            let t = Tuple::new(key_schema.clone(), vec![(i as i32).into()]);
            index.insert(&t, RecordId::new(i, i)).unwrap();
        }
        // get all
        for i in 1..=5 {
            let t = Tuple::new(key_schema.clone(), vec![(i as i32).into()]);
            let rid = index.get(&t).unwrap().unwrap();
            assert_eq!(rid.page_id, i);
        }
        // get non-exist
        let t0 = Tuple::new(key_schema.clone(), vec![0i32.into()]);
        assert!(index.get(&t0).unwrap().is_none());

        // update existing key
        let t5 = Tuple::new(key_schema.clone(), vec![5i32.into()]);
        index.insert(&t5, RecordId::new(999, 999)).unwrap();
        assert_eq!(index.get(&t5).unwrap().unwrap().page_id, 999);

        // delete
        index.delete(&t5).unwrap();
        assert!(index.get(&t5).unwrap().is_none());
    }

    #[test]
    #[ignore]
    fn split_and_directory_growth() {
        let key_schema = Arc::new(Schema::new(vec![Column::new("a", DataType::Int32, false)]));
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");
        let disk_manager = Arc::new(DiskManager::try_new(temp_path).unwrap());
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let buffer_pool = Arc::new(BufferManager::new(128, disk_scheduler));
        // small bucket size to trigger splits quickly
        let index = HashIndex::new(key_schema.clone(), buffer_pool.clone(), 2);

        // Insert enough keys to trigger multiple splits
        let total = 50;
        for i in 0..total {
            let t = Tuple::new(key_schema.clone(), vec![(i as i32).into()]);
            index.insert(&t, RecordId::new(i, i)).unwrap();
        }

        // All keys retrievable
        for i in 0..total {
            let t = Tuple::new(key_schema.clone(), vec![(i as i32).into()]);
            let rid = index.get(&t).unwrap().unwrap();
            assert_eq!(rid.page_id, i);
        }

        // Directory depth and size relation
        let depth = *index.dir_depth.read().unwrap();
        let dir = index.directory.read().unwrap();
        assert!(dir.len() >= 2); // should have grown
        assert!(dir.len().is_power_of_two());
        assert_eq!(dir.len(), 1usize << depth);

        // Buckets are not overfull and local_depth <= dir_depth
        for &pid in dir.iter() {
            let (guard, bucket) = index.load_bucket(pid).unwrap();
            let _ = &guard; // keep guard alive
            assert!(bucket.header.current_size <= bucket.header.max_size);
            assert!(bucket.header.local_depth <= depth);
        }

        // Delete some keys and ensure removal
        for i in (0..total).step_by(3) {
            let t = Tuple::new(key_schema.clone(), vec![(i as i32).into()]);
            index.delete(&t).unwrap();
            assert!(index.get(&t).unwrap().is_none());
        }
    }
}
