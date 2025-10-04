use parking_lot::{RwLock, RwLockWriteGuard};
use std::collections::VecDeque;
use std::fmt::Write;
use std::sync::Arc;

use crate::buffer::{
    BufferManager, PageId, ReadPageGuard, WritePageGuard, INVALID_PAGE_ID, PAGE_SIZE,
};
use crate::catalog::SchemaRef;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::codec::{
    BPlusTreeHeaderPageCodec, BPlusTreeInternalPageCodec, BPlusTreeLeafPageCodec,
    BPlusTreePageCodec,
};
use crate::storage::page::{BPlusTreeHeaderPage, BPlusTreeInternalPage};
use crate::storage::page::{BPlusTreeLeafPage, BPlusTreePage, RecordId};

use crate::config::BTreeConfig;
use crate::recovery::wal::codec::{PageDeltaPayload, PageWritePayload};
use crate::recovery::wal_record::WalRecordPayload;
use crate::storage::codec::BPlusTreePageTypeCodec;
pub use crate::storage::index::btree_iterator::TreeIndexIterator;
use crate::storage::page::BPlusTreePageType;
use crate::storage::tuple::Tuple;
use crate::utils::util;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

// OLC bounded restart and backoff configuration
const MAX_OLC_RESTARTS: usize = 64;
const OLC_BACKOFF_BASE_US: u64 = 50;

#[derive(Debug)]
pub struct Context<'a> {
    /// Write guards along the path from root to the current node.
    /// When a child node is safe for the operation, all ancestor locks
    /// can be released (clear this set) to reduce lock footprint.
    pub write_set: VecDeque<WritePageGuard>,

    /// Optional read guards used on read paths for validations/restarts.
    pub read_set: VecDeque<ReadPageGuard>,

    /// Holds the header page lock during structural changes (e.g., root switch).
    pub header_lock_guard: Option<RwLockWriteGuard<'a, ()>>,
}

impl<'a> Context<'a> {
    pub fn new() -> Self {
        Self {
            write_set: VecDeque::new(),
            read_set: VecDeque::new(),
            header_lock_guard: None,
        }
    }

    /// Push a write guard onto the traversal path.
    pub fn push_write_guard(&mut self, guard: WritePageGuard) {
        self.write_set.push_back(guard);
    }

    pub fn push_read_guard(&mut self, guard: ReadPageGuard) {
        self.read_set.push_back(guard);
    }

    /// Release all ancestor write locks when it is safe to proceed.
    pub fn release_all_write_locks(&mut self) {
        self.write_set.clear();
        self.header_lock_guard = None;
    }
}

// Production-ready B+ Tree index with OLC readers and latch-crabbing writers.
// - Header page holds immutable identity; root_page_id changes atomically.

#[derive(Debug)]
pub struct BPlusTreeIndex {
    pub key_schema: SchemaRef,
    pub buffer_pool: Arc<BufferManager>,
    pub internal_max_size: u32,
    pub leaf_max_size: u32,
    pub header_page_id: PageId,
    pub header_page_lock: Arc<RwLock<()>>,
    pub config: BTreeConfig,
    /// Best-effort counter of potentially dead index entries observed by readers.
    pub pending_garbage: AtomicUsize,
}


impl BPlusTreeIndex {
    /// Write a modified index page back to the buffer with WAL (FPW/Delta) just like table heap.
    /// This centralizes crash-consistent updates for header/leaf/internal pages.
    fn wal_overwrite_page(
        &self,
        guard: &mut WritePageGuard,
        new_image: Vec<u8>,
    ) -> QuillSQLResult<()> {
        debug_assert_eq!(new_image.len(), PAGE_SIZE);
        let prev_lsn = guard.lsn();
        if let Some(wal) = self.buffer_pool.wal_manager() {
            let page_id = guard.page_id();
            if wal.fpw_first_touch(page_id) {
                let res = wal.append_record_with(|_| {
                    WalRecordPayload::PageWrite(PageWritePayload {
                        page_id,
                        prev_page_lsn: prev_lsn,
                        page_image: new_image.clone(),
                    })
                })?;
                guard.overwrite(&new_image, Some(res.end_lsn));
                return Ok(());
            }

            let old = guard.data();
            if let Some((start, end)) = util::find_contiguous_diff(old, &new_image) {
                let diff_len = end - start;
                let threshold = PAGE_SIZE / 16;
                let res = if diff_len <= threshold {
                    wal.append_record_with(|_| {
                        WalRecordPayload::PageDelta(PageDeltaPayload {
                            page_id,
                            prev_page_lsn: prev_lsn,
                            offset: start as u16,
                            data: new_image[start..end].to_vec(),
                        })
                    })?
                } else {
                    wal.append_record_with(|_| {
                        WalRecordPayload::PageWrite(PageWritePayload {
                            page_id,
                            prev_page_lsn: prev_lsn,
                            page_image: new_image.clone(),
                        })
                    })?
                };
                guard.overwrite(&new_image, Some(res.end_lsn));
            } else {
                // no-op change
            }
        } else {
            guard.overwrite(&new_image, None);
        }
        Ok(())
    }
    pub fn new(
        key_schema: SchemaRef,
        buffer_pool: Arc<BufferManager>,
        internal_max_size: u32,
        leaf_max_size: u32,
    ) -> Self {
        // Create a header page to store the root_page_id
        let mut header_page_guard = buffer_pool
            .new_page()
            .expect("Failed to create header page for B+ tree");
        let header_page_id = header_page_guard.page_id();
        let header_page = BPlusTreeHeaderPage {
            root_page_id: INVALID_PAGE_ID,
        };
        let encoded = BPlusTreeHeaderPageCodec::encode(&header_page);
        header_page_guard.overwrite(&encoded, None);
        drop(header_page_guard);

        Self {
            key_schema,
            buffer_pool,
            // In our representation, internal node stores a sentinel pointer at index 0.
            // If caller passes max_keys, then max pointers (KVs) = max_keys + 1.
            internal_max_size: internal_max_size + 1,
            leaf_max_size,
            header_page_id,
            header_page_lock: Arc::new(RwLock::new(())),
            config: BTreeConfig::default(),
            pending_garbage: AtomicUsize::new(0),
        }
    }

    pub fn new_with_config(
        key_schema: SchemaRef,
        buffer_pool: Arc<BufferManager>,
        internal_max_size: u32,
        leaf_max_size: u32,
        config: BTreeConfig,
    ) -> Self {
        let mut me = Self::new(key_schema, buffer_pool, internal_max_size, leaf_max_size);
        me.config = config;
        me
    }

    pub fn open(
        key_schema: SchemaRef,
        buffer_pool: Arc<BufferManager>,
        internal_max_size: u32,
        leaf_max_size: u32,
        header_page_id: PageId,
    ) -> Self {
        Self {
            key_schema,
            buffer_pool,
            // See note in new(): store as max pointers (KVs) capacity.
            internal_max_size: internal_max_size + 1,
            leaf_max_size,
            header_page_id,
            header_page_lock: Arc::new(RwLock::new(())),
            config: BTreeConfig::default(),
            pending_garbage: AtomicUsize::new(0),
        }
    }

    pub fn open_with_config(
        key_schema: SchemaRef,
        buffer_pool: Arc<BufferManager>,
        internal_max_size: u32,
        leaf_max_size: u32,
        header_page_id: PageId,
        config: BTreeConfig,
    ) -> Self {
        let mut me = Self::open(
            key_schema,
            buffer_pool,
            internal_max_size,
            leaf_max_size,
            header_page_id,
        );
        me.config = config;
        me
    }

    pub fn get_root_page_id(&self) -> QuillSQLResult<PageId> {
        let header_guard = self.buffer_pool.fetch_page_read(self.header_page_id)?;
        let (header_page, _) = BPlusTreeHeaderPageCodec::decode(header_guard.data())?;
        Ok(header_page.root_page_id)
    }

    fn set_root_page_id(&self, page_id: PageId) -> QuillSQLResult<()> {
        let mut header_guard = self.buffer_pool.fetch_page_write(self.header_page_id)?;
        //
        let header_page = BPlusTreeHeaderPage {
            root_page_id: page_id,
        };
        let encoded = BPlusTreeHeaderPageCodec::encode(&header_page);
        self.wal_overwrite_page(&mut header_guard, encoded)?;
        Ok(())
    }

    pub fn is_empty(&self) -> QuillSQLResult<bool> {
        Ok(self.get_root_page_id()? == INVALID_PAGE_ID)
    }

    pub fn get(&self, key: &Tuple) -> QuillSQLResult<Option<RecordId>> {
        if self.is_empty()? {
            return Ok(None);
        }
        let mut guard = self.find_leaf_page_optimistic(key)?;
        // Walk right through leaf chain while key is greater than last key
        loop {
            let decoded = BPlusTreeLeafPageCodec::decode(guard.data(), self.key_schema.clone());
            if let Ok((leaf_page, _)) = decoded {
                if let Some(rid) = leaf_page.look_up(key) {
                    return Ok(Some(rid));
                }
                if leaf_page.header.current_size > 0
                    && leaf_page.header.next_page_id != INVALID_PAGE_ID
                {
                    let last_key = leaf_page.key_at((leaf_page.header.current_size - 1) as usize);
                    if *key > *last_key {
                        guard = self
                            .buffer_pool
                            .fetch_page_read(leaf_page.header.next_page_id)?;
                        continue;
                    }
                }
                return Ok(None);
            } else {
                // Retry once from root if decode failed (transient)
                guard = self.find_leaf_page_optimistic(key)?;
                let (leaf_page, _) =
                    BPlusTreeLeafPageCodec::decode(guard.data(), self.key_schema.clone())?;
                return Ok(leaf_page.look_up(key));
            }
        }
    }

    /// 辅助函数：以写模式查找叶子页面，并沿途执行闩锁耦合。
    fn find_leaf_page_pessimistic<'a>(
        &'a self,
        key: &Tuple,
        is_insert: bool,
        mut context: Context<'a>,
    ) -> QuillSQLResult<(WritePageGuard, Context<'a>)> {
        if self.config.debug_find_level >= 1 {
            eprintln!(
                "[FIND] thread={:?} begin is_insert={} key={}",
                std::thread::current().id(),
                is_insert,
                key
            );
        }
        // Do not pre-hold header lock on insert path; only take it around root modifications
        let root_page_id = self.get_root_page_id()?;
        if root_page_id == INVALID_PAGE_ID {
            // This should be handled by the caller (insert/delete)
            return Err(QuillSQLError::Internal(
                "find_leaf_page_pessimistic called on an empty tree".to_string(),
            ));
        }
        let mut current_guard = self.buffer_pool.fetch_page_write(root_page_id)?;

        loop {
            let (page, _) =
                BPlusTreePageCodec::decode(current_guard.data(), self.key_schema.clone())?;

            match page {
                BPlusTreePage::Internal(_internal) => {
                    let child_page_id = BPlusTreeInternalPageCodec::lookup_child_from_bytes(
                        current_guard.data(),
                        self.key_schema.clone(),
                        key,
                    )?;
                    if self.config.debug_find_level >= 1 {
                        eprintln!(
                            "[FIND] thread={:?} at_internal parent={} -> child={}",
                            std::thread::current().id(),
                            current_guard.page_id(),
                            child_page_id
                        );
                    }
                    let child_guard = self.buffer_pool.fetch_page_write(child_page_id)?;
                    // header-only safety check for overflow
                    let will_overflow = match BPlusTreePageTypeCodec::decode(child_guard.data())?.0
                    {
                        BPlusTreePageType::LeafPage => {
                            let (hdr, _) =
                                BPlusTreeLeafPageCodec::decode_header_only(child_guard.data())?;
                            hdr.current_size == hdr.max_size
                        }
                        BPlusTreePageType::InternalPage => {
                            let (hdr, _) =
                                BPlusTreeInternalPageCodec::decode_header_only(child_guard.data())?;
                            hdr.current_size == hdr.max_size
                        }
                    };

                    if is_insert {
                        if !will_overflow {
                            if std::env::var("QUILL_DEBUG_FIND").ok().as_deref() == Some("1") {
                                eprintln!(
                                    "[FIND] thread={:?} safe_descend release_ancestors parent={} child={}",
                                    std::thread::current().id(),
                                    current_guard.page_id(),
                                    child_page_id
                                );
                            }
                            context.release_all_write_locks();
                            drop(current_guard);
                            current_guard = child_guard;
                            continue;
                        } else if self.config.debug_find_level >= 1 {
                            eprintln!(
                                "[FIND] thread={:?} hold_parent due_to_full child={} write_set_len={}",
                                std::thread::current().id(),
                                child_page_id,
                                context.write_set.len()
                            );
                        }
                    }

                    if self.config.debug_find_level >= 2 {
                        eprintln!(
                            "[FIND DEBUG] hold-parent: parent={}, child={}, write_set_len={}",
                            current_guard.page_id(),
                            child_page_id,
                            context.write_set.len()
                        );
                    }
                    context.push_write_guard(current_guard);
                    current_guard = child_guard;
                }
                BPlusTreePage::Leaf(_) => {
                    if self.config.debug_find_level >= 1 {
                        eprintln!(
                            "[FIND] thread={:?} reached_leaf leaf_page_id={} write_set_len={}",
                            std::thread::current().id(),
                            current_guard.page_id(),
                            context.write_set.len()
                        );
                    }
                    return Ok((current_guard, context));
                }
            }
        }
    }

    /// 公共 API: 插入一个键值对，使用闩锁耦合实现高并发。
    pub fn insert(&self, key: &Tuple, rid: RecordId) -> QuillSQLResult<()> {
        let mut context = Context::new();
        if self.config.debug_insert_level >= 1 {
            eprintln!(
                "[INSERT] thread={:?} start key={}",
                std::thread::current().id(),
                key
            );
        }

        // Guard tree initialization to avoid concurrent start_new_tree races.
        if self.is_empty()? {
            let _lock = self.header_page_lock.write();
            let root_now = self.get_root_page_id()?;
            if root_now == INVALID_PAGE_ID {
                if self.config.debug_insert_level >= 1 {
                    eprintln!(
                        "[INSERT] thread={:?} start_new_tree key={}",
                        std::thread::current().id(),
                        key
                    );
                }
                // Create root leaf and set root under header lock.
                self.start_new_tree(key, rid)?;
                return Ok(());
            }
        }

        // Split-before-insert loop: ensure we never insert into a full node.
        loop {
            if self.config.debug_insert_level >= 1 {
                eprintln!(
                    "[INSERT] thread={:?} find_leaf_pessimistic key={}",
                    std::thread::current().id(),
                    key
                );
            }
            let (mut leaf_guard, mut local_ctx) =
                self.find_leaf_page_pessimistic(key, true, context)?;
            let (mut leaf_page, _) =
                BPlusTreeLeafPageCodec::decode(leaf_guard.data(), self.key_schema.clone())?;

            // If we still hold a parent, verify that this leaf is the expected child.
            // If not, redirect to the expected child to avoid misplacing keys across parent ranges.
            if let Some(parent_guard_ref) = local_ctx.write_set.back() {
                let (parent_page_chk, _) = BPlusTreeInternalPageCodec::decode(
                    parent_guard_ref.data(),
                    self.key_schema.clone(),
                )?;
                let expected_pid = parent_page_chk.look_up(key);
                if expected_pid != leaf_guard.page_id() {
                    if std::env::var("QUILL_DEBUG_FIND").ok().as_deref() == Some("1") {
                        eprintln!(
                            "[INSERT] parent_guided_redirect: from_leaf={} -> expected_child={}",
                            leaf_guard.page_id(),
                            expected_pid
                        );
                    }
                    drop(leaf_guard);
                    leaf_guard = self.buffer_pool.fetch_page_write(expected_pid)?;
                    let (new_leaf, _) =
                        BPlusTreeLeafPageCodec::decode(leaf_guard.data(), self.key_schema.clone())?;
                    leaf_page = new_leaf;
                }
            }

            // Redirect to right siblings if key no longer belongs to this leaf (post-split race)
            while leaf_page.header.current_size > 0
                && leaf_page.header.next_page_id != INVALID_PAGE_ID
            {
                let last_key_ref = leaf_page.key_at((leaf_page.header.current_size - 1) as usize);
                if *key <= *last_key_ref {
                    break;
                }
                let next_pid = leaf_page.header.next_page_id;
                // Peek the next leaf's minimal key using a read latch to avoid heavy contention
                let next_guard_peek = self.buffer_pool.fetch_page_read(next_pid)?;
                let (next_leaf_peek, _) = BPlusTreeLeafPageCodec::decode(
                    next_guard_peek.data(),
                    self.key_schema.clone(),
                )?;
                let next_first_key = if next_leaf_peek.header.current_size > 0 {
                    next_leaf_peek.key_at(0).clone()
                } else {
                    break;
                };
                drop(next_guard_peek);
                if *key < next_first_key {
                    break;
                }
                if self.config.debug_insert_level >= 1 {
                    eprintln!(
                        "[INSERT] thread={:?} redirect_to_sibling: from_leaf={} -> next_leaf={} key={} last_key={} next_first_key={}",
                        std::thread::current().id(),
                        leaf_guard.page_id(),
                        next_pid,
                        key,
                        last_key_ref,
                        next_first_key
                    );
                }
                // Release any parents held and current leaf, then jump to next sibling directly
                local_ctx.release_all_write_locks();
                drop(leaf_guard);
                let next_guard = self.buffer_pool.fetch_page_write(next_pid)?;
                let (next_leaf, _) =
                    BPlusTreeLeafPageCodec::decode(next_guard.data(), self.key_schema.clone())?;
                leaf_guard = next_guard;
                leaf_page = next_leaf;
            }

            // Update if key exists
            if let Some(existing_rid) = leaf_page.look_up_mut(key) {
                if self.config.debug_insert_level >= 1 {
                    eprintln!(
                        "[INSERT] thread={:?} update leaf_page_id={} key={} old_rid={:?} new_rid={:?}",
                        std::thread::current().id(),
                        leaf_guard.page_id(),
                        key,
                        *existing_rid,
                        rid
                    );
                }
                *existing_rid = rid;
                leaf_page.header.version += 1;
                let encoded = BPlusTreeLeafPageCodec::encode(&leaf_page);
                self.wal_overwrite_page(&mut leaf_guard, encoded)?;
                local_ctx.release_all_write_locks();
                return Ok(());
            }

            // If page is at capacity, split first, then retry to find the correct leaf
            if leaf_page.header.current_size == leaf_page.header.max_size {
                if std::env::var("QUILL_DEBUG_INSERT").ok().as_deref() == Some("1") {
                    eprintln!(
                        "[INSERT] thread={:?} leaf_full split leaf_page_id={} key={}",
                        std::thread::current().id(),
                        leaf_guard.page_id(),
                        key
                    );
                }
                // Avoid deadlock: allow split to acquire header lock when promoting root
                local_ctx.header_lock_guard = None;
                match self.split(leaf_guard, &mut local_ctx) {
                    Ok(()) => {
                        // Release all locks and retry from root with fresh context
                        local_ctx.release_all_write_locks();
                        context = Context::new();
                        continue;
                    }
                    Err(QuillSQLError::Internal(_e)) => {
                        // Structural race: drop latches and retry from root
                        local_ctx.release_all_write_locks();
                        context = Context::new();
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }

            // Safe to insert
            if std::env::var("QUILL_DEBUG_INSERT").ok().as_deref() == Some("1") {
                eprintln!(
                    "[INSERT] thread={:?} insert leaf_page_id={} key={} rid={:?}",
                    std::thread::current().id(),
                    leaf_guard.page_id(),
                    key,
                    rid
                );
            }
            leaf_page.insert(key.clone(), rid);
            leaf_page.header.version += 1;
            let encoded = BPlusTreeLeafPageCodec::encode(&leaf_page);
            self.wal_overwrite_page(&mut leaf_guard, encoded)?;
            local_ctx.release_all_write_locks();
            return Ok(());
        }
    }

    /// Public API: delete a key with latch coupling to ensure concurrency safety.
    pub fn delete(&self, key: &Tuple) -> QuillSQLResult<()> {
        if self.is_empty()? {
            return Ok(());
        }

        let mut context = Context::new();
        'restart: loop {
            let (mut leaf_guard, mut local_ctx) =
                self.find_leaf_page_pessimistic(key, false, context)?;
            let (mut leaf_page, _) =
                BPlusTreeLeafPageCodec::decode(leaf_guard.data(), self.key_schema.clone())?;

            // If we still hold a parent, prefer parent-guided redirection to avoid crossing
            // parent boundary via leaf chain which can cause livelock during structure changes.
            if let Some(parent_guard_ref) = local_ctx.write_set.back() {
                let (parent_page_chk, _) = BPlusTreeInternalPageCodec::decode(
                    parent_guard_ref.data(),
                    self.key_schema.clone(),
                )?;
                let expected_pid = parent_page_chk.look_up(key);
                if expected_pid != leaf_guard.page_id() {
                    if std::env::var("QUILL_DEBUG_FIND").ok().as_deref() == Some("1") {
                        eprintln!(
                            "[DELETE] parent_guided_redirect: from_leaf={} -> expected_child={}",
                            leaf_guard.page_id(),
                            expected_pid
                        );
                    }
                    drop(leaf_guard);
                    leaf_guard = self.buffer_pool.fetch_page_write(expected_pid)?;
                    let (new_leaf, _) =
                        BPlusTreeLeafPageCodec::decode(leaf_guard.data(), self.key_schema.clone())?;
                    leaf_page = new_leaf;
                }
            }

            // Redirect: if key belongs to right siblings, jump directly to sibling while keeping parent path
            let mut hops: u32 = 0;
            while leaf_page.header.current_size > 0
                && leaf_page.header.next_page_id != INVALID_PAGE_ID
            {
                let last_key_ref = leaf_page.key_at((leaf_page.header.current_size - 1) as usize);
                if *key <= *last_key_ref {
                    break;
                }
                let next_pid = leaf_page.header.next_page_id;
                if std::env::var("QUILL_DEBUG_FIND").ok().as_deref() == Some("1") {
                    eprintln!(
                        "[DELETE] thread={:?} redirect_to_sibling: from_leaf={} -> next_leaf={} key={} last_key={}",
                        std::thread::current().id(),
                        leaf_guard.page_id(),
                        next_pid,
                        key,
                        last_key_ref
                    );
                }
                hops += 1;
                if hops > 8 {
                    if std::env::var("QUILL_DEBUG_FIND").ok().as_deref() == Some("1") {
                        eprintln!(
                            "[DELETE] redirect_hops_exceeded: restart from root at leaf={}",
                            leaf_guard.page_id()
                        );
                    }
                    local_ctx.release_all_write_locks();
                    drop(leaf_guard);
                    context = Context::new();
                    continue 'restart;
                }
                drop(leaf_guard);
                leaf_guard = self.buffer_pool.fetch_page_write(next_pid)?;
                let (new_leaf, _) =
                    BPlusTreeLeafPageCodec::decode(leaf_guard.data(), self.key_schema.clone())?;
                leaf_page = new_leaf;

                // Do not force restart here; attempt deletion on the correct leaf first.
            }

            // Check presence & whether it is the first key (affects parent separator)
            let was_first = if leaf_page.header.current_size > 0 {
                let first_key = leaf_page.key_at(0).clone();
                &first_key == key
            } else {
                false
            };
            if leaf_page.look_up(key).is_none() {
                local_ctx.release_all_write_locks();
                return Ok(());
            }

            leaf_page.delete(key);
            leaf_page.header.version += 1;
            let encoded = BPlusTreeLeafPageCodec::encode(&leaf_page);
            self.wal_overwrite_page(&mut leaf_guard, encoded)?;

            // If the node is underflowing, handle it.
            if leaf_page.header.current_size < leaf_page.min_size() {
                // If parent path is empty but this leaf is not the actual root, rebuild path
                if local_ctx.write_set.is_empty() {
                    let root_id = self.get_root_page_id()?;
                    if leaf_guard.page_id() != root_id {
                        local_ctx.release_all_write_locks();
                        drop(leaf_guard);
                        context = Context::new();
                        continue 'restart;
                    }
                }
                match self.handle_underflow(leaf_guard, &mut local_ctx) {
                    Ok(()) => {
                        local_ctx.release_all_write_locks();
                        return Ok(());
                    }
                    Err(QuillSQLError::Internal(_)) => {
                        // Structural race/mismatch: release and retry from root
                        local_ctx.release_all_write_locks();
                        context = Context::new();
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            } else {
                // If we deleted the first key of this leaf, and there's a parent above,
                // update the parent's separator key to the new minimal key of this leaf.
                if was_first && leaf_page.header.current_size > 0 {
                    if let Some(mut parent_guard) = local_ctx.write_set.pop_back() {
                        let (mut parent_page, _) = BPlusTreeInternalPageCodec::decode(
                            parent_guard.data(),
                            self.key_schema.clone(),
                        )?;
                        if let Some(node_idx) = parent_page.value_index(leaf_guard.page_id()) {
                            if node_idx > 0 {
                                parent_page.array[node_idx].0 = leaf_page.key_at(0).clone();
                                parent_page.header.version += 1;
                                let encoded = BPlusTreeInternalPageCodec::encode(&parent_page);
                                self.wal_overwrite_page(&mut parent_guard, encoded)?;
                            }
                        }
                        // push back to maintain path for later releases
                        local_ctx.write_set.push_back(parent_guard);
                    }
                }
                local_ctx.release_all_write_locks();
                return Ok(());
            }
        }
    }

    pub fn to_dot(&self) -> QuillSQLResult<String> {
        let mut dot = String::new();
        writeln!(&mut dot, "digraph BPlusTree {{").unwrap();
        writeln!(&mut dot, "  rankdir=TB;").unwrap();
        writeln!(&mut dot, "  node [shape=record, height=.1];").unwrap();

        let root_page_id = self.get_root_page_id()?;
        if root_page_id == INVALID_PAGE_ID {
            writeln!(&mut dot, "  empty [label=\"<f0> Empty Tree\"];").unwrap();
            writeln!(&mut dot, "}}").unwrap();
            return Ok(dot);
        }

        let mut queue = VecDeque::new();
        queue.push_back(root_page_id);

        while let Some(page_id) = queue.pop_front() {
            let guard = self.buffer_pool.fetch_page_read(page_id)?;
            let (page, _) = BPlusTreePageCodec::decode(guard.data(), self.key_schema.clone())?;

            match page {
                BPlusTreePage::Internal(internal) => {
                    let mut label = String::new();
                    for i in 0..internal.header.current_size {
                        if i > 0 {
                            write!(&mut label, "|").unwrap();
                        }
                        write!(&mut label, "<p{}>", i).unwrap();
                        if i > 0 {
                            write!(&mut label, " {}", internal.key_at(i as usize)).unwrap();
                        }
                    }
                    writeln!(&mut dot, "  page{} [label=\"{}\"];", page_id, label).unwrap();

                    for i in 0..internal.header.current_size {
                        let child_id = internal.value_at(i as usize);
                        writeln!(
                            &mut dot,
                            "  \"page{}\":p{} -> \"page{}\";",
                            page_id, i, child_id
                        )
                        .unwrap();
                        queue.push_back(child_id);
                    }
                }
                BPlusTreePage::Leaf(leaf) => {
                    let mut label = String::new();
                    for i in 0..leaf.header.current_size {
                        if i > 0 {
                            write!(&mut label, "|").unwrap();
                        }
                        write!(
                            &mut label,
                            "<f{}> {} -> {}",
                            i,
                            leaf.key_at(i as usize),
                            leaf.array[i as usize].1
                        )
                        .unwrap();
                    }
                    writeln!(&mut dot, "  page{} [label=\"{}\"];", page_id, label).unwrap();

                    if leaf.header.next_page_id != INVALID_PAGE_ID {
                        writeln!(
                            &mut dot,
                            "  page{} -> page{} [style=dashed, constraint=false];",
                            page_id, leaf.header.next_page_id
                        )
                        .unwrap();
                    }
                }
            }
        }

        writeln!(&mut dot, "}}").unwrap();
        Ok(dot)
    }

    fn handle_underflow(
        &self,
        mut node_guard: WritePageGuard,
        context: &mut Context,
    ) -> QuillSQLResult<()> {
        if context.write_set.is_empty() {
            // This is the root node. Let adjust_root handle it.
            self.adjust_root(node_guard)?;
            return Ok(());
        }

        let parent_guard = match context.write_set.pop_back() {
            Some(g) => g,
            None => {
                return Err(QuillSQLError::Internal(
                    "underflow: missing parent".to_string(),
                ))
            }
        };
        let (parent_page, _) =
            BPlusTreeInternalPageCodec::decode(parent_guard.data(), self.key_schema.clone())?;

        let Some(node_idx) = parent_page.value_index(node_guard.page_id()) else {
            return Err(QuillSQLError::Internal(
                "underflow: node not found in parent".to_string(),
            ));
        };

        // Try to borrow from the left sibling first (acquire locks in PageId order to avoid deadlock).
        if node_idx > 0 {
            let left_sibling_pid = parent_page.value_at(node_idx - 1);
            let node_pid = node_guard.page_id();
            if left_sibling_pid < node_pid {
                // Enforce PageId order: release node, lock left first, then node
                drop(node_guard);
                let left_sibling_guard = self.buffer_pool.fetch_page_write(left_sibling_pid)?;
                let node_guard_new = self.buffer_pool.fetch_page_write(node_pid)?;
                node_guard = node_guard_new;
                let (left_sibling_page, _) =
                    BPlusTreePageCodec::decode(left_sibling_guard.data(), self.key_schema.clone())?;
                if left_sibling_page.current_size() > left_sibling_page.min_size() {
                    self.redistribute(
                        left_sibling_guard,
                        node_guard,
                        parent_guard,
                        node_idx,
                        true,
                    )?;
                    return Ok(());
                }
            } else {
                // We already hold node; locking left next preserves non-decreasing order
                let left_sibling_guard = self.buffer_pool.fetch_page_write(left_sibling_pid)?;
                let (left_sibling_page, _) =
                    BPlusTreePageCodec::decode(left_sibling_guard.data(), self.key_schema.clone())?;
                if left_sibling_page.current_size() > left_sibling_page.min_size() {
                    self.redistribute(
                        left_sibling_guard,
                        node_guard,
                        parent_guard,
                        node_idx,
                        true,
                    )?;
                    return Ok(());
                }
            }
        }

        // Try to borrow from the right sibling (acquire locks in PageId order to avoid deadlock).
        if node_idx < parent_page.header.current_size as usize - 1 {
            let right_sibling_pid = parent_page.value_at(node_idx + 1);
            let node_pid = node_guard.page_id();
            if right_sibling_pid < node_pid {
                // Enforce PageId order: release node, lock right first, then node
                drop(node_guard);
                let right_sibling_guard = self.buffer_pool.fetch_page_write(right_sibling_pid)?;
                let node_guard_new = self.buffer_pool.fetch_page_write(node_pid)?;
                node_guard = node_guard_new;
                let (right_sibling_page, _) = BPlusTreePageCodec::decode(
                    right_sibling_guard.data(),
                    self.key_schema.clone(),
                )?;
                if right_sibling_page.current_size() > right_sibling_page.min_size() {
                    self.redistribute(
                        right_sibling_guard,
                        node_guard,
                        parent_guard,
                        node_idx,
                        false,
                    )?;
                    return Ok(());
                }
            } else {
                let right_sibling_guard = self.buffer_pool.fetch_page_write(right_sibling_pid)?;
                let (right_sibling_page, _) = BPlusTreePageCodec::decode(
                    right_sibling_guard.data(),
                    self.key_schema.clone(),
                )?;
                if right_sibling_page.current_size() > right_sibling_page.min_size() {
                    self.redistribute(
                        right_sibling_guard,
                        node_guard,
                        parent_guard,
                        node_idx,
                        false,
                    )?;
                    return Ok(());
                }
            }
        }

        // Neither sibling can lend, so we must merge.
        if node_idx > 0 {
            // Merge with the left sibling (lock in PageId order)
            let left_sibling_pid = parent_page.value_at(node_idx - 1);
            let node_pid = node_guard.page_id();
            if left_sibling_pid < node_pid {
                // Enforce PageId order for merge: release node first
                drop(node_guard);
                let left_sibling_guard = self.buffer_pool.fetch_page_write(left_sibling_pid)?;
                let node_guard_new = self.buffer_pool.fetch_page_write(node_pid)?;
                node_guard = node_guard_new;
                self.coalesce(left_sibling_guard, node_guard, parent_guard, context)?;
            } else {
                let left_sibling_guard = self.buffer_pool.fetch_page_write(left_sibling_pid)?;
                self.coalesce(left_sibling_guard, node_guard, parent_guard, context)?;
            }
        } else {
            // Merge with the right sibling (lock in PageId order)
            let right_sibling_pid = parent_page.value_at(node_idx + 1);
            let node_pid = node_guard.page_id();
            if right_sibling_pid < node_pid {
                // Enforce PageId order for merge: release node first
                drop(node_guard);
                let right_sibling_guard = self.buffer_pool.fetch_page_write(right_sibling_pid)?;
                let node_guard_new = self.buffer_pool.fetch_page_write(node_pid)?;
                node_guard = node_guard_new;
                // node is left in this case
                self.coalesce(node_guard, right_sibling_guard, parent_guard, context)?;
            } else {
                let right_sibling_guard = self.buffer_pool.fetch_page_write(right_sibling_pid)?;
                self.coalesce(node_guard, right_sibling_guard, parent_guard, context)?;
            }
        }

        Ok(())
    }

    fn coalesce(
        &self,
        mut left_guard: WritePageGuard,
        right_guard: WritePageGuard,
        mut parent_guard: WritePageGuard,
        context: &mut Context,
    ) -> QuillSQLResult<()> {
        let (mut left_page, _) =
            BPlusTreePageCodec::decode(left_guard.data(), self.key_schema.clone())?;
        let (mut right_page, _) =
            BPlusTreePageCodec::decode(right_guard.data(), self.key_schema.clone())?;
        let (mut parent_page, _) =
            BPlusTreeInternalPageCodec::decode(parent_guard.data(), self.key_schema.clone())?;

        let right_page_id = right_guard.page_id();
        let middle_key = match parent_page.remove(right_page_id) {
            Some((k, _)) => k,
            None => {
                return Err(QuillSQLError::Internal(
                    "coalesce: parent missing right child".to_string(),
                ));
            }
        };

        match (&mut left_page, &mut right_page) {
            (BPlusTreePage::Leaf(left), BPlusTreePage::Leaf(right)) => {
                left.merge(right);
            }
            (BPlusTreePage::Internal(left), BPlusTreePage::Internal(right)) => {
                left.merge(middle_key, right);
                // B-link maintenance: after merge, left should inherit right's high bound and next pointer
                left.header.next_page_id = right.header.next_page_id;
                left.high_key = right.high_key.clone();
            }
            _ => unreachable!("Mismatched page types in coalesce"),
        }

        // Update pages on disk
        if let BPlusTreePage::Leaf(p) = &mut left_page {
            p.header.version += 1;
        } else if let BPlusTreePage::Internal(p) = &mut left_page {
            p.header.version += 1;
        }
        parent_page.header.version += 1;

        let left_encoded = BPlusTreePageCodec::encode(&left_page);
        self.wal_overwrite_page(&mut left_guard, left_encoded)?;
        let parent_encoded = BPlusTreeInternalPageCodec::encode(&parent_page);
        self.wal_overwrite_page(&mut parent_guard, parent_encoded)?;
        drop(left_guard);
        drop(right_guard);
        self.buffer_pool.delete_page(right_page_id)?;

        // After merging, if the parent becomes a root with only one child,
        // it needs to be adjusted to shrink the tree's height.
        if context.write_set.is_empty() && parent_page.header.current_size == 1 {
            self.adjust_root(parent_guard)?;
        } else if parent_page.header.current_size < parent_page.min_size() {
            // Otherwise, recursively handle underflow in the parent.
            self.handle_underflow(parent_guard, context)?;
        }
        Ok(())
    }

    fn redistribute(
        &self,
        mut from_guard: WritePageGuard,
        mut to_guard: WritePageGuard,
        mut parent_guard: WritePageGuard,
        parent_idx_of_to_node: usize,
        from_is_left_sibling: bool,
    ) -> QuillSQLResult<()> {
        let (mut from_page, _) =
            BPlusTreePageCodec::decode(from_guard.data(), self.key_schema.clone())?;
        let (mut to_page, _) =
            BPlusTreePageCodec::decode(to_guard.data(), self.key_schema.clone())?;
        let (mut parent_page, _) =
            BPlusTreeInternalPageCodec::decode(parent_guard.data(), self.key_schema.clone())?;

        if from_is_left_sibling {
            // from=left(from_internal), to=right(to_internal). Separator key is at parent_idx_of_to_node.
            let separator_idx = parent_idx_of_to_node;
            match (&mut to_page, &mut from_page) {
                (BPlusTreePage::Leaf(to_leaf), BPlusTreePage::Leaf(from_leaf)) => {
                    let item_to_move = from_leaf.remove_last_kv();
                    to_leaf.array.insert(0, item_to_move);
                    to_leaf.header.current_size += 1;
                    // Parent separator stores the minimal key of the RIGHT child
                    parent_page.array[separator_idx].0 = to_leaf.key_at(0).clone();
                    to_leaf.header.version += 1;
                    from_leaf.header.version += 1;
                }
                (BPlusTreePage::Internal(to_internal), BPlusTreePage::Internal(from_internal)) => {
                    // This logic is based on bustub's BPlusTreeInternalPage::MoveLastToFrontOf
                    // 1. Get the last key-pointer pair from the left sibling (from_internal)
                    let item_to_move = from_internal.remove_last_kv(); // This is (K_last, P_last)
                    let separator_key_in_parent = parent_page.key_at(separator_idx).clone();

                    // 2. The old separator key from the parent moves down to become the first key in the recipient (to_internal).
                    // The original sentinel pointer of the recipient becomes the pointer for this new key.
                    to_internal
                        .array
                        .insert(1, (separator_key_in_parent, to_internal.value_at(0)));

                    // 3. The pointer from the moved item becomes the new sentinel for the recipient.
                    to_internal.array[0].1 = item_to_move.1; // Set sentinel to P_last

                    // 4. The key from the moved item becomes the new separator in the parent.
                    parent_page.array[separator_idx].0 = item_to_move.0; // Set parent key to K_last

                    to_internal.header.current_size += 1;
                    to_internal.header.version += 1;
                    from_internal.header.version += 1;

                    self.refresh_internal_child_fence(
                        &parent_page,
                        to_guard.page_id(),
                        to_internal,
                    )?;
                    self.refresh_internal_child_fence(
                        &parent_page,
                        from_guard.page_id(),
                        from_internal,
                    )?;
                }
                _ => return Err(QuillSQLError::Internal("Mismatched page types".to_string())),
            }
        } else {
            // Borrow from RIGHT
            // from=right(from_internal), to=left(to_internal). Separator key is at parent_idx_of_to_node + 1.
            let separator_idx = parent_idx_of_to_node + 1;
            match (&mut to_page, &mut from_page) {
                (BPlusTreePage::Leaf(to_leaf), BPlusTreePage::Leaf(from_leaf)) => {
                    let item_to_move = from_leaf.remove_first_kv();
                    to_leaf.array.push(item_to_move);
                    to_leaf.header.current_size += 1;
                    // Parent separator stores the minimal key of the RIGHT child
                    parent_page.array[separator_idx].0 = from_leaf.key_at(0).clone();
                    to_leaf.header.version += 1;
                    from_leaf.header.version += 1;
                }
                (BPlusTreePage::Internal(to_internal), BPlusTreePage::Internal(from_internal)) => {
                    // This logic is based on bustub's BPlusTreeInternalPage::MoveFirstToEndOf
                    // 1. Get the separator from the parent and the first *real* item from the right sibling (from_internal)
                    let separator_key_in_parent = parent_page.key_at(separator_idx).clone();
                    let item_to_move = from_internal.remove_first_kv(); // This is (K_R1, P_R1)

                    // 2. The old separator moves down to the end of the recipient (to_internal).
                    // Its pointer is the original sentinel pointer of the right sibling.
                    to_internal
                        .array
                        .push((separator_key_in_parent, from_internal.value_at(0)));

                    // 3. The key from the moved item becomes the new separator in the parent.
                    parent_page.array[separator_idx].0 = item_to_move.0; // Set parent key to K_R1

                    // 4. The pointer from the moved item becomes the new sentinel of the right sibling.
                    from_internal.array[0].1 = item_to_move.1; // Set right sibling's sentinel to P_R1

                    to_internal.header.current_size += 1;
                    to_internal.header.version += 1;
                    from_internal.header.version += 1;

                    self.refresh_internal_child_fence(
                        &parent_page,
                        to_guard.page_id(),
                        to_internal,
                    )?;
                    self.refresh_internal_child_fence(
                        &parent_page,
                        from_guard.page_id(),
                        from_internal,
                    )?;
                }
                _ => return Err(QuillSQLError::Internal("Mismatched page types".to_string())),
            }
        }

        parent_page.header.version += 1;

        let from_encoded = BPlusTreePageCodec::encode(&from_page);
        self.wal_overwrite_page(&mut from_guard, from_encoded)?;
        let to_encoded = BPlusTreePageCodec::encode(&to_page);
        self.wal_overwrite_page(&mut to_guard, to_encoded)?;
        let parent_encoded = BPlusTreeInternalPageCodec::encode(&parent_page);
        self.wal_overwrite_page(&mut parent_guard, parent_encoded)?;

        Ok(())
    }

    fn refresh_internal_child_fence(
        &self,
        parent_page: &BPlusTreeInternalPage,
        child_page_id: PageId,
        child_page: &mut BPlusTreeInternalPage,
    ) -> QuillSQLResult<()> {
        let Some(child_idx) = parent_page.value_index(child_page_id) else {
            return Err(QuillSQLError::Internal(
                "redistribute: child missing from parent".to_string(),
            ));
        };
        let size = parent_page.header.current_size as usize;
        if child_idx + 1 < size {
            child_page.high_key = Some(parent_page.key_at(child_idx + 1).clone());
            child_page.header.next_page_id = parent_page.value_at(child_idx + 1);
        } else {
            child_page.high_key = parent_page.high_key.clone();
            child_page.header.next_page_id = parent_page.header.next_page_id;
        }
        Ok(())
    }

    fn adjust_root(&self, root_guard: WritePageGuard) -> QuillSQLResult<()> {
        let (root_page, _) =
            BPlusTreePageCodec::decode(root_guard.data(), self.key_schema.clone())?;

        if let BPlusTreePage::Internal(root_internal) = root_page {
            if root_internal.header.current_size == 1 {
                // The lock is already held by the caller (e.g., delete).
                // Re-acquiring it would cause a deadlock.
                let new_root_id = root_internal.value_at(0);
                // Drop page guard before touching header to avoid page<->header lock inversion
                drop(root_guard);
                let _lock = self.header_page_lock.write();
                self.set_root_page_id(new_root_id)?;
                // Delay old-root physical deallocation to avoid races with concurrent readers
            }
        } else if let BPlusTreePage::Leaf(root_leaf) = root_page {
            if root_leaf.header.current_size == 0 {
                // The lock is already held by the caller.
                drop(root_guard);
                let _lock = self.header_page_lock.write();
                self.set_root_page_id(INVALID_PAGE_ID)?;
                // Delay old-root physical deallocation to avoid races with concurrent readers
            }
        }
        Ok(())
    }

    /// Internal: create the very first node when the tree is empty.
    fn start_new_tree(&self, key: &Tuple, rid: RecordId) -> QuillSQLResult<()> {
        let mut root_guard = self.buffer_pool.new_page()?;
        let root_page_id = root_guard.page_id();
        let mut leaf_page = BPlusTreeLeafPage::new(self.key_schema.clone(), self.leaf_max_size);
        leaf_page.insert(key.clone(), rid);
        let encoded_data = BPlusTreeLeafPageCodec::encode(&leaf_page);
        self.wal_overwrite_page(&mut root_guard, encoded_data)?;
        // Update root id (release page latch before touching header to avoid lock inversion).
        // Precondition: caller holds header_page_lock.
        drop(root_guard);
        self.set_root_page_id(root_page_id)?;
        Ok(())
    }

    /// Best-effort lazy cleanup: iterate across leaves and delete keys whose RIDs
    /// satisfy the `is_globally_dead` predicate. This is REDO-safe and uses the
    /// existing delete() path to maintain separators and handle underflow properly.
    /// Notes:
    /// - Callers should pass a predicate that only returns true for tuples that are
    ///   globally invisible (e.g., deleted and the deleting txn committed).
    /// - This method is conservative and may revisit pages as structure changes; it
    ///   favors correctness over speed.
    pub fn lazy_cleanup_with<F>(
        &self,
        mut is_globally_dead: F,
        limit: Option<usize>,
    ) -> QuillSQLResult<usize>
    where
        F: FnMut(&RecordId) -> bool,
    {
        let mut cleaned = 0usize;
        let mut guard = self.find_first_leaf_page()?;
        loop {
            let (leaf, _) = BPlusTreeLeafPageCodec::decode(guard.data(), self.key_schema.clone())?;
            // Snapshot next pid before releasing read guard
            let next_pid = leaf.header.next_page_id;
            // Collect keys to delete
            let mut to_delete: Vec<Tuple> = Vec::new();
            for i in 0..(leaf.header.current_size as usize) {
                let kv = leaf.kv_at(i).clone();
                if is_globally_dead(&kv.1) {
                    to_delete.push(kv.0);
                }
            }
            drop(guard);

            for k in to_delete.into_iter() {
                self.delete(&k)?;
                cleaned += 1;
                if let Some(maxn) = limit {
                    if cleaned >= maxn {
                        return Ok(cleaned);
                    }
                }
            }

            if next_pid == INVALID_PAGE_ID {
                break;
            }
            guard = self.buffer_pool.fetch_page_read(next_pid)?;
        }
        Ok(cleaned)
    }

    fn find_leaf_page_optimistic(&self, key: &Tuple) -> QuillSQLResult<ReadPageGuard> {
        // OLC + B-link: version-check each step; if changed, restart with bounded backoff.
        let mut restarts = 0usize;
        'restart: loop {
            let mut current_guard = self.buffer_pool.fetch_page_read(self.get_root_page_id()?)?;
            loop {
                let decoded =
                    BPlusTreePageCodec::decode(current_guard.data(), self.key_schema.clone());
                if decoded.is_err() {
                    drop(current_guard);
                    restarts += 1;
                    if restarts > MAX_OLC_RESTARTS {
                        std::thread::sleep(std::time::Duration::from_micros(
                            OLC_BACKOFF_BASE_US.saturating_mul(1 << (restarts.min(10) - 1)),
                        ));
                        restarts = 0;
                    }
                    continue 'restart;
                }
                let (page, _) = decoded.unwrap();
                match page {
                    BPlusTreePage::Internal(internal) => {
                        // Double-read header-only version for OLC
                        let (hdr1, _) =
                            BPlusTreeInternalPageCodec::decode_header_only(current_guard.data())?;
                        let v1 = hdr1.version;
                        if let Some(ref hk) = internal.high_key {
                            if key >= hk && internal.header.next_page_id != INVALID_PAGE_ID {
                                let sib = self
                                    .buffer_pool
                                    .fetch_page_read(internal.header.next_page_id)?;
                                drop(current_guard);
                                current_guard = sib;
                                continue;
                            }
                        }
                        // Byte-based child lookup to avoid re-decode costs
                        let next_page_id = BPlusTreeInternalPageCodec::lookup_child_from_bytes(
                            current_guard.data(),
                            self.key_schema.clone(),
                            key,
                        )?;
                        let (hdr2, _) =
                            BPlusTreeInternalPageCodec::decode_header_only(current_guard.data())?;
                        let v2 = hdr2.version;
                        if v1 != v2 {
                            drop(current_guard);
                            restarts += 1;
                            if restarts > MAX_OLC_RESTARTS {
                                std::thread::sleep(std::time::Duration::from_micros(
                                    OLC_BACKOFF_BASE_US.saturating_mul(1 << (restarts.min(10) - 1)),
                                ));
                                restarts = 0;
                            }
                            continue 'restart;
                        }
                        let child_guard = self.buffer_pool.fetch_page_read(next_page_id)?;
                        drop(current_guard);
                        current_guard = child_guard;
                    }
                    BPlusTreePage::Leaf(_leaf) => {
                        let (h1, _) =
                            BPlusTreeLeafPageCodec::decode_header_only(current_guard.data())?;
                        let v1 = h1.version;
                        let (h2, _) =
                            BPlusTreeLeafPageCodec::decode_header_only(current_guard.data())?;
                        let v2 = h2.version;
                        if v1 != v2 {
                            drop(current_guard);
                            restarts += 1;
                            if restarts > MAX_OLC_RESTARTS {
                                std::thread::sleep(std::time::Duration::from_micros(
                                    OLC_BACKOFF_BASE_US.saturating_mul(1 << (restarts.min(10) - 1)),
                                ));
                                restarts = 0;
                            }
                            continue 'restart;
                        }
                        return Ok(current_guard);
                    }
                }
            }
        }
    }

    pub fn find_first_leaf_page(&self) -> QuillSQLResult<ReadPageGuard> {
        let mut current_page_id = self.get_root_page_id()?;
        if current_page_id == INVALID_PAGE_ID {
            return Err(QuillSQLError::Internal("Tree is empty".to_string()));
        }

        loop {
            let guard = self.buffer_pool.fetch_page_read(current_page_id)?;
            let (page, _) = BPlusTreePageCodec::decode(guard.data(), self.key_schema.clone())?;

            match page {
                BPlusTreePage::Internal(internal) => {
                    current_page_id = internal.value_at(0);
                }
                BPlusTreePage::Leaf(_) => {
                    return Ok(guard);
                }
            }
        }
    }

    /// 内部辅助函数：从根节点开始遍历，找到并返回包含目标 key 的
    /// 叶子节点的只读保护器 (ReadPageGuard)。
    ///
    /// 这个函数通过 ReadPageGuard 的 RAII 特性，在遍历时实现了闩锁耦合。
    pub fn find_leaf_page_for_iterator(
        &self,
        key: &Tuple,
        start_page_id: PageId,
    ) -> QuillSQLResult<ReadPageGuard> {
        let mut current_page_id = start_page_id;
        if current_page_id == INVALID_PAGE_ID {
            return Err(QuillSQLError::Storage("btree: empty tree".to_string()));
        }

        loop {
            // a. 为当前页面获取一个 ReadPageGuard。
            //    这会自动 pin 住页面并加上读锁。
            let current_guard = self.buffer_pool.fetch_page_read(current_page_id)?;

            // b. 解码页面内容以判断其类型。
            let (page_content, _) =
                BPlusTreePageCodec::decode(current_guard.data(), self.key_schema.clone())?;

            match page_content {
                // c. 如果是内部节点...
                BPlusTreePage::Internal(_internal_page) => {
                    // find next child id using bytes path
                    current_page_id =
                        crate::storage::codec::BPlusTreeInternalPageCodec::lookup_child_from_bytes(
                            current_guard.data(),
                            self.key_schema.clone(),
                            key,
                        )?;
                }
                // d. 如果是叶子节点...
                BPlusTreePage::Leaf(_) => {
                    // 我们已经到达了树的最底层。返回这个页面的 Guard，
                    // 它的所有权会被转移给调用者 (get 方法)，从而延长锁的生命周期。
                    return Ok(current_guard);
                }
            }
        }
    }

    /// 内部方法：分裂一个节点，并可能递归地向上传播分裂。
    fn split<'a>(
        &'a self,
        mut page_guard: WritePageGuard,
        context: &mut Context<'a>,
    ) -> QuillSQLResult<()> {
        if self.config.debug_split_level >= 2 {
            eprintln!(
                "[SPLIT DEBUG] splitting page={}, write_set_len={}",
                page_guard.page_id(),
                context.write_set.len()
            );
        }
        loop {
            let page_id = page_guard.page_id();
            if self.config.debug_split_level >= 2 {
                eprintln!(
                    "[SPLIT DEBUG] splitting page={}, write_set_len={}",
                    page_id,
                    context.write_set.len()
                );
            }
            let (mut page, _) =
                BPlusTreePageCodec::decode(page_guard.data(), self.key_schema.clone())?;

            let mut new_page_guard = self.buffer_pool.new_page()?;
            let new_page_id = new_page_guard.page_id();

            let middle_key = match &mut page {
                BPlusTreePage::Leaf(leaf_page) => {
                    let mut new_leaf =
                        BPlusTreeLeafPage::new(self.key_schema.clone(), self.leaf_max_size);
                    new_leaf.batch_insert(
                        leaf_page.split_off(leaf_page.header.current_size as usize / 2),
                    );
                    new_leaf.header.next_page_id = leaf_page.header.next_page_id;
                    leaf_page.header.next_page_id = new_page_id;
                    let new_data = BPlusTreeLeafPageCodec::encode(&new_leaf);
                    self.wal_overwrite_page(&mut new_page_guard, new_data)?;
                    leaf_page.header.version += 1;
                    if self.config.debug_split_level >= 2 && new_leaf.header.current_size > 0 {
                        eprintln!(
                            "[SPLIT DEBUG] leaf_split left={} right={} sep_key={}",
                            page_id,
                            new_page_id,
                            new_leaf.key_at(0)
                        );
                    }
                    new_leaf.key_at(0).clone()
                }
                BPlusTreePage::Internal(internal_page) => {
                    let mut new_internal =
                        BPlusTreeInternalPage::new(self.key_schema.clone(), self.internal_max_size);

                    // 计算应上推的中间键位置：
                    // 假设 array 以哨兵在索引0，真实键在 [1..num_pointers-1]
                    // promote_idx = 1 + floor((num_pointers - 1) / 2)
                    let num_pointers = internal_page.header.current_size as usize;
                    let promote_idx = 1 + (num_pointers.saturating_sub(1) / 2);

                    // 将 [promote_idx, ..) 全部从左页移出
                    let mut moved = internal_page.split_off(promote_idx);

                    // moved[0] 为 (K_mid, P_mid)。提升 K_mid 到父节点。
                    // 右页的哨兵应为 P_mid（对应右页首键的左侧指针）。
                    let (middle_key, right_sentinel_ptr) = {
                        let pair = moved.get(0).ok_or(QuillSQLError::Internal(
                            "Internal split moved entries empty".to_string(),
                        ))?;
                        (pair.0.clone(), pair.1)
                    };

                    // 右页插入哨兵 (Empty, P_mid)
                    new_internal.insert(Tuple::empty(self.key_schema.clone()), right_sentinel_ptr);

                    // 将剩余的键值对（严格大于 middle_key 的部分）批量放入右页
                    if moved.len() > 1 {
                        // 跳过 moved[0]（middle_key）
                        new_internal.batch_insert(moved.split_off(1));
                    }

                    // Set B-link wires:
                    // - Save old high_key and next pointer from left
                    let old_high_key = internal_page.high_key.clone();
                    let old_next = internal_page.header.next_page_id;
                    // - Left's high_key becomes middle_key
                    internal_page.high_key = Some(middle_key.clone());
                    // - Right's high_key inherits old left's high bound
                    new_internal.high_key = old_high_key;
                    // - Right's next pointer inherits left's next
                    new_internal.header.next_page_id = old_next;
                    let new_data = BPlusTreeInternalPageCodec::encode(&new_internal);
                    self.wal_overwrite_page(&mut new_page_guard, new_data)?;
                    // B-link: publish right sibling pointer for readers to chase
                    internal_page.header.next_page_id = new_page_id;
                    internal_page.header.version += 1;
                    if self.config.debug_split_level >= 2 {
                        eprintln!(
                            "[SPLIT DEBUG] internal_split left={} right={} promote_key={}",
                            page_id, new_page_id, middle_key
                        );
                    }
                    middle_key
                }
            };

            // 写回修改后的旧页面和新页面（保持子页锁直到父更新完成）
            let old_page_data = BPlusTreePageCodec::encode(&page);
            self.wal_overwrite_page(&mut page_guard, old_page_data)?;

            // 若当前分裂页是根（无父在 write_set），则创建新的根
            if page_guard.page_id() == self.get_root_page_id()? {
                if self.config.debug_split_level >= 2 {
                    eprintln!(
                        "[SPLIT DEBUG] root-split: old_root={}, new_right={}",
                        page_id, new_page_id
                    );
                }
                let mut new_root_guard = self.buffer_pool.new_page()?;
                let new_root_id = new_root_guard.page_id();
                let mut new_root_page =
                    BPlusTreeInternalPage::new(self.key_schema.clone(), self.internal_max_size);

                new_root_page.insert(Tuple::empty(self.key_schema.clone()), page_id);
                new_root_page.insert(middle_key, new_page_id);

                let encoded = BPlusTreeInternalPageCodec::encode(&new_root_page);
                self.wal_overwrite_page(&mut new_root_guard, encoded)?;

                // Avoid deadlock: release child page latches before taking header lock
                drop(new_page_guard);
                drop(page_guard);

                let _lock = self.header_page_lock.write();
                self.set_root_page_id(new_root_id)?;
                return Ok(());
            }

            // 否则，更新父节点
            let mut parent_guard = match context.write_set.pop_back() {
                Some(g) => g,
                None => {
                    // Parent missing due to concurrent structural change; restart from root
                    return Err(QuillSQLError::Internal(
                        "split: missing parent in context".to_string(),
                    ));
                }
            };
            if std::env::var("QUILL_DEBUG_SPLIT").ok().as_deref() == Some("2") {
                eprintln!(
                    "[SPLIT DEBUG] promote to parent={}, left={}, right={}",
                    parent_guard.page_id(),
                    page_id,
                    new_page_id
                );
            }
            let (mut parent_page, _) =
                BPlusTreeInternalPageCodec::decode(parent_guard.data(), self.key_schema.clone())?;
            // Insert the separator key right after the original left child (page_id)
            if parent_page.value_index(page_id).is_none() {
                // Parent no longer contains this child; signal upper layer to retry from root
                return Err(QuillSQLError::Internal(
                    "split: parent no longer contains left child".to_string(),
                ));
            }
            parent_page.insert_after(page_id, middle_key, new_page_id);
            parent_page.header.version += 1;

            let encoded = BPlusTreeInternalPageCodec::encode(&parent_page);
            self.wal_overwrite_page(&mut parent_guard, encoded)?;

            if parent_page.is_full() {
                // 子页到父的结构已一致，现在可释放子页锁，继续向上分裂父
                drop(new_page_guard);
                drop(page_guard);
                page_guard = parent_guard;
                // 不要释放更上层的锁，保持保守闩锁直到分裂完成
            } else {
                // 更新完成，释放子页锁，结束
                drop(new_page_guard);
                drop(page_guard);
                return Ok(());
            }
        }
    }

    /// Hint that a reader observed a globally invisible RID through this index.
    /// This increments a best-effort counter to help background cleanup scheduling.
    pub fn note_potential_garbage(&self, n: usize) {
        self.pending_garbage.fetch_add(n, AtomicOrdering::Relaxed);
    }

    /// Returns and resets the pending garbage counter.
    pub fn take_pending_garbage(&self) -> usize {
        self.pending_garbage.swap(0, AtomicOrdering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use parking_lot::deadlock;
    use std::sync::Arc;
    use std::sync::Once;
    use std::time::Duration;
    use tempfile::TempDir;

    use crate::catalog::SchemaRef;
    use crate::config::WalConfig;
    use crate::recovery::{RecoveryManager, WalManager};
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;
    use crate::storage::index::btree_index::TreeIndexIterator;
    use crate::storage::page::{BPlusTreePage, RecordId};
    use crate::storage::tuple::Tuple;
    use crate::{
        buffer::BufferManager,
        catalog::{Column, DataType, Schema},
        storage::codec::BPlusTreePageCodec,
    };

    use super::BPlusTreeIndex;

    fn ensure_deadlock_watchdog() {
        static START: Once = Once::new();
        START.call_once(|| {
            std::thread::spawn(|| loop {
                std::thread::sleep(Duration::from_millis(500));
                let deadlocks = deadlock::check_deadlock();
                if !deadlocks.is_empty() {
                    eprintln!("DEADLOCK DETECTED: {} cycles", deadlocks.len());
                    for (i, threads) in deadlocks.iter().enumerate() {
                        eprintln!("Cycle {}:", i);
                        for t in threads {
                            eprintln!("  ThreadId={:?}\n{:?}", t.thread_id(), t.backtrace());
                        }
                    }
                    panic!("deadlock detected");
                }
            });
        });
    }

    /// Creates a test environment with specified buffer pool size and B+ tree parameters
    fn create_test_index(
        buffer_pool_size: usize,
        internal_max_size: u32,
        leaf_max_size: u32,
    ) -> (TempDir, BPlusTreeIndex, SchemaRef) {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let key_schema = Arc::new(Schema::new(vec![Column::new("a", DataType::Int64, false)]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferManager::new(buffer_pool_size, disk_scheduler));
        let index = BPlusTreeIndex::new(
            key_schema.clone(),
            buffer_pool,
            internal_max_size,
            leaf_max_size,
        );

        (temp_dir, index, key_schema)
    }

    /// Helper: build bpm + wal on a fresh temp dir
    fn setup_with_wal(
        db_path: &std::path::Path,
        wal_dir: &std::path::Path,
        bpm_pages: usize,
    ) -> (Arc<BufferManager>, Arc<WalManager>, Arc<DiskScheduler>) {
        let dm = Arc::new(DiskManager::try_new(db_path).unwrap());
        let scheduler = Arc::new(DiskScheduler::new(dm));
        let mut cfg = WalConfig::default();
        cfg.directory = wal_dir.to_path_buf();
        let wal = Arc::new(WalManager::new(cfg, None, None).unwrap());
        let bpm = Arc::new(BufferManager::new(bpm_pages, scheduler.clone()));
        bpm.set_wal_manager(wal.clone());
        (bpm, wal, scheduler)
    }

    /// 根据 key 生成与 BusTub 一致的 RID，用于断言
    fn rid_from_key(key: i64) -> RecordId {
        let value = key & 0xFFFFFFFF;
        RecordId::new((key >> 32) as u32, value as u32)
    }

    #[test]
    fn test_index_recovery_replays_wal_split() {
        ensure_deadlock_watchdog();
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("test.db");
        let wal_dir = tmp.path().join("wal");

        // 1) 初始运行：带 WAL 的环境，强制小页触发分裂
        let (bpm, wal, scheduler) = setup_with_wal(&db_path, &wal_dir, 64);
        let key_schema: SchemaRef =
            Arc::new(Schema::new(vec![Column::new("a", DataType::Int64, false)]));
        let index = BPlusTreeIndex::new(key_schema.clone(), bpm.clone(), 2, 3);
        let header_pid = index.header_page_id; // 记录 header 页 id，崩溃后用来 reopen

        let keys: Vec<i64> = (1..=30).collect();
        for k in &keys {
            let t = Tuple::new(key_schema.clone(), vec![(*k).into()]);
            index.insert(&t, rid_from_key(*k)).unwrap();
        }

        // 刷 WAL 到耐久，模拟崩溃
        let _ = wal.flush(None).unwrap();
        drop(index);
        drop(bpm);
        drop(wal);
        drop(scheduler);

        // 2) 恢复阶段：重放 WAL 到数据文件
        let dm2 = Arc::new(DiskManager::try_new(&db_path).unwrap());
        let scheduler2 = Arc::new(DiskScheduler::new(dm2));
        let mut cfg2 = WalConfig::default();
        cfg2.directory = wal_dir.clone();
        let wal2 = Arc::new(WalManager::new(cfg2, None, None).unwrap());
        let rm = RecoveryManager::new(wal2.clone(), scheduler2.clone());
        let _summary = rm.replay().unwrap();

        // 3) 打开新的 BufferPool，reopen 索引并验证所有键可查询 & 有序遍历
        let bpm2 = Arc::new(BufferManager::new(128, scheduler2.clone()));
        let reopened = BPlusTreeIndex::open(key_schema.clone(), bpm2.clone(), 2, 3, header_pid);

        for k in &keys {
            let t = Tuple::new(key_schema.clone(), vec![(*k).into()]);
            let got = reopened
                .get(&t)
                .unwrap()
                .expect("missing key after recovery");
            assert_eq!(got.page_id, rid_from_key(*k).page_id);
            assert_eq!(got.slot_num, rid_from_key(*k).slot_num);
        }

        // 验证遍历顺序
        let index_arc = Arc::new(reopened);
        let mut it = TreeIndexIterator::new(index_arc, ..);
        let mut i = 1i64;
        while let Some(rid) = it.next().unwrap() {
            assert_eq!(rid.slot_num, rid_from_key(i).slot_num);
            i += 1;
        }
        assert_eq!(i, 31);
    }

    /// Helper function to create RID from i64 key (exactly like BusTub's RID construction)
    fn create_rid_from_key(key: i64) -> RecordId {
        let value = key & 0xFFFFFFFF;
        RecordId::new((key >> 32) as u32, value as u32)
    }

    /// Helper function to create tuple from i64 key
    fn create_tuple_from_key(key: i64, schema: SchemaRef) -> Tuple {
        Tuple::new(schema, vec![key.into()])
    }

    /// TEST: BasicInsertTest - equivalent to BusTub's basic insert test
    #[test]
    fn test_basic_insert() {
        let (_temp_dir, index, key_schema) = create_test_index(50, 2, 3);

        let key = 42i64;
        let tuple = create_tuple_from_key(key, key_schema.clone());
        let rid = create_rid_from_key(key);

        index.insert(&tuple, rid).unwrap();

        let root_page_id = index.get_root_page_id().unwrap();
        assert_ne!(root_page_id, crate::buffer::INVALID_PAGE_ID);

        let root_guard = index.buffer_pool.fetch_page_read(root_page_id).unwrap();
        let (root_page, _) =
            BPlusTreePageCodec::decode(root_guard.data(), key_schema.clone()).unwrap();

        assert!(matches!(root_page, BPlusTreePage::Leaf(_)));

        if let BPlusTreePage::Leaf(root_as_leaf) = root_page {
            assert_eq!(root_as_leaf.header.current_size, 1);
            assert_eq!(root_as_leaf.array[0].0, tuple);
            assert_eq!(root_as_leaf.array[0].1, rid);
        }
    }

    /// TEST: InsertTest1NoIterator - equivalent to BusTub's insert test without iterator
    #[test]
    fn test_insert_no_iterator() {
        let (_temp_dir, index, key_schema) = create_test_index(50, 2, 3);

        let keys = vec![1i64, 2, 3, 4, 5];
        for key in &keys {
            let tuple = create_tuple_from_key(*key, key_schema.clone());
            let rid = create_rid_from_key(*key);
            index.insert(&tuple, rid).unwrap();
        }

        for key in &keys {
            let tuple = create_tuple_from_key(*key, key_schema.clone());
            let result = index.get(&tuple).unwrap();
            assert!(result.is_some(), "missing key {}", key);

            let expected_rid = create_rid_from_key(*key);
            let actual_rid = result.unwrap();
            assert_eq!(actual_rid.page_id, expected_rid.page_id);
            assert_eq!(actual_rid.slot_num, expected_rid.slot_num);
        }
    }

    /// TEST: InsertTest2 - insert in reverse order with iterator validation
    #[test]
    fn test_insert_reverse_order() {
        let (_temp_dir, index, key_schema) = create_test_index(50, 2, 3);

        let keys = vec![5i64, 4, 3, 2, 1];
        for key in &keys {
            let tuple = create_tuple_from_key(*key, key_schema.clone());
            let rid = create_rid_from_key(*key);
            index.insert(&tuple, rid).unwrap();
        }

        for key in &keys {
            let tuple = create_tuple_from_key(*key, key_schema.clone());
            let result = index.get(&tuple).unwrap();
            assert!(result.is_some(), "missing key {}", key);

            let expected_rid = create_rid_from_key(*key);
            let actual_rid = result.unwrap();
            assert_eq!(actual_rid.slot_num, expected_rid.slot_num);
        }

        // Test iterator order (should be sorted)
        let index_arc = Arc::new(index);
        let mut iter = TreeIndexIterator::new(index_arc, ..);
        let mut current_key = 1i64;
        while let Some(rid) = iter.next().unwrap() {
            assert_eq!(rid.slot_num as i64, current_key);
            current_key += 1;
        }
        assert_eq!(current_key, keys.len() as i64 + 1);
    }

    /// TEST: DeleteTestNoIterator - equivalent to BusTub's delete test without iterator
    #[test]
    fn test_delete_no_iterator() {
        let (_temp_dir, index, key_schema) = create_test_index(50, 2, 3);

        let keys = vec![1i64, 2, 3, 4, 5];
        for key in &keys {
            let tuple = create_tuple_from_key(*key, key_schema.clone());
            let rid = create_rid_from_key(*key);
            index.insert(&tuple, rid).unwrap();
        }

        // Verify all keys are present and have correct RIDs
        for key in &keys {
            let tuple = create_tuple_from_key(*key, key_schema.clone());
            let result = index.get(&tuple).unwrap();
            assert!(result.is_some());

            let expected_rid = create_rid_from_key(*key);
            let actual_rid = result.unwrap();
            assert_eq!(actual_rid.page_id, expected_rid.page_id);
            assert_eq!(actual_rid.slot_num, expected_rid.slot_num);
        }

        // Remove some keys - exactly like BusTub test
        let remove_keys = vec![1i64, 5, 3, 4];
        for key in &remove_keys {
            println!("Before deleting key {}:\n{}", key, index.to_dot().unwrap());
            let tuple = create_tuple_from_key(*key, key_schema.clone());
            index.delete(&tuple).unwrap();
            println!("After deleting key {}:\n{}", key, index.to_dot().unwrap());
        }

        let mut size = 0;
        for key in &keys {
            let tuple = create_tuple_from_key(*key, key_schema.clone());
            let is_present = index.get(&tuple).unwrap().is_some();

            if !is_present {
                assert!(remove_keys.contains(key));
            } else {
                assert!(!remove_keys.contains(key));
                assert_eq!(
                    index.get(&tuple).unwrap().unwrap().slot_num,
                    (*key & 0xFFFFFFFF) as u32
                );
                size += 1;
            }
        }
        assert_eq!(size, 1);

        // Remove the remaining key and check if tree becomes empty
        let tuple = create_tuple_from_key(2, key_schema.clone());
        println!("Before deleting final key 2:\n{}", index.to_dot().unwrap());
        index.delete(&tuple).unwrap();
        println!("After deleting final key 2:\n{}", index.to_dot().unwrap());
        assert!(index.is_empty().unwrap());
    }

    #[test]
    fn test_internal_borrow_from_right_keeps_searchable() {
        let (_temp_dir, index, key_schema) = create_test_index(64, 2, 3);

        let inserts = [
            -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
        ];
        for key in inserts {
            let tuple = create_tuple_from_key(key, key_schema.clone());
            let rid = create_rid_from_key(key);
            index.insert(&tuple, rid).unwrap();
        }

        let deletes = [-5, -4, -3, -2, -1, 0, 1, 2];
        for key in deletes {
            let tuple = create_tuple_from_key(key, key_schema.clone());
            index.delete(&tuple).unwrap();
        }

        let root_page_id = index.get_root_page_id().unwrap();
        let root_guard = index.buffer_pool.fetch_page_read(root_page_id).unwrap();
        let (root_page, _) =
            BPlusTreePageCodec::decode(root_guard.data(), key_schema.clone()).unwrap();
        let (left_internal_id, right_internal_id) =
            if let BPlusTreePage::Internal(root_internal) = root_page {
                assert_eq!(root_internal.header.current_size, 2);
                (root_internal.value_at(0), root_internal.value_at(1))
            } else {
                panic!("expected internal root after deletions");
            };
        drop(root_guard);

        let left_guard = index.buffer_pool.fetch_page_read(left_internal_id).unwrap();
        let (left_page, _) =
            BPlusTreePageCodec::decode(left_guard.data(), key_schema.clone()).unwrap();
        if let BPlusTreePage::Internal(left_internal) = left_page {
            assert_eq!(
                left_internal.high_key,
                Some(create_tuple_from_key(7, key_schema.clone()))
            );
            assert_eq!(left_internal.header.next_page_id, right_internal_id);
        } else {
            panic!("expected left child to remain internal");
        }

        let survivor = create_tuple_from_key(3, key_schema.clone());
        assert!(index.get(&survivor).unwrap().is_some());
    }

    /// TEST: SequentialEdgeMixTest - equivalent to BusTub's mixed insert/delete test
    #[test]
    fn test_sequential_edge_mix() {
        ensure_deadlock_watchdog();
        for leaf_max_size in 2..=5 {
            let (_temp_dir, index, key_schema) = create_test_index(50, 3, leaf_max_size);

            let keys = vec![1i64, 5, 15, 20, 25, 2, -1, -2, 6, 14, 4];
            let mut inserted = vec![];
            let mut deleted = vec![];

            for key in &keys {
                let tuple = create_tuple_from_key(*key, key_schema.clone());
                let rid = create_rid_from_key(*key);
                index.insert(&tuple, rid).unwrap();
                inserted.push(*key);

                // Verify all inserted keys are present and deleted keys are absent
                verify_tree_state(&index, &key_schema, &inserted, &deleted);
            }

            // Delete key 1
            let tuple = create_tuple_from_key(1, key_schema.clone());
            index.delete(&tuple).unwrap();
            deleted.push(1);
            inserted.retain(|&x| x != 1);
            verify_tree_state(&index, &key_schema, &inserted, &deleted);

            // Insert key 3
            let tuple = create_tuple_from_key(3, key_schema.clone());
            let rid = create_rid_from_key(3);
            index.insert(&tuple, rid).unwrap();
            inserted.push(3);
            verify_tree_state(&index, &key_schema, &inserted, &deleted);

            // Delete remaining keys
            let delete_keys = vec![4i64, 14, 6, 2, 15, -2, -1, 3, 5, 25, 20];
            for key in &delete_keys {
                let tuple = create_tuple_from_key(*key, key_schema.clone());
                index.delete(&tuple).unwrap();
                deleted.push(*key);
                inserted.retain(|&x| x != *key);
                verify_tree_state(&index, &key_schema, &inserted, &deleted);
            }
        }
    }

    /// Helper function to verify tree state
    fn verify_tree_state(
        index: &BPlusTreeIndex,
        key_schema: &SchemaRef,
        inserted: &[i64],
        deleted: &[i64],
    ) {
        for key in inserted {
            let tuple = create_tuple_from_key(*key, key_schema.clone());
            let result = index.get(&tuple).unwrap();
            assert!(result.is_some(), "Key {} should be present", key);
        }

        for key in deleted {
            let tuple = create_tuple_from_key(*key, key_schema.clone());
            let result = index.get(&tuple).unwrap();
            assert!(result.is_none(), "Key {} should be deleted", key);
        }
    }

    /// TEST: Concurrent insert test - multi-threaded insertions
    #[test]
    fn test_concurrent_insert() {
        // spawn deadlock watchdog
        std::thread::spawn(|| loop {
            std::thread::sleep(Duration::from_millis(500));
            let deadlocks = deadlock::check_deadlock();
            if !deadlocks.is_empty() {
                eprintln!("DEADLOCK DETECTED: {} cycles", deadlocks.len());
                for (i, threads) in deadlocks.iter().enumerate() {
                    eprintln!("Cycle {}:", i);
                    for t in threads {
                        eprintln!("  ThreadId={:?}\n{:?}", t.thread_id(), t.backtrace());
                    }
                }
                panic!("deadlock detected");
            }
        });
        const NUM_ITERS: usize = 3;
        const SCALE_FACTOR: i64 = 50; // further scaled down for fast diagnostics
        const NUM_THREADS: usize = 5;

        for _iter in 0..NUM_ITERS {
            let (_temp_dir, index, key_schema) = create_test_index(64, 3, 5);
            let index = Arc::new(index);

            let keys: Vec<i64> = (1..SCALE_FACTOR).collect();
            let mut threads = vec![];

            for i in 0..NUM_THREADS {
                let index_clone = index.clone();
                let key_schema_clone = key_schema.clone();
                let keys_clone = keys.clone();

                threads.push(std::thread::spawn(move || {
                    for key in &keys_clone {
                        if (*key as usize) % NUM_THREADS == i {
                            let tuple = create_tuple_from_key(*key, key_schema_clone.clone());
                            let rid = create_rid_from_key(*key);
                            index_clone.insert(&tuple, rid).unwrap();
                        }
                    }
                }));
            }

            for thread in threads {
                thread.join().unwrap();
            }

            // Verify all keys were inserted correctly
            for key in &keys {
                let tuple = create_tuple_from_key(*key, key_schema.clone());
                let result = index.get(&tuple).unwrap();
                assert!(result.is_some());
                assert_eq!(result.unwrap().slot_num, (*key & 0xFFFFFFFF) as u32);
            }

            // Test iterator order
            let mut iter = TreeIndexIterator::new(index.clone(), ..);
            let mut current_key = 1i64;
            while let Some(rid) = iter.next().unwrap() {
                assert_eq!(rid.slot_num, (current_key & 0xFFFFFFFF) as u32);
                current_key += 1;
            }
            assert_eq!(current_key, keys.len() as i64 + 1);
        }
    }

    /// TEST: BasicScaleTest - equivalent to BusTub's large scale insertion test
    #[test]
    fn test_basic_scale() {
        let (_temp_dir, index, key_schema) = create_test_index(512, 2, 3);

        let scale = 1000i64;
        let mut keys: Vec<i64> = (1..=scale).collect();

        // Shuffle keys to randomize insertion order
        let mut seed: u64 = 0x9E3779B97F4A7C15;
        for i in (1..keys.len()).rev() {
            seed = seed
                .wrapping_mul(2862933555777941757)
                .wrapping_add(3037000493);
            let j = (seed as usize) % (i + 1);
            keys.swap(i, j);
        }

        // Insert all keys and verify immediately
        for key in &keys {
            let tuple = create_tuple_from_key(*key, key_schema.clone());
            let rid = create_rid_from_key(*key);
            index.insert(&tuple, rid).unwrap();
            let got = index.get(&tuple).unwrap();
            if got.is_none() {
                println!(
                    "After inserting {}, tree=\n{}",
                    key,
                    index.to_dot().unwrap()
                );
                panic!("immediate missing key {}", key);
            }
        }

        // // Debug view
        // println!(
        //     "Tree after {} inserts:\n{}",
        //     keys.len(),
        //     index.to_dot().unwrap()
        // );

        // Early probe for a known-missing sample to print context
        let probe = 630i64;
        let tuple = create_tuple_from_key(probe, key_schema.clone());
        if index.get(&tuple).unwrap().is_none() {
            let guard = index.find_leaf_page_optimistic(&tuple).unwrap();
            let (page, _) = BPlusTreePageCodec::decode(guard.data(), key_schema.clone()).unwrap();
            if let BPlusTreePage::Leaf(leaf) = page {
                println!(
                    "Early probe leaf for 630 has keys: {:?}",
                    leaf.array
                        .iter()
                        .map(|(t, _)| format!("{}", t))
                        .collect::<Vec<_>>()
                );
            }
            panic!("missing key 630 before verification loop");
        }

        // Verify all keys are retrievable
        for key in &keys {
            let tuple = create_tuple_from_key(*key, key_schema.clone());
            let result = index.get(&tuple).unwrap();
            assert!(result.is_some(), "missing key {}", key);

            let expected_rid = create_rid_from_key(*key);
            let actual_rid = result.unwrap();
            assert_eq!(actual_rid.slot_num, expected_rid.slot_num);
        }

        // Focus probe around 630 for debugging
        let probe = 630i64;
        let tuple = create_tuple_from_key(probe, key_schema.clone());
        if index.get(&tuple).unwrap().is_none() {
            let guard = index.find_leaf_page_optimistic(&tuple).unwrap();
            let (page, _) = BPlusTreePageCodec::decode(guard.data(), key_schema.clone()).unwrap();
            if let BPlusTreePage::Leaf(leaf) = page {
                println!(
                    "Probe leaf for 630 has keys: {:?}",
                    leaf.array
                        .iter()
                        .map(|(t, _)| format!("{}", t))
                        .collect::<Vec<_>>()
                );
            }
        }
    }

    /// TEST: Concurrent delete test
    #[test]
    fn test_concurrent_delete() {
        const NUM_ITERS: usize = 10;

        for _iter in 0..NUM_ITERS {
            let (_temp_dir, index, key_schema) = create_test_index(50, 3, 5);

            // Sequential insert
            let keys = vec![1i64, 2, 3, 4, 5];
            for key in &keys {
                let tuple = create_tuple_from_key(*key, key_schema.clone());
                let rid = create_rid_from_key(*key);
                index.insert(&tuple, rid).unwrap();
            }

            let index = Arc::new(index);
            let remove_keys = vec![1i64, 5, 3, 4];
            let mut threads = vec![];

            for i in 0..2 {
                let index_clone = index.clone();
                let key_schema_clone = key_schema.clone();
                let remove_keys_clone = remove_keys.clone();

                threads.push(std::thread::spawn(move || {
                    for key in &remove_keys_clone {
                        if (*key as usize) % 2 == i {
                            let tuple = create_tuple_from_key(*key, key_schema_clone.clone());
                            index_clone.delete(&tuple).unwrap();
                        }
                    }
                }));
            }

            for thread in threads {
                thread.join().unwrap();
            }

            // Check that only key 2 remains
            let mut size = 0;
            let index_arc = index.clone();
            let mut iter = TreeIndexIterator::new(index_arc, ..);
            while let Some(rid) = iter.next().unwrap() {
                assert_eq!(rid.slot_num, 2);
                size += 1;
            }
            assert_eq!(size, 1);
        }
    }

    /// TEST: Mixed concurrent operations
    #[test]
    fn test_concurrent_mix() {
        // spawn deadlock watchdog
        std::thread::spawn(|| loop {
            std::thread::sleep(Duration::from_millis(500));
            let deadlocks = deadlock::check_deadlock();
            if !deadlocks.is_empty() {
                eprintln!("DEADLOCK DETECTED: {} cycles", deadlocks.len());
                for (i, threads) in deadlocks.iter().enumerate() {
                    eprintln!("Cycle {}:", i);
                    for t in threads {
                        eprintln!("  ThreadId={:?}\n{:?}", t.thread_id(), t.backtrace());
                    }
                }
                panic!("deadlock detected");
            }
        });
        const NUM_ITERS: usize = 5;

        for _iter in 0..NUM_ITERS {
            let (_temp_dir, index, key_schema) = create_test_index(50, 3, 5);
            let index = Arc::new(index);

            // Divide keys for insert and delete
            let mut for_insert = vec![];
            let mut for_delete = vec![];
            let total_keys = 20i64; // further scaled down for diagnostics

            for i in 1..=total_keys {
                if i % 2 == 0 {
                    for_insert.push(i);
                } else {
                    for_delete.push(i);
                }
            }

            // Insert all keys to delete first
            for key in &for_delete {
                let tuple = create_tuple_from_key(*key, key_schema.clone());
                let rid = create_rid_from_key(*key);
                index.insert(&tuple, rid).unwrap();
            }

            let mut threads = vec![];
            let num_threads = 5;

            for i in 0..num_threads {
                let index_clone = index.clone();
                let key_schema_clone = key_schema.clone();
                let for_insert_clone = for_insert.clone();
                let for_delete_clone = for_delete.clone();

                threads.push(std::thread::spawn(move || {
                    if i % 2 == 0 {
                        // Insert thread
                        for key in &for_insert_clone {
                            let tuple = create_tuple_from_key(*key, key_schema_clone.clone());
                            let rid = create_rid_from_key(*key);
                            index_clone.insert(&tuple, rid).unwrap();
                        }
                    } else {
                        // Delete thread
                        for key in &for_delete_clone {
                            let tuple = create_tuple_from_key(*key, key_schema_clone.clone());
                            index_clone.delete(&tuple).unwrap();
                        }
                    }
                }));
            }

            for thread in threads {
                thread.join().unwrap();
            }

            // Verify only inserted keys remain
            let mut count = 0;
            for key in &for_insert {
                let tuple = create_tuple_from_key(*key, key_schema.clone());
                let result = index.get(&tuple).unwrap();
                if result.is_some() {
                    count += 1;
                }
            }
            assert_eq!(count, for_insert.len());

            // Verify deleted keys are gone
            for key in &for_delete {
                let tuple = create_tuple_from_key(*key, key_schema.clone());
                let result = index.get(&tuple).unwrap();
                assert!(result.is_none());
            }
        }
    }

    /// TEST: Iterator functionality test
    #[test]
    fn test_iterator_functionality() {
        let (_temp_dir, index, key_schema) = create_test_index(50, 3, 5);

        // Insert test data
        let keys = vec![1i64, 3, 5, 7, 9];
        for key in &keys {
            let tuple = create_tuple_from_key(*key, key_schema.clone());
            let rid = create_rid_from_key(*key);
            index.insert(&tuple, rid).unwrap();
        }

        let index = Arc::new(index);

        // Test range iterator [3, 7]
        let start_tuple = create_tuple_from_key(3, key_schema.clone());
        let end_tuple = create_tuple_from_key(7, key_schema.clone());
        let mut iterator = TreeIndexIterator::new(index.clone(), start_tuple..=end_tuple);

        let mut results = vec![];
        while let Some(rid) = iterator.next().unwrap() {
            results.push(rid.slot_num as i64);
        }
        assert_eq!(results, vec![3, 5, 7]);

        // Test unbounded iterator
        let mut iterator = TreeIndexIterator::new(index.clone(), ..);
        let mut all_results = vec![];
        while let Some(rid) = iterator.next().unwrap() {
            all_results.push(rid.slot_num as i64);
        }
        assert_eq!(all_results, vec![1, 3, 5, 7, 9]);
    }

    /// TEST: Tree structure tests - leaf split
    #[test]
    fn test_leaf_split_structure() {
        let (_temp_dir, index, key_schema) = create_test_index(50, 10, 3);

        // Insert keys to trigger leaf split
        for k in [1i64, 2, 3, 4] {
            let tuple = create_tuple_from_key(k, key_schema.clone());
            let rid = create_rid_from_key(k);
            index.insert(&tuple, rid).unwrap();
        }

        let root_page_id = index.get_root_page_id().unwrap();
        let root_guard = index.buffer_pool.fetch_page_read(root_page_id).unwrap();
        let (root_page, _) =
            BPlusTreePageCodec::decode(root_guard.data(), key_schema.clone()).unwrap();

        // Root should be internal page after split
        let BPlusTreePage::Internal(root_internal) = root_page else {
            panic!("root is not internal after leaf split");
        };

        // Check structure integrity
        let left_pid = root_internal.value_at(0);
        let right_pid = root_internal.value_at(1);

        let left_guard = index.buffer_pool.fetch_page_read(left_pid).unwrap();
        let right_guard = index.buffer_pool.fetch_page_read(right_pid).unwrap();
        let (left_page, _) =
            BPlusTreePageCodec::decode(left_guard.data(), key_schema.clone()).unwrap();
        let (right_page, _) =
            BPlusTreePageCodec::decode(right_guard.data(), key_schema.clone()).unwrap();

        let BPlusTreePage::Leaf(left_leaf) = left_page else {
            panic!("left child not leaf");
        };
        let BPlusTreePage::Leaf(_right_leaf) = right_page else {
            panic!("right child not leaf");
        };

        // Verify leaf chain linkage
        assert_eq!(left_leaf.header.next_page_id, right_pid);
    }

    // ---------------- Benchmarks (ignored by default) ----------------
    #[test]
    #[ignore]
    fn bench_get_hot_read() {
        fn getenv_usize(k: &str, default_v: usize) -> usize {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default_v)
        }

        let bpm = getenv_usize("QUILL_BENCH_BPM", 1024);
        let nkeys = getenv_usize("QUILL_BENCH_N", 20000) as i64;
        let ops = getenv_usize("QUILL_BENCH_OPS", 200000);

        let (_temp_dir, index, key_schema) = create_test_index(bpm, 3, 64);

        // Prepare keys and insert (shuffled)
        let mut keys: Vec<i64> = (1..=nkeys).collect();
        // simple LCG shuffle
        let mut seed: u64 = 0x9E3779B97F4A7C15;
        for i in (1..keys.len()).rev() {
            seed = seed
                .wrapping_mul(2862933555777941757)
                .wrapping_add(3037000493);
            let j = (seed as usize) % (i + 1);
            keys.swap(i, j);
        }
        for k in &keys {
            let t = create_tuple_from_key(*k, key_schema.clone());
            index.insert(&t, create_rid_from_key(*k)).unwrap();
        }

        // Hot set: last 10% keys
        let hot_start = (nkeys as usize * 9) / 10;
        let hot = &keys[hot_start..];

        let start = std::time::Instant::now();
        let mut found = 0usize;
        // query stream via LCG over hot set
        let mut x = 0x243F6A8885A308D3u64;
        for _ in 0..ops {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
            let idx = (x as usize) % hot.len();
            let key = hot[idx];
            let t = create_tuple_from_key(key, key_schema.clone());
            if index.get(&t).unwrap().is_some() {
                found += 1;
            }
        }
        let el = start.elapsed();
        let qps = (ops as f64) / el.as_secs_f64();
        println!(
            "bench_get_hot_read: ops={} time={:?} qps={:.1} found={}",
            ops, el, qps, found
        );
    }

    #[test]
    #[ignore]
    fn bench_range_scan() {
        fn getenv_usize(k: &str, default_v: usize) -> usize {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default_v)
        }

        let bpm = getenv_usize("QUILL_BENCH_BPM", 1024);
        let nkeys = getenv_usize("QUILL_BENCH_N", 30000) as i64;
        let passes = getenv_usize("QUILL_BENCH_PASSES", 20);

        let (_temp_dir, index, key_schema) = create_test_index(bpm, 3, 64);

        // Insert in random order
        let mut keys: Vec<i64> = (1..=nkeys).collect();
        let mut seed: u64 = 0x9E3779B97F4A7C15;
        for i in (1..keys.len()).rev() {
            seed = seed
                .wrapping_mul(2862933555777941757)
                .wrapping_add(3037000493);
            let j = (seed as usize) % (i + 1);
            keys.swap(i, j);
        }
        for k in &keys {
            let t = create_tuple_from_key(*k, key_schema.clone());
            index.insert(&t, create_rid_from_key(*k)).unwrap();
        }

        let index = Arc::new(index);
        let total = (nkeys as usize) * passes;
        let start = std::time::Instant::now();
        let mut seen = 0usize;
        for _ in 0..passes {
            let mut it = TreeIndexIterator::new(index.clone(), ..);
            while let Some(_rid) = it.next().unwrap() {
                seen += 1;
            }
        }
        let el = start.elapsed();
        let tps = (total as f64) / el.as_secs_f64();
        println!(
            "bench_range_scan: items={} time={:?} ips={:.1} seen={}",
            total, el, tps, seen
        );
    }
}
