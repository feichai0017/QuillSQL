use crate::buffer::{AtomicPageId, PageId, WritePageGuard, INVALID_PAGE_ID};
use crate::catalog::{SchemaRef, EMPTY_SCHEMA_REF};
use crate::config::TableScanConfig;
use crate::storage::codec::TablePageCodec;
use crate::storage::disk_scheduler::{DiskCommandResultReceiver, DiskScheduler};
use crate::storage::page::{RecordId, TablePage, TupleMeta, INVALID_RID};
use crate::transaction::{CommandId, TransactionId, INVALID_COMMAND_ID};
use crate::{
    buffer::BufferManager,
    error::{QuillSQLError, QuillSQLResult},
};
use bytes::BytesMut;
use std::collections::Bound;
use std::collections::VecDeque;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::recovery::wal_record::WalRecordPayload;
use crate::storage::codec::TupleCodec;
use crate::storage::heap::wal_codec::{
    HeapDeletePayload, HeapInsertPayload, HeapRecordPayload, HeapUpdatePayload, RelationIdent,
    TupleMetaRepr,
};
use crate::storage::tuple::Tuple;
use crate::utils::ring_buffer::RingBuffer;

#[derive(Debug)]
pub struct TableHeap {
    pub schema: SchemaRef,
    pub buffer_pool: Arc<BufferManager>,
    pub first_page_id: AtomicPageId,
    pub last_page_id: AtomicPageId,
}

impl TableHeap {
    /// Creates a new table heap. This involves allocating an initial page.
    pub fn try_new(schema: SchemaRef, buffer_pool: Arc<BufferManager>) -> QuillSQLResult<Self> {
        // new_page() returns a WritePageGuard.
        let mut first_page_guard = buffer_pool.new_page()?;
        let first_page_id = first_page_guard.page_id();

        // Initialize the first page as an empty TablePage.
        let table_page = TablePage::new(schema.clone(), INVALID_PAGE_ID);
        let encoded_data = TablePageCodec::encode(&table_page);

        // Use DerefMut to get a mutable reference and update the page data.
        // This also marks the page as dirty automatically.
        first_page_guard.data_mut().copy_from_slice(&encoded_data);
        first_page_guard.set_lsn(table_page.lsn());

        // The first_page_guard is dropped here, automatically unpinning the page.
        Ok(Self {
            schema,
            buffer_pool,
            first_page_id: AtomicU32::new(first_page_id),
            last_page_id: AtomicU32::new(first_page_id),
        })
    }

    fn write_back_page(
        &self,
        guard: &mut WritePageGuard,
        table_page: &mut TablePage,
    ) -> QuillSQLResult<()> {
        let new_image = TablePageCodec::encode(table_page);
        guard.apply_page_image(&new_image)?;
        Ok(())
    }

    pub(crate) fn relation_ident(&self) -> RelationIdent {
        RelationIdent {
            root_page_id: self.first_page_id.load(Ordering::SeqCst),
        }
    }

    fn append_heap_record(&self, payload: HeapRecordPayload) -> QuillSQLResult<()> {
        if let Some(wal) = self.buffer_pool.wal_manager() {
            let _wal_result =
                wal.append_record_with(|_| WalRecordPayload::Heap(payload.clone()))?;
        }
        Ok(())
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
                TablePageCodec::decode(current_page_guard.data(), self.schema.clone())?.0;
            table_page.set_lsn(current_page_guard.lsn());

            if table_page.next_tuple_offset(tuple).is_ok() {
                let tuple_bytes = TupleCodec::encode(tuple);
                let slot_id = table_page.insert_tuple(meta, tuple)?;
                let relation = self.relation_ident();
                let tuple_meta = TupleMetaRepr::from(*meta);
                let payload = HeapRecordPayload::Insert(HeapInsertPayload {
                    relation,
                    page_id: current_page_id,
                    slot_id,
                    op_txn_id: meta.insert_txn_id,
                    tuple_meta,
                    tuple_data: tuple_bytes,
                });
                self.append_heap_record(payload.clone())?;
                self.write_back_page(&mut current_page_guard, &mut table_page)?;
                return Ok(RecordId::new(current_page_id, slot_id as u32));
            }

            let mut new_page_guard = self.buffer_pool.new_page()?;
            let new_page_id = new_page_guard.page_id();
            let mut new_table_page = TablePage::new(self.schema.clone(), INVALID_PAGE_ID);
            self.write_back_page(&mut new_page_guard, &mut new_table_page)?;

            table_page.header.next_page_id = new_page_id;
            self.write_back_page(&mut current_page_guard, &mut table_page)?;
            drop(current_page_guard);

            self.last_page_id.store(new_page_id, Ordering::SeqCst);
            current_page_id = new_page_id;
        }
    }

    /// MVCC helper: insert a new tuple version with optional link to a previous version.
    pub fn mvcc_insert_version(
        &self,
        tuple: &Tuple,
        txn_id: TransactionId,
        cid: CommandId,
        prev_version: Option<RecordId>,
    ) -> QuillSQLResult<(RecordId, TupleMeta)> {
        let mut meta = TupleMeta::new(txn_id, cid);
        meta.set_prev_version(prev_version);
        let rid = self.insert_tuple(&meta, tuple)?;
        Ok((rid, meta))
    }

    /// Create a new tuple version for UPDATE while marking the source version deleted.
    /// Returns the RecordId of the new version and the original metadata (pre-change) for undo.
    pub fn mvcc_update(
        &self,
        current_rid: RecordId,
        new_tuple: Tuple,
        txn_id: TransactionId,
        cid: CommandId,
    ) -> QuillSQLResult<(RecordId, TupleMeta)> {
        let (current_meta, _existing_tuple) = self.full_tuple(current_rid)?;
        if current_meta.is_deleted && current_meta.next_version.is_some() {
            return Err(QuillSQLError::Execution(format!(
                "tuple {} has already been updated",
                current_rid
            )));
        }

        // Capture previous metadata for undo before mutation.
        let prev_meta = current_meta;

        // Insert new physical version linked to the current record.
        let (new_rid, mut new_meta) =
            self.mvcc_insert_version(&new_tuple, txn_id, cid, Some(current_rid))?;
        new_meta.set_prev_version(Some(current_rid));

        // Update the source version metadata to mark it deleted and link forward.
        let mut updated_meta = current_meta;
        updated_meta.mark_deleted(txn_id, cid);
        updated_meta.set_next_version(Some(new_rid));
        self.update_tuple_meta(updated_meta, current_rid)?;

        Ok((new_rid, prev_meta))
    }

    /// Mark a tuple version as deleted for MVCC-aware DELETE operations.
    /// Returns the previous metadata prior to marking deleted (for undo purposes).
    pub fn mvcc_mark_deleted(
        &self,
        rid: RecordId,
        txn_id: TransactionId,
        cid: CommandId,
    ) -> QuillSQLResult<TupleMeta> {
        let (mut current_meta, _) = self.full_tuple(rid)?;
        if current_meta.is_deleted {
            return Ok(current_meta);
        }
        let prev_meta = current_meta;
        current_meta.mark_deleted(txn_id, cid);
        self.update_tuple_meta(current_meta, rid)?;
        Ok(prev_meta)
    }

    /// Remove a heap version created by an aborted operation. For safety this currently
    /// expects the version to live at the tail of the page; otherwise it simply marks the slot
    /// deleted and leaves full reclamation to vacuum.
    #[allow(dead_code)]
    pub fn mvcc_remove_version(&self, rid: RecordId) -> QuillSQLResult<()> {
        let mut page_guard = self.buffer_pool.fetch_page_write(rid.page_id)?;
        let mut table_page = TablePageCodec::decode(page_guard.data(), self.schema.clone())?.0;
        table_page.set_lsn(page_guard.lsn());

        let slot = rid.slot_num as usize;
        if slot >= table_page.header.num_tuples as usize {
            return Ok(());
        }

        if slot + 1 != table_page.header.num_tuples as usize {
            // Non-tail removal: mark deleted and let vacuum reclaim later.
            let mut meta = table_page.header.tuple_infos[slot].meta;
            if !meta.is_deleted {
                meta.mark_deleted(0, INVALID_COMMAND_ID);
                table_page.update_tuple_meta(meta, slot as u16)?;
                self.write_back_page(&mut page_guard, &mut table_page)?;
            }
            return Ok(());
        }

        table_page.reclaim_tuple(slot as u16)?;
        self.write_back_page(&mut page_guard, &mut table_page)
    }

    pub fn update_tuple(&self, rid: RecordId, tuple: Tuple) -> QuillSQLResult<()> {
        let mut page_guard = self.buffer_pool.fetch_page_write(rid.page_id)?;
        let mut table_page = TablePageCodec::decode(page_guard.data(), self.schema.clone())?.0;
        table_page.set_lsn(page_guard.lsn());

        let slot = rid.slot_num as u16;
        let (old_meta, old_tuple) = table_page.tuple(slot)?;
        let old_tuple_bytes = TupleCodec::encode(&old_tuple);
        let new_tuple_bytes = TupleCodec::encode(&tuple);
        table_page.update_tuple(tuple, rid.slot_num as u16)?;
        let new_meta = table_page.header.tuple_infos[slot as usize].meta;
        let relation = self.relation_ident();
        self.append_heap_record(HeapRecordPayload::Update(HeapUpdatePayload {
            relation,
            page_id: rid.page_id,
            slot_id: slot,
            op_txn_id: new_meta.insert_txn_id,
            new_tuple_meta: TupleMetaRepr::from(new_meta),
            new_tuple_data: new_tuple_bytes,
            old_tuple_meta: Some(TupleMetaRepr::from(old_meta)),
            old_tuple_data: Some(old_tuple_bytes),
        }))?;
        self.write_back_page(&mut page_guard, &mut table_page)
    }

    pub fn update_tuple_meta(&self, meta: TupleMeta, rid: RecordId) -> QuillSQLResult<()> {
        let mut page_guard = self.buffer_pool.fetch_page_write(rid.page_id)?;
        let mut table_page = TablePageCodec::decode(page_guard.data(), self.schema.clone())?.0;
        table_page.set_lsn(page_guard.lsn());

        let slot = rid.slot_num as u16;
        let (old_meta, old_tuple) = table_page.tuple(slot)?;
        let old_tuple_bytes = TupleCodec::encode(&old_tuple);
        table_page.update_tuple_meta(meta, slot)?;
        let relation = self.relation_ident();
        let payload = if meta.is_deleted && !old_meta.is_deleted {
            HeapRecordPayload::Delete(HeapDeletePayload {
                relation,
                page_id: rid.page_id,
                slot_id: slot,
                op_txn_id: meta.delete_txn_id,
                old_tuple_meta: TupleMetaRepr::from(old_meta),
                old_tuple_data: Some(old_tuple_bytes),
            })
        } else {
            let (_, current_tuple) = table_page.tuple(slot)?;
            let new_tuple_bytes = TupleCodec::encode(&current_tuple);
            HeapRecordPayload::Update(HeapUpdatePayload {
                relation,
                page_id: rid.page_id,
                slot_id: slot,
                op_txn_id: meta.insert_txn_id,
                new_tuple_meta: TupleMetaRepr::from(meta),
                new_tuple_data: new_tuple_bytes,
                old_tuple_meta: Some(TupleMetaRepr::from(old_meta)),
                old_tuple_data: Some(old_tuple_bytes),
            })
        };
        self.append_heap_record(payload)?;
        self.write_back_page(&mut page_guard, &mut table_page)
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

        if table_page.header.num_tuples == 0 {
            Ok(None)
        } else {
            Ok(Some(RecordId::new(first_page_id, 0)))
        }
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

        if next_table_page.header.num_tuples == 0 {
            return Ok(None);
        }
        Ok(Some(RecordId::new(table_page.header.next_page_id, 0)))
    }

    /// Construct a lightweight TableHeap view for recovery operations.
    /// This instance uses an empty schema and does not rely on first/last page ids.
    pub fn recovery_view(buffer_pool: Arc<BufferManager>) -> Self {
        Self {
            schema: EMPTY_SCHEMA_REF.clone(),
            buffer_pool,
            first_page_id: AtomicU32::new(0),
            last_page_id: AtomicU32::new(0),
        }
    }

    /// Recovery-only API: set tuple meta without emitting WAL.
    /// Only used by RecoveryManager during UNDO.
    pub fn recover_set_tuple_meta(&self, rid: RecordId, meta: TupleMeta) -> QuillSQLResult<()> {
        let mut guard = self.buffer_pool.fetch_page_write(rid.page_id)?;
        let (mut header, hdr_len) =
            crate::storage::codec::TablePageHeaderCodec::decode(guard.data())?;
        if (rid.slot_num as usize) >= header.tuple_infos.len() {
            return Ok(());
        }
        let info = &mut header.tuple_infos[rid.slot_num as usize];
        if info.meta.is_deleted != meta.is_deleted {
            if meta.is_deleted {
                header.num_deleted_tuples = header.num_deleted_tuples.saturating_add(1);
            } else {
                header.num_deleted_tuples = header.num_deleted_tuples.saturating_sub(1);
            }
        }
        info.meta = meta;
        let new_header = crate::storage::codec::TablePageHeaderCodec::encode(&header);
        let copy_len = std::cmp::min(hdr_len, new_header.len());
        guard.data_mut()[0..copy_len].copy_from_slice(&new_header[..copy_len]);
        guard.mark_dirty();
        Ok(())
    }

    /// Recovery-only API: set tuple raw bytes without emitting WAL.
    /// If size mismatches, repack tuple area and update offsets.
    pub fn recover_set_tuple_bytes(&self, rid: RecordId, new_bytes: &[u8]) -> QuillSQLResult<()> {
        let mut guard = self.buffer_pool.fetch_page_write(rid.page_id)?;
        let (mut header, _hdr_len) =
            crate::storage::codec::TablePageHeaderCodec::decode(guard.data())?;
        if (rid.slot_num as usize) >= header.tuple_infos.len() {
            return Ok(());
        }
        let slot = rid.slot_num as usize;
        let info = &mut header.tuple_infos[slot];
        let off = info.offset as usize;
        let sz = info.size as usize;
        if new_bytes.len() == sz {
            if off + sz <= crate::buffer::PAGE_SIZE {
                guard.data_mut()[off..off + sz].copy_from_slice(new_bytes);
            }
            guard.mark_dirty();
            return Ok(());
        }
        let n = header.tuple_infos.len();
        let mut tuples: Vec<Vec<u8>> = Vec::with_capacity(n);
        for i in 0..n {
            let inf = &header.tuple_infos[i];
            let s = &guard.data()[inf.offset as usize..(inf.offset + inf.size) as usize];
            if i == slot {
                tuples.push(new_bytes.to_vec());
            } else {
                tuples.push(s.to_vec());
            }
        }
        let mut tail = crate::buffer::PAGE_SIZE;
        for i in 0..n {
            let sz = tuples[i].len();
            tail = tail.saturating_sub(sz);
            header.tuple_infos[i].offset = tail as u16;
            header.tuple_infos[i].size = sz as u16;
        }
        let new_header = crate::storage::codec::TablePageHeaderCodec::encode(&header);
        for b in guard.data_mut().iter_mut() {
            *b = 0;
        }
        let hdr_copy = std::cmp::min(new_header.len(), crate::buffer::PAGE_SIZE);
        guard.data_mut()[0..hdr_copy].copy_from_slice(&new_header[..hdr_copy]);
        for i in 0..n {
            let off = header.tuple_infos[i].offset as usize;
            let sz = header.tuple_infos[i].size as usize;
            if off + sz <= crate::buffer::PAGE_SIZE {
                guard.data_mut()[off..off + sz].copy_from_slice(&tuples[i][..sz]);
            }
        }
        guard.mark_dirty();
        Ok(())
    }

    pub fn recover_restore_tuple(
        &self,
        rid: RecordId,
        meta: TupleMeta,
        tuple: &Tuple,
    ) -> QuillSQLResult<()> {
        let bytes = TupleCodec::encode(tuple);
        self.recover_set_tuple_bytes(rid, &bytes)?;
        self.recover_set_tuple_meta(rid, meta)
    }

    pub fn recover_delete_tuple(
        &self,
        rid: RecordId,
        txn_id: TransactionId,
        cid: CommandId,
    ) -> QuillSQLResult<()> {
        let mut meta = self.tuple_meta(rid)?;
        if meta.is_deleted {
            return Ok(());
        }
        meta.mark_deleted(txn_id, cid);
        self.recover_set_tuple_meta(rid, meta)
    }

    pub fn delete_tuple(
        &self,
        rid: RecordId,
        txn_id: TransactionId,
        cid: CommandId,
    ) -> QuillSQLResult<()> {
        let _ = self.mvcc_mark_deleted(rid, txn_id, cid)?;
        Ok(())
    }

    /// Attempt to reclaim the tuple at `rid` if `predicate` returns true for the current metadata.
    /// Returns true when the tuple was removed.
    pub fn vacuum_slot_if<F>(&self, rid: RecordId, predicate: F) -> QuillSQLResult<bool>
    where
        F: FnOnce(&TupleMeta) -> bool,
    {
        let mut page_guard = self.buffer_pool.fetch_page_write(rid.page_id)?;
        let mut table_page = TablePageCodec::decode(page_guard.data(), self.schema.clone())?.0;
        table_page.set_lsn(page_guard.lsn());

        let slot = rid.slot_num as u16;
        if slot >= table_page.header.num_tuples {
            return Ok(false);
        }
        let meta = table_page.header.tuple_infos[slot as usize].meta;
        if !predicate(&meta) {
            return Ok(false);
        }

        table_page.reclaim_tuple(slot)?;
        self.write_back_page(&mut page_guard, &mut table_page)?;
        Ok(true)
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
    /// Streaming with generic ring buffer holding decoded tuples
    Streaming(StreamScanState),
}

#[derive(Debug)]
struct StreamScanState {
    ring: RingBuffer<(RecordId, TupleMeta, Tuple)>,
    first_page: bool,
    prefetch: StreamPrefetchState,
}

#[derive(Debug)]
struct StreamPrefetchState {
    pending: VecDeque<PageId>,
    inflight: VecDeque<StreamBatch>,
    ready: VecDeque<(PageId, BytesMut)>,
    readahead: usize,
    exhausted: bool,
}

#[derive(Debug)]
struct StreamBatch {
    page_ids: Vec<PageId>,
    rx: DiskCommandResultReceiver<Vec<BytesMut>>,
}

impl StreamPrefetchState {
    fn ensure_ready(&mut self, scheduler: &Arc<DiskScheduler>) -> QuillSQLResult<()> {
        while !self.exhausted && self.ready.is_empty() {
            let capacity = self
                .readahead
                .saturating_sub(self.ready.len() + self.inflight.len());
            if capacity > 0 && !self.pending.is_empty() {
                self.schedule_batch(scheduler, capacity)?;
                continue;
            }
            if let Some(batch) = self.inflight.pop_front() {
                let buffers = batch.rx.recv().map_err(|e| {
                    QuillSQLError::Internal(format!("DiskScheduler channel disconnected: {}", e))
                })??;
                for (pid, bytes) in batch.page_ids.into_iter().zip(buffers.into_iter()) {
                    self.ready.push_back((pid, bytes));
                }
            } else {
                self.exhausted = true;
            }
        }
        Ok(())
    }

    fn maybe_schedule(&mut self, scheduler: &Arc<DiskScheduler>) -> QuillSQLResult<()> {
        if self.exhausted {
            return Ok(());
        }
        let capacity = self
            .readahead
            .saturating_sub(self.ready.len() + self.inflight.len());
        if capacity == 0 || self.pending.is_empty() {
            return Ok(());
        }
        self.schedule_batch(scheduler, capacity)
    }

    fn schedule_batch(
        &mut self,
        scheduler: &Arc<DiskScheduler>,
        limit: usize,
    ) -> QuillSQLResult<()> {
        let mut ids = Vec::with_capacity(limit);
        while ids.len() < limit {
            if let Some(pid) = self.pending.pop_front() {
                ids.push(pid);
            } else {
                break;
            }
        }
        if ids.is_empty() {
            return Ok(());
        }
        let rx = scheduler.schedule_read_pages(ids.clone())?;
        self.inflight.push_back(StreamBatch { page_ids: ids, rx });
        Ok(())
    }
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

        // Centralized config (remove env): use TableScanConfig defaults
        let cfg = TableScanConfig::default();
        let pool_quarter = (heap.buffer_pool.buffer_pool().capacity().max(1) / 4) as u32;
        let threshold: u32 = cfg.stream_threshold_pages.unwrap_or(pool_quarter.max(1));
        let readahead: usize = cfg.readahead_pages;

        let approx_pages = heap
            .last_page_id
            .load(Ordering::SeqCst)
            .saturating_sub(heap.first_page_id.load(Ordering::SeqCst))
            + 1;

        let is_full_scan = matches!(start, Bound::Unbounded) && matches!(end, Bound::Unbounded);

        // Requested streaming decision
        let requested_stream = match streaming_hint {
            Some(true) => true,
            Some(false) => false,
            None => cfg.stream_scan_enable || approx_pages >= threshold,
        };
        // If explicitly hinted true, allow streaming even for ranged scans. Otherwise only for full scan.
        let use_streaming = if matches!(streaming_hint, Some(true)) {
            true
        } else {
            is_full_scan && requested_stream
        };

        let strategy = if use_streaming {
            let tuple_ring_cap = readahead.max(1).saturating_mul(1024);
            let ring = RingBuffer::with_capacity(tuple_ring_cap);
            let default_first = heap.first_page_id.load(Ordering::SeqCst);
            let start_pid = match &start {
                Bound::Included(r) | Bound::Excluded(r) => r.page_id,
                Bound::Unbounded => default_first,
            };
            let mut pending = VecDeque::new();
            let exhausted = if start_pid == INVALID_PAGE_ID {
                true
            } else {
                pending.push_back(start_pid);
                false
            };
            let prefetch = StreamPrefetchState {
                pending,
                inflight: VecDeque::new(),
                ready: VecDeque::new(),
                readahead: readahead.max(1),
                exhausted,
            };
            ScanStrategy::Streaming(StreamScanState {
                ring,
                first_page: true,
                prefetch,
            })
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

    pub fn next(&mut self) -> QuillSQLResult<Option<(RecordId, TupleMeta, Tuple)>> {
        if self.ended {
            return Ok(None);
        }

        // Streaming now supports bounded scans; no unconditional fallback required here.

        // Clone refs needed by streaming helper before borrowing self.strategy mutably
        let heap_arc = self.heap.clone();
        let schema = self.heap.schema.clone();
        let start_bound_cloned = self.start_bound.clone();

        // Streaming strategy (only supports full scan). For other ranges fallback to Cached.
        if let ScanStrategy::Streaming(state) = &mut self.strategy {
            // Initialize on first call
            if !self.started {
                self.started = true;
                if state.prefetch.exhausted {
                    self.ended = true;
                    return Ok(None);
                }
                // Ensure disk visibility for streaming reads
                self.heap.buffer_pool.flush_all_pages()?;
                fill_stream_ring(&heap_arc, schema.clone(), &start_bound_cloned, state)?;
            }

            loop {
                if let Some((rid, meta, tuple)) = state.ring.pop() {
                    // Respect end bound
                    match &self.end_bound {
                        Bound::Unbounded => return Ok(Some((rid, meta, tuple))),
                        Bound::Included(end) => {
                            if rid == *end {
                                self.ended = true;
                            }
                            return Ok(Some((rid, meta, tuple)));
                        }
                        Bound::Excluded(end) => {
                            if rid == *end {
                                self.ended = true;
                                return Ok(None);
                            }
                            return Ok(Some((rid, meta, tuple)));
                        }
                    }
                } else {
                    if state.prefetch.exhausted {
                        self.ended = true;
                        return Ok(None);
                    }
                    fill_stream_ring(&heap_arc, schema.clone(), &start_bound_cloned, state)?;
                    if state.ring.is_empty() {
                        if state.prefetch.exhausted {
                            self.ended = true;
                        }
                        return Ok(None);
                    }
                }
            }
        }

        // Cached strategy (original)
        if self.started {
            match self.end_bound {
                Bound::Included(rid) => {
                    if let Some(next_rid) = self.heap.get_next_rid(self.cursor)? {
                        self.cursor = next_rid;
                        if self.cursor == rid {
                            self.ended = true;
                        }
                        let (meta, tuple) = self.heap.full_tuple(self.cursor)?;
                        Ok(Some((self.cursor, meta, tuple)))
                    } else {
                        Ok(None)
                    }
                }
                Bound::Excluded(rid) => {
                    if let Some(next_rid) = self.heap.get_next_rid(self.cursor)? {
                        if next_rid == rid {
                            self.ended = true;
                            Ok(None)
                        } else {
                            self.cursor = next_rid;
                            let (meta, tuple) = self.heap.full_tuple(self.cursor)?;
                            Ok(Some((self.cursor, meta, tuple)))
                        }
                    } else {
                        Ok(None)
                    }
                }
                Bound::Unbounded => {
                    if let Some(next_rid) = self.heap.get_next_rid(self.cursor)? {
                        self.cursor = next_rid;
                        let (meta, tuple) = self.heap.full_tuple(self.cursor)?;
                        Ok(Some((self.cursor, meta, tuple)))
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
                    let (meta, tuple) = self.heap.full_tuple(self.cursor)?;
                    Ok(Some((self.cursor, meta, tuple)))
                }
                Bound::Excluded(rid) => {
                    if let Some(next_rid) = self.heap.get_next_rid(rid)? {
                        self.cursor = next_rid;
                        let (meta, tuple) = self.heap.full_tuple(self.cursor)?;
                        Ok(Some((self.cursor, meta, tuple)))
                    } else {
                        self.ended = true;
                        Ok(None)
                    }
                }
                Bound::Unbounded => {
                    if let Some(first_rid) = self.heap.get_first_rid()? {
                        self.cursor = first_rid;
                        let (meta, tuple) = self.heap.full_tuple(self.cursor)?;
                        Ok(Some((self.cursor, meta, tuple)))
                    } else {
                        self.ended = true;
                        Ok(None)
                    }
                }
            }
        }
    }
}

fn fill_stream_ring(
    heap: &Arc<TableHeap>,
    schema: SchemaRef,
    start_bound: &Bound<RecordId>,
    state: &mut StreamScanState,
) -> QuillSQLResult<()> {
    let scheduler = heap.buffer_pool.buffer_pool().disk_scheduler();
    while state.ring.len() < state.ring.capacity() && !state.prefetch.exhausted {
        state.prefetch.ensure_ready(&scheduler)?;
        let Some((pid, bytes)) = state.prefetch.ready.pop_front() else {
            break;
        };

        let (page, _) = TablePageCodec::decode(&bytes, schema.clone())?;
        if page.header.next_page_id != INVALID_PAGE_ID {
            state.prefetch.pending.push_back(page.header.next_page_id);
        }
        state.prefetch.maybe_schedule(&scheduler)?;

        let start_slot = if state.first_page {
            state.first_page = false;
            match start_bound {
                Bound::Included(r) if r.page_id == pid => r.slot_num as usize,
                Bound::Excluded(r) if r.page_id == pid => r.slot_num as usize + 1,
                _ => 0,
            }
        } else {
            0
        };

        for slot in start_slot..page.header.num_tuples as usize {
            let rid = RecordId::new(pid, slot as u32);
            let (meta, tuple) = page.tuple(slot as u16)?;
            state.ring.push((rid, meta, tuple));
            if state.ring.len() >= state.ring.capacity() {
                break;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::buffer::BufferManager;
    use crate::catalog::{Column, DataType, Schema};
    use crate::storage::codec::TupleCodec;
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;
    use crate::storage::page::EMPTY_TUPLE_META;
    use crate::storage::table_heap::TableIterator;
    use crate::storage::{table_heap::TableHeap, tuple::Tuple};
    use crate::utils::scalar::ScalarValue;

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
        let buffer_pool = Arc::new(BufferManager::new(1000, disk_scheduler));
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
        meta.mark_deleted(2, 0);
        meta.is_deleted = true;
        table_heap.update_tuple_meta(meta, rid2).unwrap();

        let meta = table_heap.tuple_meta(rid2).unwrap();
        assert_eq!(meta.insert_txn_id, 1);
        assert_eq!(meta.delete_txn_id, 2);
        assert_eq!(meta.delete_cid, 0);
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
        let buffer_pool = Arc::new(BufferManager::new(1000, disk_scheduler));
        let table_heap = TableHeap::try_new(schema.clone(), buffer_pool).unwrap();

        let meta1 = super::TupleMeta::new(1, 0);
        let rid1 = table_heap
            .insert_tuple(
                &meta1,
                &Tuple::new(schema.clone(), vec![1i8.into(), 1i16.into()]),
            )
            .unwrap();
        let meta2 = super::TupleMeta::new(2, 0);
        let rid2 = table_heap
            .insert_tuple(
                &meta2,
                &Tuple::new(schema.clone(), vec![2i8.into(), 2i16.into()]),
            )
            .unwrap();
        let meta3 = super::TupleMeta::new(3, 0);
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
    fn mvcc_update_creates_version_chain() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("mvcc_test.db");

        let schema = Arc::new(Schema::new(vec![
            Column::new("id", DataType::Int32, false),
            Column::new("val", DataType::Int32, false),
        ]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferManager::new(256, disk_scheduler));
        let table_heap = TableHeap::try_new(schema.clone(), buffer_pool).unwrap();

        let base_tuple = Tuple::new(
            schema.clone(),
            vec![ScalarValue::Int32(Some(1)), ScalarValue::Int32(Some(10))],
        );
        let rid = table_heap
            .insert_tuple(&super::TupleMeta::new(1, 0), &base_tuple)
            .expect("insert base");

        let updated_tuple = Tuple::new(
            schema.clone(),
            vec![ScalarValue::Int32(Some(1)), ScalarValue::Int32(Some(20))],
        );
        let (new_rid, _) = table_heap
            .mvcc_update(rid, updated_tuple, 2, 0)
            .expect("mvcc update");

        let old_meta = table_heap.tuple_meta(rid).expect("old meta");
        assert!(old_meta.is_deleted);
        assert_eq!(old_meta.next_version, Some(new_rid));

        let new_meta = table_heap.tuple_meta(new_rid).expect("new meta");
        assert_eq!(new_meta.prev_version, Some(rid));
        assert!(!new_meta.is_deleted);
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
        let buffer_pool = Arc::new(BufferManager::new(1000, disk_scheduler));
        let table_heap = Arc::new(TableHeap::try_new(schema.clone(), buffer_pool).unwrap());

        let meta1 = super::TupleMeta::new(1, 0);
        let rid1 = table_heap
            .insert_tuple(
                &meta1,
                &Tuple::new(schema.clone(), vec![1i8.into(), 1i16.into()]),
            )
            .unwrap();
        let meta2 = super::TupleMeta::new(2, 0);
        let rid2 = table_heap
            .insert_tuple(
                &meta2,
                &Tuple::new(schema.clone(), vec![2i8.into(), 2i16.into()]),
            )
            .unwrap();
        let meta3 = super::TupleMeta::new(3, 0);
        let rid3 = table_heap
            .insert_tuple(
                &meta3,
                &Tuple::new(schema.clone(), vec![3i8.into(), 3i16.into()]),
            )
            .unwrap();

        let mut iterator = TableIterator::new(table_heap.clone(), ..);

        let (rid, meta, tuple) = iterator.next().unwrap().unwrap();
        assert_eq!(rid, rid1);
        assert_eq!(meta, meta1);
        assert_eq!(tuple.data, vec![1i8.into(), 1i16.into()]);

        let (rid, meta, tuple) = iterator.next().unwrap().unwrap();
        assert_eq!(rid, rid2);
        assert_eq!(meta, meta2);
        assert_eq!(tuple.data, vec![2i8.into(), 2i16.into()]);

        let (rid, meta, tuple) = iterator.next().unwrap().unwrap();
        assert_eq!(rid, rid3);
        assert_eq!(meta, meta3);
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
        let buffer_pool = Arc::new(BufferManager::new(128, disk_scheduler));
        let table_heap = Arc::new(TableHeap::try_new(schema.clone(), buffer_pool).unwrap());

        // Insert many rows to span multiple pages
        let rows = 1000;
        for i in 0..rows {
            let _rid = table_heap
                .insert_tuple(
                    &super::TupleMeta::new(1, 0),
                    &Tuple::new(schema.clone(), vec![(i as i8).into(), (i as i16).into()]),
                )
                .unwrap();
        }

        // Ensure data is persisted before direct I/O streaming
        table_heap.buffer_pool.flush_all_pages().unwrap();

        // Iterate full range; should go through streaming ring buffer
        let mut it = TableIterator::new(table_heap.clone(), ..);
        let mut cnt = 0usize;
        while let Some((_rid, _meta, _t)) = it.next().unwrap() {
            cnt += 1;
        }
        assert_eq!(cnt, rows);
    }

    #[test]
    pub fn test_streaming_respects_bounds_and_fallbacks() {
        // Force-enable streaming globally; iterator should still fallback for ranged scans
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
        let buffer_pool = Arc::new(BufferManager::new(128, disk_scheduler));
        let table_heap = Arc::new(TableHeap::try_new(schema.clone(), buffer_pool).unwrap());

        let rid1 = table_heap
            .insert_tuple(
                &super::TupleMeta::new(1, 0),
                &Tuple::new(schema.clone(), vec![1i8.into(), 1i16.into()]),
            )
            .unwrap();
        let rid2 = table_heap
            .insert_tuple(
                &super::TupleMeta::new(2, 0),
                &Tuple::new(schema.clone(), vec![2i8.into(), 2i16.into()]),
            )
            .unwrap();
        let rid3 = table_heap
            .insert_tuple(
                &super::TupleMeta::new(3, 0),
                &Tuple::new(schema.clone(), vec![3i8.into(), 3i16.into()]),
            )
            .unwrap();

        // Create ranged iterator with streaming hint=true; must fallback and only return rid1..=rid2
        let mut it = TableIterator::new_with_hint(table_heap.clone(), rid1..=rid2, Some(true));

        let got1 = it.next().unwrap().unwrap();
        let got2 = it.next().unwrap().unwrap();
        let got3 = it.next().unwrap();

        assert_eq!(got1.0, rid1);
        assert_eq!(got2.0, rid2);
        assert!(got3.is_none());

        // Sanity: ensure rid3 exists but not returned in range
        let (_m, t3) = table_heap.full_tuple(rid3).unwrap();
        assert_eq!(t3.data, vec![3i8.into(), 3i16.into()]);
    }

    #[test]
    pub fn test_recover_set_tuple_meta_and_bytes() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferManager::new(128, disk_scheduler));
        let table_heap = TableHeap::try_new(schema.clone(), buffer_pool.clone()).unwrap();

        // Insert a row
        let rid = table_heap
            .insert_tuple(
                &EMPTY_TUPLE_META,
                &Tuple::new(schema.clone(), vec![1i8.into(), 10i16.into()]),
            )
            .unwrap();

        // Change bytes via recovery API
        let new_tuple = Tuple::new(schema.clone(), vec![2i8.into(), 20i16.into()]);
        let new_bytes = TupleCodec::encode(&new_tuple);
        table_heap
            .recover_set_tuple_bytes(rid, &new_bytes)
            .expect("recover bytes");

        // Verify tuple data changed
        let (_m, t) = table_heap.full_tuple(rid).unwrap();
        assert_eq!(t.data, vec![2i8.into(), 20i16.into()]);

        // Mark deleted via recovery API and verify
        let mut meta = table_heap.tuple_meta(rid).unwrap();
        meta.is_deleted = true;
        table_heap
            .recover_set_tuple_meta(rid, meta)
            .expect("recover meta");
        let m2 = table_heap.tuple_meta(rid).unwrap();
        assert!(m2.is_deleted);
    }

    #[test]
    pub fn test_recover_repack_on_size_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.db");

        let schema = Arc::new(Schema::new(vec![
            Column::new("a", DataType::Int8, false),
            Column::new("b", DataType::Int16, false),
        ]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferManager::new(128, disk_scheduler));
        let table_heap = TableHeap::try_new(schema.clone(), buffer_pool.clone()).unwrap();

        let rid = table_heap
            .insert_tuple(
                &EMPTY_TUPLE_META,
                &Tuple::new(schema.clone(), vec![1i8.into(), 10i16.into()]),
            )
            .unwrap();

        // Create a tuple with different encoded length and recover-set it
        let larger_tuple = Tuple::new(schema.clone(), vec![99i8.into(), 300i16.into()]);
        let larger_bytes = TupleCodec::encode(&larger_tuple);
        table_heap
            .recover_set_tuple_bytes(rid, &larger_bytes)
            .expect("recover larger bytes");

        let (_m, t2) = table_heap.full_tuple(rid).unwrap();
        assert_eq!(t2.data, vec![99i8.into(), 300i16.into()]);
    }

    #[test]
    fn vacuum_slot_if_reclaims_tuple() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("vacuum.db");
        let schema = Arc::new(Schema::new(vec![Column::new("v", DataType::Int32, false)]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferManager::new(32, disk_scheduler));
        let heap = TableHeap::try_new(schema.clone(), buffer_pool).unwrap();

        let meta = super::TupleMeta::new(1, 0);
        let tuple = Tuple::new(schema.clone(), vec![ScalarValue::Int32(Some(5))]);
        let rid = heap.insert_tuple(&meta, &tuple).unwrap();

        assert!(heap.full_tuple(rid).is_ok());
        assert!(heap.vacuum_slot_if(rid, |_| true).unwrap());
        assert!(heap.full_tuple(rid).is_err());
        assert!(heap.get_first_rid().unwrap().is_none());
    }

    #[test]
    fn vacuum_slot_if_respects_predicate() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("vacuum_predicate.db");
        let schema = Arc::new(Schema::new(vec![Column::new("v", DataType::Int32, false)]));
        let disk_manager = DiskManager::try_new(temp_path).unwrap();
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::new(disk_manager)));
        let buffer_pool = Arc::new(BufferManager::new(32, disk_scheduler));
        let heap = TableHeap::try_new(schema.clone(), buffer_pool).unwrap();

        let meta = super::TupleMeta::new(42, 0);
        let tuple = Tuple::new(schema.clone(), vec![ScalarValue::Int32(Some(9))]);
        let rid = heap.insert_tuple(&meta, &tuple).unwrap();

        assert!(!heap
            .vacuum_slot_if(rid, |current| current.insert_txn_id == 0)
            .unwrap());
        let (meta_after, tuple_after) = heap.full_tuple(rid).unwrap();
        assert_eq!(meta_after.insert_txn_id, 42);
        assert_eq!(tuple_after, tuple);
    }
}
