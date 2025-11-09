pub mod codec;
pub mod page;

mod buffer;
mod io;
mod record;
mod storage;
mod writer;

use parking_lot::{Condvar, Mutex};
use std::cmp;
use std::collections::VecDeque;
use std::fmt;
use std::fs::{self, File};
use std::io::{BufReader, ErrorKind, Read};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::buffer::{PageId, PAGE_SIZE};
use crate::config::WalConfig;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::control_file::{ControlFileManager, WalInitState};
use crate::recovery::wal::codec::{
    decode_frame, encode_body, encode_frame, CheckpointPayload, PageDeltaPayload, PageWritePayload,
    WalFrame, WAL_CRC_LEN, WAL_HEADER_LEN,
};
use crate::recovery::wal::page::{WalPage, WalPageFragmentKind, WAL_PAGE_SIZE};
use crate::recovery::wal_record::WalRecordPayload;
use crate::storage::disk_manager::DiskManager;
use crate::storage::disk_scheduler::DiskScheduler;
use crate::utils::util::find_contiguous_diff;
use bytes::Bytes;
use dashmap::DashSet;

use buffer::WalBuffer;
use io::{DiskSchedulerWalSink, WalSink};
use record::WalRecord;
use storage::{list_segments, segment_path, WalFlushTicket, WalStorage};
use writer::WalWriterRuntime;

pub type Lsn = u64;

#[derive(Debug, Clone, Copy)]
pub struct WalAppendContext {
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,
}

#[derive(Debug, Clone, Copy)]
pub struct WalAppendResult {
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,
}

struct WalState {
    buffer: WalBuffer,
    storage: WalStorage,
}

impl std::fmt::Debug for WalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalState")
            .field("buffer_len", &self.buffer.len())
            .finish()
    }
}

pub struct WalManager {
    next_lsn: AtomicU64,
    durable_lsn: AtomicU64,
    state: Mutex<WalState>,
    writer: Mutex<Option<WalWriterRuntime>>,
    max_buffer_records: usize,
    flush_coalesce_bytes: usize,
    last_checkpoint: AtomicU64,
    last_record_start: AtomicU64,
    control_file: Option<Arc<ControlFileManager>>,
    flush_lock: Mutex<()>,
    flush_cond: Condvar,
    checkpoint_redo_start: AtomicU64,
    touched_pages: DashSet<PageId>,
}

pub struct WalWriterHandle {
    manager: Option<Arc<WalManager>>,
}

impl WalWriterHandle {
    fn new(manager: Arc<WalManager>) -> Self {
        Self {
            manager: Some(manager),
        }
    }

    pub fn stop(mut self) -> QuillSQLResult<()> {
        if let Some(manager) = self.manager.take() {
            manager.stop_background_writer()
        } else {
            Ok(())
        }
    }
}

impl Drop for WalWriterHandle {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.take() {
            let _ = manager.stop_background_writer();
        }
    }
}

impl WalManager {
    pub fn new(
        config: WalConfig,
        init_state: Option<WalInitState>,
        control_file: Option<Arc<ControlFileManager>>,
    ) -> QuillSQLResult<Self> {
        let scheduler = Self::default_scheduler(&config)?;
        Self::new_with_scheduler(config, init_state, control_file, scheduler)
    }

    pub fn new_with_scheduler(
        config: WalConfig,
        init_state: Option<WalInitState>,
        control_file: Option<Arc<ControlFileManager>>,
        scheduler: Arc<DiskScheduler>,
    ) -> QuillSQLResult<Self> {
        let max_buffer_records = if config.buffer_capacity == 0 {
            usize::MAX
        } else {
            config.buffer_capacity
        };
        let flush_bytes = if config.flush_coalesce_bytes == 0 {
            usize::MAX
        } else {
            config.flush_coalesce_bytes
        };
        let sink: Arc<dyn WalSink> = Arc::new(DiskSchedulerWalSink::new(scheduler.clone()));
        let mut storage = WalStorage::new(config, sink)?;
        let (durable_ptr, next_ptr, checkpoint_ptr, last_record_start, redo_start) =
            if let Some(state) = init_state {
                (
                    state.durable_lsn,
                    state.max_assigned_lsn,
                    state.last_checkpoint_lsn,
                    state.last_record_start,
                    state.checkpoint_redo_start,
                )
            } else {
                let (next_offset, last_start) = storage.recover_offsets()?;
                (next_offset, next_offset, last_start, last_start, 0)
            };
        Ok(Self {
            next_lsn: AtomicU64::new(next_ptr),
            durable_lsn: AtomicU64::new(durable_ptr),
            state: Mutex::new(WalState {
                buffer: WalBuffer::new(last_record_start),
                storage,
            }),
            writer: Mutex::new(None),
            max_buffer_records,
            flush_coalesce_bytes: flush_bytes,
            last_checkpoint: AtomicU64::new(checkpoint_ptr),
            last_record_start: AtomicU64::new(last_record_start),
            control_file,
            flush_lock: Mutex::new(()),
            flush_cond: Condvar::new(),
            checkpoint_redo_start: AtomicU64::new(redo_start),
            touched_pages: DashSet::new(),
        })
    }

    #[inline]
    pub fn max_assigned_lsn(&self) -> Lsn {
        self.next_lsn.load(Ordering::Acquire)
    }

    #[inline]
    pub fn durable_lsn(&self) -> Lsn {
        self.durable_lsn.load(Ordering::Acquire)
    }

    pub fn append_record_with<F>(&self, mut build: F) -> QuillSQLResult<WalAppendResult>
    where
        F: FnMut(WalAppendContext) -> WalRecordPayload,
    {
        let preview_ctx = WalAppendContext {
            start_lsn: 0,
            end_lsn: WAL_HEADER_LEN as u64 + WAL_CRC_LEN as u64,
        };
        let preview_payload = build(preview_ctx);
        let (_, _, preview_body) = encode_body(&preview_payload);
        let preview_frame_len = WAL_HEADER_LEN + preview_body.len() + WAL_CRC_LEN;

        let mut guard = self.state.lock();
        let prev_start = guard.buffer.last_record_start();
        let start_lsn = self
            .next_lsn
            .fetch_add(preview_frame_len as u64, Ordering::SeqCst);
        let end_lsn_preview = start_lsn + preview_frame_len as u64;

        let preview_ctx = WalAppendContext {
            start_lsn,
            end_lsn: end_lsn_preview,
        };
        let payload = build(preview_ctx);
        let frame_bytes = encode_frame(start_lsn, prev_start, &payload);
        let frame_len = frame_bytes.len();
        debug_assert_eq!(frame_len, preview_frame_len);
        let end_lsn = start_lsn + frame_len as u64;
        let encoded = Bytes::from(frame_bytes);

        guard.buffer.push(WalRecord {
            start_lsn,
            end_lsn,
            payload: encoded,
        });

        let should_flush = guard.buffer.len() >= self.max_buffer_records
            || guard.buffer.bytes() >= self.flush_coalesce_bytes
            || guard.buffer.bytes() >= WAL_PAGE_SIZE;
        drop(guard);

        self.last_record_start.store(start_lsn, Ordering::Release);

        if should_flush {
            self.flush(Some(end_lsn))?;
        }
        Ok(WalAppendResult { start_lsn, end_lsn })
    }

    pub fn log_page_update(
        &self,
        page_id: PageId,
        prev_page_lsn: Lsn,
        old_image: &[u8],
        new_image: &[u8],
    ) -> QuillSQLResult<Option<WalAppendResult>> {
        if old_image.len() != new_image.len() || old_image.len() != PAGE_SIZE {
            return Err(QuillSQLError::Internal(format!(
                "page {} image size mismatch: old={}, new={}",
                page_id,
                old_image.len(),
                new_image.len()
            )));
        }

        let Some((start, end)) = find_contiguous_diff(old_image, new_image) else {
            return Ok(None);
        };

        let delta_threshold = PAGE_SIZE / 16;
        let first_touch = self.fpw_first_touch(page_id);
        if first_touch || (end - start) > delta_threshold {
            let page_image = new_image.to_vec();
            let result = self.append_record_with(|_| {
                WalRecordPayload::PageWrite(PageWritePayload {
                    page_id,
                    prev_page_lsn,
                    page_image: page_image.clone(),
                })
            })?;
            return Ok(Some(result));
        }

        let diff = new_image[start..end].to_vec();
        let result = self.append_record_with(|_| {
            WalRecordPayload::PageDelta(PageDeltaPayload {
                page_id,
                prev_page_lsn,
                offset: start as u16,
                data: diff.clone(),
            })
        })?;
        Ok(Some(result))
    }

    pub fn log_checkpoint(&self, payload: CheckpointPayload) -> QuillSQLResult<Lsn> {
        let redo_start = payload
            .dpt
            .iter()
            .map(|(_, lsn)| *lsn)
            .min()
            .unwrap_or(payload.last_lsn);
        let result = self.append_record_with(|_| WalRecordPayload::Checkpoint(payload.clone()))?;
        self.last_checkpoint
            .store(result.start_lsn, Ordering::Release);
        self.checkpoint_redo_start
            .store(redo_start, Ordering::Release);
        self.flush(Some(result.end_lsn))?;
        self.touched_pages.clear();
        if let Some(ctrl) = &self.control_file {
            ctrl.update(
                self.durable_lsn(),
                self.max_assigned_lsn(),
                self.last_checkpoint_lsn(),
                self.last_record_start.load(Ordering::Acquire),
                self.checkpoint_redo_start.load(Ordering::Acquire),
            )?;
        }
        Ok(result.end_lsn)
    }

    #[inline]
    pub fn last_checkpoint_lsn(&self) -> Lsn {
        self.last_checkpoint.load(Ordering::Acquire)
    }

    pub fn start_background_flush(
        self: &Arc<Self>,
        interval: Duration,
    ) -> QuillSQLResult<Option<WalWriterHandle>> {
        if interval.is_zero() {
            return Ok(None);
        }
        let mut guard = self.writer.lock();
        if guard.is_some() {
            return Ok(None);
        }
        let runtime = WalWriterRuntime::spawn(Arc::downgrade(self), interval)?;
        *guard = Some(runtime);
        Ok(Some(WalWriterHandle::new(Arc::clone(self))))
    }

    fn stop_background_writer(&self) -> QuillSQLResult<()> {
        if let Some(runtime) = self.writer.lock().take() {
            runtime.stop()?;
        }
        Ok(())
    }

    pub fn flush(&self, target: Option<Lsn>) -> QuillSQLResult<Lsn> {
        let recycle_lsn = self.last_checkpoint.load(Ordering::Acquire);

        let (desired, tickets) = {
            let mut guard = self.state.lock();
            let current_durable = self.durable_lsn();
            let highest_buffered = guard.buffer.highest_end_lsn().max(current_durable);
            let mut desired = target.filter(|lsn| *lsn != 0).unwrap_or(highest_buffered);
            desired = cmp::min(self.max_assigned_lsn(), desired);

            if desired <= current_durable {
                guard.storage.recycle_segments(recycle_lsn)?;
                drop(guard);
                return Ok(current_durable);
            }

            let (to_flush, _) = guard.buffer.drain_until(desired);
            if !to_flush.is_empty() {
                guard.storage.append_records(&to_flush)?;
                guard.storage.flush()?;
            }

            guard.storage.recycle_segments(recycle_lsn)?;
            let ready = guard.storage.take_ready(desired);
            (desired, ready)
        };

        self.wait_for_flush_tickets(tickets)?;
        self.durable_lsn.store(desired, Ordering::Release);
        self.persist_control_file()?;
        self.flush_cond.notify_all();
        Ok(desired)
    }

    pub fn pending_records(&self) -> Vec<WalRecord> {
        self.state.lock().buffer.pending()
    }

    fn default_scheduler(config: &WalConfig) -> QuillSQLResult<Arc<DiskScheduler>> {
        fs::create_dir_all(&config.directory)?;
        let db_path = config.directory.join("wal_scheduler.db");
        let disk_manager = Arc::new(DiskManager::try_new(&db_path)?);
        Ok(Arc::new(DiskScheduler::new(disk_manager)))
    }

    fn wait_for_flush_tickets(&self, tickets: Vec<WalFlushTicket>) -> QuillSQLResult<()> {
        for ticket in tickets {
            match ticket.receiver.recv() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(err) => {
                    return Err(QuillSQLError::Internal(format!(
                        "WAL flush completion dropped: {}",
                        err
                    )))
                }
            }
        }
        Ok(())
    }

    fn persist_control_file(&self) -> QuillSQLResult<()> {
        if let Some(ctrl) = &self.control_file {
            ctrl.update(
                self.durable_lsn(),
                self.max_assigned_lsn(),
                self.last_checkpoint_lsn(),
                self.last_record_start.load(Ordering::Acquire),
                self.checkpoint_redo_start.load(Ordering::Acquire),
            )?;
        }
        Ok(())
    }

    pub fn flush_until(&self, target: Lsn) -> QuillSQLResult<Lsn> {
        if target == 0 {
            return Ok(self.durable_lsn());
        }
        if self.durable_lsn() >= target {
            return Ok(self.durable_lsn());
        }
        self.flush(Some(target))
    }

    pub fn wait_for_durable(&self, target: Lsn) -> QuillSQLResult<()> {
        if target == 0 {
            return Ok(());
        }
        if self.durable_lsn() >= target {
            return Ok(());
        }
        self.flush(Some(target))?;
        if self.durable_lsn() >= target {
            return Ok(());
        }
        let mut guard = self.flush_lock.lock();
        while self.durable_lsn() < target {
            self.flush_cond.wait(&mut guard);
        }
        Ok(())
    }

    pub fn reader(&self) -> QuillSQLResult<WalReader> {
        let directory = self.state.lock().storage.directory_path();
        WalReader::new(directory)
    }

    pub fn fpw_first_touch(&self, page_id: PageId) -> bool {
        self.touched_pages.insert(page_id)
    }

    pub fn control_file(&self) -> Option<Arc<ControlFileManager>> {
        self.control_file.as_ref().map(Arc::clone)
    }
}

impl fmt::Debug for WalManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalManager")
            .field("next_lsn", &self.next_lsn.load(Ordering::Relaxed))
            .field("durable_lsn", &self.durable_lsn.load(Ordering::Relaxed))
            .field("max_buffer_records", &self.max_buffer_records)
            .field("flush_coalesce_bytes", &self.flush_coalesce_bytes)
            .finish()
    }
}

impl Drop for WalManager {
    fn drop(&mut self) {
        if let Some(runtime) = self.writer.lock().take() {
            let _ = runtime.stop();
        }
    }
}

pub struct WalReader {
    directory: PathBuf,
    segments: Vec<u64>,
    current_idx: usize,
    cursor: Option<SegmentCursor>,
}

impl WalReader {
    pub fn new(directory: PathBuf) -> QuillSQLResult<Self> {
        let mut segments = list_segments(&directory)?;
        segments.sort_unstable();
        Ok(Self {
            directory,
            segments,
            current_idx: 0,
            cursor: None,
        })
    }

    pub fn next_frame(&mut self) -> QuillSQLResult<Option<WalFrame>> {
        loop {
            if self.cursor.is_none() && !self.open_next_segment()? {
                return Ok(None);
            }

            if let Some(cursor) = self.cursor.as_mut() {
                match cursor.read_frame()? {
                    Some(frame) => return Ok(Some(frame)),
                    None => {
                        self.cursor = None;
                        continue;
                    }
                }
            } else {
                return Ok(None);
            }
        }
    }

    fn open_next_segment(&mut self) -> QuillSQLResult<bool> {
        let Some(segment_id) = self.segments.get(self.current_idx).copied() else {
            return Ok(false);
        };
        let path = segment_path(&self.directory, segment_id);
        let cursor = SegmentCursor::open(&path)?;
        self.cursor = Some(cursor);
        self.current_idx += 1;
        Ok(true)
    }
}

struct SegmentCursor {
    len: u64,
    offset: u64,
    reader: BufReader<File>,
    fragment_buf: Vec<u8>,
    ready_frames: VecDeque<WalFrame>,
}

impl SegmentCursor {
    fn open(path: &PathBuf) -> QuillSQLResult<Self> {
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        Ok(Self {
            len: metadata.len(),
            offset: 0,
            reader: BufReader::new(file),
            fragment_buf: Vec::new(),
            ready_frames: VecDeque::new(),
        })
    }

    fn read_frame(&mut self) -> QuillSQLResult<Option<WalFrame>> {
        loop {
            if let Some(frame) = self.ready_frames.pop_front() {
                return Ok(Some(frame));
            }
            if self.offset >= self.len {
                return Ok(None);
            }
            let mut page_buf = vec![0u8; WAL_PAGE_SIZE];
            if let Err(err) = self.reader.read_exact(&mut page_buf) {
                if err.kind() == ErrorKind::UnexpectedEof {
                    self.offset = self.len;
                    return Ok(None);
                }
                return Err(QuillSQLError::Io(err));
            }
            self.offset += WAL_PAGE_SIZE as u64;
            let page = match WalPage::unpack_frames(&page_buf) {
                Ok(page) => page,
                Err(QuillSQLError::Internal(message))
                    if message.contains("truncated")
                        || message.contains("CRC mismatch")
                        || message.contains("Invalid WAL page magic") =>
                {
                    self.offset = self.len;
                    return Ok(None);
                }
                Err(err) => return Err(err),
            };

            for slot in page.fragments() {
                let start = slot.offset as usize;
                let end = start + slot.len as usize;
                let fragment = &page.payload()[start..end];
                match slot.kind {
                    WalPageFragmentKind::Complete => {
                        self.fragment_buf.clear();
                        match decode_frame(fragment) {
                            Ok((frame, _)) => {
                                self.ready_frames.push_back(frame);
                            }
                            Err(QuillSQLError::Internal(message))
                                if message.contains("too short")
                                    || message.contains("truncated")
                                    || message.contains("CRC mismatch") =>
                            {
                                self.offset = self.len;
                                return Ok(None);
                            }
                            Err(err) => return Err(err),
                        }
                    }
                    WalPageFragmentKind::Start => {
                        self.fragment_buf.clear();
                        self.fragment_buf.extend_from_slice(fragment);
                    }
                    WalPageFragmentKind::Middle => {
                        if self.fragment_buf.is_empty() {
                            self.offset = self.len;
                            return Ok(None);
                        }
                        self.fragment_buf.extend_from_slice(fragment);
                    }
                    WalPageFragmentKind::End => {
                        if self.fragment_buf.is_empty() {
                            self.offset = self.len;
                            return Ok(None);
                        }
                        self.fragment_buf.extend_from_slice(fragment);
                        let frame_bytes = std::mem::take(&mut self.fragment_buf);
                        match decode_frame(&frame_bytes) {
                            Ok((frame, _)) => {
                                self.ready_frames.push_back(frame);
                            }
                            Err(QuillSQLError::Internal(message))
                                if message.contains("too short")
                                    || message.contains("truncated")
                                    || message.contains("CRC mismatch") =>
                            {
                                self.offset = self.len;
                                return Ok(None);
                            }
                            Err(err) => return Err(err),
                        }
                    }
                }
            }
        }
    }
}
