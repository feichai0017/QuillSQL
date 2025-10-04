pub mod codec;
pub mod page;

use parking_lot::{Condvar, Mutex};
use std::cmp;
use std::collections::VecDeque;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, BufReader, ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
use std::time::Duration;

use crate::buffer::PageId;
use crate::config::WalConfig;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::control_file::{ControlFileManager, WalInitState};
use crate::recovery::wal::codec::{
    decode_frame, encode_body, encode_frame, CheckpointPayload, WalFrame, WAL_CRC_LEN,
    WAL_HEADER_LEN,
};
use crate::recovery::wal::page::{
    WalFrameContinuation, WalPage, WalPageFragmentKind, WAL_PAGE_SIZE,
};
use crate::recovery::wal_record::WalRecordPayload;
use crate::recovery::wal_runtime::WalRuntime;
use bytes::Bytes;
use dashmap::DashSet;

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

#[derive(Clone)]
pub struct WalRecord {
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,
    pub payload: Bytes,
}

struct WalState {
    buffer: Vec<WalRecord>,
    buffer_bytes: usize,
    storage: WalStorage,
    last_record_start: Lsn,
}

impl std::fmt::Debug for WalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalState")
            .field("buffer_len", &self.buffer.len())
            .field("current_segment", &self.storage.current_segment)
            .field("last_record_start", &self.last_record_start)
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
        let runtime = Arc::new(WalRuntime::new(WalRuntime::default_worker_count()));
        let mut storage = WalStorage::new(config, runtime)?;
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
                buffer: Vec::new(),
                buffer_bytes: 0,
                storage,
                last_record_start,
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
        let prev_start = guard.last_record_start;
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
        guard.buffer_bytes = guard.buffer_bytes.saturating_add(frame_len);
        guard.last_record_start = start_lsn;

        let should_flush = guard.buffer.len() >= self.max_buffer_records
            || guard.buffer_bytes >= self.flush_coalesce_bytes
            || guard.buffer_bytes >= WAL_PAGE_SIZE;
        drop(guard);

        self.last_record_start.store(start_lsn, Ordering::Release);

        if should_flush {
            self.flush(Some(end_lsn))?;
        }
        Ok(WalAppendResult { start_lsn, end_lsn })
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
        // Reset FPW epoch
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

        let new_durable = {
            let mut guard = self.state.lock();
            let highest_buffered = guard
                .buffer
                .last()
                .map(|r| r.end_lsn)
                .unwrap_or(self.durable_lsn());
            let desired = target.filter(|lsn| *lsn != 0).unwrap_or(highest_buffered);
            let desired = cmp::min(self.max_assigned_lsn(), desired);
            if desired <= self.durable_lsn() {
                guard.storage.recycle_segments(recycle_lsn)?;
                return Ok(self.durable_lsn());
            }

            let flush_count = guard
                .buffer
                .iter()
                .take_while(|record| record.end_lsn <= desired)
                .count();

            if flush_count > 0 {
                let to_flush: Vec<WalRecord> = guard.buffer[..flush_count].to_vec();
                let flushed_bytes: usize = to_flush.iter().map(|r| r.encoded_len() as usize).sum();
                guard.storage.append_records(&to_flush)?;
                guard.storage.flush()?;
                guard.buffer.drain(..flush_count);
                guard.buffer_bytes = guard.buffer_bytes.saturating_sub(flushed_bytes);
            }

            guard.storage.recycle_segments(recycle_lsn)?;

            cmp::max(self.durable_lsn(), desired)
        };

        self.durable_lsn.store(new_durable, Ordering::Release);
        self.persist_control_file()?;
        self.flush_cond.notify_all();
        Ok(new_durable)
    }

    pub fn pending_records(&self) -> Vec<WalRecord> {
        self.state.lock().buffer.clone()
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
        let directory = self.state.lock().storage.directory.clone();
        WalReader::new(directory)
    }

    /// Returns true if this is the first touch of the page since last checkpoint.
    pub fn fpw_first_touch(&self, page_id: PageId) -> bool {
        self.touched_pages.insert(page_id)
    }

    /// Returns the control file manager if configured (used during recovery to read snapshot info).
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

#[derive(Clone)]
struct WalSegmentInfo {
    id: u64,
    size: u64,
}

struct WalStorage {
    directory: PathBuf,
    segment_size: u64,
    sync_on_flush: bool,
    current_segment: WalSegmentInfo,
    runtime: Arc<WalRuntime>,
    retain_segments: usize,
    open_page: Option<WalPage>,
    last_page_end_lsn: Lsn,
}

impl WalStorage {
    fn new(config: WalConfig, runtime: Arc<WalRuntime>) -> QuillSQLResult<Self> {
        fs::create_dir_all(&config.directory)?;
        let segment = Self::discover_latest_segment(&config.directory, config.segment_size)?;
        Ok(Self {
            directory: config.directory,
            segment_size: config.segment_size,
            sync_on_flush: config.sync_on_flush,
            current_segment: segment,
            runtime,
            retain_segments: config.retain_segments.max(1),
            open_page: None,
            last_page_end_lsn: 0,
        })
    }

    fn recover_offsets(&mut self) -> QuillSQLResult<(Lsn, Lsn)> {
        let segments = list_segments(&self.directory)?;
        let mut next_lsn = 0u64;
        let mut last_record_start = 0u64;
        let mut pending_fragment = Vec::new();
        let mut physical_offset = 0u64;

        for segment_id in segments {
            let path = segment_path(&self.directory, segment_id);
            let file = match fs::File::open(&path) {
                Ok(f) => f,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(QuillSQLError::Io(e)),
            };
            let metadata = file.metadata()?;
            let mut reader = io::BufReader::new(file);
            let segment_base = (segment_id.saturating_sub(1)) * self.segment_size;
            let mut segment_consumed = 0u64;
            pending_fragment.clear();

            while segment_consumed + WAL_PAGE_SIZE as u64 <= metadata.len() {
                let mut page_buf = vec![0u8; WAL_PAGE_SIZE];
                if let Err(err) = reader.read_exact(&mut page_buf) {
                    if err.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    }
                    return Err(QuillSQLError::Io(err));
                }

                let page = match WalPage::unpack_frames(&page_buf) {
                    Ok(page) => page,
                    Err(QuillSQLError::Internal(message))
                        if message.contains("truncated")
                            || message.contains("CRC mismatch")
                            || message.contains("Invalid WAL page magic") =>
                    {
                        break;
                    }
                    Err(err) => return Err(err),
                };

                if !page.has_payload() {
                    break;
                }

                let mut stop_segment = false;
                for slot in page.fragments() {
                    let start = slot.offset as usize;
                    let end = start + slot.len as usize;
                    let fragment = &page.payload()[start..end];
                    match slot.kind {
                        WalPageFragmentKind::Complete => {
                            pending_fragment.clear();
                            match decode_frame(fragment) {
                                Ok((frame, _)) => {
                                    let frame_len = fragment.len() as u64;
                                    last_record_start = frame.lsn;
                                    next_lsn = frame.lsn.saturating_add(frame_len);
                                }
                                Err(QuillSQLError::Internal(message))
                                    if message.contains("too short")
                                        || message.contains("truncated")
                                        || message.contains("CRC mismatch") =>
                                {
                                    stop_segment = true;
                                    pending_fragment.clear();
                                    break;
                                }
                                Err(err) => return Err(err),
                            }
                        }
                        WalPageFragmentKind::Start => {
                            pending_fragment.clear();
                            pending_fragment.extend_from_slice(fragment);
                        }
                        WalPageFragmentKind::Middle => {
                            if pending_fragment.is_empty() {
                                stop_segment = true;
                                pending_fragment.clear();
                                break;
                            }
                            pending_fragment.extend_from_slice(fragment);
                        }
                        WalPageFragmentKind::End => {
                            if pending_fragment.is_empty() {
                                stop_segment = true;
                                pending_fragment.clear();
                                break;
                            }
                            pending_fragment.extend_from_slice(fragment);
                            let frame_bytes = std::mem::take(&mut pending_fragment);
                            match decode_frame(&frame_bytes) {
                                Ok((frame, _)) => {
                                    let frame_len = frame_bytes.len() as u64;
                                    last_record_start = frame.lsn;
                                    next_lsn = frame.lsn.saturating_add(frame_len);
                                }
                                Err(QuillSQLError::Internal(message))
                                    if message.contains("too short")
                                        || message.contains("truncated")
                                        || message.contains("CRC mismatch") =>
                                {
                                    stop_segment = true;
                                    pending_fragment.clear();
                                    break;
                                }
                                Err(err) => return Err(err),
                            }
                        }
                    }
                }

                if stop_segment {
                    break;
                }
                segment_consumed += WAL_PAGE_SIZE as u64;
            }

            physical_offset = segment_base + segment_consumed;
        }

        self.last_page_end_lsn = next_lsn;
        let segment_index = physical_offset / self.segment_size;
        self.current_segment = WalSegmentInfo {
            id: segment_index + 1,
            size: physical_offset % self.segment_size,
        };

        Ok((next_lsn, last_record_start))
    }

    fn recycle_segments(&mut self, keep_from_lsn: Lsn) -> QuillSQLResult<()> {
        if self.retain_segments == 0 {
            return Ok(());
        }
        if keep_from_lsn == 0 {
            return Ok(());
        }
        let keep_segment_id = 1 + (keep_from_lsn / self.segment_size);
        let min_keep = keep_segment_id
            .saturating_sub(self.retain_segments as u64)
            .max(1);
        let segments = list_segments(&self.directory)?;
        for id in segments {
            if id < min_keep && id < self.current_segment.id {
                let path = segment_path(&self.directory, id);
                let _ = fs::remove_file(&path);
            }
        }
        Ok(())
    }

    fn append_records(&mut self, records: &[WalRecord]) -> std::io::Result<()> {
        let mut queue: Vec<WalRecord> = records.iter().cloned().collect();
        let mut carry: Option<WalFrameContinuation> = None;

        if let Some(page) = self.open_page.take() {
            let (page, leftover, next_carry) = page.continue_pack(queue);
            queue = leftover;
            carry = next_carry;

            if page.is_full() {
                self.write_page(&page)?;
            } else {
                if let Some(lsn) = page.last_end_lsn() {
                    self.last_page_end_lsn = lsn;
                }
                self.open_page = Some(page);
            }
        }

        while !queue.is_empty() || carry.is_some() {
            let (page, leftover, next_carry) =
                WalPage::pack_frames(self.last_page_end_lsn, queue, carry);
            queue = leftover;
            carry = next_carry;

            if !page.has_payload() {
                break;
            }

            if page.is_full() {
                self.write_page(&page)?;
            } else {
                if let Some(lsn) = page.last_end_lsn() {
                    self.last_page_end_lsn = lsn;
                }
                self.open_page = Some(page);
                break;
            }
        }

        debug_assert!(carry.is_none());

        Ok(())
    }

    fn rotate_segment(&mut self) -> std::io::Result<()> {
        self.flush()?;
        self.current_segment = WalSegmentInfo {
            id: self.current_segment.id + 1,
            size: 0,
        };
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(page) = self.open_page.take() {
            self.write_page(&page)?;
        }
        if self.sync_on_flush {
            self.write_bytes(self.current_segment.size, Bytes::new(), true)?;
        }
        Ok(())
    }

    fn write_page(&mut self, page: &WalPage) -> std::io::Result<()> {
        if !page.has_payload() {
            return Ok(());
        }
        if self.current_segment.size + WAL_PAGE_SIZE as u64 > self.segment_size {
            self.rotate_segment()?;
        }
        let offset = self.current_segment.size;
        let bytes = Bytes::from(page.to_bytes());
        self.write_bytes(offset, bytes, false)?;
        self.current_segment.size += WAL_PAGE_SIZE as u64;
        if let Some(lsn) = page.last_end_lsn() {
            self.last_page_end_lsn = lsn;
        }
        Ok(())
    }

    fn discover_latest_segment(
        directory: &Path,
        segment_size: u64,
    ) -> std::io::Result<WalSegmentInfo> {
        if !directory.exists() {
            return Ok(WalSegmentInfo { id: 1, size: 0 });
        }

        let mut segments = Vec::new();
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(id) = parse_segment_id(&name) {
                let size = entry.metadata()?.len();
                segments.push(WalSegmentInfo { id, size });
            }
        }

        if segments.is_empty() {
            return Ok(WalSegmentInfo { id: 1, size: 0 });
        }

        segments.sort_by_key(|info| info.id);
        let mut latest = segments.pop().unwrap();

        if latest.size >= segment_size {
            latest = WalSegmentInfo {
                id: latest.id + 1,
                size: 0,
            };
        }

        Ok(latest)
    }

    fn write_bytes(&self, offset: u64, data: Bytes, sync: bool) -> std::io::Result<()> {
        if data.is_empty() && !sync {
            return Ok(());
        }
        let path = segment_path(&self.directory, self.current_segment.id);
        self.runtime
            .write(&path, offset, data, sync)
            .map_err(|e| io_error(&e))
    }
}

impl std::fmt::Display for WalStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "segment={} bytes={} segment_size={} dir={}",
            self.current_segment.id,
            self.current_segment.size,
            self.segment_size,
            self.directory.display()
        )
    }
}

impl WalRecord {
    fn encoded_len(&self) -> u64 {
        self.end_lsn.saturating_sub(self.start_lsn)
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
            if self.cursor.is_none() {
                if !self.open_next_segment()? {
                    return Ok(None);
                }
            }

            if let Some(cursor) = self.cursor.as_mut() {
                match cursor.read_frame()? {
                    Some(frame) => return Ok(Some(frame)),
                    None => {
                        self.cursor = None;
                        continue;
                    }
                }
            }
        }
    }

    fn open_next_segment(&mut self) -> QuillSQLResult<bool> {
        let Some(segment_id) = self.segments.get(self.current_idx).copied() else {
            return Ok(false);
        };
        let path = segment_path(&self.directory, segment_id);
        let cursor = SegmentCursor::open(segment_id, &path)?;
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
    fn open(_id: u64, path: &Path) -> QuillSQLResult<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        Ok(Self {
            len,
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
                return match err.kind() {
                    ErrorKind::UnexpectedEof => {
                        self.offset = self.len;
                        Ok(None)
                    }
                    _ => Err(QuillSQLError::Io(err)),
                };
            }
            self.offset = self.offset.saturating_add(WAL_PAGE_SIZE as u64);

            match WalPage::unpack_frames(&page_buf) {
                Ok(page) => {
                    if !page.has_payload() {
                        self.offset = self.len;
                        return Ok(None);
                    }
                    for slot in page.fragments() {
                        let start = slot.offset as usize;
                        let end = start + slot.len as usize;
                        let fragment = &page.payload()[start..end];
                        match slot.kind {
                            WalPageFragmentKind::Complete => {
                                self.fragment_buf.clear();
                                match decode_frame(fragment) {
                                    Ok((frame, _)) => self.ready_frames.push_back(frame),
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
                                    Ok((frame, _)) => self.ready_frames.push_back(frame),
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
                Err(QuillSQLError::Internal(message))
                    if message.contains("truncated")
                        || message.contains("CRC mismatch")
                        || message.contains("Invalid WAL page magic") =>
                {
                    self.offset = self.len;
                    return Ok(None);
                }
                Err(err) => return Err(err),
            }
        }
    }
}

impl std::fmt::Debug for WalStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalStorage")
            .field("directory", &self.directory)
            .field("segment_size", &self.segment_size)
            .field("sync_on_flush", &self.sync_on_flush)
            .field("current_segment", &self.current_segment)
            .finish()
    }
}

impl std::fmt::Debug for WalSegmentInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalSegmentInfo")
            .field("id", &self.id)
            .field("size", &self.size)
            .finish()
    }
}

fn segment_path(directory: &Path, id: u64) -> PathBuf {
    directory.join(format!("wal_{:016X}.log", id))
}

fn parse_segment_id(name: &str) -> Option<u64> {
    let name = name.strip_prefix("wal_")?;
    let name = name.strip_suffix(".log")?;
    u64::from_str_radix(name, 16).ok()
}

fn list_segments(directory: &Path) -> QuillSQLResult<Vec<u64>> {
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let mut ids = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = parse_segment_id(&name) {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

fn io_error(err: &QuillSQLError) -> std::io::Error {
    std::io::Error::other(err.to_string())
}

#[derive(Debug)]
struct WalWriterRuntime {
    stop_flag: Arc<AtomicBool>,
    thread: thread::JoinHandle<()>,
}

impl WalWriterRuntime {
    fn spawn(target: Weak<WalManager>, interval: Duration) -> QuillSQLResult<Self> {
        let stop_flag = Arc::new(AtomicBool::new(false));
        let thread_stop = stop_flag.clone();
        let handle = thread::Builder::new()
            .name("walwriter".into())
            .spawn(move || {
                while !thread_stop.load(Ordering::Relaxed) {
                    if let Some(manager) = target.upgrade() {
                        let _ = manager.flush(None);
                    } else {
                        break;
                    }
                    thread::sleep(interval);
                }
                if let Some(manager) = target.upgrade() {
                    let _ = manager.flush(None);
                }
            })
            .map_err(|e| QuillSQLError::Internal(format!("Failed to spawn walwriter: {}", e)))?;
        Ok(Self {
            stop_flag,
            thread: handle,
        })
    }

    fn stop(self) -> QuillSQLResult<()> {
        self.stop_flag.store(true, Ordering::Release);
        self.thread
            .join()
            .map_err(|_| QuillSQLError::Internal("walwriter thread panicked".to_string()))
    }
}

impl Drop for WalManager {
    fn drop(&mut self) {
        if let Some(runtime) = self.writer.lock().take() {
            let _ = runtime.stop();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::WalConfig;
    use crate::recovery::wal_record::{
        CheckpointPayload, ResourceManagerId, TransactionPayload, TransactionRecordKind,
        WalRecordPayload,
    };
    use tempfile::TempDir;

    use super::WalManager;

    fn build_wal_manager_with_config(tmp: &TempDir, mut config: WalConfig) -> WalManager {
        config.directory = tmp.path().join("wal");
        WalManager::new(config, None, None).expect("wal manager")
    }

    fn build_wal_manager(tmp: &TempDir) -> WalManager {
        build_wal_manager_with_config(tmp, WalConfig::default())
    }

    #[test]
    fn append_and_flush_tracks_durable_lsn() {
        let tmp = TempDir::new().expect("tempdir");
        let wal = build_wal_manager(&tmp);
        let l1 = wal
            .append_record_with(|_| {
                WalRecordPayload::Transaction(TransactionPayload {
                    marker: TransactionRecordKind::Begin,
                    txn_id: 1,
                })
            })
            .expect("append record")
            .end_lsn;
        let l2 = wal
            .append_record_with(|_| {
                WalRecordPayload::Transaction(TransactionPayload {
                    marker: TransactionRecordKind::Commit,
                    txn_id: 1,
                })
            })
            .expect("append record")
            .end_lsn;
        assert!(l2 > l1);

        assert_eq!(wal.durable_lsn(), 0);
        let flushed = wal.flush(None).unwrap();
        assert_eq!(flushed, l2);
        assert_eq!(wal.durable_lsn(), l2);
        assert!(wal.pending_records().is_empty());
    }

    #[test]
    fn wal_reader_reads_back_frames() {
        let tmp = TempDir::new().expect("tempdir");
        let wal = build_wal_manager(&tmp);
        let expected: Vec<_> = (0..4)
            .map(|txn| {
                wal.append_record_with(|_| {
                    WalRecordPayload::Transaction(TransactionPayload {
                        marker: TransactionRecordKind::Begin,
                        txn_id: txn,
                    })
                })
                .expect("append")
                .start_lsn
            })
            .collect();
        wal.flush(None).expect("flush");

        let mut reader = wal.reader().expect("reader");
        let mut observed = Vec::new();
        while let Some(frame) = reader.next_frame().expect("frame") {
            observed.push(frame.lsn);
        }
        assert_eq!(observed, expected);
    }

    #[test]
    fn wal_manager_bootstraps_existing_segments() {
        use TransactionRecordKind::*;

        let tmp = TempDir::new().expect("tempdir");
        let wal_dir = tmp.path().join("wal");

        let mut config = WalConfig::default();
        config.directory = wal_dir.clone();

        let wal1 = build_wal_manager_with_config(&tmp, config.clone());
        let l1 = wal1
            .append_record_with(|_| {
                WalRecordPayload::Transaction(TransactionPayload {
                    marker: Begin,
                    txn_id: 1,
                })
            })
            .expect("append begin")
            .end_lsn;
        let l2 = wal1
            .append_record_with(|_| {
                WalRecordPayload::Transaction(TransactionPayload {
                    marker: Commit,
                    txn_id: 1,
                })
            })
            .expect("append commit")
            .end_lsn;
        assert!(l2 > l1);
        wal1.flush(None).expect("flush wal1");
        drop(wal1);

        let wal2 = build_wal_manager_with_config(&tmp, config);
        assert_eq!(wal2.durable_lsn(), l2);
        assert_eq!(wal2.max_assigned_lsn(), l2);

        let l3 = wal2
            .append_record_with(|_| {
                WalRecordPayload::Transaction(TransactionPayload {
                    marker: Abort,
                    txn_id: 2,
                })
            })
            .expect("append after reopen")
            .end_lsn;
        assert!(l3 > l2);
        wal2.flush(None).expect("flush wal2");
        assert_eq!(wal2.durable_lsn(), l3);
    }

    #[test]
    fn wal_recycles_segments_after_checkpoint() {
        let tmp = TempDir::new().expect("tempdir");
        let mut config = WalConfig::default();
        config.directory = tmp.path().join("wal");
        config.segment_size = 64;
        config.retain_segments = 1;
        config.writer_interval_ms = None;
        let wal_dir = config.directory.clone();
        let retain_segments = config.retain_segments;
        let wal = WalManager::new(config, None, None).expect("wal manager");

        for i in 0..256 {
            wal.append_record_with(|_| {
                WalRecordPayload::Transaction(TransactionPayload {
                    marker: TransactionRecordKind::Begin,
                    txn_id: i,
                })
            })
            .expect("append")
            .end_lsn;
        }
        wal.flush(None).expect("flush");

        let last_lsn = wal.max_assigned_lsn();
        let payload = CheckpointPayload {
            last_lsn,
            dirty_pages: Vec::new(),
            active_transactions: Vec::new(),
            dpt: Vec::new(),
        };
        wal.log_checkpoint(payload).expect("checkpoint");
        wal.flush(None).expect("final flush");

        drop(wal);

        let mut remaining_ids: Vec<u64> = std::fs::read_dir(&wal_dir)
            .expect("wal dir")
            .filter_map(|entry| {
                entry.ok().and_then(|e| {
                    let name = e.file_name();
                    let name = name.to_string_lossy();
                    super::parse_segment_id(&name)
                })
            })
            .collect();

        remaining_ids.sort_unstable();
        let max_id = *remaining_ids.last().unwrap_or(&0);
        let min_keep = max_id.saturating_sub(retain_segments as u64).max(1);

        assert!(
            remaining_ids.iter().all(|id| *id >= min_keep),
            "segments older than {} should be recycled, remaining {:?}",
            min_keep,
            remaining_ids
        );
    }

    #[test]
    fn auto_flush_respects_buffer_limits() {
        let tmp = TempDir::new().expect("tempdir");
        let mut config = WalConfig::default();
        config.buffer_capacity = 1;
        config.flush_coalesce_bytes = 64;
        let wal = build_wal_manager_with_config(&tmp, config);

        let lsn = wal
            .append_record_with(|_| {
                WalRecordPayload::Transaction(TransactionPayload {
                    marker: TransactionRecordKind::Begin,
                    txn_id: 42,
                })
            })
            .expect("append")
            .end_lsn;

        assert!(wal.pending_records().is_empty());
        assert!(wal.durable_lsn() >= lsn);
    }

    #[test]
    fn log_checkpoint_persists_record() {
        let tmp = TempDir::new().expect("tempdir");
        let wal = build_wal_manager(&tmp);

        let payload = CheckpointPayload {
            last_lsn: 7,
            dirty_pages: vec![1, 2, 3],
            active_transactions: vec![10, 11],
            dpt: Vec::new(),
        };
        let checkpoint_lsn = wal.log_checkpoint(payload.clone()).expect("checkpoint");
        assert!(wal.last_checkpoint_lsn() <= checkpoint_lsn);
        assert!(wal.durable_lsn() >= checkpoint_lsn);

        let mut reader = wal.reader().expect("reader");
        let mut seen = Vec::new();
        while let Some(frame) = reader.next_frame().expect("frame") {
            seen.push(frame);
        }
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].rmid, ResourceManagerId::Checkpoint);
        let observed = crate::recovery::wal_record::decode_checkpoint(&seen[0].body).unwrap();
        assert_eq!(observed.last_lsn, payload.last_lsn);
        assert_eq!(observed.dirty_pages, payload.dirty_pages);
        assert_eq!(observed.active_transactions, payload.active_transactions);
    }
}
