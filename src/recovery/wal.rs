use parking_lot::{Condvar, Mutex};
use std::cmp;
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
use std::time::Duration;

use crate::buffer::PageId;
use crate::config::WalConfig;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::control_file::{ControlFileManager, WalInitState};
use crate::recovery::wal_record::{
    build_frame, decode_frame, encode_body, CheckpointPayload, WalFrame, WalRecordPayload,
    WAL_CRC_LEN, WAL_HEADER_LEN,
};
use crate::storage::disk_scheduler::DiskScheduler;
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

#[derive(Debug, Clone)]
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

impl WalManager {
    pub fn new(
        config: WalConfig,
        scheduler: Arc<DiskScheduler>,
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
        let mut storage = WalStorage::new(config, scheduler)?;
        let (durable_ptr, next_ptr, checkpoint_ptr, last_record_start) =
            if let Some(state) = init_state {
                (
                    state.durable_lsn,
                    state.max_assigned_lsn,
                    state.last_checkpoint_lsn,
                    state.last_record_start,
                )
            } else {
                let (next_offset, last_start) = storage.recover_offsets()?;
                (next_offset, next_offset, last_start, last_start)
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
            checkpoint_redo_start: AtomicU64::new(0),
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
        let (rmid, info, body_bytes) = encode_body(&payload);
        let frame_len = WAL_HEADER_LEN + body_bytes.len() + WAL_CRC_LEN;
        debug_assert_eq!(frame_len, preview_frame_len);
        let end_lsn = start_lsn + frame_len as u64;
        let encoded = Bytes::from(build_frame(start_lsn, prev_start, rmid, info, &body_bytes));

        guard.buffer.push(WalRecord {
            start_lsn,
            end_lsn,
            payload: encoded,
        });
        guard.buffer_bytes = guard.buffer_bytes.saturating_add(frame_len);
        guard.last_record_start = start_lsn;

        let should_flush = guard.buffer.len() >= self.max_buffer_records
            || guard.buffer_bytes >= self.flush_coalesce_bytes;
        drop(guard);

        self.last_record_start.store(start_lsn, Ordering::Release);

        if should_flush {
            self.flush(Some(end_lsn))?;
        }
        Ok(WalAppendResult { start_lsn, end_lsn })
    }

    pub fn log_checkpoint(&self, payload: CheckpointPayload) -> QuillSQLResult<Lsn> {
        let redo_start = payload.last_lsn;
        let result = self.append_record_with(|_| WalRecordPayload::Checkpoint(payload.clone()))?;
        self.last_checkpoint
            .store(result.start_lsn, Ordering::Release);
        self.checkpoint_redo_start
            .store(redo_start, Ordering::Release);
        self.flush(Some(result.end_lsn))?;
        // Reset FPW epoch
        self.touched_pages.clear();
        Ok(result.end_lsn)
    }

    #[inline]
    pub fn last_checkpoint_lsn(&self) -> Lsn {
        self.last_checkpoint.load(Ordering::Acquire)
    }

    pub fn start_background_flush(self: &Arc<Self>, interval: Duration) -> QuillSQLResult<()> {
        if interval.is_zero() {
            return Ok(());
        }
        let mut guard = self.writer.lock();
        if guard.is_some() {
            return Ok(());
        }
        let runtime = WalWriterRuntime::spawn(Arc::downgrade(self), interval)?;
        *guard = Some(runtime);
        Ok(())
    }

    pub fn stop_background_flush(&self) -> QuillSQLResult<()> {
        if let Some(runtime) = self.writer.lock().take() {
            runtime.stop()?;
        }
        Ok(())
    }

    pub fn flush(&self, target: Option<Lsn>) -> QuillSQLResult<Lsn> {
        let recycle_lsn = self.checkpoint_redo_start.load(Ordering::Acquire);

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
        let (directory, scheduler) = {
            let guard = self.state.lock();
            (
                guard.storage.directory.clone(),
                guard.storage.scheduler.clone(),
            )
        };
        WalReader::new(directory, scheduler)
    }

    /// Returns true if this is the first touch of the page since last checkpoint.
    pub fn fpw_first_touch(&self, page_id: PageId) -> bool {
        self.touched_pages.insert(page_id)
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
    scheduler: Arc<DiskScheduler>,
    retain_segments: usize,
}

impl WalStorage {
    fn new(config: WalConfig, scheduler: Arc<DiskScheduler>) -> QuillSQLResult<Self> {
        fs::create_dir_all(&config.directory)?;
        let segment = Self::discover_latest_segment(&config.directory, config.segment_size)?;
        Ok(Self {
            directory: config.directory,
            segment_size: config.segment_size,
            sync_on_flush: config.sync_on_flush,
            current_segment: segment,
            scheduler,
            retain_segments: config.retain_segments.max(1),
        })
    }

    fn recover_offsets(&mut self) -> QuillSQLResult<(Lsn, Lsn)> {
        let segments = list_segments(&self.directory)?;
        let mut next_offset = 0u64;
        let mut last_record_start = 0u64;

        for segment_id in segments {
            let path = segment_path(&self.directory, segment_id);
            let mut file = match fs::File::open(&path) {
                Ok(f) => f,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(QuillSQLError::Io(e)),
            };

            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;

            let mut local_offset = 0usize;
            let segment_base = (segment_id.saturating_sub(1)) * self.segment_size;
            while local_offset < buf.len() {
                match decode_frame(&buf[local_offset..]) {
                    Ok((_frame, consumed)) => {
                        let record_start = segment_base + local_offset as u64;
                        last_record_start = record_start;
                        local_offset += consumed;
                        next_offset = segment_base + local_offset as u64;
                    }
                    Err(QuillSQLError::Internal(message))
                        if message.contains("too short")
                            || message.contains("truncated")
                            || message.contains("CRC mismatch") =>
                    {
                        // Treat partial tail or torn frame as end-of-log and stop scanning.
                        break;
                    }
                    Err(err) => return Err(err),
                }
            }
        }

        Ok((next_offset, last_record_start))
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
        for record in records {
            let len = record.encoded_len();
            if self.current_segment.size + len > self.segment_size {
                self.rotate_segment()?;
            }
            let offset = self.current_segment.size;
            self.write_bytes(offset, record.payload.clone(), false)?;
            self.current_segment.size += len;
        }
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
        if self.sync_on_flush {
            self.write_bytes(self.current_segment.size, Bytes::new(), true)?;
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
        let rx = self
            .scheduler
            .schedule_wal_write(path, offset, data, sync)
            .map_err(|e| io_error(&e))?;
        let res = rx
            .recv()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        res.map_err(|e| io_error(&e))
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
    scheduler: Arc<DiskScheduler>,
    directory: PathBuf,
    segments: Vec<u64>,
    current_idx: usize,
    offset: u64,
}

impl WalReader {
    pub fn new(directory: PathBuf, scheduler: Arc<DiskScheduler>) -> QuillSQLResult<Self> {
        let segments = list_segments(&directory)?;
        Ok(Self {
            scheduler,
            directory,
            segments,
            current_idx: 0,
            offset: 0,
        })
    }

    pub fn next_frame(&mut self) -> QuillSQLResult<Option<WalFrame>> {
        const HEADER_LEN: usize = 4 + 2 + 8 + 8 + 1 + 1 + 4;
        const CRC_LEN: usize = 4;

        loop {
            let segment_id = match self.segments.get(self.current_idx) {
                Some(id) => *id,
                None => return Ok(None),
            };
            let path = segment_path(&self.directory, segment_id);

            let header = self.read_exact(&path, self.offset, HEADER_LEN)?;
            if header.is_empty() {
                self.advance_segment();
                continue;
            }
            if header.len() < HEADER_LEN {
                return Err(QuillSQLError::Storage("Truncated WAL header".into()));
            }

            let body_len = u32::from_le_bytes(header[24..28].try_into().unwrap()) as usize;
            let body =
                self.read_exact(&path, self.offset + HEADER_LEN as u64, body_len + CRC_LEN)?;
            if body.len() < body_len + CRC_LEN {
                return Err(QuillSQLError::Storage("Truncated WAL payload".into()));
            }

            let mut frame_bytes = Vec::with_capacity(HEADER_LEN + body_len + CRC_LEN);
            frame_bytes.extend_from_slice(&header);
            frame_bytes.extend_from_slice(&body);
            self.offset += (HEADER_LEN + body_len + CRC_LEN) as u64;

            let (frame, consumed) = decode_frame(&frame_bytes)?;
            if consumed != frame_bytes.len() {
                return Err(QuillSQLError::Storage("WAL decode length mismatch".into()));
            }
            return Ok(Some(frame));
        }
    }

    fn read_exact(&self, path: &Path, offset: u64, len: usize) -> QuillSQLResult<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let rx = self
            .scheduler
            .schedule_wal_read(path.to_path_buf(), offset, len)
            .map_err(|e| e)?;
        let result = rx
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("WAL reader recv failed: {}", e)))?;
        result
    }

    fn advance_segment(&mut self) {
        self.current_idx += 1;
        self.offset = 0;
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
    std::io::Error::new(std::io::ErrorKind::Other, err.to_string())
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
        CheckpointPayload, TransactionPayload, TransactionRecordKind, WalRecordPayload,
    };
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::TempDir;

    use super::WalManager;

    fn build_scheduler(db_path: &Path) -> Arc<DiskScheduler> {
        let disk_manager = Arc::new(DiskManager::try_new(db_path).expect("disk manager"));
        Arc::new(DiskScheduler::new(disk_manager))
    }

    fn build_wal_manager_with_db(
        tmp: &TempDir,
        db_filename: &str,
        mut config: WalConfig,
    ) -> WalManager {
        let db_path = tmp.path().join(db_filename);
        config.directory = tmp.path().join("wal");
        let scheduler = build_scheduler(&db_path);
        WalManager::new(config, scheduler, None, None).expect("wal manager")
    }

    fn build_wal_manager(tmp: &TempDir) -> WalManager {
        build_wal_manager_with_db(tmp, "wal_test.db", WalConfig::default())
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

        let wal1 = build_wal_manager_with_db(&tmp, "wal_bootstrap.db", config.clone());
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

        let wal2 = build_wal_manager_with_db(&tmp, "wal_bootstrap.db", config);
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
        let db_path = tmp.path().join("wal_recycle.db");

        let mut config = WalConfig::default();
        config.directory = tmp.path().join("wal");
        config.segment_size = 64;
        config.retain_segments = 1;
        config.writer_interval_ms = None;
        let wal_dir = config.directory.clone();
        let segment_size = config.segment_size;
        let retain_segments = config.retain_segments;
        let scheduler = build_scheduler(&db_path);
        let wal = WalManager::new(config, scheduler, None, None).expect("wal manager");

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

        let keep_segment_id = 1 + (last_lsn / segment_size);
        let min_keep = keep_segment_id
            .saturating_sub(retain_segments as u64)
            .max(1);

        drop(wal);

        let remaining_ids: Vec<u64> = std::fs::read_dir(&wal_dir)
            .expect("wal dir")
            .filter_map(|entry| {
                entry.ok().and_then(|e| {
                    let name = e.file_name();
                    let name = name.to_string_lossy();
                    super::parse_segment_id(&name)
                })
            })
            .collect();

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
        let wal = build_wal_manager_with_db(&tmp, "wal_auto_flush.db", config);

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
        match &seen[0].payload {
            WalRecordPayload::Checkpoint(observed) => {
                assert_eq!(observed.last_lsn, payload.last_lsn);
                assert_eq!(observed.dirty_pages, payload.dirty_pages);
                assert_eq!(observed.active_transactions, payload.active_transactions);
            }
            other => panic!("expected checkpoint, got {:?}", other),
        }
    }
}
