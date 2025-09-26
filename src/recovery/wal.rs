use parking_lot::Mutex;
use std::cmp;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
use std::time::Duration;

use crate::config::WalConfig;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::wal_record::{decode_frame, WalFrame, WalRecordPayload};
use crate::storage::disk_scheduler::DiskScheduler;
use bytes::Bytes;

pub type Lsn = u64;

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub payload: Bytes,
}

struct WalState {
    buffer: Vec<WalRecord>,
    storage: WalStorage,
}

impl std::fmt::Debug for WalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalState")
            .field("buffer_len", &self.buffer.len())
            .field("current_segment", &self.storage.current_segment)
            .finish()
    }
}

#[derive(Debug)]
pub struct WalManager {
    next_lsn: AtomicU64,
    durable_lsn: AtomicU64,
    state: Mutex<WalState>,
    writer: Mutex<Option<WalWriterRuntime>>,
}

impl WalManager {
    pub fn new(config: WalConfig) -> QuillSQLResult<Self> {
        let _ = config;
        Err(QuillSQLError::Internal(
            "WalManager::new without DiskScheduler is unsupported; use new_with_scheduler".into(),
        ))
    }

    pub fn new_with_scheduler(
        config: WalConfig,
        scheduler: Arc<DiskScheduler>,
    ) -> QuillSQLResult<Self> {
        let storage = WalStorage::new(config, scheduler)?;
        Ok(Self {
            // Reserve 0 for "invalid" so real records start at 1.
            next_lsn: AtomicU64::new(1),
            durable_lsn: AtomicU64::new(0),
            state: Mutex::new(WalState {
                buffer: Vec::new(),
                storage,
            }),
            writer: Mutex::new(None),
        })
    }

    #[inline]
    pub fn max_assigned_lsn(&self) -> Lsn {
        self.next_lsn.load(Ordering::Acquire).saturating_sub(1)
    }

    #[inline]
    pub fn durable_lsn(&self) -> Lsn {
        self.durable_lsn.load(Ordering::Acquire)
    }

    pub fn append(&self, payload: Vec<u8>) -> Lsn {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let mut guard = self.state.lock();
        guard.buffer.push(WalRecord {
            lsn,
            payload: Bytes::from(payload),
        });
        lsn
    }

    pub fn append_record_with<F>(&self, build: F) -> QuillSQLResult<Lsn>
    where
        F: FnOnce(Lsn) -> WalRecordPayload,
    {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let payload = build(lsn);
        let encoded = Bytes::from(payload.encode(lsn));
        let mut guard = self.state.lock();
        guard.buffer.push(WalRecord {
            lsn,
            payload: encoded,
        });
        Ok(lsn)
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
        let mut guard = self.state.lock();
        let highest_buffered = guard.buffer.last().map(|r| r.lsn).unwrap_or(0);
        let desired = target.filter(|lsn| *lsn != 0).unwrap_or(highest_buffered);
        let new_durable = cmp::min(self.max_assigned_lsn(), desired);
        if new_durable == 0 {
            return Ok(self.durable_lsn());
        }

        let flush_count = guard
            .buffer
            .iter()
            .take_while(|record| record.lsn <= new_durable)
            .count();

        if flush_count > 0 {
            let to_flush: Vec<WalRecord> = guard.buffer[..flush_count].to_vec();
            guard.storage.append_records(&to_flush)?;
            guard.storage.flush()?;
            guard.buffer.drain(..flush_count);
        }

        self.durable_lsn.store(new_durable, Ordering::Release);
        Ok(new_durable)
    }

    pub fn pending_records(&self) -> Vec<WalRecord> {
        self.state.lock().buffer.clone()
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
        })
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
        self.payload.len() as u64
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
        const HEADER_LEN: usize = 4 + 2 + 8 + 1 + 4;
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

            let body_len =
                u32::from_le_bytes([header[15], header[16], header[17], header[18]]) as usize;
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
        TransactionPayload, TransactionRecordKind, WalRecordPayload,
    };
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;
    use std::sync::Arc;
    use tempfile::TempDir;

    use super::WalManager;

    fn build_wal_manager(tmp: &TempDir) -> WalManager {
        let db_path = tmp.path().join("wal_test.db");
        let disk_manager = Arc::new(DiskManager::try_new(&db_path).expect("disk manager"));
        let scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let mut config = WalConfig::default();
        config.directory = tmp.path().join("wal");
        WalManager::new_with_scheduler(config, scheduler).expect("wal manager")
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
            .expect("append record");
        let l2 = wal
            .append_record_with(|_| {
                WalRecordPayload::Transaction(TransactionPayload {
                    marker: TransactionRecordKind::Commit,
                    txn_id: 1,
                })
            })
            .expect("append record");
        assert_eq!(l1 + 1, l2);

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
}
