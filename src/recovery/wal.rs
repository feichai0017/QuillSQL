use parking_lot::Mutex;
use std::cmp;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::WalConfig;
use crate::error::QuillSQLResult;
use crate::recovery::wal_record::WalRecordPayload;

pub type Lsn = u64;

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub payload: Vec<u8>,
}

struct WalState {
    buffer: Vec<WalRecord>,
    storage: WalStorage,
}

impl std::fmt::Debug for WalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalState")
            .field("buffer_len", &self.buffer.len())
            .field("storage", &self.storage)
            .finish()
    }
}

#[derive(Debug)]
pub struct WalManager {
    next_lsn: AtomicU64,
    durable_lsn: AtomicU64,
    state: Mutex<WalState>,
}

impl WalManager {
    pub fn new(config: WalConfig) -> QuillSQLResult<Self> {
        let storage = WalStorage::new(config)?;
        Ok(Self {
            // Reserve 0 for "invalid" so real records start at 1.
            next_lsn: AtomicU64::new(1),
            durable_lsn: AtomicU64::new(0),
            state: Mutex::new(WalState {
                buffer: Vec::new(),
                storage,
            }),
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
        guard.buffer.push(WalRecord { lsn, payload });
        lsn
    }

    pub fn append_record_with<F>(&self, build: F) -> QuillSQLResult<Lsn>
    where
        F: FnOnce(Lsn) -> WalRecordPayload,
    {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let payload = build(lsn);
        let encoded = payload.encode(lsn);
        let mut guard = self.state.lock();
        guard.buffer.push(WalRecord {
            lsn,
            payload: encoded,
        });
        Ok(lsn)
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
    writer: BufWriter<File>,
}

impl WalStorage {
    fn new(config: WalConfig) -> QuillSQLResult<Self> {
        fs::create_dir_all(&config.directory)?;
        let segment = Self::discover_latest_segment(&config.directory, config.segment_size)?;
        let (segment_info, writer) = Self::open_segment(&config.directory, segment)?;

        Ok(Self {
            directory: config.directory,
            segment_size: config.segment_size,
            sync_on_flush: config.sync_on_flush,
            current_segment: segment_info,
            writer,
        })
    }

    fn append_records(&mut self, records: &[WalRecord]) -> std::io::Result<()> {
        for record in records {
            self.write_record(record)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()?;
        if self.sync_on_flush {
            self.writer.get_mut().sync_all()?;
        }
        Ok(())
    }

    fn write_record(&mut self, record: &WalRecord) -> std::io::Result<()> {
        let encoded_len = record.encoded_len();
        if self.current_segment.size + encoded_len > self.segment_size {
            self.rotate_segment()?;
        }

        let payload_len = record.payload.len() as u32;
        self.writer.write_all(&payload_len.to_le_bytes())?;
        self.writer.write_all(&record.lsn.to_le_bytes())?;
        self.writer.write_all(&record.payload)?;

        self.current_segment.size += encoded_len;
        Ok(())
    }

    fn rotate_segment(&mut self) -> std::io::Result<()> {
        self.flush()?;
        let next_id = self.current_segment.id + 1;
        let (segment_info, writer) = Self::open_segment(
            &self.directory,
            WalSegmentInfo {
                id: next_id,
                size: 0,
            },
        )?;
        self.current_segment = segment_info;
        self.writer = writer;
        Ok(())
    }

    fn open_segment(
        directory: &Path,
        segment: WalSegmentInfo,
    ) -> std::io::Result<(WalSegmentInfo, BufWriter<File>)> {
        let path = segment_path(directory, segment.id);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?;
        let metadata = file.metadata()?;
        let size = metadata.len();

        if size == 0 && segment.size != 0 {
            // If caller specified size but file is empty, honor specified size
            Ok((
                WalSegmentInfo {
                    id: segment.id,
                    size: segment.size,
                },
                BufWriter::new(file),
            ))
        } else {
            Ok((
                WalSegmentInfo {
                    id: segment.id,
                    size,
                },
                BufWriter::new(file),
            ))
        }
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
        const HEADER_LEN: u64 = 4 + 8; // payload length + LSN
        HEADER_LEN + self.payload.len() as u64
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

#[cfg(test)]
mod tests {
    use crate::config::WalConfig;
    use crate::recovery::wal_record::{
        TransactionPayload, TransactionRecordKind, WalRecordPayload,
    };
    use tempfile::TempDir;

    use super::WalManager;

    #[test]
    fn append_and_flush_tracks_durable_lsn() {
        let tmp = TempDir::new().expect("tempdir");
        let mut config = WalConfig::default();
        config.directory = tmp.path().join("wal");
        let wal = WalManager::new(config).expect("wal manager init");
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
}
