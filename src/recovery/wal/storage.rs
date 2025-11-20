use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;

use crate::config::WalConfig;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::recovery::wal::codec::decode_frame;
use crate::recovery::wal::page::{
    WalFrameContinuation, WalPage, WalPageFragmentKind, WAL_PAGE_SIZE,
};
use crate::recovery::wal::Lsn;

use super::io::{WalIoTicket, WalSink};
use super::record::WalRecord;

#[derive(Clone, Debug)]
struct WalSegmentInfo {
    id: u64,
    size: u64,
}

#[derive(Clone, Debug)]
struct SealedSegment {
    id: u64,
    last_lsn: Lsn,
}

pub(super) struct WalFlushTicket {
    pub(super) lsn: Lsn,
    pub(super) receiver: WalIoTicket,
}

pub(super) struct WalStorage {
    pub(super) directory: PathBuf,
    segment_size: u64,
    sync_on_flush: bool,
    current_segment: WalSegmentInfo,
    sink: Arc<dyn WalSink>,
    retain_segments: usize,
    open_page: Option<WalPage>,
    last_page_end_lsn: Lsn,
    last_sync_request: Lsn,
    active_segment_synced_lsn: Lsn,
    sealed_segments: VecDeque<SealedSegment>,
    pending: VecDeque<WalFlushTicket>,
}

impl WalStorage {
    pub(super) fn new(config: WalConfig, sink: Arc<dyn WalSink>) -> QuillSQLResult<Self> {
        fs::create_dir_all(&config.directory)?;
        let min_segment = (WAL_PAGE_SIZE as u64) * 2;
        let segment_size = config.segment_size.max(min_segment);
        let segment = discover_latest_segment(&config.directory, segment_size)?;
        Ok(Self {
            directory: config.directory,
            segment_size,
            sync_on_flush: config.sync_on_flush,
            current_segment: segment,
            sink,
            retain_segments: config.retain_segments.max(1),
            open_page: None,
            last_page_end_lsn: 0,
            last_sync_request: 0,
            active_segment_synced_lsn: 0,
            sealed_segments: VecDeque::new(),
            pending: VecDeque::new(),
        })
    }

    pub(super) fn recover_offsets(&mut self) -> QuillSQLResult<(Lsn, Lsn)> {
        let segments = list_segments(&self.directory)?;
        let mut next_lsn = 0u64;
        let mut last_record_start = 0u64;
        let mut pending_fragment = Vec::new();
        let mut physical_offset = 0u64;

        for segment_id in segments {
            let path = segment_path(&self.directory, segment_id);
            let file = match File::open(&path) {
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
        self.last_sync_request = next_lsn;
        self.active_segment_synced_lsn = next_lsn;
        self.sealed_segments.clear();
        let segment_index = physical_offset / self.segment_size;
        self.current_segment = WalSegmentInfo {
            id: segment_index + 1,
            size: physical_offset % self.segment_size,
        };

        Ok((next_lsn, last_record_start))
    }

    pub(super) fn append_records(&mut self, records: &[WalRecord]) -> QuillSQLResult<()> {
        let mut queue: Vec<WalRecord> = records.to_vec();
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

    pub(super) fn flush(&mut self, force_sync: bool) -> QuillSQLResult<()> {
        self.flush_open_page()?;

        if self.sync_on_flush || force_sync {
            self.sync_sealed_segments()?;
            self.sync_active_segment()?;
        }
        Ok(())
    }

    pub(super) fn take_ready(&mut self, upto: Lsn) -> Vec<WalFlushTicket> {
        let mut ready = Vec::new();
        while let Some(front) = self.pending.front() {
            if front.lsn <= upto {
                if let Some(ticket) = self.pending.pop_front() {
                    ready.push(ticket);
                }
            } else {
                break;
            }
        }
        ready
    }

    pub(super) fn recycle_segments(&mut self, keep_from_lsn: Lsn) -> QuillSQLResult<()> {
        if self.retain_segments == 0 || keep_from_lsn == 0 {
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

    pub(super) fn directory_path(&self) -> PathBuf {
        self.directory.clone()
    }

    fn flush_open_page(&mut self) -> QuillSQLResult<()> {
        if let Some(page) = self.open_page.take() {
            self.write_page(&page)?;
        }
        Ok(())
    }

    fn seal_current_segment(&mut self) {
        if self.current_segment.size == 0 {
            return;
        }
        let sealed = SealedSegment {
            id: self.current_segment.id,
            last_lsn: self.last_page_end_lsn,
        };
        self.sealed_segments.push_back(sealed);
        self.current_segment = WalSegmentInfo {
            id: self.current_segment.id + 1,
            size: 0,
        };
        self.active_segment_synced_lsn = 0;
    }

    fn sync_sealed_segments(&mut self) -> QuillSQLResult<()> {
        while let Some(segment) = self.sealed_segments.pop_front() {
            self.schedule_segment_sync(segment.id, segment.last_lsn)?;
        }
        Ok(())
    }

    fn sync_active_segment(&mut self) -> QuillSQLResult<()> {
        if self.last_page_end_lsn == 0 || self.last_page_end_lsn == self.active_segment_synced_lsn {
            return Ok(());
        }
        self.schedule_segment_sync(self.current_segment.id, self.last_page_end_lsn)?;
        self.active_segment_synced_lsn = self.last_page_end_lsn;
        Ok(())
    }

    fn schedule_segment_sync(&mut self, segment_id: u64, upto_lsn: Lsn) -> QuillSQLResult<()> {
        let path = segment_path(&self.directory, segment_id);
        if let Some(receiver) = self.sink.schedule_fsync(path)? {
            self.pending.push_back(WalFlushTicket {
                lsn: upto_lsn,
                receiver,
            });
        }
        self.last_sync_request = self.last_sync_request.max(upto_lsn);
        Ok(())
    }

    fn write_page(&mut self, page: &WalPage) -> QuillSQLResult<()> {
        if !page.has_payload() {
            return Ok(());
        }
        if self.current_segment.size + WAL_PAGE_SIZE as u64 > self.segment_size {
            self.seal_current_segment();
        }
        let offset = self.current_segment.size;
        let bytes = Bytes::from(page.to_bytes());
        let path = segment_path(&self.directory, self.current_segment.id);
        let receiver = self.sink.schedule_write(path, offset, bytes, false)?;
        self.current_segment.size += WAL_PAGE_SIZE as u64;
        if let Some(lsn) = page.last_end_lsn() {
            self.last_page_end_lsn = lsn;
        }
        if let Some(receiver) = receiver {
            let ticket_lsn = self.last_page_end_lsn;
            self.pending.push_back(WalFlushTicket {
                lsn: ticket_lsn,
                receiver,
            });
        }
        Ok(())
    }
}

fn discover_latest_segment(directory: &Path, segment_size: u64) -> QuillSQLResult<WalSegmentInfo> {
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

pub(super) fn segment_path(directory: &Path, id: u64) -> PathBuf {
    directory.join(format!("wal_{:016X}.log", id))
}

pub(super) fn parse_segment_id(name: &str) -> Option<u64> {
    let name = name.strip_prefix("wal_")?;
    let name = name.strip_suffix(".log")?;
    u64::from_str_radix(name, 16).ok()
}

pub(super) fn list_segments(directory: &Path) -> QuillSQLResult<Vec<u64>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WalConfig;
    use crate::recovery::wal::codec::encode_frame;
    use crate::recovery::wal_record::{
        PageWritePayload, TransactionPayload, TransactionRecordKind, WalRecordPayload,
    };
    use bytes::Bytes;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::mpsc;
    use tempfile::TempDir;

    #[derive(Default)]
    struct BlockingSink;

    impl BlockingSink {
        fn finish_ticket(&self) -> WalIoTicket {
            let (tx, rx) = mpsc::channel();
            tx.send(Ok(())).unwrap();
            rx
        }
    }

    impl WalSink for BlockingSink {
        fn schedule_write(
            &self,
            path: PathBuf,
            offset: u64,
            data: Bytes,
            sync: bool,
        ) -> QuillSQLResult<Option<WalIoTicket>> {
            if data.is_empty() && !sync {
                return Ok(None);
            }
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&path)?;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&data)?;
            if sync {
                file.sync_data()?;
            }
            Ok(Some(self.finish_ticket()))
        }

        fn schedule_fsync(&self, path: PathBuf) -> QuillSQLResult<Option<WalIoTicket>> {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&path)?;
            file.sync_all()?;
            Ok(Some(self.finish_ticket()))
        }
    }

    fn make_record(start: Lsn, prev: Lsn, txn_id: u64) -> WalRecord {
        let payload = WalRecordPayload::Transaction(TransactionPayload {
            marker: TransactionRecordKind::Begin,
            txn_id,
        });
        let bytes = encode_frame(start, prev, &payload);
        let end_lsn = start + bytes.len() as u64;
        WalRecord {
            start_lsn: start,
            end_lsn,
            payload: Bytes::from(bytes),
        }
    }

    fn build_records(count: usize) -> Vec<WalRecord> {
        let mut records = Vec::new();
        let mut start = 0;
        let mut prev = 0;
        for i in 0..count {
            let record = make_record(start, prev, i as u64 + 1);
            prev = start;
            start = record.end_lsn;
            records.push(record);
        }
        records
    }

    fn drain_tickets(storage: &mut WalStorage, upto: Lsn) {
        for ticket in storage.take_ready(upto) {
            ticket.receiver.recv().unwrap().unwrap();
        }
    }

    fn build_large_page_records(count: usize) -> Vec<WalRecord> {
        let mut records = Vec::new();
        let mut start = 0;
        let mut prev = 0;
        for i in 0..count {
            let payload = WalRecordPayload::PageWrite(PageWritePayload {
                page_id: i as u32 + 1,
                prev_page_lsn: prev,
                page_image: vec![i as u8; WAL_PAGE_SIZE],
            });
            let bytes = encode_frame(start, prev, &payload);
            let end_lsn = start + bytes.len() as u64;
            records.push(WalRecord {
                start_lsn: start,
                end_lsn,
                payload: Bytes::from(bytes),
            });
            prev = start;
            start = end_lsn;
        }
        records
    }

    #[test]
    fn recover_offsets_reads_existing_segments() {
        let tmp = TempDir::new().expect("tempdir");
        let mut config = WalConfig::default();
        config.directory = tmp.path().to_path_buf();
        config.segment_size = WAL_PAGE_SIZE as u64 * 4;

        let sink = Arc::new(BlockingSink::default());
        let mut storage = WalStorage::new(config.clone(), sink.clone()).expect("storage");
        let records = build_records(4);
        storage.append_records(&records).expect("append");
        storage.flush(true).expect("flush");
        let upto = storage.last_page_end_lsn;
        drain_tickets(&mut storage, upto);
        drop(storage);

        let mut reloaded = WalStorage::new(config, sink).expect("storage reload");
        let (next_lsn, last_start) = reloaded.recover_offsets().expect("recover");
        assert!(next_lsn > 0);
        assert_eq!(last_start, records.last().unwrap().start_lsn);
    }

    #[test]
    fn recycle_segments_removes_old_files() {
        let tmp = TempDir::new().expect("tempdir");
        let mut config = WalConfig::default();
        config.directory = tmp.path().to_path_buf();
        config.segment_size = WAL_PAGE_SIZE as u64;
        config.retain_segments = 1;

        let sink = Arc::new(BlockingSink::default());
        let mut storage = WalStorage::new(config, sink).expect("storage");
        let records = build_large_page_records(6);
        for chunk in records.chunks(1) {
            storage.append_records(chunk).expect("append");
            storage.flush(true).expect("flush");
            let upto = storage.last_page_end_lsn;
            drain_tickets(&mut storage, upto);
        }

        let segments_before = list_segments(&storage.directory).expect("segments");
        assert!(
            segments_before.len() >= 3,
            "expected multiple segments, got {:?}",
            segments_before
        );

        let keep_lsn = records.last().unwrap().start_lsn;
        storage.recycle_segments(keep_lsn).expect("recycle");
        let segments_after = list_segments(&storage.directory).expect("segments");
        assert!(segments_after.len() < segments_before.len());
    }
}
