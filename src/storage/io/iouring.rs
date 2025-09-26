#[cfg(target_os = "linux")]
use bytes::{Bytes, BytesMut};
#[cfg(target_os = "linux")]
use io_uring::{opcode, types, IoUring};
#[cfg(target_os = "linux")]
use std::collections::{hash_map::Entry, HashMap};
#[cfg(target_os = "linux")]
use std::fs::{self, File, OpenOptions};
#[cfg(target_os = "linux")]
use std::io;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::sync::mpsc::{self, Receiver, Sender};
#[cfg(target_os = "linux")]
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::thread;

#[cfg(target_os = "linux")]
use crate::buffer::{PageId, PAGE_SIZE};
#[cfg(target_os = "linux")]
use crate::config::IOSchedulerConfig;
#[cfg(target_os = "linux")]
use crate::error::QuillSQLError;
#[cfg(target_os = "linux")]
use crate::error::QuillSQLResult;
#[cfg(target_os = "linux")]
use crate::storage::disk_manager::DiskManager;
#[cfg(target_os = "linux")]
use crate::storage::disk_scheduler::DiskRequest;

#[cfg(target_os = "linux")]
type DiskResultSender<T> = Sender<QuillSQLResult<T>>;

#[cfg(target_os = "linux")]
struct PendingEntry {
    kind: PendingKind,
}

#[cfg(target_os = "linux")]
enum PendingKind {
    Read {
        buffer: Vec<u8>,
        sender: DiskResultSender<BytesMut>,
    },
    ReadBatch {
        buffer: Vec<u8>,
        batch: *mut BatchState,
        index: usize,
    },
    Write {
        _data: Bytes,
        state: *mut WriteState,
    },
    Fsync {
        state: *mut WriteState,
    },
    WalRead {
        buffer: Vec<u8>,
        sender: DiskResultSender<Vec<u8>>,
        expected: usize,
    },
}

#[cfg(target_os = "linux")]
struct WalFileHandle {
    _file: File,
    fd: i32,
}

#[cfg(target_os = "linux")]
struct BatchState {
    sender: DiskResultSender<Vec<BytesMut>>,
    results: Vec<Option<BytesMut>>,
    remaining: usize,
    error: Option<QuillSQLError>,
    finished: bool,
}

#[cfg(target_os = "linux")]
impl BatchState {
    fn new(sender: DiskResultSender<Vec<BytesMut>>, len: usize) -> Self {
        BatchState {
            sender,
            results: vec![None; len],
            remaining: len,
            error: None,
            finished: false,
        }
    }

    fn record_result(
        &mut self,
        index: usize,
        outcome: QuillSQLResult<BytesMut>,
    ) -> Option<QuillSQLResult<Vec<BytesMut>>> {
        if self.finished {
            return None;
        }

        match outcome {
            Ok(bytes) => {
                self.results[index] = Some(bytes);
            }
            Err(err) => {
                if self.error.is_none() {
                    self.error = Some(err);
                }
            }
        }

        if self.remaining > 0 {
            self.remaining -= 1;
        }

        if self.remaining == 0 {
            self.finished = true;
            if let Some(err) = self.error.take() {
                Some(Err(err))
            } else {
                let mut ordered = Vec::with_capacity(self.results.len());
                for entry in self.results.drain(..) {
                    ordered.push(entry.expect("batch result missing"));
                }
                Some(Ok(ordered))
            }
        } else {
            None
        }
    }
}

#[cfg(target_os = "linux")]
struct WriteState {
    sender: DiskResultSender<()>,
    pending: u8,
    error: Option<QuillSQLError>,
    finished: bool,
}

#[cfg(target_os = "linux")]
impl WriteState {
    fn new(sender: DiskResultSender<()>, pending: u8) -> Self {
        WriteState {
            sender,
            pending,
            error: None,
            finished: false,
        }
    }

    fn record(&mut self, outcome: QuillSQLResult<()>) -> bool {
        if self.finished {
            return false;
        }

        if let Err(err) = outcome {
            if self.error.is_none() {
                self.error = Some(err);
            }
        }

        if self.pending > 0 {
            self.pending -= 1;
        }

        if self.pending == 0 {
            self.finished = true;
            let final_res = match self.error.take() {
                Some(err) => Err(err),
                None => Ok(()),
            };
            if let Err(send_err) = self.sender.send(final_res) {
                log::error!("io_uring write result send failed: {}", send_err);
            }
            true
        } else {
            false
        }
    }
}

#[cfg(target_os = "linux")]
fn next_token(counter: &mut u64) -> u64 {
    let token = *counter;
    *counter = counter.wrapping_add(1);
    if *counter == 0 {
        *counter = 1;
    }
    token
}

#[cfg(target_os = "linux")]
fn push_sqe(ring: &mut IoUring, sqe: io_uring::squeue::Entry) {
    let entry = sqe;
    loop {
        let push_result = unsafe {
            let mut sq = ring.submission();
            sq.push(&entry)
        };

        match push_result {
            Ok(()) => break,
            Err(_) => {
                if let Err(err) = ring.submit() {
                    log::error!("io_uring submit while pushing sqe failed: {}", err);
                    break;
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn queue_read(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, PendingEntry>,
    token_counter: &mut u64,
    fd: i32,
    page_id: PageId,
    sender: DiskResultSender<BytesMut>,
) {
    use crate::storage::page::META_PAGE_SIZE;

    let token = next_token(token_counter);
    let mut buffer = vec![0u8; PAGE_SIZE];
    let offset = (*META_PAGE_SIZE + (page_id - 1) as usize * PAGE_SIZE) as u64;
    let read_e = opcode::Read::new(types::Fd(fd), buffer.as_mut_ptr(), PAGE_SIZE as u32)
        .offset(offset)
        .build()
        .user_data(token);

    push_sqe(ring, read_e);

    let entry = PendingEntry {
        kind: PendingKind::Read { buffer, sender },
    };
    pending.insert(token, entry);
}

#[cfg(target_os = "linux")]
fn queue_read_batch(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, PendingEntry>,
    token_counter: &mut u64,
    fd: i32,
    page_ids: Vec<PageId>,
    sender: DiskResultSender<Vec<BytesMut>>,
) {
    use crate::storage::page::META_PAGE_SIZE;

    if page_ids.is_empty() {
        if let Err(err) = sender.send(Ok(vec![])) {
            log::error!("io_uring batch read send failed: {}", err);
        }
        return;
    }

    let batch_ptr = Box::into_raw(Box::new(BatchState::new(sender, page_ids.len())));

    for (index, page_id) in page_ids.into_iter().enumerate() {
        let token = next_token(token_counter);
        let mut buffer = vec![0u8; PAGE_SIZE];
        let offset = (*META_PAGE_SIZE + (page_id - 1) as usize * PAGE_SIZE) as u64;
        let read_e = opcode::Read::new(types::Fd(fd), buffer.as_mut_ptr(), PAGE_SIZE as u32)
            .offset(offset)
            .build()
            .user_data(token);

        push_sqe(ring, read_e);

        pending.insert(
            token,
            PendingEntry {
                kind: PendingKind::ReadBatch {
                    buffer,
                    batch: batch_ptr,
                    index,
                },
            },
        );
    }
}

#[cfg(target_os = "linux")]
fn queue_write(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, PendingEntry>,
    token_counter: &mut u64,
    fd: i32,
    page_id: PageId,
    data: Bytes,
    sender: DiskResultSender<()>,
    fsync_on_write: bool,
) {
    use crate::storage::page::META_PAGE_SIZE;

    let pending_ops = if fsync_on_write { 2 } else { 1 };
    let state_ptr = Box::into_raw(Box::new(WriteState::new(sender, pending_ops)));

    let write_token = next_token(token_counter);
    let offset = (*META_PAGE_SIZE + (page_id - 1) as usize * PAGE_SIZE) as u64;
    let write_entry = opcode::Write::new(types::Fd(fd), data.as_ptr(), PAGE_SIZE as u32)
        .offset(offset)
        .build()
        .user_data(write_token);

    push_sqe(ring, write_entry);

    pending.insert(
        write_token,
        PendingEntry {
            kind: PendingKind::Write {
                _data: data,
                state: state_ptr,
            },
        },
    );

    if fsync_on_write {
        let fsync_token = next_token(token_counter);
        let fsync_entry = opcode::Fsync::new(types::Fd(fd))
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .user_data(fsync_token);

        push_sqe(ring, fsync_entry);

        pending.insert(
            fsync_token,
            PendingEntry {
                kind: PendingKind::Fsync { state: state_ptr },
            },
        );
    }
}

#[cfg(target_os = "linux")]
fn queue_wal_write(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, PendingEntry>,
    token_counter: &mut u64,
    wal_handles: &mut HashMap<PathBuf, WalFileHandle>,
    path: PathBuf,
    offset: u64,
    data: Bytes,
    sender: DiskResultSender<()>,
    sync: bool,
) -> QuillSQLResult<()> {
    if data.is_empty() && !sync {
        if let Err(err) = sender.send(Ok(())) {
            log::error!("io_uring WAL send failed: {}", err);
        }
        return Ok(());
    }

    let fd = match ensure_wal_handle(wal_handles, &path) {
        Ok(fd) => fd,
        Err(err) => {
            log::error!("ensure_wal_handle failed: {}", err);
            let _ = sender.send(Err(err));
            return Ok(());
        }
    };

    let pending_ops = (if data.is_empty() { 0 } else { 1 }) + if sync { 1 } else { 0 };
    if pending_ops == 0 {
        if let Err(err) = sender.send(Ok(())) {
            log::error!("io_uring WAL send failed: {}", err);
        }
        return Ok(());
    }

    let state_ptr = Box::into_raw(Box::new(WriteState::new(sender, pending_ops as u8)));

    if !data.is_empty() {
        let write_token = next_token(token_counter);
        let write_entry = opcode::Write::new(types::Fd(fd), data.as_ptr(), data.len() as u32)
            .offset(offset)
            .build()
            .user_data(write_token);
        push_sqe(ring, write_entry);
        pending.insert(
            write_token,
            PendingEntry {
                kind: PendingKind::Write {
                    _data: data.clone(),
                    state: state_ptr,
                },
            },
        );
    }

    if sync {
        let fsync_token = next_token(token_counter);
        let fsync_entry = opcode::Fsync::new(types::Fd(fd))
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .user_data(fsync_token);
        push_sqe(ring, fsync_entry);
        pending.insert(
            fsync_token,
            PendingEntry {
                kind: PendingKind::Fsync { state: state_ptr },
            },
        );
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn queue_wal_read(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, PendingEntry>,
    token_counter: &mut u64,
    wal_handles: &mut HashMap<PathBuf, WalFileHandle>,
    path: PathBuf,
    offset: u64,
    len: usize,
    sender: DiskResultSender<Vec<u8>>,
) -> QuillSQLResult<()> {
    if len == 0 {
        let _ = sender.send(Ok(Vec::new()));
        return Ok(());
    }
    let fd = ensure_wal_handle(wal_handles, &path)?;
    let mut buffer = vec![0u8; len];
    let token = next_token(token_counter);
    let read_entry = opcode::Read::new(types::Fd(fd), buffer.as_mut_ptr(), len as u32)
        .offset(offset)
        .build()
        .user_data(token);
    push_sqe(ring, read_entry);
    pending.insert(
        token,
        PendingEntry {
            kind: PendingKind::WalRead {
                buffer,
                sender,
                expected: len,
            },
        },
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn ensure_wal_handle(
    wal_handles: &mut HashMap<PathBuf, WalFileHandle>,
    path: &Path,
) -> QuillSQLResult<i32> {
    match wal_handles.entry(path.to_path_buf()) {
        Entry::Occupied(entry) => Ok(entry.get().fd),
        Entry::Vacant(entry) => {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(path)?;
            let fd = file.as_raw_fd();
            entry.insert(WalFileHandle { _file: file, fd });
            Ok(fd)
        }
    }
}

#[cfg(target_os = "linux")]
fn parse_wal_read_result(
    code: i32,
    mut buffer: Vec<u8>,
    expected: usize,
) -> QuillSQLResult<Vec<u8>> {
    if code < 0 {
        let err = io::Error::from_raw_os_error(-code);
        return Err(QuillSQLError::Storage(format!(
            "io_uring wal read failed: {}",
            err
        )));
    }
    let actual = code as usize;
    if actual > expected {
        return Err(QuillSQLError::Storage(format!(
            "io_uring wal read returned {actual} bytes (expected ≤ {expected})"
        )));
    }
    buffer.truncate(actual);
    Ok(buffer)
}

#[cfg(target_os = "linux")]
fn handle_request_direct(
    request: DiskRequest,
    ring: &mut IoUring,
    pending: &mut HashMap<u64, PendingEntry>,
    token_counter: &mut u64,
    fd: i32,
    disk_manager: &Arc<DiskManager>,
    fsync_on_write: bool,
    wal_handles: &mut HashMap<PathBuf, WalFileHandle>,
) {
    match request {
        DiskRequest::ReadPage {
            page_id,
            result_sender,
        } => queue_read(ring, pending, token_counter, fd, page_id, result_sender),
        DiskRequest::ReadPages {
            page_ids,
            result_sender,
        } => queue_read_batch(ring, pending, token_counter, fd, page_ids, result_sender),
        DiskRequest::WritePage {
            page_id,
            data,
            result_sender,
        } => queue_write(
            ring,
            pending,
            token_counter,
            fd,
            page_id,
            data,
            result_sender,
            fsync_on_write,
        ),
        DiskRequest::WriteWal {
            path,
            offset,
            bytes,
            sync,
            result_sender,
        } => {
            let _ = queue_wal_write(
                ring,
                pending,
                token_counter,
                wal_handles,
                path,
                offset,
                bytes,
                result_sender,
                sync,
            );
        }
        DiskRequest::ReadWal {
            path,
            offset,
            len,
            result_sender,
        } => {
            let _ = queue_wal_read(
                ring,
                pending,
                token_counter,
                wal_handles,
                path,
                offset,
                len,
                result_sender,
            );
        }
        DiskRequest::AllocatePage { result_sender } => {
            let _ = result_sender.send(disk_manager.allocate_page());
        }
        DiskRequest::DeallocatePage {
            page_id,
            result_sender,
        } => {
            let _ = result_sender.send(disk_manager.deallocate_page(page_id));
        }
        DiskRequest::Shutdown => {
            unreachable!("Shutdown handled earlier");
        }
    }
}

#[cfg(target_os = "linux")]
fn drain_completions(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, PendingEntry>,
    fsync_on_write: bool,
) {
    while let Some(cqe) = ring.completion().next() {
        let token = cqe.user_data();
        let entry = match pending.remove(&token) {
            Some(entry) => entry,
            None => continue,
        };

        let result_code = cqe.result();

        match entry.kind {
            PendingKind::Read { buffer, sender } => {
                let outcome = parse_read_result(result_code, buffer);
                if let Err(err) = sender.send(outcome) {
                    log::error!("io_uring read result send failed: {}", err);
                }
            }
            PendingKind::ReadBatch {
                buffer,
                batch,
                index,
            } => {
                let outcome = parse_read_result(result_code, buffer);
                unsafe {
                    let batch_ref = &mut *batch;
                    if let Some(result) = batch_ref.record_result(index, outcome) {
                        if let Err(err) = batch_ref.sender.send(result) {
                            log::error!("io_uring batch read send failed: {}", err);
                        }
                        drop(Box::from_raw(batch));
                    }
                }
            }
            PendingKind::Write { _data: _, state } => {
                let outcome = parse_write_result(result_code);
                unsafe {
                    let state_ref = &mut *state;
                    let finished = state_ref.record(outcome);
                    if finished {
                        drop(Box::from_raw(state));
                    }
                }
            }
            PendingKind::Fsync { state } => {
                let outcome = parse_fsync_result(result_code, fsync_on_write);
                unsafe {
                    let state_ref = &mut *state;
                    let finished = state_ref.record(outcome);
                    if finished {
                        drop(Box::from_raw(state));
                    }
                }
            }
            PendingKind::WalRead {
                buffer,
                sender,
                expected,
            } => {
                let outcome = parse_wal_read_result(result_code, buffer, expected);
                if let Err(err) = sender.send(outcome) {
                    log::error!("io_uring wal read result send failed: {}", err);
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn parse_read_result(code: i32, buffer: Vec<u8>) -> QuillSQLResult<BytesMut> {
    if code < 0 {
        let err = io::Error::from_raw_os_error(-code);
        Err(QuillSQLError::Storage(format!(
            "io_uring read failed: {}",
            err
        )))
    } else if code as usize != PAGE_SIZE {
        Err(QuillSQLError::Storage(format!(
            "io_uring short read: {} bytes",
            code
        )))
    } else {
        Ok(BytesMut::from(&buffer[..]))
    }
}

#[cfg(target_os = "linux")]
fn parse_write_result(code: i32) -> QuillSQLResult<()> {
    if code < 0 {
        let err = io::Error::from_raw_os_error(-code);
        Err(QuillSQLError::Storage(format!(
            "io_uring write failed: {}",
            err
        )))
    } else if code as usize != PAGE_SIZE {
        Err(QuillSQLError::Storage(format!(
            "io_uring short write: {} bytes",
            code
        )))
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn parse_fsync_result(code: i32, fsync_on_write: bool) -> QuillSQLResult<()> {
    if !fsync_on_write {
        return Ok(());
    }
    if code < 0 {
        let err = io::Error::from_raw_os_error(-code);
        Err(QuillSQLError::Storage(format!(
            "io_uring fdatasync failed: {}",
            err
        )))
    } else {
        Ok(())
    }
}

/// Start io_uring backend with N workers and one dispatcher.
#[cfg(target_os = "linux")]
pub fn start(
    disk_manager: Arc<DiskManager>,
    config: IOSchedulerConfig,
) -> (
    Sender<DiskRequest>,
    thread::JoinHandle<()>,
    Vec<thread::JoinHandle<()>>,
) {
    let worker_count = config.workers;
    let (request_sender, request_receiver) = mpsc::channel::<DiskRequest>();

    let mut worker_senders = Vec::with_capacity(worker_count);
    let mut worker_threads = Vec::with_capacity(worker_count);
    for i in 0..worker_count {
        let (tx, rx) = mpsc::channel::<DiskRequest>();
        worker_senders.push(tx);
        let dm = disk_manager.clone();
        let entries = config.iouring_queue_depth as u32;
        let handle = thread::Builder::new()
            .name(format!("disk-scheduler-iouring-worker-{}", i))
            .spawn(move || {
                io_uring_worker_loop(rx, dm, entries, config.fsync_on_write);
            })
            .expect("Failed to spawn DiskScheduler io_uring worker thread");
        worker_threads.push(handle);
    }

    let dispatcher_thread = thread::Builder::new()
        .name("disk-scheduler-dispatcher".to_string())
        .spawn(move || {
            crate::storage::io::thread_pool::dispatcher_loop(request_receiver, worker_senders);
        })
        .expect("Failed to spawn DiskScheduler dispatcher thread");

    (request_sender, dispatcher_thread, worker_threads)
}

#[cfg(target_os = "linux")]
fn io_uring_worker_loop(
    receiver: Receiver<DiskRequest>,
    disk_manager: Arc<DiskManager>,
    entries: u32,
    fsync_on_write: bool,
) {
    log::debug!("Disk I/O io_uring worker thread started.");
    let mut ring = IoUring::new(entries).expect("io_uring init failed");

    // Clone a dedicated File for this worker to get a stable fd
    let file = disk_manager
        .try_clone_db_file()
        .expect("clone db file for io_uring failed");
    let fd = file.as_raw_fd();

    let mut pending: HashMap<u64, PendingEntry> = HashMap::new();
    let mut wal_handles: HashMap<PathBuf, WalFileHandle> = HashMap::new();
    let mut token_counter: u64 = 1;
    let mut shutdown = false;

    while !shutdown || !pending.is_empty() {
        // Always process completions first to reduce queue pressure.
        drain_completions(&mut ring, &mut pending, fsync_on_write);

        if shutdown && pending.is_empty() {
            break;
        }

        let mut received_any = false;
        while let Ok(request) = receiver.try_recv() {
            received_any = true;
            match request {
                DiskRequest::ReadPage {
                    page_id,
                    result_sender,
                } => {
                    queue_read(
                        &mut ring,
                        &mut pending,
                        &mut token_counter,
                        fd,
                        page_id,
                        result_sender,
                    );
                }
                DiskRequest::ReadPages {
                    page_ids,
                    result_sender,
                } => {
                    queue_read_batch(
                        &mut ring,
                        &mut pending,
                        &mut token_counter,
                        fd,
                        page_ids,
                        result_sender,
                    );
                }
                DiskRequest::WritePage {
                    page_id,
                    data,
                    result_sender,
                } => {
                    queue_write(
                        &mut ring,
                        &mut pending,
                        &mut token_counter,
                        fd,
                        page_id,
                        data,
                        result_sender,
                        fsync_on_write,
                    );
                }
                DiskRequest::WriteWal {
                    path,
                    offset,
                    bytes,
                    sync,
                    result_sender,
                } => {
                    let _ = queue_wal_write(
                        &mut ring,
                        &mut pending,
                        &mut token_counter,
                        &mut wal_handles,
                        path,
                        offset,
                        bytes,
                        result_sender,
                        sync,
                    );
                }
                DiskRequest::ReadWal {
                    path,
                    offset,
                    len,
                    result_sender,
                } => {
                    let _ = queue_wal_read(
                        &mut ring,
                        &mut pending,
                        &mut token_counter,
                        &mut wal_handles,
                        path,
                        offset,
                        len,
                        result_sender,
                    );
                }
                DiskRequest::AllocatePage { result_sender } => {
                    let _ = result_sender.send(disk_manager.allocate_page());
                }
                DiskRequest::DeallocatePage {
                    page_id,
                    result_sender,
                } => {
                    let _ = result_sender.send(disk_manager.deallocate_page(page_id));
                }
                DiskRequest::Shutdown => {
                    log::debug!("Disk I/O io_uring worker received Shutdown signal.");
                    shutdown = true;
                    break;
                }
            }
        }

        if shutdown && pending.is_empty() {
            break;
        }

        if pending.is_empty() {
            // No outstanding I/O: block waiting for next request.
            match receiver.recv() {
                Ok(req) => match req {
                    DiskRequest::Shutdown => {
                        log::debug!(
                            "Disk I/O io_uring worker received Shutdown signal (idle loop)."
                        );
                        shutdown = true;
                    }
                    other => {
                        handle_request_direct(
                            other,
                            &mut ring,
                            &mut pending,
                            &mut token_counter,
                            fd,
                            &disk_manager,
                            fsync_on_write,
                            &mut wal_handles,
                        );
                    }
                },
                Err(_) => {
                    shutdown = true;
                }
            }
        } else if !received_any {
            // Pending I/O but no new work; wait for at least one completion.
            if let Err(e) = ring.submit_and_wait(1) {
                log::error!("io_uring submit_and_wait failed: {}", e);
            }
        } else if let Err(e) = ring.submit() {
            log::error!("io_uring submit failed: {}", e);
        }
    }

    // Final drain to deliver any remaining completions before exit.
    drain_completions(&mut ring, &mut pending, fsync_on_write);

    log::debug!("Disk I/O io_uring worker thread finished.");
}
