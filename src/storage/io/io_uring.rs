use bytes::{Bytes, BytesMut};
use io_uring::{opcode, types, IoUring};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{self, ErrorKind, IoSliceMut};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

use crate::buffer::{PageId, PAGE_SIZE};
use crate::error::QuillSQLError;
use crate::error::QuillSQLResult;
use crate::storage::disk_manager::{AlignedPageBuf, DiskManager};
use crate::storage::disk_scheduler::{DiskRequest, RequestReceiver};
use crate::storage::page::META_PAGE_SIZE;

type DiskResultSender<T> = Sender<QuillSQLResult<T>>;

struct PendingEntry {
    kind: PendingKind,
}

enum PendingKind {
    ReadFixed {
        idx: usize,
        sender: DiskResultSender<BytesMut>,
    },
    ReadVec {
        buffer: Vec<u8>,
        sender: DiskResultSender<BytesMut>,
    },
    ReadBatchFixed {
        idx: usize,
        batch: Rc<RefCell<BatchState>>,
        index: usize,
    },
    ReadBatchVec {
        buffer: Vec<u8>,
        batch: Rc<RefCell<BatchState>>,
        index: usize,
    },
    WriteFixed {
        idx: usize,
        state: Rc<RefCell<WriteState>>,
        len: usize,
    },
    WriteVec {
        state: Rc<RefCell<WriteState>>,
        _buffer: Bytes,
        len: usize,
    },
    Fsync {
        state: Rc<RefCell<WriteState>>,
        required: bool,
    },
}

struct BatchState {
    sender: DiskResultSender<Vec<BytesMut>>,
    results: Vec<Option<BytesMut>>,
    remaining: usize,
    error: Option<QuillSQLError>,
    finished: bool,
}

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

struct WriteState {
    sender: DiskResultSender<()>,
    pending: u8,
    error: Option<QuillSQLError>,
    finished: bool,
}

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

fn next_token(counter: &mut u64) -> u64 {
    let token = *counter;
    *counter = counter.wrapping_add(1);
    if *counter == 0 {
        *counter = 1;
    }
    token
}

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

struct FixedBufferPool {
    buffers: Vec<AlignedPageBuf>,
    free: Mutex<Vec<usize>>,
}

impl FixedBufferPool {
    fn new(count: usize) -> QuillSQLResult<Self> {
        if count > u16::MAX as usize {
            return Err(QuillSQLError::Internal(
                "io_uring fixed buffer count exceeds u16::MAX".into(),
            ));
        }
        let mut buffers = Vec::with_capacity(count);
        for _ in 0..count {
            buffers.push(AlignedPageBuf::new_zeroed()?);
        }
        let mut free = Vec::with_capacity(count);
        for idx in 0..count {
            free.push(idx);
        }
        Ok(Self {
            buffers,
            free: Mutex::new(free),
        })
    }

    fn register(&mut self, ring: &mut IoUring) -> io::Result<()> {
        if self.buffers.is_empty() {
            return Ok(());
        }
        let io_slices: Vec<IoSliceMut<'_>> = self
            .buffers
            .iter_mut()
            .map(|buf| IoSliceMut::new(buf.as_mut_slice()))
            .collect();
        let ptr = io_slices.as_ptr() as *const libc::iovec;
        let iovecs = unsafe { std::slice::from_raw_parts(ptr, io_slices.len()) };
        match unsafe { ring.submitter().register_buffers(iovecs) } {
            Ok(()) => Ok(()),
            Err(err)
                if err.kind() == ErrorKind::OutOfMemory
                    || err.raw_os_error() == Some(libc::ENOMEM)
                    || err.kind() == ErrorKind::InvalidInput
                    || err.raw_os_error() == Some(libc::EINVAL) =>
            {
                log::warn!(
                    "io_uring buffer registration failed ({}); falling back to heap buffers",
                    err
                );
                self.buffers.clear();
                self.free.lock().unwrap().clear();
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn acquire(&self) -> Option<(usize, *mut u8)> {
        let idx = self.free.lock().unwrap().pop()?;
        let ptr = self.buffers[idx].ptr();
        Some((idx, ptr))
    }

    fn release(&self, idx: usize) {
        self.free.lock().unwrap().push(idx);
    }

    fn fill_from_slice(&mut self, idx: usize, data: &[u8]) {
        debug_assert_eq!(data.len(), PAGE_SIZE);
        let slice = self.buffers[idx].as_mut_slice();
        slice.copy_from_slice(data);
    }

    fn extract_bytes(&self, idx: usize, len: usize) -> BytesMut {
        BytesMut::from(&self.buffers[idx].as_slice()[..len])
    }
}

struct WalFileEntry {
    _path: PathBuf,
    _file: std::fs::File,
    fd: i32,
}

struct WalFileCache {
    files: HashMap<PathBuf, WalFileEntry>,
}

impl WalFileCache {
    fn new() -> Self {
        Self {
            files: HashMap::new(),
        }
    }

    fn fd_for(&mut self, path: &Path) -> io::Result<i32> {
        if let Some(entry) = self.files.get(path) {
            return Ok(entry.fd);
        }

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        let fd = file.as_raw_fd();
        let entry = WalFileEntry {
            _path: path.to_path_buf(),
            _file: file,
            fd,
        };
        self.files.insert(path.to_path_buf(), entry);
        Ok(fd)
    }
}

fn queue_read(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, PendingEntry>,
    token_counter: &mut u64,
    buffer_pool: &FixedBufferPool,
    fd: i32,
    page_id: PageId,
    sender: DiskResultSender<BytesMut>,
) {
    let token = next_token(token_counter);
    let offset = page_file_offset(page_id);

    if let Some((idx, ptr)) = buffer_pool.acquire() {
        let read_e = opcode::ReadFixed::new(types::Fd(fd), ptr, PAGE_SIZE as u32, idx as u16)
            .offset(offset)
            .build()
            .user_data(token);
        push_sqe(ring, read_e);
        pending.insert(
            token,
            PendingEntry {
                kind: PendingKind::ReadFixed { idx, sender },
            },
        );
    } else {
        let mut buffer = vec![0u8; PAGE_SIZE];
        let read_e = opcode::Read::new(types::Fd(fd), buffer.as_mut_ptr(), PAGE_SIZE as u32)
            .offset(offset)
            .build()
            .user_data(token);
        push_sqe(ring, read_e);
        pending.insert(
            token,
            PendingEntry {
                kind: PendingKind::ReadVec { buffer, sender },
            },
        );
    }
}

fn queue_read_batch(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, PendingEntry>,
    token_counter: &mut u64,
    buffer_pool: &FixedBufferPool,
    fd: i32,
    page_ids: Vec<PageId>,
    sender: DiskResultSender<Vec<BytesMut>>,
) {
    if page_ids.is_empty() {
        if let Err(err) = sender.send(Ok(vec![])) {
            log::error!("io_uring batch read send failed: {}", err);
        }
        return;
    }

    let batch = Rc::new(RefCell::new(BatchState::new(sender, page_ids.len())));

    for (index, page_id) in page_ids.into_iter().enumerate() {
        let token = next_token(token_counter);
        let offset = page_file_offset(page_id);
        if let Some((idx, ptr)) = buffer_pool.acquire() {
            let read_e = opcode::ReadFixed::new(types::Fd(fd), ptr, PAGE_SIZE as u32, idx as u16)
                .offset(offset)
                .build()
                .user_data(token);
            push_sqe(ring, read_e);
            pending.insert(
                token,
                PendingEntry {
                    kind: PendingKind::ReadBatchFixed {
                        idx,
                        batch: Rc::clone(&batch),
                        index,
                    },
                },
            );
        } else {
            let mut buffer = vec![0u8; PAGE_SIZE];
            let read_e = opcode::Read::new(types::Fd(fd), buffer.as_mut_ptr(), PAGE_SIZE as u32)
                .offset(offset)
                .build()
                .user_data(token);
            push_sqe(ring, read_e);
            pending.insert(
                token,
                PendingEntry {
                    kind: PendingKind::ReadBatchVec {
                        buffer,
                        batch: Rc::clone(&batch),
                        index,
                    },
                },
            );
        }
    }
}

fn queue_write(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, PendingEntry>,
    token_counter: &mut u64,
    buffer_pool: &mut FixedBufferPool,
    fd: i32,
    page_id: PageId,
    data: Bytes,
    sender: DiskResultSender<()>,
    fsync_on_write: bool,
) {
    let has_data = !data.is_empty();
    let pending_ops = (if has_data { 1 } else { 0 }) + if fsync_on_write { 1 } else { 0 };

    if pending_ops == 0 {
        if let Err(err) = sender.send(Ok(())) {
            log::error!("io_uring write result send failed: {}", err);
        }
        return;
    }

    let state = Rc::new(RefCell::new(WriteState::new(sender, pending_ops as u8)));

    if has_data {
        debug_assert_eq!(data.len(), PAGE_SIZE);
        let write_token = next_token(token_counter);
        let offset = page_file_offset(page_id);

        if let Some((idx, ptr)) = buffer_pool.acquire() {
            buffer_pool.fill_from_slice(idx, &data);
            let entry = opcode::WriteFixed::new(types::Fd(fd), ptr, data.len() as u32, idx as u16)
                .offset(offset)
                .build()
                .user_data(write_token);
            push_sqe(ring, entry);
            pending.insert(
                write_token,
                PendingEntry {
                    kind: PendingKind::WriteFixed {
                        idx,
                        state: Rc::clone(&state),
                        len: data.len(),
                    },
                },
            );
        } else {
            let buffer = data.clone();
            let entry = opcode::Write::new(types::Fd(fd), buffer.as_ptr(), buffer.len() as u32)
                .offset(offset)
                .build()
                .user_data(write_token);
            push_sqe(ring, entry);
            pending.insert(
                write_token,
                PendingEntry {
                    kind: PendingKind::WriteVec {
                        state: Rc::clone(&state),
                        _buffer: buffer,
                        len: data.len(),
                    },
                },
            );
        }
    }

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
                kind: PendingKind::Fsync {
                    state,
                    required: true,
                },
            },
        );
    }
}

fn queue_wal_write(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, PendingEntry>,
    token_counter: &mut u64,
    fd: i32,
    offset: u64,
    data: Bytes,
    sender: DiskResultSender<()>,
    sync: bool,
) {
    let has_data = !data.is_empty();
    let pending_ops = (if has_data { 1 } else { 0 }) + if sync { 1 } else { 0 };

    if pending_ops == 0 {
        if let Err(err) = sender.send(Ok(())) {
            log::error!("io_uring WAL write result send failed: {}", err);
        }
        return;
    }

    let state = Rc::new(RefCell::new(WriteState::new(sender, pending_ops as u8)));

    if has_data {
        let write_token = next_token(token_counter);
        let buffer = data.clone();
        let entry = opcode::Write::new(types::Fd(fd), buffer.as_ptr(), buffer.len() as u32)
            .offset(offset)
            .build()
            .user_data(write_token);
        push_sqe(ring, entry);
        pending.insert(
            write_token,
            PendingEntry {
                kind: PendingKind::WriteVec {
                    state: Rc::clone(&state),
                    _buffer: buffer,
                    len: data.len(),
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
                kind: PendingKind::Fsync {
                    state,
                    required: true,
                },
            },
        );
    }
}

fn queue_wal_fsync(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, PendingEntry>,
    token_counter: &mut u64,
    fd: i32,
    sender: DiskResultSender<()>,
) {
    let state = Rc::new(RefCell::new(WriteState::new(sender, 1)));
    let fsync_token = next_token(token_counter);
    let fsync_entry = opcode::Fsync::new(types::Fd(fd))
        .flags(types::FsyncFlags::DATASYNC)
        .build()
        .user_data(fsync_token);
    push_sqe(ring, fsync_entry);
    pending.insert(
        fsync_token,
        PendingEntry {
            kind: PendingKind::Fsync {
                state,
                required: true,
            },
        },
    );
}

fn handle_request_direct(
    request: DiskRequest,
    ring: &mut IoUring,
    pending: &mut HashMap<u64, PendingEntry>,
    token_counter: &mut u64,
    fd: i32,
    disk_manager: &Arc<DiskManager>,
    fsync_on_write: bool,
    buffer_pool: &mut FixedBufferPool,
    wal_files: &mut WalFileCache,
) -> bool {
    match request {
        DiskRequest::ReadPage {
            page_id,
            result_sender,
        } => {
            queue_read(
                ring,
                pending,
                token_counter,
                buffer_pool,
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
                ring,
                pending,
                token_counter,
                buffer_pool,
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
                ring,
                pending,
                token_counter,
                buffer_pool,
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
            data,
            sync,
            result_sender,
        } => match wal_files.fd_for(&path) {
            Ok(wal_fd) => {
                queue_wal_write(
                    ring,
                    pending,
                    token_counter,
                    wal_fd,
                    offset,
                    data,
                    result_sender,
                    sync,
                );
            }
            Err(err) => {
                let _ = result_sender.send(Err(QuillSQLError::Io(err)));
            }
        },
        DiskRequest::FsyncWal {
            path,
            result_sender,
        } => match wal_files.fd_for(&path) {
            Ok(wal_fd) => {
                queue_wal_fsync(ring, pending, token_counter, wal_fd, result_sender);
            }
            Err(err) => {
                let _ = result_sender.send(Err(QuillSQLError::Io(err)));
            }
        },
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
            return true;
        }
    }
    false
}

fn drain_completions(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, PendingEntry>,
    buffer_pool: &FixedBufferPool,
) {
    while let Some(cqe) = ring.completion().next() {
        let token = cqe.user_data();
        if let Some(entry) = pending.remove(&token) {
            let result_code = cqe.result();
            match entry.kind {
                PendingKind::ReadFixed { idx, sender } => {
                    let bytes = buffer_pool.extract_bytes(idx, PAGE_SIZE);
                    buffer_pool.release(idx);
                    let outcome = parse_read_result(result_code, bytes);
                    if let Err(err) = sender.send(outcome) {
                        log::error!("io_uring read fixed result send failed: {}", err);
                    }
                }
                PendingKind::ReadVec { buffer, sender } => {
                    let buffer = BytesMut::from(&buffer[..]);
                    let outcome = parse_read_result(result_code, buffer);
                    if let Err(err) = sender.send(outcome) {
                        log::error!("io_uring read result send failed: {}", err);
                    }
                }
                PendingKind::ReadBatchFixed { idx, batch, index } => {
                    let bytes = buffer_pool.extract_bytes(idx, PAGE_SIZE);
                    buffer_pool.release(idx);
                    let outcome = parse_read_result(result_code, bytes);
                    let mut batch_ref = batch.borrow_mut();
                    if let Some(result) = batch_ref.record_result(index, outcome) {
                        if let Err(err) = batch_ref.sender.send(result) {
                            log::error!("io_uring batch read send failed: {}", err);
                        }
                    }
                }
                PendingKind::ReadBatchVec {
                    buffer,
                    batch,
                    index,
                } => {
                    let buffer = BytesMut::from(&buffer[..]);
                    let outcome = parse_read_result(result_code, buffer);
                    let mut batch_ref = batch.borrow_mut();
                    if let Some(result) = batch_ref.record_result(index, outcome) {
                        if let Err(err) = batch_ref.sender.send(result) {
                            log::error!("io_uring batch read send failed: {}", err);
                        }
                    }
                }
                PendingKind::WriteFixed { idx, state, len } => {
                    buffer_pool.release(idx);
                    let outcome = parse_write_result(result_code, len);
                    let _ = state.borrow_mut().record(outcome);
                }
                PendingKind::WriteVec { state, len, .. } => {
                    let outcome = parse_write_result(result_code, len);
                    let _ = state.borrow_mut().record(outcome);
                }
                PendingKind::Fsync { state, required } => {
                    let outcome = parse_fsync_result(result_code, required);
                    let _ = state.borrow_mut().record(outcome);
                }
            }
        }
    }
}

fn parse_read_result(code: i32, buffer: BytesMut) -> QuillSQLResult<BytesMut> {
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
        Ok(buffer)
    }
}

fn parse_write_result(code: i32, expected: usize) -> QuillSQLResult<()> {
    if code < 0 {
        let err = io::Error::from_raw_os_error(-code);
        Err(QuillSQLError::Storage(format!(
            "io_uring write failed: {}",
            err
        )))
    } else if code as usize != expected {
        Err(QuillSQLError::Storage(format!(
            "io_uring short write: {} bytes",
            code
        )))
    } else {
        Ok(())
    }
}

fn parse_fsync_result(code: i32, required: bool) -> QuillSQLResult<()> {
    if !required {
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

#[inline]
fn page_file_offset(page_id: PageId) -> u64 {
    (*META_PAGE_SIZE + (page_id - 1) as usize * PAGE_SIZE) as u64
}

pub(crate) fn worker_loop(
    receiver: RequestReceiver,
    disk_manager: Arc<DiskManager>,
    entries: u32,
    fixed_buffers: usize,
    sqpoll_idle: Option<u32>,
    fsync_on_write: bool,
) {
    log::debug!("Disk I/O io_uring worker thread started.");
    let mut builder = IoUring::builder();
    if let Some(idle) = sqpoll_idle {
        builder.setup_sqpoll(idle);
    }
    let mut ring = builder.build(entries).expect("io_uring init failed");

    let mut buffer_pool = FixedBufferPool::new(fixed_buffers).expect("create fixed buffer pool");
    buffer_pool
        .register(&mut ring)
        .expect("register fixed buffers");

    // Clone a dedicated File for this worker to get a stable fd
    let file = disk_manager
        .try_clone_db_file()
        .expect("clone db file for io_uring failed");
    let fd = file.as_raw_fd();

    let mut pending: HashMap<u64, PendingEntry> = HashMap::new();
    let mut token_counter: u64 = 1;
    let mut shutdown = false;
    let mut wal_files = WalFileCache::new();

    while !shutdown || !pending.is_empty() {
        // Always process completions first to reduce queue pressure.
        drain_completions(&mut ring, &mut pending, &buffer_pool);

        if shutdown && pending.is_empty() {
            break;
        }

        let mut received_any = false;
        while let Some(request) = receiver.try_recv() {
            received_any = true;
            if handle_request_direct(
                request,
                &mut ring,
                &mut pending,
                &mut token_counter,
                fd,
                &disk_manager,
                fsync_on_write,
                &mut buffer_pool,
                &mut wal_files,
            ) {
                shutdown = true;
                break;
            }
        }

        if receiver.is_shutdown() {
            shutdown = true;
        }

        if shutdown && pending.is_empty() {
            break;
        }

        if pending.is_empty() {
            // No outstanding I/O: block waiting for next request.
            match receiver.recv() {
                Some(req) => {
                    if handle_request_direct(
                        req,
                        &mut ring,
                        &mut pending,
                        &mut token_counter,
                        fd,
                        &disk_manager,
                        fsync_on_write,
                        &mut buffer_pool,
                        &mut wal_files,
                    ) {
                        shutdown = true;
                    }
                }
                None => {
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
    drain_completions(&mut ring, &mut pending, &buffer_pool);

    log::debug!("Disk I/O io_uring worker thread finished.");
}
