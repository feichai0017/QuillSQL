use std::{collections::VecDeque, fs::OpenOptions, path::Path, sync::Arc};

use bytes::{Bytes, BytesMut};
use log as _; // preserve existing logging usage
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::thread;

use super::{DiskCommandResultReceiver, IOBackend};
use crate::buffer::PageId;
use crate::config::IOSchedulerConfig;
use crate::error::QuillSQLResult;
use crate::storage::disk_manager::DiskManager;
use crate::storage::disk_scheduler::DiskRequest;

/// Start the legacy thread-pool backend and return the request sender plus join handles.
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

    // Create per-worker channels
    let mut worker_senders = Vec::with_capacity(worker_count);
    let mut worker_threads = Vec::with_capacity(worker_count);
    for i in 0..worker_count {
        let (tx, rx) = mpsc::sync_channel::<WorkerMessage>(channel_backlog(&config));
        worker_senders.push(tx);
        let dm = disk_manager.clone();
        let handle = thread::Builder::new()
            .name(format!("disk-scheduler-worker-{}", i))
            .spawn(move || io_worker_loop(rx, dm))
            .expect("Failed to spawn DiskScheduler worker thread");
        worker_threads.push(handle);
    }

    // Dispatcher thread
    let dispatcher_thread = thread::Builder::new()
        .name("disk-scheduler-dispatcher".to_string())
        .spawn(move || dispatcher_loop_sync(request_receiver, worker_senders))
        .expect("Failed to spawn DiskScheduler dispatcher thread");

    (request_sender, dispatcher_thread, worker_threads)
}

// ---- internal worker/dispatcher loops (moved from DiskScheduler) ----
fn dispatcher_loop_sync(
    receiver: Receiver<DiskRequest>,
    worker_senders: Vec<SyncSender<WorkerMessage>>,
) {
    log::debug!("DiskScheduler dispatcher thread started.");
    let mut rr_idx: usize = 0;
    while let Ok(request) = receiver.recv() {
        match request {
            DiskRequest::Shutdown => {
                log::debug!("Dispatcher received Shutdown. Broadcasting to workers...");
                for tx in &worker_senders {
                    let _ = tx.send(WorkerMessage::Shutdown);
                }
                break;
            }
            other => {
                if worker_senders.is_empty() {
                    log::error!("No worker_senders available to handle request");
                    break;
                }
                let n = worker_senders.len();
                let mut attempts = 0usize;
                let mut sent = false;
                while attempts < n {
                    let idx = rr_idx % n;
                    rr_idx = rr_idx.wrapping_add(1);
                    if worker_senders[idx]
                        .send(WorkerMessage::Request(other.clone()))
                        .is_ok()
                    {
                        sent = true;
                        break;
                    }
                    attempts += 1;
                }
                if !sent {
                    log::error!("All worker_senders are closed; dropping request");
                    break;
                }
            }
        }
    }
    log::debug!("DiskScheduler dispatcher thread finished.");
}

/// Legacy dispatcher that forwards `DiskRequest` directly to worker senders.
/// Retained for the io_uring backend which still expects raw requests.
pub(crate) fn dispatcher_loop(
    receiver: Receiver<DiskRequest>,
    worker_senders: Vec<Sender<DiskRequest>>,
) {
    log::debug!("DiskScheduler dispatcher thread started (plain).");
    let mut rr_idx: usize = 0;
    while let Ok(request) = receiver.recv() {
        match request {
            DiskRequest::Shutdown => {
                log::debug!("Dispatcher received Shutdown. Broadcasting to workers...");
                for tx in &worker_senders {
                    let _ = tx.send(DiskRequest::Shutdown);
                }
                break;
            }
            other => {
                if worker_senders.is_empty() {
                    log::error!("No worker_senders available to handle request");
                    break;
                }
                let n = worker_senders.len();
                let mut attempts = 0usize;
                let mut sent = false;
                while attempts < n {
                    let idx = rr_idx % n;
                    rr_idx = rr_idx.wrapping_add(1);
                    if worker_senders[idx].send(other.clone()).is_ok() {
                        sent = true;
                        break;
                    }
                    attempts += 1;
                }
                if !sent {
                    log::error!("All worker_senders are closed; dropping request");
                    break;
                }
            }
        }
    }
    log::debug!("DiskScheduler dispatcher thread finished.");
}

fn io_worker_loop(receiver: Receiver<WorkerMessage>, disk_manager: Arc<DiskManager>) {
    log::debug!("Disk I/O worker thread started.");

    #[cfg(target_os = "linux")]
    let file: WorkerFile = disk_manager.try_clone_db_file().ok();
    #[cfg(not(target_os = "linux"))]
    let file: WorkerFile = ();

    let mut local_queue: VecDeque<DiskRequest> = VecDeque::new();
    let mut shutting_down = false;

    while !shutting_down {
        // Process any queued work before blocking for more messages.
        while let Some(request) = local_queue.pop_front() {
            if matches!(request, DiskRequest::Shutdown) {
                shutting_down = true;
                break;
            }
            handle_request(&disk_manager, &file, request);
        }

        if shutting_down {
            break;
        }

        // Block for at least one message, then opportunistically drain more.
        match receiver.recv() {
            Ok(WorkerMessage::Request(req)) => {
                local_queue.push_back(req);
                loop {
                    match receiver.try_recv() {
                        Ok(WorkerMessage::Request(next_req)) => local_queue.push_back(next_req),
                        Ok(WorkerMessage::Shutdown) => {
                            shutting_down = true;
                            break;
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            shutting_down = true;
                            break;
                        }
                    }
                }
            }
            Ok(WorkerMessage::Shutdown) | Err(_) => {
                shutting_down = true;
            }
        }
    }

    // Finish any work that arrived before shutdown.
    while let Some(request) = local_queue.pop_front() {
        if matches!(request, DiskRequest::Shutdown) {
            break;
        }
        handle_request(&disk_manager, &file, request);
    }

    log::debug!("Disk I/O worker thread finished.");
}

#[cfg(target_os = "linux")]
type WorkerFile = Option<std::fs::File>;

#[cfg(not(target_os = "linux"))]
type WorkerFile = ();

fn handle_request(disk_manager: &DiskManager, file: &WorkerFile, request: DiskRequest) {
    match request {
        DiskRequest::ReadPage {
            page_id,
            result_sender,
        } => {
            let bytes_result = perform_read(disk_manager, file, page_id);
            if result_sender.send(bytes_result).is_err() {
                log::error!(
                    "DiskScheduler failed to send ReadPage result for {}",
                    page_id
                );
            }
        }
        DiskRequest::ReadPages {
            page_ids,
            result_sender,
        } => {
            let mut pages = Vec::with_capacity(page_ids.len());
            let mut error = None;
            for pid in page_ids.into_iter() {
                match perform_read(disk_manager, file, pid) {
                    Ok(bytes) => pages.push(bytes),
                    Err(e) => {
                        error = Some(e);
                        break;
                    }
                }
            }
            if let Some(e) = error {
                let _ = result_sender.send(Err(e));
            } else {
                let _ = result_sender.send(Ok(pages));
            }
        }
        DiskRequest::WritePage {
            page_id,
            data,
            result_sender,
        } => {
            let write_result = perform_write(disk_manager, file, page_id, &data);
            if result_sender.send(write_result).is_err() {
                log::error!(
                    "DiskScheduler failed to send WritePage result for {}",
                    page_id
                );
            }
        }
        DiskRequest::AllocatePage { result_sender } => {
            let result = disk_manager.allocate_page();
            if result_sender.send(result).is_err() {
                log::error!("DiskScheduler failed to send AllocatePage result");
            }
        }
        DiskRequest::DeallocatePage {
            page_id,
            result_sender,
        } => {
            let result = disk_manager.deallocate_page(page_id);
            if result_sender.send(result).is_err() {
                log::error!(
                    "DiskScheduler failed to send DeallocatePage result for {}",
                    page_id
                );
            }
        }
        DiskRequest::WriteWal {
            path,
            offset,
            bytes,
            sync,
            result_sender,
        } => {
            let result = perform_wal_write(&path, offset, &bytes, sync);
            if result_sender.send(result).is_err() {
                log::error!("DiskScheduler failed to send WriteWal result");
            }
        }
        DiskRequest::ReadWal {
            path,
            offset,
            len,
            result_sender,
        } => {
            let result = perform_wal_read(&path, offset, len);
            if result_sender.send(result).is_err() {
                log::error!("DiskScheduler failed to send ReadWal result");
            }
        }
        DiskRequest::Shutdown => {
            log::debug!("DiskScheduler worker received Shutdown request");
        }
    }
}

#[derive(Debug)]
enum WorkerMessage {
    Request(DiskRequest),
    Shutdown,
}

fn channel_backlog(config: &IOSchedulerConfig) -> usize {
    // default to 2x workers to keep some buffering without unbounded growth
    std::cmp::max(32, config.workers * 2)
}

#[cfg(target_os = "linux")]
fn perform_read(
    disk_manager: &DiskManager,
    file: &WorkerFile,
    page_id: PageId,
) -> QuillSQLResult<BytesMut> {
    match file {
        Some(f) => disk_manager
            .read_page_at_unlocked(f, page_id)
            .map(|data| BytesMut::from(&data[..])),
        None => disk_manager
            .read_page(page_id)
            .map(|data| BytesMut::from(&data[..])),
    }
}

#[cfg(not(target_os = "linux"))]
fn perform_read(
    disk_manager: &DiskManager,
    _file: &WorkerFile,
    page_id: PageId,
) -> QuillSQLResult<BytesMut> {
    disk_manager
        .read_page(page_id)
        .map(|data| BytesMut::from(&data[..]))
}

#[cfg(target_os = "linux")]
fn perform_write(
    disk_manager: &DiskManager,
    file: &WorkerFile,
    page_id: PageId,
    data: &[u8],
) -> QuillSQLResult<()> {
    match file {
        Some(f) => disk_manager
            .write_page_at_unlocked(f, page_id, data)
            .or_else(|_| disk_manager.write_page(page_id, data)),
        None => disk_manager.write_page(page_id, data),
    }
}

#[cfg(not(target_os = "linux"))]
fn perform_write(
    disk_manager: &DiskManager,
    _file: &WorkerFile,
    page_id: PageId,
    data: &[u8],
) -> QuillSQLResult<()> {
    disk_manager.write_page(page_id, data)
}

fn perform_wal_write(path: &Path, offset: u64, bytes: &[u8], sync: bool) -> QuillSQLResult<()> {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(path)?;
    if !bytes.is_empty() {
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(bytes)?;
    }
    if sync {
        file.sync_all()?;
    }
    Ok(())
}

fn perform_wal_read(path: &Path, offset: u64, len: usize) -> QuillSQLResult<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = OpenOptions::new().read(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; len];
    let mut read_total = 0usize;
    while read_total < len {
        let n = file.read(&mut buf[read_total..])?;
        if n == 0 {
            buf.truncate(read_total);
            break;
        }
        read_total += n;
    }
    Ok(buf)
}

/// ThreadPool backend (kept for future direct usage of IOBackend trait).
pub struct ThreadPoolBackend {
    pub scheduler: Arc<crate::storage::disk_scheduler::DiskScheduler>,
}

impl IOBackend for ThreadPoolBackend {
    fn schedule_read(
        &self,
        page_id: PageId,
    ) -> QuillSQLResult<DiskCommandResultReceiver<BytesMut>> {
        self.scheduler.schedule_read(page_id)
    }
    fn schedule_read_pages(
        &self,
        page_ids: Vec<PageId>,
    ) -> QuillSQLResult<DiskCommandResultReceiver<Vec<BytesMut>>> {
        self.scheduler.schedule_read_pages(page_ids)
    }
    fn schedule_write(
        &self,
        page_id: PageId,
        data: Bytes,
    ) -> QuillSQLResult<DiskCommandResultReceiver<()>> {
        self.scheduler.schedule_write(page_id, data)
    }
    fn schedule_allocate(&self) -> QuillSQLResult<DiskCommandResultReceiver<PageId>> {
        self.scheduler.schedule_allocate()
    }
    fn schedule_deallocate(
        &self,
        page_id: PageId,
    ) -> QuillSQLResult<DiskCommandResultReceiver<()>> {
        self.scheduler.schedule_deallocate(page_id)
    }
}
