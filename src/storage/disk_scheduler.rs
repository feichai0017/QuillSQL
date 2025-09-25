use super::disk_manager::DiskManager;
use crate::buffer::PageId;
use crate::config::{IOSchedulerConfig, IOStrategy};
use crate::error::{QuillSQLError, QuillSQLResult};
use bytes::{Bytes, BytesMut};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;

#[derive(Debug)]
pub enum DiskError {
    Io(std::io::Error),
    Cancelled,
}

pub enum DiskResponse {
    Read { data: BytesMut },
    Write,
    Allocate { page_id: PageId },
    Error(QuillSQLError),
}
// IOStrategy and IOSchedulerConfig moved to crate::config

// Type alias for the sender part of the result channel
pub type DiskCommandResultSender<T> = Sender<QuillSQLResult<T>>;
// Type alias for the receiver part of the result channel
pub type DiskCommandResultReceiver<T> = Receiver<QuillSQLResult<T>>;

// Commands sent from BufferPoolManager to the DiskScheduler task
#[derive(Debug, Clone)]
pub enum DiskRequest {
    ReadPage {
        page_id: PageId,
        result_sender: DiskCommandResultSender<BytesMut>,
    },
    /// Read arbitrary pages by id order; returns buffers in the same order
    ReadPages {
        page_ids: Vec<PageId>,
        result_sender: DiskCommandResultSender<Vec<BytesMut>>,
    },
    WritePage {
        page_id: PageId,
        data: Bytes,
        result_sender: DiskCommandResultSender<()>,
    },
    AllocatePage {
        result_sender: Sender<QuillSQLResult<PageId>>,
    },
    DeallocatePage {
        page_id: PageId,
        result_sender: DiskCommandResultSender<()>,
    },
    Shutdown,
}

// Structure to manage the background I/O thread
#[derive(Debug)]
pub struct DiskScheduler {
    request_sender: Sender<DiskRequest>,
    // Dispatcher thread receives all requests and forwards to workers
    dispatcher_thread: Option<thread::JoinHandle<()>>,
    // Worker threads execute actual I/O tasks concurrently
    worker_threads: Vec<thread::JoinHandle<()>>,
    /// Centralized runtime configuration
    pub config: IOSchedulerConfig,
}

impl DiskScheduler {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self {
        Self::new_with_config(disk_manager, IOSchedulerConfig::default())
    }

    pub fn new_with_config(disk_manager: Arc<DiskManager>, config: IOSchedulerConfig) -> Self {
        let worker_count = config.workers;
        let (request_sender, request_receiver) = mpsc::channel::<DiskRequest>();

        // Create per-worker channels
        let mut worker_senders = Vec::with_capacity(worker_count);
        let mut worker_threads = Vec::with_capacity(worker_count);
        for i in 0..worker_count {
            let (tx, rx) = mpsc::channel::<DiskRequest>();
            worker_senders.push(tx);
            let dm = disk_manager.clone();
            let handle = thread::Builder::new()
                .name(format!("disk-scheduler-worker-{}", i))
                .spawn(move || {
                    Self::io_worker_loop(rx, dm);
                })
                .expect("Failed to spawn DiskScheduler worker thread");
            worker_threads.push(handle);
        }

        // Spawn dispatcher thread to forward requests in round-robin
        let dispatcher_thread = thread::Builder::new()
            .name("disk-scheduler-dispatcher".to_string())
            .spawn(move || {
                Self::dispatcher_loop(request_receiver, worker_senders);
            })
            .expect("Failed to spawn DiskScheduler dispatcher thread");

        DiskScheduler {
            request_sender,
            dispatcher_thread: Some(dispatcher_thread),
            worker_threads,
            config,
        }
    }

    /// Create scheduler with explicit strategy. IoUring currently logs and
    /// falls back to thread-pool to preserve compatibility.
    pub fn new_with_strategy(disk_manager: Arc<DiskManager>, strategy: IOStrategy) -> Self {
        match strategy {
            IOStrategy::ThreadPool { workers } => {
                let mut cfg = IOSchedulerConfig::default();
                if let Some(w) = workers {
                    cfg.workers = w;
                }
                Self::new_with_config(disk_manager, cfg)
            }
            IOStrategy::IoUring { queue_depth } => {
                Self::new_with_iouring(disk_manager, queue_depth)
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn new_with_iouring(disk_manager: Arc<DiskManager>) -> Self {
        eprintln!("WARN: IoUring selected on non-Linux platform; falling back to thread-pool");
        Self::new(disk_manager)
    }

    #[cfg(target_os = "linux")]
    fn new_with_iouring(disk_manager: Arc<DiskManager>, queue_depth: Option<usize>) -> Self {
        let mut config = IOSchedulerConfig::default();
        if let Some(q) = queue_depth {
            config.iouring_queue_depth = q;
        }
        let worker_count = config.workers;

        let (request_sender, request_receiver) = mpsc::channel::<DiskRequest>();

        let mut worker_senders = Vec::with_capacity(worker_count);
        let mut worker_threads = Vec::with_capacity(worker_count);
        for i in 0..worker_count {
            let (tx, rx) = mpsc::channel::<DiskRequest>();
            worker_senders.push(tx);
            let dm = disk_manager.clone();
            let handle = thread::Builder::new()
                .name(format!("disk-scheduler-iouring-worker-{}", i))
                .spawn(move || {
                    Self::io_uring_worker_loop(rx, dm);
                })
                .expect("Failed to spawn DiskScheduler io_uring worker thread");
            worker_threads.push(handle);
        }

        let dispatcher_thread = thread::Builder::new()
            .name("disk-scheduler-dispatcher".to_string())
            .spawn(move || {
                Self::dispatcher_loop(request_receiver, worker_senders);
            })
            .expect("Failed to spawn DiskScheduler dispatcher thread");

        DiskScheduler {
            request_sender,
            dispatcher_thread: Some(dispatcher_thread),
            worker_threads,
            config,
        }
    }

    // Dispatcher: forwards incoming requests to worker queues
    fn dispatcher_loop(receiver: Receiver<DiskRequest>, worker_senders: Vec<Sender<DiskRequest>>) {
        log::debug!("DiskScheduler dispatcher thread started.");
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
                    // Try to send to a live worker; attempt up to N times
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

    // The background worker loop that processes disk requests
    fn io_worker_loop(receiver: Receiver<DiskRequest>, disk_manager: Arc<DiskManager>) {
        log::debug!("Disk I/O worker thread started.");
        #[cfg(target_os = "linux")]
        let file = disk_manager.try_clone_db_file().ok();
        while let Ok(request) = receiver.recv() {
            // Loop until channel closes or Shutdown
            match request {
                DiskRequest::ReadPage {
                    page_id,
                    result_sender,
                } => {
                    #[cfg(target_os = "linux")]
                    let result = if let Some(f) = &file {
                        disk_manager.read_page_at_unlocked(f, page_id)
                    } else {
                        disk_manager.read_page(page_id)
                    };
                    #[cfg(not(target_os = "linux"))]
                    let result = disk_manager.read_page(page_id);
                    // Convert [u8; 4096] to BytesMut before sending
                    let bytes_result = result.map(|data| BytesMut::from(&data[..]));
                    if result_sender.send(bytes_result).is_err() {
                        log::error!(
                            "DiskScheduler failed to send ReadPage result for {}",
                            page_id
                        );
                    }
                }
                // ReadPagesContiguous merged into ReadPages
                DiskRequest::ReadPages {
                    page_ids,
                    result_sender,
                } => {
                    let mut pages = Vec::with_capacity(page_ids.len());
                    for pid in page_ids.into_iter() {
                        let result = {
                            #[cfg(target_os = "linux")]
                            {
                                if let Some(f) = &file {
                                    disk_manager.read_page_at_unlocked(f, pid)
                                } else {
                                    disk_manager.read_page(pid)
                                }
                            }
                            #[cfg(not(target_os = "linux"))]
                            {
                                disk_manager.read_page(pid)
                            }
                        };
                        match result.map(|data| BytesMut::from(&data[..])) {
                            Ok(b) => pages.push(b),
                            Err(e) => {
                                let _ = result_sender.send(Err(e));
                                pages.clear();
                                break;
                            }
                        }
                    }
                    if !pages.is_empty() {
                        let _ = result_sender.send(Ok(pages));
                    }
                }
                DiskRequest::WritePage {
                    page_id,
                    data,
                    result_sender,
                } => {
                    // Pass slice from Bytes
                    #[cfg(target_os = "linux")]
                    let result = if let Some(f) = &file {
                        disk_manager
                            .write_page_at_unlocked(f, page_id, &data)
                            .or_else(|_| {
                                // fallback to locked path on error
                                disk_manager.write_page(page_id, &data)
                            })
                    } else {
                        disk_manager.write_page(page_id, &data)
                    };
                    #[cfg(not(target_os = "linux"))]
                    let result = disk_manager.write_page(page_id, &data);
                    if result_sender.send(result).is_err() {
                        log::error!(
                            "DiskScheduler failed to send WritePage result for {}",
                            page_id
                        );
                    }
                }
                // WritePagesContiguous removed
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
                DiskRequest::Shutdown => {
                    log::debug!("Disk I/O worker thread received Shutdown signal.");
                    break; // Exit loop
                }
            }
        }
        log::debug!("Disk I/O worker thread finished.");
    }

    #[cfg(target_os = "linux")]
    fn io_uring_worker_loop(receiver: Receiver<DiskRequest>, disk_manager: Arc<DiskManager>) {
        use crate::buffer::PAGE_SIZE;
        use crate::storage::page::META_PAGE_SIZE;
        use bytes::BytesMut;
        use io_uring::{opcode, types, IoUring};
        use std::io;

        log::debug!("Disk I/O io_uring worker thread started.");
        let entries = self::IOSchedulerConfig::default().iouring_queue_depth as u32;
        let mut ring = IoUring::new(entries).expect("io_uring init failed");

        // Clone a dedicated File for this worker to get a stable fd
        let file = disk_manager
            .try_clone_db_file()
            .expect("clone db file for io_uring failed");
        let fd = file.as_raw_fd();

        while let Ok(request) = receiver.recv() {
            match request {
                DiskRequest::ReadPage {
                    page_id,
                    result_sender,
                } => {
                    let mut buf = vec![0u8; PAGE_SIZE];
                    let offset = (*META_PAGE_SIZE + (page_id - 1) as usize * PAGE_SIZE) as u64;
                    let read_e =
                        opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), PAGE_SIZE as u32)
                            .offset(offset)
                            .build();
                    unsafe { ring.submission().push(&read_e) }.expect("submit read sqe");
                    ring.submit_and_wait(1).expect("submit_and_wait read");
                    let cqe = ring.completion().next().expect("cqe read");
                    let res = cqe.result();
                    if res < 0 {
                        let err = io::Error::from_raw_os_error(-res);
                        let _ = result_sender.send(Err(QuillSQLError::Storage(format!(
                            "io_uring read failed: {}",
                            err
                        ))));
                        continue;
                    }
                    if res as usize != PAGE_SIZE {
                        let _ = result_sender.send(Err(QuillSQLError::Storage(format!(
                            "short read: {} bytes",
                            res
                        ))));
                        continue;
                    }
                    let bytes = BytesMut::from(&buf[..]);
                    let _ = result_sender.send(Ok(bytes));
                }
                // ReadPagesContiguous merged into ReadPages
                DiskRequest::ReadPages {
                    page_ids,
                    result_sender,
                } => {
                    use crate::buffer::PAGE_SIZE;
                    use crate::storage::page::META_PAGE_SIZE;
                    if page_ids.is_empty() {
                        let _ = result_sender.send(Ok(vec![]));
                        continue;
                    }
                    // Detect contiguity
                    let mut is_contig = true;
                    for w in page_ids.windows(2) {
                        if w[1] != w[0] + 1 {
                            is_contig = false;
                            break;
                        }
                    }
                    let mut submit_read = |pid: PageId, raws: &mut Vec<Vec<u8>>| {
                        let mut buf = vec![0u8; PAGE_SIZE];
                        let offset = (*META_PAGE_SIZE + (pid - 1) as usize * PAGE_SIZE) as u64;
                        let e =
                            opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), PAGE_SIZE as u32)
                                .offset(offset)
                                .build();
                        unsafe { ring.submission().push(&e) }.expect("submit read sqe");
                        raws.push(buf);
                    };

                    let mut raw_bufs: Vec<Vec<u8>> = Vec::with_capacity(page_ids.len());
                    if is_contig {
                        // Submit in order
                        for pid in page_ids.iter().copied() {
                            submit_read(pid, &mut raw_bufs);
                        }
                        ring.submit_and_wait(raw_bufs.len())
                            .expect("submit_and_wait reads");
                    } else {
                        for pid in page_ids.iter().copied() {
                            submit_read(pid, &mut raw_bufs);
                        }
                        ring.submit_and_wait(raw_bufs.len())
                            .expect("submit_and_wait reads");
                    }
                    let mut ok = true;
                    for _ in 0..raw_bufs.len() {
                        if let Some(cqe) = ring.completion().next() {
                            if cqe.result() < 0 {
                                ok = false;
                            }
                        } else {
                            ok = false;
                        }
                    }
                    if ok {
                        let mut out = Vec::with_capacity(raw_bufs.len());
                        for buf in raw_bufs.into_iter() {
                            out.push(BytesMut::from(&buf[..]));
                        }
                        let _ = result_sender.send(Ok(out));
                    } else {
                        let _ = result_sender.send(Err(QuillSQLError::Storage(
                            "iouring batch read failed".into(),
                        )));
                    }
                }
                DiskRequest::WritePage {
                    page_id,
                    data,
                    result_sender,
                } => {
                    let offset = (*META_PAGE_SIZE + (page_id - 1) as usize * PAGE_SIZE) as u64;
                    let ptr = data.as_ptr();
                    // submit write + fdatasync in a small batch; require two CQEs
                    let write_e = opcode::Write::new(types::Fd(fd), ptr, PAGE_SIZE as u32)
                        .offset(offset)
                        .build();
                    let fsync_e = opcode::Fsync::new(types::Fd(fd))
                        .flags(types::FsyncFlags::DATASYNC)
                        .build();
                    unsafe { ring.submission().push(&write_e) }.expect("submit write sqe");
                    unsafe { ring.submission().push(&fsync_e) }.expect("submit fsync sqe");
                    ring.submit_and_wait(2).expect("submit write+fdatasync");
                    // collect two CQEs
                    let mut ok = true;
                    for _ in 0..2 {
                        if let Some(cqe) = ring.completion().next() {
                            let res = cqe.result();
                            if res < 0 {
                                ok = false;
                            }
                        } else {
                            ok = false;
                        }
                    }
                    if !ok {
                        let _ = result_sender.send(Err(QuillSQLError::Storage(
                            "io_uring write+fdatasync failed".to_string(),
                        )));
                        continue;
                    }
                    let _ = result_sender.send(Ok(()));
                }
                // WritePagesContiguous removed
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
                    break;
                }
            }
        }
        log::debug!("Disk I/O io_uring worker thread finished.");
    }

    // --- Public methods to send requests ---

    pub fn schedule_read(
        &self,
        page_id: PageId,
    ) -> QuillSQLResult<DiskCommandResultReceiver<BytesMut>> {
        let (tx, rx) = mpsc::channel();
        self.request_sender
            .send(DiskRequest::ReadPage {
                page_id,
                result_sender: tx,
            })
            .map_err(|e| QuillSQLError::Internal(format!("Failed to send Read request: {}", e)))?;
        Ok(rx)
    }

    pub fn schedule_write(
        &self,
        page_id: PageId,
        data: Bytes,
    ) -> QuillSQLResult<DiskCommandResultReceiver<()>> {
        let (tx, rx) = mpsc::channel();
        self.request_sender
            .send(DiskRequest::WritePage {
                page_id,
                data,
                result_sender: tx,
            })
            .map_err(|e| QuillSQLError::Internal(format!("Failed to send Write request: {}", e)))?;
        Ok(rx)
    }

    pub fn schedule_read_pages(
        &self,
        page_ids: Vec<PageId>,
    ) -> QuillSQLResult<DiskCommandResultReceiver<Vec<BytesMut>>> {
        let (tx, rx) = mpsc::channel();
        self.request_sender
            .send(DiskRequest::ReadPages {
                page_ids,
                result_sender: tx,
            })
            .map_err(|e| {
                QuillSQLError::Internal(format!("Failed to send ReadPages request: {}", e))
            })?;
        Ok(rx)
    }

    // removed schedule_write_pages_contiguous

    pub fn schedule_allocate(&self) -> QuillSQLResult<Receiver<QuillSQLResult<PageId>>> {
        let (tx, rx) = mpsc::channel();
        self.request_sender
            .send(DiskRequest::AllocatePage { result_sender: tx })
            .map_err(|e| {
                QuillSQLError::Internal(format!("Failed to send Allocate request: {}", e))
            })?;
        Ok(rx)
    }

    pub fn schedule_deallocate(
        &self,
        page_id: PageId,
    ) -> QuillSQLResult<DiskCommandResultReceiver<()>> {
        let (tx, rx) = mpsc::channel();
        self.request_sender
            .send(DiskRequest::DeallocatePage {
                page_id,
                result_sender: tx,
            })
            .map_err(|e| {
                QuillSQLError::Internal(format!("Failed to send Deallocate request: {}", e))
            })?;
        Ok(rx)
    }
}

// Implement Drop for graceful shutdown
impl Drop for DiskScheduler {
    fn drop(&mut self) {
        // println!("DEBUG: DiskScheduler dropping. Sending Shutdown signal...");
        // Send shutdown signal. Ignore error if channel already closed.
        let _ = self.request_sender.send(DiskRequest::Shutdown);

        // Join dispatcher first
        if let Some(handle) = self.dispatcher_thread.take() {
            if let Err(e) = handle.join() {
                log::error!("Disk dispatcher thread panicked: {:?}", e);
            }
        }

        // Then join all workers
        for handle in self.worker_threads.drain(..) {
            if let Err(e) = handle.join() {
                log::error!("Disk worker thread panicked: {:?}", e);
            }
        }
    }
}

// --- Tests for DiskScheduler ---
#[cfg(test)]
mod tests {
    use super::DiskManager;
    use super::*;
    use crate::buffer::PAGE_SIZE;
    use crate::error::QuillSQLResult;
    use bytes::{Bytes, BytesMut};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    // Helper to create a scheduler with temp directory
    fn create_test_scheduler() -> (TempDir, Arc<DiskScheduler>, Arc<DiskManager>) {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let dm = Arc::new(DiskManager::try_new(temp_dir.path().join("test.db")).unwrap());
        let scheduler = Arc::new(DiskScheduler::new(dm.clone()));
        (temp_dir, scheduler, dm)
    }

    // Helper to create dummy page data as Bytes
    fn create_dummy_page_bytes(content: &str) -> Bytes {
        let mut data = BytesMut::zeroed(PAGE_SIZE);
        let content_bytes = content.as_bytes();
        let len = std::cmp::min(content_bytes.len(), PAGE_SIZE);
        data[..len].copy_from_slice(&content_bytes[..len]);
        data.freeze() // Convert to Bytes
    }

    // Helper to read content back from BytesMut
    fn read_page_content(data: &BytesMut) -> String {
        let first_null = data.iter().position(|&b| b == 0).unwrap_or(data.len());
        String::from_utf8_lossy(&data[..first_null]).to_string()
    }

    #[test]
    fn test_scheduler_allocate_write_read() -> QuillSQLResult<()> {
        let (_temp_dir, scheduler, _dm) = create_test_scheduler();

        // allocate pagge
        let rx_alloc = scheduler.schedule_allocate()?;
        let page_id = rx_alloc
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;

        // write page test
        let content = "Hello DiskScheduler!";
        let data_bytes = create_dummy_page_bytes(content);
        let rx_write = scheduler.schedule_write(page_id, data_bytes)?;
        rx_write
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;

        // read and verify data
        let rx_read = scheduler.schedule_read(page_id)?;
        let read_result = rx_read
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;
        assert_eq!(read_page_content(&read_result), content);

        Ok(())
    }

    #[test]
    fn test_scheduler_deallocate() -> QuillSQLResult<()> {
        let (_temp_dir, scheduler, dm) = create_test_scheduler();

        // allocate page and write data
        let page_id = scheduler
            .schedule_allocate()?
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;

        scheduler
            .schedule_write(page_id, create_dummy_page_bytes("Test Data"))?
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;

        // free page
        let rx_dealloc = scheduler.schedule_deallocate(page_id)?;
        rx_dealloc
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;

        // verify deallocation by attempting to read (should return zeroed data)
        let data_after_dealloc = dm.read_page(page_id)?;
        assert!(data_after_dealloc.iter().all(|&b| b == 0));

        Ok(())
    }

    #[test]
    fn test_concurrent_operations() -> QuillSQLResult<()> {
        let (_temp_dir, scheduler, _dm) = create_test_scheduler();

        // 创建测试页面
        let page_id = scheduler
            .schedule_allocate()?
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;

        scheduler
            .schedule_write(page_id, create_dummy_page_bytes("Concurrent Test"))?
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;

        // 启动多个并发读取线程
        let mut handles = vec![];
        let num_threads = 10; // 增加并发线程数

        for i in 0..num_threads {
            let scheduler_clone = scheduler.clone();
            let handle = thread::spawn(move || {
                // 每个线程有轻微延迟，增加并发可能性
                thread::sleep(Duration::from_millis(i * 5));

                scheduler_clone
                    .schedule_read(page_id)
                    .map_err(|e| e.to_string())
                    .and_then(|rx| rx.recv().map_err(|e| e.to_string()))
                    .and_then(|res| res.map_err(|e| e.to_string()))
            });
            handles.push(handle);
        }

        // 验证所有线程都能正确读取数据
        for handle in handles {
            match handle.join().unwrap() {
                Ok(read_data) => assert_eq!(read_page_content(&read_data), "Concurrent Test"),
                Err(e) => panic!("Concurrent read thread failed: {}", e),
            }
        }

        Ok(())
    }

    #[test]
    fn test_mixed_operations() -> QuillSQLResult<()> {
        let (_temp_dir, scheduler, _dm) = create_test_scheduler();

        // 分配多个页面
        let mut page_ids = vec![];
        let num_pages = 5;

        for _ in 0..num_pages {
            let page_id = scheduler
                .schedule_allocate()?
                .recv()
                .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;
            page_ids.push(page_id);
        }

        // 对每个页面执行读写测试
        for (i, &page_id) in page_ids.iter().enumerate() {
            let content = format!("Page {} content", i);

            // 写入
            scheduler
                .schedule_write(page_id, create_dummy_page_bytes(&content))?
                .recv()
                .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;

            // 读取并验证
            let read_data = scheduler
                .schedule_read(page_id)?
                .recv()
                .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;

            assert_eq!(read_page_content(&read_data), content);
        }

        // 释放一部分页面
        for &page_id in page_ids.iter().take(2) {
            scheduler
                .schedule_deallocate(page_id)?
                .recv()
                .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;
        }

        Ok(())
    }

    #[test]
    fn test_scheduler_shutdown() -> QuillSQLResult<()> {
        let (_temp_dir, scheduler, _dm) = create_test_scheduler();
        let scheduler_arc = scheduler;

        // 启动后台线程，在调度器关闭后尝试操作
        let scheduler_clone = scheduler_arc.clone();
        let handle = thread::spawn(move || {
            // 等待一段时间，以便主线程有时间关闭调度器
            thread::sleep(Duration::from_millis(100));

            // 尝试在调度器关闭后分配页面，应该会失败
            scheduler_clone
                .schedule_allocate()
                .map_err(|e| e.to_string())
                .and_then(|rx| rx.recv().map_err(|e| e.to_string()))
                .and_then(|res| res.map_err(|e| e.to_string()))
        });

        // 关闭调度器
        drop(scheduler_arc);

        // 检查后台线程结果
        match handle.join().unwrap() {
            Ok(page_id) => println!("Thread completed after shutdown: {:?}", page_id),
            Err(e) => println!("Thread failed as expected after shutdown: {}", e),
        }

        Ok(())
    }

    #[test]
    fn test_large_data_transfer() -> QuillSQLResult<()> {
        let (_temp_dir, scheduler, _dm) = create_test_scheduler();

        // 分配页面
        let page_id = scheduler
            .schedule_allocate()?
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;

        // 创建一个接近页面大小限制的大数据
        let large_string = "X".repeat(PAGE_SIZE - 100);
        let large_data = create_dummy_page_bytes(&large_string);

        // 写入大数据
        scheduler
            .schedule_write(page_id, large_data)?
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;

        // 读取并验证大数据
        let read_result = scheduler
            .schedule_read(page_id)?
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;

        // 验证数据长度，避免完整字符串比较
        let read_content = read_page_content(&read_result);
        assert_eq!(read_content.len(), large_string.len());
        assert_eq!(&read_content[0..10], &large_string[0..10]); // 检查前缀

        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_iouring_allocate_write_read() -> QuillSQLResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let dm = Arc::new(DiskManager::try_new(temp_dir.path().join("test.db")).unwrap());
        let scheduler = Arc::new(DiskScheduler::new_with_strategy(
            dm.clone(),
            IOStrategy::IoUring {
                queue_depth: Some(256),
            },
        ));

        // allocate
        let rx_alloc = scheduler.schedule_allocate()?;
        let page_id = rx_alloc
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;

        // write
        let mut data = BytesMut::zeroed(PAGE_SIZE);
        data[..4].copy_from_slice(&[1, 2, 3, 4]);
        scheduler
            .schedule_write(page_id, data.freeze())?
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;

        // read
        let read = scheduler
            .schedule_read(page_id)?
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;
        assert_eq!(&read[..4], &[1, 2, 3, 4]);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_iouring_concurrent_reads() -> QuillSQLResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let dm = Arc::new(DiskManager::try_new(temp_dir.path().join("test.db")).unwrap());
        let scheduler = Arc::new(DiskScheduler::new_with_strategy(
            dm.clone(),
            IOStrategy::IoUring {
                queue_depth: Some(256),
            },
        ));

        let page_id = scheduler
            .schedule_allocate()?
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;

        scheduler
            .schedule_write(page_id, {
                let mut b = BytesMut::zeroed(PAGE_SIZE);
                b[..13].copy_from_slice(b"Hello, World!");
                b.freeze()
            })?
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("RecvError: {}", e)))??;

        let mut handles = vec![];
        for _ in 0..8u32 {
            let s = scheduler.clone();
            handles.push(thread::spawn(move || {
                let data = s
                    .schedule_read(page_id)
                    .map_err(|e| e.to_string())
                    .and_then(|rx| rx.recv().map_err(|e| e.to_string()))
                    .and_then(|res| res.map_err(|e| e.to_string()))?;
                if &data[..13] != b"Hello, World!" {
                    return Err("mismatch".into());
                }
                Ok::<(), String>(())
            }));
        }
        for h in handles {
            h.join().unwrap().unwrap();
        }
        Ok(())
    }
}
