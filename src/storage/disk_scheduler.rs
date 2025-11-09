use super::disk_manager::DiskManager;
use crate::buffer::PageId;
use crate::config::IOSchedulerConfig;
use crate::error::{QuillSQLError, QuillSQLResult};
use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;

#[cfg(not(target_os = "linux"))]
use crate::storage::io::blocking;
#[cfg(target_os = "linux")]
use crate::storage::io::io_uring;

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
// Type alias for the sender part of the result channel
pub type DiskCommandResultSender<T> = mpsc::Sender<QuillSQLResult<T>>;
// Type alias for the receiver part of the result channel
pub type DiskCommandResultReceiver<T> = mpsc::Receiver<QuillSQLResult<T>>;

// Commands sent from BufferManager to the DiskScheduler task
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
    WriteWal {
        path: PathBuf,
        offset: u64,
        data: Bytes,
        sync: bool,
        result_sender: DiskCommandResultSender<()>,
    },
    FsyncWal {
        path: PathBuf,
        result_sender: DiskCommandResultSender<()>,
    },
    AllocatePage {
        result_sender: mpsc::Sender<QuillSQLResult<PageId>>,
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
    request_sender: RequestSender,
    worker_threads: Vec<thread::JoinHandle<()>>,
    pub config: IOSchedulerConfig,
}

pub(crate) struct RequestQueue {
    queue: Mutex<VecDeque<DiskRequest>>,
    condvar: Condvar,
    shutdown: AtomicBool,
}

impl RequestQueue {
    pub(crate) fn new() -> Self {
        RequestQueue {
            queue: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
            shutdown: AtomicBool::new(false),
        }
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    fn mark_shutdown(&self) {
        if !self.shutdown.swap(true, Ordering::AcqRel) {
            self.condvar.notify_all();
        }
    }
}

#[derive(Clone)]
pub(crate) struct RequestSender {
    queue: Arc<RequestQueue>,
}

impl fmt::Debug for RequestSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestSender").finish()
    }
}

impl RequestSender {
    pub(crate) fn new(queue: Arc<RequestQueue>) -> Self {
        RequestSender { queue }
    }

    fn send(&self, request: DiskRequest) -> Result<(), DiskRequest> {
        if self.queue.is_shutdown() {
            return Err(request);
        }

        let mut guard = self.queue.queue.lock().unwrap();
        if self.queue.is_shutdown() {
            return Err(request);
        }

        guard.push_back(request);
        self.queue.condvar.notify_one();
        Ok(())
    }

    fn close(&self) {
        self.queue.mark_shutdown();
    }
}

#[derive(Clone)]
pub(crate) struct RequestReceiver {
    queue: Arc<RequestQueue>,
}

impl RequestReceiver {
    pub(crate) fn new(queue: Arc<RequestQueue>) -> Self {
        RequestReceiver { queue }
    }

    pub(crate) fn try_recv(&self) -> Option<DiskRequest> {
        let mut guard = self.queue.queue.lock().unwrap();
        guard.pop_front()
    }

    pub(crate) fn recv(&self) -> Option<DiskRequest> {
        let mut guard = self.queue.queue.lock().unwrap();
        loop {
            if let Some(request) = guard.pop_front() {
                return Some(request);
            }

            if self.queue.is_shutdown() {
                return None;
            }

            guard = self.queue.condvar.wait(guard).unwrap();
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.queue.is_shutdown()
    }
}

impl DiskScheduler {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self {
        Self::new_with_config(disk_manager, IOSchedulerConfig::default())
    }

    pub fn new_with_config(disk_manager: Arc<DiskManager>, config: IOSchedulerConfig) -> Self {
        #[cfg(target_os = "linux")]
        let (request_sender, worker_threads) = spawn_runtime(disk_manager.clone(), config.clone());

        #[cfg(not(target_os = "linux"))]
        let (request_sender, worker_threads) =
            blocking::spawn_runtime(disk_manager.clone(), config.clone());

        DiskScheduler {
            request_sender,
            worker_threads,
            config,
        }
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
            .map_err(|_| {
                QuillSQLError::Internal(
                    "Failed to enqueue Read request: scheduler shutting down".to_string(),
                )
            })?;
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
            .map_err(|_| {
                QuillSQLError::Internal(
                    "Failed to enqueue Write request: scheduler shutting down".to_string(),
                )
            })?;
        Ok(rx)
    }

    pub fn schedule_wal_write(
        &self,
        path: PathBuf,
        offset: u64,
        data: Bytes,
        sync: bool,
    ) -> QuillSQLResult<DiskCommandResultReceiver<()>> {
        let (tx, rx) = mpsc::channel();
        self.request_sender
            .send(DiskRequest::WriteWal {
                path,
                offset,
                data,
                sync,
                result_sender: tx,
            })
            .map_err(|_| {
                QuillSQLError::Internal(
                    "Failed to enqueue WAL write request: scheduler shutting down".to_string(),
                )
            })?;
        Ok(rx)
    }

    pub fn schedule_wal_fsync(
        &self,
        path: PathBuf,
    ) -> QuillSQLResult<DiskCommandResultReceiver<()>> {
        let (tx, rx) = mpsc::channel();
        self.request_sender
            .send(DiskRequest::FsyncWal {
                path,
                result_sender: tx,
            })
            .map_err(|_| {
                QuillSQLError::Internal(
                    "Failed to enqueue WAL fsync request: scheduler shutting down".to_string(),
                )
            })?;
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
            .map_err(|_| {
                QuillSQLError::Internal(
                    "Failed to enqueue ReadPages request: scheduler shutting down".to_string(),
                )
            })?;
        Ok(rx)
    }

    // removed schedule_write_pages_contiguous

    pub fn schedule_allocate(&self) -> QuillSQLResult<mpsc::Receiver<QuillSQLResult<PageId>>> {
        let (tx, rx) = mpsc::channel();
        self.request_sender
            .send(DiskRequest::AllocatePage { result_sender: tx })
            .map_err(|_| {
                QuillSQLError::Internal(
                    "Failed to enqueue Allocate request: scheduler shutting down".to_string(),
                )
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
            .map_err(|_| {
                QuillSQLError::Internal(
                    "Failed to enqueue Deallocate request: scheduler shutting down".to_string(),
                )
            })?;
        Ok(rx)
    }
}

// Implement Drop for graceful shutdown
impl Drop for DiskScheduler {
    fn drop(&mut self) {
        for _ in 0..self.config.workers {
            let _ = self.request_sender.send(DiskRequest::Shutdown);
        }
        self.request_sender.close();
        for handle in self.worker_threads.drain(..) {
            if let Err(e) = handle.join() {
                log::error!("Disk worker thread panicked: {:?}", e);
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn spawn_runtime(
    disk_manager: Arc<DiskManager>,
    config: IOSchedulerConfig,
) -> (RequestSender, Vec<thread::JoinHandle<()>>) {
    let worker_count = config.workers;
    let queue = Arc::new(RequestQueue::new());
    let sender = RequestSender::new(queue.clone());

    let mut worker_threads = Vec::with_capacity(worker_count);
    for i in 0..worker_count {
        let dm = disk_manager.clone();
        let worker_config = config;
        let entries = worker_config.iouring_queue_depth as u32;
        let fixed_count = worker_config.iouring_fixed_buffers;
        let sqpoll_idle = worker_config.iouring_sqpoll_idle_ms;
        let fsync_on_write = worker_config.fsync_on_write;
        let rx = RequestReceiver::new(queue.clone());
        let handle = thread::Builder::new()
            .name(format!("disk-scheduler-iouring-worker-{}", i))
            .spawn(move || {
                io_uring::worker_loop(rx, dm, entries, fixed_count, sqpoll_idle, fsync_on_write);
            })
            .expect("Failed to spawn DiskScheduler io_uring worker thread");
        worker_threads.push(handle);
    }

    (sender, worker_threads)
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
        let mut cfg = IOSchedulerConfig::default();
        cfg.iouring_queue_depth = 256;
        let scheduler = Arc::new(DiskScheduler::new_with_config(dm.clone(), cfg));

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
        let mut cfg = IOSchedulerConfig::default();
        cfg.iouring_queue_depth = 256;
        let scheduler = Arc::new(DiskScheduler::new_with_config(dm.clone(), cfg));

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
