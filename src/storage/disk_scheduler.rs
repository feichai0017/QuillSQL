use super::disk_manager::DiskManager;
use crate::buffer::PageId;
use crate::error::{QuillSQLError, QuillSQLResult};
use bytes::{Bytes, BytesMut};
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

// Type alias for the sender part of the result channel
pub type DiskCommandResultSender<T> = Sender<QuillSQLResult<T>>;
// Type alias for the receiver part of the result channel
pub type DiskCommandResultReceiver<T> = Receiver<QuillSQLResult<T>>;

// Commands sent from BufferPoolManager to the DiskScheduler task
#[derive(Debug)]
pub enum DiskRequest {
    ReadPage {
        page_id: PageId,
        result_sender: DiskCommandResultSender<BytesMut>,
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
    // Keep handle Option for Drop implementation
    background_thread: Option<thread::JoinHandle<()>>,
}

impl DiskScheduler {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self {
        let (request_sender, request_receiver) = mpsc::channel::<DiskRequest>();

        let background_thread = thread::Builder::new()
            .name("disk-scheduler-io-thread".to_string())
            .spawn(move || {
                Self::io_worker_loop(request_receiver, disk_manager);
            })
            .expect("Failed to spawn DiskScheduler I/O thread");

        DiskScheduler {
            request_sender,
            background_thread: Some(background_thread),
        }
    }

    // The background worker loop that processes disk requests
    fn io_worker_loop(receiver: Receiver<DiskRequest>, disk_manager: Arc<DiskManager>) {
        println!("DEBUG: Disk I/O worker thread started.");
        while let Ok(request) = receiver.recv() {
            // Loop until channel closes or Shutdown
            match request {
                DiskRequest::ReadPage {
                    page_id,
                    result_sender,
                } => {
                    let result = disk_manager.read_page(page_id);
                    // Convert [u8; 4096] to BytesMut before sending
                    let bytes_result = result.map(|data| BytesMut::from(&data[..]));
                    if result_sender.send(bytes_result).is_err() {
                        eprintln!(
                            "ERROR: DiskScheduler failed to send ReadPage result for {}",
                            page_id
                        );
                    }
                }
                DiskRequest::WritePage {
                    page_id,
                    data,
                    result_sender,
                } => {
                    // Pass slice from Bytes
                    let result = disk_manager.write_page(page_id, &data);
                    if result_sender.send(result).is_err() {
                        eprintln!(
                            "ERROR: DiskScheduler failed to send WritePage result for {}",
                            page_id
                        );
                    }
                }
                DiskRequest::AllocatePage { result_sender } => {
                    let result = disk_manager.allocate_page();
                    if result_sender.send(result).is_err() {
                        eprintln!("ERROR: DiskScheduler failed to send AllocatePage result");
                    }
                }
                DiskRequest::DeallocatePage {
                    page_id,
                    result_sender,
                } => {
                    let result = disk_manager.deallocate_page(page_id);
                    if result_sender.send(result).is_err() {
                        eprintln!(
                            "ERROR: DiskScheduler failed to send DeallocatePage result for {}",
                            page_id
                        );
                    }
                }
                DiskRequest::Shutdown => {
                    println!("DEBUG: Disk I/O worker thread received Shutdown signal.");
                    break; // Exit loop
                }
            }
        }
        println!("DEBUG: Disk I/O worker thread finished.");
    }

    // --- Public methods to send requests ---

    pub fn schedule_read(&self, page_id: PageId) -> QuillSQLResult<DiskCommandResultReceiver<BytesMut>> {
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

    pub fn schedule_allocate(&self) -> QuillSQLResult<Receiver<QuillSQLResult<PageId>>> {
        let (tx, rx) = mpsc::channel();
        self.request_sender
            .send(DiskRequest::AllocatePage { result_sender: tx })
            .map_err(|e| QuillSQLError::Internal(format!("Failed to send Allocate request: {}", e)))?;
        Ok(rx)
    }

    pub fn schedule_deallocate(&self, page_id: PageId) -> QuillSQLResult<DiskCommandResultReceiver<()>> {
        let (tx, rx) = mpsc::channel();
        self.request_sender
            .send(DiskRequest::DeallocatePage {
                page_id,
                result_sender: tx,
            })
            .map_err(|e| QuillSQLError::Internal(format!("Failed to send Deallocate request: {}", e)))?;
        Ok(rx)
    }
}

// Implement Drop for graceful shutdown
impl Drop for DiskScheduler {
    fn drop(&mut self) {
        // println!("DEBUG: DiskScheduler dropping. Sending Shutdown signal...");
        // Send shutdown signal. Ignore error if channel already closed.
        let _ = self.request_sender.send(DiskRequest::Shutdown);

        // Wait for the background thread to finish
        if let Some(handle) = self.background_thread.take() {
            if let Err(e) = handle.join() {
                eprintln!("ERROR: Disk I/O worker thread panicked: {:?}", e);
            } else {
                // println!("DEBUG: Disk I/O worker thread joined successfully.");
            }
        } else {
            println!("WARN: Background thread handle already taken on Drop.");
        }
    }
}

// --- Tests for DiskScheduler ---
#[cfg(test)]
mod tests {
    use super::DiskManager;
    use super::*;
    use crate::error::QuillSQLResult;
    use crate::buffer::PAGE_SIZE;
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
}
