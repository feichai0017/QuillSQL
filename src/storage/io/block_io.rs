#![cfg(not(target_os = "linux"))]

use crate::config::IOSchedulerConfig;
use crate::error::QuillSQLResult;
use crate::storage::disk_manager::DiskManager;
use crate::storage::disk_scheduler::{DiskRequest, RequestQueue, RequestReceiver, RequestSender};
use bytes::{Bytes, BytesMut};
use std::fs::{create_dir_all, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

pub fn spawn_runtime(
    disk_manager: Arc<DiskManager>,
    config: IOSchedulerConfig,
) -> (RequestSender, Vec<thread::JoinHandle<()>>) {
    let queue = Arc::new(RequestQueue::new());
    let sender = RequestSender::new(queue.clone());

    let mut worker_threads = Vec::with_capacity(config.workers);
    for i in 0..config.workers {
        let dm = disk_manager.clone();
        let rx = RequestReceiver::new(queue.clone());
        let handle = thread::Builder::new()
            .name(format!("disk-scheduler-blocking-worker-{}", i))
            .spawn(move || worker_loop(rx, dm))
            .expect("Failed to spawn blocking disk scheduler worker");
        worker_threads.push(handle);
    }

    (sender, worker_threads)
}

fn worker_loop(receiver: RequestReceiver, disk_manager: Arc<DiskManager>) {
    while let Some(request) = receiver.recv() {
        match request {
            DiskRequest::ReadPage {
                page_id,
                result_sender,
            } => {
                let result = disk_manager
                    .read_page(page_id)
                    .map(|page| BytesMut::from(&page[..]));
                let _ = result_sender.send(result);
            }
            DiskRequest::ReadPages {
                page_ids,
                result_sender,
            } => {
                let mut buffers = Vec::with_capacity(page_ids.len());
                let mut error = None;
                for pid in page_ids {
                    match disk_manager.read_page(pid) {
                        Ok(page) => buffers.push(BytesMut::from(&page[..])),
                        Err(err) => {
                            error = Some(err);
                            break;
                        }
                    }
                }
                let result = match error {
                    Some(err) => Err(err),
                    None => Ok(buffers),
                };
                let _ = result_sender.send(result);
            }
            DiskRequest::WritePage {
                page_id,
                data,
                result_sender,
            } => {
                let result = disk_manager.write_page(page_id, data.as_ref());
                let _ = result_sender.send(result);
            }
            DiskRequest::WriteWal {
                path,
                offset,
                data,
                sync,
                result_sender,
            } => {
                let result = write_wal_blocking(&path, offset, &data, sync);
                let _ = result_sender.send(result);
            }
            DiskRequest::FsyncWal {
                path,
                result_sender,
            } => {
                let result = fsync_wal_blocking(&path);
                let _ = result_sender.send(result);
            }
            DiskRequest::AllocatePage { result_sender } => {
                let result = disk_manager.allocate_page();
                let _ = result_sender.send(result);
            }
            DiskRequest::DeallocatePage {
                page_id,
                result_sender,
            } => {
                let result = disk_manager.deallocate_page(page_id);
                let _ = result_sender.send(result);
            }
            DiskRequest::Shutdown => break,
        }
    }
}

fn write_wal_blocking(path: &PathBuf, offset: u64, data: &Bytes, sync: bool) -> QuillSQLResult<()> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            create_dir_all(parent)?;
        }
    }
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(data.as_ref())?;
    if sync {
        file.sync_data()?;
    }
    Ok(())
}

fn fsync_wal_blocking(path: &PathBuf) -> QuillSQLResult<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;
    file.sync_all()?;
    Ok(())
}
