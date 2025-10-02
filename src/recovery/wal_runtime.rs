use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::error::{QuillSQLError, QuillSQLResult};

#[derive(Debug)]
enum WalCommand {
    Write {
        path: PathBuf,
        offset: u64,
        bytes: Bytes,
        sync: bool,
        responder: Sender<QuillSQLResult<()>>,
    },
    Read {
        path: PathBuf,
        offset: u64,
        len: usize,
        responder: Sender<QuillSQLResult<Vec<u8>>>,
    },
    Shutdown,
}

#[derive(Debug)]
pub struct WalRuntime {
    sender: Sender<WalCommand>,
    workers: usize,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl WalRuntime {
    pub fn new(workers: usize) -> Self {
        let worker_count = workers.max(1);
        let (tx, rx) = mpsc::channel::<WalCommand>();
        let shared_rx = Arc::new(Mutex::new(rx));
        let mut handles = Vec::with_capacity(worker_count);

        for idx in 0..worker_count {
            let rx = Arc::clone(&shared_rx);
            let handle = thread::Builder::new()
                .name(format!("wal-runtime-{}", idx))
                .spawn(move || worker_loop(rx))
                .expect("Failed to spawn WAL runtime thread");
            handles.push(handle);
        }

        WalRuntime {
            sender: tx,
            workers: worker_count,
            handles: Mutex::new(handles),
        }
    }

    pub fn default_worker_count() -> usize {
        std::thread::available_parallelism()
            .map(|n| (n.get() / 2).max(1))
            .unwrap_or(1)
    }

    pub fn write(&self, path: &Path, offset: u64, bytes: Bytes, sync: bool) -> QuillSQLResult<()> {
        let (tx, rx) = mpsc::channel();
        self.sender
            .send(WalCommand::Write {
                path: path.to_path_buf(),
                offset,
                bytes,
                sync,
                responder: tx,
            })
            .map_err(|e| QuillSQLError::Internal(format!("Failed to queue WAL write: {}", e)))?;
        rx.recv()
            .map_err(|e| QuillSQLError::Internal(format!("WAL write worker dropped: {}", e)))??;
        Ok(())
    }

    pub fn read(&self, path: &Path, offset: u64, len: usize) -> QuillSQLResult<Vec<u8>> {
        let (tx, rx) = mpsc::channel();
        self.sender
            .send(WalCommand::Read {
                path: path.to_path_buf(),
                offset,
                len,
                responder: tx,
            })
            .map_err(|e| QuillSQLError::Internal(format!("Failed to queue WAL read: {}", e)))?;
        let result = rx
            .recv()
            .map_err(|e| QuillSQLError::Internal(format!("WAL read worker dropped: {}", e)))??;
        Ok(result)
    }

    pub fn shutdown(&self) {
        for _ in 0..self.workers {
            let _ = self.sender.send(WalCommand::Shutdown);
        }

        let mut handles = self.handles.lock();
        while let Some(handle) = handles.pop() {
            if let Err(e) = handle.join() {
                log::error!("WAL runtime thread panicked: {:?}", e);
            }
        }
    }
}

impl Drop for WalRuntime {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn worker_loop(receiver: Arc<Mutex<Receiver<WalCommand>>>) {
    loop {
        let command = {
            let guard = receiver.lock();
            guard.recv()
        };

        match command {
            Ok(WalCommand::Write {
                path,
                offset,
                bytes,
                sync,
                responder,
            }) => {
                let result = wal_write(&path, offset, &bytes, sync);
                if responder.send(result).is_err() {
                    log::error!("WAL write responder dropped: {:?}", path);
                }
            }
            Ok(WalCommand::Read {
                path,
                offset,
                len,
                responder,
            }) => {
                let result = wal_read(&path, offset, len);
                if responder.send(result).is_err() {
                    log::error!("WAL read responder dropped: {:?}", path);
                }
            }
            Ok(WalCommand::Shutdown) | Err(_) => break,
        }
    }
}

fn wal_write(path: &Path, offset: u64, bytes: &[u8], sync: bool) -> QuillSQLResult<()> {
    use std::fs::OpenOptions;
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
        file.sync_data()?;
    }

    Ok(())
}

fn wal_read(path: &Path, offset: u64, len: usize) -> QuillSQLResult<Vec<u8>> {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom};

    let mut file = OpenOptions::new().read(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;

    let mut buf = vec![0u8; len];
    let mut read_total = 0usize;
    while read_total < len {
        let n = file.read(&mut buf[read_total..])?;
        if n == 0 {
            return Err(QuillSQLError::Storage(format!(
                "wal short read: expected {} bytes got {}",
                len, read_total
            )));
        }
        read_total += n;
    }
    Ok(buf)
}
