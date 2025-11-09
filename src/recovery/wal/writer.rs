use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
use std::time::Duration;

use crate::error::{QuillSQLError, QuillSQLResult};

use super::WalManager;

#[derive(Debug)]
pub(super) struct WalWriterRuntime {
    stop_flag: Arc<AtomicBool>,
    thread: thread::JoinHandle<()>,
}

impl WalWriterRuntime {
    pub(super) fn spawn(target: Weak<WalManager>, interval: Duration) -> QuillSQLResult<Self> {
        let stop_flag = Arc::new(AtomicBool::new(false));
        let thread_stop = stop_flag.clone();
        let handle = thread::Builder::new()
            .name("walwriter".into())
            .spawn(move || {
                while !thread_stop.load(Ordering::Relaxed) {
                    if let Some(manager) = target.upgrade() {
                        let _ = manager.flush(None);
                    } else {
                        break;
                    }
                    thread::sleep(interval);
                }
                if let Some(manager) = target.upgrade() {
                    let _ = manager.flush(None);
                }
            })
            .map_err(|e| QuillSQLError::Internal(format!("Failed to spawn walwriter: {}", e)))?;
        Ok(Self {
            stop_flag,
            thread: handle,
        })
    }

    pub(super) fn stop(self) -> QuillSQLResult<()> {
        self.stop_flag.store(true, Ordering::Release);
        self.thread
            .join()
            .map_err(|_| QuillSQLError::Internal("walwriter thread panicked".to_string()))
    }
}
