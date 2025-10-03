use std::thread::JoinHandle;
use std::time::Duration;

/// High-level categories of background workers maintained by the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerKind {
    WalWriter,
    Checkpoint,
    BufferPoolWriter,
}

#[derive(Debug, Clone, Copy)]
pub struct WorkerMetadata {
    pub kind: WorkerKind,
    pub interval: Option<Duration>,
}

pub struct WorkerHandle {
    metadata: WorkerMetadata,
    stop_fn: Option<Box<dyn FnOnce() + Send + 'static>>,
    join_handle: Option<JoinHandle<()>>,
}

impl WorkerHandle {
    pub fn new(
        metadata: WorkerMetadata,
        stop_fn: impl FnOnce() + Send + 'static,
        join_handle: Option<JoinHandle<()>>,
    ) -> Self {
        Self {
            metadata,
            stop_fn: Some(Box::new(stop_fn)),
            join_handle,
        }
    }

    pub fn metadata(&self) -> WorkerMetadata {
        self.metadata
    }

    pub fn shutdown(&mut self) {
        if let Some(stop) = self.stop_fn.take() {
            stop();
        }
    }

    pub fn join(&mut self) {
        if let Some(handle) = self.join_handle.take() {
            if let Err(err) = handle.join() {
                log::warn!(
                    "Background worker {:?} terminated with panic: {:?}",
                    self.metadata.kind,
                    err
                );
            }
        }
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        self.shutdown();
        self.join();
    }
}

pub struct BackgroundWorkers {
    workers: Vec<WorkerHandle>,
}

impl BackgroundWorkers {
    pub fn new() -> Self {
        Self {
            workers: Vec::new(),
        }
    }

    pub fn register(&mut self, handle: WorkerHandle) {
        self.workers.push(handle);
    }

    pub fn register_opt(&mut self, handle: Option<WorkerHandle>) {
        if let Some(handle) = handle {
            self.register(handle);
        }
    }

    pub fn shutdown_all(&mut self) {
        for worker in &mut self.workers {
            worker.shutdown();
        }
        for worker in &mut self.workers {
            worker.join();
        }
    }

    pub fn workers(&self) -> &[WorkerHandle] {
        &self.workers
    }

    pub fn snapshot(&self) -> Vec<WorkerMetadata> {
        self.workers
            .iter()
            .map(|worker| worker.metadata())
            .collect()
    }
}

impl Drop for BackgroundWorkers {
    fn drop(&mut self) {
        self.shutdown_all();
    }
}
