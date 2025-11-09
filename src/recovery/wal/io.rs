use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::QuillSQLResult;
use crate::storage::disk_scheduler::{DiskCommandResultReceiver, DiskScheduler};

pub type WalIoTicket = DiskCommandResultReceiver<()>;

pub trait WalSink: Send + Sync {
    fn schedule_write(
        &self,
        path: PathBuf,
        offset: u64,
        data: Bytes,
        sync: bool,
    ) -> QuillSQLResult<Option<WalIoTicket>>;

    fn schedule_fsync(&self, path: PathBuf) -> QuillSQLResult<Option<WalIoTicket>>;
}

#[derive(Clone)]
pub struct DiskSchedulerWalSink {
    scheduler: Arc<DiskScheduler>,
}

impl DiskSchedulerWalSink {
    pub fn new(scheduler: Arc<DiskScheduler>) -> Self {
        Self { scheduler }
    }
}

impl WalSink for DiskSchedulerWalSink {
    fn schedule_write(
        &self,
        path: PathBuf,
        offset: u64,
        data: Bytes,
        sync: bool,
    ) -> QuillSQLResult<Option<WalIoTicket>> {
        if data.is_empty() && !sync {
            return Ok(None);
        }
        let receiver = self
            .scheduler
            .schedule_wal_write(path, offset, data, sync)?;
        Ok(Some(receiver))
    }

    fn schedule_fsync(&self, path: PathBuf) -> QuillSQLResult<Option<WalIoTicket>> {
        let receiver = self.scheduler.schedule_wal_fsync(path)?;
        Ok(Some(receiver))
    }
}
