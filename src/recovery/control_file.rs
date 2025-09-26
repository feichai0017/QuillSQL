use std::fs;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use rand::random;
use serde::{Deserialize, Serialize};

use crate::error::QuillSQLResult;
use crate::recovery::Lsn;

const CONTROL_FILE_NAME: &str = "control.dat";
const CONTROL_FILE_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlFileData {
    version: u32,
    system_id: u128,
    wal_segment_size: u64,
    durable_lsn: Lsn,
    max_assigned_lsn: Lsn,
    last_checkpoint_lsn: Lsn,
    last_record_start: Lsn,
}

impl ControlFileData {
    fn new(system_id: u128, wal_segment_size: u64) -> Self {
        Self {
            version: CONTROL_FILE_VERSION,
            system_id,
            wal_segment_size,
            durable_lsn: 0,
            max_assigned_lsn: 0,
            last_checkpoint_lsn: 0,
            last_record_start: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ControlFileSnapshot {
    pub durable_lsn: Lsn,
    pub max_assigned_lsn: Lsn,
    pub last_checkpoint_lsn: Lsn,
    pub last_record_start: Lsn,
}

#[derive(Debug)]
pub struct ControlFileManager {
    path: PathBuf,
    inner: Mutex<ControlFileData>,
}

#[derive(Debug, Clone, Copy)]
pub struct WalInitState {
    pub durable_lsn: Lsn,
    pub max_assigned_lsn: Lsn,
    pub last_checkpoint_lsn: Lsn,
    pub last_record_start: Lsn,
}

impl WalInitState {
    pub fn next_lsn(&self) -> Lsn {
        self.max_assigned_lsn
    }
}

impl ControlFileManager {
    pub fn load_or_init(
        directory: &Path,
        wal_segment_size: u64,
    ) -> QuillSQLResult<(Self, WalInitState)> {
        fs::create_dir_all(directory)?;
        let path = directory.join(CONTROL_FILE_NAME);
        let mut needs_persist = false;
        let (data, newly_created) = if path.exists() {
            let bytes = fs::read(&path)?;
            let mut data: ControlFileData = bincode::deserialize(&bytes)?;
            if data.version != CONTROL_FILE_VERSION {
                return Err(crate::error::QuillSQLError::Internal(format!(
                    "Unsupported control file version: {}",
                    data.version
                )));
            }
            if data.wal_segment_size != wal_segment_size {
                data.wal_segment_size = wal_segment_size;
                needs_persist = true;
            }
            (data, false)
        } else {
            let system_id = generate_system_id();
            needs_persist = true;
            (ControlFileData::new(system_id, wal_segment_size), true)
        };

        let snapshot = WalInitState {
            durable_lsn: data.durable_lsn,
            max_assigned_lsn: data.max_assigned_lsn,
            last_checkpoint_lsn: data.last_checkpoint_lsn,
            last_record_start: data.last_record_start,
        };

        let manager = Self {
            path,
            inner: Mutex::new(data),
        };

        if newly_created || needs_persist {
            manager.persist()?;
        }

        Ok((manager, snapshot))
    }

    pub fn update(
        &self,
        durable_lsn: Lsn,
        max_assigned_lsn: Lsn,
        last_checkpoint_lsn: Lsn,
        last_record_start: Lsn,
    ) -> QuillSQLResult<()> {
        {
            let mut guard = self.inner.lock();
            guard.durable_lsn = durable_lsn;
            guard.max_assigned_lsn = max_assigned_lsn;
            guard.last_checkpoint_lsn = last_checkpoint_lsn;
            guard.last_record_start = last_record_start;
        }
        self.persist()
    }

    pub fn snapshot(&self) -> ControlFileSnapshot {
        let guard = self.inner.lock();
        ControlFileSnapshot {
            durable_lsn: guard.durable_lsn,
            max_assigned_lsn: guard.max_assigned_lsn,
            last_checkpoint_lsn: guard.last_checkpoint_lsn,
            last_record_start: guard.last_record_start,
        }
    }

    fn persist(&self) -> QuillSQLResult<()> {
        let guard = self.inner.lock();
        let bytes = bincode::serialize(&*guard)?;
        drop(guard);
        let tmp_path = self.path.with_extension("tmp");
        fs::write(&tmp_path, &bytes)?;
        fs::rename(tmp_path, &self.path)?;
        Ok(())
    }
}

fn generate_system_id() -> u128 {
    random::<u128>()
}
