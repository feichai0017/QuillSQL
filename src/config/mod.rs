use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct IOSchedulerConfig {
    /// Number of io_uring worker threads (or fallback thread pool workers)
    pub workers: usize,
    /// IoUring queue depth (Linux only). Ignored on non-Linux.
    #[cfg(target_os = "linux")]
    pub iouring_queue_depth: usize,
    /// Number of registered fixed buffers for io_uring (Linux only).
    #[cfg(target_os = "linux")]
    pub iouring_fixed_buffers: usize,
    /// Optional SQPOLL idle time in milliseconds (Linux only).
    #[cfg(target_os = "linux")]
    pub iouring_sqpoll_idle_ms: Option<u32>,
    /// Whether the IO backend should force an fsync/fdatasync after writes.
    pub fsync_on_write: bool,
    /// WAL handler worker threads (buffered I/O)
    pub wal_workers: usize,
}

impl IOSchedulerConfig {
    pub fn default_workers() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    }
}

impl Default for IOSchedulerConfig {
    fn default() -> Self {
        IOSchedulerConfig {
            workers: Self::default_workers(),
            #[cfg(target_os = "linux")]
            iouring_queue_depth: 256,
            #[cfg(target_os = "linux")]
            iouring_fixed_buffers: 256,
            #[cfg(target_os = "linux")]
            iouring_sqpoll_idle_ms: None,
            fsync_on_write: true,
            wal_workers: std::cmp::max(1, Self::default_workers() / 2),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BufferPoolConfig {
    pub buffer_pool_size: usize,
    pub lru_k_k: usize,
    pub tiny_lfu_enable: bool,
    pub tiny_lfu_counters: usize,
    pub admission_enable: bool,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        BufferPoolConfig {
            buffer_pool_size: 5000,
            lru_k_k: 2,
            tiny_lfu_enable: true,
            tiny_lfu_counters: 4,
            admission_enable: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalConfig {
    pub directory: PathBuf,
    pub segment_size: u64,
    pub sync_on_flush: bool,
    pub writer_interval_ms: Option<u64>,
    pub buffer_capacity: usize,
    pub flush_coalesce_bytes: usize,
    pub synchronous_commit: bool,
    pub checkpoint_interval_ms: Option<u64>,
    pub retain_segments: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct BackgroundConfig {
    pub wal_writer_interval: Option<Duration>,
    pub checkpoint_interval: Option<Duration>,
    pub bg_writer_interval: Option<Duration>,
    pub vacuum: IndexVacuumConfig,
    pub mvcc_vacuum: MvccVacuumConfig,
}

pub fn background_config(
    wal_config: &WalConfig,
    vacuum_cfg: IndexVacuumConfig,
    mvcc_cfg: MvccVacuumConfig,
) -> BackgroundConfig {
    let wal_writer_interval = wal_config.writer_interval_ms.and_then(duration_from_ms);
    let checkpoint_interval = wal_config.checkpoint_interval_ms.and_then(duration_from_ms);

    let env_interval = std::env::var("QUILL_BG_WRITER_INTERVAL_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok());
    let bg_interval_ms = env_interval.unwrap_or(vacuum_cfg.interval_ms);
    let bg_writer_interval = duration_from_ms(bg_interval_ms);

    let mvcc_interval_ms = std::env::var("QUILL_MVCC_VACUUM_INTERVAL_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(mvcc_cfg.interval_ms);
    let mvcc_batch = std::env::var("QUILL_MVCC_VACUUM_BATCH")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(mvcc_cfg.batch_limit)
        .max(1);

    BackgroundConfig {
        wal_writer_interval,
        checkpoint_interval,
        bg_writer_interval,
        vacuum: vacuum_cfg,
        mvcc_vacuum: MvccVacuumConfig {
            interval_ms: mvcc_interval_ms,
            batch_limit: mvcc_batch,
        },
    }
}

fn duration_from_ms(ms: u64) -> Option<Duration> {
    if ms == 0 {
        None
    } else {
        Some(Duration::from_millis(ms))
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            directory: PathBuf::from("wal"),
            segment_size: 16 * 1024 * 1024, // 16 MiB segments by default
            sync_on_flush: true,
            writer_interval_ms: Some(50),
            buffer_capacity: 256,
            flush_coalesce_bytes: 2 * 1024 * 1024,
            synchronous_commit: false,
            checkpoint_interval_ms: Some(5000),
            retain_segments: 8,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BTreeConfig {
    pub seq_batch_enable: bool,
    pub seq_window: usize,
    pub prefetch_enable: bool,
    pub prefetch_window: usize,
    pub debug_find_level: u8,
    pub debug_insert_level: u8,
    pub debug_split_level: u8,
}

impl Default for BTreeConfig {
    fn default() -> Self {
        BTreeConfig {
            seq_batch_enable: true,
            seq_window: 32,
            prefetch_enable: true,
            prefetch_window: 2,
            debug_find_level: 0,
            debug_insert_level: 0,
            debug_split_level: 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TableScanConfig {
    pub stream_scan_enable: bool,
    pub stream_threshold_pages: Option<u32>,
    pub readahead_pages: usize,
}

impl Default for TableScanConfig {
    fn default() -> Self {
        TableScanConfig {
            stream_scan_enable: false,
            stream_threshold_pages: None, // None => use pool_size/4
            readahead_pages: 2,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct IndexVacuumConfig {
    /// Background vacuum interval in milliseconds
    pub interval_ms: u64,
    /// Pending garbage counter threshold to trigger a cleanup batch
    pub trigger_threshold: usize,
    /// Max number of entries to cleanup in one batch
    pub batch_limit: usize,
}

impl Default for IndexVacuumConfig {
    fn default() -> Self {
        Self {
            interval_ms: 10_000,     // 10s
            trigger_threshold: 4096, // pending count to trigger
            batch_limit: 128,        // small batch to avoid stalls
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MvccVacuumConfig {
    /// Background MVCC vacuum interval in milliseconds
    pub interval_ms: u64,
    /// Max tuples to reclaim per iteration
    pub batch_limit: usize,
}

impl Default for MvccVacuumConfig {
    fn default() -> Self {
        Self {
            interval_ms: 15_000, // 15s
            batch_limit: 512,
        }
    }
}
