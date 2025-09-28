use std::path::PathBuf;

#[derive(Debug, Clone, Copy)]
pub enum IOStrategy {
    ThreadPool { workers: Option<usize> },
    IoUring { queue_depth: Option<usize> },
}

#[derive(Debug, Clone, Copy)]
pub struct IOSchedulerConfig {
    /// Number of worker threads (for both ThreadPool and IoUring workers)
    pub workers: usize,
    /// IoUring queue depth (Linux only). Ignored on non-Linux.
    #[cfg(target_os = "linux")]
    pub iouring_queue_depth: usize,
    /// Whether the IO backend should force an fsync/fdatasync after writes.
    pub fsync_on_write: bool,
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
            fsync_on_write: true,
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

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            directory: PathBuf::from("wal"),
            segment_size: 16 * 1024 * 1024, // 16 MiB segments by default
            sync_on_flush: true,
            writer_interval_ms: Some(50),
            buffer_capacity: 256,
            flush_coalesce_bytes: 2 * 1024 * 1024,
            synchronous_commit: true,
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
