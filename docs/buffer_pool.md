# Buffer Pool Manager — Architecture, Features, Optimizations

## 1. Architecture Overview
- Data Structures:
  - pool: Vec<Arc<RwLock<Page>>> (per-frame rwlock)
  - page_table: DashMap<PageId, FrameId>
  - replacer: Sharded LRUK (reduces lock contention)
  - free_list (+ optional sharded distrib by frame id)
  - inflight_loads: DashMap<PageId, Arc<Mutex<()>>> to serialize same-page loads
- Guards & Pin Protocol:
  - fetch_page_read/write: validate mapping under read lock, pin (pin_count++), mark non-evictable; on guard drop, unpin and if pin_count==0 mark evictable via complete_unpin.
- I/O Path:
  - allocate_frame: try free_list; else replacer.evict(); if victim dirty, flush_page before reuse; update page_table mapping.
  - flush_page: copy data under read lock, schedule write, clear dirty.
  - delete_page: fail if pinned; else destroy page memory, remove from replacer, return frame to free_list.

## 2. Concurrency & Safety
- Per-frame RwLock isolates page data; DashMap for page_table allows concurrent lookups.
- inflight_loads guards against thundering herd on misses (double-check after acquiring guard).
- complete_unpin runs without holding page locks to avoid lock order inversion.
- Eviction checks pin_count and dirty state; never evict pinned pages.

## 3. Optimizations
- Sharded LRUK replacer: lower lock contention on hot paths (record_access, evict, set_evictable).
- TinyLFU admission filter (optional): approximate frequency to bias against admitting cold scan pages; periodic aging to avoid saturation.
- Background cleaner (optional via env QUILL_BPM_CLEANER=1):
  - Periodically measures dirty ratio and batch flushes pages; ages TinyLFU.
- Prefetch API: prefetch_page best-effort warm-up used by B+Tree iterator.

## 4. Tuning & Env
- QUILL_BPM_CLEANER=1 to enable cleaner; QUILL_BPM_SLEEP_MS (default 50), QUILL_BPM_DIRTY_PCT (default 0.2), QUILL_BPM_BATCH (default 32).
- Admission aggressiveness: adjustable by code (e.g., deny only when no free frame and LFU estimate==0). For benchmarking hot reads, switch to count-only.
- Buffer pool size matters: under-provisioning increases flush/evict activity and tail latency.

## 5. Failure Modes & Handling
- Mapping drift under concurrency: fetch validates page_id in frame before pin; if mismatch, spins/retries.
- delete vs fetch race: delete_page reinserts mapping if pinned/busy; tests verify safety.
- Eviction of dirty pages always flushes before reuse.

## 6. Future Work
- True sharded free lists (round-robin pop/push) for even lower contention.
- Group flush / write coalescing and rate limiting.
- NUMA-aware partitioning and per-shard statistics and telemetry.
- Adaptive admission (Window-TinyLFU) and background tuning.
