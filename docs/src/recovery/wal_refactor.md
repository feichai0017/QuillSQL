# WAL Refactor Plan

This document captures the incremental plan for modernising QuillSQL’s write-ahead log so it behaves more like the logical+physiological WALs used in PostgreSQL and DuckDB. The goal is to keep the system teaching-friendly while unlocking better performance, portability, and clarity.

## Current State (2024-12)

- **Redo relies on page images.** `WritePageGuard::apply_page_image` emits `PageWrite` or `PageDelta` records, and the page `ResourceManager` replays them. Heap-specific WAL payloads only drive UNDO; `HeapResourceManager::redo` is a no-op.
- **Index changes are implicit.** B+Tree operations mutate pages directly and depend on page-level logging. There is no index-specific WAL, so crash recovery assumes page FPWs cover all structure changes.
- **WAL buffer is a Vec.** `WalBuffer` pushes encoded frames into a growable `Vec` protected by a mutex. There is no ring-buffer behaviour, no lock-free enqueue path, and no segmentation of pending bytes.
- **Single-segment, synchronous bias.** `WalStorage` appends to one file at a time and calls `fsync` aggressively when `sync_on_flush` is on. There is no multi-segment writer or async fsync pipeline.
- **Platform coupling.** The default sink goes through `DiskScheduler`, which uses Linux `io_uring`. Non-Linux builds require feature gates or fall back to synchronous file writes manually.

## Pain Points

1. **Large WAL volume.** Pure page images lead to 4 KiB records for small tuple updates. Teaching scenarios that poke tuple headers end up saturating I/O.
2. **Redo gaps for heap/index.** ARIES invariants (pageLSN check, idempotent redo) are not enforced per operation, so we cannot safely disable FPWs.
3. **Difficult to unit test.** Without logical redo, simulating crash/restart flows in Rust tests requires writing real page images, slowing CI.
4. **Bottlenecked buffer.** The WAL buffer becomes a scaling bottleneck during heavy insert workloads because each writer must lock the Vec and reallocate.
5. **Portability concerns.** macOS / Windows builds cannot reuse the current scheduler path, so WAL cannot flush reliably there.

## Target Architecture

### Logical Heap WAL
- `HeapRecordPayload::{Insert,Update,Delete}` remain, but redo will decode them and reapply the tuple mutation (insert slot, rewrite bytes, toggle metadata).
- ARIES-style replay: check pageLSN, skip if already applied, otherwise write tuple data and set pageLSN to frame LSN.
- Only emit page images on “first dirty after checkpoint” or when tuple size forces overflow, mirroring PostgreSQL’s FPW policy.

### Index WAL & Resource Managers
- Introduce `IndexWalPayload` (Insert/Delete/Split/Merge) and `BtreeResourceManager`.
- Payload stores logical key bytes plus RecordId so redo can replay structural changes without consulting heap.
- Recovery registers the new RM alongside Heap/Page and routes frames via `ResourceManagerId::Index`.

### WAL Buffer & Storage
- Replace `WalBuffer`’s `Vec` with the existing `RingBuffer<T>` plus an atomic enqueue cursor so appenders never contend on reallocation.
- Support multiple WAL segments under `wal_dir/segment_{lsn}.wal` and allow concurrent writes + asynchronous fsync of sealed segments.
- Default to lazy flush: `sync_on_flush = false`, and flush only at checkpoints or explicit `flush_until`.

### Platform Fallback
- Wrap `DiskSchedulerWalSink` behind a trait and provide a synchronous `FileWalSink` (using `std::fs::File`) when `disk_scheduler` feature is disabled.
- Gate Linux-specific code so macOS users can still run tests with the fallback sink.

## Incremental Implementation Plan

1. **Heap Redo Enablement**
   - Extend `TableHeap` with recovery helpers (insert/update/delete without WAL, accepting `lsn`).
   - Implement `HeapResourceManager::redo` to call the helpers.
   - Update unit tests to cover redo idempotency.

2. **B+Tree WAL**
   - Define index payloads + codec, add `ResourceManagerId::Index`.
   - Teach recovery to register `BtreeResourceManager`.
   - Emit index WAL from `BPlusTreeIndex` mutations and add tests that crash/recover around splits.

3. **WAL Buffer & Storage**
   - Swap in ring-buffer implementation with lock-free enqueue (using atomics + `parking_lot` condvar for flushers).
   - Update `WalStorage` to manage multiple active segments and asynchronous fsync tickets.

4. **Portability + Config**
   - Add `FileWalSink` fallback and expose a `wal_io=fallback` feature gate.
   - Change defaults (`sync_on_flush=false`, `persist_control_file_on_flush=false`) and document the durability trade-offs.

5. **Docs & Benchmarks**
   - Update `docs/src/recovery/*.md` with the new flow.
   - Expand `benches/storage_bench.rs` to measure throughput with lazy flush and logical redo.

Each milestone should land with focused tests (unit + integration) plus at least one crash-recovery scenario using the new WAL semantics.
