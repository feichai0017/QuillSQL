# Write-Ahead Logging

This note dives into the WAL subsystem that powers QuillSQL’s recovery story. It
explains how frames are generated, buffered, flushed, and replayed, and why logical
heap/index records complement traditional physical FPWs.

---

## 1. Component map

| Layer | Location | Responsibility |
| ----- | -------- | -------------- |
| **WalManager** | `src/recovery/wal/mod.rs` | Assigns LSNs, encodes `WalRecordPayload`, enqueues frames, and drives `WalStorage::flush`. |
| **WalBuffer** | `src/recovery/wal/buffer.rs` | Lock-free ring buffer (`ConcurrentRingBuffer`) that tracks pending frame count, bytes, and highest end LSN. |
| **WalStorage** | `src/recovery/wal/storage.rs` | Maintains WAL directory/segments, builds `WalPage`s, and dispatches write/fsync tickets to a `WalSink` (default: `DiskSchedulerWalSink`). |
| **WalWriterRuntime** | `src/recovery/wal/writer.rs` | Background thread that periodically calls `WalManager::flush(None)`. |
| **ControlFileManager** | `src/recovery/control_file.rs` | Persists `durable_lsn`, `max_assigned_lsn`, checkpoint metadata, and redo start for fast crash recovery. |
| **Resource managers** | `src/recovery/resource_manager.rs`, `storage/*/heap_recovery.rs`, `storage/*/index_recovery.rs` | Decode payloads per `ResourceManagerId` and execute redo/undo logic for heap/index/page operations. |

End-to-end DML flow:

1. `ExecutionContext` mutates tuples/index entries via `TableHeap` / `BPlusTreeIndex`.
2. Operators invoke `WalManager::append_record_with` or `log_page_update` after data-page
   changes succeed.
3. Frames enter `WalBuffer`. Once thresholds are met, `flush()` drains frames into
   `WalStorage`, which schedules asynchronous writes/fsyncs.
4. During commit, `TransactionManager` waits on `WalManager::wait_for_durable(lsn)` to
   guarantee WAL persistence before releasing locks.

---

## 2. Frame & record taxonomy

`wal_record.rs` defines the canonical envelope. `encode_frame(lsn, prev_lsn, payload)`
serializes a record, while `WalAppendContext` reports the LSN range back to the caller.

| ResourceManagerId | Payload | Purpose |
| ----------------- | ------- | ------- |
| `Page` | `PageWritePayload` | Full-page image (FPW) on first-touch or large diffs; carries `prev_page_lsn` for redo checks. |
| `Page` | `PageDeltaPayload` | Offset + byte slice for small modifications to keep WAL compact. |
| `Heap` | `HeapRecordPayload::{Insert,Update,Delete}` | Logical tuples containing relation id, page/slot, `TupleMetaRepr`, and tuple bytes. |
| `Index` | `IndexRecordPayload::{LeafInsert,LeafDelete,LeafUpdate}` | Logical B+Tree leaf mutations with key bytes and `RecordId`. |
| `Transaction` | `TransactionPayload` | BEGIN / COMMIT / ABORT markers that seed Undo’s per-txn chains. |
| `Checkpoint` | `CheckpointPayload` | Captures ATT/DPT snapshots plus redo start. |
| `Clr` | `ClrPayload` | Compensation Log Records documenting each undo step and its `undo_next_lsn`. |

`ResourceManagerId` determines how `RedoExecutor` / `UndoExecutor` route frames:
Page → physical redo only; Heap/Index → logical redo/undo using storage helpers;
Transaction/CLR/Checkpoint → interpreted by the analysis/undo phases.

---

## 3. Heap WAL: MVCC-aware logical logging

- **Emission points** – `TableHeap::{insert,update,delete}_tuple` call
  `append_heap_record` (see `src/storage/heap/table_heap.rs:72-156`) after updating the
  in-memory page. Each record stores the relation identifier, page/slot, tuple metadata,
  and before/after tuple bytes.
- **Encoding** – `src/storage/heap/wal_codec.rs` serializes `TupleMeta` (insert/delete
  txn id, command id, MVCC chain pointers) plus optional previous tuple bytes in a
  compact layout.
- **Redo** – `HeapResourceManager` (`src/storage/heap/heap_recovery.rs`) decodes the
  payload, reconstructs a `PageImage`, applies insert / overwrite / delete to the target
  slot, and writes the rebuilt page back, updating the page LSN. If the buffer pool is
  available, `TableHeap::recovery_view` mutates the cached page directly.
- **Undo** – Uses the stored “before” metadata and bytes to restore tuples or clear
  delete flags when loser transactions roll back.
- **Interaction with FPW** – Logical heap redo allows tuple-level replay without FPWs.
  FPWs are still emitted for first touches or large diffs to guard against torn-page
  scenarios, but small updates remain lightweight.

---

## 4. Index WAL: logical B+Tree leaf operations

- **Emission points** – `BPlusTreeIndex` records every leaf insert/delete/update via
  `append_index_record` (`src/storage/index/btree_index.rs:86-145`), using
  `src/storage/index/wal_codec.rs` to encode key schema, key bytes, target page, and
  transaction id.
- **Redo** (`src/storage/index/index_recovery.rs`) steps:
  1. Decode the key with `TupleCodec` using the stored schema.
  2. Fetch the leaf through the buffer pool (preferred) or `DiskScheduler`.
  3. Skip if `page_lsn >= frame_lsn`; otherwise apply the logical mutation and bump the
     leaf version.
  4. Write the updated page back with the frame LSN.
- **Undo** – Performs the inverse operation (insert→delete, delete→insert old RID,
  update→restore old RID) only for loser transactions.
- **Benefits** – Heap and index WAL are decoupled: heap redo never reads index pages,
  and logical leaf updates avoid writing full-page images for small key/value changes.
  Structural operations (splits/merges) still rely on page-level logging.

---

## 5. Buffering & flush strategy

- **Thresholds** – `max_buffer_records` (from `WalOptions::buffer_capacity`),
  `flush_coalesce_bytes`, and one WAL page (4 KiB) trigger batched flushes.
- **Flush mechanics** – `WalManager::flush_with_mode` drains frames up to a target LSN,
  asks `WalStorage::append_records` to write them, then waits on all `WalFlushTicket`s.
  `flush_until` forces durability before commit or after undo.
- **Checkpoints** – `log_checkpoint` forces a flush, records `checkpoint_redo_start` in
  the control file, and clears the “touched pages” set so new FPWs fire only once per
  checkpoint interval.
- **Background writer** – `WalWriterRuntime` runs when `WalOptions::writer_interval_ms`
  is non-zero, smoothing out flush pressure even when foregound transactions are light.

---

## 6. Relation to ARIES

1. **Analysis** – `AnalysisPass` parses the latest checkpoint, reconstructs ATT/DPT by
   scanning the tail of the log, and chooses a redo start (`min(dpt.rec_lsn)`).
2. **Redo (repeat history)** – `RedoExecutor` iterates from `start_lsn`, invoking the
   appropriate resource manager for each frame. Page RM checks pageLSN before applying
   FPW/delta; Heap/Index RMs use logical payloads to rebuild tuples or leaf entries.
3. **Undo** – `UndoExecutor` chains loser transactions backwards, calling each resource
   manager’s undo method and emitting CLRs with `undo_next_lsn`. If recovery crashes
   mid-undo, the next run resumes at the recorded `undo_next_lsn`.

---

## 7. Tuning & troubleshooting

- **Configuration** – `WalOptions` inside `DatabaseOptions` expose `segment_size`,
  `sync_on_flush`, `writer_interval_ms`, `synchronous_commit`, `retain_segments`, etc.
- **Introspection** – `WalManager::pending_records()` dumps in-memory frames for
  debugging; `background::BackgroundWorkers::snapshot()` reports WAL writer/checkpoint
  worker metadata. Enabling `RUST_LOG=trace` reveals FPW vs delta decisions and flush
  cadence.
- **Crash testing** – Insert a forced `std::process::exit(1)` after specific DMLs, then
  restart and inspect `RecoverySummary` (redo count + loser transactions) to ensure
  heap/index WAL cover the intended cases.

---

With logical heap/index records plus FPWs as a safety net, QuillSQL’s WAL stays
teaching-friendly while mirroring production-grade recoverability. When introducing
new components (e.g., custom indexes or vacuum steps), define a payload + resource
manager and they will automatically participate in ARIES.
