# Recovery / WAL Module

`src/recovery/` guarantees the Durability part of ACID. Through WAL, checkpoints, and the
ARIES algorithm, QuillSQL can recover to a consistent state after crashes.

---

## Responsibilities

- Generate WAL records for every change and assign monotonically increasing LSNs.
- Manage WAL buffers, synchronous flushes, and the optional background WAL writer.
- Run the ARIES Analysis → Redo → Undo sequence during startup.
- Maintain the control file so the latest checkpoint and log truncation point are known.

---

## Directory Layout

| Path | Description | Key Types |
| ---- | ----------- | --------- |
| `recovery/wal/` | Log writer, buffer, storage, background writer runtime. | `WalManager`, `WalBuffer`, `WalStorage` |
| `recovery/wal_record.rs` | Serialized record variants exposed to subsystems. | `WalRecordPayload`, `ResourceManagerId` |
| `recovery/resource_manager.rs` | Registry that maps payloads to redo/undo handlers. | `ResourceManager`, `RedoContext`, `UndoContext` |
| `recovery/recovery_manager.rs` | ARIES driver. | `RecoveryManager`, `RecoverySummary` |
| `recovery/control_file.rs` | Persistent metadata (checkpoint info). | `ControlFileManager` |

---

## WalManager & I/O pipeline

- **Buffering** – `WalManager` uses a lock-free `WalBuffer` ring. `append_record_with`
  attaches an LSN, encodes the frame, pushes it into the ring, and auto-flushes when
  either record count or byte thresholds (`max_buffer_records`,
  `flush_coalesce_bytes`, or a full WAL page) are met. Writers never block on Vec
  reallocation and thus scale with concurrent transactions.
- **Physical logging** – `log_page_update` inspects old/new page images using
  `find_contiguous_diff`. First touches (tracked via `DashSet<PageId>`) or large diffs
  emit 4 KiB `PageWrite` FPWs; otherwise `PageDelta` keeps logs small. Both payloads
  carry the previous `page_lsn` so redo can enforce the WAL rule.
- **Segmentation & flush** – `WalStorage` appends frames to rolling segments under the
  configured WAL directory, queues async write/fsync tickets via the `DiskScheduler`
  sink, and recycles sealed segments after checkpoints. `flush()` drains buffered
  records up to an optional target LSN, waits for outstanding tickets, updates
  `durable_lsn`, and optionally persists the control file snapshot.
- **Background writer** – `start_background_flush` spawns `WalWriterRuntime`, which
  periodically calls `flush(None)`; `WalWriterHandle` lives in the background worker
  registry so shutdown is coordinated with the db lifecycle.
- **Checkpoints** – `log_checkpoint` writes a `Checkpoint` frame containing ATT/DPT,
  forces a flush, clears the “first-touch” set (so new FPWs are generated as needed),
  and updates `checkpoint_redo_start` in the control file. Recovery uses this redo
  start to avoid rescanning the whole log.
- **Readers & durability waits** – `WalManager::reader` iterates frames straight from
  the WAL directory. `wait_for_durable` is the gate that synchronous commits call
  after emitting a commit record; it reuses `flush_with_mode` and condition variables
  to block until `durable_lsn >= target`.

## Record types & resource managers

All log frames share the envelope defined in `wal_record.rs` and are routed by
`ResourceManagerId`:

- **Page (`ResourceManagerId::Page`)** – purely physical `PageWrite` and `PageDelta`
  payloads. The default page resource manager checks `page_lsn` through the buffer
  pool (when available) before rewriting the page on redo. No undo is required
  because these are low-level physical changes.
- **Heap (`ResourceManagerId::Heap`)** – logical payloads emitted by
  `TableHeap::append_heap_record` include relation id, page/slot, tuple metadata, and
  raw bytes. `HeapResourceManager` decodes them, rebuilds a temporary page image, and
  replays inserts/updates/deletes by splicing tuples back into the heap. Undo paths
  rely on the same metadata to restore tuple bytes or clear delete flags.
- **Index (`ResourceManagerId::Index`)** – `BPlusTreeIndex` writes logical leaf
  operations (`LeafInsert/Delete`) that store key bytes and the target `RecordId`.
  `IndexResourceManager` decodes the tuple via `TupleCodec` and mutates the leaf (via
  the buffer pool if available, otherwise direct disk I/O). Redo is idempotent (missing
  keys are inserted), while undo mirrors insert/delete semantics.
- **Transaction / CLR / Checkpoint** – `TransactionPayload` (BEGIN/COMMIT/ABORT)
  enables the undo index to link per-transaction log records. `ClrPayload` documents
  each undo step so the recovery process can survive crashes mid-undo. Checkpoints
  snapshot ATT + DPT for the analysis phase.

`ensure_default_resource_managers_registered` wires Page, Heap, and Index resource
managers together so both redo (`RedoExecutor`) and undo (`UndoExecutor`) can dispatch
records uniformly.

## Heap / Index WAL design

- **Heap** – Execution operators call `ExecutionContext::insert_tuple_with_indexes`
  (and friends), which ultimately invoke `TableHeap::append_heap_record`. Each tuple
  carries `TupleMetaRepr` (insert/delete txn + CID, MVCC chain pointers) plus encoded
  column bytes. During redo, `HeapResourceManager` reconstructs page images, inserts
  or overwrites slots, and updates tuple metadata before writing the packed page back
  via `TableHeap::recovery_view`. Undo leverages the stored “before image” to restore
  bytes or toggle delete flags; vacuum code can later reclaim dead slots once
  `safe_xmin` passes them.
- **Index** – B+Tree leaf operations log the logical key/value change, unaffected by
  page layout. Redo loads the leaf (buffer pool or disk), checks the leaf version to
  avoid reapplying newer changes, and performs the requested insert/delete. Undo mirrors
  the inverse operation so loser transactions roll back cleanly without touching the
  heap. This keeps heap and index WAL independent: heap redo never needs to read index
  pages, and vice versa.
- **Page images** – For heap/index structural changes that rewrite large sections (e.g.,
  new heap page, B+Tree splits), page guards still emit FPWs through
  `WalManager::log_page_update`. The logical heap/index WAL layers ensure redo can
  operate even when FPWs are skipped after the first touch.

---

## ARIES Recovery

1. **Analysis** – read the latest checkpoint, rebuild the Dirty Page Table (DPT) and
   Active Transaction Table (ATT).
2. **Redo** – scan forward from the checkpoint LSN, reapplying operations when the DPT
   indicates a page may be out of date.
3. **Undo** – roll back transactions still in ATT, writing Compensation Log Records (CLR)
   so the process is idempotent even if another crash happens mid-undo.

`RecoverySummary` reports how many records were redone and which transactions require
manual attention—great for classroom demonstrations.

---

## Interactions

- **TransactionManager** – emits `Begin`, `Commit`, `Abort` records and supplies undo
  information.
- **BufferManager** – links to WAL via `set_wal_manager`; checkpoints rely on the buffer’s
  dirty page metadata.
- **Background workers** – WAL writer and checkpoint worker live in `background` and use
  handles exposed by `WalManager`.

---

## Teaching Ideas

- Simulate crashes (e.g., panic right after `wal_manager.flush(None)`) and inspect log
  output on restart.
- Add a new WAL record type (like `CreateIndex`) to see how `RecoveryManager` must be
  extended.
- Compare physical vs logical redo costs to discuss ARIES trade-offs.

## Recovery Lab Playbook (CMU 15-445 style)

1. **Mini ARIES** – Disable the background WAL writer, perform a few INSERT/UPDATE
   operations, crash the process mid-transaction, and single-step through
   `analysis_pass.rs` to observe ATT/DPT reconstruction.
2. **Logical vs physical redo** – Comment out `WalManager::log_page_update` for heap
   pages and re-run the experiment. Recovery still succeeds thanks to logical heap
   payloads; re-enable FPWs to contrast log volume.
3. **Index crash test** – Inject a panic after `BPlusTreeIndex::insert` logs a leaf
   insert. On restart, watch the undo phase remove the stray key before replay finishes.
4. **Group commit tuning** – Play with `WalOptions::flush_coalesce_bytes`,
   `writer_interval_ms`, and `synchronous_commit` to demonstrate how throughput vs
   latency tradeoffs mirror the 15-445 checkpoints/commit labs.

---

Further reading: [ARIES details](../recovery/aries.md)
