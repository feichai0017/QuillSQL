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
| `wal_manager.rs` | Log writer, buffer manager, background thread spawner. | `WalManager`, `WalWriterHandle` |
| `wal_record.rs` | All record variants. | `WalRecord`, `LogSequenceNumber` |
| `recovery_manager.rs` | ARIES driver. | `RecoveryManager`, `RecoverySummary` |
| `control_file.rs` | Persistent metadata (checkpoint info). | `ControlFileManager` |

---

## WAL Highlights

- **Write-ahead rule** – before flushing a dirty page, ensure `page_lsn <= flushed_lsn`
  via `WalManager::flush_until`.
- **Record types** – logical (`HeapInsert`, `IndexDelete`) for redo/undo and physical
  (`PageWrite`, `PageDelta`) for large changes.
- **Segmentation** – WAL files are cut into segments per `WalOptions::segment_size`, and
  background writers can flush them asynchronously.

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

---

Further reading: [ARIES details](../recovery/aries.md)
