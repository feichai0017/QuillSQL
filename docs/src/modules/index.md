# Index Module

Indexes live in `src/storage/index/`. QuillSQL currently ships a B+Tree (B-link variant)
that is exposed to execution via `IndexHandle`. Indexes allow point lookups and range
scans in O(log n), dramatically reducing the need for full table scans.

---

## Responsibilities

- Maintain an ordered key → `RecordId` mapping per indexed table.
- Support point probes, range scans, insert/update/delete maintenance.
- Cooperate with MVCC: entries reference heap tuples while visibility checks remain in
  execution/transaction code.
- Provide `IndexHandle::range_scan`, returning a `TupleStream` so physical operators don’t
  need to know tree internals.

---

## Directory Layout

| Path | Purpose | Key Types |
| ---- | ------- | --------- |
| `btree_index.rs` | Core B+Tree, page formats, insert/delete logic. | `BPlusTreeIndex` |
| `btree_iterator.rs` | Range-scan iterator with sibling traversal. | `TreeIndexIterator` |
| `btree_codec.rs` | Page encode/decode utilities. | `BPlusTreeLeafPageCodec` |

---

## Key Concepts

### B-link Structure
Each leaf stores a pointer to its right sibling. Iterators use this to keep scanning even
if a concurrent split occurs, avoiding restarts from the root and enabling latch-free
range scans.

### Latch Crabbing
Insert/delete operations climb the tree with shared latches and upgrade only when
necessary (e.g., right before splitting), reducing contention.

### Range Scan → TupleStream
`IndexHandle::range_scan` wraps `TreeIndexIterator` and automatically fetches heap tuples,
returning `(rid, meta, tuple)` triples. Execution remains storage-agnostic.

### Garbage Signals
`BPlusTreeIndex::note_potential_garbage` counts invisible keys; background index vacuum
consults the counter plus `IndexVacuumConfig` before pruning.

---

## Interactions

- **Catalog** – `Catalog::create_index` registers the index with `IndexRegistry`; execution
  fetches `Arc<dyn IndexHandle>` via the registry.
- **Execution** – `PhysicalIndexScan` uses `ExecutionContext::index_stream`; DML operators
  call `insert_tuple_with_indexes` so heap writes and index maintenance stay in sync.
- **Transaction/MVCC** – heaps store transaction metadata; indexes just reference RIDs, so
  MVCC visibility is enforced when tuples are materialised.
- **Recovery** – WAL contains `IndexInsert/IndexDelete` records to replay structural
  changes after crashes.

---

## Teaching Ideas

- Build a covering index example to show how avoiding heap lookups improves latency.
- Instrument `TreeIndexIterator` to visualise sibling traversal during range scans.
- Compare SeqScan vs IndexScan on selective predicates to highlight indexing benefits.

---

Further reading: [B+Tree internals](../index/btree_index.md)
