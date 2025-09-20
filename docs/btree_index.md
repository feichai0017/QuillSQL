# B+ Tree Index — Architecture, Features, Optimizations

## 1. Architecture Overview
- Node Types: Internal, Leaf. Internal keeps key separators and child pointers; Leaf keeps (key, RID) and a linked-list via next_page_id.
- Header Page: fixed entry point storing root_page_id; atomic root switch on split/shrink.
- Concurrency:
  - Write path: latch crabbing (hold parent only if child is unsafe). Deadlock avoided via PageId-ordered sibling locking.
  - Read path: B-link + OLC. Readers can chase right siblings if key >= high_key; verify header.version and restart on change.
- Page Layout:
  - Internal header: {type, current_size, max_size, version, next_page_id} + high_key (optional) + array of (key, child)
  - Leaf header: {type, current_size, max_size, next_page_id, version} + array of (key, rid)

## 2. Key Features
- B-link (high_key + next): smooth structural publication; readers bypass transient parent states.
- OLC (version checks): page-level optimistic validation, restart on change or decode failure.
- Parent-guided redirection: insert/delete confirm expected child from parent; avoid crossing parent boundaries.
- Iterator: leaf-chain based forward scan, compatible with B-link/OLC, no key loss under concurrent splits/merges.
- Safety: split/merge protocols preserve invariants; root adjust with header lock to avoid lock inversion.

## 3. Optimizations
- Right-sibling chase on reads (B-link), reduces restarts from root.
- Lightweight OLC in iterator; next-next prefetch to improve range-scan cache hit.
- Internal key prefix compression:
  - Encode: for each key, write LCP with previous key (u16) + suffix length (u32) + suffix + child pointer.
  - Decode: reconstruct from LCP + suffix, compatible with legacy raw format via a compression flag.
- Buffer Pool Synergy: prefetch_page API for iterator; admission (TinyLFU) reduces scan pollution; sharded LRUK to reduce lock contention.

## 4. Algorithms (high level)
- Find (read):
  1) start from root, decode internal; if key >= high_key and next!=INVALID, chase next; else pick child via upper_bound.
  2) version check before/after; if changed, restart root.
  3) at leaf, version check, then lookup.
- Insert (write):
  1) latch-crabbing descend; if child full, hold parent.
  2) split: write right sibling (with high_key/next), then publish left.next/high_key/version, then update parent.
  3) on parent full, propagate split upward.
- Delete (write):
  1) descend; if underflow, try borrow from siblings (PageId order), else coalesce; maintain B-link wires: left inherits right's high_key/next on merge.

## 5. Benchmarks & Notes
- Hot-read (single-thread) may dip if admission denies cold pages during warm-up or if cleaner thread interferes.
  - Mitigation: disable cleaner for read-only runs; relax admission (count-only) or warm cache first.
- Range-scan benefits from prefix compression and iterator prefetch due to fewer cache misses and smaller internal pages.

## 6. Tuning Tips
- Disable cleaner for pure reads: do not set QUILL_BPM_CLEANER=1
- Loosen admission: make TinyLFU count-only during hot-read benchmarks.
- Increase Buffer Pool for large scans.

## 7. Future Work
- Stronger OLC with bounded retries and telemetry.
- CSB-like internal layout and columnar key prefixing.
- NUMA-aware partitioning and router.
- WAL/MVCC for crash recovery and snapshot isolation.
