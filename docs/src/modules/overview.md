# Module Overview

This page gives a teaching-friendly tour of every QuillSQL subsystem. Each section
explains where the code lives (`src/<module>`), the most important types, and the
design decisions that make the module fit into the whole system. Read it together
with [`architecture.md`](../architecture.md) for the big-picture data flow.

---

## 1. SQL Front-End (`src/sql`)

| Submodule | Responsibilities | Key Types / Functions |
| --------- | ---------------- | --------------------- |
| `lexer.rs`, `parser.rs` | Translate raw SQL into an AST using `sqlparser` plus Quill-specific extensions. | `parse_sql(&str) -> Vec<Statement>` |
| `ast/mod.rs` | Normalises identifiers, handles multi-part names, attaches spans for diagnostics. | `NormalizedIdent`, `ObjectNameExt` |
| `plan/lowering.rs` | Bridges AST → planner structs for DDL extras not in upstream `sqlparser`. | `CreateIndexSpec`, `ColumnDefExt` |

Implementation notes:
- We intentionally keep the AST “SQL-shaped”. No premature desugaring occurs in the
  parser, which keeps the step-by-step teaching narrative simple: **SQL text → AST →
  logical plan**.
- Error messages bubble up with span information, so labs around parser extensions can
  show students exactly which token misbehaved.
- Suggested exercises: extend the parser with `ALTER TABLE` or window functions, then
  observe how the new AST nodes flow into the planner layer.

---

## 2. Logical Planning (`src/plan`)

| Component | Description |
| --------- | ----------- |
| `LogicalPlanner` | Converts AST nodes into strongly typed `LogicalPlan` variants. Responsible for type checking, alias resolution, and scope handling. |
| `PlannerContext` | Surface for catalog lookups while planning. |
| `PhysicalPlanner` | Lowers optimized logical plans into physical Volcano operators (`PhysicalPlan`). |

Highlights:
- Every `LogicalPlan` variant stores its child plans in `Arc`, so the tree is cheap to
  clone for optimizer passes or debugging prints.
- Planner enforces column binding rules: scope stacks keep track of aliases, CTEs, and
  correlation so students can see how real compilers resolve identifiers.
- DDL nodes capture `TableReference` + `Schema` objects. Later phases use them to skip
  repeated catalog lookups (helpful for labs about metadata caching).

---

## 3. Optimizer (`src/optimizer`)

| Piece | Responsibility |
| ----- | -------------- |
| `LogicalOptimizer` | Applies a pipeline of rule-based rewrites (predicate pushdown, projection pruning, constant folding). |
| `rules` | Individual `OptimizerRule` implementations. |

Teaching hooks:
- Each rule implements `OptimizerRule::rewrite(&LogicalPlan) -> Option<LogicalPlan>`. The
  return type makes it obvious whether a rewrite fired, so labs can instrument hit counts
  or add tracing.
- Because rules are pure functions, students can safely reorder or A/B test heuristics in
  isolation (e.g., “what if we only push predicates below joins when the selectivity
  estimate exceeds X?”).
- Example lab: add constant folding or join commutation, run the sqllogictest suite, and
  compare plan dumps to see new shapes.

---

## 4. Execution Engine (`src/execution`)

| Element | Details |
| ------- | ------- |
| `PhysicalPlan` | Enum covering all Volcano operators. Each variant implements `VolcanoExecutor`. |
| `VolcanoExecutor` | `init(&mut ExecutionContext)` and `next(&mut ExecutionContext)` pair define the iterator model. |
| `ExecutionContext` | Supplies catalog access, expression evaluation, and (most importantly) a pluggable `StorageEngine`. |

Design notes:
- Operators stay declarative. They describe *what* to scan or modify and delegate the
  *how* to storage handles. For example, `PhysicalSeqScan` now requests a `TupleStream`
  via `ExecutionContext::table_stream`, so it never touches `TableHeap` internals.
- Helper methods such as `eval_predicate`, `insert_tuple_with_indexes`, or
  `prepare_row_for_write` encapsulate MVCC/locking rules so physical plans remain short.
- ExecutionContext caches `TxnContext`, making it straightforward to teach isolation
  semantics: examine `TxnContext::lock_table` and `read_visible_tuple` to see when locks
  are taken or released.
- Suggested lab: implement a new physical operator (e.g., hash join) by wiring two child
  `PhysicalPlan`s without ever touching storage-specific code. This highlights the payoff
  of the handle abstraction.

---

## 5. Transaction Runtime (`src/transaction`)

| Type | Purpose |
| ---- | ------- |
| `TransactionManager` | Creates/commits/aborts transactions, manages undo chains, and coordinates WAL durability. |
| `TxnContext` | Per-command wrapper passed to execution. Provides MVCC snapshot (`TransactionSnapshot`), lock helpers, and command ids. |
| `LockManager` | Multi-granularity 2PL with deadlock detection. |

Deeper dive:
- `Transaction` stores a cached `TransactionSnapshot` plus its undo records. Students can
  inspect `Transaction::set_snapshot` to see how Repeatable Read/Serializable keep a
  stable view.
- `TxnRuntime` pairs a transaction with a command id. Every SQL statement increments the
  command id so MVCC can distinguish between tuples created earlier in the same
  transaction vs. the current statement—great for explaining “recent writes are invisible
  during UPDATE scanning”.
- `LockManager` exposes functions like `lock_table` / `lock_row`. Internally it keeps a
  wait-for graph and victim selection policy, which makes deadlock detection tangible for
  concurrency lectures.
- Undo entries (`UndoRecord`) store heap + index data. When an abort occurs the engine
  generates CLRs, demonstrating ARIES-style logical undo.

---

## 6. Storage Engine & Handles (`src/storage`)

| Component | Highlights |
| --------- | ---------- |
| `engine.rs` | Defines `StorageEngine`, `TableHandle`, `IndexHandle`, `TupleStream`, and `IndexScanRequest`. |
| `table_heap` | Slotted-page heap with MVCC metadata (`TupleMeta`, forward/back pointers). |
| `index` | B+Tree (B-link) implementation with iterators and codecs. |

Key ideas for teaching:
Topics to emphasise:
- **Handle abstraction**: Execution asks the engine for a `TableHandle`, then calls
  `full_scan()` to receive a `TupleStream`. The default engine simply wraps
  `TableHeap`/`BPlusTreeIndex`, but students can plug in their own engines for research.
- **TupleStream**: Minimal interface returning `(RecordId, TupleMeta, Tuple)` triples.
  Operators layer MVCC visibility on top, while the stream hides buffering details.
- **Scan extensions**: Consider extending `full_scan()` to accept projection/batch hints—
  a natural exercise for showing how execution and storage negotiate capabilities.
- `table_heap` demonstrates MVCC version chains (forward/back pointers) and the slotted
  page layout. Encourage students to trace `MvccHeap::update` alongside WAL entries to
  see how new versions link together.
- `index/btree_index.rs` implements a B-link tree with separate codecs. Scanning via
  `TreeIndexIterator` shows how to perform lock-free range scans using sibling pointers—
  perfect for advanced systems lectures.

---

## 7. Buffer Manager & Disk I/O (`src/buffer`, `src/storage/disk_*`)

| Module | Description |
| ------ | ----------- |
| `buffer::BufferManager` | Page table + LRU-K/TinyLFU replacer, dirty tracking, guard types for safe borrowing. |
| `storage::disk_scheduler` | `io_uring`-powered async I/O. Foreground threads enqueue read/write/fsync commands and await completions. |
| `storage::disk_manager` | Thin wrapper for file layout, page enlargement, and durability fences. |

Extra details:
- Page guards come in three flavours: read, write, and upgradeable. Each implements `Drop`
  semantics that release latches automatically, reinforcing RAII patterns.
- Replacement policy combines LRU-K history with TinyLFU admission. Labs can toggle the
  feature flag to measure hit-rate differences under sqllogictest or custom workloads.
- DiskScheduler uses lock-free queues plus dedicated worker tasks. A teaching exercise is
  to run with `RUST_LOG=debug` and observe how read/write/fsync commands are batched.

---

## 8. Recovery & WAL (`src/recovery`)

| Item | Notes |
| ---- | ----- |
| `WalManager` | Allocates LSNs, buffers log records, drives background WAL writer, and integrates with checkpoints. |
| `RecoveryManager` | Implements ARIES analysis/redo/undo. Uses `ControlFileManager` snapshots to seed restart. |
| `wal_record.rs` | Defines logical (`HeapInsert`, `IndexDelete`, index structure/root records) and physical (`PageWrite` FPWs) records. |

Teaching hook:
- WAL and data share the disk scheduler. Students can trace one UPDATE from log append,
  to buffer dirtying, to redo/undo via the exact same `RecordId`.
- Recovery exports statistics (redo count, loser transactions) so labs can check their
  WAL experiments automatically.
- `recovery/analysis.rs` shows how Dirty Page Table and Active Transaction Table are
  reconstructed—ideal for demystifying ARIES’ first phase.
- Students can implement “logical replay only” or “page image replay” modes by toggling
  the commit record types, then verify behaviour using the provided transaction tests.

---

## 9. Background Services (`src/background`)

| Worker | What it does |
| ------ | ------------ |
| WAL writer | Flushes WAL buffer at configured cadence. |
| Checkpoint | Captures the Dirty Page Table + Active Transaction Table. |
| Buffer writer | Flushes dirty frames to keep checkpoints short. |
| MVCC vacuum | Reclaims tuple versions older than `safe_xmin`. |

More context:
- Workers share a `BackgroundWorkers` registry so the database can spawn/stop them as a
  group (handy for tests). The registry exposes `shutdown_all()` which unit tests call to
  ensure a clean exit.
- Config structs (`IndexVacuumConfig`, `MvccVacuumConfig`, etc.) live in `src/config`
  and are exposed through `DatabaseOptions` for easy fiddling in labs.
- WAL writer and checkpoint worker simply wrap closures around `tokio::task::JoinHandle`.
  This design keeps async runtime code out of the core engine, making it simpler for
  students to trace background effects.
- Exercise idea: tweak `MvccVacuumConfig::batch_limit` and observe how many tuple
  versions stay behind by querying hidden statistics tables.

---

## 10. Configuration & Session Layer (`src/database`, `src/session`, `src/config`)

| Type | Role |
| ---- | ---- |
| `Database` | Boots disk/WAL/buffer components, runs recovery, wires background workers, and executes SQL strings. |
| `SessionContext` | Tracks per-connection defaults (autocommit, isolation) and holds the active transaction handle. |
| `config::*` | Central place for WAL, buffer pool, vacuum, and HTTP/CLI tuning knobs. |

Extra pointers:
- Front-ends (`bin/client`, `bin/server`) both embed a `Database`, proving that the core
  library can serve multiple UIs without change.
- `DatabaseOptions` show how to construct dev/test setups (temporary files, alternate WAL
  directories) in a few lines.
- `session::SessionContext` demonstrates auto-commit semantics: it lazily starts a
  transaction and uses `TransactionScope` to interpret `SET TRANSACTION` statements.
- Configuration structs derive `Clone + Debug`, making them easy to print in labs or
  feed from environment variables (HTTP server uses `QUILL_DB_FILE`, `PORT`, etc.).

---

## 11. Testing & Documentation (`tests/`, `docs/`)

| Area | Purpose |
| ---- | ------- |
| `tests/sql_example/*.slt` | sqllogictest suites for SQL coverage. |
| `tests/transaction_tests.rs` | Unit tests for MVCC invariants, locking, and visibility. |
| `docs/` | This mdBook. Every module adds its own deep-dive chapter, making it straightforward for students to jump from code to guided explanations. |

Testing strategy:
- Developers can run `cargo test -q` for fast feedback. Long-running suites can be wrapped
  with `timeout` as suggested in `AGENTS.md`.
- Example-driven docs (like this page) mirror the repository layout, so onboarding students
  can find code and tests with minimal guesswork.
- Encourage students to add sqllogictest cases alongside code changes. Because each case lives
  in `tests/sql_example`, git diffs double as documentation.
- For modules with heavy concurrency (lock manager, WAL), pair unit tests with tracing: the
  CI logs become walkthroughs for tricky paths.

---

## 12. Suggested Reading Order for Learners

1. **Introduction ➜ Architecture** – get the 10,000ft view.
2. **Module Overview (this page)** – map names to directories.
3. **Execution + Storage chapters** – understand TupleStreams, handles, and MVCC.
4. **Recovery + Transaction** – see how WAL && MVCC interlock.
5. **Buffer, Index, Background** – dive into advanced systems topics once the basics stick.

Treat this document as a living index: update it whenever a subsystem gains new entry
points (e.g., asynchronous scans, new index types) so future contributors always know
where to look next.
