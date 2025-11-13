# The Volcano Execution Model

Once the [Query Planner](../modules/plan.md) has produced an optimized `PhysicalPlan`, it's the job of the **Execution Engine** to run it and produce results. The execution engine is the component that brings the plan to life, interacting with the transaction manager and storage layer to process data.

QuillSQL uses the classic **Volcano Model**, also known as the **Iterator Model**. This is a pull-based execution model where each physical operator in the plan tree acts as an iterator that the parent operator can "pull" rows from.

## 1. The `VolcanoExecutor` Trait

At the heart of the execution model is the `VolcanoExecutor` trait (`execution/mod.rs`). Every physical operator implements this simple trait:

```rust
pub trait VolcanoExecutor {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()>;
    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>>;
    fn output_schema(&self) -> SchemaRef;
}
```

- **`init()`**: This method is called once at the beginning of execution. It allows an operator to set up its initial state (e.g., a `SeqScan` operator would initialize its table iterator here).
- **`next()`**: This is the core method. When called, the operator produces its next output tuple. It returns `Some(tuple)` if it has a row, or `None` if it has exhausted its data source. The top-level `ExecutionEngine` simply calls `next()` on the root of the plan tree in a loop until it receives `None`.

## 2. The `ExecutionContext`

Notice that both `init()` and `next()` take a mutable `ExecutionContext`. This object is the “world” in which the query runs. It is passed down the operator tree and exposes:

- **`Catalog` + `StorageEngine`**: Operators call `context.table(&table_ref)` to obtain a `TableBinding`. The binding encapsulates heap/index access (scan, insert, delete, update, prepare-row-for-write) so operators never touch `TableHeap` or `MvccHeap` directly.
- **`TxnContext`**: Provides the current transaction, MVCC snapshot, and helper methods for row/table locks and visibility checks.
- **Expression helpers**: `eval_expr` / `eval_predicate` evaluate AST expressions against a tuple without leaking `ScalarValue` plumbing into operators.

Thanks to the binding abstraction, the operator code only expresses “what” should happen (scan/update/delete) while the binding implements “how” (MVCC chain navigation, undo logging, index maintenance).

## 3. Anatomy of Physical Operators

Data flows *up* the tree from the leaves (scans) to the root. Let's see how it works by examining a few key operators.

### Leaf Operator: `PhysicalSeqScan`

A sequential scan sits at the leaf of a plan tree and streams tuples from a table binding.

- **`init()`**:
    1.  Acquires an `IntentionShared` lock on the table via `TxnContext`.
    2.  Requests a `TableBinding` (`context.table(&table_ref)`) and calls `binding.scan(...)` to obtain a `TupleStream`. The binding hides the actual `TableIterator` implementation and any MVCC plumbing.
- **`next()`**:
    1.  Pops the next `(rid, meta, tuple)` from its prefetch buffer (which in turn pulls from the binding’s stream).
    2.  Calls `TxnContext::read_visible_tuple` to perform the MVCC visibility check and acquire the required shared row lock.
    3.  Returns the tuple if visible; otherwise, keep looping.

### Unary Operator: `PhysicalFilter`

A filter has one child operator (its `input`). It implements a `WHERE` clause.

- **`next()`**: Its logic is a simple, tight loop:
    1.  It calls `self.input.next()` to get a tuple from its child.
    2.  If the child returns `None`, the filter is also exhausted and returns `None`.
    3.  If it receives a tuple, it evaluates its predicate expression (e.g., `age > 30`) against the tuple.
    4.  If the predicate evaluates to `true`, it returns the tuple. Otherwise, it loops back to step 1.

### Binary Operator: `PhysicalNestedLoopJoin`

A join has two children: a left (outer) and a right (inner).

- **`next()`**: It implements the classic nested loop join algorithm:
    1.  Fetch one tuple from the **outer** (left) child and hold onto it.
    2.  Enter a loop: fetch tuples one by one from the **inner** (right) child.
    3.  For each inner tuple, combine it with the held outer tuple and evaluate the join condition. If it matches, return the combined tuple.
    4.  When the inner child is exhausted, **rewind it** by calling `self.right_input.init()` again.
    5.  Go back to step 1 to fetch the *next* tuple from the outer child.
    6.  Repeat until the outer child is also exhausted.

## 4. Putting It All Together

Consider the query `SELECT name FROM users WHERE age > 30`. The physical plan is `Projection -> Filter -> SeqScan`.

1.  The `ExecutionEngine` calls `next()` on the `Projection` operator.
2.  The `Projection` operator needs a tuple, so it calls `next()` on its child, `Filter`.
3.  The `Filter` operator needs a tuple, so it calls `next()` on its child, `SeqScan`.
4.  The `SeqScan` operator asks its `TableBinding` for the next tuple, and after the MVCC check finds a visible tuple for a user with `age = 25`.
5.  `SeqScan` returns this tuple up to `Filter`.
6.  `Filter` evaluates `age > 30` on the tuple. It's false, so it loops, calling `SeqScan.next()` again.
7.  `SeqScan` pulls another visible tuple (this time `age = 40`, `name = 'Alice'`) through the binding.
8.  `SeqScan` returns this tuple up to `Filter`.
9.  `Filter` evaluates `age > 30`. It's true! It returns the tuple for Alice up to `Projection`.
10. `Projection` takes the full tuple for Alice, creates a new tuple containing only the `name` column (`'Alice'`), and returns this new tuple as the result.

This process repeats, with tuples flowing up the tree one at a time, until the `SeqScan` operator runs out of pages and returns `None`, which then propagates up the tree, signaling the end of execution.

---

## For Study & Discussion

1.  **Push vs. Pull Models**: The Volcano model is a "pull-based" model. An alternative is a "push-based" model, where operators push their results to their parents as soon as they are ready. What are the potential advantages and disadvantages of each model, particularly concerning cache efficiency and control flow?

2.  **Blocking vs. Non-Blocking Operators**: Some operators, like `PhysicalFilter`, can produce their first output row as soon as they receive their first input row. These are **non-blocking**. Other operators, like `PhysicalSort`, must consume their *entire* input before they can produce even a single row of output. These are **blocking**. What is the impact of blocking operators on query latency and memory usage?

3.  **Programming Exercise**: The current `PhysicalNestedLoopJoin` is simple but can be inefficient as it re-scans the entire inner table for every outer row. Implement a `PhysicalBlockNestedLoopJoin` operator. This version would read a *block* (a small batch) of tuples from the outer table into an in-memory buffer, and then iterate through the inner table once for that entire block. This can significantly reduce the number of times the inner table needs to be scanned.

4.  **Programming Exercise**: Implement the `PhysicalLimit` operator. Its `next()` method should:
    a.  Keep an internal counter.
    b.  If the counter is less than the `offset`, pull and discard tuples from its child.
    c.  If the counter is between `offset` and `offset + limit`, pull a tuple from its child and return it.
    d.  Once the limit is reached, it should stop pulling from its child and return `None` for all subsequent calls. This is important for efficiency, as it stops the execution of the entire sub-tree below it.
