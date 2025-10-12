# The Lifecycle of a Query

When you submit a SQL query to a database, it doesn't just magically produce a result. The database undertakes a sophisticated, multi-stage process to translate the declarative SQL statement (which describes *what* data you want) into an imperative, efficient execution plan (which describes *how* to get that data). This entire process is the responsibility of the **Query Planner**.

In QuillSQL, this process follows a classic, compiler-like pipeline, which is a cornerstone of modern database architecture as taught in courses like CMU 15-445.

The journey from a SQL string to an executable plan involves several transformations:

**SQL String** -> **AST (Abstract Syntax Tree)** -> **Logical Plan** -> **Optimized Logical Plan** -> **Physical Plan**

Let's break down each stage.

### Stage 1: Parsing (SQL -> AST)

The first step is purely syntactic. The raw SQL query string is fed into a parser. QuillSQL uses the excellent `sqlparser` crate for this. The parser checks if the SQL conforms to valid grammar and, if so, converts it into an **Abstract Syntax Tree (AST)**.

An AST is a direct tree representation of the SQL query's structure. For example, `SELECT id FROM users WHERE age > 30` would be parsed into a tree structure with nodes representing the `SELECT` clause, the table `users`, the `WHERE` clause, and the predicate `age > 30`.

### Stage 2: Logical Planning (AST -> Logical Plan)

Next, the `LogicalPlanner` (`plan/logical_planner.rs`) walks the AST and converts it into a **Logical Plan**. 

A Logical Plan is a tree of relational algebra operators. It describes the query in terms of high-level data operations, completely independent of how the data is stored or which algorithms will be used. It defines *what* to do, not *how* to do it.

Key logical operators in QuillSQL (`plan/logical_plan/mod.rs`) include:
- **`TableScan(users)`**: Represents reading the entire `users` table.
- **`Filter(predicate: age > 30)`**: Represents filtering rows based on a condition.
- **`Projection(columns: [id])`**: Represents selecting specific columns.
- **`Join`**: Represents joining two data sources.
- **`Aggregate`**: Represents a `GROUP BY` operation.
- **`Sort`**: Represents an `ORDER BY` operation.

For our example query, the initial logical plan might look like this:

```
Projection(columns=[id])
  └── Filter(predicate=age > 30)
        └── TableScan(users)
```

### Stage 3: Logical Optimization

Before executing the plan, we have a crucial opportunity to make it more efficient. The `LogicalOptimizer` (`optimizer/logical_optimizer.rs`) takes the logical plan and applies a series of **transformation rules** to produce a new, equivalent logical plan that is expected to be faster.

QuillSQL uses a simple but effective rule-based optimizer. A classic example of such a rule is **Predicate Pushdown**. Consider this query:

`SELECT name FROM (SELECT * FROM users JOIN cities ON users.city_id = cities.id) WHERE users.age > 30;`

A naive logical plan would first perform a full `JOIN` between `users` and `cities` and *then* filter the massive result. Predicate pushdown is a rule that would rewrite the plan to apply the `age > 30` filter *before* the join:

**Before Optimization:**
```
Filter(users.age > 30)
  └── Join(users.city_id = cities.id)
        ├── TableScan(users)
        └── TableScan(cities)
```

**After Optimization (Predicate Pushdown):**
```
Join(users.city_id = cities.id)
  ├── Filter(users.age > 30)
  │     └── TableScan(users)
  └── TableScan(cities)
```

By filtering early, we dramatically reduce the number of rows that need to be processed by the expensive `Join` operator. QuillSQL implements similar rules, such as `PushDownLimit`, which pushes `LIMIT` clauses down the tree to reduce the amount of data processed.

### Stage 4: Physical Planning (Logical Plan -> Physical Plan)

Finally, the `PhysicalPlanner` (`plan/physical_planner.rs`) converts the optimized logical plan into a **Physical Plan**. 

A Physical Plan describes exactly *how* the query will be executed. It maps each logical operator to a concrete algorithm or implementation.

- A `LogicalPlan::TableScan` becomes a `PhysicalSeqScan` (a sequential scan of the table heap).
- A `LogicalPlan::Filter` becomes a `PhysicalFilter`, which implements the filtering logic.
- A `LogicalPlan::Join` becomes a `PhysicalNestedLoopJoin`. This is where the database commits to a specific join algorithm. A more advanced database might have multiple options (e.g., `PhysicalHashJoin`, `PhysicalSortMergeJoin`) and would use a cost model to choose the best one. QuillSQL currently implements Nested Loop Join.

Each node in the physical plan tree is an executor that the [Execution Engine](../modules/execution.md) can run. This final plan is what gets executed to produce the query result.

## Conclusion

This layered approach—from syntax to a logical representation, then to an optimized logical representation, and finally to a concrete physical execution plan—is fundamental to database design. It provides a clear separation of concerns and, most importantly, creates a dedicated **optimization stage**, which is the key to achieving high performance on a wide variety of SQL queries.

---

## For Study & Discussion

1.  **Logical vs. Physical**: Why is the separation between logical and physical plans so important? What would be the disadvantages of a simpler system that converted the AST directly into a physical plan?

2.  **Join Algorithms**: QuillSQL currently only implements `NestedLoopJoin`. What are two other common join algorithms? Describe how they work and in what scenarios they would be more performant than a nested loop join.

3.  **Programming Exercise (Advanced)**: Implement a `PhysicalHashJoin` operator. This is a significant undertaking that involves:
    a.  Creating a `PhysicalHashJoin` struct that implements the `VolcanoExecutor` trait.
    b.  In the `init()` phase, it should consume the entire "build" side (typically the smaller, right-hand table) and build an in-memory hash table from its rows.
    c.  In the `next()` phase, it should read from the "probe" side (the left-hand table) one row at a time, probe the hash table for matches, and emit the joined tuples.
    d.  Modify the `PhysicalPlanner` to choose `PhysicalHashJoin` instead of `PhysicalNestedLoopJoin` for equi-joins.

4.  **Programming Exercise**: Add support for the `UNION ALL` operator. This would involve:
    a.  Adding a `Union` variant to the `LogicalPlan` enum.
    b.  Updating the `LogicalPlanner` to recognize the `UNION` syntax in the AST and create a `LogicalPlan::Union` node.
    c.  Creating a `PhysicalUnion` executor that pulls tuples from its first child until it's exhausted, and then pulls tuples from its second child.


