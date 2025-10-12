# Rule-Based Optimization

After the `LogicalPlanner` creates an initial `LogicalPlan`, it's passed to the **`LogicalOptimizer`**. The initial plan is a direct, syntactically correct translation of the SQL query, but it's often not the most efficient way to execute it. The optimizer's job is to transform this plan into an equivalent, but more performant, logical plan.

## The Optimizer in QuillSQL

QuillSQL implements a **Rule-Based Optimizer**. This is a common and powerful approach where the optimizer is equipped with a set of predefined transformation rules. It repeatedly applies these rules to the logical plan tree until no more rules can be applied, or a maximum number of passes is reached.

The main components are:

- **`LogicalOptimizer` (`optimizer/logical_optimizer.rs`)**: The main driver. It holds a list of rules and contains the logic to recursively walk the plan tree and apply them.
- **`LogicalOptimizerRule` Trait**: An interface that every optimization rule must implement. Its core method is `try_optimize`, which takes a plan node and attempts to return a rewritten, optimized version of that node.

## Deep Dive: The `PushDownLimit` Rule

One of the most classic and effective optimizations is "pushing down" operations as far as possible towards the data source. Let's examine the `PushDownLimit` rule (`optimizer/rule/push_down_limit.rs`) to see this in action.

Consider the following query:

```sql
SELECT * FROM users ORDER BY signup_date LIMIT 10;
```

#### The Naive Plan

A naive logical plan for this query would be:

```
Limit(10)
  └── Sort(by: signup_date)
        └── TableScan(users)
```

If executed directly, this plan would:
1.  Scan the *entire* `users` table.
2.  Sort the *entire* table by `signup_date`.
3.  Finally, discard all but the first 10 rows.

This is incredibly inefficient, especially for a large table, as it involves a massive, memory-intensive sort operation.

#### The Optimization Rule

The `PushDownLimit` rule is designed to recognize this specific pattern: a `Limit` operator directly on top of a `Sort` operator.

When the optimizer applies this rule, the `try_optimize` method matches on the `Limit` node. It inspects its child and sees that it's a `Sort` node. The rule then knows it can apply its logic.

#### The Rewritten Plan

The rule rewrites the plan tree by "pushing" the limit information *into* the `Sort` node itself:

```
Limit(10)
  └── Sort(by: signup_date, limit: 10)
        └── TableScan(users)
```

Notice the new `limit: 10` property on the `Sort` node. This seemingly small change has a huge performance impact. When the `PhysicalSort` operator is created from this logical node, it now knows that it only needs to find the top 10 rows. Instead of performing a full sort, it can use a much more efficient algorithm, like a **heap sort (using a min-heap of size 10)**, to find the top 10 rows in a single pass over the data.

This optimization avoids sorting the entire table, dramatically reducing both CPU and memory consumption.

## Other Rules

QuillSQL implements other simple but effective rules:

- **`EliminateLimit`**: Removes a `LIMIT` clause if it provides no value (e.g., `LIMIT NULL`).
- **`MergeLimit`**: If two `LIMIT` clauses are stacked on top of each other (which can happen after other rule transformations), this rule merges them into a single, more restrictive `LIMIT`.

## Conclusion

While QuillSQL's optimizer is currently rule-based and relatively simple, it demonstrates the fundamental principles of query optimization. By separating the logical representation of a query from its physical execution and applying equivalence-preserving transformations, a database can achieve massive performance gains. More advanced systems build on this with a **Cost-Based Optimizer**, which uses table statistics to estimate the "cost" of different physical plans (e.g., choosing between a `NestedLoopJoin` and a `HashJoin`) and pick the cheapest one.

---

## For Study & Discussion

1.  **Rule Ordering**: The `LogicalOptimizer` applies its list of rules in a fixed order for a set number of passes. Can the order in which rules are applied affect the final, optimized plan? Can one rule's transformation enable another rule to be applied in a subsequent pass?

2.  **Cost-Based vs. Rule-Based**: What is the primary limitation of a purely rule-based optimizer? When would a rule-based optimizer make a poor decision that a cost-based optimizer (with accurate statistics) would get right? (Hint: consider join algorithm selection).

3.  **Programming Exercise**: Implement the classic **Predicate Pushdown** rule. Your rule should look for a `Filter` operator whose child is a `Join`. If the filter's predicate only uses columns from one side of the join, the rule should push the `Filter` node down to that side of the join, below the `Join` node. This is one of the most effective optimizations in any database.

4.  **Programming Exercise**: Implement a **Constant Folding** rule. This rule would traverse expression trees and pre-compute constant expressions. For example:
    - An expression `WHERE age = 10 + 5` would be rewritten to `WHERE age = 15`.
    - An expression `WHERE 1 = 1` would be evaluated to `true`, and a smart optimizer could then potentially eliminate the `WHERE` clause entirely.
