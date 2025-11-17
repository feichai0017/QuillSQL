# Optimizer Module

`src/optimizer/` contains a lightweight, teaching-friendly rule engine. It rewrites
`LogicalPlan` trees into cheaper equivalents without requiring a full cost-based
framework.

---

## Responsibilities

- Define the `OptimizerRule` trait (“match → rewrite”).
- Ship built-in rules such as predicate pushdown, projection pruning, and limit pushdown.
- Provide a pipeline (`LogicalOptimizer`) that repeatedly applies rules until reaching a
  fixpoint, while remaining extensible for future cost models.

---

## Directory Layout

| Path | Description | Key Types |
| ---- | ----------- | --------- |
| `mod.rs` | Optimizer entry point. | `LogicalOptimizer` |
| `rule.rs` | Trait + shared helpers. | `OptimizerRule` |
| `rules/*` | Concrete rewrites. | `PushDownFilter`, `PushDownLimit`, … |

---

## How It Works

1. `LogicalOptimizer::optimize(plan)` iterates through the registered rule list.
2. Each rule implements `fn apply(&LogicalPlan) -> Option<LogicalPlan>`. Returning `Some`
   means the rule fired; the pipeline restarts to reach a fixpoint.
3. Rules are pure functions, which keeps them easy to unit test and reason about.

Examples:
- **PushDownFilter** moves filters below scans/joins to reduce input size sooner.
- **PushDownLimit** applies LIMIT before expensive joins/sorts when safe.
- **PruneProjection** removes unused columns so execution/storage decode less data.

### Extending With Statistics

The optimizer intentionally remains heuristics-only, and the physical planner sticks to
simple sequential scans. For coursework, students can still read `TableStatistics` from
the catalog to prototype their own cardinality estimates or cost heuristics (e.g., to
experiment with when to prefer an index scan), but no estimator ships in-tree.

---

## Interactions

- **LogicalPlan** – the optimizer only sees logical nodes; physical/storage layers remain
  untouched.
- **Catalog / Statistics** – current rules are heuristic, but `TableStatistics` remains
  available for students who want to prototype their own cost-based decisions.
- **Execution** – leaner logical plans translate into simpler physical plans (e.g.,
  predicate pushdown allows `PhysicalSeqScan` to discard rows earlier).

---

## Teaching Ideas

- Implement a new rule (join reordering, constant folding) and use `RUST_LOG=trace` to
  compare plan dumps before/after.
- Discuss pipeline ordering—swap rule order and observe different outcomes.
- Prototype a tiny cost estimator using row counts from `TableStatistics` to decide on
  index scans vs sequential scans.

---

Further reading: [Rule-Based Optimization](../optimizer/rules.md)
