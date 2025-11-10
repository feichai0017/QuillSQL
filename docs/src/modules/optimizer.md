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

### Cost Guidance (DuckDB-inspired)

Although the rule engine itself stays heuristics-only, the physical planner now taps into
`CostEstimator`, which borrows the same intuition as DuckDB’s simple cardinality model:
- equality selectivity ≈ `1 / distinct_count`
- range selectivity derived from min/max assuming uniform distribution

The estimator multiplies per-predicate selectivities (clamped to `>= 1%`), producing an
estimated filtered row count. Physical planner decisions (e.g., sequential scan vs B+Tree
index) are then based on these cost numbers, so students can see how statistics influence
operator choice without implementing a full-blown CBO.

---

## Interactions

- **LogicalPlan** – the optimizer only sees logical nodes; physical/storage layers remain
  untouched.
- **Catalog / Statistics** – current rules are heuristic, but `TableStatistics` is
  available for students to experiment with cost-based decisions (e.g., choose index scan
  when selectivity is low).
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
