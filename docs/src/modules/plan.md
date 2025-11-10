# Query Planner Module

`src/plan/` bridges parsed SQL and executable operators. It converts the AST into a
logical plan, applies rewrites (via the optimizer), and finally emits a physical plan
(`PhysicalPlan`) that the Volcano engine can run.

---

## Responsibilities

1. **LogicalPlanner** – walks the AST, binds table/column names using `PlannerContext`,
   performs type checking, and builds a `LogicalPlan` tree.
2. **PlannerContext** – exposes catalog lookups plus scope information for CTEs, subqueries,
   and aliases.
3. **PhysicalPlanner** – lowers an optimized `LogicalPlan` into a tree of Volcano operators.

---

## Directory Layout

| Path | Description | Key Types |
| ---- | ----------- | --------- |
| `logical_plan.rs` | Logical algebra nodes. | `LogicalPlan`, `LogicalExpr`, `JoinType` |
| `logical_planner.rs` | AST → logical transformation. | `LogicalPlanner` |
| `physical_plan.rs` | `PhysicalPlan` enum definition. | `PhysicalPlan`, `Physical*` structs |
| `physical_planner.rs` | Logical → physical lowering. | `PhysicalPlanner` |
| `planner_context.rs` | Catalog/scope abstraction. | `PlannerContext` |

---

## Workflow

1. **Name binding** – `LogicalPlanner` resolves table + column references, creates
   `TableReference`s, and validates schemas via the catalog.
2. **Logical tree** – each SQL clause becomes a logical node (FROM → `SeqScan`, WHERE →
   `Filter`, GROUP BY → `Aggregate`, etc.).
3. **Physical selection** – `PhysicalPlanner` picks concrete algorithms (sequential scan,
   index scan, nested-loop join, sort, limit …). Because every physical node implements
   `VolcanoExecutor`, the execution engine can pull tuples immediately.

---

## Interactions

- **SQL front-end** – provides the AST; helper traits (`NormalizedIdent`, etc.) keep name
  resolution consistent.
- **Catalog** – `PlannerContext` relies on it to confirm table/index existence and fetch
  schemas.
- **Optimizer** – operates purely on `LogicalPlan`; the planner must emit clean,
  traversable trees so rules can fire.
- **Execution** – physical nodes carry `TableReference`, `SchemaRef`, and hints that the
  execution engine passes to the storage layer.

---

## Teaching Ideas

- Implement a new logical operator (e.g., `LogicalDistinct`) and add the corresponding
  physical operator to trace the full lifecycle.
- Experiment with early projection inside the logical plan and observe its impact on
  downstream operators.
- Use `pretty_format_logical_plan`/`physical_plan` dumps to visualise rewrites before and
  after optimizer passes.

---

Further reading: [The Lifecycle of a Query](../plan/lifecycle.md)
