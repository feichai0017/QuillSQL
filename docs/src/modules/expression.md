# Expression & Scalar Evaluation

The expression subsystem (`src/expression/`) powers column computations, predicates, and
UPDATE assignments. It keeps expression trees approachable while demonstrating how they
are evaluated during execution.

---

## Responsibilities

- Store planner-produced expression trees (`Expr`) in a serializable, traversable enum.
- Bind column references, constants, and built-in functions.
- Evaluate expressions against `Tuple`s at runtime, yielding `ScalarValue`.
- Provide type inference and casting so arithmetic/comparison operators remain well-typed.

---

## Directory Layout

| Path | Description | Key Types |
| ---- | ----------- | --------- |
| `mod.rs` | Public API and core enum. | `Expr`, `ExprTrait` |
| `scalar.rs` | Runtime scalar representation + conversions. | `ScalarValue`, `DataType` |
| `binder.rs` | Helpers for the planner/SQL binder. | `BoundExpr` |

---

## Concepts

### Expr Enum
Expresses column refs, literals, comparisons, logical ops, arithmetic, and function
invocations. Each variant implements `ExprTrait::evaluate(&self, tuple)` and returns a
`ScalarValue`.

### ScalarValue
Unified runtime value across types (int, bigint, bool, decimal, varchar, …). Includes
`cast_to(DataType)` so results can be coerced to the target column type before writes.

### Type Inference
Planner code invokes `Expr::data_type(schema)` to predict result types. Execution then
casts when needed—e.g., `UPDATE t SET a = b + 1` uses the column’s declared type for `a`.

---

## Interactions

- **Planner** – builds `Expr` trees with bound columns; execution reuses them verbatim.
- **ExecutionContext** – exposes `eval_expr` and `eval_predicate`, wrapping expression
  evaluation plus boolean coercion (`NULL` becomes false for predicates).
- **Optimizer** – rules like constant folding traverse `Expr` trees and reuse
  `ScalarValue` arithmetic helpers.

---

## Teaching Ideas

- Add a simple built-in function (`length(expr)`) to follow the pipeline from parsing to
  evaluation.
- Implement short-circuiting or full three-valued boolean logic and validate with
  sqllogictest.
- Instrument `Expr::evaluate` with tracing to visualise expression evaluation inside
  physical operators.
