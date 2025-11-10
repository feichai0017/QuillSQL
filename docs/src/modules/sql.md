# SQL Front-End

The SQL front-end lives in `src/sql/`. It turns raw UTF-8 query text into the abstract
syntax trees (ASTs) consumed by planning, while layering Quill-specific name handling
and diagnostics on top of [`sqlparser`](https://docs.rs/sqlparser).

---

## Responsibilities

- Parse SQL text into `sqlparser::ast::Statement` values.
- Record precise spans so error messages can highlight the exact byte range.
- Normalise identifiers (case folding, quoted names, multi-part paths).
- Provide helper traits so the logical planner can lower AST nodes without duplicating
  syntax checks.

---

## Directory Layout

| Path | Purpose | Key Types |
| ---- | ------- | --------- |
| `lexer.rs` | Token helpers that preserve offsets. | `Token`, `TokenExt` |
| `parser.rs` | Single entry point used across the codebase. | `parse_sql`, `SqlInput` |
| `ast/mod.rs` | Planner-facing helpers. | `NormalizedIdent`, `ObjectNameExt` |
| `error.rs` | Span-aware parser errors. | `SqlError`, `SqlSpan` |

---

## Parsing Pipeline

1. **Lexing** – wrap sqlparser’s lexer so every token keeps start/end offsets.
2. **AST generation** – invoke sqlparser to produce standard `Statement` structs.
3. **Normalisation** – convert identifiers into `NormalizedIdent`, deal with schema
   qualifiers, and build pieces of `TableReference`.
4. **Planner bridge** – traits like `ColumnRefExt` expose methods such as `relation()` or
   `column()` so `LogicalPlanner` can treat different SQL syntaxes uniformly.

---

## Interactions

- **Logical planner** consumes the AST directly and relies on helper traits from this
  module to convert identifiers into catalog references.
- **Database / Session** catch `SqlError` values, so both CLI and HTTP front-ends show
  consistent caret diagnostics.
- **Tests** (`tests/sql_example/*.slt`, `tests/sql_parser.rs`) assert on parser output and
  error strings to keep teaching feedback stable.

---

## Implementation Notes

- `SqlSpan` stores byte offsets, which makes it trivial to slice the original SQL and
  render highlighted errors.
- Extended statements (e.g., `EXPLAIN`, `BEGIN TRANSACTION`) show how to Layer
  Quill-specific syntax without forking sqlparser entirely.
- We avoid desugaring at this stage so students can trace SQL → AST → logical plan step
  by step.

---

## Teaching Ideas

- Add a new statement (`CREATE VIEW`, `ALTER TABLE ...`) and follow the AST through the
  pipeline.
- Improve error hints (“Did you forget FROM?”) to see how better diagnostics aid users.
- Write fuzz tests that round-trip SQL → AST → SQL to discuss parser determinism.
