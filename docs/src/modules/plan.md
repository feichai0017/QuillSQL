# Query Plan

The Query Planner is the "brain" of the database. It is responsible for taking a declarative SQL query string (which describes *what* data to retrieve) and converting it into a highly optimized, imperative plan (which describes *how* to retrieve that data).

This process is one of the most complex and important aspects of a relational database system.

## Core Concepts

- **AST (Abstract Syntax Tree)**: The raw SQL text is first parsed into a tree structure that represents the syntax of the query.

- **Logical Plan**: The AST is then converted into a logical plan. This is a tree of relational algebra operators (e.g., `Filter`, `Projection`, `Join`) that describes the logical steps required to fulfill the query, independent of any specific algorithm or data layout.

- **Physical Plan**: The logical plan is then converted into a physical plan. This plan consists of concrete operators (or "iterators") that implement specific algorithms (e.g., `NestedLoopJoin`, `SeqScan`). This is the plan that is actually executed.

This section contains:

- **[The Lifecycle of a Query](./../plan/lifecycle.md)**: A deep dive into the journey from SQL string to executable physical plan.