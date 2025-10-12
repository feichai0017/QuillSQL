# Query Optimizer

A naive, direct translation of a SQL query into an execution plan is often dramatically inefficient. The **Query Optimizer** is a critical stage in the query processing pipeline that rearranges and transforms the initial logical plan into an equivalent, but far cheaper, plan to execute.

## Core Concepts

- **Rule-Based Optimization**: QuillSQL uses a rule-based optimizer. It applies a series of pre-defined, heuristic rules to the logical plan to improve it. This is in contrast to a cost-based optimizer, which would estimate the "cost" of many possible plans and choose the cheapest one.

- **Logical Transformations**: The key insight is that many different logical plans can produce the exact same result. For example, filtering data before a join is usually much faster than joining two large tables and filtering the result, but the final output is identical. The optimizer's job is to find and apply these beneficial transformations.

This section contains:

- **[Rule-Based Optimization](./../optimizer/rules.md)**: A deep dive into how the rule-based optimizer works, using the `PushDownLimit` rule as a concrete example.