mod aggregate;
mod create_index;
mod create_table;
mod empty_relation;
mod filter;
mod insert;
mod join;
mod limit;
mod project;
mod sort;
mod table_scan;
mod update;
mod util;
mod values;

pub use aggregate::Aggregate;
pub use create_index::CreateIndex;
pub use create_table::CreateTable;
pub use empty_relation::EmptyRelation;
pub use filter::Filter;
pub use insert::Insert;
pub use join::{Join, JoinType};
pub use limit::Limit;
pub use project::Project;
pub use sort::{OrderByExpr, Sort};
pub use table_scan::TableScan;
pub use update::Update;
pub use util::*;
pub use values::Values;

use crate::catalog::{
    SchemaRef, EMPTY_SCHEMA_REF, INSERT_OUTPUT_SCHEMA_REF, UPDATE_OUTPUT_SCHEMA_REF,
};
use crate::error::{QuillSQLError, QuillSQLResult};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
    Filter(Filter),
    Insert(Insert),
    Join(Join),
    Limit(Limit),
    Project(Project),
    TableScan(TableScan),
    Sort(Sort),
    Values(Values),
    EmptyRelation(EmptyRelation),
    Aggregate(Aggregate),
    Update(Update),
}

impl LogicalPlan {
    pub fn schema(&self) -> &SchemaRef {
        match self {
            LogicalPlan::CreateTable(_) => &EMPTY_SCHEMA_REF,
            LogicalPlan::CreateIndex(_) => &EMPTY_SCHEMA_REF,
            LogicalPlan::Filter(Filter { input, .. }) => input.schema(),
            LogicalPlan::Insert(_) => &INSERT_OUTPUT_SCHEMA_REF,
            LogicalPlan::Join(Join { schema, .. }) => schema,
            LogicalPlan::Limit(Limit { input, .. }) => input.schema(),
            LogicalPlan::Project(Project { schema, .. }) => schema,
            LogicalPlan::TableScan(TableScan { table_schema, .. }) => table_schema,
            LogicalPlan::Sort(Sort { input, .. }) => input.schema(),
            LogicalPlan::Values(Values { schema, .. }) => schema,
            LogicalPlan::EmptyRelation(EmptyRelation { schema, .. }) => schema,
            LogicalPlan::Aggregate(Aggregate { schema, .. }) => schema,
            LogicalPlan::Update(_) => &UPDATE_OUTPUT_SCHEMA_REF,
        }
    }

    pub fn inputs(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Filter(Filter { input, .. }) => vec![input],
            LogicalPlan::Insert(Insert { input, .. }) => vec![input],
            LogicalPlan::Join(Join { left, right, .. }) => vec![left, right],
            LogicalPlan::Limit(Limit { input, .. }) => vec![input],
            LogicalPlan::Project(Project { input, .. }) => vec![input],
            LogicalPlan::Sort(Sort { input, .. }) => vec![input],
            LogicalPlan::Aggregate(Aggregate { input, .. }) => vec![input],
            LogicalPlan::CreateTable(_)
            | LogicalPlan::CreateIndex(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Update(_)
            | LogicalPlan::EmptyRelation(_) => vec![],
        }
    }

    pub fn with_new_inputs(&self, inputs: &[LogicalPlan]) -> QuillSQLResult<LogicalPlan> {
        match self {
            LogicalPlan::Filter(Filter { predicate, .. }) => Ok(LogicalPlan::Filter(Filter {
                predicate: predicate.clone(),
                input: Arc::new(
                    inputs
                        .first()
                        .ok_or_else(|| {
                            QuillSQLError::Internal(format!(
                                "inputs {:?} should have at least one",
                                inputs
                            ))
                        })?
                        .clone(),
                ),
            })),
            LogicalPlan::Insert(Insert {
                table,
                table_schema,
                projected_schema,
                ..
            }) => Ok(LogicalPlan::Insert(Insert {
                table: table.clone(),
                table_schema: table_schema.clone(),
                projected_schema: projected_schema.clone(),
                input: Arc::new(
                    inputs
                        .first()
                        .ok_or_else(|| {
                            QuillSQLError::Internal(format!(
                                "inputs {:?} should have at least one",
                                inputs
                            ))
                        })?
                        .clone(),
                ),
            })),
            LogicalPlan::Join(Join {
                join_type,
                condition,
                schema,
                ..
            }) => Ok(LogicalPlan::Join(Join {
                left: Arc::new(
                    inputs
                        .first()
                        .ok_or_else(|| {
                            QuillSQLError::Internal(format!(
                                "inputs {:?} should have at least two",
                                inputs
                            ))
                        })?
                        .clone(),
                ),
                right: Arc::new(
                    inputs
                        .first()
                        .ok_or_else(|| {
                            QuillSQLError::Internal(format!(
                                "inputs {:?} should have at least two",
                                inputs
                            ))
                        })?
                        .clone(),
                ),
                join_type: *join_type,
                condition: condition.clone(),
                schema: schema.clone(),
            })),
            LogicalPlan::Limit(Limit { limit, offset, .. }) => Ok(LogicalPlan::Limit(Limit {
                limit: *limit,
                offset: *offset,
                input: Arc::new(
                    inputs
                        .first()
                        .ok_or_else(|| {
                            QuillSQLError::Internal(format!(
                                "inputs {:?} should have at least one",
                                inputs
                            ))
                        })?
                        .clone(),
                ),
            })),
            LogicalPlan::Project(Project { exprs, schema, .. }) => {
                Ok(LogicalPlan::Project(Project {
                    exprs: exprs.clone(),
                    schema: schema.clone(),
                    input: Arc::new(
                        inputs
                            .first()
                            .ok_or_else(|| {
                                QuillSQLError::Internal(format!(
                                    "inputs {:?} should have at least one",
                                    inputs
                                ))
                            })?
                            .clone(),
                    ),
                }))
            }
            LogicalPlan::Sort(Sort {
                order_by, limit, ..
            }) => Ok(LogicalPlan::Sort(Sort {
                order_by: order_by.clone(),
                limit: *limit,
                input: Arc::new(
                    inputs
                        .first()
                        .ok_or_else(|| {
                            QuillSQLError::Internal(format!(
                                "inputs {:?} should have at least one",
                                inputs
                            ))
                        })?
                        .clone(),
                ),
            })),
            LogicalPlan::Aggregate(Aggregate {
                group_exprs,
                aggr_exprs,
                schema,
                ..
            }) => Ok(LogicalPlan::Aggregate(Aggregate {
                group_exprs: group_exprs.clone(),
                aggr_exprs: aggr_exprs.clone(),
                schema: schema.clone(),
                input: Arc::new(
                    inputs
                        .first()
                        .ok_or_else(|| {
                            QuillSQLError::Internal(format!(
                                "inputs {:?} should have at least one",
                                inputs
                            ))
                        })?
                        .clone(),
                ),
            })),
            LogicalPlan::CreateTable(_)
            | LogicalPlan::CreateIndex(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Update(_)
            | LogicalPlan::EmptyRelation(_) => Ok(self.clone()),
        }
    }
}

impl std::fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalPlan::CreateTable(v) => write!(f, "{v}"),
            LogicalPlan::CreateIndex(v) => write!(f, "{v}"),
            LogicalPlan::Filter(v) => write!(f, "{v}"),
            LogicalPlan::Insert(v) => write!(f, "{v}"),
            LogicalPlan::Join(v) => write!(f, "{v}"),
            LogicalPlan::Limit(v) => write!(f, "{v}"),
            LogicalPlan::Project(v) => write!(f, "{v}"),
            LogicalPlan::TableScan(v) => write!(f, "{v}"),
            LogicalPlan::Sort(v) => write!(f, "{v}"),
            LogicalPlan::Values(v) => write!(f, "{v}"),
            LogicalPlan::EmptyRelation(v) => write!(f, "{v}"),
            LogicalPlan::Aggregate(v) => write!(f, "{v}"),
            LogicalPlan::Update(v) => write!(f, "{v}"),
        }
    }
}
