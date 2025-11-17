mod aggregate;
mod analyze;
mod create_index;
mod create_table;
mod delete;
mod drop_index;
mod drop_table;
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
pub use analyze::Analyze;
pub use create_index::CreateIndex;
pub use create_table::CreateTable;
pub use delete::Delete;
pub use drop_index::DropIndex;
pub use drop_table::DropTable;
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
    SchemaRef, DELETE_OUTPUT_SCHEMA_REF, EMPTY_SCHEMA_REF, INSERT_OUTPUT_SCHEMA_REF,
    UPDATE_OUTPUT_SCHEMA_REF,
};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::transaction::IsolationLevel;
use sqlparser::ast::{TransactionAccessMode, TransactionMode};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
    DropTable(DropTable),
    DropIndex(DropIndex),
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
    Delete(Delete),
    Analyze(Analyze),
    BeginTransaction(TransactionModes),
    CommitTransaction,
    RollbackTransaction,
    SetTransaction {
        scope: TransactionScope,
        modes: TransactionModes,
    },
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
            LogicalPlan::Delete(_) => &DELETE_OUTPUT_SCHEMA_REF,
            LogicalPlan::DropTable(_) => &EMPTY_SCHEMA_REF,
            LogicalPlan::DropIndex(_) => &EMPTY_SCHEMA_REF,
            LogicalPlan::Analyze(_) => &EMPTY_SCHEMA_REF,
            LogicalPlan::BeginTransaction(_)
            | LogicalPlan::CommitTransaction
            | LogicalPlan::RollbackTransaction
            | LogicalPlan::SetTransaction { .. } => &EMPTY_SCHEMA_REF,
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
            | LogicalPlan::DropTable(_)
            | LogicalPlan::DropIndex(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Update(_)
            | LogicalPlan::Delete(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::BeginTransaction(_)
            | LogicalPlan::CommitTransaction
            | LogicalPlan::RollbackTransaction
            | LogicalPlan::SetTransaction { .. } => vec![],
        }
    }

    pub fn with_new_inputs(&self, inputs: &[LogicalPlan]) -> QuillSQLResult<LogicalPlan> {
        match self {
            LogicalPlan::Filter(Filter { predicate, .. }) => Ok(LogicalPlan::Filter(Filter {
                predicate: predicate.clone(),
                input: expect_single_input(inputs)?,
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
                input: expect_single_input(inputs)?,
            })),
            LogicalPlan::Join(Join {
                join_type,
                condition,
                schema,
                ..
            }) => {
                let (left, right) = expect_two_inputs(inputs)?;
                Ok(LogicalPlan::Join(Join {
                    left,
                    right,
                    join_type: *join_type,
                    condition: condition.clone(),
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::Limit(Limit { limit, offset, .. }) => Ok(LogicalPlan::Limit(Limit {
                limit: *limit,
                offset: *offset,
                input: expect_single_input(inputs)?,
            })),
            LogicalPlan::Project(Project { exprs, schema, .. }) => {
                Ok(LogicalPlan::Project(Project {
                    exprs: exprs.clone(),
                    schema: schema.clone(),
                    input: expect_single_input(inputs)?,
                }))
            }
            LogicalPlan::Sort(Sort {
                order_by, limit, ..
            }) => Ok(LogicalPlan::Sort(Sort {
                order_by: order_by.clone(),
                limit: *limit,
                input: expect_single_input(inputs)?,
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
                input: expect_single_input(inputs)?,
            })),
            LogicalPlan::CreateTable(_)
            | LogicalPlan::CreateIndex(_)
            | LogicalPlan::DropTable(_)
            | LogicalPlan::DropIndex(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Update(_)
            | LogicalPlan::Delete(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::BeginTransaction(_)
            | LogicalPlan::CommitTransaction
            | LogicalPlan::RollbackTransaction
            | LogicalPlan::SetTransaction { .. } => Ok(self.clone()),
        }
    }
}

fn expect_single_input(inputs: &[LogicalPlan]) -> QuillSQLResult<Arc<LogicalPlan>> {
    expect_input_at(inputs, 0, "should have at least one input")
}

fn expect_two_inputs(
    inputs: &[LogicalPlan],
) -> QuillSQLResult<(Arc<LogicalPlan>, Arc<LogicalPlan>)> {
    let left = expect_input_at(inputs, 0, "should have at least two inputs")?;
    let right = expect_input_at(inputs, 1, "should have at least two inputs")?;
    Ok((left, right))
}

fn expect_input_at(
    inputs: &[LogicalPlan],
    index: usize,
    expectation: &str,
) -> QuillSQLResult<Arc<LogicalPlan>> {
    inputs
        .get(index)
        .cloned()
        .map(Arc::new)
        .ok_or_else(|| QuillSQLError::Internal(format!("inputs {:?} {}", inputs, expectation)))
}

impl std::fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalPlan::CreateTable(v) => write!(f, "{v}"),
            LogicalPlan::CreateIndex(v) => write!(f, "{v}"),
            LogicalPlan::DropTable(v) => write!(f, "{v}"),
            LogicalPlan::DropIndex(v) => write!(f, "{v}"),
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
            LogicalPlan::Delete(v) => write!(f, "{v}"),
            LogicalPlan::Analyze(v) => write!(f, "Analyze: {}", v.table),
            LogicalPlan::BeginTransaction(_) => write!(f, "BeginTransaction"),
            LogicalPlan::CommitTransaction => write!(f, "CommitTransaction"),
            LogicalPlan::RollbackTransaction => write!(f, "RollbackTransaction"),
            LogicalPlan::SetTransaction { .. } => write!(f, "SetTransaction"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TransactionScope {
    Session,
    Transaction,
}

#[derive(Debug, Clone, Default)]
pub struct TransactionModes {
    pub isolation_level: Option<IsolationLevel>,
    pub access_mode: Option<TransactionAccessMode>,
}

impl TransactionModes {
    pub fn from_modes(modes: &[TransactionMode]) -> Self {
        let mut result = TransactionModes::default();
        for mode in modes {
            match mode {
                TransactionMode::IsolationLevel(level) => {
                    let isolation = match level {
                        sqlparser::ast::TransactionIsolationLevel::ReadUncommitted => {
                            IsolationLevel::ReadUncommitted
                        }
                        sqlparser::ast::TransactionIsolationLevel::ReadCommitted => {
                            IsolationLevel::ReadCommitted
                        }
                        sqlparser::ast::TransactionIsolationLevel::RepeatableRead => {
                            IsolationLevel::RepeatableRead
                        }
                        sqlparser::ast::TransactionIsolationLevel::Serializable => {
                            IsolationLevel::Serializable
                        }
                    };
                    result.isolation_level = Some(isolation);
                }
                TransactionMode::AccessMode(mode) => {
                    result.access_mode = Some(*mode);
                }
            }
        }
        result
    }

    pub fn unwrap_effective_isolation(&self, fallback: IsolationLevel) -> IsolationLevel {
        self.isolation_level.unwrap_or(fallback)
    }

    pub fn unwrap_effective_access_mode(
        &self,
        fallback: TransactionAccessMode,
    ) -> TransactionAccessMode {
        self.access_mode.unwrap_or(fallback)
    }
}
