use std::sync::OnceLock;

use crate::storage::engine::TableBinding;
use crate::utils::table_ref::TableReference;

mod aggregate;
mod analyze;
mod create_index;
mod create_table;
mod delete;
mod drop_index;
mod drop_table;
mod empty;
mod filter;
mod index_scan;
mod insert;
mod limit;
mod nested_loop_join;
mod project;
mod scan;
mod seq_scan;
mod sort;
mod update;
mod values;

pub use aggregate::PhysicalAggregate;
pub use analyze::PhysicalAnalyze;
pub use create_index::PhysicalCreateIndex;
pub use create_table::PhysicalCreateTable;
pub use delete::PhysicalDelete;
pub use drop_index::PhysicalDropIndex;
pub use drop_table::PhysicalDropTable;
pub use empty::PhysicalEmpty;
pub use filter::PhysicalFilter;
pub use index_scan::PhysicalIndexScan;
pub use insert::PhysicalInsert;
pub use limit::PhysicalLimit;
pub use nested_loop_join::PhysicalNestedLoopJoin;
pub use project::PhysicalProject;
pub use seq_scan::PhysicalSeqScan;
pub use sort::PhysicalSort;
pub use update::PhysicalUpdate;
pub use values::PhysicalValues;

use crate::catalog::SchemaRef;
use crate::{
    error::QuillSQLResult,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::tuple::Tuple,
};

#[derive(Debug)]
pub enum PhysicalPlan {
    Empty(PhysicalEmpty),
    Values(PhysicalValues),
    SeqScan(PhysicalSeqScan),
    IndexScan(PhysicalIndexScan),
    Limit(PhysicalLimit),
    Sort(PhysicalSort),
    Update(PhysicalUpdate),
    Delete(PhysicalDelete),
    Insert(PhysicalInsert),
    Project(PhysicalProject),
    Filter(PhysicalFilter),
    NestedLoopJoin(PhysicalNestedLoopJoin),
    Aggregate(PhysicalAggregate),
    Analyze(PhysicalAnalyze),
    CreateTable(PhysicalCreateTable),
    CreateIndex(PhysicalCreateIndex),
    DropTable(PhysicalDropTable),
    DropIndex(PhysicalDropIndex),
}

impl PhysicalPlan {
    pub fn inputs(&self) -> Vec<&PhysicalPlan> {
        match self {
            PhysicalPlan::Project(PhysicalProject { input, .. }) => vec![input],
            PhysicalPlan::Filter(PhysicalFilter { input, .. }) => vec![input],
            PhysicalPlan::Limit(PhysicalLimit { input, .. }) => vec![input],
            PhysicalPlan::Insert(PhysicalInsert { input, .. }) => vec![input],
            PhysicalPlan::NestedLoopJoin(PhysicalNestedLoopJoin {
                left_input,
                right_input,
                ..
            }) => vec![left_input, right_input],
            PhysicalPlan::Sort(PhysicalSort { input, .. }) => vec![input],
            PhysicalPlan::Aggregate(PhysicalAggregate { input, .. }) => vec![input],
            PhysicalPlan::Empty(_)
            | PhysicalPlan::CreateTable(_)
            | PhysicalPlan::CreateIndex(_)
            | PhysicalPlan::DropTable(_)
            | PhysicalPlan::DropIndex(_)
            | PhysicalPlan::SeqScan(_)
            | PhysicalPlan::IndexScan(_)
            | PhysicalPlan::Update(_)
            | PhysicalPlan::Delete(_)
            | PhysicalPlan::Values(_)
            | PhysicalPlan::Analyze(_) => vec![],
        }
    }
}

impl VolcanoExecutor for PhysicalPlan {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        match self {
            PhysicalPlan::Empty(op) => op.init(context),
            PhysicalPlan::CreateTable(op) => op.init(context),
            PhysicalPlan::CreateIndex(op) => op.init(context),
            PhysicalPlan::DropTable(op) => op.init(context),
            PhysicalPlan::DropIndex(op) => op.init(context),
            PhysicalPlan::Insert(op) => op.init(context),
            PhysicalPlan::Values(op) => op.init(context),
            PhysicalPlan::Project(op) => op.init(context),
            PhysicalPlan::Filter(op) => op.init(context),
            PhysicalPlan::SeqScan(op) => op.init(context),
            PhysicalPlan::IndexScan(op) => op.init(context),
            PhysicalPlan::Limit(op) => op.init(context),
            PhysicalPlan::NestedLoopJoin(op) => op.init(context),
            PhysicalPlan::Sort(op) => op.init(context),
            PhysicalPlan::Aggregate(op) => op.init(context),
            PhysicalPlan::Update(op) => op.init(context),
            PhysicalPlan::Delete(op) => op.init(context),
            PhysicalPlan::Analyze(op) => op.init(context),
        }
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        match self {
            PhysicalPlan::Empty(op) => op.next(context),
            PhysicalPlan::CreateTable(op) => op.next(context),
            PhysicalPlan::CreateIndex(op) => op.next(context),
            PhysicalPlan::DropTable(op) => op.next(context),
            PhysicalPlan::DropIndex(op) => op.next(context),
            PhysicalPlan::Insert(op) => op.next(context),
            PhysicalPlan::Values(op) => op.next(context),
            PhysicalPlan::Project(op) => op.next(context),
            PhysicalPlan::Filter(op) => op.next(context),
            PhysicalPlan::SeqScan(op) => op.next(context),
            PhysicalPlan::IndexScan(op) => op.next(context),
            PhysicalPlan::Limit(op) => op.next(context),
            PhysicalPlan::NestedLoopJoin(op) => op.next(context),
            PhysicalPlan::Sort(op) => op.next(context),
            PhysicalPlan::Aggregate(op) => op.next(context),
            PhysicalPlan::Update(op) => op.next(context),
            PhysicalPlan::Delete(op) => op.next(context),
            PhysicalPlan::Analyze(op) => op.next(context),
        }
    }

    fn output_schema(&self) -> SchemaRef {
        match self {
            Self::Empty(op) => op.output_schema(),
            Self::CreateTable(op) => op.output_schema(),
            Self::CreateIndex(op) => op.output_schema(),
            Self::DropTable(op) => op.output_schema(),
            Self::DropIndex(op) => op.output_schema(),
            Self::Insert(op) => op.output_schema(),
            Self::Values(op) => op.output_schema(),
            Self::Project(op) => op.output_schema(),
            Self::Filter(op) => op.output_schema(),
            Self::SeqScan(op) => op.output_schema(),
            Self::IndexScan(op) => op.output_schema(),
            Self::Limit(op) => op.output_schema(),
            Self::NestedLoopJoin(op) => op.output_schema(),
            Self::Sort(op) => op.output_schema(),
            Self::Aggregate(op) => op.output_schema(),
            Self::Update(op) => op.output_schema(),
            Self::Delete(op) => op.output_schema(),
            Self::Analyze(op) => op.output_schema(),
        }
    }
}

impl std::fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty(op) => write!(f, "{op}"),
            Self::CreateTable(op) => write!(f, "{op}"),
            Self::CreateIndex(op) => write!(f, "{op}"),
            Self::DropTable(op) => write!(f, "{op}"),
            Self::DropIndex(op) => write!(f, "{op}"),
            Self::Insert(op) => write!(f, "{op}"),
            Self::Values(op) => write!(f, "{op}"),
            Self::Project(op) => write!(f, "{op}"),
            Self::Filter(op) => write!(f, "{op}"),
            Self::SeqScan(op) => write!(f, "{op}"),
            Self::IndexScan(op) => write!(f, "{op}"),
            Self::Limit(op) => write!(f, "{op}"),
            Self::NestedLoopJoin(op) => write!(f, "{op}"),
            Self::Sort(op) => write!(f, "{op}"),
            Self::Aggregate(op) => write!(f, "{op}"),
            Self::Update(op) => write!(f, "{op}"),
            Self::Delete(op) => write!(f, "{op}"),
            Self::Analyze(op) => write!(f, "{op}"),
        }
    }
}

pub(crate) fn resolve_table_binding<'a>(
    cache: &'a OnceLock<TableBinding>,
    context: &mut ExecutionContext,
    table: &TableReference,
) -> QuillSQLResult<&'a TableBinding> {
    if cache.get().is_none() {
        let binding = context.table(table)?;
        let _ = cache.set(binding);
    }
    Ok(cache.get().expect("table binding not initialized"))
}

pub(crate) fn stream_not_ready(op: &str) -> crate::error::QuillSQLError {
    crate::error::QuillSQLError::Execution(format!("{op} stream not initialized"))
}
