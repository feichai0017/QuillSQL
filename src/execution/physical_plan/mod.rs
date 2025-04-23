mod aggregate;
mod create_index;
mod create_table;
mod empty;
mod filter;
mod index_scan;
mod insert;
mod limit;
mod nested_loop_join;
mod project;
mod seq_scan;
mod sort;
mod update;
mod values;

pub use aggregate::PhysicalAggregate;
pub use create_index::PhysicalCreateIndex;
pub use create_table::PhysicalCreateTable;
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
    execution::{ExecutionContext, VolcanoExecutor},
    storage::tuple::Tuple,
    error::QuillSQLResult,
};

#[derive(Debug)]
pub enum PhysicalPlan {
    Empty(PhysicalEmpty),
    CreateTable(PhysicalCreateTable),
    CreateIndex(PhysicalCreateIndex),
    Project(PhysicalProject),
    Filter(PhysicalFilter),
    SeqScan(PhysicalSeqScan),
    IndexScan(PhysicalIndexScan),
    Limit(PhysicalLimit),
    Insert(PhysicalInsert),
    Values(PhysicalValues),
    NestedLoopJoin(PhysicalNestedLoopJoin),
    Sort(PhysicalSort),
    Aggregate(PhysicalAggregate),
    Update(PhysicalUpdate),
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
            | PhysicalPlan::SeqScan(_)
            | PhysicalPlan::IndexScan(_)
            | PhysicalPlan::Update(_)
            | PhysicalPlan::Values(_) => vec![],
        }
    }
}

impl VolcanoExecutor for PhysicalPlan {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        match self {
            PhysicalPlan::Empty(op) => op.init(context),
            PhysicalPlan::CreateTable(op) => op.init(context),
            PhysicalPlan::CreateIndex(op) => op.init(context),
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
        }
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        match self {
            PhysicalPlan::Empty(op) => op.next(context),
            PhysicalPlan::CreateTable(op) => op.next(context),
            PhysicalPlan::CreateIndex(op) => op.next(context),
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
        }
    }

    fn output_schema(&self) -> SchemaRef {
        match self {
            Self::Empty(op) => op.output_schema(),
            Self::CreateTable(op) => op.output_schema(),
            Self::CreateIndex(op) => op.output_schema(),
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
        }
    }
}

impl std::fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty(op) => write!(f, "{op}"),
            Self::CreateTable(op) => write!(f, "{op}"),
            Self::CreateIndex(op) => write!(f, "{op}"),
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
        }
    }
}
