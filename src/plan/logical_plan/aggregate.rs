use crate::catalog::SchemaRef;
use crate::expression::Expr;
use crate::plan::logical_plan::LogicalPlan;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Aggregate {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// Grouping expressions
    pub group_exprs: Vec<Expr>,
    /// Aggregate expressions
    pub aggr_exprs: Vec<Expr>,
    /// The schema description of the aggregate output
    pub schema: SchemaRef,
}

impl std::fmt::Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Aggregate")
    }
}
