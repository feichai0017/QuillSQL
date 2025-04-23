use crate::expression::Expr;
use crate::plan::logical_plan::LogicalPlan;
use std::sync::Arc;

#[derive(derive_new::new, Debug, Clone)]
pub struct Filter {
    /// The predicate expression, which must have Boolean type.
    pub predicate: Expr,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
}

impl std::fmt::Display for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Filter: {}", self.predicate)
    }
}
