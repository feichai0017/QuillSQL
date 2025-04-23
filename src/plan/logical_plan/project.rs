use crate::catalog::SchemaRef;
use crate::expression::Expr;
use crate::plan::logical_plan::LogicalPlan;
use std::sync::Arc;

#[derive(derive_new::new, Debug, Clone)]
pub struct Project {
    pub exprs: Vec<Expr>,
    pub input: Arc<LogicalPlan>,
    pub schema: SchemaRef,
}

impl std::fmt::Display for Project {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Project: {}",
            self.exprs
                .iter()
                .map(|e| format!("{e}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}
