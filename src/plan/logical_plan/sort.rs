use crate::expression::Expr;
use crate::plan::logical_plan::LogicalPlan;
use std::sync::Arc;

#[derive(derive_new::new, Debug, Clone)]
pub struct Sort {
    pub order_by: Vec<OrderByExpr>,
    pub input: Arc<LogicalPlan>,
    pub limit: Option<usize>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct OrderByExpr {
    /// The expression to sort on
    pub expr: Box<Expr>,
    /// The direction of the sort
    pub asc: bool,
    /// Whether to put Nulls before all other data values
    pub nulls_first: bool,
}

impl std::fmt::Display for OrderByExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.expr,
            if self.asc { "ASC" } else { "DESC" },
            if self.nulls_first {
                "NULLS FIRST"
            } else {
                "NULLS LAST"
            }
        )
    }
}

impl std::fmt::Display for Sort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Sort: {}",
            self.order_by
                .iter()
                .map(|e| format!("{e}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}
