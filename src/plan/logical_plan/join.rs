use crate::catalog::SchemaRef;
use crate::expression::Expr;
use crate::plan::logical_plan::LogicalPlan;
use std::sync::Arc;

#[derive(derive_new::new, Debug, Clone)]
pub struct Join {
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub join_type: JoinType,
    pub condition: Option<Expr>,
    pub schema: SchemaRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    // select * from x inner join y on ...
    Inner,
    // select * from x left (outer) join y on ...
    LeftOuter,
    // select * from x right (outer) join y on ...
    RightOuter,
    // select * from x full (outer) join y on ...
    FullOuter,
    // select * from x, y
    // select * from x cross join y
    Cross,
}

impl std::fmt::Display for Join {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} Join", self.join_type)?;
        if let Some(condition) = self.condition.as_ref() {
            write!(f, ": On {condition}")?;
        }
        Ok(())
    }
}

impl std::fmt::Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
