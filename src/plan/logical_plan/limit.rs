use crate::plan::logical_plan::LogicalPlan;
use std::sync::Arc;

#[derive(derive_new::new, Debug, Clone)]
pub struct Limit {
    pub limit: Option<usize>,
    pub offset: usize,
    pub input: Arc<LogicalPlan>,
}

impl std::fmt::Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Limit: {}, offset: {}",
            self.limit.map_or("None".to_string(), |v| v.to_string()),
            self.offset
        )
    }
}
