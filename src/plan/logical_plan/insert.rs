use crate::catalog::SchemaRef;
use crate::plan::logical_plan::LogicalPlan;
use crate::utils::table_ref::TableReference;
use std::sync::Arc;

#[derive(derive_new::new, Debug, Clone)]
pub struct Insert {
    pub table: TableReference,
    pub table_schema: SchemaRef,
    pub projected_schema: SchemaRef,
    pub input: Arc<LogicalPlan>,
}

impl std::fmt::Display for Insert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Insert: {} ({})",
            self.table,
            self.projected_schema
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}
