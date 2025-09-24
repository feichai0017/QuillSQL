use crate::catalog::SchemaRef;
use crate::expression::Expr;
use crate::utils::table_ref::TableReference;

#[derive(derive_new::new, Debug, Clone)]
pub struct TableScan {
    pub table_ref: TableReference,
    pub table_schema: SchemaRef,
    pub filters: Vec<Expr>,
    pub limit: Option<usize>,
    /// Optional hint: prefer streaming seqscan (bypass buffer pool)
    pub streaming_hint: Option<bool>,
}

impl std::fmt::Display for TableScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableScan: {}", self.table_ref)
    }
}
