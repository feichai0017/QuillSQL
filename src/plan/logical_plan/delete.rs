use crate::catalog::SchemaRef;
use crate::expression::Expr;
use crate::utils::table_ref::TableReference;

#[derive(derive_new::new, Debug, Clone)]
pub struct Delete {
    /// Target table reference
    pub table: TableReference,
    /// Cached schema for the table heap
    pub table_schema: SchemaRef,
    /// Optional predicate bound during planning
    pub selection: Option<Expr>,
}

impl std::fmt::Display for Delete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Delete: {}", self.table)
    }
}
