use crate::catalog::SchemaRef;
use crate::expression::Expr;
use crate::utils::table_ref::TableReference;
use std::collections::HashMap;

#[derive(derive_new::new, Debug, Clone)]
pub struct Update {
    pub table: TableReference,
    pub table_schema: SchemaRef,
    pub assignments: HashMap<String, Expr>,
    pub selection: Option<Expr>,
}

impl std::fmt::Display for Update {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Update: {}", self.table,)
    }
}
