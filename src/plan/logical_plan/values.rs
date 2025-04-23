use crate::catalog::SchemaRef;
use crate::expression::Expr;

#[derive(derive_new::new, Debug, Clone)]
pub struct Values {
    pub schema: SchemaRef,
    pub values: Vec<Vec<Expr>>,
}

impl std::fmt::Display for Values {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Values")
    }
}
