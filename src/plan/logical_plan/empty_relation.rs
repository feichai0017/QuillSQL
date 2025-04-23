use crate::catalog::SchemaRef;

#[derive(Debug, Clone)]
pub struct EmptyRelation {
    /// Whether to produce a placeholder row
    pub produce_one_row: bool,
    /// The schema description of the output
    pub schema: SchemaRef,
}

impl std::fmt::Display for EmptyRelation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EmptyRelation")
    }
}
