use crate::utils::table_ref::TableReference;

#[derive(Debug, Clone)]
pub struct Analyze {
    pub table: TableReference,
}
