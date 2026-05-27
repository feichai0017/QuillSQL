use crate::catalog::Column;
use crate::utils::table_ref::TableReference;

#[derive(Debug, Clone)]
pub struct CreateTable {
    pub name: TableReference,
    pub columns: Vec<Column>,
    pub if_not_exists: bool,
}

impl std::fmt::Display for CreateTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CreateTable: {}", self.name)
    }
}
