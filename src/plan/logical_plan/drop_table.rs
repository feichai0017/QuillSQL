use crate::utils::table_ref::TableReference;

#[derive(Debug, Clone)]
pub struct DropTable {
    pub name: TableReference,
    pub if_exists: bool,
}

impl std::fmt::Display for DropTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.if_exists {
            write!(f, "DropTable IF EXISTS: {}", self.name)
        } else {
            write!(f, "DropTable: {}", self.name)
        }
    }
}
