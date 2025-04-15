use super::types::{DataType, Value};

#[derive(Debug, PartialEq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
}
