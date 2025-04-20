use std::fmt::Display;
use std::sync::Arc;

use derive_with::With;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

use super::types::{DataType, Row, Value};

pub type SchemaRef = Arc<Schema>;
pub type ColumnRef = Arc<Column>;

/// Represents the schema of a row (potentially intermediate result) during query processing.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Schema {
    pub columns: Vec<ColumnRef>,
}


impl Schema {
    /// Creates a new Schema from a vector of owned Columns.
    pub fn new(columns: Vec<Column>) -> Self {
        Self {
            columns: columns.into_iter().map(Arc::new).collect(),
        }
    }

    /// Creates a new Schema from a vector of ColumnRefs.
    /// Performs basic validation for duplicate column names (within the same relation if specified).
    pub fn new_with_refs(columns: Vec<ColumnRef>) -> Self {
        // Basic validation: check for duplicate unqualified names for now
        // A more robust check would consider the `relation` field.
        for i in 0..columns.len() {
            for j in i + 1..columns.len() {
                if columns[i].name == columns[j].name {
                    // In a real system, might error or handle qualified names differently
                    println!(
                        "WARN: Duplicate column name '{}' found in Schema construction.",
                        columns[i].name
                    );
                }
            }
        }
        Self { columns }
    }

    /// Creates an empty Schema.
    pub fn empty() -> Self {
        Self { columns: vec![] }
    }

    /// Returns the number of columns in the schema.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Finds the index of a column by its unqualified name.
    /// Returns an error if not found or if the name is ambiguous (present multiple times).
    pub fn index_of(&self, name: &str) -> Result<usize> {
        let mut found_index = None;
        let mut duplicate = false;
        for (idx, col) in self.columns.iter().enumerate() {
            if col.name == name {
                if found_index.is_some() {
                    duplicate = true;
                    break; // Found a duplicate
                }
                found_index = Some(idx);
            }
        }
        if duplicate {
            Err(Error::Internal(format!("Ambiguous column name: {}", name)))
        } else {
            found_index.ok_or_else(|| Error::Internal(format!("Column not found: {}", name)))
        }
    }

    pub fn try_merge(schemas: impl IntoIterator<Item = Self>) -> Result<Self> {
        let mut columns = Vec::new();
        for schema in schemas {
            columns.extend(schema.columns);
        }
        Ok(Self::new_with_refs(columns))
    }

    pub fn project(&self, indices: &[usize]) -> Result<Self> {
        let new_columns = indices
            .iter()
            .map(|i| self.column_with_index(*i))
            .collect::<Result<Vec<ColumnRef>>>()?;
        Ok(Schema::new_with_refs(new_columns))
    }

    pub fn column_with_name(&self, name: &str) -> Result<ColumnRef> {
        let index = self.index_of(name)?;
        Ok(self.columns[index].clone())
    }


     /// Gets a column reference by index.
     pub fn column_with_index(&self, index: usize) -> Result<ColumnRef> {
          self.columns
              .get(index)
              .cloned()
              .ok_or_else(|| Error::Internal(format!("Index out of bounds: {}", index)))
     }

}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    // 校验表的有效性
    pub fn validate(&self) -> Result<()> {
        // 校验是否有列信息
        if self.columns.is_empty() {
            return Err(Error::Internal(format!(
                "table {} has no columns",
                self.name
            )));
        }

        // 校验是否有主键
        match self.columns.iter().filter(|c| c.primary_key).count() {
            1 => {}
            0 => {
                return Err(Error::Internal(format!(
                    "No primary key for table {}",
                    self.name
                )))
            }
            _ => {
                return Err(Error::Internal(format!(
                    "Multiple primary keys for table {}",
                    self.name
                )))
            }
        }

        // 校验列信息
        for col in &self.columns {
            // 主键不能为空
            if col.primary_key && col.nullable {
                return Err(Error::Internal(format!(
                    "Primary key {} cannot be nullable in table{}",
                    col.name, self.name
                )));
            }
            // 校验默认值是否和列类型匹配
            if let Some(default_val) = &col.default {
                match default_val.datatype() {
                    Some(dt) => {
                        if dt != col.datatype {
                            return Err(Error::Internal(format!(
                                "Default value for column {} mismatch in table{}",
                                col.name, self.name
                            )));
                        }
                    }
                    None => {}
                }
            }
        }

        Ok(())
    }

    pub fn get_primary_key(&self, row: &Row) -> Result<Value> {
        let pos = self
            .columns
            .iter()
            .position(|c| c.primary_key)
            .expect("No primary key found");
        Ok(row[pos].clone())
    }

    pub fn get_col_index(&self, col_name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c.name == col_name)
            .ok_or(Error::Internal(format!("column {} not found", col_name)))
    }
}

impl Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let col_desc = self
            .columns
            .iter()
            .map(|c| format!("{}", c))
            .collect::<Vec<_>>()
            .join(",\n");
        write!(f, "CREATE TABLE {} (\n{}\n)", self.name, col_desc)
    }
}

#[derive(Debug, Serialize, Deserialize, With)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub primary_key: bool,
    pub index: bool,
}

impl Column {
    pub fn new(name: String, datatype: DataType, nullable: bool, default: Option<Value>, primary_key: bool, index: bool) -> Self {
        Self { name, datatype, nullable, default, primary_key, index }
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.datatype == other.datatype
    }
}

impl Eq for Column {}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut col_desc = format!("    {} {:?}", self.name, self.datatype);
        if self.primary_key {
            col_desc += " PRIMARY KEY";
        }
        if !self.nullable && !self.primary_key {
            col_desc += " NOT NULL";
        }
        if let Some(v) = &self.default {
            col_desc += &format!(" DEFAULT {}", v.to_string());
        }
        write!(f, "{}", col_desc)
    }
}
