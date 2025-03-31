use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

use super::types::{DataType, Row, Value};

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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub primary_key: bool,
    pub index: bool,
}

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
