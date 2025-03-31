use std::collections::{BTreeMap, HashMap};

use crate::{
    error::{Error, Result},
    sql::{
        engine::Transaction,
        parser::ast::Expression,
        schema::Table,
        types::{Row, Value},
    },
};

use super::{Executor, ResultSet};

pub struct Insert {
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    ) -> Box<Self> {
        Box::new(Self {
            table_name,
            columns,
            values,
        })
    }
}

// 列对齐
// tbl:
// insert into tbl values(1, 2, 3);
// a       b       c          d
// 1       2       3      default 填充
fn pad_row(table: &Table, row: &Row) -> Result<Row> {
    let mut results = row.clone();
    for column in table.columns.iter().skip(row.len()) {
        if let Some(default) = &column.default {
            results.push(default.clone());
        } else {
            return Err(Error::Internal(format!(
                "No default value for column {}",
                column.name
            )));
        }
    }

    Ok(results)
}

// tbl:
// insert into tbl(d, c) values(1, 2);
//    a          b       c          d
// default   default     2          1
fn make_row(table: &Table, columns: &Vec<String>, values: &Row) -> Result<Row> {
    // 判断列数是否和value数一致
    if columns.len() != values.len() {
        return Err(Error::Internal(format!("columns and values num mismatch")));
    }

    let mut inputs = HashMap::new();
    for (i, col_name) in columns.iter().enumerate() {
        inputs.insert(col_name, values[i].clone());
    }

    let mut results = Vec::new();
    for col in table.columns.iter() {
        if let Some(value) = inputs.get(&col.name) {
            results.push(value.clone());
        } else if let Some(value) = &col.default {
            results.push(value.clone());
        } else {
            return Err(Error::Internal(format!(
                "No value given for the column {}",
                col.name
            )));
        }
    }

    Ok(results)
}

impl<T: Transaction> Executor<T> for Insert {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let mut count = 0;
        // 先取出表信息
        let table = txn.must_get_table(self.table_name.clone())?;
        for exprs in self.values {
            // 将表达式转换成 value
            let row = exprs
                .into_iter()
                .map(|e| Value::from_expression(e))
                .collect::<Vec<_>>();
            // 如果没有指定插入的列
            let insert_row = if self.columns.is_empty() {
                pad_row(&table, &row)?
            } else {
                // 指定了插入的列，需要对 value 信息进行整理
                make_row(&table, &self.columns, &row)?
            };

            // 插入数据
            txn.create_row(self.table_name.clone(), insert_row)?;
            count += 1;
        }

        Ok(ResultSet::Insert { count })
    }
}

// Update 执行器
pub struct Update<T: Transaction> {
    table_name: String,
    source: Box<dyn Executor<T>>,
    columns: BTreeMap<String, Expression>,
}

impl<T: Transaction> Update<T> {
    pub fn new(
        table_name: String,
        source: Box<dyn Executor<T>>,
        columns: BTreeMap<String, Expression>,
    ) -> Box<Self> {
        Box::new(Self {
            table_name,
            source,
            columns,
        })
    }
}

impl<T: Transaction> Executor<T> for Update<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let mut updated = 0;
        // 执行扫描操作，获取到扫描的结果
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, rows } => {
                let table = txn.must_get_table(self.table_name)?;
                // 遍历所有需要更新的行
                for row in rows {
                    let mut new_row = row.clone();
                    let pk = table.get_primary_key(&row)?;

                    for (i, col) in columns.iter().enumerate() {
                        if let Some(expr) = self.columns.get(col) {
                            new_row[i] = Value::from_expression(expr.clone());
                        }
                    }
                    // 执行更新操作
                    // 如果有主键更新，删除原来的数据，新增一条新的数据
                    // 否则就 table_name + primary key => 更新数据
                    txn.update_row(&table, &pk, new_row)?;
                    updated += 1;
                }
            }
            _ => return Err(Error::Internal("Unexpected result set".into())),
        }
        Ok(ResultSet::Update { count: updated })
    }
}

// Delete 执行器
pub struct Delete<T: Transaction> {
    table_name: String,
    source: Box<dyn Executor<T>>,
}

impl<T: Transaction> Delete<T> {
    pub fn new(table_name: String, source: Box<dyn Executor<T>>) -> Box<Self> {
        Box::new(Self { table_name, source })
    }
}

impl<T: Transaction> Executor<T> for Delete<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns: _, rows } => {
                let mut count = 0;
                let table = txn.must_get_table(self.table_name)?;
                for row in rows {
                    // 取出主键
                    let pk = table.get_primary_key(&row)?;
                    txn.delete_row(&table, &pk)?;
                    count += 1;
                }

                Ok(ResultSet::Delete { count })
            }
            _ => return Err(Error::Internal("Unexpected result set".into())),
        }
    }
}
