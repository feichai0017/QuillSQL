use std::collections::HashMap;

use crate::{
    error::{Error, Result},
    sql::{engine::Transaction, executor::Executor, parser::ast::Expression, schema::Table, types::{Row, Value}},
};

pub struct Insert {
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(table_name: String, columns: Vec<String>, values: Vec<Vec<Expression>>) -> Box<Self> {
                Box::new(Self {table_name, columns, values})
    }
}

// column alignment
// tbl:
// insert into tbl values (1, 2, 3)
// row:
// a    b    c   d
// 1    2    3   default

fn pad_row(table: &Table, row: &Row) -> Result<Row> {
    let mut results = row.clone();
    for column in table.columns.iter().skip(row.len()) {
        if let Some(default) = &column.default {
            results.push(default.clone());
        } else {
            return Err(Error::Internal(format!("Column {} has no default value", column.name)));
        }
    }
    Ok(results)
}

// tbl:
// insert into tbl (d, c) values (1, 2)
// row:
// a           b      c   d
// default  default   2   1

fn make_row(table: &Table, columns: &Vec<String>, values: &Row) -> Result<Row> {
    // check cloumn number is equal with value number
    if columns.len() != values.len() {
        return Err(Error::Internal(format!("Columns mismatch with values")));
    }

    let mut inputs = HashMap::new();
    for (i, column) in columns.iter().enumerate() {
        inputs.insert(column, values[i].clone());
    }

    let mut results = Vec::new();
    for column in table.columns.iter() {
        if let Some(v) = inputs.get(&column.name) {
            results.push(v.clone());
        } else if let Some(v) = &column.default {
            results.push(v.clone());
        } else {
            return Err(Error::Internal(format!("Column {} has no default value", column.name)));
        }
    }
    Ok(results)
}

impl<T: Transaction> Executor<T> for Insert {


    fn execute(self: Box<Self>, txn: &mut T) -> Result<super::ResultSet> {
        let mut count = 0;

        let table = txn.must_get_table(self.table_name.clone())?;
        for exprs in self.values {

            let row = exprs.into_iter()
            .map(|e| Value::from_expression(e))
            .collect::<Vec<_>>();
            
            let insert_row = if self.columns.is_empty() {
                pad_row(&table, &row)?
            } else {
                make_row(&table, &self.columns, &row)?
            };

            txn.create_row(self.table_name.clone(), insert_row)?;
            count += 1;
        }
        Ok(super::ResultSet::Insert { count })
    }
}
