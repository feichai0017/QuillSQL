use std::collections::HashMap;

use crate::{
    error::{Error, Result},
    sql::{
        engine::Transaction,
        parser::ast::{self, Expression},
        types::Value,
    },
};

use super::{Executor, ResultSet};

pub struct Aggregate<T: Transaction> {
    source: Box<dyn Executor<T>>,
    exprs: Vec<(Expression, Option<String>)>,
    group_by: Option<Expression>,
}

impl<T: Transaction> Aggregate<T> {
    pub fn new(
        source: Box<dyn Executor<T>>,
        exprs: Vec<(Expression, Option<String>)>,
        group_by: Option<Expression>,
    ) -> Box<Self> {
        Box::new(Self {
            source,
            exprs,
            group_by,
        })
    }
}

impl<T: Transaction> Executor<T> for Aggregate<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        if let ResultSet::Scan { columns, rows } = self.source.execute(txn)? {
            let mut new_cols = Vec::new();
            let mut new_rows = Vec::new();

            // 计算函数
            let mut calc = |col_val: Option<&Value>,
                            rows: &Vec<Vec<Value>>|
             -> Result<Vec<Value>> {
                let mut new_row = Vec::new();
                for (expr, alias) in &self.exprs {
                    match expr {
                        ast::Expression::Function(func_name, col_name) => {
                            let calculator = <dyn Calculator>::build(&func_name)?;
                            let val = calculator.calc(&col_name, &columns, rows)?;

                            // min(a)            -> min
                            // min(a) as min_val -> min_val
                            if new_cols.len() < self.exprs.len() {
                                new_cols.push(if let Some(a) = alias {
                                    a.clone()
                                } else {
                                    func_name.clone()
                                });
                            }
                            new_row.push(val);
                        }
                        ast::Expression::Field(col) => {
                            if let Some(ast::Expression::Field(group_col)) = &self.group_by {
                                if *col != *group_col {
                                    return Err(Error::Internal(format!("{} must appear in the GROUP BY clause or aggregate function", col)));
                                }
                            }

                            if new_cols.len() < self.exprs.len() {
                                new_cols.push(if let Some(a) = alias {
                                    a.clone()
                                } else {
                                    col.clone()
                                });
                            }
                            new_row.push(col_val.unwrap().clone());
                        }
                        _ => return Err(Error::Internal("unexpected expression".into())),
                    }
                }
                Ok(new_row)
            };

            // 判断有没有 Group By
            // select c2, min(c1), max(c3) from t group by c2;
            // c1 c2 c3
            // 1 aa 4.6
            // 3 cc 3.4
            // 2 bb 5.2
            // 4 cc 6.1
            // 5 aa 8.3
            // ----|------
            // ----|------
            // ----v------
            // 1 aa 4.6
            // 5 aa 8.3
            //
            // 2 bb 5.2
            //
            // 3 cc 3.4
            // 4 cc 6.1
            if let Some(ast::Expression::Field(group_col)) = &self.group_by {
                // 对数据进行分组，然后计算每组的统计
                let pos = match columns.iter().position(|c| *c == *group_col) {
                    Some(pos) => pos,
                    None => {
                        return Err(Error::Internal(format!(
                            "group by column {} not in table",
                            group_col
                        )))
                    }
                };

                // 针对 Group By 的列进行分组
                let mut agg_map = HashMap::new();
                for row in rows.iter() {
                    let key = &row[pos];
                    let value = agg_map.entry(key).or_insert(Vec::new());
                    value.push(row.clone());
                }

                for (key, row) in agg_map {
                    let row = calc(Some(key), &row)?;
                    new_rows.push(row);
                }
            } else {
                let row = calc(None, &rows)?;
                new_rows.push(row);
            }

            return Ok(ResultSet::Scan {
                columns: new_cols,
                rows: new_rows,
            });
        }

        Err(Error::Internal("Unexpected result set".into()))
    }
}

// 通用 Agg 计算定义
pub trait Calculator {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value>;
}

impl dyn Calculator {
    pub fn build(func_name: &String) -> Result<Box<dyn Calculator>> {
        Ok(match func_name.to_uppercase().as_ref() {
            "COUNT" => Count::new(),
            "SUM" => Sum::new(),
            "MIN" => Min::new(),
            "MAX" => Max::new(),
            "AVG" => Avg::new(),
            _ => return Err(Error::Internal("unknown aggregate function".into())),
        })
    }
}

pub struct Count;

impl Count {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

impl Calculator for Count {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value> {
        let pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => return Err(Error::Internal(format!("column {} not in table", col_name))),
        };

        // a b      c
        // 1 X     3.1
        // 2 NULL  6.4
        // 3 Z     1.5
        let mut count = 0;
        for row in rows.iter() {
            if row[pos] != Value::Null {
                count += 1;
            }
        }
        Ok(Value::Integer(count))
    }
}

pub struct Min;

impl Min {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

impl Calculator for Min {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value> {
        let pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => return Err(Error::Internal(format!("column {} not in table", col_name))),
        };

        // a b      c
        // 1 X     NULL
        // 2 NULL  6.4
        // 3 Z     1.5
        let mut min_val = Value::Null;
        let mut values = Vec::new();
        for row in rows.iter() {
            if row[pos] != Value::Null {
                values.push(&row[pos]);
            }
        }
        if !values.is_empty() {
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            min_val = values[0].clone();
        }
        Ok(min_val)
    }
}

pub struct Max;

impl Max {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

impl Calculator for Max {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value> {
        let pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => return Err(Error::Internal(format!("column {} not in table", col_name))),
        };

        // a b      c
        // 1 X     NULL
        // 2 NULL  6.4
        // 3 Z     1.5
        let mut max_val = Value::Null;
        let mut values = Vec::new();
        for row in rows.iter() {
            if row[pos] != Value::Null {
                values.push(&row[pos]);
            }
        }
        if !values.is_empty() {
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            max_val = values[values.len() - 1].clone();
        }
        Ok(max_val)
    }
}

pub struct Sum;
impl Sum {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}
impl Calculator for Sum {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value> {
        let pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => return Err(Error::Internal(format!("column {} not in table", col_name))),
        };

        // a b      c
        // 1 X     NULL
        // 2 NULL  6.4
        // 3 Z     1.5
        let mut sum = None;
        for row in rows.iter() {
            match row[pos] {
                Value::Null => {}
                Value::Integer(v) => {
                    if sum == None {
                        sum = Some(0.0);
                    }
                    sum = Some(sum.unwrap() + v as f64);
                }
                Value::Float(v) => {
                    if sum == None {
                        sum = Some(0.0);
                    }
                    sum = Some(sum.unwrap() + v);
                }
                _ => return Err(Error::Internal(format!("can not calc column {}", col_name))),
            }
        }

        Ok(match sum {
            Some(s) => Value::Float(s),
            None => Value::Null,
        })
    }
}

pub struct Avg;

impl Avg {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

impl Calculator for Avg {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value> {
        let sum = Sum::new().calc(col_name, cols, rows)?;
        let count = Count::new().calc(col_name, cols, rows)?;
        Ok(match (sum, count) {
            (Value::Float(s), Value::Integer(c)) => Value::Float(s / c as f64),
            _ => Value::Null,
        })
    }
}
