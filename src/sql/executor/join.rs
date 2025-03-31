use std::collections::HashMap;

use crate::{
    error::{Error, Result},
    sql::{
        engine::Transaction,
        parser::ast::{self, evaluate_expr, Expression},
        types::Value,
    },
};

use super::{Executor, ResultSet};

pub struct NestedLoopJoin<T: Transaction> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
    predicate: Option<Expression>,
    outer: bool,
}

impl<T: Transaction> NestedLoopJoin<T> {
    pub fn new(
        left: Box<dyn Executor<T>>,
        right: Box<dyn Executor<T>>,
        predicate: Option<Expression>,
        outer: bool,
    ) -> Box<Self> {
        Box::new(Self {
            left,
            right,
            predicate,
            outer,
        })
    }
}

impl<T: Transaction> Executor<T> for NestedLoopJoin<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        // 先执行左边的
        if let ResultSet::Scan {
            columns: lcols,
            rows: lrows,
        } = self.left.execute(txn)?
        {
            let mut new_rows = Vec::new();
            let mut new_cols = lcols.clone();
            // 再执行右边的
            if let ResultSet::Scan {
                columns: rcols,
                rows: rrows,
            } = self.right.execute(txn)?
            {
                new_cols.extend(rcols.clone());

                for lrow in &lrows {
                    let mut matched = false;

                    for rrow in &rrows {
                        let mut row = lrow.clone();

                        // 如果有条件，查看是否满足 Join 条件
                        if let Some(expr) = &self.predicate {
                            match evaluate_expr(expr, &lcols, lrow, &rcols, rrow)? {
                                Value::Null => {}
                                Value::Boolean(false) => {}
                                Value::Boolean(true) => {
                                    row.extend(rrow.clone());
                                    new_rows.push(row);
                                    matched = true;
                                }
                                _ => return Err(Error::Internal("Unexpected expression".into())),
                            }
                        } else {
                            row.extend(rrow.clone());
                            new_rows.push(row);
                        }
                    }

                    if self.outer && !matched {
                        let mut row = lrow.clone();
                        for _ in 0..rrows[0].len() {
                            row.push(Value::Null);
                        }
                        new_rows.push(row);
                    }
                }
            }
            return Ok(ResultSet::Scan {
                columns: new_cols,
                rows: new_rows,
            });
        }

        Err(Error::Internal("Unexpected result set".into()))
    }
}

pub struct HashJoin<T: Transaction> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
    predicate: Option<Expression>,
    outer: bool,
}

impl<T: Transaction> HashJoin<T> {
    pub fn new(
        left: Box<dyn Executor<T>>,
        right: Box<dyn Executor<T>>,
        predicate: Option<Expression>,
        outer: bool,
    ) -> Box<Self> {
        Box::new(Self {
            left,
            right,
            predicate,
            outer,
        })
    }
}

impl<T: Transaction> Executor<T> for HashJoin<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        // 先执行左边的
        if let ResultSet::Scan {
            columns: lcols,
            rows: lrows,
        } = self.left.execute(txn)?
        {
            let mut new_rows = Vec::new();
            let mut new_cols = lcols.clone();
            // 再执行右边的
            if let ResultSet::Scan {
                columns: rcols,
                rows: rrows,
            } = self.right.execute(txn)?
            {
                new_cols.extend(rcols.clone());

                // 解析 HashJoin 条件
                let (lfield, rfield) = match parse_join_filter(self.predicate) {
                    Some(filter) => filter,
                    None => return Err(Error::Internal("failed to parse join predicate".into())),
                };
                // 获取 join 列在表中列的位置
                let lpos = match lcols.iter().position(|c| *c == lfield) {
                    Some(pos) => pos,
                    None => {
                        return Err(Error::Internal(format!(
                            "column {} not exist in table",
                            lfield
                        )))
                    }
                };
                let rpos = match rcols.iter().position(|c| *c == rfield) {
                    Some(pos) => pos,
                    None => {
                        return Err(Error::Internal(format!(
                            "column {} not exist in table",
                            rfield
                        )))
                    }
                };

                // 构建哈希表
                let mut table = HashMap::new();
                for row in &rrows {
                    let rows = table.entry(row[rpos].clone()).or_insert(Vec::new());
                    rows.push(row.clone());
                }

                // 扫描左边获取记录
                for lrow in lrows {
                    match table.get(&lrow[lpos]) {
                        Some(rows) => {
                            for r in rows {
                                let mut row = lrow.clone();
                                row.extend(r.clone());
                                new_rows.push(row);
                            }
                        }
                        None => {
                            if self.outer {
                                let mut row = lrow.clone();
                                for _ in 0..rrows[0].len() {
                                    row.push(Value::Null);
                                }
                                new_rows.push(row);
                            }
                        }
                    }
                }

                return Ok(ResultSet::Scan {
                    columns: new_cols,
                    rows: new_rows,
                });
            }
        }
        Err(Error::Internal("Unexpected result set".into()))
    }
}

fn parse_join_filter(predicate: Option<Expression>) -> Option<(String, String)> {
    match predicate {
        Some(expr) => match expr {
            Expression::Field(f) => Some((f, "".into())),
            Expression::Operation(operation) => match operation {
                ast::Operation::Equal(l, r) => {
                    let lv = parse_join_filter(Some(*l));
                    let rv = parse_join_filter(Some(*r));

                    Some((lv.unwrap().0, rv.unwrap().0))
                }
                _ => None,
            },
            _ => None,
        },
        None => None,
    }
}
