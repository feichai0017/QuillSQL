use std::{collections::BTreeMap, fmt::Display};

use crate::{
    error::{Error, Result},
    sql::types::{DataType, Value},
};

// Abstract Syntax Tree
#[derive(Debug, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    DropTable {
        name: String,
    },
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>,
    },
    Select {
        select: Vec<(Expression, Option<String>)>,
        from: FromItem,
        where_clause: Option<Expression>,
        group_by: Option<Expression>,
        having: Option<Expression>,
        order_by: Vec<(String, OrderDirection)>,
        limit: Option<Expression>,
        offset: Option<Expression>,
    },
    Update {
        table_name: String,
        columns: BTreeMap<String, Expression>,
        where_clause: Option<Expression>,
    },
    Delete {
        table_name: String,
        where_clause: Option<Expression>,
    },
    Begin,
    Commit,
    Rollback,
    Explain {
        stmt: Box<Statement>,
    },
}

#[derive(Debug, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

// column define
#[derive(Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub primary_key: bool,
    pub index: bool,
}

#[derive(Debug, PartialEq)]
pub enum FromItem {
    Table {
        name: String,
    },

    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        join_type: JoinType,
        predicate: Option<Expression>,
    },
}

#[derive(Debug, PartialEq)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
}

// statement define
#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Field(String),
    Consts(Consts),
    Operation(Operation),
    Function(String, String),
}

impl From<Consts> for Expression {
    fn from(value: Consts) -> Self {
        Self::Consts(value)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Consts {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Operation {
    Equal(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Field(v) => write!(f, "{}", v),
            Expression::Consts(c) => write!(
                f,
                "{}",
                Value::from_expression(Expression::Consts(c.clone()))
            ),
            Expression::Operation(operation) => match operation {
                Operation::Equal(l, r) => write!(f, "{} = {}", l, r),
                Operation::GreaterThan(l, r) => write!(f, "{} > {}", l, r),
                Operation::LessThan(l, r) => write!(f, "{} < {}", l, r),
            },
            Expression::Function(name, field) => write!(f, "{}({})", name, field),
        }
    }
}

pub fn evaluate_expr(
    expr: &Expression,
    lcols: &Vec<String>,
    lrows: &Vec<Value>,
    rcols: &Vec<String>,
    rrows: &Vec<Value>,
) -> Result<Value> {
    match expr {
        Expression::Field(col_name) => {
            let pos = match lcols.iter().position(|c| *c == *col_name) {
                Some(pos) => pos,
                None => {
                    return Err(Error::Internal(format!(
                        "column {} is not in table",
                        col_name
                    )))
                }
            };
            Ok(lrows[pos].clone())
        }
        Expression::Consts(consts) => Ok(match consts {
            Consts::Null => Value::Null,
            Consts::Boolean(b) => Value::Boolean(*b),
            Consts::Integer(i) => Value::Integer(*i),
            Consts::Float(f) => Value::Float(*f),
            Consts::String(s) => Value::String(s.clone()),
        }),
        Expression::Operation(operation) => match operation {
            Operation::Equal(lexpr, rexpr) => {
                let lv = evaluate_expr(&lexpr, lcols, lrows, rcols, rrows)?;
                let rv = evaluate_expr(&rexpr, rcols, rrows, lcols, lrows)?;
                Ok(match (lv, rv) {
                    (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l == r),
                    (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l == r),
                    (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 == r),
                    (Value::Float(l), Value::Integer(r)) => Value::Boolean(l == r as f64),
                    (Value::Float(l), Value::Float(r)) => Value::Boolean(l == r),
                    (Value::String(l), Value::String(r)) => Value::Boolean(l == r),
                    (Value::Null, _) => Value::Null,
                    (_, Value::Null) => Value::Null,
                    (l, r) => {
                        return Err(Error::Internal(format!(
                            "can not compare exression {} and {}",
                            l, r
                        )))
                    }
                })
            }
            Operation::GreaterThan(lexpr, rexpr) => {
                let lv = evaluate_expr(&lexpr, lcols, lrows, rcols, rrows)?;
                let rv = evaluate_expr(&rexpr, rcols, rrows, lcols, lrows)?;
                Ok(match (lv, rv) {
                    (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l > r),
                    (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l > r),
                    (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 > r),
                    (Value::Float(l), Value::Integer(r)) => Value::Boolean(l > r as f64),
                    (Value::Float(l), Value::Float(r)) => Value::Boolean(l > r),
                    (Value::String(l), Value::String(r)) => Value::Boolean(l > r),
                    (Value::Null, _) => Value::Null,
                    (_, Value::Null) => Value::Null,
                    (l, r) => {
                        return Err(Error::Internal(format!(
                            "can not compare exression {} and {}",
                            l, r
                        )))
                    }
                })
            }
            Operation::LessThan(lexpr, rexpr) => {
                let lv = evaluate_expr(&lexpr, lcols, lrows, rcols, rrows)?;
                let rv = evaluate_expr(&rexpr, rcols, rrows, lcols, lrows)?;
                Ok(match (lv, rv) {
                    (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l < r),
                    (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l < r),
                    (Value::Integer(l), Value::Float(r)) => Value::Boolean((l as f64) < r),
                    (Value::Float(l), Value::Integer(r)) => Value::Boolean(l < r as f64),
                    (Value::Float(l), Value::Float(r)) => Value::Boolean(l < r),
                    (Value::String(l), Value::String(r)) => Value::Boolean(l < r),
                    (Value::Null, _) => Value::Null,
                    (_, Value::Null) => Value::Null,
                    (l, r) => {
                        return Err(Error::Internal(format!(
                            "can not compare exression {} and {}",
                            l, r
                        )))
                    }
                })
            }
        },
        _ => return Err(Error::Internal("unexpected expression".into())),
    }
}
