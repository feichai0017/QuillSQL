use std::{collections::BTreeMap, fmt::Display};

use planner::Planner;

use crate::error::Result;

use super::{
    engine::Transaction,
    executor::{Executor, ResultSet},
    parser::ast::{self, Expression, OrderDirection},
    schema::Table,
    types::Value,
};

mod planner;

// 执行节点
#[derive(Debug, PartialEq)]
pub enum Node {
    // 创建表
    CreateTable {
        schema: Table,
    },

    // 删除表
    DropTable {
        name: String,
    },

    // 插入数据
    Insert {
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    },

    // 扫描节点
    Scan {
        table_name: String,
        filter: Option<Expression>,
    },

    // 更新节点
    Update {
        table_name: String,
        source: Box<Node>,
        columns: BTreeMap<String, Expression>,
    },

    // 删除节点
    Delete {
        table_name: String,
        source: Box<Node>,
    },

    // 排序节点
    Order {
        source: Box<Node>,
        order_by: Vec<(String, OrderDirection)>,
    },

    // Limit 节点
    Limit {
        source: Box<Node>,
        limit: usize,
    },

    // Offset 节点
    Offset {
        source: Box<Node>,
        offset: usize,
    },

    // 投影节点
    Projection {
        source: Box<Node>,
        exprs: Vec<(Expression, Option<String>)>,
    },

    // 嵌套循环 Join 节点
    NestedLoopJoin {
        left: Box<Node>,
        right: Box<Node>,
        predicate: Option<Expression>,
        outer: bool,
    },

    // 哈希 Join 节点
    HashJoin {
        left: Box<Node>,
        right: Box<Node>,
        predicate: Option<Expression>,
        outer: bool,
    },

    // Agg 聚集节点
    Aggregate {
        source: Box<Node>,
        exprs: Vec<(Expression, Option<String>)>,
        group_by: Option<Expression>,
    },

    // 过滤节点
    Filter {
        source: Box<Node>,
        predicate: Expression,
    },

    // 索引查询节点
    IndexScan {
        table_name: String,
        field: String,
        value: Value,
    },

    // 主键查询节点
    PrimaryKeyScan {
        table_name: String,
        value: Value,
    },
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.format(f, "", true)
    }
}

impl Node {
    fn format(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        prefix: &str,
        root: bool,
    ) -> std::fmt::Result {
        if !root {
            writeln!(f)?;
        } else {
            writeln!(f, "           SQL PLAN           ")?;
            writeln!(f, "------------------------------")?;
        }

        let prefix = if prefix.is_empty() {
            "  ->  ".to_string()
        } else {
            write!(f, "{}", prefix)?;
            format!("  {}", prefix)
        };

        match self {
            Node::CreateTable { schema } => {
                write!(f, "Create Table {}", schema.name)
            }
            Node::DropTable { name } => {
                write!(f, "Drop Table {}", name)
            }
            Node::Insert {
                table_name,
                columns: _,
                values: _,
            } => {
                write!(f, "Insert Into {}", table_name)
            }
            Node::Scan { table_name, filter } => {
                write!(f, "Seq Scan On {}", table_name)?;
                if let Some(filter) = filter {
                    write!(f, " ({})", filter)?;
                }
                Ok(())
            }
            Node::Update {
                table_name,
                source,
                columns: _,
            } => {
                write!(f, "Update On {}", table_name)?;
                (*source).format(f, &prefix, false)
            }
            Node::Delete { table_name, source } => {
                write!(f, "Delete On {}", table_name)?;
                (*source).format(f, &prefix, false)
            }
            Node::Order { source, order_by } => {
                let desc = order_by
                    .iter()
                    .map(|c| {
                        format!(
                            "{} {}",
                            c.0,
                            if c.1 == OrderDirection::Asc {
                                "asc"
                            } else {
                                "desc"
                            }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                write!(f, "Order By ({})", desc)?;
                (*source).format(f, &prefix, false)
            }
            Node::Limit { source, limit } => {
                write!(f, "Limit {}", limit)?;
                (*source).format(f, &prefix, false)
            }
            Node::Offset { source, offset } => {
                write!(f, "Offset {}", offset)?;
                (*source).format(f, &prefix, false)
            }
            Node::Projection { source, exprs } => {
                let desc = exprs
                    .iter()
                    .map(|(e, alias)| {
                        format!(
                            "{}{}",
                            e,
                            if alias.is_some() {
                                format!(" as {}", alias.clone().unwrap())
                            } else {
                                "".into()
                            }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Projection ({})", desc)?;
                (*source).format(f, &prefix, false)
            }
            Node::NestedLoopJoin {
                left,
                right,
                predicate,
                outer: _,
            } => {
                write!(f, "Nested Loop Join")?;
                if let Some(expr) = predicate {
                    write!(f, "({})", expr)?;
                }
                (*left).format(f, &prefix, false)?;
                (*right).format(f, &prefix, false)
            }
            Node::HashJoin {
                left,
                right,
                predicate,
                outer: _,
            } => {
                write!(f, "Hash Join")?;
                if let Some(expr) = predicate {
                    write!(f, "({})", expr)?;
                }
                (*left).format(f, &prefix, false)?;
                (*right).format(f, &prefix, false)
            }
            Node::Aggregate {
                source,
                exprs,
                group_by: _,
            } => {
                let desc = exprs
                    .iter()
                    .map(|(e, alias)| {
                        format!(
                            "{}{}",
                            e,
                            if alias.is_some() {
                                format!(" as {}", alias.clone().unwrap())
                            } else {
                                "".into()
                            }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Aggregate ({})", desc)?;
                (*source).format(f, &prefix, false)
            }
            Node::Filter { source, predicate } => {
                write!(f, "Filter ({})", predicate)?;
                (*source).format(f, &prefix, false)
            }
            Node::IndexScan {
                table_name,
                field,
                value: _,
            } => {
                write!(f, "Index Scan On {}.{}", table_name, field)
            }
            Node::PrimaryKeyScan { table_name, value } => {
                write!(f, "Primary Key Scan On {}({})", table_name, value)
            }
        }
    }
}

#[derive(Debug, PartialEq)]
// 执行计划定义，底层是不同类型执行节点
pub struct Plan(pub Node);

impl Plan {
    pub fn build<T: Transaction>(stmt: ast::Statement, txn: &mut T) -> Result<Self> {
        Planner::new(txn).build(stmt)
    }

    pub fn execute<T: Transaction + 'static>(self, txn: &mut T) -> Result<ResultSet> {
        <dyn Executor<T>>::build(self.0).execute(txn)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::Result,
        sql::{
            engine::{kv::KVEngine, Engine},
            parser::{
                ast::{self, Expression},
                Parser,
            },
            plan::{Node, Plan},
        },
        storage::disk::DiskEngine,
    };

    #[test]
    fn test_plan_create_table() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut txn = kvengine.begin()?;

        let sql1 = "
        create table tbl1 (
            a int default 100,
            b float not null,
            c varchar null,
            d bool default true
        );
        ";
        let stmt1 = Parser::new(sql1).parse()?;
        let p1 = Plan::build(stmt1, &mut txn);

        let sql2 = "
        create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        );
        ";
        let stmt2 = Parser::new(sql2).parse()?;
        let p2 = Plan::build(stmt2, &mut txn);
        assert_eq!(p1, p2);

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_plan_insert() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut txn = kvengine.begin()?;

        let sql1 = "insert into tbl1 values (1, 2, 3, 'a', true);";
        let stmt1 = Parser::new(sql1).parse()?;
        let p1 = Plan::build(stmt1, &mut txn)?;
        assert_eq!(
            p1,
            Plan(Node::Insert {
                table_name: "tbl1".to_string(),
                columns: vec![],
                values: vec![vec![
                    Expression::Consts(ast::Consts::Integer(1)),
                    Expression::Consts(ast::Consts::Integer(2)),
                    Expression::Consts(ast::Consts::Integer(3)),
                    Expression::Consts(ast::Consts::String("a".to_string())),
                    Expression::Consts(ast::Consts::Boolean(true)),
                ]],
            })
        );

        let sql2 = "insert into tbl2 (c1, c2, c3) values (3, 'a', true),(4, 'b', false);";
        let stmt2 = Parser::new(sql2).parse()?;
        let p2 = Plan::build(stmt2, &mut txn)?;
        assert_eq!(
            p2,
            Plan(Node::Insert {
                table_name: "tbl2".to_string(),
                columns: vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
                values: vec![
                    vec![
                        Expression::Consts(ast::Consts::Integer(3)),
                        Expression::Consts(ast::Consts::String("a".to_string())),
                        Expression::Consts(ast::Consts::Boolean(true)),
                    ],
                    vec![
                        Expression::Consts(ast::Consts::Integer(4)),
                        Expression::Consts(ast::Consts::String("b".to_string())),
                        Expression::Consts(ast::Consts::Boolean(false)),
                    ],
                ],
            })
        );

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_plan_select() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut txn = kvengine.begin()?;

        let sql = "select * from tbl1;";
        let stmt = Parser::new(sql).parse()?;
        let plan = Plan::build(stmt, &mut txn)?;
        assert_eq!(
            plan,
            Plan(Node::Scan {
                table_name: "tbl1".to_string(),
                filter: None,
            })
        );

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }
}
