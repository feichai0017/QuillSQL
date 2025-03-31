use super::{engine::Transaction, plan::Node, types::Row};
use crate::error::Result;
use agg::Aggregate;
use join::{HashJoin, NestedLoopJoin};
use mutation::{Delete, Insert, Update};
use query::{Filter, IndexScan, Limit, Offset, Order, PrimaryKeyScan, Projection, Scan};
use schema::{CreateTable, DropTable};

mod agg;
mod join;
mod mutation;
mod query;
mod schema;

// 执行器定义
pub trait Executor<T: Transaction> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet>;
}

impl<T: Transaction + 'static> dyn Executor<T> {
    pub fn build(node: Node) -> Box<dyn Executor<T>> {
        match node {
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::DropTable { name } => DropTable::new(name),
            Node::Insert {
                table_name,
                columns,
                values,
            } => Insert::new(table_name, columns, values),
            Node::Scan { table_name, filter } => Scan::new(table_name, filter),
            Node::Update {
                table_name,
                source,
                columns,
            } => Update::new(table_name, Self::build(*source), columns),
            Node::Delete { table_name, source } => Delete::new(table_name, Self::build(*source)),
            Node::Order { source, order_by } => Order::new(Self::build(*source), order_by),
            Node::Limit { source, limit } => Limit::new(Self::build(*source), limit),
            Node::Offset { source, offset } => Offset::new(Self::build(*source), offset),
            Node::Projection { source, exprs } => Projection::new(Self::build(*source), exprs),
            Node::NestedLoopJoin {
                left,
                right,
                predicate,
                outer,
            } => NestedLoopJoin::new(Self::build(*left), Self::build(*right), predicate, outer),
            Node::Aggregate {
                source,
                exprs,
                group_by,
            } => Aggregate::new(Self::build(*source), exprs, group_by),
            Node::Filter { source, predicate } => Filter::new(Self::build(*source), predicate),
            Node::IndexScan {
                table_name,
                field,
                value,
            } => IndexScan::new(table_name, field, value),
            Node::PrimaryKeyScan { table_name, value } => PrimaryKeyScan::new(table_name, value),
            Node::HashJoin {
                left,
                right,
                predicate,
                outer,
            } => HashJoin::new(Self::build(*left), Self::build(*right), predicate, outer),
        }
    }
}

// 执行结果集
#[derive(Debug, PartialEq)]
pub enum ResultSet {
    CreateTable {
        table_name: String,
    },
    DropTable {
        table_name: String,
    },
    Insert {
        count: usize,
    },
    Scan {
        columns: Vec<String>,
        rows: Vec<Row>,
    },
    Update {
        count: usize,
    },
    Delete {
        count: usize,
    },
    Begin {
        version: u64,
    },
    Commit {
        version: u64,
    },
    Rollback {
        version: u64,
    },
    Explain {
        plan: String,
    },
}

impl ResultSet {
    pub fn to_string(&self) -> String {
        match self {
            ResultSet::CreateTable { table_name } => format!("CREATE TABLE {}", table_name),
            ResultSet::DropTable { table_name } => format!("DROP TABLE {}", table_name),
            ResultSet::Insert { count } => format!("INSERT {} rows", count),
            ResultSet::Scan { columns, rows } => {
                let rows_len = rows.len();

                // 找到每一列最大的长度
                let mut max_len = columns.iter().map(|c| c.len()).collect::<Vec<_>>();
                for one_row in rows {
                    for (i, v) in one_row.iter().enumerate() {
                        if v.to_string().len() > max_len[i] {
                            max_len[i] = v.to_string().len();
                        }
                    }
                }

                // 展示列
                let columns = columns
                    .iter()
                    .zip(max_len.iter())
                    .map(|(col, &len)| format!("{:width$}", col, width = len))
                    .collect::<Vec<_>>()
                    .join(" |");

                // 展示分隔符
                let sep = max_len
                    .iter()
                    .map(|v| format!("{}", "-".repeat(*v + 1)))
                    .collect::<Vec<_>>()
                    .join("+");

                // 展示列数据
                let rows = rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .zip(max_len.iter())
                            .map(|(v, &len)| format!("{:width$}", v.to_string(), width = len))
                            .collect::<Vec<_>>()
                            .join(" |")
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                format!("{}\n{}\n{}\n({} rows)", columns, sep, rows, rows_len)
            }
            ResultSet::Update { count } => format!("UPDATE {} rows", count),
            ResultSet::Delete { count } => format!("DELETE {} rows", count),
            ResultSet::Begin { version } => format!("TRANSACTION {} BEGIN", version),
            ResultSet::Commit { version } => format!("TRANSACTION {} COMMIT", version),
            ResultSet::Rollback { version } => format!("TRANSACTION {} ROLLBACK", version),
            ResultSet::Explain { plan } => plan.to_string(),
        }
    }
}
