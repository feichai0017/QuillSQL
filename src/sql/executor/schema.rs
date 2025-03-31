use crate::{
    error::Result,
    sql::{engine::Transaction, schema::Table},
};

use super::{Executor, ResultSet};

// 创建表
pub struct CreateTable {
    schema: Table,
}

impl CreateTable {
    pub fn new(schema: Table) -> Box<Self> {
        Box::new(Self { schema })
    }
}

impl<T: Transaction> Executor<T> for CreateTable {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table_name = self.schema.name.clone();
        txn.create_table(self.schema)?;
        Ok(ResultSet::CreateTable { table_name })
    }
}

// 删除表
pub struct DropTable {
    name: String,
}

impl DropTable {
    pub fn new(name: String) -> Box<Self> {
        Box::new(Self { name })
    }
}

impl<T: Transaction> Executor<T> for DropTable {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        txn.drop_table(self.name.clone())?;
        Ok(ResultSet::DropTable {
            table_name: self.name,
        })
    }
}
