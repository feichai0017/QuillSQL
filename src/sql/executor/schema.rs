use crate::{
    error::Result,
    sql::{executor::Executor, schema::Table, engine::Transaction},
};

pub struct CreateTable {
    schema: Table,
}

impl CreateTable {
    pub fn new(schema: Table) -> Box<Self> {
        Box::new(Self { schema })
    }
}

impl<T: Transaction> Executor<T> for CreateTable {
    fn execute(&self, txn: &mut T) -> Result<super::ResultSet> {
        todo!()
    }
}
