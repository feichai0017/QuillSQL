use crate::{
    error::Result,
    sql::executor::{Executor, ResultSet},
    sql::engine::Transaction,
};

pub struct Scan {
    table_name: String,
}

impl Scan {
    pub fn new(table_name: String) -> Box<Self> {
        Box::new(Self { table_name })
    }
}

impl<T: Transaction> Executor<T> for Scan {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table = txn.must_get_table(self.table_name.clone())?;
        let rows = txn.scan_table(self.table_name.clone())?;
        Ok(ResultSet::Scan { 
            columns: table.columns.into_iter().map(|c| c.name.clone()).collect(),
            rows 
        })
    }
}