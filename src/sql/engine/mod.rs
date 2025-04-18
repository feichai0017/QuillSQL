use crate::error::{Error, Result};

use crate::sql::schema::Table;
use crate::sql::types::Row;
use crate::sql::parser::Parser;
use crate::sql::executor::ResultSet;
use crate::sql::plan::Plan;

pub mod kv;

pub trait Engine: Clone {
    type Transaction: Transaction;

    fn begin(&self) -> Result<Self::Transaction>;

    fn session(&self) -> Result<Session<Self>> {
        Ok(Session {
            engine: self.clone(),
        })
    }
}


pub trait Transaction {
    fn commit(&self) -> Result<()>;

    fn rollback(&self) -> Result<()>;

    fn create_row(&mut self, table_name: String, row: Row) -> Result<()>;

    fn scan_table(&self, table_name: String) -> Result<Vec<Row>>;

    fn create_table(&mut self, table: Table) -> Result<()>;

    fn get_table(&self, table_name: String) -> Result<Option<Table>>;

    fn must_get_table(&self, table_name: String) -> Result<Table> {
        self.get_table(table_name.clone())?
            .ok_or(Error::Internal(format!("Table {} not found", table_name)))
    }
}

pub struct Session<E: Engine> {
    engine: E,
}

impl<E: Engine> Session<E> {
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
        match Parser::new(sql).parse()? {
            stmt => {
                let mut txn = self.engine.begin()?;
                
                match Plan::build(stmt).execute(&mut txn) {
                    Ok(result) => {
                        txn.commit()?;
                        Ok(result)
                    },
                    Err(err) => {
                        txn.rollback()?;
                        Err(err)
                    }
                }
            }
        }
    }
}

    