use crate::{storage::{self, engine::Engine as StorageEngine}, error::{Result, Error}};
use super::{Engine, Transaction};
use crate::sql::schema::Table;
use crate::sql::types::Row;
use serde::{Serialize, Deserialize};
pub struct KVEngine<E: StorageEngine> {
    pub kv: storage::mvcc::Mvcc<E>,
}

impl<E: StorageEngine> Clone for KVEngine<E> {
    fn clone(&self) -> Self {
        Self {
            kv: self.kv.clone(),
        }
    }
}

impl<E: StorageEngine> KVEngine<E> {
    pub fn new(engine: E) -> Self {
        Self {
            kv: storage::mvcc::Mvcc::new(engine),
        }
    }
}

impl<E: StorageEngine> Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;
    
    fn begin(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin()?))
    }
}

pub struct KVTransaction<E: StorageEngine> {
    txn: storage::mvcc::MvccTransaction<E>,
}

impl<E: StorageEngine> KVTransaction<E> {
    pub fn new(txn: storage::mvcc::MvccTransaction<E>) -> Self {
        Self { txn }
    }
}

impl<E: StorageEngine> Transaction for KVTransaction<E> {
    fn commit(&self) -> Result<()> {
        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        Ok(())
    }

    fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
        let table =self.must_get_table(table_name.clone())?;
        for (i, column) in table.columns.iter().enumerate() {
            match row[i].datatype() {
                None if column.nullable => {},
                None => return Err(Error::Internal(format!("Column {} is not nullable", column.name))),
                Some(datatype) if datatype != column.datatype => {
                    return Err(Error::Internal(format!("Column {} has the wrong datatype", column.name)));
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn scan_table(&self, table: String) -> Result<Vec<Row>> {
        todo!()
    }

    fn create_table(&mut self, table: Table) -> Result<()> {
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!("Table {} already exists", table.name)));
        }
        if table.columns.is_empty() {
            return Err(Error::Internal(format!("Table {} must have at least one column", table.name)));
        }
        
        let key = Key::Table(table.name.clone());
        let value = bincode::serialize(&table)?;
        self.txn.set(bincode::serialize(&key)?, value);
        Ok(())
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key = Key::Table(table_name);
        let value = self.txn.get(bincode::serialize(&key)?)?
        .map(|v| bincode::deserialize(&v))
        .transpose()?;
    
        Ok(value)
    }
    
    
}

#[derive(Debug, Serialize, Deserialize)]
enum Key {
    Table(String),
    Row(String, String),
}
