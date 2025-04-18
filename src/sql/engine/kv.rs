use crate::{storage::{self, engine::Engine as StorageEngine}, error::{Result, Error}};
use super::{Engine, Transaction};
use crate::sql::schema::Table;
use crate::sql::types::{Row, Value};
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

        let id = Key::Row(table_name.clone(), row[0].clone());
        let value = bincode::serialize(&row)?;
        self.txn.set(bincode::serialize(&id)?, value)?;

        Ok(())
    }

    fn scan_table(&self, table_name: String) -> Result<Vec<Row>> {
        let prefix = KeyPrefix::Row(table_name);
        let results = self.txn.scan_prefix(bincode::serialize(&prefix)?)?;

        let mut rows = Vec::new();
        for result in results {
            let row: Row = bincode::deserialize(&result.value)?;
            rows.push(row);
        }
        Ok(rows)
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
        self.txn.set(bincode::serialize(&key)?, value)?;

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
    Row(String, Value),
}

#[derive(Debug, Serialize, Deserialize)]
enum KeyPrefix {
    Table,
    Row(String),
}

#[cfg(test)]
mod tests {
    use crate::{error::Result, sql::engine::Engine, storage::memory::MemoryEngine};
    use super::KVEngine;

    #[test]
    fn test_create_table() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut session = kvengine.session()?;

        session.execute("create table t1 (a int, b text, c integer);")?;
        session.execute("insert into t1 values (1, 'foo', 1);")?;

        let v1 = session.execute("select * from t1;")?;

        println!("{:?}", v1);
        
        Ok(())
    }
}