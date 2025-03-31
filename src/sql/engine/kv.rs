use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::{
    error::{Error, Result},
    sql::{
        parser::ast::{evaluate_expr, Expression},
        schema::Table,
        types::{Row, Value},
    },
    storage::{self, engine::Engine as StorageEngine, keycode::serialize_key},
};

use super::{Engine, Transaction};

// KV Engine 定义
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

// KV Transaction 定义，实际上对存储引擎中 MvccTransaction 的封装
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
        self.txn.commit()
    }

    fn rollback(&self) -> Result<()> {
        self.txn.rollback()
    }

    fn version(&self) -> u64 {
        self.txn.version()
    }

    fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
        let table = self.must_get_table(table_name.clone())?;
        // 校验行的有效性
        for (i, col) in table.columns.iter().enumerate() {
            match row[i].datatype() {
                None if col.nullable => {}
                None => {
                    return Err(Error::Internal(format!(
                        "column {} cannot be null",
                        col.name
                    )))
                }
                Some(dt) if dt != col.datatype => {
                    return Err(Error::Internal(format!(
                        "column {} type mismatch",
                        col.name
                    )))
                }
                _ => {}
            }
        }

        // 找到表中的主键作为一行数据的唯一标识
        let pk = table.get_primary_key(&row)?;
        // 查看主键对应的数据是否已经存在了
        let id = Key::Row(table_name.clone(), pk.clone()).encode()?;
        if self.txn.get(id.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "Duplicate data for primary key {} in table {}",
                pk, table_name
            )));
        }

        // 存放数据
        let value = bincode::serialize(&row)?;
        self.txn.set(id, value)?;

        // 维护索引
        let index_cols = table
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.index)
            .collect::<Vec<_>>();
        for (i, index_col) in index_cols {
            let mut index = self.load_index(&table_name, &index_col.name, &row[i])?;
            index.insert(pk.clone());
            self.save_index(&table_name, &index_col.name, &row[i], index)?;
        }

        Ok(())
    }

    fn update_row(&mut self, table: &Table, id: &Value, row: Row) -> Result<()> {
        let new_pk = table.get_primary_key(&row)?;
        // 更新了主键，则删除旧的数据，加一条新的数据
        if *id != new_pk {
            self.delete_row(table, id)?;
            self.create_row(table.name.clone(), row)?;
            return Ok(());
        }

        // 维护索引
        let index_cols = table
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.index)
            .collect::<Vec<_>>();
        for (i, index_col) in index_cols {
            if let Some(old_row) = self.read_by_id(&table.name, id)? {
                // 索引列没有被更新
                if old_row[i] == row[i] {
                    continue;
                }

                let mut old_index = self.load_index(&table.name, &index_col.name, &old_row[i])?;
                old_index.remove(id);
                self.save_index(&table.name, &index_col.name, &old_row[i], old_index)?;

                let mut new_index = self.load_index(&table.name, &index_col.name, &row[i])?;
                new_index.insert(id.clone());
                self.save_index(&table.name, &index_col.name, &row[i], new_index)?;
            }
        }

        let key = Key::Row(table.name.clone(), new_pk).encode()?;
        let value = bincode::serialize(&row)?;
        self.txn.set(key, value)?;

        Ok(())
    }

    fn delete_row(&mut self, table: &Table, id: &Value) -> Result<()> {
        // 维护索引
        let index_cols = table
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.index)
            .collect::<Vec<_>>();
        for (i, index_col) in index_cols {
            if let Some(row) = self.read_by_id(&table.name, id)? {
                let mut index = self.load_index(&table.name, &index_col.name, &row[i])?;
                index.remove(id);
                self.save_index(&table.name, &index_col.name, &row[i], index)?;
            }
        }

        let key = Key::Row(table.name.clone(), id.clone()).encode()?;
        self.txn.delete(key)
    }

    fn load_index(
        &self,
        table_name: &str,
        col_name: &str,
        col_value: &Value,
    ) -> Result<HashSet<Value>> {
        let key = Key::Index(table_name.into(), col_name.into(), col_value.clone()).encode()?;
        Ok(self
            .txn
            .get(key)?
            .map(|v| bincode::deserialize(&v))
            .transpose()?
            .unwrap_or_default())
    }

    fn save_index(
        &self,
        table_name: &str,
        col_name: &str,
        col_value: &Value,
        index: HashSet<Value>,
    ) -> Result<()> {
        let key = Key::Index(table_name.into(), col_name.into(), col_value.clone()).encode()?;
        if index.is_empty() {
            self.txn.delete(key)
        } else {
            self.txn.set(key, bincode::serialize(&index)?)
        }
    }

    fn read_by_id(&self, table_name: &str, id: &Value) -> Result<Option<Row>> {
        Ok(self
            .txn
            .get(Key::Row(table_name.into(), id.clone()).encode()?)?
            .map(|v| bincode::deserialize(&v))
            .transpose()?)
    }

    fn scan_table(&self, table_name: String, filter: Option<Expression>) -> Result<Vec<Row>> {
        let prefix = KeyPrefix::Row(table_name.clone()).encode()?;
        let table = self.must_get_table(table_name)?;
        let results = self.txn.scan_prefix(prefix)?;

        let mut rows = Vec::new();
        for result in results {
            // 过滤数据
            let row: Row = bincode::deserialize(&result.value)?;
            if let Some(expr) = &filter {
                let cols = table.columns.iter().map(|c| c.name.clone()).collect();
                match evaluate_expr(expr, &cols, &row, &cols, &row)? {
                    Value::Null => {}
                    Value::Boolean(false) => {}
                    Value::Boolean(true) => {
                        rows.push(row);
                    }
                    _ => return Err(Error::Internal("Unexpected expression".into())),
                }
            } else {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    fn create_table(&mut self, table: Table) -> Result<()> {
        // 判断表是否已经存在
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "table {} already exists",
                table.name
            )));
        }

        // 判断表的有效性
        table.validate()?;

        let key = Key::Table(table.name.clone()).encode()?;
        let value = bincode::serialize(&table)?;
        self.txn.set(key, value)?;

        Ok(())
    }

    fn drop_table(&mut self, table_name: String) -> Result<()> {
        let table = self.must_get_table(table_name.clone())?;
        // 删除表的数据
        let rows = self.scan_table(table_name, None)?;
        for row in rows {
            self.delete_row(&table, &table.get_primary_key(&row)?)?;
        }

        // 删除表元数据
        let key = Key::Table(table.name).encode()?;
        self.txn.delete(key)
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key = Key::Table(table_name).encode()?;
        Ok(self
            .txn
            .get(key)?
            .map(|v| bincode::deserialize(&v))
            .transpose()?)
    }

    fn get_table_names(&self) -> Result<Vec<String>> {
        let prefix = KeyPrefix::Table.encode()?;
        let results = self.txn.scan_prefix(prefix)?;
        let mut names = Vec::new();
        for result in results {
            let table: Table = bincode::deserialize(&result.value)?;
            names.push(table.name);
        }
        Ok(names)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Key {
    Table(String),
    Row(String, Value),
    Index(String, String, Value),
}

impl Key {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(self)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum KeyPrefix {
    Table,
    Row(String),
}

impl KeyPrefix {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(self)
    }
}

#[cfg(test)]
mod tests {

    use super::KVEngine;
    use crate::storage::engine::Engine as StorageEngine;
    use crate::{
        error::Result,
        sql::{
            engine::{Engine, Session},
            executor::ResultSet,
            types::{Row, Value},
        },
        storage::disk::DiskEngine,
    };

    fn setup_table<E: StorageEngine + 'static>(s: &mut Session<KVEngine<E>>) -> Result<()> {
        s.execute(
            "create table t1 (
                     a int primary key,
                     b text default 'vv',
                     c integer default 100
                 );",
        )?;

        s.execute(
            "create table t2 (
                     a int primary key,
                     b integer default 100,
                     c float default 1.1,
                     d bool default false,
                     e boolean default true,
                     f text default 'v1',
                     g string default 'v2',
                     h varchar default 'v3'
                 );",
        )?;

        s.execute(
            "create table t3 (
                     a int primary key,
                     b int default 12 null,
                     c integer default NULL,
                     d float not NULL
                 );",
        )?;

        s.execute(
            "create table t4 (
                     a bool primary key,
                     b int default 12,
                     d boolean default true
                 );",
        )?;
        Ok(())
    }

    fn scan_table_and_compare<E: StorageEngine + 'static>(
        s: &mut Session<KVEngine<E>>,
        table_name: &str,
        expect: Vec<Row>,
    ) -> Result<()> {
        match s.execute(&format!("select * from {};", table_name))? {
            ResultSet::Scan { columns: _, rows } => {
                assert_eq!(rows, expect);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn scan_table_and_print<E: StorageEngine + 'static>(
        s: &mut Session<KVEngine<E>>,
        table_name: &str,
    ) -> Result<()> {
        match s.execute(&format!("select * from {};", table_name))? {
            ResultSet::Scan { columns: _, rows } => {
                for row in rows {
                    println!("{:?}", row);
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[test]
    fn test_create_table() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_insert() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;

        // t1
        s.execute("insert into t1 (a) values (1);")?;
        s.execute("insert into t1 values (2, 'a', 2);")?;
        s.execute("insert into t1(b,a) values ('b', 3);")?;

        scan_table_and_compare(
            &mut s,
            "t1",
            vec![
                vec![
                    Value::Integer(1),
                    Value::String("vv".to_string()),
                    Value::Integer(100),
                ],
                vec![
                    Value::Integer(2),
                    Value::String("a".to_string()),
                    Value::Integer(2),
                ],
                vec![
                    Value::Integer(3),
                    Value::String("b".to_string()),
                    Value::Integer(100),
                ],
            ],
        )?;

        // t2
        s.execute("insert into t2 (a) values (1);")?;
        scan_table_and_compare(
            &mut s,
            "t2",
            vec![vec![
                Value::Integer(1),
                Value::Integer(100),
                Value::Float(1.1),
                Value::Boolean(false),
                Value::Boolean(true),
                Value::String("v1".to_string()),
                Value::String("v2".to_string()),
                Value::String("v3".to_string()),
            ]],
        )?;

        // t3
        s.execute("insert into t3 (a, d) values (1, 1.1);")?;
        scan_table_and_compare(
            &mut s,
            "t3",
            vec![vec![
                Value::Integer(1),
                Value::Integer(12),
                Value::Null,
                Value::Float(1.1),
            ]],
        )?;

        // t4
        s.execute("insert into t4 (a) values (true);")?;
        scan_table_and_compare(
            &mut s,
            "t4",
            vec![vec![
                Value::Boolean(true),
                Value::Integer(12),
                Value::Boolean(true),
            ]],
        )?;

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_update() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;

        s.execute("insert into t2 values (1, 1, 1.1, true, true, 'v1', 'v2', 'v3');")?;
        s.execute("insert into t2 values (2, 2, 2.2, false, false, 'v4', 'v5', 'v6');")?;
        s.execute("insert into t2 values (3, 3, 3.3, true, false, 'v7', 'v8', 'v9');")?;
        s.execute("insert into t2 values (4, 4, 4.4, false, true, 'v10', 'v11', 'v12');")?;

        let res = s.execute("update t2 set b = 100 where a = 1;")?;
        assert_eq!(res, ResultSet::Update { count: 1 });
        let res = s.execute("update t2 set d = false where d = true;")?;
        assert_eq!(res, ResultSet::Update { count: 2 });

        scan_table_and_compare(
            &mut s,
            "t2",
            vec![
                vec![
                    Value::Integer(1),
                    Value::Integer(100),
                    Value::Float(1.1),
                    Value::Boolean(false),
                    Value::Boolean(true),
                    Value::String("v1".to_string()),
                    Value::String("v2".to_string()),
                    Value::String("v3".to_string()),
                ],
                vec![
                    Value::Integer(2),
                    Value::Integer(2),
                    Value::Float(2.2),
                    Value::Boolean(false),
                    Value::Boolean(false),
                    Value::String("v4".to_string()),
                    Value::String("v5".to_string()),
                    Value::String("v6".to_string()),
                ],
                vec![
                    Value::Integer(3),
                    Value::Integer(3),
                    Value::Float(3.3),
                    Value::Boolean(false),
                    Value::Boolean(false),
                    Value::String("v7".to_string()),
                    Value::String("v8".to_string()),
                    Value::String("v9".to_string()),
                ],
                vec![
                    Value::Integer(4),
                    Value::Integer(4),
                    Value::Float(4.4),
                    Value::Boolean(false),
                    Value::Boolean(true),
                    Value::String("v10".to_string()),
                    Value::String("v11".to_string()),
                    Value::String("v12".to_string()),
                ],
            ],
        )?;

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_delete() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;

        s.execute("insert into t2 values (1, 1, 1.1, true, true, 'v1', 'v2', 'v3');")?;
        s.execute("insert into t2 values (2, 2, 2.2, false, false, 'v4', 'v5', 'v6');")?;
        s.execute("insert into t2 values (3, 3, 3.3, true, false, 'v7', 'v8', 'v9');")?;
        s.execute("insert into t2 values (4, 4, 4.4, false, true, 'v10', 'v11', 'v12');")?;

        let res = s.execute("delete from t2 where a = 1;")?;
        assert_eq!(res, ResultSet::Delete { count: 1 });
        scan_table_and_compare(
            &mut s,
            "t2",
            vec![
                vec![
                    Value::Integer(2),
                    Value::Integer(2),
                    Value::Float(2.2),
                    Value::Boolean(false),
                    Value::Boolean(false),
                    Value::String("v4".to_string()),
                    Value::String("v5".to_string()),
                    Value::String("v6".to_string()),
                ],
                vec![
                    Value::Integer(3),
                    Value::Integer(3),
                    Value::Float(3.3),
                    Value::Boolean(true),
                    Value::Boolean(false),
                    Value::String("v7".to_string()),
                    Value::String("v8".to_string()),
                    Value::String("v9".to_string()),
                ],
                vec![
                    Value::Integer(4),
                    Value::Integer(4),
                    Value::Float(4.4),
                    Value::Boolean(false),
                    Value::Boolean(true),
                    Value::String("v10".to_string()),
                    Value::String("v11".to_string()),
                    Value::String("v12".to_string()),
                ],
            ],
        )?;

        let res = s.execute("delete from t2 where d = false;")?;
        assert_eq!(res, ResultSet::Delete { count: 2 });
        scan_table_and_compare(
            &mut s,
            "t2",
            vec![vec![
                Value::Integer(3),
                Value::Integer(3),
                Value::Float(3.3),
                Value::Boolean(true),
                Value::Boolean(false),
                Value::String("v7".to_string()),
                Value::String("v8".to_string()),
                Value::String("v9".to_string()),
            ]],
        )?;

        let res = s.execute("delete from t2;")?;
        assert_eq!(res, ResultSet::Delete { count: 1 });
        scan_table_and_compare(&mut s, "t2", vec![])?;

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_sort() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;

        s.execute("insert into t3 values (1, 34, 22, 1.22);")?;
        s.execute("insert into t3 values (4, 23, 65, 4.23);")?;
        s.execute("insert into t3 values (3, 56, 22, 2.88);")?;
        s.execute("insert into t3 values (2, 87, 57, 6.78);")?;
        s.execute("insert into t3 values (5, 87, 14, 3.28);")?;
        s.execute("insert into t3 values (7, 87, 82, 9.52);")?;

        match s.execute("select a, b as col2 from t3 order by c, a desc limit 100;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(2, columns.len());
                assert_eq!(6, rows.len());
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_cross_join() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t1 (a int primary key);")?;
        s.execute("create table t2 (b int primary key);")?;
        s.execute("create table t3 (c int primary key);")?;

        s.execute("insert into t1 values (1), (2), (3);")?;
        s.execute("insert into t2 values (4), (5), (6);")?;
        s.execute("insert into t3 values (7), (8), (9);")?;

        match s.execute("select * from t1 cross join t2 cross join t3;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(3, columns.len());
                assert_eq!(27, rows.len());
                // for row in rows {
                //     println!("{:?}", row);
                // }
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_join() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t1 (a int primary key);")?;
        s.execute("create table t2 (b int primary key);")?;
        s.execute("create table t3 (c int primary key);")?;

        s.execute("insert into t1 values (1), (2), (3);")?;
        s.execute("insert into t2 values (2), (3), (4);")?;
        s.execute("insert into t3 values (3), (8), (9);")?;

        match s.execute("select * from t1 right join t2 on a = b join t3 on a = c;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(3, columns.len());
                assert_eq!(1, rows.len());
                // for row in rows {
                //     println!("{:?}", row);
                // }
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_agg() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t1 (a int primary key, b text, c float);")?;

        s.execute("insert into t1 values (1, 'aa', 3.1);")?;
        s.execute("insert into t1 values (2, 'cc', 5.3);")?;
        s.execute("insert into t1 values (3, null, NULL);")?;
        s.execute("insert into t1 values (4, 'dd', 4.6);")?;

        match s.execute("select count(a) as total, max(b), min(a), sum(c), avg(c) from t1;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(columns, vec!["total", "max", "min", "sum", "avg"]);
                assert_eq!(
                    rows,
                    vec![vec![
                        Value::Integer(4),
                        Value::String("dd".to_string()),
                        Value::Integer(1),
                        Value::Float(13.0),
                        Value::Float(13.0 / 3.0)
                    ]]
                );
            }
            _ => unreachable!(),
        }

        s.execute("create table t2 (a int primary key, b text, c float);")?;
        s.execute("insert into t2 values (1, NULL, NULL);")?;
        s.execute("insert into t2 values (2, NULL, NULL);")?;
        match s.execute("select count(a) as total, max(b), min(a), sum(c), avg(c) from t2;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(columns, vec!["total", "max", "min", "sum", "avg"]);
                assert_eq!(
                    rows,
                    vec![vec![
                        Value::Integer(2),
                        Value::Null,
                        Value::Integer(1),
                        Value::Null,
                        Value::Null
                    ]]
                );
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_group_by() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t1 (a int primary key, b text, c float);")?;

        s.execute("insert into t1 values (1, 'aa', 3.1);")?;
        s.execute("insert into t1 values (2, 'bb', 5.3);")?;
        s.execute("insert into t1 values (3, null, NULL);")?;
        s.execute("insert into t1 values (4, null, 4.6);")?;
        s.execute("insert into t1 values (5, 'bb', 5.8);")?;
        s.execute("insert into t1 values (6, 'dd', 1.4);")?;

        match s.execute("select b, min(c), max(a), avg(c) from t1 group by b order by avg;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(columns, vec!["b", "min", "max", "avg"]);
                assert_eq!(
                    rows,
                    vec![
                        vec![
                            Value::String("dd".to_string()),
                            Value::Float(1.4),
                            Value::Integer(6),
                            Value::Float(1.4)
                        ],
                        vec![
                            Value::String("aa".to_string()),
                            Value::Float(3.1),
                            Value::Integer(1),
                            Value::Float(3.1)
                        ],
                        vec![
                            Value::Null,
                            Value::Float(4.6),
                            Value::Integer(4),
                            Value::Float(4.6)
                        ],
                        vec![
                            Value::String("bb".to_string()),
                            Value::Float(5.3),
                            Value::Integer(5),
                            Value::Float(5.55)
                        ],
                    ]
                );
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_filter() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t1 (a int primary key, b text, c float, d bool);")?;

        s.execute("insert into t1 values (1, 'aa', 3.1, true);")?;
        s.execute("insert into t1 values (2, 'bb', 5.3, true);")?;
        s.execute("insert into t1 values (3, null, NULL, false);")?;
        s.execute("insert into t1 values (4, null, 4.6, false);")?;
        s.execute("insert into t1 values (5, 'bb', 5.8, true);")?;
        s.execute("insert into t1 values (6, 'dd', 1.4, false);")?;

        match s.execute("select * from t1 where d < true;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(4, columns.len());
                assert_eq!(3, rows.len());
            }
            _ => unreachable!(),
        }

        match s.execute("select b, sum(c) from t1 group by b having sum < 5 order by sum;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(2, columns.len());
                assert_eq!(3, rows.len());
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_index() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t (a int primary key, b text index, c float index, d bool);")?;
        s.execute("insert into t values (1, 'a', 1.1, true);")?;
        s.execute("insert into t values (2, 'b', 2.1, true);")?;
        s.execute("insert into t values (3, 'a', 3.2, false);")?;
        s.execute("insert into t values (4, 'c', 1.1, true);")?;
        s.execute("insert into t values (5, 'd', 2.1, false);")?;

        s.execute("delete from t where a = 4;")?;

        match s.execute("select * from t where c = 1.1;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(columns.len(), 4);
                assert_eq!(rows.len(), 1);
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_primary_key_scan() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t (a int primary key, b text index, c float index, d bool);")?;
        s.execute("insert into t values (1, 'a', 1.1, true);")?;
        s.execute("insert into t values (2, 'b', 2.1, true);")?;
        s.execute("insert into t values (3, 'a', 3.2, false);")?;

        match s.execute("select * from t where a = 2;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(columns.len(), 4);
                assert_eq!(rows.len(), 1);
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_hash_join() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t1 (a int primary key);")?;
        s.execute("create table t2 (b int primary key);")?;
        s.execute("create table t3 (c int primary key);")?;

        s.execute("insert into t1 values (1), (2), (3);")?;
        s.execute("insert into t2 values (2), (3), (4);")?;
        s.execute("insert into t3 values (3), (8), (9);")?;

        match s.execute("select * from t1 join t2 on a = b join t3 on a = c;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(columns.len(), 3);
                assert_eq!(rows.len(), 1);
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }
}
