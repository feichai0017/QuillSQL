use crate::database::Database;
use crate::error::QuillSQLError;
use crate::session::SessionContext;
use crate::storage::tuple::Tuple;
use regex::Regex;
use sqllogictest::{DBOutput, DefaultColumnType};
use std::path::{Path, PathBuf};

pub struct QuillSQLDB {
    db: Database,
    session: SessionContext,
}

impl Default for QuillSQLDB {
    fn default() -> Self {
        Self::new()
    }
}

impl QuillSQLDB {
    pub fn new() -> Self {
        let mut db = Database::new_temp().unwrap();
        let session = SessionContext::new(db.default_isolation());
        Self { db, session }
    }
}

fn tuples_to_sqllogictest_string(tuples: Vec<Tuple>) -> Vec<Vec<String>> {
    let mut output = vec![];
    for tuple in tuples.iter() {
        let mut row = vec![];
        for value in tuple.data.iter() {
            row.push(format!("{value}"));
        }
        output.push(row);
    }
    output
}

impl sqllogictest::DB for QuillSQLDB {
    type Error = QuillSQLError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let is_query_sql = {
            let lower_sql = sql.trim_start().to_ascii_lowercase();
            lower_sql.starts_with("select") || lower_sql.starts_with("explain")
        };
        let tuples = self.db.run_with_session(&mut self.session, sql)?;
        if tuples.is_empty() {
            if is_query_sql {
                return Ok(DBOutput::Rows {
                    types: vec![],
                    rows: vec![],
                });
            } else {
                return Ok(DBOutput::StatementComplete(0));
            }
        }
        let types = vec![DefaultColumnType::Any; tuples[0].schema.column_count()];
        let rows = tuples_to_sqllogictest_string(tuples);
        Ok(DBOutput::Rows { types, rows })
    }
}

#[test]
fn sqllogictest() {
    let test_files = read_dir_recursive("src/tests/sql_example/");
    println!("test_files: {:?}", test_files);

    for file in test_files {
        let db = QuillSQLDB::new();
        let mut tester = sqllogictest::Runner::new(db);
        println!(
            "======== start to run file {} ========",
            file.to_str().unwrap()
        );
        tester.run_file(file).unwrap();
    }
}

fn read_dir_recursive<P: AsRef<Path>>(path: P) -> Vec<PathBuf> {
    let mut dst = vec![];
    read_dir_recursive_impl(&mut dst, path.as_ref());
    dst
}

fn read_dir_recursive_impl(dst: &mut Vec<PathBuf>, path: &Path) {
    let push_file = |dst: &mut Vec<PathBuf>, path: PathBuf| {
        // skip _xxx.slt file
        if Regex::new(r"/_.*\.slt")
            .unwrap()
            .is_match(path.to_str().unwrap())
        {
            println!("skip file: {:?}", path);
        } else {
            dst.push(path);
        }
    };

    if path.is_dir() {
        let entries = std::fs::read_dir(path).unwrap();
        for entry in entries {
            let path = entry.unwrap().path();

            if path.is_dir() {
                read_dir_recursive_impl(dst, &path);
            } else {
                push_file(dst, path);
            }
        }
    } else {
        push_file(dst, path.to_path_buf());
    }
}
