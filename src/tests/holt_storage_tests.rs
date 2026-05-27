use std::ops::Bound;

use tempfile::TempDir;

use crate::database::Database;
use crate::session::SessionContext;
use crate::storage::engine::IndexScanRequest;
use crate::storage::tuple::Tuple;
use crate::utils::scalar::ScalarValue;
use crate::utils::table_ref::TableReference;

fn value_as_i32(value: &ScalarValue) -> i32 {
    match value {
        ScalarValue::Int32(Some(v)) => *v,
        other => panic!("expected Int32(Some(_)), got {:?}", other),
    }
}

fn first_column(rows: Vec<Tuple>) -> Vec<i32> {
    rows.iter().map(|row| value_as_i32(&row.data[0])).collect()
}

#[test]
fn holt_table_insert_update_delete_and_rollback() {
    let mut db = Database::new_temp().expect("database");
    db.run("create table ht(id int, v int) engine=holt")
        .expect("create holt table");
    db.run("create index ht_idx on ht using holt (id)")
        .expect("create holt index");
    db.run("insert into ht values (1, 10), (2, 20), (3, 30)")
        .expect("insert rows");

    db.run("update ht set v = v + 5 where id = 2")
        .expect("update row");
    db.run("delete from ht where id = 1").expect("delete row");

    let rows = db
        .run("select id from ht order by id")
        .expect("select visible rows");
    assert_eq!(first_column(rows), vec![2, 3]);

    let mut session = SessionContext::new(db.default_isolation());
    session.set_autocommit(false);
    db.run_with_session(&mut session, "begin")
        .expect("begin transaction");
    db.run_with_session(&mut session, "insert into ht values (4, 40)")
        .expect("insert rolled back row");
    db.run_with_session(&mut session, "rollback")
        .expect("rollback transaction");

    let rows = db
        .run("select id from ht order by id")
        .expect("select after rollback");
    assert_eq!(first_column(rows), vec![2, 3]);
}

#[test]
fn holt_table_reopens_committed_rows() {
    let temp = TempDir::new().expect("temp dir");
    let db_path = temp.path().join("quill.db");
    let db_path = db_path.to_str().expect("db path").to_string();

    {
        let mut db = Database::new_on_disk(&db_path).expect("open database");
        db.run("create table ht(id int, v int) engine=holt")
            .expect("create holt table");
        db.run("insert into ht values (1, 10), (2, 20)")
            .expect("insert rows");
        db.flush().expect("flush database");
    }

    {
        let mut db = Database::new_on_disk(&db_path).expect("reopen database");
        let rows = db
            .run("select id from ht order by id")
            .expect("select reopened rows");
        assert_eq!(first_column(rows), vec![1, 2]);
    }
}

#[test]
fn temp_database_drops_after_holt_store_shutdown() {
    for _ in 0..3 {
        let mut db = Database::new_temp().expect("database");
        db.run("create table ht(id int, v int)")
            .expect("create table");
        db.run("insert into ht values (1, 10)").expect("insert row");
        db.flush().expect("flush database");
    }
}

#[test]
fn holt_index_range_scan_returns_matching_rows() {
    let mut db = Database::new_temp().expect("database");
    db.run("create table ht(id int, v int) engine=holt")
        .expect("create holt table");
    db.run("create index ht_idx on ht using holt (id)")
        .expect("create holt index");
    db.run("insert into ht values (1, 10), (2, 20), (3, 30), (4, 40)")
        .expect("insert rows");

    let table_ref = TableReference::Bare {
        table: "ht".to_string(),
    };
    let key_schema = db
        .catalog
        .table_indexes(&table_ref)
        .expect("table indexes")
        .into_iter()
        .find(|idx| idx.name == "ht_idx")
        .expect("ht_idx")
        .key_schema;
    let binding = db.table_binding(&table_ref).expect("table binding");
    let start = Tuple::new(key_schema.clone(), vec![ScalarValue::Int32(Some(2))]);
    let end = Tuple::new(key_schema, vec![ScalarValue::Int32(Some(4))]);
    let mut stream = binding
        .index_scan(
            "ht_idx",
            IndexScanRequest::new(Bound::Included(start), Bound::Excluded(end)),
        )
        .expect("index scan");
    let mut ids = Vec::new();
    while let Some((_rid, _meta, tuple)) = stream.next().expect("next index row") {
        ids.push(value_as_i32(&tuple.data[0]));
    }
    assert_eq!(ids, vec![2, 3]);
}

#[test]
fn non_holt_storage_options_are_rejected() {
    let mut db = Database::new_temp().expect("database");
    let err = db
        .run("create table bt(id int, v int) engine=btree")
        .expect_err("BTree storage engine should be removed");
    assert!(
        err.to_string().contains("only ENGINE=HOLT is supported"),
        "{err:?}"
    );

    db.run("create table ht(id int, v int)")
        .expect("create default Holt table");
    let err = db
        .run("create index ht_btree_idx on ht using btree (id)")
        .expect_err("BTree indexes should be removed");
    assert!(
        err.to_string().contains("only USING HOLT is supported"),
        "{err:?}"
    );
}

#[test]
fn sql_uses_holt_index_for_range_predicate_after_backfill() {
    let mut db = Database::new_temp().expect("database");
    db.run("create table ht(id int, v int) engine=holt")
        .expect("create holt table");
    db.run("insert into ht values (1, 10), (2, 20), (3, 30), (4, 40)")
        .expect("insert rows before index");
    db.run("create index ht_idx on ht using holt (id)")
        .expect("create holt index");

    let rows = db
        .run("select id from ht where id >= 2 and id < 4 order by id")
        .expect("indexed range query");
    assert_eq!(first_column(rows), vec![2, 3]);

    let trace = db.debug_last_trace().expect("debug trace");
    assert!(
        trace.physical_plan.contains("IndexScan: ht_idx"),
        "{}",
        trace.physical_plan
    );
}

#[test]
fn sql_uses_composite_holt_index_for_full_equality() {
    let mut db = Database::new_temp().expect("database");
    db.run("create table ht(a int, b int, v int) engine=holt")
        .expect("create holt table");
    db.run("insert into ht values (1, 1, 10), (1, 2, 20), (2, 1, 30)")
        .expect("insert rows before index");
    db.run("create index ht_ab_idx on ht using holt (a, b)")
        .expect("create composite holt index");

    let rows = db
        .run("select v from ht where b = 2 and a = 1")
        .expect("indexed equality query");
    assert_eq!(first_column(rows), vec![20]);

    let trace = db.debug_last_trace().expect("debug trace");
    assert!(
        trace.physical_plan.contains("IndexScan: ht_ab_idx"),
        "{}",
        trace.physical_plan
    );
}

#[test]
fn default_table_engine_is_holt() {
    let mut db = Database::new_temp().expect("database");
    db.run("create table dt(id int, v int)")
        .expect("create default table");
    db.run("create index dt_idx on dt(id)")
        .expect("create default index");
    db.run("insert into dt values (1, 10), (2, 20)")
        .expect("insert rows");

    let table_ref = TableReference::Bare {
        table: "dt".to_string(),
    };
    assert!(db.catalog.table_id(&table_ref).expect("table id") > 0);
    let indexes = db.catalog.table_indexes(&table_ref).expect("indexes");
    assert!(
        indexes
            .iter()
            .find(|index| index.name == "dt_idx")
            .expect("dt_idx")
            .index_id
            > 0
    );

    let rows = db
        .run("select id from dt where id = 2")
        .expect("select from default holt table");
    assert_eq!(first_column(rows), vec![2]);
}

#[test]
fn information_schema_tables_are_holt_projection_tables() {
    let db = Database::new_temp().expect("database");
    let table_ref = TableReference::Full {
        catalog: "quillsql".to_string(),
        schema: "information_schema".to_string(),
        table: "tables".to_string(),
    };
    assert!(db.catalog.table_id(&table_ref).expect("table id") > 0);
}

#[test]
fn dropped_default_holt_table_does_not_reappear_after_reopen() {
    let temp = TempDir::new().expect("temp dir");
    let db_path = temp.path().join("quill.db");
    let db_path = db_path.to_str().expect("db path").to_string();

    {
        let mut db = Database::new_on_disk(&db_path).expect("open database");
        db.run("create table dt(id int, v int)")
            .expect("create default holt table");
        db.run("create index dt_idx on dt(id)")
            .expect("create default holt index");
        db.run("insert into dt values (1, 10)").expect("insert row");
        db.run("drop table dt").expect("drop table");
        db.flush().expect("flush database");
    }

    {
        let mut db = Database::new_on_disk(&db_path).expect("reopen database");
        let rows = db
            .run("select table_name from information_schema.tables where table_name = 'dt'")
            .expect("query information_schema");
        assert!(rows.is_empty());
        db.run("select * from dt")
            .expect_err("dropped Holt table should not reload from descriptor");
    }
}
