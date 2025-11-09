use quill_sql::database::Database;
use quill_sql::utils::table_ref::TableReference;

#[test]
fn analyze_statement_updates_catalog_stats() {
    let mut db = Database::new_temp().unwrap();
    db.run("create table t_analyze (a int)").unwrap();
    db.run("insert into t_analyze values (1), (2), (3)")
        .unwrap();

    db.run("analyze table t_analyze").unwrap();

    let stats = db
        .table_statistics(&TableReference::Bare {
            table: "t_analyze".to_string(),
        })
        .unwrap();
    assert_eq!(stats.row_count, 3);
}
