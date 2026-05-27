use quill_sql::database::Database;
use tempfile::TempDir;

fn rows(result: quill_sql::database::QueryOutput) -> Vec<Vec<String>> {
    result.rows_as_strings()
}

#[tokio::test]
async fn holt_table_supports_datafusion_select_and_dml() {
    let db = Database::new_temp().expect("database");
    db.run("create table t(id int, v int)")
        .await
        .expect("create table");
    db.run("insert into t values (1, 10), (2, 20), (3, 30)")
        .await
        .expect("insert rows");

    assert_eq!(
        rows(
            db.run("select id, v from t where id >= 2 order by id")
                .await
                .unwrap()
        ),
        vec![
            vec!["2".to_string(), "20".to_string()],
            vec!["3".to_string(), "30".to_string()]
        ]
    );

    db.run("update t set v = v + 5 where id = 2")
        .await
        .expect("update row");
    db.run("delete from t where id = 1")
        .await
        .expect("delete row");

    assert_eq!(
        rows(db.run("select id, v from t order by id").await.unwrap()),
        vec![
            vec!["2".to_string(), "25".to_string()],
            vec!["3".to_string(), "30".to_string()]
        ]
    );
}

#[tokio::test]
async fn committed_holt_data_survives_reopen() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("db");

    {
        let db = Database::new_on_disk(path.to_str().unwrap()).expect("open database");
        db.run("create table t(id bigint, v bigint)")
            .await
            .expect("create table");
        db.run("insert into t values (1, 10), (2, 20)")
            .await
            .expect("insert rows");
        db.flush().expect("flush");
    }

    let db = Database::new_on_disk(path.to_str().unwrap()).expect("reopen database");
    assert_eq!(
        rows(db.run("select sum(v) from t").await.unwrap()),
        vec![vec!["30".to_string()]]
    );
}

#[tokio::test]
async fn datafusion_information_schema_sees_holt_tables() {
    let db = Database::new_temp().expect("database");
    db.run("create table t(id int, v int)")
        .await
        .expect("create table");

    assert_eq!(
        rows(
            db.run(
                "select table_name from information_schema.tables \
                 where table_schema = 'public' and table_name = 't'"
            )
            .await
            .unwrap()
        ),
        vec![vec!["t".to_string()]]
    );
}

#[tokio::test]
async fn holt_secondary_index_preserves_query_results() {
    let db = Database::new_temp().expect("database");
    db.run("create table t(id int, v int)")
        .await
        .expect("create table");
    db.run("insert into t values (1, 10), (2, 20), (3, 30), (4, 40)")
        .await
        .expect("insert rows");
    db.run("create index idx_t_id on t(id)")
        .await
        .expect("create index");

    assert_eq!(
        rows(
            db.run("select v from t where id >= 2 and id <= 3 order by v")
                .await
                .unwrap()
        ),
        vec![vec!["20".to_string()], vec!["30".to_string()]]
    );
}

#[tokio::test]
async fn index_using_clause_is_rejected_because_holt_is_implicit() {
    let db = Database::new_temp().expect("database");
    db.run("create table t(id int)")
        .await
        .expect("create table");

    let err = db
        .run("create index idx_t_id using holt on t(id)")
        .await
        .expect_err("USING clause should be rejected");
    assert!(err.to_string().contains("Holt is the only index backend"));
}

#[tokio::test]
async fn explain_mentions_holt_scan_exec() {
    let db = Database::new_temp().expect("database");
    db.run("create table t(id int)")
        .await
        .expect("create table");
    db.run("insert into t values (1)")
        .await
        .expect("insert row");

    let text = rows(db.run("explain select * from t").await.unwrap())
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("HoltScanExec"), "{text}");
}
