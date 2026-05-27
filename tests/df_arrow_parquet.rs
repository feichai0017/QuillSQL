use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::context::SessionContext;
use quill_sql::database::{Database, QueryOutput};
use tempfile::TempDir;

fn rows(result: QueryOutput) -> Vec<Vec<String>> {
    result.rows_as_strings()
}

#[tokio::test]
async fn datafusion_memory_table_executes_sql() {
    let db = Database::new_temp().expect("database");
    db.run("create table t as select 1::bigint as id, 10::bigint as v")
        .await
        .expect("create table");
    db.run("insert into t values (2, 20), (3, 30)")
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
}

#[tokio::test]
async fn parquet_table_is_the_persistent_storage_path() {
    let dir = TempDir::new().expect("temp dir");
    let parquet_path = dir.path().join("people.parquet");
    write_people_parquet(parquet_path.to_str().unwrap()).await;

    let db = Database::new_temp().expect("database");
    db.register_parquet("people", parquet_path.to_str().unwrap())
        .await
        .expect("register parquet");

    assert_eq!(
        rows(
            db.run("select name from people where id >= 2 order by id")
                .await
                .unwrap()
        ),
        vec![vec!["bob".to_string()], vec!["cara".to_string()]]
    );
}

#[tokio::test]
async fn explain_uses_datafusion_physical_plan() {
    let db = Database::new_temp().expect("database");
    let text = rows(db.run("explain select 1").await.unwrap())
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("ProjectionExec"), "{text}");
}

async fn write_people_parquet(path: &str) {
    let ctx = SessionContext::new();
    ctx.sql(
        "select 1::bigint as id, 'ada' as name \
         union all select 2::bigint, 'bob' \
         union all select 3::bigint, 'cara'",
    )
    .await
    .expect("build dataframe")
    .write_parquet(path, DataFrameWriteOptions::new(), None)
    .await
    .expect("write parquet");
}
