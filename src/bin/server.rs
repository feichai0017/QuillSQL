use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use quill_sql::database::{Database, DatabaseOptions};
use tokio::sync::{Mutex, MutexGuard};

/// Shared app state holding a Database protected by a mutex.
#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Database>>,
    options: DatabaseOptions,
}

fn rebuild_db(opts: &DatabaseOptions) -> Database {
    Database::new(opts.clone()).expect("open database")
}

fn database_options_from_env() -> DatabaseOptions {
    DatabaseOptions {
        data_dir: std::env::var("QUILL_DATA_DIR").ok().map(PathBuf::from),
    }
}

async fn lock_db<'a>(state: &'a AppState) -> MutexGuard<'a, Database> {
    state.db.lock().await
}

async fn rebuild(
    State(state): State<AppState>,
) -> Result<Json<&'static str>, (StatusCode, String)> {
    let new_db = rebuild_db(&state.options);
    let mut db_guard = state.db.lock().await;
    *db_guard = new_db;
    Ok(Json("rebuilt"))
}

/// Request payload for /api/sql
#[derive(Deserialize)]
struct SqlRequest {
    sql: String,
}

/// Response payload for /api/sql
#[derive(Serialize)]
struct SqlResponse {
    rows: Vec<Vec<String>>, // simple strings for frontend consumption
}

/// Response payload for /api/sql_batch
#[derive(Serialize)]
struct SqlBatchResponse {
    results: Vec<Vec<Vec<String>>>,
}

/// Remove single-line SQL comments beginning with `--`.
/// This intentionally does NOT strip inline comments inside string literals.
fn strip_sql_comments(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for line in input.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("--") {
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

async fn debug_trace_last(
    State(state): State<AppState>,
) -> Result<Json<quill_sql::database::DebugTrace>, (StatusCode, String)> {
    let db_guard = lock_db(&state).await;
    match db_guard.debug_last_trace() {
        Some(trace) => Ok(Json(trace)),
        None => Err((StatusCode::NOT_FOUND, "no query executed yet".to_string())),
    }
}

async fn debug_plan_last(
    State(state): State<AppState>,
) -> Result<Json<quill_sql::database::DebugPlanSnapshot>, (StatusCode, String)> {
    let db_guard = lock_db(&state).await;
    match db_guard.debug_last_plan() {
        Some(plan) => Ok(Json(plan)),
        None => Err((StatusCode::NOT_FOUND, "no query executed yet".to_string())),
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let db_options = database_options_from_env();
    let db = rebuild_db(&db_options);

    let state = AppState {
        db: Arc::new(Mutex::new(db)),
        options: db_options,
    };

    // Static services
    let static_service =
        tower_http::services::ServeDir::new("public").append_index_html_on_directories(true);
    let docs_service = tower_http::services::ServeDir::new("docs");

    let app = Router::new()
        .route("/api/sql", post(api_sql))
        .route("/api/sql_batch", post(api_sql_batch))
        .route("/admin/rebuild", post(rebuild))
        .route("/debug/trace/last", get(debug_trace_last))
        .route("/debug/plan/last", get(debug_plan_last))
        .nest_service("/docs", docs_service)
        .fallback_service(static_service)
        .with_state(state);

    // CORS for simple local testing
    let app = app.layer(tower_http::cors::CorsLayer::very_permissive());

    // Bind address: prefer PORT for platforms like Vercel/Heroku
    let bind_addr = if let Ok(port) = std::env::var("PORT") {
        format!("0.0.0.0:{}", port)
    } else {
        std::env::var("QUILL_HTTP_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string())
    };
    let addr: SocketAddr = bind_addr.parse().expect("invalid bind addr");
    println!("Serving on http://{}", addr);
    axum::serve(
        tokio::net::TcpListener::bind(addr)
            .await
            .expect("bind http"),
        app,
    )
    .await
    .expect("server error");
}

/// Execute SQL and return rows of strings
async fn api_sql(
    State(state): State<AppState>,
    Json(req): Json<SqlRequest>,
) -> Result<Json<SqlResponse>, (StatusCode, String)> {
    let db = lock_db(&state).await;
    let cleaned = strip_sql_comments(&req.sql);
    let output = db
        .run(&cleaned)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("{}", e)))?;
    let rows = output.rows_as_strings();
    Ok(Json(SqlResponse { rows }))
}

/// Execute multiple SQL statements separated by ';' and return all result sets
async fn api_sql_batch(
    State(state): State<AppState>,
    Json(req): Json<SqlRequest>,
) -> Result<Json<SqlBatchResponse>, (StatusCode, String)> {
    let db = lock_db(&state).await;
    let cleaned = strip_sql_comments(&req.sql);
    let statements = cleaned
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .take(100);
    let mut results: Vec<Vec<Vec<String>>> = Vec::new();
    for stmt in statements {
        let output = db
            .run(stmt)
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("{}", e)))?;
        results.push(output.rows_as_strings());
    }
    Ok(Json(SqlBatchResponse { results }))
}
