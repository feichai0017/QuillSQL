use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;

use quill_sql::database::{Database, DatabaseOptions};
use quill_sql::transaction::IsolationLevel;
use std::str::FromStr;

/// Shared app state holding a Database protected by a mutex.
#[derive(Clone)]
struct AppState {
    db: Arc<std::sync::Mutex<Database>>,
    options: DatabaseOptions,
}

fn rebuild_db(opts: &DatabaseOptions) -> Database {
    let db = if let Ok(path) = std::env::var("QUILL_DB_FILE") {
        Database::new_on_disk_with_options(&path, opts.clone()).expect("open db file")
    } else {
        Database::new_temp_with_options(opts.clone()).expect("open temp db")
    };
    db
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

async fn debug_wal_head(
    State(state): State<AppState>,
) -> Result<Json<quill_sql::recovery::wal::WalHeadDebug>, (StatusCode, String)> {
    let db_guard = state
        .db
        .lock()
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "DB poisoned".to_string()))?;
    Ok(Json(db_guard.debug_wal_head()))
}

async fn debug_buffer_stats(
    State(state): State<AppState>,
) -> Result<Json<quill_sql::database::BufferDebugStats>, (StatusCode, String)> {
    let db_guard = state
        .db
        .lock()
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "DB poisoned".to_string()))?;
    Ok(Json(db_guard.debug_buffer_stats()))
}

async fn debug_locks_snapshot(
    State(state): State<AppState>,
) -> Result<Json<quill_sql::transaction::LockDebugSnapshot>, (StatusCode, String)> {
    let db_guard = state
        .db
        .lock()
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "DB poisoned".to_string()))?;
    Ok(Json(db_guard.debug_lock_snapshot()))
}

async fn debug_trace_last(
    State(state): State<AppState>,
) -> Result<Json<quill_sql::database::DebugTrace>, (StatusCode, String)> {
    let db_guard = state
        .db
        .lock()
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "DB poisoned".to_string()))?;
    match db_guard.debug_last_trace() {
        Some(trace) => Ok(Json(trace)),
        None => Err((StatusCode::NOT_FOUND, "no query executed yet".to_string())),
    }
}

async fn debug_plan_last(
    State(state): State<AppState>,
) -> Result<Json<quill_sql::database::DebugPlanSnapshot>, (StatusCode, String)> {
    let db_guard = state
        .db
        .lock()
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "DB poisoned".to_string()))?;
    match db_guard.debug_last_plan() {
        Some(plan) => Ok(Json(plan)),
        None => Err((StatusCode::NOT_FOUND, "no query executed yet".to_string())),
    }
}

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<usize>,
}

async fn debug_wal_peek(
    State(state): State<AppState>,
    Query(q): Query<LimitQuery>,
) -> Result<Json<Vec<quill_sql::recovery::wal::WalPeekDebug>>, (StatusCode, String)> {
    let db = state.db.lock().ok();
    let db = match db {
        Some(guard) => guard,
        None => {
            // Attempt to rebuild database if poisoned
            let rebuilt = rebuild_db(&state.options);
            let mut db_lock = state
                .db
                .lock()
                .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "DB poisoned".to_string()))?;
            *db_lock = rebuilt;
            db_lock
        }
    };
    let limit = q.limit.unwrap_or(10).min(500);
    db.debug_wal_peek(limit)
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e)))
}

async fn debug_mvcc_versions(
    State(state): State<AppState>,
) -> Result<Json<quill_sql::database::MvccVersionsDebug>, (StatusCode, String)> {
    let db = state.db.lock().ok();
    let db = match db {
        Some(guard) => guard,
        None => {
            let rebuilt = rebuild_db(&state.options);
            let mut db_lock = state
                .db
                .lock()
                .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "DB poisoned".to_string()))?;
            *db_lock = rebuilt;
            db_lock
        }
    };
    db.debug_mvcc_versions()
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e)))
}

async fn debug_wal_segments(
    State(state): State<AppState>,
) -> Result<Json<quill_sql::database::WalSegmentsDebug>, (StatusCode, String)> {
    let db = state
        .db
        .lock()
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "DB poisoned".to_string()))?;
    db.debug_wal_segments()
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e)))
}

async fn debug_txns(
    State(state): State<AppState>,
) -> Result<Json<quill_sql::transaction::TxnDebugSnapshot>, (StatusCode, String)> {
    let db = state.db.lock().ok();
    let db = match db {
        Some(guard) => guard,
        None => {
            let rebuilt = rebuild_db(&state.options);
            let mut db_lock = state
                .db
                .lock()
                .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "DB poisoned".to_string()))?;
            *db_lock = rebuilt;
            db_lock
        }
    };
    Ok(Json(db.debug_txn_snapshot()))
}

#[tokio::main]
async fn main() {
    env_logger::init();

    // Build database (in-memory temp by default); WAL config centralized via DatabaseOptions/WalConfig defaults
    let default_isolation_level = std::env::var("QUILL_DEFAULT_ISOLATION")
        .ok()
        .as_deref()
        .map(IsolationLevel::from_str)
        .transpose()
        .unwrap_or_else(|e| panic!("invalid QUILL_DEFAULT_ISOLATION: {}", e));
    let db_options = DatabaseOptions {
        default_isolation_level,
        ..DatabaseOptions::default()
    };

    let db = if let Ok(path) = std::env::var("QUILL_DB_FILE") {
        Database::new_on_disk_with_options(&path, db_options.clone()).expect("open db file")
    } else {
        Database::new_temp_with_options(db_options).expect("open temp db")
    };

    let state = AppState {
        db: Arc::new(std::sync::Mutex::new(db)),
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
        .route("/debug/wal/head", get(debug_wal_head))
        .route("/debug/wal/peek", get(debug_wal_peek))
        .route("/debug/wal/segments", get(debug_wal_segments))
        .route("/debug/buffer/stats", get(debug_buffer_stats))
        .route("/debug/locks/snapshot", get(debug_locks_snapshot))
        .route("/debug/txns", get(debug_txns))
        .route("/debug/trace/last", get(debug_trace_last))
        .route("/debug/mvcc/versions", get(debug_mvcc_versions))
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

// no env parsing for WAL in server; configuration centralized in DatabaseOptions

/// Execute SQL and return rows of strings
async fn api_sql(
    State(state): State<AppState>,
    Json(req): Json<SqlRequest>,
) -> Result<Json<SqlResponse>, (StatusCode, String)> {
    let mut db = state
        .db
        .lock()
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "DB poisoned".to_string()))?;
    let cleaned = strip_sql_comments(&req.sql);
    let tuples = db
        .run(&cleaned)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("{}", e)))?;
    let rows = tuples
        .into_iter()
        .map(|t| t.data.into_iter().map(|v| format!("{}", v)).collect())
        .collect();
    Ok(Json(SqlResponse { rows }))
}

/// Execute multiple SQL statements separated by ';' and return all result sets
async fn api_sql_batch(
    State(state): State<AppState>,
    Json(req): Json<SqlRequest>,
) -> Result<Json<SqlBatchResponse>, (StatusCode, String)> {
    let mut db = state
        .db
        .lock()
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "DB poisoned".to_string()))?;
    let cleaned = strip_sql_comments(&req.sql);
    let statements = cleaned
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .take(100);
    let mut results: Vec<Vec<Vec<String>>> = Vec::new();
    for stmt in statements {
        let tuples = db
            .run(stmt)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("{}", e)))?;
        let rows: Vec<Vec<String>> = tuples
            .into_iter()
            .map(|t| t.data.into_iter().map(|v| format!("{}", v)).collect())
            .collect();
        results.push(rows);
    }
    Ok(Json(SqlBatchResponse { results }))
}
