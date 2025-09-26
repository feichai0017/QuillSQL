use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use quill_sql::database::{Database, DatabaseOptions, WalOptions};

/// Shared app state holding a Database protected by a mutex
#[derive(Clone)]
struct AppState {
    db: Arc<std::sync::Mutex<Database>>,
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

#[tokio::main]
async fn main() {
    env_logger::init();

    // Build database (in-memory temp by default); enable file path via QUILL_DB_FILE
    let db_options = DatabaseOptions {
        wal: WalOptions {
            directory: std::env::var("QUILL_WAL_DIR").ok().map(PathBuf::from),
            segment_size: std::env::var("QUILL_WAL_SEGMENT_SIZE")
                .ok()
                .and_then(|v| v.parse::<u64>().ok()),
            sync_on_flush: std::env::var("QUILL_WAL_SYNC_ON_FLUSH")
                .ok()
                .and_then(|v| parse_env_bool(&v)),
            writer_interval_ms: std::env::var("QUILL_WAL_WRITER_INTERVAL_MS")
                .ok()
                .and_then(|v| match v.parse::<u64>() {
                    Ok(0) => Some(None),
                    Ok(ms) => Some(Some(ms)),
                    Err(_) => None,
                }),
        },
    };

    let db = if let Ok(path) = std::env::var("QUILL_DB_FILE") {
        Database::new_on_disk_with_options(&path, db_options.clone()).expect("open db file")
    } else {
        Database::new_temp_with_options(db_options).expect("open temp db")
    };

    let state = AppState {
        db: Arc::new(std::sync::Mutex::new(db)),
    };

    // Static services
    let static_service =
        tower_http::services::ServeDir::new("public").append_index_html_on_directories(true);
    let docs_service = tower_http::services::ServeDir::new("docs");

    let app = Router::new()
        .route("/api/sql", post(api_sql))
        .route("/api/sql_batch", post(api_sql_batch))
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

fn parse_env_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

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
