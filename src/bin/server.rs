use axum::{
    extract::State,
    http::StatusCode,
    response::Html,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;

use quill_sql::database::Database;

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

#[tokio::main]
async fn main() {
    env_logger::init();

    // Build database (in-memory temp by default); enable file path via QUILL_DB_FILE
    let db = if let Ok(path) = std::env::var("QUILL_DB_FILE") {
        Database::new_on_disk(&path).expect("open db file")
    } else {
        Database::new_temp().expect("open temp db")
    };

    let state = AppState {
        db: Arc::new(std::sync::Mutex::new(db)),
    };

    // Static services
    let static_service = tower_http::services::ServeDir::new("public")
        .not_found_service(axum::routing::get(index_html));
    let docs_service = tower_http::services::ServeDir::new("docs");

    let app = Router::new()
        .route("/", get(index_html))
        .route("/api/sql", post(api_sql))
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

/// Simple index fallback when no file exists
async fn index_html() -> Html<&'static str> {
    // Lightweight redirect to the static index file to avoid compile-time include path issues
    Html("<!doctype html><html><head><meta http-equiv=refresh content='0;url=/index.html'></head><body></body></html>")
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
    let tuples = db
        .run(&req.sql)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("{}", e)))?;
    let rows = tuples
        .into_iter()
        .map(|t| t.data.into_iter().map(|v| format!("{}", v)).collect())
        .collect();
    Ok(Json(SqlResponse { rows }))
}
