//! Generic HTTP server for BitDex — no dataset-specific code.
//!
//! Feature-gated behind `server`. Provides `BitdexServer` which starts blank
//! and creates indexes via API.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::{get, post, delete};
use axum::Router;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;

use crate::concurrent_engine::ConcurrentEngine;
use crate::config::{Config, DataSchema};
use crate::loader;
use crate::query::BitdexQuery;

// ---------------------------------------------------------------------------
// Server state
// ---------------------------------------------------------------------------

/// Load status for an index.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status")]
pub enum LoadStatus {
    #[serde(rename = "idle")]
    Idle,
    #[serde(rename = "loading")]
    Loading {
        records_loaded: u64,
        elapsed_secs: f64,
    },
    #[serde(rename = "complete")]
    Complete {
        records_loaded: u64,
        elapsed_secs: f64,
    },
    #[serde(rename = "error")]
    Error { message: String },
}

/// Persisted index definition (saved as config.json in the index directory).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    pub config: Config,
    pub data_schema: DataSchema,
}

/// Live state for a single index.
struct IndexState {
    engine: Arc<ConcurrentEngine>,
    definition: IndexDefinition,
    load_progress: Arc<AtomicU64>,
    load_status: Arc<Mutex<LoadStatus>>,
}

/// Shared application state.
struct AppState {
    data_dir: PathBuf,
    index: Mutex<Option<IndexState>>,
}

type SharedState = Arc<AppState>;

// ---------------------------------------------------------------------------
// API request/response types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct CreateIndexRequest {
    name: String,
    config: Config,
    data_schema: DataSchema,
}

#[derive(Deserialize)]
struct LoadRequest {
    path: String,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default = "default_threads")]
    threads: usize,
}

fn default_threads() -> usize {
    4
}

#[derive(Deserialize)]
struct DocumentRequest {
    slot_id: u32,
}

#[derive(Deserialize)]
struct DocumentBatchRequest {
    slot_ids: Vec<u32>,
}

// ---------------------------------------------------------------------------
// Public server entry point
// ---------------------------------------------------------------------------

/// The BitDex HTTP server. Starts blank and creates indexes via API.
pub struct BitdexServer {
    data_dir: PathBuf,
}

impl BitdexServer {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// Start the HTTP server. Blocks until the server shuts down.
    pub async fn serve(self, addr: SocketAddr) -> std::io::Result<()> {
        // Ensure data directory exists
        std::fs::create_dir_all(&self.data_dir).ok();

        let state = Arc::new(AppState {
            data_dir: self.data_dir.clone(),
            index: Mutex::new(None),
        });

        // Try to restore an existing index from disk
        if let Err(e) = restore_index(&state) {
            eprintln!("Warning: failed to restore index from disk: {e}");
        }

        let app = Router::new()
            // Index management
            .route("/api/indexes", post(handle_create_index))
            .route("/api/indexes", get(handle_list_indexes))
            .route("/api/indexes/{name}", get(handle_get_index))
            .route("/api/indexes/{name}", delete(handle_delete_index))
            // Data loading
            .route("/api/indexes/{name}/load", post(handle_load))
            .route("/api/indexes/{name}/load/status", get(handle_load_status))
            // Query & documents
            .route("/api/indexes/{name}/query", post(handle_query))
            .route("/api/indexes/{name}/document", post(handle_document))
            .route("/api/indexes/{name}/documents", post(handle_documents_batch))
            .route("/api/indexes/{name}/stats", get(handle_stats))
            // Utility
            .route("/api/health", get(handle_health))
            // Serve static UI
            .route("/", get(handle_ui))
            .layer(CorsLayer::permissive())
            .with_state(state);

        eprintln!("BitDex server listening on http://{}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Index restoration from disk
// ---------------------------------------------------------------------------

fn restore_index(state: &SharedState) -> Result<(), String> {
    let indexes_dir = state.data_dir.join("indexes");
    if !indexes_dir.exists() {
        return Ok(());
    }

    // Scan for index directories with config.json
    let entries = std::fs::read_dir(&indexes_dir).map_err(|e| e.to_string())?;
    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let config_path = path.join("config.json");
        if !config_path.exists() {
            continue;
        }

        let json = std::fs::read_to_string(&config_path).map_err(|e| e.to_string())?;
        let def: IndexDefinition = serde_json::from_str(&json).map_err(|e| e.to_string())?;

        // Create engine from persisted config
        let docstore_path = path.join("docstore.redb");
        let mut config = def.config.clone();
        config.storage.bitmap_path = Some(path.join("bitmaps.redb"));

        // Always use new_with_path so bitmaps restore from bitmap_path even if
        // docstore doesn't exist yet (it will be created fresh).
        let engine = ConcurrentEngine::new_with_path(config, &docstore_path)
            .map_err(|e| e.to_string())?;

        let alive = engine.alive_count();
        eprintln!(
            "Restored index '{}' from disk ({} records)",
            def.name, alive
        );

        let load_status = if alive > 0 {
            LoadStatus::Complete {
                records_loaded: alive,
                elapsed_secs: 0.0,
            }
        } else {
            LoadStatus::Idle
        };

        *state.index.lock() = Some(IndexState {
            engine: Arc::new(engine),
            definition: def,
            load_progress: Arc::new(AtomicU64::new(alive)),
            load_status: Arc::new(Mutex::new(load_status)),
        });

        // Only restore the first index (single-index for now)
        break;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Handlers: Index management
// ---------------------------------------------------------------------------

async fn handle_create_index(
    State(state): State<SharedState>,
    Json(req): Json<CreateIndexRequest>,
) -> impl IntoResponse {
    // Validate name
    if req.name.is_empty() || req.name.len() > 64 || !req.name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Invalid index name. Use alphanumeric, underscore, or hyphen."})),
        ).into_response();
    }

    // Check if an index already exists
    {
        let guard = state.index.lock();
        if guard.is_some() {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"error": "An index already exists. Delete it first."})),
            ).into_response();
        }
    }

    // Validate config
    if let Err(e) = req.config.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("Invalid config: {e}")})),
        ).into_response();
    }

    // Create index directory
    let index_dir = state.data_dir.join("indexes").join(&req.name);
    if let Err(e) = std::fs::create_dir_all(&index_dir) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("Failed to create index directory: {e}")})),
        ).into_response();
    }

    // Persist config
    let definition = IndexDefinition {
        name: req.name.clone(),
        config: req.config.clone(),
        data_schema: req.data_schema,
    };
    let config_json = serde_json::to_string_pretty(&definition).unwrap();
    let config_path = index_dir.join("config.json");
    if let Err(e) = std::fs::write(&config_path, &config_json) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("Failed to write config: {e}")})),
        ).into_response();
    }

    // Create engine
    let docstore_path = index_dir.join("docstore.redb");
    let mut config = req.config;
    config.storage.bitmap_path = Some(index_dir.join("bitmaps.redb"));

    let engine = match ConcurrentEngine::new_with_path(config, &docstore_path) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("Failed to create engine: {e}")})),
            ).into_response();
        }
    };

    *state.index.lock() = Some(IndexState {
        engine: Arc::new(engine),
        definition,
        load_progress: Arc::new(AtomicU64::new(0)),
        load_status: Arc::new(Mutex::new(LoadStatus::Idle)),
    });

    (
        StatusCode::CREATED,
        Json(serde_json::json!({"name": req.name, "status": "created"})),
    ).into_response()
}

async fn handle_list_indexes(State(state): State<SharedState>) -> impl IntoResponse {
    let guard = state.index.lock();
    let indexes: Vec<serde_json::Value> = match guard.as_ref() {
        Some(idx) => vec![serde_json::json!({
            "name": idx.definition.name,
            "alive_count": idx.engine.alive_count(),
        })],
        None => vec![],
    };
    Json(serde_json::json!({"indexes": indexes}))
}

async fn handle_get_index(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let guard = state.index.lock();
    match guard.as_ref() {
        Some(idx) if idx.definition.name == name => {
            Json(serde_json::json!({
                "name": idx.definition.name,
                "config": idx.definition.config,
                "data_schema": idx.definition.data_schema,
                "stats": {
                    "alive_count": idx.engine.alive_count(),
                    "slot_count": idx.engine.slot_counter(),
                }
            })).into_response()
        }
        _ => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
        ).into_response(),
    }
}

async fn handle_delete_index(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let mut guard = state.index.lock();
    let exists = guard.as_ref().map(|idx| idx.definition.name == name).unwrap_or(false);
    if !exists {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
        ).into_response();
    }

    // Check if loading
    if let Some(idx) = guard.as_ref() {
        let status = idx.load_status.lock();
        if matches!(*status, LoadStatus::Loading { .. }) {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"error": "Cannot delete index while loading"})),
            ).into_response();
        }
    }

    // Drop the index
    *guard = None;

    // Remove index directory
    let index_dir = state.data_dir.join("indexes").join(&name);
    if index_dir.exists() {
        if let Err(e) = std::fs::remove_dir_all(&index_dir) {
            eprintln!("Warning: failed to remove index directory: {e}");
        }
    }

    Json(serde_json::json!({"status": "deleted"})).into_response()
}

// ---------------------------------------------------------------------------
// Handlers: Data loading
// ---------------------------------------------------------------------------

async fn handle_load(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<LoadRequest>,
) -> impl IntoResponse {
    let (engine, schema, load_progress, load_status) = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => {
                // Check if already loading
                {
                    let status = idx.load_status.lock();
                    if matches!(*status, LoadStatus::Loading { .. }) {
                        return (
                            StatusCode::CONFLICT,
                            Json(serde_json::json!({"error": "Already loading"})),
                        ).into_response();
                    }
                }
                (
                    Arc::clone(&idx.engine),
                    idx.definition.data_schema.clone(),
                    Arc::clone(&idx.load_progress),
                    Arc::clone(&idx.load_status),
                )
            }
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    let path = PathBuf::from(&req.path);
    if !path.exists() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("File not found: {}", req.path)})),
        ).into_response();
    }

    // Reset progress
    load_progress.store(0, Ordering::Release);
    *load_status.lock() = LoadStatus::Loading {
        records_loaded: 0,
        elapsed_secs: 0.0,
    };

    let limit = req.limit;
    let threads = req.threads;
    let progress = Arc::clone(&load_progress);
    let status = Arc::clone(&load_status);

    // Spawn blocking loading task
    tokio::task::spawn_blocking(move || {
        // Enter loading mode
        engine.enter_loading_mode();

        match loader::load_ndjson(&engine, &schema, &path, limit, threads, progress.clone()) {
            Ok(stats) => {
                // Exit loading mode (publishes staging and restarts maintenance)
                engine.exit_loading_mode();

                let alive = engine.alive_count();
                eprintln!("Load complete: {} records alive", alive);

                // Save bitmap snapshot for fast restart
                if let Err(e) = engine.save_snapshot() {
                    eprintln!("Warning: failed to save bitmap snapshot: {e}");
                } else {
                    eprintln!("Bitmap snapshot saved");
                }

                *status.lock() = LoadStatus::Complete {
                    records_loaded: stats.records_loaded,
                    elapsed_secs: stats.elapsed.as_secs_f64(),
                };
            }
            Err(e) => {
                engine.exit_loading_mode();
                *status.lock() = LoadStatus::Error {
                    message: e.to_string(),
                };
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"status": "loading"})),
    ).into_response()
}

async fn handle_load_status(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let guard = state.index.lock();
    match guard.as_ref() {
        Some(idx) if idx.definition.name == name => {
            let status = idx.load_status.lock().clone();
            // If still loading, update records_loaded from the atomic counter
            let status = match status {
                LoadStatus::Loading { .. } => {
                    let loaded = idx.load_progress.load(Ordering::Acquire);
                    LoadStatus::Loading {
                        records_loaded: loaded,
                        elapsed_secs: 0.0, // we don't track start time here; could be added
                    }
                }
                other => other,
            };
            Json(serde_json::to_value(&status).unwrap()).into_response()
        }
        _ => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
        ).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Handlers: Query & documents
// ---------------------------------------------------------------------------

async fn handle_query(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(query): Json<BitdexQuery>,
) -> impl IntoResponse {
    let engine = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => Arc::clone(&idx.engine),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    let start = Instant::now();
    match engine.execute_query(&query) {
        Ok(result) => {
            let elapsed_us = start.elapsed().as_micros() as u64;
            let cursor = result.cursor.map(|c| serde_json::to_value(c).unwrap());
            Json(serde_json::json!({
                "ids": result.ids,
                "cursor": cursor,
                "total_matched": result.total_matched,
                "elapsed_us": elapsed_us,
            })).into_response()
        }
        Err(e) => {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            ).into_response()
        }
    }
}

async fn handle_document(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<DocumentRequest>,
) -> impl IntoResponse {
    let engine = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => Arc::clone(&idx.engine),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    match engine.get_document(req.slot_id) {
        Ok(Some(doc)) => Json(serde_json::json!({"fields": doc.fields})).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "not found"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    }
}

async fn handle_documents_batch(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
    Json(req): Json<DocumentBatchRequest>,
) -> impl IntoResponse {
    let engine = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => Arc::clone(&idx.engine),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    let mut docs = Vec::with_capacity(req.slot_ids.len());
    for slot_id in &req.slot_ids {
        match engine.get_document(*slot_id) {
            Ok(Some(doc)) => docs.push(serde_json::json!({"slot_id": slot_id, "fields": doc.fields})),
            Ok(None) => docs.push(serde_json::json!({"slot_id": slot_id, "fields": null})),
            Err(_) => docs.push(serde_json::json!({"slot_id": slot_id, "fields": null})),
        }
    }
    Json(serde_json::json!({"documents": docs})).into_response()
}

async fn handle_stats(
    State(state): State<SharedState>,
    AxumPath(name): AxumPath<String>,
) -> impl IntoResponse {
    let engine = {
        let guard = state.index.lock();
        match guard.as_ref() {
            Some(idx) if idx.definition.name == name => Arc::clone(&idx.engine),
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": format!("Index '{}' not found", name)})),
                ).into_response();
            }
        }
    };

    let (entries, hits, misses, rebuilds) = engine.bound_cache_stats();
    Json(serde_json::json!({
        "alive_count": engine.alive_count(),
        "slot_count": engine.slot_counter(),
        "bound_cache_entries": entries,
        "bound_cache_hits": hits,
        "bound_cache_misses": misses,
        "bound_cache_rebuilds": rebuilds,
    })).into_response()
}

// ---------------------------------------------------------------------------
// Handlers: Utility
// ---------------------------------------------------------------------------

async fn handle_health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn handle_ui() -> impl IntoResponse {
    Html(include_str!("../static/index.html"))
}
