//! BitDex V2 HTTP Server
//!
//! Loads Civitai image data from NDJSON and serves it via HTTP + web UI.
//! Data persists across restarts (bitmaps + docstore on disk by default).
//!
//! Usage:
//!   cargo run --release --bin server -- [OPTIONS]
//!
//! Options:
//!   --data <PATH>          Path to images NDJSON file (default: auto-detect)
//!   --port <N>             HTTP port (default: 3000)
//!   --in-memory-docstore   Fully ephemeral — no persistence
//!   --limit <N>            Max records to load (default: all)
//!   --fresh                Wipe existing data and reload from NDJSON

#[global_allocator]
static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

use std::collections::HashMap;
use std::fs::File;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::{get, post};
use axum::Router;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{Config, FilterFieldConfig, SortFieldConfig};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::mutation::{Document, FieldValue};
use bitdex_v2::query::{BitdexQuery, Value};

// ---------------------------------------------------------------------------
// Civitai-specific data schema (not part of the bitdex library)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct NdjsonRecord {
    id: u64,
    // -- Indexed fields (bitmap filter/sort) --
    post_id: Option<u64>,
    user_id: Option<u64>,
    nsfw_level: Option<u64>,
    #[serde(rename = "type")]
    image_type: Option<String>,
    base_model: Option<String>,
    has_meta: Option<bool>,
    on_site: Option<bool>,
    poi: Option<bool>,
    minor: Option<bool>,
    sort_at: Option<u64>,
    sort_at_unix: Option<u64>,
    published_at: Option<u64>,
    published_at_unix: Option<u64>,
    reaction_count: Option<u64>,
    comment_count: Option<u64>,
    collected_count: Option<u64>,
    tag_ids: Option<Vec<u64>>,
    model_version_ids: Option<Vec<u64>>,
    tool_ids: Option<Vec<u64>>,
    technique_ids: Option<Vec<u64>>,
    width: Option<u64>,
    height: Option<u64>,
    // -- Doc-only fields (stored but not indexed) --
    url: Option<String>,
    hash: Option<String>,
    availability: Option<String>,
    #[serde(alias = "combinedNsfwLevel")]
    combined_nsfw_level: Option<u64>,
    ai_nsfw_level: Option<u64>,
    index: Option<u64>,
    posted_to_id: Option<u64>,
    hide_meta: Option<bool>,
    needs_review: Option<serde_json::Value>,
    has_positive_prompt: Option<bool>,
    acceptable_minor: Option<bool>,
    blocked_for: Option<serde_json::Value>,
    remix_of_id: Option<serde_json::Value>,
    existed_at_unix: Option<u64>,
    collections: Option<Vec<u64>>,
    model_version_ids_manual: Option<Vec<u64>>,
    #[serde(alias = "promptNsfw")]
    prompt_nsfw: Option<bool>,
}

impl NdjsonRecord {
    fn to_document(&self) -> Document {
        let mut fields = HashMap::new();

        // -- Indexed fields (bitmap filter/sort) --
        if let Some(v) = self.nsfw_level.or(self.combined_nsfw_level) {
            fields.insert("nsfwLevel".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        if let Some(v) = self.user_id {
            fields.insert("userId".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        if let Some(ref v) = self.image_type {
            fields.insert("type".into(), FieldValue::Single(Value::Integer(type_to_int(v))));
        }
        if let Some(v) = self.has_meta {
            fields.insert("hasMeta".into(), FieldValue::Single(Value::Bool(v)));
        }
        if let Some(v) = self.on_site {
            fields.insert("onSite".into(), FieldValue::Single(Value::Bool(v)));
        }
        if let Some(v) = self.poi {
            fields.insert("poi".into(), FieldValue::Single(Value::Bool(v)));
        }
        if let Some(v) = self.minor {
            fields.insert("minor".into(), FieldValue::Single(Value::Bool(v)));
        }
        if let Some(ref tags) = self.tag_ids {
            if !tags.is_empty() {
                fields.insert(
                    "tagIds".into(),
                    FieldValue::Multi(tags.iter().map(|&t| Value::Integer(t as i64)).collect()),
                );
            }
        }
        if let Some(ref mv) = self.model_version_ids {
            if !mv.is_empty() {
                fields.insert(
                    "modelVersionIds".into(),
                    FieldValue::Multi(mv.iter().map(|&v| Value::Integer(v as i64)).collect()),
                );
            }
        }
        if let Some(ref t) = self.tool_ids {
            if !t.is_empty() {
                fields.insert(
                    "toolIds".into(),
                    FieldValue::Multi(t.iter().map(|&v| Value::Integer(v as i64)).collect()),
                );
            }
        }
        if let Some(ref t) = self.technique_ids {
            if !t.is_empty() {
                fields.insert(
                    "techniqueIds".into(),
                    FieldValue::Multi(t.iter().map(|&v| Value::Integer(v as i64)).collect()),
                );
            }
        }
        if let Some(v) = self.reaction_count {
            fields.insert("reactionCount".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        // Prefer sortAtUnix, fall back to sortAt (may already be unix timestamp)
        let sort_val = self.sort_at_unix.or(self.sort_at);
        if let Some(v) = sort_val {
            fields.insert("sortAt".into(), FieldValue::Single(Value::Integer((v as u32) as i64)));
        }
        if let Some(v) = self.comment_count {
            fields.insert("commentCount".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        if let Some(v) = self.collected_count {
            fields.insert("collectedCount".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        fields.insert("id".into(), FieldValue::Single(Value::Integer(self.id as i64)));

        // -- Doc-only fields (stored in docstore, not bitmap-indexed) --
        // These are fields not in the filter/sort config, so the engine just
        // stores them in the docstore for retrieval via /api/document.
        if let Some(ref v) = self.url {
            fields.insert("url".into(), FieldValue::Single(Value::String(v.clone())));
        }
        if let Some(ref v) = self.hash {
            fields.insert("hash".into(), FieldValue::Single(Value::String(v.clone())));
        }
        if let Some(v) = self.width {
            fields.insert("width".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        if let Some(v) = self.height {
            fields.insert("height".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        if let Some(ref v) = self.availability {
            fields.insert("availability".into(), FieldValue::Single(Value::String(v.clone())));
        }
        if let Some(ref v) = self.base_model {
            fields.insert("baseModel".into(), FieldValue::Single(Value::String(v.clone())));
        }
        if let Some(v) = self.post_id {
            fields.insert("postId".into(), FieldValue::Single(Value::Integer(v as i64)));
        }

        Document { fields }
    }
}

fn type_to_int(t: &str) -> i64 {
    match t {
        "image" => 1,
        "video" => 2,
        "audio" => 3,
        _ => 0,
    }
}

fn civitai_config() -> Config {
    Config {
        filter_fields: vec![
            FilterFieldConfig { name: "nsfwLevel".into(), field_type: FilterFieldType::SingleValue, storage: Default::default(), behaviors: None },
            FilterFieldConfig { name: "userId".into(), field_type: FilterFieldType::SingleValue, storage: Default::default(), behaviors: None },
            FilterFieldConfig { name: "type".into(), field_type: FilterFieldType::SingleValue, storage: Default::default(), behaviors: None },
            FilterFieldConfig { name: "hasMeta".into(), field_type: FilterFieldType::Boolean, storage: Default::default(), behaviors: None },
            FilterFieldConfig { name: "onSite".into(), field_type: FilterFieldType::Boolean, storage: Default::default(), behaviors: None },
            FilterFieldConfig { name: "poi".into(), field_type: FilterFieldType::Boolean, storage: Default::default(), behaviors: None },
            FilterFieldConfig { name: "minor".into(), field_type: FilterFieldType::Boolean, storage: Default::default(), behaviors: None },
            FilterFieldConfig { name: "tagIds".into(), field_type: FilterFieldType::MultiValue, storage: Default::default(), behaviors: None },
            FilterFieldConfig { name: "modelVersionIds".into(), field_type: FilterFieldType::MultiValue, storage: Default::default(), behaviors: None },
            FilterFieldConfig { name: "toolIds".into(), field_type: FilterFieldType::MultiValue, storage: Default::default(), behaviors: None },
            FilterFieldConfig { name: "techniqueIds".into(), field_type: FilterFieldType::MultiValue, storage: Default::default(), behaviors: None },
        ],
        sort_fields: vec![
            SortFieldConfig { name: "reactionCount".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32 },
            SortFieldConfig { name: "sortAt".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32 },
            SortFieldConfig { name: "commentCount".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32 },
            SortFieldConfig { name: "collectedCount".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32 },
            SortFieldConfig { name: "id".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32 },
        ],
        max_page_size: 200,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

struct Args {
    data_path: Option<PathBuf>,
    port: u16,
    in_memory_docstore: bool,
    limit: Option<usize>,
    threads: usize,
    fresh: bool,
}

fn auto_detect_data() -> Option<PathBuf> {
    let candidates = [
        // Prefer v2 (full Meilisearch fields including url/hash)
        PathBuf::from(r"C:\Dev\Repos\open-source\bitdex\data\images-full-v2.ndjson"),
        PathBuf::from(r"C:\Dev\Repos\open-source\bitdex\data\images-full.ndjson"),
        PathBuf::from(r"C:\Dev\Repos\open-source\bitdex\data\images.ndjson"),
        PathBuf::from("data/images.ndjson"),
    ];
    for c in &candidates {
        if c.exists() { return Some(c.clone()); }
    }
    None
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut data_path: Option<PathBuf> = None;
    let mut port: u16 = 3000;
    let mut in_memory_docstore = false;
    let mut limit: Option<usize> = None;
    let mut threads: usize = 4;
    let mut fresh = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--data" => { i += 1; data_path = Some(PathBuf::from(&args[i])); }
            "--port" => { i += 1; port = args[i].parse().expect("--port must be a number"); }
            "--in-memory-docstore" => { in_memory_docstore = true; }
            "--limit" => { i += 1; limit = Some(args[i].parse().expect("--limit must be a number")); }
            "--threads" => { i += 1; threads = args[i].parse().expect("--threads must be a number"); }
            "--fresh" => { fresh = true; }
            other => { eprintln!("Unknown argument: {other}"); std::process::exit(1); }
        }
        i += 1;
    }

    Args { data_path, port, in_memory_docstore, limit, threads, fresh }
}

// ---------------------------------------------------------------------------
// Bulk loading (pipelined: reader thread → channel → rayon parallel parse)
// ---------------------------------------------------------------------------

fn memrchr_newline(data: &[u8]) -> Option<usize> {
    data.iter().rposition(|&b| b == b'\n')
}

fn load_data(engine: &ConcurrentEngine, data_path: &Path, limit: Option<usize>, num_threads: usize) {
    println!("Loading data from: {}", data_path.display());

    let record_limit = limit.unwrap_or(usize::MAX);
    // Bitmap chunk size stays at 5M for efficient parallel decompose/merge/apply.
    // Memory is bounded by writing docs to docstore inline per-block (before accumulation),
    // so Documents only live in memory from parse until the next bitmap chunk flush.
    let chunk_size: usize = if record_limit < 5_000_000 { record_limit } else { 5_000_000 };
    let read_batch_size: usize = 500_000;
    let target_batch_bytes = read_batch_size * 600;

    let mut staging = engine.clone_staging();

    let data_path_owned = data_path.to_owned();
    let (block_tx, block_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(2);

    let reader_handle = thread::spawn(move || {
        let file = File::open(&data_path_owned).expect("Failed to open data file");
        let mut reader = std::io::BufReader::with_capacity(16 * 1024 * 1024, file);
        let mut buf = vec![0u8; 4 * 1024 * 1024];
        let mut accum = Vec::<u8>::with_capacity(target_batch_bytes + 4 * 1024 * 1024);

        loop {
            let bytes_read = reader.read(&mut buf).unwrap_or(0);
            if bytes_read == 0 {
                if !accum.is_empty() {
                    let _ = block_tx.send(accum);
                }
                break;
            }
            accum.extend_from_slice(&buf[..bytes_read]);

            if accum.len() >= target_batch_bytes {
                if let Some(last_nl) = memrchr_newline(&accum) {
                    let remainder = accum[last_nl + 1..].to_vec();
                    accum.truncate(last_nl + 1);
                    let batch = std::mem::replace(&mut accum, Vec::with_capacity(target_batch_bytes + 4 * 1024 * 1024));
                    accum = remainder; // leftover becomes new accum start
                    if block_tx.send(batch).is_err() { break; }
                }
            }
        }
    });

    // Set of indexed field names — doc-only fields (url, hash, etc.) are stripped from
    // the bitmap chunk accumulator after docstore write to cut memory by ~60%.
    let indexed_fields: std::collections::HashSet<String> = {
        let config = civitai_config();
        let mut s = std::collections::HashSet::new();
        for f in &config.filter_fields { s.insert(f.name.clone()); }
        for f in &config.sort_fields { s.insert(f.name.clone()); }
        s.insert("id".to_string());
        s
    };

    let mut doc_chunk: Vec<(u32, Document)> = Vec::with_capacity(chunk_size);
    let mut id_counter: u32 = 0;
    let mut total_inserted: usize = 0;
    let mut chunks_processed: usize = 0;
    let wall_start = Instant::now();
    let mut ds_handles: Vec<thread::JoinHandle<()>> = Vec::new();

    while let Ok(raw_block) = block_rx.recv() {
        if total_inserted >= record_limit { break; }

        let block_str = std::str::from_utf8(&raw_block).expect("NDJSON not valid UTF-8");
        let base_id = id_counter;

        let lines: Vec<&str> = block_str.split('\n')
            .map(|l| l.trim_end_matches('\r'))
            .filter(|l| !l.is_empty())
            .collect();
        let line_count = lines.len() as u32;

        let mut parsed: Vec<(u32, Document)> = lines.into_par_iter()
            .enumerate()
            .filter_map(|(i, line)| {
                serde_json::from_str::<NdjsonRecord>(line).ok().map(|rec| {
                    let id = base_id + i as u32;
                    (id, rec.to_document())
                })
            })
            .collect();
        id_counter += line_count;

        // Respect limit
        if total_inserted + parsed.len() > record_limit {
            parsed.truncate(record_limit - total_inserted);
        }

        // Extract only indexed fields for the bitmap chunk accumulator (integers/bools, cheap).
        // Full Documents (with url/hash strings) are moved to the docstore writer with no clone.
        let stripped: Vec<(u32, Document)> = parsed.iter().map(|(id, doc)| {
            let fields = doc.fields.iter()
                .filter(|(k, _)| indexed_fields.contains(k.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            (*id, Document { fields })
        }).collect();

        // Move full docs to background docstore writer — no clone, no string duplication.
        // Wait for previous write to complete before spawning next (max 1 in-flight).
        // This bounds peak Document memory to ~1 block (~500K records, ~250MB).
        if let Some(h) = ds_handles.pop() {
            h.join().unwrap();
        }
        ds_handles.push(engine.spawn_docstore_writer(parsed));

        doc_chunk.extend(stripped);

        if doc_chunk.len() >= chunk_size {
            let count = engine.put_bulk_loading(&mut staging, &doc_chunk, num_threads);
            total_inserted += count;
            chunks_processed += 1;
            let elapsed = wall_start.elapsed();
            let rate = total_inserted as f64 / elapsed.as_secs_f64();
            println!("  chunk {}: {} total ({:.0}/s)", chunks_processed, total_inserted, rate);
            doc_chunk = Vec::with_capacity(chunk_size);
        }
    }

    if !doc_chunk.is_empty() {
        let count = engine.put_bulk_loading(&mut staging, &doc_chunk, num_threads);
        total_inserted += count;
        chunks_processed += 1;
        let rate = total_inserted as f64 / wall_start.elapsed().as_secs_f64();
        println!("  chunk {}: {} total ({:.0}/s)", chunks_processed, total_inserted, rate);
    }

    // Drop the receiver so the reader thread's send() returns Err and it stops
    // (otherwise it keeps reading the full file even after we hit the limit)
    drop(block_rx);
    reader_handle.join().unwrap();

    // Wait for all outstanding docstore writes
    for h in ds_handles {
        h.join().unwrap();
    }
    println!("Loaded {} records in {} chunks", total_inserted, chunks_processed);

    let publish_start = Instant::now();
    engine.publish_staging(staging);
    let publish_elapsed = publish_start.elapsed();

    let wall_elapsed = wall_start.elapsed();
    let rate = total_inserted as f64 / wall_elapsed.as_secs_f64();
    println!(
        "Loaded {} records in {:.1}s ({:.0}/s), publish: {:.2}s, alive: {}",
        total_inserted, wall_elapsed.as_secs_f64(), rate,
        publish_elapsed.as_secs_f64(), engine.alive_count()
    );
}

// ---------------------------------------------------------------------------
// API types
// ---------------------------------------------------------------------------


#[derive(Deserialize)]
struct DocumentRequest {
    slot_id: u32,
}

#[derive(Deserialize)]
struct DocumentBatchRequest {
    slot_ids: Vec<u32>,
}

#[derive(Serialize)]
struct StatsResponse {
    alive_count: u64,
    slot_count: u32,
    bound_cache_entries: usize,
    bound_cache_hits: usize,
    bound_cache_misses: usize,
    bound_cache_rebuilds: usize,
}

#[derive(Serialize)]
struct ConfigResponse {
    filter_fields: Vec<FilterFieldInfo>,
    sort_fields: Vec<String>,
    max_page_size: usize,
}

#[derive(Serialize)]
struct FilterFieldInfo {
    name: String,
    field_type: String,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

type AppState = Arc<ConcurrentEngine>;

async fn handle_query(
    State(engine): State<AppState>,
    Json(query): Json<BitdexQuery>,
) -> impl IntoResponse {
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
            (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e.to_string() }))).into_response()
        }
    }
}

async fn handle_document(
    State(engine): State<AppState>,
    Json(req): Json<DocumentRequest>,
) -> impl IntoResponse {
    match engine.get_document(req.slot_id) {
        Ok(Some(doc)) => Json(serde_json::json!({ "fields": doc.fields })).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "not found" }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

async fn handle_documents_batch(
    State(engine): State<AppState>,
    Json(req): Json<DocumentBatchRequest>,
) -> impl IntoResponse {
    let mut docs = Vec::with_capacity(req.slot_ids.len());
    for slot_id in &req.slot_ids {
        match engine.get_document(*slot_id) {
            Ok(Some(doc)) => docs.push(serde_json::json!({ "slot_id": slot_id, "fields": doc.fields })),
            Ok(None) => docs.push(serde_json::json!({ "slot_id": slot_id, "fields": null })),
            Err(_) => docs.push(serde_json::json!({ "slot_id": slot_id, "fields": null })),
        }
    }
    Json(serde_json::json!({ "documents": docs }))
}

async fn handle_stats(State(engine): State<AppState>) -> impl IntoResponse {
    let (entries, hits, misses, rebuilds) = engine.bound_cache_stats();
    Json(StatsResponse {
        alive_count: engine.alive_count(),
        slot_count: engine.slot_counter(),
        bound_cache_entries: entries,
        bound_cache_hits: hits,
        bound_cache_misses: misses,
        bound_cache_rebuilds: rebuilds,
    })
}

async fn handle_config() -> impl IntoResponse {
    let config = civitai_config();
    let filter_fields: Vec<FilterFieldInfo> = config.filter_fields.iter().map(|f| {
        FilterFieldInfo {
            name: f.name.clone(),
            field_type: match f.field_type {
                FilterFieldType::SingleValue => "SingleValue".to_string(),
                FilterFieldType::MultiValue => "MultiValue".to_string(),
                FilterFieldType::Boolean => "Boolean".to_string(),
            },
        }
    }).collect();
    let sort_fields: Vec<String> = config.sort_fields.iter().map(|f| f.name.clone()).collect();
    Json(ConfigResponse { filter_fields, sort_fields, max_page_size: config.max_page_size })
}

async fn handle_health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn handle_index() -> impl IntoResponse {
    Html(include_str!("../../static/index.html"))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let args = parse_args();

    let docstore_path = PathBuf::from("server_docstore.redb");
    let bitmap_path = PathBuf::from("server_bitmaps.redb");

    println!("BitDex V2 Server");
    println!("  port: {}", args.port);
    println!("  threads: {}", args.threads);
    if let Some(limit) = args.limit {
        println!("  limit: {}", limit);
    }
    if args.fresh {
        println!("  --fresh: wiping existing data");
        std::fs::remove_file(&docstore_path).ok();
        std::fs::remove_file(&bitmap_path).ok();
        // redb may leave .lock files
        std::fs::remove_file(docstore_path.with_extension("redb.lock")).ok();
        std::fs::remove_file(bitmap_path.with_extension("redb.lock")).ok();
    }

    // Build config with bitmap persistence enabled (unless in-memory mode)
    let mut config = civitai_config();
    if !args.in_memory_docstore {
        config.storage.bitmap_path = Some(bitmap_path.clone());
    }

    let engine = if args.in_memory_docstore {
        println!("  mode: in-memory (no persistence)");
        ConcurrentEngine::new(config).expect("Failed to create engine")
    } else {
        println!("  docstore: {}", docstore_path.display());
        println!("  bitmaps:  {}", bitmap_path.display());
        ConcurrentEngine::new_with_path(config, &docstore_path).expect("Failed to create engine")
    };

    // Check if data was restored from disk
    let alive = engine.alive_count();
    if alive > 0 {
        println!("Restored {} records from disk — skipping NDJSON load", alive);
    } else {
        // Need to load from NDJSON
        let data_path = args.data_path
            .or_else(auto_detect_data)
            .expect("No persisted data found and no NDJSON file available. Use --data <PATH>");

        println!("  data: {}", data_path.display());
        load_data(&engine, &data_path, args.limit, args.threads);

        // Persist bitmap snapshot for fast restart
        if !args.in_memory_docstore {
            let snap_start = Instant::now();
            engine.save_snapshot().expect("Failed to save bitmap snapshot");
            println!("Bitmap snapshot saved in {:.1}s", snap_start.elapsed().as_secs_f64());
        }
    }

    let state: AppState = Arc::new(engine);

    let app = Router::new()
        .route("/", get(handle_index))
        .route("/api/query", post(handle_query))
        .route("/api/document", post(handle_document))
        .route("/api/documents", post(handle_documents_batch))
        .route("/api/stats", get(handle_stats))
        .route("/api/config", get(handle_config))
        .route("/api/health", get(handle_health))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", args.port);
    println!("\nServer listening on http://localhost:{}", args.port);

    let listener = tokio::net::TcpListener::bind(&addr).await.expect("Failed to bind");
    axum::serve(listener, app).await.expect("Server failed");
}
