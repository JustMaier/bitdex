//! Bitdex V2 Benchmark Harness
//!
//! Loads real Civitai image data from an NDJSON file and measures insert, update,
//! and query performance at scale.
//!
//! Usage:
//!   cargo run --release --bin benchmark -- [OPTIONS]
//!
//! Options:
//!   --data <PATH>     Path to images.ndjson (default: auto-detect)
//!   --limit <N>       Max records to load (default: all)
//!   --json            Output machine-readable JSON report
//!   --stages <LIST>   Comma-separated stages to run: insert,update,query,concurrent,mixed,all (default: all)
//!   --threads <N>     Number of threads for concurrent benchmarks (default: 1, >1 uses ConcurrentEngine)

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{Config, FilterFieldConfig, SortFieldConfig};
use bitdex_v2::engine::Engine;
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::mutation::{Document, FieldValue};
use bitdex_v2::query::{FilterClause, SortClause, SortDirection, Value};

// ---------------------------------------------------------------------------
// NDJSON record definition
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct NdjsonRecord {
    id: u64,
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
    prompt_nsfw: Option<bool>,
    sort_at: Option<u64>,
    published_at: Option<u64>,
    reaction_count: Option<u64>,
    comment_count: Option<u64>,
    collected_count: Option<u64>,
    tag_ids: Option<Vec<u64>>,
    model_version_ids: Option<Vec<u64>>,
    tool_ids: Option<Vec<u64>>,
    technique_ids: Option<Vec<u64>>,
    width: Option<u64>,
    height: Option<u64>,
}

impl NdjsonRecord {
    fn to_document(&self) -> Document {
        let mut fields = HashMap::new();

        if let Some(v) = self.nsfw_level {
            fields.insert("nsfwLevel".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        if let Some(v) = self.user_id {
            // Truncate userId to u32 range for filter bitmap keys
            fields.insert("userId".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        if let Some(ref v) = self.image_type {
            // Hash the type string to a u64 key for filter storage
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
        // Sort fields
        if let Some(v) = self.reaction_count {
            fields.insert("reactionCount".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        if let Some(v) = self.sort_at {
            // Truncate to u32 for sort layers (lower 32 bits preserves relative ordering
            // for recent timestamps within a reasonable window)
            fields.insert("sortAt".into(), FieldValue::Single(Value::Integer((v as u32) as i64)));
        }
        if let Some(v) = self.comment_count {
            fields.insert("commentCount".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        if let Some(v) = self.collected_count {
            fields.insert("collectedCount".into(), FieldValue::Single(Value::Integer(v as i64)));
        }
        // Use the record id itself as a sort field
        fields.insert("id".into(), FieldValue::Single(Value::Integer(self.id as i64)));

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

// ---------------------------------------------------------------------------
// Config matching the Civitai schema
// ---------------------------------------------------------------------------

fn civitai_config() -> Config {
    Config {
        filter_fields: vec![
            FilterFieldConfig { name: "nsfwLevel".into(), field_type: FilterFieldType::SingleValue },
            FilterFieldConfig { name: "userId".into(), field_type: FilterFieldType::SingleValue },
            FilterFieldConfig { name: "type".into(), field_type: FilterFieldType::SingleValue },
            FilterFieldConfig { name: "hasMeta".into(), field_type: FilterFieldType::Boolean },
            FilterFieldConfig { name: "onSite".into(), field_type: FilterFieldType::Boolean },
            FilterFieldConfig { name: "poi".into(), field_type: FilterFieldType::Boolean },
            FilterFieldConfig { name: "minor".into(), field_type: FilterFieldType::Boolean },
            FilterFieldConfig { name: "tagIds".into(), field_type: FilterFieldType::MultiValue },
            FilterFieldConfig { name: "modelVersionIds".into(), field_type: FilterFieldType::MultiValue },
            FilterFieldConfig { name: "toolIds".into(), field_type: FilterFieldType::MultiValue },
            FilterFieldConfig { name: "techniqueIds".into(), field_type: FilterFieldType::MultiValue },
        ],
        sort_fields: vec![
            SortFieldConfig { name: "reactionCount".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32 },
            SortFieldConfig { name: "sortAt".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32 },
            SortFieldConfig { name: "commentCount".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32 },
            SortFieldConfig { name: "collectedCount".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32 },
            SortFieldConfig { name: "id".into(), source_type: "uint32".into(), encoding: "linear".into(), bits: 32 },
        ],
        max_page_size: 100,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// CLI arg parsing (minimal, no extra dependencies)
// ---------------------------------------------------------------------------

struct Args {
    data_path: PathBuf,
    limit: Option<usize>,
    json_output: bool,
    stages: Vec<String>,
    threads: usize,
    channel_capacity: usize,
    flush_interval_us: u64,
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut data_path: Option<PathBuf> = None;
    let mut limit: Option<usize> = None;
    let mut json_output = false;
    let mut stages = vec!["all".to_string()];
    let mut threads: usize = 1;
    let mut channel_capacity: usize = 0; // 0 = auto
    let mut flush_interval_us: u64 = 100;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--data" => {
                i += 1;
                data_path = Some(PathBuf::from(&args[i]));
            }
            "--limit" => {
                i += 1;
                limit = Some(args[i].parse().expect("--limit must be a number"));
            }
            "--json" => {
                json_output = true;
            }
            "--stages" => {
                i += 1;
                stages = args[i].split(',').map(|s| s.trim().to_string()).collect();
            }
            "--threads" => {
                i += 1;
                threads = args[i].parse().expect("--threads must be a number");
                if threads == 0 { threads = 1; }
            }
            "--channel-capacity" => {
                i += 1;
                channel_capacity = args[i].parse().expect("--channel-capacity must be a number");
            }
            "--flush-interval-us" => {
                i += 1;
                flush_interval_us = args[i].parse().expect("--flush-interval-us must be a number");
            }
            other => {
                eprintln!("Unknown argument: {other}");
                std::process::exit(1);
            }
        }
        i += 1;
    }

    // Auto-detect data path
    let data_path = data_path.unwrap_or_else(|| {
        let candidates = [
            PathBuf::from(r"C:\Dev\Repos\open-source\bitdex\data\images.ndjson"),
            PathBuf::from("data/images.ndjson"),
            PathBuf::from("../bitdex/data/images.ndjson"),
        ];
        for c in &candidates {
            if c.exists() {
                return c.clone();
            }
        }
        eprintln!("Could not find images.ndjson. Use --data <PATH> to specify.");
        std::process::exit(1);
    });

    Args { data_path, limit, json_output, stages, threads, channel_capacity, flush_interval_us }
}

fn should_run(stages: &[String], name: &str) -> bool {
    stages.iter().any(|s| s == "all" || s == name)
}

// ---------------------------------------------------------------------------
// Latency stats
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize)]
struct LatencyStats {
    count: usize,
    total_ms: f64,
    min_ms: f64,
    max_ms: f64,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

fn compute_stats(mut durations: Vec<Duration>) -> LatencyStats {
    assert!(!durations.is_empty());
    durations.sort();
    let count = durations.len();
    let total: Duration = durations.iter().sum();
    let total_ms = total.as_secs_f64() * 1000.0;
    let min_ms = durations[0].as_secs_f64() * 1000.0;
    let max_ms = durations[count - 1].as_secs_f64() * 1000.0;
    let mean_ms = total_ms / count as f64;

    let p = |pct: f64| -> f64 {
        let idx = ((pct / 100.0) * count as f64).ceil() as usize;
        let idx = idx.min(count).saturating_sub(1);
        durations[idx].as_secs_f64() * 1000.0
    };

    LatencyStats {
        count,
        total_ms,
        min_ms,
        max_ms,
        mean_ms,
        p50_ms: p(50.0),
        p95_ms: p(95.0),
        p99_ms: p(99.0),
    }
}

// ---------------------------------------------------------------------------
// Memory tracking
// ---------------------------------------------------------------------------

fn rss_bytes() -> u64 {
    #[cfg(target_os = "windows")]
    {
        use std::mem::MaybeUninit;
        // Use Windows API to get working set size
        unsafe {
            let process = windows_process_handle();
            let mut pmc: MaybeUninit<PROCESS_MEMORY_COUNTERS> = MaybeUninit::zeroed();
            if GetProcessMemoryInfo(process, pmc.as_mut_ptr(), std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32) != 0 {
                (*pmc.as_ptr()).working_set_size as u64
            } else {
                0
            }
        }
    }
    #[cfg(target_os = "linux")]
    {
        // Read from /proc/self/statm
        if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
            if let Some(rss_pages) = statm.split_whitespace().nth(1) {
                if let Ok(pages) = rss_pages.parse::<u64>() {
                    return pages * 4096;
                }
            }
        }
        0
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux")))]
    {
        0
    }
}

#[cfg(target_os = "windows")]
#[repr(C)]
#[allow(non_snake_case)]
struct PROCESS_MEMORY_COUNTERS {
    cb: u32,
    page_fault_count: u32,
    peak_working_set_size: usize,
    working_set_size: usize,
    quota_peak_paged_pool_usage: usize,
    quota_paged_pool_usage: usize,
    quota_peak_non_paged_pool_usage: usize,
    quota_non_paged_pool_usage: usize,
    pagefile_usage: usize,
    peak_pagefile_usage: usize,
}

#[cfg(target_os = "windows")]
extern "system" {
    fn GetCurrentProcess() -> isize;
}

#[cfg(target_os = "windows")]
#[link(name = "psapi")]
extern "system" {
    fn GetProcessMemoryInfo(process: isize, ppsmemCounters: *mut PROCESS_MEMORY_COUNTERS, cb: u32) -> i32;
}

#[cfg(target_os = "windows")]
unsafe fn windows_process_handle() -> isize {
    GetCurrentProcess()
}

fn format_bytes(b: u64) -> String {
    if b >= 1 << 30 {
        format!("{:.2} GB", b as f64 / (1u64 << 30) as f64)
    } else if b >= 1 << 20 {
        format!("{:.2} MB", b as f64 / (1u64 << 20) as f64)
    } else if b >= 1 << 10 {
        format!("{:.2} KB", b as f64 / (1u64 << 10) as f64)
    } else {
        format!("{b} B")
    }
}

fn _format_rate(count: usize, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64();
    if secs == 0.0 {
        return "inf".to_string();
    }
    let rate = count as f64 / secs;
    if rate >= 1_000_000.0 {
        format!("{:.2}M/s", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1}K/s", rate / 1_000.0)
    } else {
        format!("{:.0}/s", rate)
    }
}

// ---------------------------------------------------------------------------
// Benchmark report structures (for JSON output)
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Serialize)]
struct BenchmarkReport {
    dataset: DatasetInfo,
    insert_benchmarks: Vec<InsertBenchmark>,
    update_benchmark: Option<UpdateBenchmark>,
    query_benchmarks: Vec<QueryBenchmark>,
    concurrent_insert_benchmark: Option<ConcurrentInsertBenchmark>,
    mixed_rw_benchmark: Option<MixedRwBenchmark>,
    memory_snapshots: Vec<MemorySnapshot>,
}

#[derive(Debug, serde::Serialize)]
struct DatasetInfo {
    path: String,
    total_records: usize,
    records_loaded: usize,
    parse_time_ms: f64,
}

#[derive(Debug, serde::Serialize)]
struct InsertBenchmark {
    batch_label: String,
    record_count: usize,
    insert_ms: f64,
    wall_ms: f64,
    insert_rate_per_sec: f64,
    rss_before_bytes: u64,
    rss_after_bytes: u64,
    rss_delta_bytes: u64,
}

#[derive(Debug, serde::Serialize)]
struct UpdateBenchmark {
    record_count: usize,
    elapsed_ms: f64,
    rate_per_sec: f64,
}

#[derive(Debug, serde::Serialize)]
struct QueryBenchmark {
    name: String,
    description: String,
    iterations: usize,
    stats: LatencyStats,
}

#[derive(Debug, serde::Serialize)]
struct ConcurrentInsertBenchmark {
    threads: usize,
    record_count: usize,
    wall_ms: f64,
    total_docs_per_sec: f64,
    per_thread_docs_per_sec: f64,
    alive_after: u64,
    rss_before_bytes: u64,
    rss_after_bytes: u64,
}

#[derive(Debug, serde::Serialize)]
struct MixedRwBenchmark {
    writer_threads: usize,
    reader_threads: usize,
    records_inserted: usize,
    queries_executed: usize,
    wall_ms: f64,
    insert_rate_per_sec: f64,
    query_stats: LatencyStats,
}

#[derive(Debug, serde::Serialize)]
struct MemorySnapshot {
    stage: String,
    rss_bytes: u64,
    rss_human: String,
    alive_count: u64,
}

// ---------------------------------------------------------------------------
// Streaming helpers — re-read the NDJSON file for each phase instead of
// holding millions of parsed records in RAM.
// ---------------------------------------------------------------------------

/// Count total records in the file (raw byte scan -- just counts newlines).
fn count_records(path: &PathBuf, limit: usize) -> usize {
    use std::io::Read;
    let file = File::open(path).expect("Failed to open data file");
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut buf = [0u8; 64 * 1024];
    let mut count = 0usize;
    loop {
        if count >= limit { break; }
        let bytes_read = reader.read(&mut buf).unwrap_or(0);
        if bytes_read == 0 { break; }
        for &b in &buf[..bytes_read] {
            if b == b'\n' {
                count += 1;
                if count >= limit { break; }
            }
        }
    }
    count
}

/// Stream records from the NDJSON file, calling `f` for each parsed record.
/// Stops after `limit` successful records. Returns (records_processed, parse_errors).
fn stream_records<F>(path: &PathBuf, limit: usize, mut f: F) -> (usize, usize)
where
    F: FnMut(&NdjsonRecord),
{
    let file = File::open(path).expect("Failed to open data file");
    let reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let mut count = 0usize;
    let mut errors = 0usize;
    for line_result in reader.lines() {
        if count >= limit { break; }
        let line = match line_result {
            Ok(l) => l,
            Err(_) => { errors += 1; continue; }
        };
        if line.is_empty() { continue; }
        match serde_json::from_str::<NdjsonRecord>(&line) {
            Ok(rec) => { f(&rec); count += 1; }
            Err(_) => { errors += 1; }
        }
    }
    (count, errors)
}

/// Load records into a Vec for concurrent benchmarks (needs pre-parsed data
/// so chunks can be distributed to threads).
fn load_records(path: &PathBuf, limit: usize) -> Vec<(u32, Document)> {
    let mut records = Vec::new();
    stream_records(path, limit, |rec| {
        records.push((rec.id as u32, rec.to_document()));
    });
    records
}

/// Wait for the ConcurrentEngine flush thread to catch up.
fn wait_for_flush(engine: &ConcurrentEngine, expected_alive: u64, max_ms: u64) {
    let deadline = Instant::now() + Duration::from_millis(max_ms);
    while Instant::now() < deadline {
        if engine.alive_count() >= expected_alive {
            thread::sleep(Duration::from_millis(5));
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let args = parse_args();

    println!("==========================================================");
    println!("  Bitdex V2 Benchmark Harness");
    println!("==========================================================");
    println!();
    println!("Data:       {}", args.data_path.display());
    println!("Limit:      {}", args.limit.map_or("all".to_string(), |n| n.to_string()));
    println!("Threads:    {}", args.threads);
    println!("Channel:    {}", if args.channel_capacity > 0 { args.channel_capacity.to_string() } else { "auto".to_string() });
    println!("Flush us:   {}", args.flush_interval_us);
    println!("Stages:     {:?}", args.stages);
    println!();

    let limit = args.limit.unwrap_or(usize::MAX);

    // -----------------------------------------------------------------------
    // Phase 1: Count records (quick scan, no full parse into memory)
    // -----------------------------------------------------------------------
    println!("--- Phase 1: Counting records ---");
    let count_start = Instant::now();
    let total_records = count_records(&args.data_path, limit);
    let count_elapsed = count_start.elapsed();
    println!("  {} records in {:.2}s", total_records, count_elapsed.as_secs_f64());
    println!("  RSS after count: {}", format_bytes(rss_bytes()));
    println!();

    let mut report = BenchmarkReport {
        dataset: DatasetInfo {
            path: args.data_path.display().to_string(),
            total_records,
            records_loaded: total_records,
            parse_time_ms: count_elapsed.as_secs_f64() * 1000.0,
        },
        insert_benchmarks: Vec::new(),
        update_benchmark: None,
        query_benchmarks: Vec::new(),
        concurrent_insert_benchmark: None,
        mixed_rw_benchmark: None,
        memory_snapshots: vec![MemorySnapshot {
            stage: "before_insert".into(),
            rss_bytes: rss_bytes(),
            rss_human: format_bytes(rss_bytes()),
            alive_count: 0,
        }],
    };

    // -----------------------------------------------------------------------
    // Phase 2: Single-threaded insert benchmarks at varying batch sizes
    // -----------------------------------------------------------------------
    if should_run(&args.stages, "insert") {
        println!("--- Phase 2: Insert Benchmarks (single-threaded, Engine) ---");

        let batch_sizes: Vec<usize> = vec![1_000, 10_000, 100_000, 500_000, 1_000_000, total_records]
            .into_iter()
            .filter(|&s| s <= total_records)
            .collect();

        let batch_sizes: Vec<usize> = {
            let mut v = batch_sizes;
            v.dedup();
            v
        };

        for &batch_size in &batch_sizes {
            let label = if batch_size == total_records {
                format!("all ({})", total_records)
            } else {
                format!("{}", batch_size)
            };

            let rss_before = rss_bytes();
            let mut engine = Engine::new(civitai_config()).unwrap();
            let mut insert_time = Duration::ZERO;

            let wall_start = Instant::now();
            stream_records(&args.data_path, batch_size, |rec| {
                let id = rec.id as u32;
                let doc = rec.to_document();
                let put_start = Instant::now();
                engine.put(id, &doc).unwrap();
                insert_time += put_start.elapsed();
            });
            let wall_elapsed = wall_start.elapsed();
            let rss_after = rss_bytes();
            let rss_delta = rss_after.saturating_sub(rss_before);

            let insert_rate = batch_size as f64 / insert_time.as_secs_f64();

            println!("  [{:>12}] insert: {:.2}s  wall: {:.2}s  ({:.0}/s)  RSS: {} (+{})  alive: {}",
                label,
                insert_time.as_secs_f64(),
                wall_elapsed.as_secs_f64(),
                insert_rate,
                format_bytes(rss_after),
                format_bytes(rss_delta),
                engine.alive_count()
            );

            report.insert_benchmarks.push(InsertBenchmark {
                batch_label: label.clone(),
                record_count: batch_size,
                insert_ms: insert_time.as_secs_f64() * 1000.0,
                wall_ms: wall_elapsed.as_secs_f64() * 1000.0,
                insert_rate_per_sec: insert_rate,
                rss_before_bytes: rss_before,
                rss_after_bytes: rss_after,
                rss_delta_bytes: rss_delta,
            });

            report.memory_snapshots.push(MemorySnapshot {
                stage: format!("insert_{}", label),
                rss_bytes: rss_after,
                rss_human: format_bytes(rss_after),
                alive_count: engine.alive_count(),
            });
        }
        println!();
    }

    // -----------------------------------------------------------------------
    // Phase 2b: Concurrent insert benchmark (ConcurrentEngine, N threads)
    // -----------------------------------------------------------------------
    if args.threads > 1 && should_run(&args.stages, "concurrent") {
        println!("--- Phase 2b: Concurrent Insert Benchmark ({} threads, ConcurrentEngine) ---", args.threads);
        println!("  Loading records into memory for thread distribution...");

        let load_start = Instant::now();
        let records = load_records(&args.data_path, total_records);
        let load_elapsed = load_start.elapsed();
        println!("  Loaded {} records in {:.2}s (parse + to_document)", records.len(), load_elapsed.as_secs_f64());

        let rss_before = rss_bytes();
        // Use tunable config for concurrent benchmarks
        let mut config = civitai_config();
        // Auto-size channel capacity: ~50 ops per doc * batch_count to avoid backpressure
        if args.channel_capacity > 0 {
            config.channel_capacity = args.channel_capacity;
        } else {
            config.channel_capacity = (records.len() * 50).max(100_000).min(10_000_000);
        }
        config.flush_interval_us = args.flush_interval_us;
        println!("  Channel capacity: {}, flush interval: {}us", config.channel_capacity, config.flush_interval_us);
        let engine = Arc::new(ConcurrentEngine::new(config).unwrap());

        // Split records into chunks for each thread
        let chunk_size = (records.len() + args.threads - 1) / args.threads;
        let chunks: Vec<Vec<(u32, Document)>> = records
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect();

        let total_inserted = Arc::new(AtomicUsize::new(0));

        println!("  Inserting with {} threads ({} records/thread avg, auto-coalesced)...", args.threads, chunk_size);
        let wall_start = Instant::now();

        let handles: Vec<_> = chunks
            .into_iter()
            .map(|chunk| {
                let engine = Arc::clone(&engine);
                let counter = Arc::clone(&total_inserted);
                thread::spawn(move || {
                    let mut count = 0usize;
                    // Simple put() calls — docstore writes are auto-coalesced by the flush thread
                    for (id, doc) in &chunk {
                        engine.put(*id, doc).unwrap();
                        count += 1;
                    }
                    counter.fetch_add(count, Ordering::Relaxed);
                    count
                })
            })
            .collect();

        let mut per_thread_counts = Vec::new();
        for h in handles {
            per_thread_counts.push(h.join().unwrap());
        }

        let wall_elapsed = wall_start.elapsed();
        let total_count = total_inserted.load(Ordering::Relaxed);

        // Wait for all mutations to flush
        println!("  Waiting for flush thread to catch up...");
        wait_for_flush(&engine, total_count as u64, 30_000);
        let alive = engine.alive_count();

        let rss_after = rss_bytes();
        let total_rate = total_count as f64 / wall_elapsed.as_secs_f64();
        let per_thread_rate = total_rate / args.threads as f64;

        println!("  Concurrent insert complete:");
        println!("    Records:          {}", total_count);
        println!("    Wall time:        {:.2}s", wall_elapsed.as_secs_f64());
        println!("    Total throughput: {:.0} docs/s", total_rate);
        println!("    Per-thread avg:   {:.0} docs/s", per_thread_rate);
        println!("    Alive after:      {}", alive);
        println!("    RSS: {} (delta: {})", format_bytes(rss_after), format_bytes(rss_after.saturating_sub(rss_before)));
        for (i, count) in per_thread_counts.iter().enumerate() {
            println!("    Thread {}: {} records", i, count);
        }
        println!();

        report.concurrent_insert_benchmark = Some(ConcurrentInsertBenchmark {
            threads: args.threads,
            record_count: total_count,
            wall_ms: wall_elapsed.as_secs_f64() * 1000.0,
            total_docs_per_sec: total_rate,
            per_thread_docs_per_sec: per_thread_rate,
            alive_after: alive,
            rss_before_bytes: rss_before,
            rss_after_bytes: rss_after,
        });

        report.memory_snapshots.push(MemorySnapshot {
            stage: format!("concurrent_insert_{}t", args.threads),
            rss_bytes: rss_after,
            rss_human: format_bytes(rss_after),
            alive_count: alive,
        });
    }

    // -----------------------------------------------------------------------
    // Phase 3: Build the full engine (streaming from file, single-threaded)
    // -----------------------------------------------------------------------
    println!("--- Building full engine for update/query benchmarks ---");
    let mut engine = Engine::new(civitai_config()).unwrap();
    let build_start = Instant::now();
    stream_records(&args.data_path, limit, |rec| {
        let id = rec.id as u32;
        let doc = rec.to_document();
        engine.put(id, &doc).unwrap();
    });
    let build_elapsed = build_start.elapsed();
    let rss = rss_bytes();
    println!("  Loaded {} records in {:.2}s", total_records, build_elapsed.as_secs_f64());
    println!("  Alive: {}", engine.alive_count());
    println!("  RSS: {}", format_bytes(rss));
    println!();

    report.memory_snapshots.push(MemorySnapshot {
        stage: "full_engine".into(),
        rss_bytes: rss,
        rss_human: format_bytes(rss),
        alive_count: engine.alive_count(),
    });

    // -----------------------------------------------------------------------
    // Phase 4: Update/re-insert benchmark (re-reads file from top)
    // -----------------------------------------------------------------------
    if should_run(&args.stages, "update") {
        println!("--- Phase 4: Update (Increment reactionCount) Benchmark ---");

        let update_count = total_records.min(100_000);
        let mut update_time = Duration::ZERO;
        let wall_start = Instant::now();
        stream_records(&args.data_path, update_count, |rec| {
            let id = rec.id as u32;
            let mut doc = rec.to_document();
            // Increment reactionCount by 1 to exercise sort layer XOR diff
            if let Some(FieldValue::Single(Value::Integer(ref mut v))) = doc.fields.get_mut("reactionCount") {
                *v += 1;
            }
            let put_start = Instant::now();
            engine.put(id, &doc).unwrap();
            update_time += put_start.elapsed();
        });
        let wall_elapsed = wall_start.elapsed();
        let update_rate = update_count as f64 / update_time.as_secs_f64();

        println!("  Updated {} records in {:.2}s (wall: {:.2}s) ({:.0}/s)",
            update_count, update_time.as_secs_f64(), wall_elapsed.as_secs_f64(), update_rate);
        println!("  Alive after upsert: {} (should be unchanged)", engine.alive_count());
        println!();

        report.update_benchmark = Some(UpdateBenchmark {
            record_count: update_count,
            elapsed_ms: update_time.as_secs_f64() * 1000.0,
            rate_per_sec: update_rate,
        });
    }

    // -----------------------------------------------------------------------
    // Phase 5: Query benchmarks
    // -----------------------------------------------------------------------
    if should_run(&args.stages, "query") {
        println!("--- Phase 5: Query Benchmarks ---");
        println!();

        // Quick streaming pass to collect frequency stats for realistic queries.
        let mut user_freq: HashMap<i64, usize> = HashMap::new();
        let mut tag_freq: HashMap<i64, usize> = HashMap::new();
        let mut sample_tag_ids: Vec<i64> = Vec::new();
        let sample_limit = 100_000.min(total_records);

        stream_records(&args.data_path, sample_limit, |rec| {
            if let Some(uid) = rec.user_id {
                *user_freq.entry(uid as i64).or_default() += 1;
            }
            if let Some(ref tags) = rec.tag_ids {
                for &t in tags {
                    *tag_freq.entry(t as i64).or_default() += 1;
                    if sample_tag_ids.len() < 500 {
                        sample_tag_ids.push(t as i64);
                    }
                }
            }
        });

        let frequent_user_id = user_freq.iter()
            .max_by_key(|(_, &count)| count)
            .map(|(&uid, _)| uid)
            .unwrap_or(1);

        let popular_tag = tag_freq.iter()
            .max_by_key(|(_, &count)| count)
            .map(|(&tid, _)| tid)
            .unwrap_or(304);
        let medium_tag = tag_freq.iter()
            .find(|(_, &count)| count > 100 && count < 5000)
            .map(|(&tid, _)| tid)
            .unwrap_or(5133);

        let iterations = 200;

        struct QuerySpec {
            name: &'static str,
            description: &'static str,
            filters: Vec<FilterClause>,
            sort: Option<SortClause>,
            limit: usize,
        }

        let queries = vec![
            // --- Filter-only queries ---
            QuerySpec {
                name: "filter_eq_nsfwLevel_1",
                description: "Single eq filter on low-cardinality field (nsfwLevel=1)",
                filters: vec![FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
                sort: None,
                limit: 50,
            },
            QuerySpec {
                name: "filter_eq_onSite_true",
                description: "Boolean filter (onSite=true)",
                filters: vec![FilterClause::Eq("onSite".into(), Value::Bool(true))],
                sort: None,
                limit: 50,
            },
            QuerySpec {
                name: "filter_eq_userId",
                description: "Single eq on high-cardinality field (userId)",
                filters: vec![FilterClause::Eq("userId".into(), Value::Integer(frequent_user_id))],
                sort: None,
                limit: 50,
            },
            QuerySpec {
                name: "filter_eq_tagId_popular",
                description: "Single tag filter on popular tag",
                filters: vec![FilterClause::Eq("tagIds".into(), Value::Integer(popular_tag))],
                sort: None,
                limit: 50,
            },
            QuerySpec {
                name: "filter_and_2_clauses",
                description: "AND of nsfwLevel + onSite",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                ],
                sort: None,
                limit: 50,
            },
            QuerySpec {
                name: "filter_and_3_clauses",
                description: "AND of nsfwLevel + onSite + popular tag",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                    FilterClause::Eq("tagIds".into(), Value::Integer(popular_tag)),
                ],
                sort: None,
                limit: 50,
            },
            QuerySpec {
                name: "filter_and_3_with_userId",
                description: "AND of nsfwLevel + onSite + userId (narrow result)",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                    FilterClause::Eq("userId".into(), Value::Integer(frequent_user_id)),
                ],
                sort: None,
                limit: 50,
            },
            QuerySpec {
                name: "filter_in_nsfwLevel",
                description: "IN filter on nsfwLevel with multiple values",
                filters: vec![FilterClause::In("nsfwLevel".into(), vec![
                    Value::Integer(1), Value::Integer(2), Value::Integer(4),
                ])],
                sort: None,
                limit: 50,
            },
            QuerySpec {
                name: "filter_not_eq_nsfwLevel",
                description: "NOT nsfwLevel=28 (large result set via andnot)",
                filters: vec![FilterClause::NotEq("nsfwLevel".into(), Value::Integer(28))],
                sort: None,
                limit: 50,
            },
            // --- Filter + sort queries ---
            QuerySpec {
                name: "sort_reactionCount_desc",
                description: "All records sorted by reactionCount descending",
                filters: vec![],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            QuerySpec {
                name: "filter_nsfw1_sort_reactions",
                description: "nsfwLevel=1 sorted by reactionCount desc",
                filters: vec![FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            QuerySpec {
                name: "filter_nsfw1_onSite_sort_reactions",
                description: "nsfwLevel=1 + onSite sorted by reactionCount desc",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                ],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            QuerySpec {
                name: "filter_tag_sort_reactions",
                description: "Popular tag sorted by reactionCount desc",
                filters: vec![FilterClause::Eq("tagIds".into(), Value::Integer(popular_tag))],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            QuerySpec {
                name: "filter_3_clauses_sort_reactions",
                description: "nsfwLevel=1 + onSite + tag sorted by reactionCount",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                    FilterClause::Eq("tagIds".into(), Value::Integer(popular_tag)),
                ],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            QuerySpec {
                name: "filter_sort_commentCount",
                description: "nsfwLevel=1 sorted by commentCount desc",
                filters: vec![FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
                sort: Some(SortClause { field: "commentCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            QuerySpec {
                name: "filter_sort_id_asc",
                description: "nsfwLevel=1 sorted by id ascending (newest last)",
                filters: vec![FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
                sort: Some(SortClause { field: "id".into(), direction: SortDirection::Asc }),
                limit: 50,
            },
            // --- Queries with repeated prefixes (cache testing) ---
            QuerySpec {
                name: "prefix_shared_A",
                description: "[Cache prefix] nsfwLevel=1 + onSite + tag(popular)",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                    FilterClause::Eq("tagIds".into(), Value::Integer(popular_tag)),
                ],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            QuerySpec {
                name: "prefix_shared_B",
                description: "[Cache prefix] nsfwLevel=1 + onSite + tag(medium)",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                    FilterClause::Eq("tagIds".into(), Value::Integer(medium_tag)),
                ],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            QuerySpec {
                name: "prefix_shared_C",
                description: "[Cache prefix] nsfwLevel=1 + onSite + userId",
                filters: vec![
                    FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                    FilterClause::Eq("onSite".into(), Value::Bool(true)),
                    FilterClause::Eq("userId".into(), Value::Integer(frequent_user_id)),
                ],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
            // --- Wide OR query ---
            QuerySpec {
                name: "filter_or_3_tags",
                description: "OR of 3 different tags sorted by reactionCount",
                filters: vec![FilterClause::Or(vec![
                    FilterClause::Eq("tagIds".into(), Value::Integer(popular_tag)),
                    FilterClause::Eq("tagIds".into(), Value::Integer(medium_tag)),
                    FilterClause::Eq("tagIds".into(), Value::Integer(
                        sample_tag_ids.get(10).copied().unwrap_or(304)
                    )),
                ])],
                sort: Some(SortClause { field: "reactionCount".into(), direction: SortDirection::Desc }),
                limit: 50,
            },
        ];

        // Warm-up: run each query 10 times to populate trie cache
        let warmup_passes = 10;
        println!("  Warming up ({} passes x {} queries)...", warmup_passes, queries.len());
        for _ in 0..warmup_passes {
            for q in &queries {
                let _ = engine.query(&q.filters, q.sort.as_ref(), q.limit);
            }
        }
        println!();

        // Run benchmarks
        println!("  {:<40} {:>8} {:>8} {:>8} {:>8} {:>8}",
            "Query", "p50", "p95", "p99", "mean", "count");
        println!("  {}", "-".repeat(82));

        for q in &queries {
            let mut durations = Vec::with_capacity(iterations);
            for _ in 0..iterations {
                let start = Instant::now();
                let result = engine.query(&q.filters, q.sort.as_ref(), q.limit);
                let elapsed = start.elapsed();
                // Ensure the query succeeded
                let _ = result.unwrap();
                durations.push(elapsed);
            }

            let stats = compute_stats(durations);

            println!("  {:<40} {:>7.3} {:>7.3} {:>7.3} {:>7.3}ms {:>5}",
                q.name,
                stats.p50_ms,
                stats.p95_ms,
                stats.p99_ms,
                stats.mean_ms,
                stats.count,
            );

            report.query_benchmarks.push(QueryBenchmark {
                name: q.name.to_string(),
                description: q.description.to_string(),
                iterations,
                stats,
            });
        }
        println!();
    }

    // -----------------------------------------------------------------------
    // Phase 6: Mixed read/write benchmark (ConcurrentEngine)
    // Some threads insert while others query concurrently
    // -----------------------------------------------------------------------
    if args.threads > 1 && should_run(&args.stages, "mixed") {
        println!("--- Phase 6: Mixed Read/Write Benchmark ({} threads, ConcurrentEngine) ---", args.threads);

        // Use half threads for writing, half for reading (min 1 each)
        let writer_threads = (args.threads / 2).max(1);
        let reader_threads = (args.threads - writer_threads).max(1);

        // Load a subset of records for writing (use 50K or total if less)
        let mixed_record_count = total_records.min(50_000);
        println!("  Loading {} records for mixed benchmark...", mixed_record_count);
        let records = load_records(&args.data_path, mixed_record_count);

        let engine = Arc::new(ConcurrentEngine::new(civitai_config()).unwrap());

        // Pre-populate with half the records so readers have data to query
        let prepop_count = records.len() / 2;
        for (id, doc) in &records[..prepop_count] {
            engine.put(*id, doc).unwrap();
        }
        wait_for_flush(&engine, prepop_count as u64, 10_000);
        println!("  Pre-populated {} records, alive: {}", prepop_count, engine.alive_count());

        // The remaining records will be inserted by writers during the mixed phase
        let write_records: Vec<(u32, Document)> = records[prepop_count..].to_vec();
        let write_chunk_size = (write_records.len() + writer_threads - 1) / writer_threads;
        let write_chunks: Vec<Vec<(u32, Document)>> = write_records
            .chunks(write_chunk_size)
            .map(|c| c.to_vec())
            .collect();

        let total_queries = Arc::new(AtomicUsize::new(0));
        let total_writes = Arc::new(AtomicUsize::new(0));
        let all_query_durations: Arc<parking_lot::Mutex<Vec<Duration>>> =
            Arc::new(parking_lot::Mutex::new(Vec::new()));

        let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

        println!("  Running mixed workload: {} writer threads, {} reader threads...", writer_threads, reader_threads);
        let wall_start = Instant::now();

        // Spawn writer threads
        let mut handles = Vec::new();
        for chunk in write_chunks {
            let engine = Arc::clone(&engine);
            let counter = Arc::clone(&total_writes);
            let stop = Arc::clone(&stop_flag);
            handles.push(thread::spawn(move || {
                for (id, doc) in &chunk {
                    if stop.load(Ordering::Relaxed) { break; }
                    engine.put(*id, doc).unwrap();
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        // Spawn reader threads
        for _ in 0..reader_threads {
            let engine = Arc::clone(&engine);
            let counter = Arc::clone(&total_queries);
            let durations = Arc::clone(&all_query_durations);
            let stop = Arc::clone(&stop_flag);
            handles.push(thread::spawn(move || {
                let query_patterns: Vec<Vec<FilterClause>> = vec![
                    vec![FilterClause::Eq("nsfwLevel".into(), Value::Integer(1))],
                    vec![FilterClause::Eq("onSite".into(), Value::Bool(true))],
                    vec![
                        FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
                        FilterClause::Eq("onSite".into(), Value::Bool(true)),
                    ],
                    vec![FilterClause::Eq("hasMeta".into(), Value::Bool(true))],
                ];
                let sort = SortClause {
                    field: "reactionCount".into(),
                    direction: SortDirection::Desc,
                };
                let mut local_durations = Vec::new();
                let mut idx = 0;
                while !stop.load(Ordering::Relaxed) {
                    let filters = &query_patterns[idx % query_patterns.len()];
                    let start = Instant::now();
                    let result = engine.query(filters, Some(&sort), 50);
                    let elapsed = start.elapsed();
                    let _ = result; // query may return partial results during concurrent writes
                    local_durations.push(elapsed);
                    counter.fetch_add(1, Ordering::Relaxed);
                    idx += 1;
                }
                durations.lock().extend(local_durations);
            }));
        }

        // Wait for writer threads to finish (they're bounded by the chunk size)
        // Reader threads run until stop_flag is set
        // Wait for the first N handles (writers)
        for h in handles.drain(..writer_threads.min(handles.len())) {
            h.join().unwrap();
        }

        // Signal readers to stop
        stop_flag.store(true, Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }

        let wall_elapsed = wall_start.elapsed();
        let writes = total_writes.load(Ordering::Relaxed);
        let queries = total_queries.load(Ordering::Relaxed);

        // Wait for flush
        wait_for_flush(&engine, (prepop_count + writes) as u64, 10_000);

        let insert_rate = writes as f64 / wall_elapsed.as_secs_f64();

        let query_durations = Arc::try_unwrap(all_query_durations)
            .unwrap_or_else(|arc| arc.lock().clone().into())
            .into_inner();

        println!("  Mixed workload complete:");
        println!("    Wall time:     {:.2}s", wall_elapsed.as_secs_f64());
        println!("    Records inserted: {} ({:.0} docs/s)", writes, insert_rate);
        println!("    Queries executed: {}", queries);
        println!("    Alive after:   {}", engine.alive_count());

        if !query_durations.is_empty() {
            let stats = compute_stats(query_durations);
            println!("    Query latency under concurrent writes:");
            println!("      p50: {:.3}ms  p95: {:.3}ms  p99: {:.3}ms  mean: {:.3}ms",
                stats.p50_ms, stats.p95_ms, stats.p99_ms, stats.mean_ms);

            report.mixed_rw_benchmark = Some(MixedRwBenchmark {
                writer_threads,
                reader_threads,
                records_inserted: writes,
                queries_executed: queries,
                wall_ms: wall_elapsed.as_secs_f64() * 1000.0,
                insert_rate_per_sec: insert_rate,
                query_stats: stats,
            });
        }
        println!();
    }

    // -----------------------------------------------------------------------
    // Final memory snapshot
    // -----------------------------------------------------------------------
    let final_rss = rss_bytes();
    report.memory_snapshots.push(MemorySnapshot {
        stage: "final".into(),
        rss_bytes: final_rss,
        rss_human: format_bytes(final_rss),
        alive_count: engine.alive_count(),
    });

    println!("--- Final State ---");
    println!("  Alive documents: {}", engine.alive_count());
    println!("  Slot counter:    {}", engine.slot_counter());
    println!("  RSS:             {}", format_bytes(final_rss));
    println!();

    // -----------------------------------------------------------------------
    // JSON output
    // -----------------------------------------------------------------------
    if args.json_output {
        let json = serde_json::to_string_pretty(&report).unwrap();
        let out_path = PathBuf::from("benchmark_report.json");
        std::fs::write(&out_path, &json).expect("Failed to write JSON report");
        println!("JSON report written to: {}", out_path.display());
    }

    println!("==========================================================");
    println!("  Benchmark complete.");
    println!("==========================================================");
}
