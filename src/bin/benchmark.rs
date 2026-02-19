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
//!   --stages <LIST>   Comma-separated stages to run: insert,update,query,all (default: all)

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use bitdex_v2::config::{Config, FilterFieldConfig, SortFieldConfig};
use bitdex_v2::engine::Engine;
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::mutation::{Document, FieldValue};
use bitdex_v2::query::{FilterClause, SortClause, SortDirection, Value};

// ---------------------------------------------------------------------------
// NDJSON record definition
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
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
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut data_path: Option<PathBuf> = None;
    let mut limit: Option<usize> = None;
    let mut json_output = false;
    let mut stages = vec!["all".to_string()];

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

    Args { data_path, limit, json_output, stages }
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

fn format_rate(count: usize, elapsed: Duration) -> String {
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
    elapsed_ms: f64,
    rate_per_sec: f64,
    rss_after_bytes: u64,
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
struct MemorySnapshot {
    stage: String,
    rss_bytes: u64,
    rss_human: String,
    alive_count: u64,
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
    println!("Data:   {}", args.data_path.display());
    println!("Limit:  {}", args.limit.map_or("all".to_string(), |n| n.to_string()));
    println!("Stages: {:?}", args.stages);
    println!();

    // -----------------------------------------------------------------------
    // Phase 1: Parse NDJSON
    // -----------------------------------------------------------------------
    println!("--- Phase 1: Parsing NDJSON ---");
    let parse_start = Instant::now();

    let file = File::open(&args.data_path).expect("Failed to open data file");
    let reader = BufReader::with_capacity(8 * 1024 * 1024, file); // 8MB buffer

    let mut records: Vec<NdjsonRecord> = Vec::new();
    let mut parse_errors = 0usize;
    let limit = args.limit.unwrap_or(usize::MAX);

    for line_result in reader.lines() {
        if records.len() >= limit {
            break;
        }
        let line = match line_result {
            Ok(l) => l,
            Err(_) => { parse_errors += 1; continue; }
        };
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<NdjsonRecord>(&line) {
            Ok(rec) => records.push(rec),
            Err(_) => { parse_errors += 1; }
        }
    }

    let parse_elapsed = parse_start.elapsed();
    let total_records = records.len();

    println!("  Parsed {} records in {:.2}s ({} errors)",
        total_records,
        parse_elapsed.as_secs_f64(),
        parse_errors
    );
    println!("  Parse rate: {}", format_rate(total_records, parse_elapsed));
    println!("  RSS after parse: {}", format_bytes(rss_bytes()));
    println!();

    let mut report = BenchmarkReport {
        dataset: DatasetInfo {
            path: args.data_path.display().to_string(),
            total_records,
            records_loaded: total_records,
            parse_time_ms: parse_elapsed.as_secs_f64() * 1000.0,
        },
        insert_benchmarks: Vec::new(),
        update_benchmark: None,
        query_benchmarks: Vec::new(),
        memory_snapshots: vec![MemorySnapshot {
            stage: "after_parse".into(),
            rss_bytes: rss_bytes(),
            rss_human: format_bytes(rss_bytes()),
            alive_count: 0,
        }],
    };

    // -----------------------------------------------------------------------
    // Phase 2: Insert benchmarks at varying batch sizes
    // -----------------------------------------------------------------------
    if should_run(&args.stages, "insert") {
        println!("--- Phase 2: Insert Benchmarks ---");

        let batch_sizes: Vec<usize> = vec![1_000, 10_000, 100_000, 500_000, 1_000_000, total_records]
            .into_iter()
            .filter(|&s| s <= total_records)
            .collect();

        // Deduplicate the last entry if total_records already matches a threshold
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

            let mut engine = Engine::new(civitai_config()).unwrap();

            let start = Instant::now();
            for rec in records.iter().take(batch_size) {
                let id = rec.id as u32;
                let doc = rec.to_document();
                engine.put(id, &doc).unwrap();
            }
            let elapsed = start.elapsed();
            let rss = rss_bytes();

            let rate = batch_size as f64 / elapsed.as_secs_f64();

            println!("  [{:>12}] {:.2}s  ({:.0}/s)  RSS: {}  alive: {}",
                label,
                elapsed.as_secs_f64(),
                rate,
                format_bytes(rss),
                engine.alive_count()
            );

            report.insert_benchmarks.push(InsertBenchmark {
                batch_label: label.clone(),
                record_count: batch_size,
                elapsed_ms: elapsed.as_secs_f64() * 1000.0,
                rate_per_sec: rate,
                rss_after_bytes: rss,
            });

            report.memory_snapshots.push(MemorySnapshot {
                stage: format!("insert_{}", label),
                rss_bytes: rss,
                rss_human: format_bytes(rss),
                alive_count: engine.alive_count(),
            });
        }
        println!();
    }

    // -----------------------------------------------------------------------
    // Phase 3: Create the full engine for update + query benchmarks
    // -----------------------------------------------------------------------
    println!("--- Building full engine for update/query benchmarks ---");
    let mut engine = Engine::new(civitai_config()).unwrap();
    let build_start = Instant::now();
    for rec in &records {
        let id = rec.id as u32;
        let doc = rec.to_document();
        engine.put(id, &doc).unwrap();
    }
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
    // Phase 4: Update/re-insert benchmark (upsert on existing records)
    // -----------------------------------------------------------------------
    if should_run(&args.stages, "update") {
        println!("--- Phase 4: Update (Re-insert/Upsert) Benchmark ---");

        let update_count = total_records.min(100_000);
        let start = Instant::now();
        for rec in records.iter().take(update_count) {
            let id = rec.id as u32;
            let doc = rec.to_document();
            engine.put(id, &doc).unwrap();
        }
        let elapsed = start.elapsed();
        let rate = update_count as f64 / elapsed.as_secs_f64();

        println!("  Re-inserted {} records in {:.2}s ({:.0}/s)",
            update_count, elapsed.as_secs_f64(), rate);
        println!("  Alive after upsert: {} (should be unchanged)", engine.alive_count());
        println!();

        report.update_benchmark = Some(UpdateBenchmark {
            record_count: update_count,
            elapsed_ms: elapsed.as_secs_f64() * 1000.0,
            rate_per_sec: rate,
        });
    }

    // -----------------------------------------------------------------------
    // Phase 5: Query benchmarks
    // -----------------------------------------------------------------------
    if should_run(&args.stages, "query") {
        println!("--- Phase 5: Query Benchmarks ---");
        println!();

        // Collect some real values from the data for realistic queries
        let empty_u64_vec: Vec<u64> = Vec::new();
        let sample_tag_ids: Vec<i64> = records.iter().take(1000)
            .flat_map(|r| r.tag_ids.as_ref().unwrap_or(&empty_u64_vec).iter().copied())
            .map(|v| v as i64)
            .take(500)
            .collect();

        // Get the first userId that appears frequently
        let mut user_freq: HashMap<i64, usize> = HashMap::new();
        for r in records.iter().take(100_000) {
            if let Some(uid) = r.user_id {
                *user_freq.entry(uid as i64).or_default() += 1;
            }
        }
        let frequent_user_id = user_freq.iter()
            .max_by_key(|(_, &count)| count)
            .map(|(&uid, _)| uid)
            .unwrap_or(1);

        // Collect a popular tag
        let mut tag_freq: HashMap<i64, usize> = HashMap::new();
        for r in records.iter().take(100_000) {
            if let Some(ref tags) = r.tag_ids {
                for &t in tags {
                    *tag_freq.entry(t as i64).or_default() += 1;
                }
            }
        }
        let popular_tag = tag_freq.iter()
            .max_by_key(|(_, &count)| count)
            .map(|(&tid, _)| tid)
            .unwrap_or(304);
        let medium_tag = tag_freq.iter()
            .filter(|(_, &count)| count > 100 && count < 5000)
            .next()
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
            // These share the same nsfwLevel=1 + onSite=true prefix but differ in the third clause
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

        // Warm-up: run each query once before measuring
        for q in &queries {
            let _ = engine.query(&q.filters, q.sort.as_ref(), q.limit);
        }

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
