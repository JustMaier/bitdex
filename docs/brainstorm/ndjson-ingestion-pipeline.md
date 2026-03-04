# High-Speed NDJSON Ingestion Pipeline

## Status: Brainstorm Proposal (with measured benchmarks)

## Problem Statement

Current ingestion is single-threaded and achieves ~62K docs/s for the 104.6M-record Civitai dataset (59 GB NDJSON). This means ~28 minutes just for `put()` calls. The theoretical NVMe I/O ceiling is ~3 GB/s, which would read the entire file in ~20 seconds. We are **100x slower than the I/O ceiling**, leaving massive room for improvement.

Target: **300K docs/s** (5-7 minutes for 104M records).

## MEASURED RESULTS (5M records, 3.74 GB, images.ndjson, 16-core / 32-thread CPU)

Benchmark binary: `src/bin/parse_throughput.rs`

### Raw parse throughput (no channel, threads operate independently)

| Threads | Parse Only | Parse + to_document() |
|---------|-----------|----------------------|
| 1 | 436K/s (1.0x) | 287K/s (0.7x) |
| 8 | 2.63M/s (6.0x) | 1.25M/s (2.9x) |
| 16 | 4.60M/s (10.6x) | 2.58M/s (5.9x) |
| 24 | 5.75M/s (13.2x) | 3.44M/s (7.9x) |
| 32 | 5.97M/s (13.7x) | 3.71M/s (8.5x) |

Scaling is near-linear through 16 physical cores. Hyperthreading adds ~40% more at 32 threads.

### Channel-based pipeline (parsers -> bounded channel -> consumers)

| Config | Rate | Notes |
|--------|------|-------|
| 4P -> 1C (bounded 8192) | **857K/s** | Single consumer can handle 4 parsers |
| 8P -> 1C (bounded 8192) | **211K/s** | Single consumer overwhelmed, producer contention |
| 8P -> 1C (batched 256) | **663K/s** | Batching reduces channel ops, big improvement |
| 8P -> 4C (cap 16384) | **1.25M/s** | Sweet spot for 8 parsers |
| **16P -> 8C (cap 16384)** | **1.77M/s** | **Best channel config tested** |
| 16P -> 16C (cap 16384) | **862K/s** | Too many consumers = receive-side contention |
| 24P -> 8C (cap 16384) | **440K/s** | Too many parsers for 8 consumers |

### Key findings from measurements

1. **Single-threaded parse already exceeds 300K/s** (436K/s parse-only). The 62K/s reported in benchmarks includes engine.put() overhead. Parsing is NOT the bottleneck.

2. **to_document() adds ~34% overhead** (436K -> 287K). HashMap allocation + field value construction is significant. With 2+ parallel threads this is absorbed.

3. **Without channels, scaling is excellent**: 16 threads = 2.58M/s for full parse+convert. 32 threads = 3.71M/s. CPU work parallelizes cleanly.

4. **Channel contention is the critical pipeline design issue**:
   - Single consumer caps at ~850K/s with 4 parsers and *degrades* beyond that.
   - Batched sends help (663K/s vs 211K/s at 8P) by reducing channel operations.
   - Multi-consumer is the real fix: 16P -> 8C = 1.77M/s.
   - Optimal parser:consumer ratio is ~2:1. Exceeding it in either direction hurts.

5. **Parsing + conversion ceiling**: ~3.7M/s at 32 threads. The write path (62K/s) is **50-60x slower**.

6. **In the real pipeline, parser threads can call engine.put() directly** — no intermediate channel needed. The coalescer channel inside ConcurrentEngine serves as the pipeline's channel. So the relevant number is the raw parallel parse+convert: **2.58M/s at 16 threads**.

---

## 1. Current Bottleneck Analysis

The current pipeline in `src/bin/benchmark.rs` is a single-threaded, sequential chain:

```
File::open() → BufReader(8 MB) → .lines() → serde_json::from_str() → to_document() → engine.put()
```

### Stage-by-stage cost breakdown (estimated per record)

| Stage | Operation | Est. Cost | Bottleneck? |
|-------|-----------|-----------|-------------|
| I/O | `BufReader::lines()` | ~1 us | No — 8 MB buffer is adequate for sequential read |
| Line alloc | `.lines()` returns `String` (heap alloc per line) | ~0.5 us | Minor — but 104M allocs add up |
| JSON parse | `serde_json::from_str::<NdjsonRecord>()` | ~3-5 us | **Yes** — serde_json is ~300-500 MB/s on structured data |
| Conversion | `rec.to_document()` — builds HashMap, converts types | ~1-2 us | Minor — HashMap + Vec allocs |
| Diff compute | `diff_document()` — computes MutationOps | ~1 us | Minor — pure computation |
| Channel send | `sender.send_batch(ops)` | ~0.1 us | No — crossbeam is fast |
| Docstore send | `doc_tx.send((id, stored))` | ~0.1 us | No |
| **Docstore write** | redb write txn (flush thread) | ~5-10 us | **Yes** — serialized, single-writer |
| **Bitmap apply** | flush thread applies MutationOps | ~2-5 us | Moderate — depends on batch coalescing |

### Dominant bottlenecks

1. **JSON parsing (~3-5 us/record)**: serde_json processes ~300-500 MB/s of structured JSON. At ~564 bytes/record average (59 GB / 104.6M), this is the single-thread CPU-bound limit. Parallelizing parsing alone could 4-8x throughput.

2. **Docstore writes**: Even with batching in the flush thread, redb's single-writer model serializes all writes. At 104M records, docstore write time is significant.

3. **String allocation**: `BufReader::lines()` allocates a new `String` per line. At 104M lines, this is ~104M heap allocations that the allocator must manage.

4. **HashMap construction in `to_document()`**: Each document creates a new `HashMap<String, FieldValue>` with ~15 field entries. The String keys are allocated fresh each time despite being the same field names every record.

### What is NOT a bottleneck

- **File I/O**: NVMe sequential reads at 3 GB/s dwarf our processing rate. The 8 MB BufReader is sufficient.
- **Channel sends**: crossbeam-channel is sub-microsecond.
- **Bitmap computation** (`diff_document`): Pure arithmetic, fast.

---

## 2. Parallel File Reading Strategies

### 2A. Memory-Mapped I/O with Parallel Chunk Processing

```rust
use memmap2::MmapOptions;

let file = File::open(&path)?;
let mmap = unsafe { MmapOptions::new().map(&file)? };
let data = &mmap[..];

// Split into N chunks, each thread finds its first newline
let chunk_size = data.len() / num_threads;
let mut boundaries = vec![0usize];
for i in 1..num_threads {
    let start = i * chunk_size;
    // Scan forward to next newline
    let adjusted = memchr::memchr(b'\n', &data[start..])
        .map(|pos| start + pos + 1)
        .unwrap_or(data.len());
    boundaries.push(adjusted);
}
boundaries.push(data.len());
```

**Pros:**
- Zero-copy: OS pages file directly into address space, no read() syscalls
- Kernel readahead is excellent for sequential access patterns
- Each thread gets a `&[u8]` slice — zero coordination between readers
- No line-by-line allocation — each thread works on raw bytes

**Cons:**
- `unsafe` for mmap creation (well-understood, widely used)
- 59 GB file exceeds typical RAM — fine, the OS will page intelligently
- Line boundary adjustment adds ~N newline scans (trivial cost)

**Expected speedup:** Near-linear with thread count for the I/O stage. 8 threads = ~8x I/O throughput.

### 2B. Pre-Split with `memchr` Line Scanning

Rather than `BufReader::lines()`, use `memchr::memchr` to find newlines in large buffers:

```rust
let mut offset = 0;
while offset < chunk.len() {
    let end = memchr::memchr(b'\n', &chunk[offset..])
        .map(|p| offset + p)
        .unwrap_or(chunk.len());
    let line = &chunk[offset..end]; // &[u8], zero-copy
    // Parse directly from &[u8] with serde_json::from_slice()
    offset = end + 1;
}
```

`memchr` uses SIMD (SSE2/AVX2) to scan for newlines at ~16-32 GB/s. This is essentially free compared to any other stage.

### 2C. Recommendation

**Use 2A (mmap) + 2B (memchr line scanning).** The combination gives:
- Zero-copy file access
- SIMD-accelerated line boundary detection
- Each parser thread works on an independent `&[u8]` slice
- No allocator contention between threads

---

## 3. Parallel JSON Parsing

### 3A. Multi-threaded serde_json

The simplest approach: N threads, each with its own chunk from mmap, each calling `serde_json::from_slice::<NdjsonRecord>()`.

**Expected throughput per thread:** ~200-400 MB/s (serde_json on NdjsonRecord).
**With 8 threads:** ~1.6-3.2 GB/s — limited by NVMe read speed at that point.
**Records/s:** At 564 bytes/record: 8 threads * 350K/thread = ~2.8M records/s (parsing only).

This alone would bring parsing from 28 minutes to ~40 seconds. The bottleneck shifts entirely to the write path.

### 3B. SIMD-Accelerated Parsing (simd-json)

The `simd-json` crate uses SIMD instructions (AVX2/SSE4.2) to accelerate structural character detection and number parsing. Benchmarks show 2-4x faster than serde_json for typical JSON workloads.

```toml
simd-json = "0.14"
```

```rust
// simd-json requires mutable byte buffer (modifies in place for performance)
let mut line_buf = chunk[offset..end].to_vec();
let record: NdjsonRecord = simd_json::from_slice(&mut line_buf)?;
```

**Caveats:**
- Requires mutable buffer (copies the line) — negates some zero-copy benefit
- Requires `#[derive(serde::Deserialize)]` (same as serde_json — compatible)
- AVX2 feature detection at runtime is automatic
- Not compatible with `from_str` — must use `from_slice` with `&mut [u8]`

**Expected throughput per thread:** ~600-1200 MB/s.
**With 8 threads:** ~4.8-9.6 GB/s — I/O bound even on fast NVMe.

### 3C. Zero-Copy Parsing

For fields that are just integers, we can avoid full JSON parsing with a custom lightweight parser. The NdjsonRecord has a fixed schema: all fields are integers, booleans, or arrays of integers. No nested objects, no string values (except `type` and `baseModel`).

A custom parser using `serde_json::StreamDeserializer` or hand-rolled SIMD number parsing could avoid allocating intermediate structures entirely. However, the engineering cost is high and the gain over simd-json is marginal (maybe 1.5x).

**Recommendation:** Start with simd-json. Only pursue zero-copy if benchmarks show parsing is still the bottleneck after parallelization.

### 3D. Recommendation

**Use 3A (multi-threaded serde_json) first, benchmark, then try 3B (simd-json) as a drop-in replacement.** The parallelism gives 8x; simd-json gives another 2-4x. Together they far exceed the 300K/s target for the parsing stage alone.

---

## 4. Pipeline Architecture

### Overview

A three-stage pipeline with bounded channels providing backpressure between stages:

```
 [Stage 1: Reader]      [Stage 2: Parsers]      [Stage 3: Writers]

  mmap + memchr    -->   N parser threads   -->   M writer threads
  split into N          serde/simd-json          engine.put()
  byte chunks           to_document()            (already async via channel)

         bounded channel         bounded channel
         (byte slices)           (id, Document)
```

### Stage 1: Reader (1 thread or mmap)

With mmap, there is no dedicated reader thread. Each parser thread gets a pre-computed byte range and reads directly from the memory map.

Alternative without mmap: A single reader thread with 64 MB read buffer, splitting into line batches and distributing to parser threads via a bounded channel.

```rust
// Option A: mmap (preferred)
let mmap = unsafe { MmapOptions::new().map(&file)? };
// Compute chunk boundaries, spawn parser threads with &mmap[start..end]

// Option B: reader thread (fallback for non-mmap systems)
let (line_tx, line_rx) = crossbeam_channel::bounded::<Vec<u8>>(1024);
thread::spawn(move || {
    let mut buf = vec![0u8; 64 * 1024 * 1024];
    // Read into buf, split on newlines, send line batches
});
```

### Stage 2: Parsers (N threads, recommended N = physical cores - 2)

Each parser thread:
1. Reads lines from its mmap chunk (or receives from reader channel)
2. Parses JSON into `NdjsonRecord`
3. Calls `to_document()` to build the `Document`
4. Sends `(id, Document)` to the writer channel

```rust
fn parser_thread(chunk: &[u8], doc_tx: Sender<(u32, Document)>, id_range: Range<u32>) {
    let mut offset = 0;
    let mut id_counter = id_range.start;
    while offset < chunk.len() {
        let end = memchr::memchr(b'\n', &chunk[offset..])
            .map(|p| offset + p)
            .unwrap_or(chunk.len());

        let line = &chunk[offset..end];
        offset = end + 1;
        if line.is_empty() { continue; }

        match serde_json::from_slice::<NdjsonRecord>(line) {
            Ok(rec) => {
                let id = id_counter; // or rec.id as u32
                id_counter += 1;
                let doc = rec.to_document();
                doc_tx.send((id, doc)).unwrap();
            }
            Err(_) => { /* count error */ }
        }
    }
}
```

### Stage 3: Writers (M threads, recommended M = 2-4)

Each writer thread calls `engine.put(id, doc)`. The engine's internal channel + flush thread handles the actual bitmap and docstore writes.

```rust
fn writer_thread(engine: &ConcurrentEngine, doc_rx: Receiver<(u32, Document)>) {
    while let Ok((id, doc)) = doc_rx.recv() {
        engine.put(id, &doc).unwrap();
    }
}
```

### Backpressure

- **Parser-to-Writer channel**: Bounded at ~8K-16K entries. Each `(u32, Document)` is ~500-1000 bytes. At 16K entries: ~16 MB buffer.
- **Backpressure behavior**: When the writer channel is full, parser threads block on `send()`. This naturally throttles parsing to match the write path's capacity.
- **No backpressure on I/O**: mmap-based reading has no explicit buffer — the OS manages page faults. For the reader-thread approach, the reader-to-parser channel (bounded at 1024 line batches) provides the backpressure.

### Thread count recommendations

| System | Parsers (Stage 2) | Writers (Stage 3) | Flush thread | Total |
|--------|-------------------|-------------------|--------------|-------|
| 8-core | 5 | 2 | 1 (existing) | 8 |
| 16-core | 10 | 4 | 1 (existing) | 15 |
| 32-core | 20 | 8 | 1 (existing) | 29 |

The flush thread is the true serialization point. Adding more writer threads past the channel's capacity just adds contention on the crossbeam channel — harmless but pointless.

---

## 5. Reducing Per-Record Allocation Overhead

### 5A. Intern Field Names

Currently `to_document()` allocates 15+ `String` keys per record via `.into()`:
```rust
fields.insert("nsfwLevel".into(), ...);  // allocates String each time
```

Instead, use `Arc<str>` or pre-allocated static strings:
```rust
// Thread-local or static field name set
static FIELD_NSFW_LEVEL: &str = "nsfwLevel";

// Or use the existing FieldRegistry from mutation.rs
let registry = FieldRegistry::from_config(&config);
fields.insert(registry.get("nsfwLevel").to_string(), ...);
```

Better yet, change `Document.fields` from `HashMap<String, FieldValue>` to `HashMap<Arc<str>, FieldValue>` to eliminate all per-record String allocations for field names. This is a larger refactor but saves ~15 allocations/record * 104M = 1.56 billion allocations.

### 5B. Pre-size HashMap

```rust
fn to_document(&self) -> Document {
    let mut fields = HashMap::with_capacity(16); // avoid rehashing
    // ...
}
```

### 5C. Reuse Buffers with Thread-Local Pools

Each parser thread can reuse a `Document` buffer:
```rust
thread_local! {
    static DOC_BUF: RefCell<HashMap<String, FieldValue>> = RefCell::new(HashMap::with_capacity(16));
}
```

This eliminates the per-record HashMap allocation and instead just clears and refills.

---

## 6. Binary Pre-Processed Format

### Motivation

JSON parsing is the CPU-dominant stage. If the NDJSON file is loaded more than once (common during development and benchmarking), we can parse it once and write a binary format for subsequent loads.

### Design

**First load:**
```
NDJSON (59 GB) → parallel parse → Vec<(u32, BinRecord)> → write .bitdex-cache file
```

**Subsequent loads:**
```
.bitdex-cache → mmap → zero-copy deserialize → Vec<(u32, Document)>
```

### Format Options

| Format | Deser speed | Size (est.) | Zero-copy? | Ecosystem |
|--------|------------|-------------|------------|-----------|
| bincode | ~2-5 GB/s | ~25-30 GB | No | Mature, simple |
| rkyv | ~8-15 GB/s | ~30-35 GB | Yes | Fast, zero-copy |
| FlatBuffers | ~10-20 GB/s | ~35-40 GB | Yes | Google-backed |
| Cap'n Proto | ~10-20 GB/s | ~35-40 GB | Yes | Sandstorm |
| Raw struct layout | 3 GB/s (I/O bound) | ~20-25 GB | Yes | Custom |

### Recommended: `rkyv` with mmap

`rkyv` (archive) provides zero-copy deserialization: the archived data is directly interpretable without any deserialization step. You mmap the file and cast pointers.

```toml
rkyv = { version = "0.8", features = ["validation"] }
```

```rust
// Write phase (first load)
let records: Vec<BinRecord> = parse_ndjson_parallel(&path);
let bytes = rkyv::to_bytes::<Vec<BinRecord>>(&records)?;
std::fs::write("images.bitdex-cache", &bytes)?;

// Read phase (subsequent loads)
let mmap = unsafe { MmapOptions::new().map(&File::open("images.bitdex-cache")?)? };
let archived = rkyv::access::<ArchivedVec<ArchivedBinRecord>>(&mmap)?;
// archived[i] is directly usable — no deserialization
```

### Expected Performance

- **First load**: Same as parallel NDJSON parse (~40s for 104M records) + write time (~10-15s)
- **Subsequent loads**: mmap at I/O speed (~7-10s for 25 GB) + zero deser overhead
- **Speedup over NDJSON**: ~3-5x for subsequent loads (parsing eliminated entirely)

### Cache Invalidation

The `.bitdex-cache` file should include a header with:
- Source file hash (first 4 KB + file size + mtime) for staleness detection
- Schema version for forward compatibility
- Record count for validation

---

## 7. Memory Estimates

### Pipeline Buffer Memory

| Buffer | Size | Memory |
|--------|------|--------|
| mmap virtual address space | 59 GB | ~0 GB (demand-paged by OS) |
| mmap resident pages (OS managed) | ~1-4 GB | Kernel decides, shared with page cache |
| Parser-to-Writer channel | 16K entries * 800 B avg | ~13 MB |
| Writer-to-Engine channel (existing) | Configurable, typically 1-10M ops | ~100-500 MB |
| Doc write channel (existing) | Unbounded | Varies |
| **Total pipeline overhead** | | **~13-500 MB** (excluding existing engine channels) |

### Per-Thread Stack + Scratch Memory

| Component | Per Thread | 8 Threads |
|-----------|-----------|-----------|
| Thread stack | 8 MB (default) | 64 MB |
| simd-json scratch buffer | ~line length (~1 KB) | ~8 KB |
| Document build scratch | ~2 KB | ~16 KB |
| **Total per-thread** | ~8 MB | **~64 MB** |

The pipeline's memory overhead is negligible compared to the engine's 6-14 GB bitmap memory.

---

## 8. Realistic Throughput Targets

### Parsing throughput (Stage 2, measured on 5M records / 3.74 GB)

| Strategy | Threads | Measured records/s | Projected wall time (104M) |
|----------|---------|-------------------|---------------------------|
| BufReader + serde_json (parse only) | 1 | 423K | 4.1 min |
| BufReader + serde_json + to_document | 1 | 269K | 6.5 min |
| Parallel serde_json (parse only) | 8 | 2.60M | 40 sec |
| Parallel serde_json + to_document | 8 | 1.66M | 63 sec |
| Parallel + channel (8P -> 4C) | 8+4 | 1.17M | 89 sec |
| Parallel simd-json (estimated) | 8 | ~3-5M | 20-35 sec |
| Binary cache rkyv (estimated) | 1 | ~3-10M | 10-35 sec |

### Write throughput (Stage 3, engine.put())

This is the real ceiling. `engine.put()` in the current architecture involves:
1. `diff_document()` — fast, pure CPU
2. Channel send to coalescer — fast
3. Doc channel send to flush thread — fast
4. **Flush thread**: apply mutations to bitmaps + write docs to redb — **serialized**

Current single-threaded put throughput: ~62K/s. With loading mode (skips snapshot publishing): ~80-100K/s.

The other brainstorm proposals (channel-centric backpressure, bulk bitmap construction, hybrid flush) address the flush thread bottleneck. Without those improvements, the realistic ceiling for the write path is ~100-150K/s.

### End-to-end projections (updated with measured parsing rates, 16-core system)

| Configuration | Parsing (measured) | Write Path | End-to-end | Wall time (104M) |
|---------------|-------------------|------------|------------|------------------|
| Current (single-threaded) | 287K/s (parse+convert) | 62K/s | **62K/s** | ~28 min |
| 8T parse + current write | 1.25M/s | 62K/s | **62K/s** | ~28 min |
| 16T parse + current write | 2.58M/s | 100K/s | **100K/s** | ~17 min |
| 16T parse + improved flush | 2.58M/s | 300K/s | **300K/s** | ~6 min |
| 32T parse + bulk write | 3.71M/s | 500K/s | **500K/s** | ~3.5 min |

**Key insight confirmed by benchmarks:** Parsing is NOT the bottleneck. The write path is 50-60x slower than our parsing capacity. Even if we did zero optimization to parsing, we have massive headroom. With 16 threads parse+convert delivers 2.58M/s — enough to saturate any conceivable write path improvement. The 300K/s target depends entirely on write path improvements (bulk bitmap construction, hybrid flush).

---

## 9. Interaction with Write Path Improvements

The NDJSON pipeline and the write path improvements are **complementary and independent**:

### What this proposal provides
- Parsing throughput that far exceeds the write path ceiling
- Backpressure via bounded channels so parsers naturally throttle to the write path's speed
- The pipeline can saturate any write path improvement without code changes

### What the write path proposals provide
- **Channel-centric backpressure**: Structured channel hierarchy that this pipeline feeds into
- **Bulk bitmap construction**: Batch sort-layer construction that benefits from high-throughput parsing
- **Hybrid flush**: Decoupled merge + publish that can absorb higher write rates

### Integration points

1. **Writer threads call `engine.put()`** — this feeds directly into the existing coalescer channel. No changes needed to the write path interface.

2. **Loading mode** must be entered before the pipeline starts and exited after all writers complete. The pipeline should handle this automatically.

3. **ID assignment**: With parallel parsing, sequential slot IDs need careful handling:
   - **Remap mode** (`--remap-ids`): Pre-compute each chunk's ID range based on estimated record count per chunk. Slight overallocation is fine — gaps in slot IDs are handled by the alive bitmap.
   - **Postgres ID mode**: Each record carries its own ID in the JSON. No coordination needed.

4. **Error handling**: Parse errors in parallel threads should be counted atomically (AtomicUsize) and reported at the end. A per-thread error vector is also fine for detailed diagnostics.

---

## 10. Implementation Plan

### Phase 1: Parallel serde_json with mmap (lowest effort, biggest win)

1. Add `memmap2` and `memchr` to Cargo.toml
2. Write `parallel_stream_records()` function:
   - mmap the file
   - Compute chunk boundaries (one per thread)
   - Spawn parser threads, each scanning lines with memchr and parsing with serde_json
   - Send `(u32, Document)` via bounded crossbeam channel
3. Writer threads recv from channel and call `engine.put()`
4. Wrap in `enter_loading_mode()` / `exit_loading_mode()`
5. Benchmark against current single-threaded path

**Expected outcome:** 3-5x throughput improvement (200-300K/s parsing, engine.put is the ceiling).

### Phase 2: simd-json drop-in (small effort, moderate win)

1. Add `simd-json` to Cargo.toml
2. Replace `serde_json::from_slice()` with `simd_json::from_slice()` in parser threads
3. Benchmark delta

**Expected outcome:** Additional 1.5-2x on parsing throughput. Only matters if write path improves enough that parsing becomes the bottleneck again.

### Phase 3: Binary cache (moderate effort, big win for repeated loads)

1. Add `rkyv` to Cargo.toml
2. Define `BinRecord` struct with rkyv derives
3. First load: parallel NDJSON parse, write `.bitdex-cache`
4. Subsequent loads: mmap `.bitdex-cache`, iterate archived records
5. Add `--cache` flag to benchmark binary

**Expected outcome:** Subsequent loads at I/O speed (~10s for 104M records). Parsing is eliminated.

### Phase 4: Allocation reduction (small effort, incremental win)

1. Pre-size HashMap in `to_document()` (trivial)
2. Change `Document.fields` to `HashMap<Arc<str>, FieldValue>` (moderate refactor)
3. Thread-local buffer reuse (small)

**Expected outcome:** 10-20% improvement in per-record processing time. Marginal but free.

---

## 11. New Dependencies

| Crate | Purpose | Size impact |
|-------|---------|-------------|
| `memmap2` | Memory-mapped file I/O | Tiny (~200 LOC) |
| `memchr` | SIMD newline scanning | Tiny, widely used |
| `simd-json` (optional) | SIMD-accelerated JSON parsing | Moderate |
| `rkyv` (optional) | Zero-copy binary serialization | Moderate |

`memmap2` and `memchr` are near-universal in the Rust ecosystem and add negligible compile time.

---

## 12. Benchmarking Strategy

### Micro-benchmarks (criterion)

1. **JSON parsing throughput**: serde_json vs simd-json, single-threaded, measured in MB/s
2. **Line scanning throughput**: memchr vs BufReader::lines(), measured in GB/s
3. **to_document() overhead**: Isolated, measured in ns/record
4. **Document allocation**: HashMap<String, FieldValue> vs HashMap<Arc<str>, FieldValue>

### Macro-benchmarks (benchmark.rs)

1. **Parse-only throughput**: Read + parse + to_document, discard results. Measures max parsing rate.
2. **Parse + put throughput**: Full pipeline including engine.put(). Measures end-to-end.
3. **Scaling with threads**: 1, 2, 4, 8, 16 parser threads. Identify the knee where write path saturates.
4. **Binary cache vs NDJSON**: First load (NDJSON) vs subsequent load (cache).
5. **Memory overhead**: RSS with pipeline vs without.

### Key metrics to collect

- Records/s at each thread count
- Parser thread utilization (% time spent parsing vs blocked on send)
- Writer thread utilization (% time spent in put() vs blocked on recv)
- Flush thread utilization (% time applying mutations vs idle)
- Channel fullness at steady state (measures backpressure balance)

---

## Appendix A: Record Size Distribution

The Civitai NDJSON has variable-length records due to `tagIds`, `modelVersionIds`, and other array fields. Approximate distribution:

- Median line length: ~400 bytes
- Mean line length: ~564 bytes (59 GB / 104.6M)
- P99 line length: ~2-4 KB (records with many tags)
- Max line length: ~10-20 KB (outliers)

This is favorable for parallel chunk processing: chunks will have roughly equal record counts despite byte-offset splitting.

## Appendix B: Comparison with Other Systems

| System | Ingestion rate | Method |
|--------|---------------|--------|
| ClickHouse | ~1M rows/s | Parallel insert, columnar, no indexing at insert time |
| Elasticsearch | ~50-100K docs/s | Single-threaded per shard, Lucene segments |
| Meilisearch | ~25-50K docs/s | Single-threaded, sorted inverted index |
| **Bitdex current** | **62K docs/s** | Single-threaded parse + put |
| **Bitdex target** | **300K docs/s** | Parallel parse + improved write path |
| **Bitdex theoretical** | **1-3M parse/s** | Parsing only (write path is the ceiling) |

Our 300K/s target is competitive with or exceeding specialized search engines, while maintaining bitmap-only indexing.
