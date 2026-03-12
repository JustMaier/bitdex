use roaring::RoaringBitmap;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

struct Entry {
    last_used: Instant,
    _bitmap: RoaringBitmap,
    _min_tracked_value: u32,
}

fn make_bitmap() -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    // 10K entries, scattered to avoid extreme compression
    for i in 0..10_000u32 {
        bm.insert(i.wrapping_mul(7919)); // prime scatter
    }
    bm
}

fn populate(n: usize) -> HashMap<u64, Entry> {
    let base = Instant::now();
    let mut map = HashMap::with_capacity(n);
    for i in 0..n as u64 {
        // Space timestamps 1us apart so they're all distinct
        let last_used = base + Duration::from_nanos(i * 1000);
        map.insert(i, Entry {
            last_used,
            _bitmap: make_bitmap(),
            _min_tracked_value: i as u32,
        });
    }
    map
}

// ── Linear scan ──────────────────────────────────────────────────────

fn linear_scan_find_lru(map: &HashMap<u64, Entry>) -> u64 {
    map.iter()
        .min_by_key(|(_, e)| e.last_used)
        .map(|(&k, _)| k)
        .unwrap()
}

// ── BTreeMap secondary index ─────────────────────────────────────────

fn build_btree_index(map: &HashMap<u64, Entry>) -> BTreeMap<(Instant, u64), ()> {
    let mut bt = BTreeMap::new();
    for (&k, e) in map.iter() {
        bt.insert((e.last_used, k), ());
    }
    bt
}

fn btree_find_lru(bt: &BTreeMap<(Instant, u64), ()>) -> (Instant, u64) {
    let &(instant, key) = bt.keys().next().unwrap();
    (instant, key)
}

// ── Vec sorted by access time (append log) ───────────────────────────

fn build_access_vec(map: &HashMap<u64, Entry>) -> Vec<(Instant, u64)> {
    let mut v: Vec<(Instant, u64)> = map.iter().map(|(&k, e)| (e.last_used, k)).collect();
    v.sort();
    v
}

fn vec_find_lru(v: &[(Instant, u64)]) -> (Instant, u64) {
    v[0]
}

// ── Benchmark harness ────────────────────────────────────────────────

const ITERS: u32 = 10_000;

fn bench_linear_scan(n: usize) -> Duration {
    let map = populate(n);
    let start = Instant::now();
    let mut sink: u64 = 0;
    for _ in 0..ITERS {
        sink = sink.wrapping_add(linear_scan_find_lru(&map));
    }
    let elapsed = start.elapsed();
    // prevent optimizing away
    std::hint::black_box(sink);
    elapsed
}

fn bench_btree(n: usize) -> Duration {
    let map = populate(n);
    let bt = build_btree_index(&map);
    let start = Instant::now();
    let mut sink: u64 = 0;
    for _ in 0..ITERS {
        let (_, key) = btree_find_lru(&bt);
        sink = sink.wrapping_add(key);
    }
    let elapsed = start.elapsed();
    std::hint::black_box(sink);
    elapsed
}

fn bench_vec(n: usize) -> Duration {
    let map = populate(n);
    let v = build_access_vec(&map);
    let start = Instant::now();
    let mut sink: u64 = 0;
    for _ in 0..ITERS {
        let (_, key) = vec_find_lru(&v);
        sink = sink.wrapping_add(key);
    }
    let elapsed = start.elapsed();
    std::hint::black_box(sink);
    elapsed
}

fn bench_btree_full_evict_reinsert(n: usize) -> Duration {
    // Simulates full eviction cycle: find LRU, remove from both, insert new entry into both
    let mut map = populate(n);
    let mut bt = build_btree_index(&map);
    let start = Instant::now();
    for i in 0..ITERS {
        // Find LRU
        let &(instant, key) = bt.keys().next().unwrap();
        // Remove from both
        bt.remove(&(instant, key));
        map.remove(&key);
        // Insert replacement
        let now = Instant::now();
        let new_key = (n as u64) + (i as u64);
        map.insert(new_key, Entry {
            last_used: now,
            _bitmap: RoaringBitmap::new(), // cheap placeholder for the cycle
            _min_tracked_value: 0,
        });
        bt.insert((now, new_key), ());
    }
    let elapsed = start.elapsed();
    std::hint::black_box(map.len());
    elapsed
}

fn bench_linear_full_evict(n: usize) -> Duration {
    // Simulates full eviction cycle: linear scan for LRU, remove, insert new
    let mut map = populate(n);
    let start = Instant::now();
    for i in 0..ITERS {
        let lru_key = linear_scan_find_lru(&map);
        map.remove(&lru_key);
        let now = Instant::now();
        let new_key = (n as u64) + (i as u64);
        map.insert(new_key, Entry {
            last_used: now,
            _bitmap: RoaringBitmap::new(),
            _min_tracked_value: 0,
        });
    }
    let elapsed = start.elapsed();
    std::hint::black_box(map.len());
    elapsed
}

fn classify(ns: f64) -> &'static str {
    if ns < 1_000.0 {
        "sub-us"
    } else if ns < 10_000.0 {
        "sub-10us"
    } else if ns < 100_000.0 {
        "sub-100us"
    } else {
        ">100us"
    }
}

#[test]
fn bench_lru_eviction() {
    let sizes = [100, 500, 1_000, 5_000, 10_000];

    println!("\n{}", "=".repeat(70));
    println!("  LRU Eviction Microbenchmark  ({ITERS} iterations per measurement)");
    println!("  Entry: HashMap<u64, Entry> with 10K-element RoaringBitmap each");
    println!("{}\n", "=".repeat(70));

    // ── Lookup-only benchmarks ───────────────────────────────────────

    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│  LOOKUP ONLY (find LRU victim, no mutation)                      │");
    println!("├────────┬──────────────────┬──────────────────┬──────────────────┤");
    println!("│  N     │  Linear Scan     │  BTreeMap .next()│  Vec[0]          │");
    println!("├────────┼──────────────────┼──────────────────┼──────────────────┤");

    for &n in &sizes {
        let lin = bench_linear_scan(n);
        let bt = bench_btree(n);
        let vc = bench_vec(n);

        let lin_ns = lin.as_nanos() as f64 / ITERS as f64;
        let bt_ns = bt.as_nanos() as f64 / ITERS as f64;
        let vc_ns = vc.as_nanos() as f64 / ITERS as f64;

        println!(
            "│ {n:>5}  │ {lin_ns:>8.0}ns {:<7} │ {bt_ns:>8.0}ns {:<7} │ {vc_ns:>8.0}ns {:<7} │",
            classify(lin_ns),
            classify(bt_ns),
            classify(vc_ns),
        );
    }
    println!("└────────┴──────────────────┴──────────────────┴──────────────────┘\n");

    // ── Full eviction cycle benchmarks ───────────────────────────────

    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│  FULL EVICTION CYCLE (find + remove + insert replacement)        │");
    println!("├────────┬────────────────────────────┬────────────────────────────┤");
    println!("│  N     │  Linear Scan               │  BTreeMap Index            │");
    println!("├────────┼────────────────────────────┼────────────────────────────┤");

    for &n in &sizes {
        let lin = bench_linear_full_evict(n);
        let bt = bench_btree_full_evict_reinsert(n);

        let lin_ns = lin.as_nanos() as f64 / ITERS as f64;
        let bt_ns = bt.as_nanos() as f64 / ITERS as f64;

        println!(
            "│ {n:>5}  │ {lin_ns:>10.0}ns {:<12} │ {bt_ns:>10.0}ns {:<12} │",
            classify(lin_ns),
            classify(bt_ns),
        );
    }
    println!("└────────┴────────────────────────────┴────────────────────────────┘\n");

    // ── Memory overhead estimate ─────────────────────────────────────

    println!("Memory overhead estimates (BTreeMap secondary index):");
    for &n in &sizes {
        // BTreeMap node: ~(Instant=16B + u64=8B + ptr overhead) * entries
        // Rough: ~64 bytes per entry in a BTreeMap
        let overhead_kb = (n * 64) as f64 / 1024.0;
        println!("  N={n:>5}: ~{overhead_kb:.1} KB for BTreeMap index");
    }

    println!("\n--- RECOMMENDATION ---");
    println!("See numbers above. For N<=5000 (our max), linear scan is likely");
    println!("fine if sub-10us. BTreeMap adds complexity + memory for marginal gain.");
    println!("Cache store is called infrequently (only on miss), so even 10-50us is acceptable.");
}
