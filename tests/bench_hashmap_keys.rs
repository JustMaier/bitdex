//! Microbenchmark: HashMap lookup latency with complex composite keys.
//!
//! Run: cargo test --release -- --nocapture bench_hashmap_keys

use std::collections::HashMap;
use std::hash::{Hash, Hasher, BuildHasherDefault};
use std::time::Instant;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CanonicalClause {
    field: String,
    op: String,
    value_repr: String,
}

type CompositeKey = (Vec<CanonicalClause>, String, u8);

/// Alternative: store pre-computed hash alongside the real key for fast rejection.
#[derive(Clone, Debug)]
struct PreHashedKey {
    hash: u64,
    key: CompositeKey,
}

impl PartialEq for PreHashedKey {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.key == other.key
    }
}
impl Eq for PreHashedKey {}

impl Hash for PreHashedKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

// ---------------------------------------------------------------------------
// Key generation helpers
// ---------------------------------------------------------------------------

const FIELDS: &[&str] = &["nsfwLevel", "type", "onSite", "tagId", "sortAtUnix", "userId", "postId", "baseModel"];
const OPS: &[&str] = &["eq", "neq", "in", "bucket_gte", "bucket_lte", "gt", "lt"];
const VALUES: &[&str] = &["1", "2", "3", "image", "true", "false", "24h", "30d", "7d", "1y", "video", "article"];
const SORT_FIELDS: &[&str] = &["reactionCount", "sortAt", "__slot__", "commentCount", "collectedCount"];

/// Deterministic pseudo-random from seed (simple LCG).
fn lcg(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 16
}

fn pick<'a>(items: &'a [&str], seed: &mut u64) -> &'a str {
    items[(lcg(seed) as usize) % items.len()]
}

fn make_key(seed: &mut u64) -> CompositeKey {
    let n_clauses = 2 + (lcg(seed) as usize % 3); // 2..4
    let mut clauses: Vec<CanonicalClause> = (0..n_clauses)
        .map(|_| CanonicalClause {
            field: pick(FIELDS, seed).to_string(),
            op: pick(OPS, seed).to_string(),
            value_repr: pick(VALUES, seed).to_string(),
        })
        .collect();
    // Canonical sort by field name then op then value
    clauses.sort_by(|a, b| (&a.field, &a.op, &a.value_repr).cmp(&(&b.field, &b.op, &b.value_repr)));
    let sort_field = pick(SORT_FIELDS, seed).to_string();
    let direction = (lcg(seed) % 2) as u8;
    (clauses, sort_field, direction)
}

fn compute_hash(key: &CompositeKey) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    h.finish()
}

fn make_prehashed(key: CompositeKey) -> PreHashedKey {
    let hash = compute_hash(&key);
    PreHashedKey { hash, key }
}

// ---------------------------------------------------------------------------
// Benchmark runner
// ---------------------------------------------------------------------------

fn run_bench(n: usize) {
    let iters = 10_000usize;
    let mut seed: u64 = 0xDEAD_BEEF_u64.wrapping_mul(n as u64 + 1);

    // --- Build keys --------------------------------------------------------
    let mut keys: Vec<CompositeKey> = Vec::with_capacity(n);
    for _ in 0..n {
        keys.push(make_key(&mut seed));
    }

    // --- Build HashMap<CompositeKey, usize> --------------------------------
    let mut map: HashMap<CompositeKey, usize> = HashMap::with_capacity(n);
    for (i, k) in keys.iter().enumerate() {
        map.insert(k.clone(), i);
    }

    // --- Build PreHashed map -----------------------------------------------
    let mut ph_map: HashMap<PreHashedKey, usize> = HashMap::with_capacity(n);
    for (i, k) in keys.iter().enumerate() {
        ph_map.insert(make_prehashed(k.clone()), i);
    }

    // --- Hit lookup keys (cycle through existing keys) ---------------------
    let hit_keys: Vec<CompositeKey> = (0..iters).map(|i| keys[i % n].clone()).collect();
    let hit_ph_keys: Vec<PreHashedKey> = hit_keys.iter().map(|k| make_prehashed(k.clone())).collect();

    // --- Miss lookup keys (different seed → very unlikely to collide) ------
    let mut miss_seed: u64 = 0xCAFE_BABE_u64.wrapping_mul(n as u64 + 7);
    let miss_keys: Vec<CompositeKey> = (0..iters).map(|_| make_key(&mut miss_seed)).collect();
    let miss_ph_keys: Vec<PreHashedKey> = miss_keys.iter().map(|k| make_prehashed(k.clone())).collect();

    // === Measure hit latency (standard) ====================================
    let start = Instant::now();
    let mut found = 0usize;
    for k in &hit_keys {
        if map.get(k).is_some() {
            found += 1;
        }
    }
    let hit_ns = start.elapsed().as_nanos() as f64 / iters as f64;

    // === Measure miss latency (standard) ===================================
    let start = Instant::now();
    let mut missed = 0usize;
    for k in &miss_keys {
        if map.get(k).is_none() {
            missed += 1;
        }
    }
    let miss_ns = start.elapsed().as_nanos() as f64 / iters as f64;

    // === Measure hit latency (pre-hashed) ==================================
    let start = Instant::now();
    let mut ph_found = 0usize;
    for k in &hit_ph_keys {
        if ph_map.get(k).is_some() {
            ph_found += 1;
        }
    }
    let ph_hit_ns = start.elapsed().as_nanos() as f64 / iters as f64;

    // === Measure miss latency (pre-hashed) =================================
    let start = Instant::now();
    let mut ph_missed = 0usize;
    for k in &miss_ph_keys {
        if ph_map.get(k).is_none() {
            ph_missed += 1;
        }
    }
    let ph_miss_ns = start.elapsed().as_nanos() as f64 / iters as f64;

    // === Measure raw hash computation time =================================
    // Pick a representative 3-clause key
    let sample_key = &keys[0];
    let hash_iters = 100_000usize;
    let start = Instant::now();
    let mut sink: u64 = 0;
    for _ in 0..hash_iters {
        sink = sink.wrapping_add(compute_hash(sample_key));
    }
    let hash_ns = start.elapsed().as_nanos() as f64 / hash_iters as f64;
    // Prevent optimizing away
    std::hint::black_box(sink);

    // === Print results =====================================================
    println!(
        "  N={n:>5} | hit {hit_ns:>7.1}ns  miss {miss_ns:>7.1}ns | pre-hash hit {ph_hit_ns:>7.1}ns  miss {ph_miss_ns:>7.1}ns | raw hash {hash_ns:>5.1}ns | found={found} missed={missed} ph_found={ph_found} ph_missed={ph_missed}"
    );
}

// ---------------------------------------------------------------------------
// Bonus: measure with FxHashMap (via manual FxHash-like impl)
// ---------------------------------------------------------------------------

/// A simple Fx-style hasher (fast, non-cryptographic).
struct FxHasher {
    hash: u64,
}

const SEED: u64 = 0x517cc1b727220a95;

impl Hasher for FxHasher {
    fn finish(&self) -> u64 {
        self.hash
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.hash = self.hash.rotate_left(5) ^ (b as u64).wrapping_mul(SEED);
        }
    }
    fn write_u8(&mut self, i: u8) {
        self.hash = self.hash.rotate_left(5) ^ (i as u64).wrapping_mul(SEED);
    }
    fn write_u64(&mut self, i: u64) {
        self.hash = self.hash.rotate_left(5) ^ i.wrapping_mul(SEED);
    }
    fn write_usize(&mut self, i: usize) {
        self.hash = self.hash.rotate_left(5) ^ (i as u64).wrapping_mul(SEED);
    }
}

impl Default for FxHasher {
    fn default() -> Self {
        FxHasher { hash: 0 }
    }
}

type FxBuildHasher = BuildHasherDefault<FxHasher>;

fn run_bench_fx(n: usize) {
    let iters = 10_000usize;
    let mut seed: u64 = 0xDEAD_BEEF_u64.wrapping_mul(n as u64 + 1);

    let mut keys: Vec<CompositeKey> = Vec::with_capacity(n);
    for _ in 0..n {
        keys.push(make_key(&mut seed));
    }

    let mut map: HashMap<CompositeKey, usize, FxBuildHasher> =
        HashMap::with_capacity_and_hasher(n, FxBuildHasher::default());
    for (i, k) in keys.iter().enumerate() {
        map.insert(k.clone(), i);
    }

    let hit_keys: Vec<CompositeKey> = (0..iters).map(|i| keys[i % n].clone()).collect();

    let mut miss_seed: u64 = 0xCAFE_BABE_u64.wrapping_mul(n as u64 + 7);
    let miss_keys: Vec<CompositeKey> = (0..iters).map(|_| make_key(&mut miss_seed)).collect();

    let start = Instant::now();
    let mut found = 0usize;
    for k in &hit_keys {
        if map.get(k).is_some() { found += 1; }
    }
    let hit_ns = start.elapsed().as_nanos() as f64 / iters as f64;

    let start = Instant::now();
    let mut missed = 0usize;
    for k in &miss_keys {
        if map.get(k).is_none() { missed += 1; }
    }
    let miss_ns = start.elapsed().as_nanos() as f64 / iters as f64;

    println!(
        "  N={n:>5} | fx-hash hit {hit_ns:>7.1}ns  miss {miss_ns:>7.1}ns | found={found} missed={missed}"
    );
}

// ---------------------------------------------------------------------------
// Bonus 2: Arc<str> interned keys
// ---------------------------------------------------------------------------

use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct InternedClause {
    field: Arc<str>,
    op: Arc<str>,
    value_repr: Arc<str>,
}

type InternedKey = (Vec<InternedClause>, Arc<str>, u8);

fn run_bench_interned(n: usize) {
    let iters = 10_000usize;
    let mut seed: u64 = 0xDEAD_BEEF_u64.wrapping_mul(n as u64 + 1);

    // Intern pool
    let mut intern: HashMap<String, Arc<str>> = HashMap::new();
    let mut get_interned = |s: &str| -> Arc<str> {
        intern.entry(s.to_string()).or_insert_with(|| Arc::from(s)).clone()
    };

    let mut keys: Vec<InternedKey> = Vec::with_capacity(n);
    for _ in 0..n {
        let nc = 2 + (lcg(&mut seed) as usize % 3);
        let mut clauses: Vec<InternedClause> = (0..nc)
            .map(|_| InternedClause {
                field: get_interned(pick(FIELDS, &mut seed)),
                op: get_interned(pick(OPS, &mut seed)),
                value_repr: get_interned(pick(VALUES, &mut seed)),
            })
            .collect();
        clauses.sort_by(|a, b| (&*a.field, &*a.op, &*a.value_repr).cmp(&(&*b.field, &*b.op, &*b.value_repr)));
        let sort_field = get_interned(pick(SORT_FIELDS, &mut seed));
        let direction = (lcg(&mut seed) % 2) as u8;
        keys.push((clauses, sort_field, direction));
    }

    let mut map: HashMap<InternedKey, usize> = HashMap::with_capacity(n);
    for (i, k) in keys.iter().enumerate() {
        map.insert(k.clone(), i);
    }

    let hit_keys: Vec<InternedKey> = (0..iters).map(|i| keys[i % n].clone()).collect();

    let mut miss_seed: u64 = 0xCAFE_BABE_u64.wrapping_mul(n as u64 + 7);
    let miss_keys: Vec<InternedKey> = (0..iters).map(|_| {
        let nc = 2 + (lcg(&mut miss_seed) as usize % 3);
        let mut clauses: Vec<InternedClause> = (0..nc)
            .map(|_| InternedClause {
                field: get_interned(pick(FIELDS, &mut miss_seed)),
                op: get_interned(pick(OPS, &mut miss_seed)),
                value_repr: get_interned(pick(VALUES, &mut miss_seed)),
            })
            .collect();
        clauses.sort_by(|a, b| (&*a.field, &*a.op, &*a.value_repr).cmp(&(&*b.field, &*b.op, &*b.value_repr)));
        let sort_field = get_interned(pick(SORT_FIELDS, &mut miss_seed));
        let direction = (lcg(&mut miss_seed) % 2) as u8;
        (clauses, sort_field, direction)
    }).collect();

    let start = Instant::now();
    let mut found = 0usize;
    for k in &hit_keys {
        if map.get(k).is_some() { found += 1; }
    }
    let hit_ns = start.elapsed().as_nanos() as f64 / iters as f64;

    let start = Instant::now();
    let mut missed = 0usize;
    for k in &miss_keys {
        if map.get(k).is_none() { missed += 1; }
    }
    let miss_ns = start.elapsed().as_nanos() as f64 / iters as f64;

    println!(
        "  N={n:>5} | interned hit {hit_ns:>7.1}ns  miss {miss_ns:>7.1}ns | found={found} missed={missed}"
    );
}

// ---------------------------------------------------------------------------
// Test entry point
// ---------------------------------------------------------------------------

#[test]
fn bench_hashmap_keys() {
    println!();
    println!("=== HashMap Composite Key Lookup Benchmark ===");
    println!("  Key: (Vec<CanonicalClause>, String, u8), 2-4 clauses each");
    println!("  10,000 lookups per measurement, --release mode");
    println!();

    println!("--- Standard HashMap (SipHash) ---");
    for &n in &[100, 500, 1000, 5000] {
        run_bench(n);
    }

    println!();
    println!("--- FxHash-style HashMap ---");
    for &n in &[100, 500, 1000, 5000] {
        run_bench_fx(n);
    }

    println!();
    println!("--- Arc<str> interned keys (SipHash) ---");
    for &n in &[100, 500, 1000, 5000] {
        run_bench_interned(n);
    }

    println!();
    println!("=== Summary ===");
    println!("  - Raw hash time shows cost of hashing a single composite key");
    println!("  - Pre-hashed keys store u64 hash, skip re-hashing on lookup");
    println!("  - FxHash is faster for short keys but weaker distribution");
    println!("  - Arc<str> interned keys avoid per-lookup string allocation");
    println!();
}
