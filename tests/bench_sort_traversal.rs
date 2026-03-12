//! Microbenchmark: sort traversal cost for bound formation on cache miss.
//!
//! Validates the cost of MSB-to-LSB top-K extraction from 32 sort bit layers
//! at 105M scale with various filter sizes.
//!
//! Run: cargo test --release -- --nocapture bench_sort_traversal

use roaring::RoaringBitmap;
use std::time::Instant;

const UNIVERSE: u32 = 105_000_000;
const TARGET_K: u64 = 10_000;
const NUM_LAYERS: usize = 32;

/// Simple xorshift64 PRNG for reproducible randomness without external deps.
struct Xorshift64(u64);

impl Xorshift64 {
    fn new(seed: u64) -> Self { Self(seed) }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// Build 32 sort layers representing bit-decomposed sort values.
/// Each slot gets a random u32 value; layer i contains all slots where bit i is set.
/// This ensures proper independent distribution across layers.
fn build_sort_layers() -> Vec<RoaringBitmap> {
    let mut layers: Vec<Vec<u32>> = (0..NUM_LAYERS).map(|_| Vec::with_capacity(UNIVERSE as usize / 2)).collect();
    let mut rng = Xorshift64::new(0xDEAD_BEEF_CAFE_1234);

    // Assign each slot a random u32 sort value, then decompose into bit layers
    for slot in 0..UNIVERSE {
        let val = rng.next() as u32;
        for bit in 0..NUM_LAYERS {
            if val & (1 << bit) != 0 {
                layers[bit].push(slot);
            }
        }
    }

    layers.into_iter().map(|vals| {
        // vals are already sorted since we iterate slots in order
        RoaringBitmap::from_sorted_iter(vals.into_iter()).unwrap()
    }).collect()
}

/// Build a filter bitmap with `count` slots, spread across the universe.
fn build_filter_bitmap(count: u32) -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    if count >= UNIVERSE {
        // Full universe
        bm.insert_range(0..UNIVERSE);
        return bm;
    }

    // Spread slots evenly across the universe for realistic distribution
    let step = UNIVERSE / count;
    let mut vals = Vec::with_capacity(count as usize);
    for i in 0..count {
        let slot = i.wrapping_mul(step);
        if slot < UNIVERSE {
            vals.push(slot);
        }
    }
    bm.extend(vals.into_iter());
    bm
}

/// MSB-to-LSB top-K traversal: narrow candidates through 32 bit layers.
fn top_k_traversal(
    filter: &RoaringBitmap,
    sort_layers: &[RoaringBitmap],
    target_k: u64,
) -> RoaringBitmap {
    let mut candidates = filter.clone();

    // MSB first (bit 31 down to bit 0)
    for layer in sort_layers.iter().rev() {
        let overlap = &candidates & layer;
        if overlap.len() >= target_k {
            candidates = overlap;
        }
        // else: keep candidates as-is, this bit doesn't discriminate
    }

    // Trim to exactly target_k
    if candidates.len() > target_k {
        let mut trimmed = RoaringBitmap::new();
        for (i, val) in candidates.iter().enumerate() {
            if i as u64 >= target_k {
                break;
            }
            trimmed.insert(val);
        }
        trimmed
    } else {
        candidates
    }
}

#[test]
fn bench_sort_traversal() {
    println!("\n=== Sort Traversal Microbenchmark ===");
    println!("Universe: {}M slots, {} sort layers, target_k: {}\n", UNIVERSE / 1_000_000, NUM_LAYERS, TARGET_K);

    // Build sort layers
    let t0 = Instant::now();
    let sort_layers = build_sort_layers();
    let layer_build_ms = t0.elapsed().as_millis();
    println!("Sort layer build time: {}ms", layer_build_ms);

    // Report layer cardinalities
    let avg_card: u64 = sort_layers.iter().map(|l| l.len()).sum::<u64>() / NUM_LAYERS as u64;
    println!("Average layer cardinality: {}M ({:.1}% of universe)\n", avg_card / 1_000_000, avg_card as f64 / UNIVERSE as f64 * 100.0);

    // Filter sizes to test
    let filter_sizes: Vec<(u32, &str)> = vec![
        (1_000, "1K (userId)"),
        (10_000, "10K"),
        (100_000, "100K"),
        (424_000, "424K (30d bucket)"),
        (1_000_000, "1M"),
        (5_000_000, "5M"),
        (21_000_000, "21M"),
        (53_000_000, "53M (1y bucket)"),
    ];

    println!("{:<25} {:>12} {:>12} {:>12} {:>10}", "Filter Size", "Traversal", "Trim", "Total", "Result");
    println!("{}", "-".repeat(75));

    for (count, label) in &filter_sizes {
        let filter = build_filter_bitmap(*count);
        let actual_count = filter.len();

        // If filter is already <= target_k, skip traversal
        if actual_count <= TARGET_K {
            let t = Instant::now();
            let _result = filter.clone();
            let clone_us = t.elapsed().as_micros();
            println!("{:<25} {:>12} {:>12} {:>10}us {:>10}",
                format!("{} (n={})", label, actual_count),
                "SKIP",
                "SKIP",
                clone_us,
                actual_count,
            );
            continue;
        }

        // Warm up: 1 run
        let _ = top_k_traversal(&filter, &sort_layers, TARGET_K);

        // Benchmark: 5 runs, take median
        let mut times_us = Vec::new();
        let runs = 5;

        for _ in 0..runs {
            let t = Instant::now();
            let result = top_k_traversal(&filter, &sort_layers, TARGET_K);
            let elapsed = t.elapsed();
            let _ = result.len(); // prevent optimization
            times_us.push(elapsed.as_micros());
        }

        times_us.sort();
        let median = times_us[runs / 2];

        // Also measure just the traversal (no trim) for the last run
        let t_trav = Instant::now();
        let mut candidates = filter.clone();
        for layer in sort_layers.iter().rev() {
            let overlap = &candidates & layer;
            if overlap.len() >= TARGET_K {
                candidates = overlap;
            }
        }
        let traversal_us = t_trav.elapsed().as_micros();
        let candidates_len = candidates.len();

        // Measure trim separately
        let t_trim = Instant::now();
        if candidates.len() > TARGET_K {
            let mut trimmed = RoaringBitmap::new();
            for (i, val) in candidates.iter().enumerate() {
                if i as u64 >= TARGET_K {
                    break;
                }
                trimmed.insert(val);
            }
            let _ = trimmed.len();
        }
        let trim_us = t_trim.elapsed().as_micros();

        println!("{:<25} {:>10}us {:>10}us {:>10}us {:>10}",
            format!("{} (n={})", label, actual_count),
            traversal_us,
            trim_us,
            median,
            format!("{} (pre-trim {})", TARGET_K, candidates_len),
        );
    }

    println!("\n=== Clone Cost (filter bitmap clone, no traversal) ===");
    println!("{:<25} {:>12}", "Filter Size", "Clone Time");
    println!("{}", "-".repeat(40));

    for (count, label) in &filter_sizes {
        let filter = build_filter_bitmap(*count);
        // Warm
        let _ = filter.clone();

        let mut times = Vec::new();
        for _ in 0..5 {
            let t = Instant::now();
            let c = filter.clone();
            let _ = c.len();
            times.push(t.elapsed().as_micros());
        }
        times.sort();
        println!("{:<25} {:>10}us", label, times[2]);
    }

    // Diagnostic: show how candidates narrow through layers for 424K
    println!("\n=== Narrowing Diagnostic (424K filter) ===");
    let filter_424k = build_filter_bitmap(424_000);
    let mut candidates = filter_424k.clone();
    println!("Start: {} candidates", candidates.len());
    for (i, layer) in sort_layers.iter().rev().enumerate() {
        let overlap = &candidates & layer;
        let bit_idx = 31 - i;
        if overlap.len() >= TARGET_K {
            println!("  Layer {:>2} (bit {:>2}): overlap={:>10} >= {} -> NARROW", i, bit_idx, overlap.len(), TARGET_K);
            candidates = overlap;
        } else {
            println!("  Layer {:>2} (bit {:>2}): overlap={:>10} <  {} -> KEEP {}", i, bit_idx, overlap.len(), TARGET_K, candidates.len());
        }
    }
    println!("Final candidates: {}", candidates.len());

    // Key insight: with uniform random bits, each layer halves candidates.
    // 424K -> 212K -> 106K -> 53K -> 26.5K -> 13.25K -> ~6.6K (< 10K, stop narrowing at layer 5)
    // So only ~5 layers should narrow before we're below target_k.
    // But the AND cost at 105M scale is the issue.

    // Measure just a single layer AND cost
    println!("\n=== Single Layer AND Cost ===");
    for (count, label) in &filter_sizes {
        let filter = build_filter_bitmap(*count);
        if filter.len() <= TARGET_K { continue; }

        let layer = &sort_layers[31]; // MSB layer
        // Warm
        let _ = &filter & layer;

        let mut times = Vec::new();
        for _ in 0..10 {
            let t = Instant::now();
            let r = &filter & layer;
            let _ = r.len();
            times.push(t.elapsed().as_micros());
        }
        times.sort();
        println!("{:<25} single AND: {:>8}us (median of 10)", label, times[5]);
    }

    println!("\nDone.");
}
