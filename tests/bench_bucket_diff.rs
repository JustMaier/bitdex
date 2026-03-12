//! Microbenchmark: time bucket diff cost at 24h / 30d / 1y scales.
//!
//! Run with: cargo test --release -- --nocapture bench_bucket_diff

use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::time::Instant;

/// Build a pair of (old, new) bitmaps.
/// `n` total docs in old, `drop` removed, `add` added to new.
/// Universe is [0 .. universe_max).
fn make_pair(n: u32, drop: u32, add: u32, universe_max: u32) -> (RoaringBitmap, RoaringBitmap) {
    // Deterministic pseudo-random via simple LCG
    let mut state: u64 = 0xDEAD_BEEF;
    let mut next = move || -> u32 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((state >> 33) as u32) % universe_max
    };

    // Build old bitmap with n unique values
    let mut old = RoaringBitmap::new();
    while old.len() < n as u64 {
        old.insert(next());
    }

    // Clone to new, then drop some and add some
    let mut new = old.clone();

    // Collect old values to pick drops from
    let old_vals: Vec<u32> = old.iter().collect();
    let mut dropped = 0u32;
    let _idx = 0usize;
    // Use a second LCG stream for selection
    let mut state2: u64 = 0xCAFE_BABE;
    let mut next2 = move || -> usize {
        state2 = state2.wrapping_mul(6364136223846793005).wrapping_add(1);
        (state2 >> 33) as usize
    };
    while dropped < drop {
        let pick = next2() % old_vals.len();
        if new.remove(old_vals[pick]) {
            dropped += 1;
        }
    }

    // Add new values not in old
    let mut added = 0u32;
    while added < add {
        let v = next();
        if !old.contains(v) && !new.contains(v) {
            new.insert(v);
            added += 1;
        }
    }

    (old, new)
}

struct BenchResult {
    label: &'static str,
    old_card: u64,
    new_card: u64,
    xor_ns: u128,
    xor_card: u64,
    andnot_drops_ns: u128,
    drops_card: u64,
    andnot_adds_ns: u128,
    adds_card: u64,
    iter_ns: u128,
    iter_count: u64,
    pipeline_ns: u128,
}

fn bench_scale(
    label: &'static str,
    n: u32,
    drop: u32,
    add: u32,
    universe_max: u32,
    iters: u32,
) -> BenchResult {
    let (old, new) = make_pair(n, drop, add, universe_max);

    // Warm up
    let _ = &old ^ &new;
    let _ = &old - &new;
    let _ = &new - &old;

    // XOR
    let mut xor_total = 0u128;
    let mut xor_card = 0u64;
    for _ in 0..iters {
        let t = Instant::now();
        let diff = &old ^ &new;
        xor_total += t.elapsed().as_nanos();
        xor_card = diff.len();
    }

    // ANDNOT drops (old - new)
    let mut drops_total = 0u128;
    let mut drops_card = 0u64;
    for _ in 0..iters {
        let t = Instant::now();
        let drops = &old - &new;
        drops_total += t.elapsed().as_nanos();
        drops_card = drops.len();
    }

    // ANDNOT adds (new - old)
    let mut adds_total = 0u128;
    let mut adds_card = 0u64;
    for _ in 0..iters {
        let t = Instant::now();
        let adds = &new - &old;
        adds_total += t.elapsed().as_nanos();
        adds_card = adds.len();
    }

    // Iterate the XOR diff
    let diff = &old ^ &new;
    let mut iter_total = 0u128;
    let mut iter_count = 0u64;
    for _ in 0..iters {
        let t = Instant::now();
        let mut c = 0u64;
        for _ in diff.iter() {
            c += 1;
        }
        iter_total += t.elapsed().as_nanos();
        iter_count = c;
    }

    // Full pipeline: ANDNOT drops + ANDNOT adds + iterate both + 100 HashMap lookups per slot
    // Build a fake meta-index (HashMap<u32, Vec<u64>>)
    let mut meta: HashMap<u32, Vec<u64>> = HashMap::new();
    // Populate with some entries from the diff
    for (i, slot) in diff.iter().enumerate() {
        if i % 10 == 0 {
            meta.insert(slot, vec![1, 2, 3]);
        }
    }
    // Also add some extra random entries
    for i in 0..1000u32 {
        meta.entry(i * 7).or_insert_with(|| vec![42]);
    }

    let mut pipeline_total = 0u128;
    for _ in 0..iters {
        let t = Instant::now();
        let drops = &old - &new;
        let adds = &new - &old;
        // Iterate drops, simulate meta-index lookups
        let mut _acc = 0u64;
        for slot in drops.iter() {
            // Simulate 100 meta-index lookups per slot
            for offset in 0..100u32 {
                if let Some(v) = meta.get(&slot.wrapping_add(offset)) {
                    _acc += v.len() as u64;
                }
            }
        }
        // Iterate adds
        for slot in adds.iter() {
            for offset in 0..100u32 {
                if let Some(v) = meta.get(&slot.wrapping_add(offset)) {
                    _acc += v.len() as u64;
                }
            }
        }
        // Use _acc to prevent optimization
        std::hint::black_box(_acc);
        pipeline_total += t.elapsed().as_nanos();
    }

    BenchResult {
        label,
        old_card: old.len(),
        new_card: new.len(),
        xor_ns: xor_total / iters as u128,
        xor_card,
        andnot_drops_ns: drops_total / iters as u128,
        drops_card,
        andnot_adds_ns: adds_total / iters as u128,
        adds_card,
        iter_ns: iter_total / iters as u128,
        iter_count,
        pipeline_ns: pipeline_total / iters as u128,
    }
}

fn fmt_ns(ns: u128) -> String {
    if ns >= 1_000_000 {
        format!("{:.2} ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.2} us", ns as f64 / 1_000.0)
    } else {
        format!("{} ns", ns)
    }
}

#[test]
fn bench_bucket_diff() {
    println!("\n{}", "=".repeat(80));
    println!("  Time Bucket Diff Microbenchmark (roaring bitmap)");
    println!("{}\n", "=".repeat(80));

    let configs: Vec<(&str, u32, u32, u32, u32, u32)> = vec![
        // (label, n_docs, drops, adds, universe_max, iterations)
        ("24h bucket", 1_500, 50, 50, 200_000, 10_000),
        ("30d bucket", 424_000, 1_000, 1_000, 5_000_000, 1_000),
        ("1y  bucket", 53_000_000, 5_000, 5_000, 110_000_000, 100),
    ];

    for (label, n, drop, add, universe, iters) in configs {
        let r = bench_scale(label, n, drop, add, universe, iters);
        println!("--- {} ---", r.label);
        println!(
            "  old: {} docs, new: {} docs",
            r.old_card, r.new_card
        );
        println!(
            "  XOR (symmetric diff):  {} -> {} changed slots",
            fmt_ns(r.xor_ns),
            r.xor_card
        );
        println!(
            "  ANDNOT (drops):        {} -> {} dropped",
            fmt_ns(r.andnot_drops_ns),
            r.drops_card
        );
        println!(
            "  ANDNOT (adds):         {} -> {} added",
            fmt_ns(r.andnot_adds_ns),
            r.adds_card
        );
        println!(
            "  Iterate diff:          {} ({} slots)",
            fmt_ns(r.iter_ns),
            r.iter_count
        );
        println!(
            "  Full pipeline:         {} (ANDNOT x2 + iterate + 100 meta lookups/slot)",
            fmt_ns(r.pipeline_ns)
        );
        println!();
    }
}
