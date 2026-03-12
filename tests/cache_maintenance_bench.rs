//! Microbenchmark: batch bitmap AND operations for cache live maintenance.
//!
//! Simulates the flush thread evaluating N cache entries against a small batch
//! of changed slots. Each entry has 2-3 filter clauses; we AND the batch bitmap
//! with each clause's field bitmap to find matching slots, then insert qualifying
//! ones into a small result bitmap.
//!
//! Run: cargo test --release --test cache_maintenance_bench -- --nocapture

use roaring::RoaringBitmap;
use std::time::Instant;

/// Build a bitmap with `count` bits set, spread across `universe` range.
/// Uses a deterministic stride pattern for reproducibility.
fn make_bitmap(universe: u32, count: u32) -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    if count == 0 {
        return bm;
    }
    let stride = universe / count;
    for i in 0..count {
        bm.insert(i * stride);
    }
    bm
}

/// Build a bitmap with `count` bits set starting near a random-ish offset.
fn make_sparse_bitmap(universe: u32, count: u32, seed: u32) -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    // Use a simple LCG-like scatter
    let prime: u64 = 2654435761; // Knuth multiplicative hash constant
    for i in 0..count {
        let val = ((seed as u64).wrapping_add((i as u64).wrapping_mul(prime))) % (universe as u64);
        bm.insert(val as u32);
    }
    bm
}

/// A simulated cache entry: a list of indices into the field_bitmaps array.
struct CacheEntry {
    clause_indices: Vec<usize>,
    /// Small result bitmap (simulating the bound/top-K bitmap for this entry)
    result: RoaringBitmap,
}

#[test]
fn bench_cache_maintenance() {
    let universe: u32 = 105_000_000;

    println!("\n=== Cache Maintenance Microbenchmark (105M scale) ===\n");
    println!("Building field bitmaps...");

    let t0 = Instant::now();

    // Build field bitmaps at realistic densities
    let field_bitmaps: Vec<(&str, RoaringBitmap)> = vec![
        ("nsfwLevel=1 (60M)", make_bitmap(universe, 60_000_000)),
        ("nsfwLevel=2 (30M)", make_bitmap(universe, 30_000_000)),
        ("type=image (80M)", make_bitmap(universe, 80_000_000)),
        ("onSite=true (50M)", make_bitmap(universe, 50_000_000)),
        ("tagId=12345 (500K)", make_sparse_bitmap(universe, 500_000, 12345)),
        ("time_30d (424K)", make_sparse_bitmap(universe, 424_000, 99999)),
    ];

    println!(
        "Field bitmaps built in {:.1}ms",
        t0.elapsed().as_secs_f64() * 1000.0
    );
    for (name, bm) in &field_bitmaps {
        println!(
            "  {}: cardinality={}, serialized_size={}KB",
            name,
            bm.len(),
            bm.serialized_size() / 1024
        );
    }

    // Build a batch bitmap: 50 changed slots (typical flush cycle)
    let batch = make_sparse_bitmap(universe, 50, 42);
    println!(
        "\nBatch bitmap: {} slots (simulating one flush cycle)\n",
        batch.len()
    );

    // Test with varying cache entry counts
    let entry_counts = [100, 500, 1000, 5000];

    for &n in &entry_counts {
        // Build N cache entries, each with 2-3 filter clauses
        let mut entries: Vec<CacheEntry> = Vec::with_capacity(n);
        for i in 0..n {
            // Deterministic clause selection: 2-3 clauses per entry
            let num_clauses = if i % 3 == 0 { 3 } else { 2 };
            let mut clause_indices = Vec::with_capacity(num_clauses);
            for c in 0..num_clauses {
                clause_indices.push((i * 7 + c * 3) % field_bitmaps.len());
            }
            // Pre-populate a small result bitmap (simulating existing bound cache entry)
            let result = make_sparse_bitmap(universe, 10_000, (i * 31 + 77) as u32);
            entries.push(CacheEntry {
                clause_indices,
                result,
            });
        }

        // Warm up: one pass to ensure everything is in CPU cache
        for entry in &mut entries {
            let mut matched = batch.clone();
            for &idx in &entry.clause_indices {
                matched &= &field_bitmaps[idx].1;
            }
            entry.result |= &matched;
        }

        // Timed run: multiple iterations for stability
        let iterations = 20;
        let start = Instant::now();
        for _ in 0..iterations {
            for entry in &mut entries {
                // Step 1: AND batch with each clause bitmap
                let mut matched = batch.clone();
                for &idx in &entry.clause_indices {
                    matched &= &field_bitmaps[idx].1;
                }
                // Step 2: "Sort qualification check" — just check cardinality
                // In real code this would compare sort values
                if matched.len() > 0 {
                    // Step 3: Insert qualifying slots into result bitmap
                    entry.result |= &matched;
                }
            }
        }
        let elapsed = start.elapsed();
        let total_us = elapsed.as_secs_f64() * 1_000_000.0;
        let per_iter_us = total_us / iterations as f64;
        let per_entry_ns = (per_iter_us * 1000.0) / n as f64;

        println!(
            "N={:5} entries: total={:8.1}us  per_entry={:6.0}ns  ({:.3}ms total)  [{}]",
            n,
            per_iter_us,
            per_entry_ns,
            per_iter_us / 1000.0,
            if per_iter_us < 1000.0 {
                "SUB-MS"
            } else {
                "OVER 1ms"
            }
        );
    }

    // Also benchmark the AND operation in isolation (no result bitmap mutation)
    println!("\n--- AND-only (no result mutation) ---");
    for &n in &entry_counts {
        let mut entries: Vec<Vec<usize>> = Vec::with_capacity(n);
        for i in 0..n {
            let num_clauses = if i % 3 == 0 { 3 } else { 2 };
            let mut clause_indices = Vec::with_capacity(num_clauses);
            for c in 0..num_clauses {
                clause_indices.push((i * 7 + c * 3) % field_bitmaps.len());
            }
            entries.push(clause_indices);
        }

        let iterations = 50;
        let start = Instant::now();
        let mut total_matched: u64 = 0;
        for _ in 0..iterations {
            for clause_indices in &entries {
                let mut matched = batch.clone();
                for &idx in clause_indices {
                    matched &= &field_bitmaps[idx].1;
                }
                total_matched += matched.len();
            }
        }
        let elapsed = start.elapsed();
        let per_iter_us = elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64;
        let per_entry_ns = (per_iter_us * 1000.0) / n as f64;

        println!(
            "N={:5}: total={:8.1}us  per_entry={:6.0}ns  ({:.3}ms)  matched_sum={}",
            n,
            per_iter_us,
            per_entry_ns,
            per_iter_us / 1000.0,
            total_matched
        );
    }

    // Bonus: what about larger batch sizes (200, 1000 slots)?
    println!("\n--- Varying batch size (N=1000 entries, 3 clauses each) ---");
    let batch_sizes = [10, 50, 200, 1000, 5000];
    for &bs in &batch_sizes {
        let big_batch = make_sparse_bitmap(universe, bs, 42);
        let n = 1000;
        let mut entries: Vec<Vec<usize>> = Vec::with_capacity(n);
        for i in 0..n {
            let mut clause_indices = Vec::with_capacity(3);
            for c in 0..3 {
                clause_indices.push((i * 7 + c * 3) % field_bitmaps.len());
            }
            entries.push(clause_indices);
        }

        let iterations = 20;
        let start = Instant::now();
        for _ in 0..iterations {
            for clause_indices in &entries {
                let mut matched = big_batch.clone();
                for &idx in clause_indices {
                    matched &= &field_bitmaps[idx].1;
                }
                std::hint::black_box(&matched);
            }
        }
        let elapsed = start.elapsed();
        let per_iter_us = elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64;

        println!(
            "batch={:5} slots, N=1000: total={:8.1}us  ({:.3}ms)  per_entry={:.0}ns",
            bs,
            per_iter_us,
            per_iter_us / 1000.0,
            per_iter_us * 1000.0 / n as f64,
        );
    }

    // Alternative approach: iterate the small batch and check membership
    // in each clause bitmap (avoid full bitmap AND)
    println!("\n--- Iterator approach: check each batch slot against clause bitmaps ---");
    for &n in &entry_counts {
        let mut entries_clauses: Vec<Vec<usize>> = Vec::with_capacity(n);
        let mut results: Vec<RoaringBitmap> = Vec::with_capacity(n);
        for i in 0..n {
            let num_clauses = if i % 3 == 0 { 3 } else { 2 };
            let mut clause_indices = Vec::with_capacity(num_clauses);
            for c in 0..num_clauses {
                clause_indices.push((i * 7 + c * 3) % field_bitmaps.len());
            }
            entries_clauses.push(clause_indices);
            results.push(make_sparse_bitmap(universe, 10_000, (i * 31 + 77) as u32));
        }

        let batch_slots: Vec<u32> = batch.iter().collect();

        let iterations = 50;
        let start = Instant::now();
        for _ in 0..iterations {
            for (idx, clause_indices) in entries_clauses.iter().enumerate() {
                for &slot in &batch_slots {
                    let mut matches = true;
                    for &ci in clause_indices {
                        if !field_bitmaps[ci].1.contains(slot) {
                            matches = false;
                            break;
                        }
                    }
                    if matches {
                        results[idx].insert(slot);
                    }
                }
            }
        }
        let elapsed = start.elapsed();
        let per_iter_us = elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64;
        let per_entry_ns = (per_iter_us * 1000.0) / n as f64;

        println!(
            "N={:5}: total={:8.1}us  per_entry={:6.0}ns  ({:.3}ms)  [{}]",
            n,
            per_iter_us,
            per_entry_ns,
            per_iter_us / 1000.0,
            if per_iter_us < 1000.0 { "SUB-MS" } else { "OVER 1ms" }
        );
    }

    // Also test: pre-group entries by clause signature to amortize AND
    println!("\n--- Grouped approach: deduplicate clause combos, AND once per group ---");
    {
        use std::collections::HashMap;
        let n = 5000;
        let mut groups: HashMap<Vec<usize>, Vec<usize>> = HashMap::new();
        for i in 0..n {
            let num_clauses = if i % 3 == 0 { 3 } else { 2 };
            let mut clause_indices = Vec::with_capacity(num_clauses);
            for c in 0..num_clauses {
                clause_indices.push((i * 7 + c * 3) % field_bitmaps.len());
            }
            clause_indices.sort();
            groups.entry(clause_indices).or_default().push(i);
        }
        println!("  {} entries collapsed into {} unique clause combos", n, groups.len());

        let mut results: Vec<RoaringBitmap> = (0..n)
            .map(|i| make_sparse_bitmap(universe, 10_000, (i * 31 + 77) as u32))
            .collect();

        let iterations = 50;
        let start = Instant::now();
        for _ in 0..iterations {
            for (clause_indices, entry_ids) in &groups {
                // AND once per unique clause combo
                let mut matched = batch.clone();
                for &idx in clause_indices {
                    matched &= &field_bitmaps[idx].1;
                }
                if matched.len() > 0 {
                    for &eid in entry_ids {
                        results[eid] |= &matched;
                    }
                }
            }
        }
        let elapsed = start.elapsed();
        let per_iter_us = elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64;
        println!(
            "  N=5000 grouped: total={:.1}us ({:.3}ms)  unique_combos={}",
            per_iter_us,
            per_iter_us / 1000.0,
            groups.len()
        );
    }

    println!("\n=== Done ===\n");
}
