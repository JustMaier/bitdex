# Channel-Centric Backpressure Design

## Problem Statement

The current write path has two modes: **normal** (publishes a snapshot every flush cycle ~100us) and **loading mode** (skips snapshot publishing entirely). This is a binary switch — either queries see fresh data with an expensive clone cascade, or queries see completely stale data with zero publishing overhead.

We need a **smooth, auto-throttling** system that:
- Eliminates the binary loading mode switch
- Automatically reduces publishing frequency under sustained write pressure
- Automatically resumes normal publishing when write pressure drops
- Prevents diff bloat in VersionedBitmaps during bursts
- Achieves 300K docs/s target (up from 62K docs/s baseline)

## Architecture: Channel Depth as the Single Control Signal

The crossbeam channel between writers and the flush thread already has a natural depth signal: `rx.len()`. This depth directly measures how far behind the flush thread is — the exact metric we need for throttling.

### Key Insight

The expensive operations in the flush loop are:
1. **`staging.clone()`** — O(num_fields), triggers `Arc::make_mut()` deep-clones on next mutation cycle
2. **Cache invalidation** — lock contention on trie cache, bound cache
3. **Filter diff compaction** — merges accumulated diffs into bases
4. **Redb docstore commits** — fsync-bound I/O

By making the frequency of these operations proportional to channel pressure, we get smooth auto-throttling with zero binary switches.

## Design: Pressure-Driven Flush Loop

### Pressure Levels

Define four pressure levels based on channel fill ratio (`depth / capacity`):

| Level | Fill Ratio | Name | Behavior |
|-------|-----------|------|----------|
| 0 | 0-10% | Idle | Normal operation: publish every cycle, compact every 50 cycles |
| 1 | 10-50% | Warm | Publish every 4th cycle, compact every 100 cycles |
| 2 | 50-80% | Hot | Publish every 16th cycle, compact every 200 cycles, skip bound maintenance |
| 3 | 80-100% | Critical | Skip publishing entirely (like current loading mode), compact only on level transition |

The thresholds are configurable but these defaults work well because:
- **10% = 10K ops** at default 100K capacity = ~400-600 documents worth of mutations. If we're accumulating this much, flush is falling behind.
- **50% = 50K ops** = serious sustained pressure, publishing every cycle is clearly wasteful.
- **80% = 80K ops** = approaching channel saturation, writers will start blocking on `send()`. We need maximum flush throughput.

### Pressure Level Struct

```rust
struct PressureState {
    level: u8,                    // 0-3
    cycles_since_publish: u64,    // tracks publish cadence
    cycles_since_compact: u64,    // tracks compaction cadence
    level_entry_time: Instant,    // when we entered current level (for hysteresis)
    cumulative_ops_at_level: u64, // ops processed at current level (for metrics)
}

impl PressureState {
    /// Update pressure level from channel depth.
    /// Uses hysteresis: requires sustained pressure for 3 cycles to escalate,
    /// but de-escalates immediately when pressure drops.
    fn update(&mut self, fill_ratio: f64, cycle: u64) -> PressureTransition {
        let target_level = match fill_ratio {
            r if r < 0.10 => 0,
            r if r < 0.50 => 1,
            r if r < 0.80 => 2,
            _ => 3,
        };

        if target_level > self.level {
            // Escalation: immediate (channel is filling, don't wait)
            let old = self.level;
            self.level = target_level;
            self.level_entry_time = Instant::now();
            self.cycles_since_publish = 0;
            self.cycles_since_compact = 0;
            PressureTransition::Escalated { from: old, to: target_level }
        } else if target_level < self.level {
            // De-escalation: require the channel to stay below threshold
            // for at least 2 cycles to avoid oscillation
            if self.level_entry_time.elapsed() > Duration::from_micros(200) {
                let old = self.level;
                self.level = target_level;
                self.level_entry_time = Instant::now();
                PressureTransition::DeEscalated { from: old, to: target_level }
            } else {
                PressureTransition::Held
            }
        } else {
            PressureTransition::Held
        }
    }

    fn should_publish(&mut self) -> bool {
        self.cycles_since_publish += 1;
        let interval = match self.level {
            0 => 1,   // every cycle
            1 => 4,   // every 4th
            2 => 16,  // every 16th
            _ => u64::MAX, // never (level 3)
        };
        if self.cycles_since_publish >= interval {
            self.cycles_since_publish = 0;
            true
        } else {
            false
        }
    }

    fn should_compact(&mut self) -> bool {
        self.cycles_since_compact += 1;
        let interval = match self.level {
            0 => 50,   // ~5ms at 100us flush interval
            1 => 100,  // ~10ms
            2 => 200,  // ~20ms
            _ => u64::MAX, // only on transition
        };
        if self.cycles_since_compact >= interval {
            self.cycles_since_compact = 0;
            true
        } else {
            false
        }
    }

    fn should_maintain_bounds(&self) -> bool {
        // Skip bound maintenance at level 2+ (it's O(mutated_slots * matching_bounds))
        self.level < 2
    }

    fn should_invalidate_cache(&self) -> bool {
        // Cache invalidation is cheap but unnecessary at critical pressure
        // since we're not publishing anyway
        self.level < 3
    }
}
```

### Adaptive Flush Loop Pseudocode

```rust
fn flush_loop(/* existing params */) {
    let mut pressure = PressureState::new();
    let capacity = channel_capacity as f64;

    while !shutdown.load(Relaxed) {
        thread::sleep(current_sleep);

        // Step 1: Read channel depth BEFORE draining
        let depth = coalescer.pending_count();
        let fill_ratio = depth as f64 / capacity;
        let transition = pressure.update(fill_ratio, flush_cycle);

        // Step 2: On de-escalation from level 3, force-publish + compact
        // (same semantics as current loading mode exit)
        if let PressureTransition::DeEscalated { from: 3, .. } = transition {
            for (_name, field) in staging.filters.fields_mut() {
                field.merge_dirty();
            }
            invalidate_all_caches();
            inner.store(Arc::new(staging.clone()));
            staging_dirty = false;
        }

        // Step 3: Drain channel and prepare batch (unchanged)
        let bitmap_count = coalescer.prepare();

        if bitmap_count > 0 {
            staging_dirty = true;

            // Extract Tier 2 mutations, apply to staging (unchanged)
            let tier2_mutations = coalescer.take_tier2_mutations(&tier2_fields);
            coalescer.apply_prepared(&mut staging.slots, &mut staging.filters, &mut staging.sorts);
            activate_deferred_alive(&mut staging);

            // Step 4: Conditional maintenance based on pressure level
            if pressure.should_maintain_bounds() {
                do_bound_maintenance(&mut staging, &coalescer, &flush_bound_cache);
            }

            // Route Tier 2 mutations (always — cheap and needed for correctness)
            route_tier2_mutations(tier2_mutations, &flush_pending, &flush_tier2_cache);

            // Cache invalidation (skip at critical pressure — no publish means no readers see stale data)
            if pressure.should_invalidate_cache() {
                invalidate_changed_caches(&coalescer, &cache, &field_names, &flush_tier2_cache);
                invalidate_changed_bounds(&coalescer, &flush_bound_cache);
            }

            // Periodic compaction
            if pressure.should_compact() {
                for (_name, field) in staging.filters.fields_mut() {
                    field.merge_dirty();
                }
            }

            // Step 5: Conditional snapshot publishing
            if pressure.should_publish() && staging_dirty {
                inner.store(Arc::new(staging.clone()));
                staging_dirty = false;
            }
        }

        // Step 6: Drain docstore channel with pressure-aware batching
        drain_docstore_channel(&doc_rx, &mut doc_batch, &docstore, &pressure);

        // Adaptive sleep (unchanged logic)
        if bitmap_count > 0 || !doc_batch.is_empty() {
            current_sleep = min_sleep;
        } else {
            current_sleep = (current_sleep * 2).min(max_sleep);
        }

        flush_cycle += 1;
    }
}
```

## Diff Bloat Prevention

### The Problem

At pressure levels 1-3, we publish less frequently. This means the diff layer in VersionedBitmaps accumulates more changes between publishes. Unbounded diff growth causes:
1. **Memory waste**: diff sets/clears bitmaps grow alongside bases
2. **Slower queries**: `apply_diff()` and `fused()` do more work per bitmap
3. **Longer compaction pauses**: when we finally merge, there's more to merge

### The Solution: Diff Size Budget

Add a **diff size budget** that triggers compaction regardless of pressure level. This is already partially implemented via `merge_diff_threshold` in `StorageConfig` (default: 10,000 set+clear entries). The key change is making the flush loop check diff sizes and compact hot bitmaps proactively.

```rust
// In flush loop, after applying mutations:
const DIFF_BUDGET_CHECK_INTERVAL: u64 = 20; // every 20 cycles

if flush_cycle % DIFF_BUDGET_CHECK_INTERVAL == 0 {
    let threshold = config.storage.merge_diff_threshold;
    for (_name, field) in staging.filters.fields_mut() {
        // Only compact bitmaps that have grown beyond budget
        field.merge_if_over_threshold(threshold);
    }
}
```

This adds a new method to `FilterField`:

```rust
impl FilterField {
    /// Merge only VersionedBitmaps whose diff serialized size exceeds threshold.
    /// Unlike merge_dirty() which merges ALL dirty bitmaps, this is selective.
    fn merge_if_over_threshold(&mut self, threshold: usize) {
        for (_, vb) in self.values.iter_mut() {
            let vb = Arc::make_mut(vb);
            if vb.diff().serialized_size() > threshold {
                vb.merge();
            }
        }
    }
}
```

The budget check is O(num_distinct_values) per field, but only runs every 20 cycles and only triggers `merge()` on the outliers. At 104M records with 104K userId values, this is ~104K lookups every 2ms — negligible.

### Diff Bloat Under Sustained Level 3

At level 3 (80-100% fill), we skip compaction entirely. But writes keep accumulating diffs. The safety net:

1. **Channel backpressure itself limits rate**: at 80% fill, writers start spending more time blocked on `send()`, which naturally limits how fast diffs can grow.
2. **De-escalation forces compaction**: when we drop below 80% fill, the level 3 -> level 2 transition triggers a full compaction pass before resuming publishing.
3. **Max diff cardinality is bounded by unique slot IDs**: a diff can have at most `alive_count` entries in sets + clears. In practice, batch grouping means the same slot appears once per (field, value) pair.

## Edge Cases

### Burst Ends Suddenly

**Scenario**: Writers stop abruptly after sustained level 3 pressure. Channel drains to empty within a few flush cycles.

**Behavior**:
1. Next cycle after drain: `fill_ratio` drops to 0%, pressure updates from level 3 to level 0.
2. `DeEscalated { from: 3, to: 0 }` triggers the force-publish path:
   - Compact all filter diffs
   - Invalidate all caches
   - Publish snapshot
3. Queries immediately see fresh data.

**No staleness gap**: The force-publish on de-escalation from level 3 ensures the same semantics as `exit_loading_mode()`.

### Reader Queries During Burst

**Scenario**: A query arrives while pressure is at level 2 (publishing every 16th cycle).

**Behavior**:
- The query sees the last published snapshot (up to 16 flush cycles stale at level 2, fully stale at level 3).
- This is acceptable because:
  - At level 2, we're publishing every ~1.6ms (16 * 100us). Most queries don't need sub-2ms freshness.
  - At level 3, the system is saturated — the alternative is queries seeing fresh data but writes running 5x slower.
- **Post-validation against in-flight writes** still works: the `InFlightTracker` ensures overlapping slot IDs are revalidated regardless of snapshot staleness.

### Channel Drains Faster Than Expected

**Scenario**: Write pressure spikes to level 2, but the flush thread catches up within a few cycles (e.g., the burst was small).

**Behavior**:
1. Pressure escalates to level 2 immediately.
2. After 2+ cycles with `fill_ratio < 0.50`, pressure de-escalates to level 0.
3. The 2-cycle hysteresis on de-escalation prevents thrashing between levels.
4. Normal publishing resumes.

**Risk**: If the burst is exactly at a threshold boundary, we might oscillate. The hysteresis timer (200us) dampens this effectively — at 100us flush interval, it requires two consecutive sub-threshold readings.

### Concurrent Readers + Writers at Level 1

**Scenario**: Steady-state mixed workload with moderate write pressure (10-50% fill).

**Behavior**:
- Publish every 4th cycle = every 400us.
- Queries see data that's at most 400us stale (vs 100us at level 0).
- This is the sweet spot: 4x fewer `staging.clone()` operations, but queries still get near-real-time data.
- Cache invalidation still runs every cycle, so cached results are always invalidated promptly even if the new snapshot isn't published yet.

**Note**: Cache invalidation at level 1 technically invalidates cache entries that won't be visible to queries yet (the snapshot hasn't been published). This is conservative — it means the first query after publish will re-compute the filter bitmap. This is correct but slightly wasteful. We could defer invalidation to align with publish cycles, but the complexity isn't worth it for level 1.

## Smooth Transition Behavior

The key to smooth transitions is that **pressure levels change the cadence, not the mechanics**. The flush loop always:
1. Drains the channel
2. Groups and sorts mutations
3. Applies to staging

Only the *optional* steps (publish, compact, bound maintain, cache invalidate) are gated by pressure level. This means:
- Escalation is instantaneous and lossless (mutations are always applied)
- De-escalation is safe (force-publish on level 3 exit, gradual resume otherwise)
- No special "mode" flag — just a continuous variable driving cadence

### Comparison to Current Loading Mode

| Aspect | Current Loading Mode | Channel Backpressure |
|--------|---------------------|---------------------|
| Activation | Manual `enter_loading_mode()` call | Automatic from channel depth |
| Granularity | Binary (on/off) | 4 levels with smooth transitions |
| Query freshness | Fully stale during loading | Stale proportional to pressure (400us to fully stale) |
| Exit behavior | Manual `exit_loading_mode()` + polling | Automatic force-publish on de-escalation |
| Diff bloat | Unbounded until exit | Budget-capped via periodic threshold checks |
| Bound maintenance | Skipped entirely | Skipped only at level 2+ |
| Cache invalidation | Skipped entirely | Skipped only at level 3 |

## Configuration

Add to `Config`:

```rust
/// Backpressure configuration for the flush thread.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackpressureConfig {
    /// Channel fill ratio thresholds for pressure levels [warm, hot, critical].
    /// Default: [0.10, 0.50, 0.80]
    #[serde(default = "default_pressure_thresholds")]
    pub pressure_thresholds: [f64; 3],

    /// Publish cadence per pressure level [idle, warm, hot].
    /// Level 3 (critical) never publishes. Values are flush cycle counts.
    /// Default: [1, 4, 16]
    #[serde(default = "default_publish_cadence")]
    pub publish_cadence: [u64; 3],

    /// Compaction cadence per pressure level [idle, warm, hot].
    /// Level 3 compacts only on de-escalation.
    /// Default: [50, 100, 200]
    #[serde(default = "default_compact_cadence")]
    pub compact_cadence: [u64; 3],

    /// Maximum diff serialized bytes before forced compaction (per bitmap).
    /// Default: 10000 (already exists as merge_diff_threshold)
    #[serde(default = "default_diff_budget")]
    pub diff_budget: usize,
}
```

## Redb Commit Frequency

The docstore `put_batch()` is already batched — the flush thread drains the doc channel and writes one redb transaction per flush cycle. Under pressure:

- **Level 0-1**: Batch sizes are naturally small (few docs accumulate between 100-400us flush cycles). Redb commits are frequent but small.
- **Level 2-3**: Batch sizes grow large (more docs accumulate between 1.6ms-indefinite flush cycles). Redb commits are less frequent but larger.

This is exactly the right behavior: under write pressure, we batch more documents per redb transaction, amortizing the fsync cost. No special handling needed — the existing drain-and-batch logic automatically adapts.

For additional throughput at level 2+, we can defer docstore commits:

```rust
// Only commit docstore every N cycles at high pressure
let doc_commit_interval = match pressure.level {
    0 | 1 => 1,  // every cycle
    2 => 4,      // every 4th cycle
    _ => 16,     // every 16th cycle
};
if flush_cycle % doc_commit_interval == 0 && !doc_batch.is_empty() {
    docstore.put_batch(&doc_batch)?;
    doc_batch.clear();
} else {
    // Accumulate but don't commit yet
    while let Ok(item) = doc_rx.try_recv() {
        doc_batch.push(item);
    }
}
```

This means docs accumulate in memory longer during bursts but reduces redb write transaction overhead by 4-16x.

## Merge Thread Interaction

The merge thread runs on a separate timer (default 5s). Under write pressure:
- Merge thread continues snapshotting and persisting to redb normally
- It reads from `inner.load_full()` which gets the last published snapshot
- At level 3, the published snapshot is stale, so merge persists stale data — this is fine because the merge thread's purpose is crash recovery, not freshness
- On de-escalation, the force-publish updates the snapshot, and the next merge cycle persists the fresh state

No changes needed to the merge thread.

## Expected Performance Impact

### Why This Achieves 300K docs/s

At current 62K docs/s with normal publishing:
- Each doc = ~20 MutationOps sent through channel
- 62K docs/s = 1.24M ops/s through channel
- Flush thread drains + applies + publishes at ~100us cadence = 10K flush cycles/s
- Each cycle drains ~124 ops, applies them, and publishes

The bottleneck is `staging.clone()` + `Arc::make_mut()`. At level 2+ (16x fewer publishes):
- `staging.clone()` happens every 1.6ms instead of every 100us
- Each clone is more expensive (larger diffs to compact), but 16x fewer clones
- Net: ~10-14x reduction in clone overhead

With loading mode (current), we get 56K docs/s at 5M (removing clone entirely). With level 2-3 (near-zero cloning), we should approach that throughput while still publishing periodically. The 300K target likely requires the **bulk bitmap construction** path (separate brainstorm) for the initial insert phase, but channel backpressure ensures sustained writes don't regress.

### Steady-State Benefit

For production workloads (100-1000 writes/s with concurrent reads), the system stays at level 0 — identical to current behavior. The backpressure only activates when the flush thread genuinely falls behind.

## Migration Path

1. Add `PressureState` to the flush loop (replaces `was_loading` / `staging_dirty` logic)
2. Remove `loading_mode: Arc<AtomicBool>` from `ConcurrentEngine`
3. Remove `enter_loading_mode()` / `exit_loading_mode()` public API
4. Benchmark harness no longer needs to call loading mode — backpressure activates automatically during bulk inserts
5. `rebuild_from_docstore()` no longer needs special loading/exit logic

The external API simplifies: callers just call `put()` and the system auto-tunes.
