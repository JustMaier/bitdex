# Bulk Bitmap Construction Path

## Problem Statement

Current write throughput: **62K docs/s** at 1M records. Target: **300K docs/s** (5x).

The bottleneck is the per-document MutationOp generation and channel transport. Each document produces 16-28 MutationOps (1 alive + 5-10 filter inserts + 10-17 sort layer sets). At 104M records, this means ~2 billion individual `MutationOp` messages through the crossbeam channel, each carrying a `Vec<u32>` with a single slot ID.

### Where Time Goes (Per Document)

1. **diff_document()** — Computes ~20 MutationOps, each allocating a `Vec<u32>` with 1 element (~24 bytes heap + Arc<str> clone per op)
2. **Channel send** — Each op sent individually via `sender.send_batch(ops)`, which loops and calls `self.tx.send(op)` per op
3. **Channel drain** — Flush thread drains all ops into `WriteBatch.ops: Vec<MutationOp>`
4. **Group and sort** — Iterates all ops, groups by (field, value), extends Vec<u32> per group, then sorts each Vec
5. **Apply** — Iterates grouped Vecs, calls `insert_bulk()` or `set_layer_bulk()` on VersionedBitmaps

Steps 1-3 are dominated by per-op overhead: allocation, channel contention, and intermediate buffering. Steps 4-5 are already reasonably efficient once data is grouped.

## Design: Direct Accumulator Architecture

Instead of generating per-document MutationOps and sending them through a channel, accumulate (field, value) -> Vec<slot_id> mappings directly in caller-side buffers. When a threshold is hit, flush the accumulated data directly to bitmaps.

### Architecture Overview

```
                          Normal Path (< threshold)
                          ========================
Writer Thread ──> diff_document() ──> MutationOp channel ──> Flush thread
                                                              │
                          Bulk Path (>= threshold)            │
                          ============================        │
Writer Threads ──> BulkAccumulator ──────────────────> Bulk Flush ──> Bitmap State
                   (per-thread or shared)                     │
                                                              ▼
                                                        DocStore batch
```

### Core Data Structure: BulkAccumulator

```rust
/// Accumulates (field, value) -> sorted slot IDs for bulk bitmap construction.
/// One per writer thread to avoid contention, merged before flush.
struct BulkAccumulator {
    /// filter_field_name -> (bitmap_key -> Vec<slot_id>)
    filter_sets: HashMap<Arc<str>, HashMap<u64, Vec<u32>>>,

    /// filter_field_name -> (bitmap_key -> Vec<slot_id>)  [for upsert removes]
    filter_clears: HashMap<Arc<str>, HashMap<u64, Vec<u32>>>,

    /// sort_field_name -> (bit_layer -> Vec<slot_id>)
    sort_sets: HashMap<Arc<str>, Vec<Vec<u32>>>,  // indexed by bit_layer

    /// sort_field_name -> (bit_layer -> Vec<slot_id>)  [for upsert clears]
    sort_clears: HashMap<Arc<str>, Vec<Vec<u32>>>,

    /// Alive inserts
    alive_inserts: Vec<u32>,

    /// Alive removes (rare during bulk load)
    alive_removes: Vec<u32>,

    /// Document count accumulated so far
    doc_count: usize,

    /// Docstore batch
    doc_batch: Vec<(u32, StoredDoc)>,
}
```

### Key Insight: Bypass diff_document Entirely for Fresh Inserts

During bulk loads, documents are almost always **fresh inserts** (slot not alive, no old doc). The entire `diff_document()` path — reading old doc from docstore, computing diffs, generating MutationOps — can be replaced with direct field value decomposition:

```rust
impl BulkAccumulator {
    /// Accumulate a fresh insert document. No diff, no MutationOps, no channel.
    fn accumulate_insert(&mut self, slot: u32, doc: &Document, config: &Config, registry: &FieldRegistry) {
        // Filter fields: decompose directly to (field, value) -> slot
        for fc in &config.filter_fields {
            if let Some(fv) = doc.fields.get(&fc.name) {
                let field_name = registry.get(&fc.name);
                match fv {
                    FieldValue::Single(v) => {
                        if let Some(key) = value_to_bitmap_key(v) {
                            self.filter_sets
                                .entry(field_name)
                                .or_default()
                                .entry(key)
                                .or_default()
                                .push(slot);
                        }
                    }
                    FieldValue::Multi(vals) => {
                        for v in vals {
                            if let Some(key) = value_to_bitmap_key(v) {
                                self.filter_sets
                                    .entry(field_name.clone())
                                    .or_default()
                                    .entry(key)
                                    .or_default()
                                    .push(slot);
                            }
                        }
                    }
                }
            }
        }

        // Sort fields: decompose value to bit layers directly
        for sc in &config.sort_fields {
            if let Some(fv) = doc.fields.get(&sc.name) {
                if let FieldValue::Single(val) = fv {
                    if let Some(sort_val) = value_to_sort_u32(val) {
                        let field_name = registry.get(&sc.name);
                        let layers = self.sort_sets
                            .entry(field_name)
                            .or_insert_with(|| vec![Vec::new(); sc.bits as usize]);
                        for bit in 0..sc.bits as usize {
                            if (sort_val >> bit) & 1 == 1 {
                                layers[bit].push(slot);
                            }
                        }
                    }
                }
            }
        }

        self.alive_inserts.push(slot);
        self.doc_count += 1;
    }
}
```

### Bitmap Construction from Accumulated Data

When the accumulator hits its flush threshold, convert accumulated Vec<u32> directly to bitmap operations:

```rust
impl BulkAccumulator {
    /// Flush accumulated data directly to bitmap state.
    /// Called while holding mutable access to the staging InnerEngine.
    fn flush_to_bitmaps(
        &mut self,
        slots: &mut SlotAllocator,
        filters: &mut FilterIndex,
        sorts: &mut SortIndex,
    ) {
        // Sort all slot vectors for optimal roaring extend() performance.
        // This is the same trick WriteBatch uses, but we do it once over
        // much larger vectors instead of per-flush-cycle.

        // Filter inserts: sort and extend
        for (field_name, value_map) in &mut self.filter_sets {
            if let Some(field) = filters.get_field_mut(field_name) {
                for (value, slot_ids) in value_map.iter_mut() {
                    slot_ids.sort_unstable();
                    field.insert_bulk(*value, slot_ids.drain(..));
                }
            }
        }

        // Filter removes (upserts only)
        for (field_name, value_map) in &mut self.filter_clears {
            if let Some(field) = filters.get_field_mut(field_name) {
                for (value, slot_ids) in value_map.iter_mut() {
                    field.remove_bulk(*value, slot_ids);
                    slot_ids.clear();
                }
            }
        }

        // Sort layer sets
        for (field_name, layers) in &mut self.sort_sets {
            if let Some(field) = sorts.get_field_mut(field_name) {
                for (bit, slot_ids) in layers.iter_mut().enumerate() {
                    if !slot_ids.is_empty() {
                        slot_ids.sort_unstable();
                        field.set_layer_bulk(bit, slot_ids.drain(..));
                    }
                }
            }
        }

        // Sort layer clears
        for (field_name, layers) in &mut self.sort_clears {
            if let Some(field) = sorts.get_field_mut(field_name) {
                for (bit, slot_ids) in layers.iter_mut().enumerate() {
                    if !slot_ids.is_empty() {
                        field.clear_layer_bulk(bit, slot_ids);
                        slot_ids.clear();
                    }
                }
            }
        }

        // Alive
        if !self.alive_inserts.is_empty() {
            self.alive_inserts.sort_unstable();
            slots.alive_insert_bulk(self.alive_inserts.drain(..));
        }
        for &slot in &self.alive_removes {
            slots.alive_remove_one(slot);
        }
        self.alive_removes.clear();

        // Eager merge for sort diffs (required before readers see them)
        // Note: During loading mode, this only matters at exit.
        // Filter diffs accumulate — merged periodically or on loading mode exit.

        self.doc_count = 0;
    }
}
```

### Alternative: from_sorted_iter for Initial Load

For the very first load into empty bitmaps, we can go even further. Instead of extending existing bitmaps, build new ones from scratch:

```rust
/// Build a fresh RoaringBitmap from a pre-sorted Vec<u32>.
/// This is the absolute fastest path — roaring-rs can construct optimal
/// container layout in a single pass.
fn build_bitmap_from_sorted(sorted_slots: &[u32]) -> RoaringBitmap {
    RoaringBitmap::from_sorted_iter(sorted_slots.iter().copied()).unwrap()
}
```

This is relevant for the initial backfill scenario (Phase 5 integration). During `from_docstore()`, bitmaps start empty, so every insert is additive. We could accumulate ALL slots for a given (field, value) pair, sort once, and call `from_sorted_iter` once per bitmap. This avoids the overhead of extending pre-existing containers.

However, this requires holding all slot IDs in memory simultaneously. At 104M records with ~20 values per doc, that's ~2B slot references. At 4 bytes each: **~8 GB** — too much. The chunked approach (flush every N docs) is more practical. See memory estimates below.

## Memory Estimates for Accumulation Buffers

Each slot ID is a `u32` (4 bytes). Additional overhead per Vec: 24 bytes (ptr + len + cap). HashMap entry overhead: ~80 bytes per entry (including key).

### Per-Document Analysis (Civitai Schema)

A typical document has:
- 5-6 filter fields, ~10 distinct values (tagIds contributes most)
- 3 sort fields, each decomposed into ~16 set bits on average
- Total: ~10 filter slot IDs + ~48 sort slot IDs + 1 alive = **~59 slot IDs per document**

### Accumulator Memory by Flush Threshold

| Flush Threshold | Slot IDs | Slot ID Memory | HashMap Overhead | Total Estimate |
|----------------:|---------:|---------------:|-----------------:|---------------:|
| 1,000 docs | 59K | 236 KB | ~50 KB | **~300 KB** |
| 10,000 docs | 590K | 2.36 MB | ~50 KB | **~2.5 MB** |
| 50,000 docs | 2.95M | 11.8 MB | ~50 KB | **~12 MB** |
| 100,000 docs | 5.9M | 23.6 MB | ~50 KB | **~24 MB** |
| 1,000,000 docs | 59M | 236 MB | ~50 KB | **~240 MB** |

HashMap overhead is negligible because the number of distinct (field, value) pairs is bounded by the schema — typically ~200-300K distinct filter values and ~96 sort bit layers.

**Recommendation: Flush threshold of 50,000-100,000 docs.** This gives 12-24 MB memory usage (trivial relative to the 12+ GB RSS) while batching enough for efficient bitmap construction.

### Per-Thread vs Shared Accumulator

**Option A: Shared accumulator with Mutex** — Simpler, but contention at high thread counts. The accumulate step is fast (no disk I/O), so lock hold time is short (~100ns per doc).

**Option B: Per-thread accumulators, merged before flush** — Zero contention during accumulation. Merge step sorts and concatenates all thread-local Vecs. At 4 threads x 50K docs = 200K docs total before flush.

**Recommendation: Per-thread accumulators (Option B).** The merge cost is trivial compared to bitmap construction, and it eliminates all contention during the hot path.

```rust
/// Per-thread accumulator. Writer threads accumulate locally,
/// then submit the entire buffer to the flush thread when full.
struct PerThreadAccumulator {
    inner: BulkAccumulator,
    flush_threshold: usize,  // docs per thread before submitting
}

impl PerThreadAccumulator {
    fn accumulate_and_maybe_submit(
        &mut self,
        slot: u32,
        doc: &Document,
        config: &Config,
        registry: &FieldRegistry,
        submit_fn: impl FnOnce(BulkAccumulator),
    ) {
        self.inner.accumulate_insert(slot, doc, config, registry);

        if self.inner.doc_count >= self.flush_threshold {
            let completed = std::mem::replace(&mut self.inner, BulkAccumulator::new());
            submit_fn(completed);
        }
    }
}
```

## Interaction with the DocStore

### Current: Per-Document Writes

The current path sends each `(slot_id, StoredDoc)` through a separate crossbeam channel. The flush thread drains and calls `docstore.put_batch()`. This already batches, but the channel transport adds overhead.

### Bulk Path: Co-located Batching

The BulkAccumulator holds `doc_batch: Vec<(u32, StoredDoc)>` alongside the bitmap data. When the accumulator flushes, docstore writes happen in the same batch:

```rust
impl BulkAccumulator {
    fn flush_all(
        &mut self,
        slots: &mut SlotAllocator,
        filters: &mut FilterIndex,
        sorts: &mut SortIndex,
        docstore: &DocStore,
    ) -> Result<()> {
        // 1. Write docs to disk first (crash safety: doc exists before bitmap)
        if !self.doc_batch.is_empty() {
            docstore.put_batch(&self.doc_batch)?;
            self.doc_batch.clear();
        }

        // 2. Apply bitmaps
        self.flush_to_bitmaps(slots, filters, sorts);

        Ok(())
    }
}
```

### Ordering: Docstore Before Bitmaps

Writing docs before bitmaps is important for correctness: if we crash after bitmap writes but before doc writes, a future upsert would see the slot as alive but have no old doc for diffing. Writing docs first means on crash recovery, we may have docs without bitmap entries — benign, since the slot won't be alive and the doc will be overwritten on re-insert.

**Batched redb writes are critical.** At 50K docs per batch, the single redb write transaction amortizes the fsync cost. Without batching, redb serializes write transactions and each `put()` does a full fsync. The existing `put_batch()` method already handles this correctly.

## Detecting Bulk Mode vs Normal Mode

### Signal: Loading Mode Flag

The engine already has `loading_mode: Arc<AtomicBool>` that's toggled by `enter_loading_mode()` / `exit_loading_mode()`. This is the primary signal:

- **Loading mode ON** -> Use bulk accumulator path
- **Loading mode OFF** -> Use normal MutationOp channel path

This is clean because loading mode already disables snapshot publishing and maintenance — the bulk path just extends this optimization.

### Signal: Channel Depth (Adaptive)

For steady-state writes that aren't explicitly in loading mode, we could detect high write pressure via channel depth:

```rust
const BULK_MODE_THRESHOLD: usize = 10_000;  // pending ops in channel

fn should_use_bulk_path(&self) -> bool {
    self.loading_mode.load(Ordering::Relaxed)
        || self.sender.pending_count() > BULK_MODE_THRESHOLD
}
```

**Recommendation: Start with loading mode only.** Adaptive switching adds complexity (what happens to in-flight MutationOps when switching? how do we handle partial accumulators?). Loading mode is the 90% case for bulk writes.

## Handling Transition: Mid-Load Slowdown

### Scenario: Bulk load in progress, writes slow down or stop

The accumulator may have a partial batch (< threshold docs). Options:

1. **Timer-based flush**: If no new docs arrive within N milliseconds, flush the partial batch. This is the flush thread's existing adaptive sleep pattern.
2. **Explicit drain**: When `exit_loading_mode()` is called, force-flush all per-thread accumulators.
3. **Drop guard**: If the accumulator is dropped (thread panics), flush remaining data.

```rust
impl Drop for PerThreadAccumulator {
    fn drop(&mut self) {
        if self.inner.doc_count > 0 {
            // Submit partial batch — cannot panic here
            self.submit_partial();
        }
    }
}
```

### Scenario: Upserts during bulk load

During bulk loads, most documents are fresh inserts. But some may be upserts (slot already alive from a previous batch). The accumulator must handle this:

```rust
fn accumulate(&mut self, slot: u32, doc: &Document, config: &Config,
              registry: &FieldRegistry, is_upsert: bool, old_doc: Option<&StoredDoc>) {
    if is_upsert {
        // Use diff path — accumulate removes for old values, inserts for new
        self.accumulate_upsert(slot, doc, config, registry, old_doc.unwrap());
    } else {
        // Fast path — just accumulate inserts
        self.accumulate_insert(slot, doc, config, registry);
    }
}
```

The upsert case is identical to the normal diff path but stores the results in the accumulator's HashMaps instead of generating MutationOps. The optimization is still significant: we avoid channel overhead and get better batching.

## Interaction with VersionedBitmap Diffs

### During Loading Mode: Write Directly to Diffs

The existing `insert_bulk()` and `set_layer_bulk()` methods on VersionedBitmap write to the diff layer. This is correct behavior:

- **Filter diffs**: Accumulate in diff layer, merged periodically by the compaction interval or on loading mode exit
- **Sort diffs**: Merged eagerly after each flush (required by sort traversal's `debug_assert!(!is_dirty())`)
- **Alive diffs**: Merged eagerly (required for correct `is_alive` checks)

### Should We Bypass Diffs Entirely?

For the initial backfill (empty engine, all fresh inserts), we could write directly to base bitmaps:

```rust
// Hypothetical: build fresh base bitmaps from accumulated data
fn build_base_bitmaps(accumulated: &HashMap<u64, Vec<u32>>) -> HashMap<u64, VersionedBitmap> {
    accumulated.iter().map(|(&value, slots)| {
        let bitmap = RoaringBitmap::from_sorted_iter(slots.iter().copied()).unwrap();
        (value, VersionedBitmap::new(bitmap))
    }).collect()
}
```

**However, this only works for the first load.** If any bitmaps already exist (e.g., from persistence restore), we must OR into existing data, which means the diff layer is the right target.

**Recommendation: Keep using diff layer writes.** The overhead of diffs during loading mode is minimal because:
1. Loading mode suppresses snapshot publishing, so no Arc clone cascade
2. Diffs are merged on loading mode exit
3. The diff layer's `insert_bulk()` already uses efficient roaring extend

The real win comes from eliminating MutationOp allocation, channel transport, and intermediate grouping — not from bypassing diffs.

## Integration with Existing Loading Mode

The bulk accumulator path integrates cleanly with the existing loading mode:

```rust
impl ConcurrentEngine {
    /// Bulk put — uses accumulator path when in loading mode.
    /// Called from from_docstore() and future backfill pipeline.
    pub fn put_bulk(&self, id: u32, doc: &Document) -> Result<()> {
        if self.loading_mode.load(Ordering::Relaxed) {
            // Accumulator path: no channel, no per-doc MutationOps
            // Thread-local accumulator handles batching
            self.bulk_accumulate(id, doc)
        } else {
            // Normal path: diff -> MutationOps -> channel
            self.put(id, doc)
        }
    }
}
```

### Integration Points

1. **`enter_loading_mode()`** — Initialize per-thread accumulators, set flag
2. **`put()` during loading mode** — Route to accumulator instead of channel
3. **Accumulator full** — Submit to flush thread via a separate `BulkBatch` channel (not the MutationOp channel)
4. **`exit_loading_mode()`** — Force-flush all accumulators, merge all diffs, publish snapshot, invalidate caches (existing behavior)

### Flush Thread Changes

The flush thread needs a second channel for bulk batches:

```rust
// In flush thread loop:
// 1. Check for bulk batches first (higher priority during loading)
while let Ok(bulk_batch) = bulk_rx.try_recv() {
    bulk_batch.flush_to_bitmaps(&mut staging.slots, &mut staging.filters, &mut staging.sorts);
}
// 2. Normal MutationOp drain (for any non-bulk writes that leak through)
let bitmap_count = coalescer.prepare();
// ... existing logic
```

## Projected Performance Impact

### Sources of Speedup

| Optimization | Estimated Speedup |
|---|---|
| Eliminate ~20 MutationOp allocations per doc (Vec<u32> + Arc<str> per op) | 2-3x |
| Eliminate channel send/recv overhead (20 sends -> 0) | 1.5-2x |
| Eliminate intermediate WriteBatch grouping (already grouped in accumulator) | 1.2-1.5x |
| Larger sorted vectors for roaring extend (50K vs ~100 slots per group) | 1.1-1.3x |
| Batched docstore writes (already partially done, but now co-located) | 1.1x |

These compound multiplicatively. Conservative estimate: **3-5x** overall improvement, putting us in the 180K-310K docs/s range.

### Why Not More?

The remaining bottleneck will be:
1. **roaring bitmap extend() itself** — This is O(n) in the number of bits being set, with good constants. Already optimized.
2. **Docstore serialization** — bincode serialize + redb write. NVMe-bound at high throughput.
3. **Memory allocation for accumulator Vecs** — Mitigated by reusing buffers across flushes.

## Summary

| Aspect | Current Path | Bulk Path |
|---|---|---|
| Per-doc overhead | ~20 MutationOp allocs + channel sends | 1 HashMap insert per field value |
| Grouping | Post-hoc in WriteBatch::group_and_sort() | Pre-grouped in accumulator |
| Channel messages | ~20 per document | 1 per flush (50K docs) |
| Sorted input to roaring | Sorted per flush cycle (~100 slots) | Sorted per accumulator flush (~50K slots) |
| DocStore batching | Separate channel + batch in flush thread | Co-located with bitmap batch |
| Memory | Transient (channel buffer) | 12-24 MB per accumulator |
| Complexity | Simple (existing code) | Medium (new code path, per-thread state) |

The bulk path eliminates the two biggest costs in the current write path: per-document MutationOp allocation and channel transport. It preserves all existing correctness guarantees (diffs, loading mode, snapshot isolation) while providing a 3-5x throughput improvement for bulk loads.
