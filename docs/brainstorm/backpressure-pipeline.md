# Backpressure-Driven Flush Pipeline — Implementation Proposal

## Overview

This document is the detailed implementation proposal for replacing the current multi-channel, multi-lock `put()` path with a single-channel, pressure-driven pipeline where `put()` is a ~100ns channel send and the flush thread owns all processing.

The existing code has these key problems:
1. `put()` does 6 operations per document: mark in-flight, snapshot load, alive check, mutex lock on bulk_accum, doc clone for docstore channel, clear in-flight. Per-call cost: ~30us, ceiling: ~33K docs/s.
2. Two separate channels (MutationOp channel + docstore channel) that the flush thread drains independently.
3. Binary loading mode flag that callers must manually toggle.
4. BulkAccumulator behind a shared Mutex that writer threads contend on.

Target: `put()` costs ~100-200ns. Flush thread does all real work. Backpressure auto-adjusts from steady-state to burst mode based on channel depth.

---

## 1. Channel Design

### What the Channel Carries

```rust
enum InputMessage {
    Put { slot: u32, doc: Document },
    Delete { slot: u32 },
    Patch { slot: u32, patch: PatchPayload },
}
```

**Why not just `(u32, Document)`?** We need to support delete and patch through the same pipeline. Using an enum keeps one channel for all mutation types. Delete and Patch are small (no HashMap), so the enum's size is dominated by the Put variant.

**Ownership model: caller gives up ownership.** The `Document` struct contains `HashMap<String, FieldValue>` which is heap-allocated. Options considered:

| Option | Cost at put() | Cost at flush thread | Memory |
|--------|--------------|---------------------|--------|
| `doc.clone()` at put(), send clone | ~2-5us (HashMap clone + String clones + Vec clones for multi-value) | Zero | Double memory briefly |
| Send owned `Document` (caller moves) | Zero | Zero | Single copy |
| `Arc<Document>` wrapping | ~50ns (Arc alloc + refcount) | ~30ns (Arc deref) | Single copy + 16 bytes overhead |

**Decision: Send owned Document.** The caller constructs a `Document` to call `put()` — they have no further use for it. The signature becomes `put(id: u32, doc: Document)` (not `&Document`). For callers that need to retain the doc (rare), they clone before calling.

This is a **signature change** from the current `put(&self, id: u32, doc: &Document)` to `put(&self, id: u32, doc: Document)`. The benchmark harness and `rebuild_from_docstore()` both construct docs inline, so this is a straightforward change.

For `put_many`, the signature becomes `put_many(&self, docs: Vec<(u32, Document)>)` — each doc is moved into the channel individually.

### Channel Capacity

**Memory budget calculation:**

A typical Civitai `Document` contains ~12 fields: 3 filter fields (nsfwLevel, tagIds, onSite), 1 sort field (reactionCount), plus several extra fields stored for docstore. Each field entry is a `(String, FieldValue)` pair. Estimated per-doc size:

| Component | Size |
|-----------|------|
| HashMap overhead (12 entries, ~50% load) | ~384 bytes |
| String keys (avg 12 chars each, 12 fields) | ~12 * (24+12) = ~432 bytes |
| FieldValue::Single values (9 fields) | ~9 * 24 = ~216 bytes |
| FieldValue::Multi (tagIds, avg 8 tags) | ~24 + 8*16 = ~152 bytes |
| InputMessage enum overhead | ~8 bytes (discriminant + padding) |
| **Total per message** | **~1.2 KB** |

Channel capacity targets:

| Capacity | Memory | Backpressure Latency |
|----------|--------|---------------------|
| 10,000 | ~12 MB | Minimal — writers block quickly |
| 100,000 | ~120 MB | Good — 0.3s buffer at 300K docs/s |
| 500,000 | ~600 MB | Too much — wastes RAM |

**Decision: 100,000 capacity (default).** This gives ~0.3 seconds of buffering at our 300K docs/s target, enough to absorb burst writes without blocking callers, while keeping memory overhead at ~120 MB (< 1% of 14.5 GB RSS).

Configurable via `backpressure.channel_capacity` in Config.

### Backpressure: Bounded Channel Blocking

When the channel is full, `put()` blocks on `crossbeam::bounded::send()`. This is the correct behavior:

1. **It's self-regulating.** If the flush thread can't keep up, writers slow down to match. No unbounded memory growth.
2. **It propagates upstream.** In the benchmark's parser->writer->flush pipeline, blocking writers causes blocking parsers. The whole pipeline auto-throttles.
3. **The caller already blocks today.** Current `put()` blocks on the MutationOp channel (bounded at `channel_capacity`) and the docstore channel. The new design has one blocking point instead of two.
4. **Steady-state never blocks.** At production write rates (100-1000 docs/s), the channel never approaches capacity.

**Alternative considered: `try_send()` returning an error.** Rejected because callers would need retry logic, which adds complexity for no benefit. Blocking is the right semantic for a write pipeline.

---

## 2. New put() Method

### Signature

```rust
impl ConcurrentEngine {
    /// PUT(id, document) — enqueue for processing.
    ///
    /// Sends the document to the flush thread for processing. The flush thread
    /// handles alive checking, fresh vs upsert routing, bitmap mutation, and
    /// doc persistence. This call blocks only if the input channel is full
    /// (backpressure from the flush thread).
    ///
    /// Cost: ~100-200ns (one channel send, no locks, no snapshot load, no clone).
    pub fn put(&self, id: u32, doc: Document) -> Result<()> {
        self.input_tx.send(InputMessage::Put { slot: id, doc })
            .map_err(|_| BitdexError::CapacityExceeded("input channel disconnected".into()))?;
        Ok(())
    }

    /// DELETE(id) — enqueue alive removal.
    pub fn delete(&self, id: u32) -> Result<()> {
        self.input_tx.send(InputMessage::Delete { slot: id })
            .map_err(|_| BitdexError::CapacityExceeded("input channel disconnected".into()))?;
        Ok(())
    }

    /// PATCH(id, partial) — enqueue partial update.
    pub fn patch(&self, id: u32, patch: PatchPayload) -> Result<()> {
        self.input_tx.send(InputMessage::Patch { slot: id, patch })
            .map_err(|_| BitdexError::CapacityExceeded("input channel disconnected".into()))?;
        Ok(())
    }
}
```

### Per-Call Cost Analysis

| Operation | Old put() | New put() |
|-----------|----------|----------|
| mark_in_flight | DashSet insert (~150ns) | Removed |
| snapshot load | ArcSwap::load() (~10ns) | Removed |
| alive check | bitmap.contains() (~50ns) | Removed (flush thread does it) |
| was_ever_allocated | bitmap.contains() (~50ns) | Removed |
| bulk_accum.lock() | Mutex contention (~500ns-5us) | Removed |
| accumulate() | HashMap + Vec ops (~2-5us) | Removed |
| doc.fields.clone() | HashMap clone (~2-5us) | Removed (owned move) |
| doc_tx.send() | Channel send (~100ns) | Removed |
| input_tx.send() | N/A | Channel send (~100-200ns) |
| clear_in_flight | DashSet remove (~150ns) | Removed |
| **Total** | **~5-15us** | **~100-200ns** |

### put_many() for Batch Callers

```rust
pub fn put_many(&self, docs: Vec<(u32, Document)>) -> Result<()> {
    for (id, doc) in docs {
        self.input_tx.send(InputMessage::Put { slot: id, doc })
            .map_err(|_| BitdexError::CapacityExceeded("input channel disconnected".into()))?;
    }
    Ok(())
}
```

This is intentionally simple — just a loop of sends. The flush thread handles batching internally. No need for the current put_many()'s phased approach (bulk snapshot load, batch docstore reads, bulk ops collection) since all that work moved to the flush thread.

---

## 3. Flush Thread State Machine

### Pressure Levels

The flush thread reads `input_rx.len()` at the start of each cycle to determine pressure.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PressureLevel {
    Steady = 0,   // < low_watermark
    Warm = 1,     // low_watermark..high_watermark/2
    Hot = 2,      // high_watermark/2..high_watermark
    Burst = 3,    // >= high_watermark
}

struct PressureState {
    level: PressureLevel,
    cycles_at_level: u64,
    cycles_since_publish: u64,
    cycles_since_compact: u64,
    total_ops_at_level: u64,
}
```

### Behavior at Each Level

| Aspect | Steady (0) | Warm (1) | Hot (2) | Burst (3) |
|--------|-----------|----------|---------|-----------|
| **Drain strategy** | try_recv() loop, up to 1000 msgs | try_recv() loop, up to 5000 msgs | try_recv() loop, up to 20000 msgs | try_recv() loop, drain ALL |
| **Fresh insert routing** | diff_document + WriteBatch | diff_document + WriteBatch | BulkAccumulator | BulkAccumulator |
| **Upsert routing** | diff_document + WriteBatch | diff_document + WriteBatch | diff_document + WriteBatch | diff_document + WriteBatch |
| **Publish frequency** | Every cycle | Every 4th cycle | Every 16th cycle | Skip entirely |
| **Compaction frequency** | Every 50 cycles | Every 100 cycles | Every 200 cycles | Only on de-escalation |
| **Bound maintenance** | Yes | Yes | Skip | Skip |
| **Cache invalidation** | Yes | Yes | Yes (deferred to publish) | Skip |
| **Doc persist batch size** | Send every cycle | Send every 4th cycle | Send every 16th cycle | Send every 100th cycle |
| **Merge thread signal** | Normal interval | Normal interval | Signal "faster" | Signal "fastest" |
| **Accumulator flush threshold** | N/A (not using accumulator) | N/A | 5,000 docs | 50,000 docs or time limit |

### State Transition Hysteresis

```rust
impl PressureState {
    fn update(&mut self, channel_len: usize, channel_capacity: usize, config: &BackpressureConfig) {
        let target = if channel_len < config.low_watermark {
            PressureLevel::Steady
        } else if channel_len < config.high_watermark / 2 {
            PressureLevel::Warm
        } else if channel_len < config.high_watermark {
            PressureLevel::Hot
        } else {
            PressureLevel::Burst
        };

        if target as u8 > self.level as u8 {
            // ESCALATION: immediate. Channel is filling, don't wait.
            self.level = target;
            self.cycles_at_level = 0;
            self.cycles_since_publish = 0;
            self.cycles_since_compact = 0;
        } else if (target as u8) < self.level as u8 {
            // DE-ESCALATION: require 5+ cycles below threshold to avoid thrashing.
            // At 100us flush interval, this is 500us — prevents oscillation at boundaries.
            self.cycles_at_level += 1;
            if self.cycles_at_level >= config.deescalation_delay_cycles {
                // Special handling: de-escalation FROM Burst triggers force-publish
                let was_burst = self.level == PressureLevel::Burst;
                self.level = target;
                self.cycles_at_level = 0;
                if was_burst {
                    // Caller must handle: compact all diffs, invalidate all caches, force publish
                }
            }
        } else {
            self.cycles_at_level += 1;
        }
    }

    fn should_publish(&mut self) -> bool {
        self.cycles_since_publish += 1;
        let interval = match self.level {
            PressureLevel::Steady => 1,
            PressureLevel::Warm => 4,
            PressureLevel::Hot => 16,
            PressureLevel::Burst => u64::MAX, // never
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
            PressureLevel::Steady => 50,
            PressureLevel::Warm => 100,
            PressureLevel::Hot => 200,
            PressureLevel::Burst => u64::MAX, // only on transition
        };
        if self.cycles_since_compact >= interval {
            self.cycles_since_compact = 0;
            true
        } else {
            false
        }
    }

    fn should_maintain_bounds(&self) -> bool {
        self.level as u8 <= PressureLevel::Warm as u8
    }

    fn should_invalidate_cache(&self) -> bool {
        self.level != PressureLevel::Burst
    }

    fn max_drain_per_cycle(&self) -> usize {
        match self.level {
            PressureLevel::Steady => 1_000,
            PressureLevel::Warm => 5_000,
            PressureLevel::Hot => 20_000,
            PressureLevel::Burst => usize::MAX, // drain everything
        }
    }

    fn accumulator_flush_threshold(&self) -> usize {
        match self.level {
            PressureLevel::Steady | PressureLevel::Warm => usize::MAX, // not using accumulator
            PressureLevel::Hot => 5_000,
            PressureLevel::Burst => 50_000,
        }
    }
}
```

### Flush Loop Pseudocode

```rust
fn flush_loop(
    input_rx: Receiver<InputMessage>,
    inner: Arc<ArcSwap<InnerEngine>>,
    config: Arc<Config>,
    field_registry: FieldRegistry,
    docstore: Arc<DocStore>,
    doc_buffer: Arc<Mutex<HashMap<u32, StoredDoc>>>,
    cache: Arc<Mutex<TrieCache>>,
    bound_cache: Arc<Mutex<BoundCacheManager>>,
    pending: Arc<Mutex<PendingBuffer>>,
    tier2_cache: Option<Arc<Tier2Cache>>,
    persist_tx: Sender<Vec<(u32, StoredDoc)>>,
    shutdown: Arc<AtomicBool>,
) {
    let mut staging = initial_staging.clone();
    let mut pressure = PressureState::new();
    let mut accumulator = BulkAccumulator::new();
    let mut coalescer_batch = WriteBatch::new(); // reuse allocation
    let mut doc_batch: Vec<(u32, StoredDoc)> = Vec::new();
    let mut staging_dirty = false;
    let mut flush_cycle: u64 = 0;

    let min_sleep = Duration::from_micros(config.flush_interval_us);
    let max_sleep = Duration::from_micros(config.flush_interval_us * 10);
    let mut current_sleep = min_sleep;

    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(current_sleep);

        // === Step 1: Read pressure signal BEFORE draining ===
        let channel_len = input_rx.len();
        let was_burst = pressure.level == PressureLevel::Burst;
        pressure.update(channel_len, config.backpressure.channel_capacity, &config.backpressure);
        let left_burst = was_burst && pressure.level != PressureLevel::Burst;

        // === Step 2: Drain channel ===
        let max_drain = pressure.max_drain_per_cycle();
        let mut drained = 0;
        let mut messages: Vec<InputMessage> = Vec::new();
        while drained < max_drain {
            match input_rx.try_recv() {
                Ok(msg) => {
                    messages.push(msg);
                    drained += 1;
                }
                Err(_) => break,
            }
        }

        if messages.is_empty() && !left_burst {
            // Nothing to do — exponential backoff
            current_sleep = (current_sleep * 2).min(max_sleep);
            flush_cycle += 1;
            continue;
        }
        current_sleep = min_sleep;

        // === Step 3: Route each message ===
        let use_accumulator = pressure.level as u8 >= PressureLevel::Hot as u8;
        let mut upsert_ops: Vec<MutationOp> = Vec::new();
        let mut delete_ops: Vec<MutationOp> = Vec::new();

        for msg in messages {
            match msg {
                InputMessage::Put { slot, doc } => {
                    let is_alive = staging.slots.is_alive(slot);
                    let was_allocated = if !is_alive {
                        staging.slots.was_ever_allocated(slot)
                    } else {
                        false
                    };

                    if !is_alive && !was_allocated && use_accumulator {
                        // FRESH INSERT + HIGH PRESSURE → BulkAccumulator
                        accumulator.accumulate(slot, &doc, &config, &field_registry);
                    } else {
                        // UPSERT or FRESH INSERT at low pressure → diff_document path
                        let old_doc = if is_alive || was_allocated {
                            // Check write-through buffer first, then docstore
                            let buffered = doc_buffer.lock().get(&slot).cloned();
                            if buffered.is_some() {
                                buffered
                            } else {
                                docstore.get(slot).ok().flatten()
                            }
                        } else {
                            None
                        };

                        let ops = diff_document(
                            slot,
                            old_doc.as_ref(),
                            &doc,
                            &config,
                            is_alive,
                            &field_registry,
                        );
                        upsert_ops.extend(ops);
                    }

                    // Enqueue doc for persistence (all paths)
                    doc_batch.push((slot, StoredDoc { fields: doc.fields }));
                }
                InputMessage::Delete { slot } => {
                    delete_ops.push(MutationOp::AliveRemove { slots: vec![slot] });
                }
                InputMessage::Patch { slot, patch } => {
                    let ops = diff_patch(slot, &patch, &config, &field_registry);
                    upsert_ops.extend(ops);
                }
            }
        }

        // === Step 4: Apply diff_document ops via WriteBatch ===
        let mut coalescer_had_changes = false;
        if !upsert_ops.is_empty() || !delete_ops.is_empty() {
            coalescer_batch.clear_and_ingest(upsert_ops.into_iter().chain(delete_ops.into_iter()));
            coalescer_batch.group_and_sort();
            // Extract Tier 2 before apply
            let tier2_mutations = coalescer_batch.take_tier2_mutations(&tier2_fields);
            coalescer_batch.apply(&mut staging.slots, &mut staging.filters, &mut staging.sorts);
            coalescer_had_changes = true;
            staging_dirty = true;

            // Route Tier 2 + cache invalidation (same as current)
            // ... (existing logic, omitted for brevity)
        }

        // === Step 5: Flush BulkAccumulator if threshold reached ===
        let accum_threshold = pressure.accumulator_flush_threshold();
        let mut bulk_had_changes = false;
        if accumulator.doc_count() >= accum_threshold
            || (accumulator.doc_count() > 0
                && pressure.level as u8 < PressureLevel::Hot as u8)
        {
            // Pressure dropped below Hot — force flush remaining accumulator
            let result = accumulator.flush();
            result.apply(&mut staging.slots, &mut staging.filters, &mut staging.sorts);
            bulk_had_changes = true;
            staging_dirty = true;
        }

        // === Step 6: Deferred alive activation ===
        if coalescer_had_changes || bulk_had_changes {
            let now_unix = SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
            let activated = staging.slots.activate_due(now_unix);
            if !activated.is_empty() {
                staging.slots.merge_alive();
            }
        }

        // === Step 7: Maintenance (pressure-gated) ===
        if (coalescer_had_changes || bulk_had_changes) && !left_burst {
            if pressure.should_maintain_bounds() {
                // D3/E3: bound cache live maintenance (same as current)
                // ...
            }
            if pressure.should_invalidate_cache() {
                // Targeted cache invalidation (same as current)
                // ...
            }
            if pressure.should_compact() {
                for (_name, field) in staging.filters.fields_mut() {
                    field.merge_dirty();
                }
            }

            // D3: bound invalidation on filter changes
            // ...
        }

        // === Step 8: De-escalation from Burst → force publish ===
        if left_burst && staging_dirty {
            // Flush remaining accumulator
            if accumulator.doc_count() > 0 {
                let result = accumulator.flush();
                result.apply(&mut staging.slots, &mut staging.filters, &mut staging.sorts);
            }
            // Full compaction
            for (_name, field) in staging.filters.fields_mut() {
                field.merge_dirty();
            }
            // Invalidate ALL caches
            {
                let mut c = cache.lock();
                for name in &filter_field_names {
                    c.invalidate_field(name);
                }
            }
            if let Some(ref t2c) = tier2_cache {
                for field_name in &tier2_fields {
                    let arc: Arc<str> = Arc::from(field_name.as_str());
                    t2c.invalidate_field(&arc);
                }
            }
            // Mark all bounds for rebuild
            {
                let mut bc = bound_cache.lock();
                for (_, entry) in bc.iter_mut() {
                    entry.mark_for_rebuild();
                }
            }
            // Force publish
            inner.store(Arc::new(staging.clone()));
            staging_dirty = false;
        }

        // === Step 9: Conditional snapshot publish ===
        if staging_dirty && pressure.should_publish() {
            inner.store(Arc::new(staging.clone()));
            staging_dirty = false;
        }

        // === Step 10: Send doc batch to persist thread ===
        let doc_send_interval = match pressure.level {
            PressureLevel::Steady => 1,
            PressureLevel::Warm => 4,
            PressureLevel::Hot => 16,
            PressureLevel::Burst => 100,
        };
        if !doc_batch.is_empty() && (flush_cycle % doc_send_interval == 0 || left_burst) {
            // Write-through buffer: store docs for immediate upsert reads
            {
                let mut buf = doc_buffer.lock();
                for (id, doc) in &doc_batch {
                    buf.insert(*id, doc.clone());
                }
            }
            let batch_to_send = std::mem::take(&mut doc_batch);
            let _ = persist_tx.try_send(batch_to_send);
        }

        flush_cycle += 1;
    }

    // === Shutdown: final drain + publish ===
    // (same logic as left_burst, but drain channel completely)
}
```

---

## 4. Fresh vs Upsert Routing IN the Flush Thread

### Why This Works

The flush thread owns `staging` — the authoritative mutable copy of all bitmap state. It can check `staging.slots.is_alive(slot)` directly with zero synchronization cost. This is the key architectural win: the alive check, which currently requires an ArcSwap load per document, becomes a direct bitmap lookup on data the flush thread already has in L1 cache.

### Routing Logic

```
For each InputMessage::Put { slot, doc }:

1. is_alive = staging.slots.is_alive(slot)
2. was_allocated = !is_alive && staging.slots.was_ever_allocated(slot)

3. If !is_alive && !was_allocated (TRULY FRESH):
   - If pressure >= Hot: accumulator.accumulate(slot, &doc, &config, &registry)
   - If pressure < Hot: diff_document(slot, None, &doc, &config, false, &registry) → WriteBatch

4. If is_alive (UPSERT):
   - Read old doc: doc_buffer.get(slot) || docstore.get(slot)
   - diff_document(slot, old_doc, &doc, &config, true, &registry) → WriteBatch

5. If !is_alive && was_allocated (RE-INSERT of dead slot):
   - Read old doc: doc_buffer.get(slot) || docstore.get(slot)
   - diff_document(slot, old_doc, &doc, &config, false, &registry) → WriteBatch
   (This clears stale bits from the old doc before setting new ones)
```

### Old Doc Lookup for Upserts

The flush thread has access to both `doc_buffer` (write-through) and `docstore` (persistent). The lookup order is:

1. **doc_buffer first** (in-memory HashMap): The buffer holds docs that the persist thread hasn't written to redb yet. This is always the most recent version.
2. **docstore second** (redb): If not in buffer, read from disk. redb random reads are ~1-5us on NVMe.

**Correctness concern: can the flush thread read the docstore while the persist thread writes?**

Yes. redb supports concurrent readers with a single writer. The persist thread holds a write transaction; the flush thread opens a read transaction. redb uses MVCC, so the read sees a consistent snapshot even if the write is in-flight. However, the read might see a STALE version if the persist thread hasn't committed yet. The write-through buffer handles this case — if a doc was recently written by the flush thread but not yet persisted, it's in the buffer.

**Ordering guarantee:** The flush thread processes messages in channel order (FIFO). If slot 42 is written twice (put A, then put B), the flush thread sees put A first, populates the buffer with A's doc, then sees put B, reads A from the buffer for diffing, and replaces the buffer entry with B's doc. This is correct.

**One subtle issue:** The current code has separate `doc_tx` / `doc_rx` for doc persistence. In the new design, the flush thread itself populates `doc_batch` from the message payload — no separate channel needed for docs. The flush thread owns the `Document` data (it was moved through the input channel), so it can construct `StoredDoc` directly.

---

## 5. In-Flight Tracking

### Current Design

The `InFlightTracker` (DashSet-based) serves one purpose: post-validation. When a query returns results, it checks if any result slots are currently being mutated by a concurrent `put()`. If so, it revalidates those slots against the query filters using the snapshot's bitmap state.

### Analysis: Is In-Flight Tracking Still Needed?

With the new design, `put()` is just a channel send. The mutation isn't "in flight" in the same way — the document sits in the channel until the flush thread processes it. The flush thread applies mutations to staging and then publishes. Readers see the published snapshot.

**Scenario analysis:**

1. **Writer sends put(42, doc_A). Flush thread hasn't processed it yet. Reader queries.**
   - Reader sees the old snapshot. Slot 42 has old values (or isn't alive). Result is consistent with the snapshot. No problem.

2. **Flush thread processes put(42, doc_A), applies to staging, but hasn't published yet. Reader queries.**
   - Reader sees the last published snapshot (pre-mutation). Slot 42 has old values. Result is consistent. No problem.

3. **Flush thread publishes. Reader queries.**
   - Reader sees the new snapshot with slot 42's new values. Correct.

**In no scenario does a reader see a partially-mutated slot.** The ArcSwap::store() is atomic — readers see either the old snapshot or the new one, never a hybrid. This is the entire point of the snapshot architecture.

### Decision: Remove InFlightTracker

**The InFlightTracker was needed because the old design had writer threads mutating shared state (bulk_accum Mutex) concurrently with readers loading snapshots.** A writer could mark a slot alive in the snapshot (via flush thread) but not yet have its filter values applied (still in the MutationOp channel). A reader could see slot 42 as alive but with stale filter values, returning incorrect results.

In the new design, the flush thread is the only writer. It applies all mutations atomically to staging before publishing. Readers either see the pre-publish or post-publish state, both of which are consistent.

**Exception: There IS a window.** The bulk accumulator path at Hot/Burst pressure means the flush thread calls `accumulator.accumulate()` in one cycle and `accumulator.flush() + result.apply()` in a later cycle. If a snapshot is published between accumulate and flush, a reader could see:
- The slot's alive bit set (via the accumulator's apply), but
- The slot's filter values missing (not yet applied)

Wait — actually, the BulkAccumulator's `apply()` sets alive bits AND filter/sort bits in the same call. There's no partial application. The accumulator only applies when `flush()` is called, which returns a `BulkFlushResult` that `apply()` processes atomically.

But what about the snapshot publish timing? If we publish AFTER applying the BulkFlushResult, the snapshot is consistent. If we publish BEFORE (because should_publish() returned true on a cycle where the accumulator wasn't flushed yet), slots accumulated but not yet flushed are invisible (they're not in staging at all).

**Conclusion: No partial state is visible to readers. Remove InFlightTracker.**

This eliminates:
- `DashSet` allocation and contention (~150ns per put() for insert + remove)
- `find_overlapping()` scan on every query (~O(result_size * in_flight_size))
- `post_validate()` call on every query

### What Replaces It

Nothing. The atomic snapshot publish guarantees consistency.

---

## 6. Doc Persistence

### Pipeline

```
Flush thread processes InputMessage::Put { slot, doc }
  → Extracts doc.fields to build StoredDoc
  → Appends (slot, StoredDoc) to local doc_batch Vec
  → At doc_send_interval: sends doc_batch to persist thread
  → Also inserts into write-through doc_buffer for upsert correctness
```

### Pressure-Aware Batching

| Pressure Level | Doc Send Interval | Avg Batch Size | redb Transactions/s |
|---------------|-------------------|----------------|---------------------|
| Steady (0) | Every cycle (~100us) | ~10-50 docs | ~10,000/s |
| Warm (1) | Every 4th cycle (~400us) | ~40-200 docs | ~2,500/s |
| Hot (2) | Every 16th cycle (~1.6ms) | ~500-5000 docs | ~625/s |
| Burst (3) | Every 100th cycle (~10ms) | ~5000-50000 docs | ~100/s |

This progressively batches more docs per redb transaction under pressure, reducing the per-doc amortized write cost.

### Write-Through Buffer

The buffer is populated by the flush thread BEFORE sending the batch to the persist thread. This ensures that any subsequent upsert in the same flush cycle (or future cycles before persist commits) reads the most recent doc.

```rust
// In flush loop, when sending doc batch:
{
    let mut buf = doc_buffer.lock();
    for (id, doc) in &doc_batch {
        buf.insert(*id, doc.clone());
    }
}
let batch_to_send = std::mem::take(&mut doc_batch);
let _ = persist_tx.try_send(batch_to_send);
```

The persist thread removes entries after successful `put_batch()`:

```rust
// In persist thread:
while let Ok(batch) = persist_rx.recv() {
    let slot_ids: Vec<u32> = batch.iter().map(|(id, _)| *id).collect();
    docstore.put_batch(&batch)?;
    {
        let mut buf = doc_buffer.lock();
        for id in &slot_ids {
            buf.remove(id);
        }
    }
}
```

**Buffer size concern:** At Burst pressure with 300K docs/s and a persist thread that handles ~50K docs/s in batched transactions, the buffer could grow to hold 250K docs (~150 MB). This is a lot but manageable. The persist thread should keep up better than this since larger batches are more efficient for redb.

**Hard cap:** If the buffer exceeds `backpressure.write_through_cap` (default 100,000 entries), the flush thread blocks on `persist_tx.send()` instead of `try_send()`. This is a safety valve — it means the flush thread waits for the persist thread to drain a batch. Writers then block on the input channel (which fills because the flush thread is blocked). Full end-to-end backpressure.

### When to Populate the Buffer

The flush thread populates the buffer from the `doc.fields` in the `InputMessage::Put`. Since the flush thread now owns the Document (it was moved through the channel), it can construct `StoredDoc { fields: doc.fields }` directly — zero-copy field transfer from Document to StoredDoc.

Actually, there's a subtlety: the flush thread needs the `doc` both for:
1. Routing: `accumulator.accumulate(slot, &doc, ...)` or `diff_document(slot, ..., &doc, ...)`
2. Persistence: `StoredDoc { fields: doc.fields }`

Since (1) borrows `doc` and (2) consumes `doc.fields`, we need to do (1) first, then (2). But `accumulate()` takes `&Document` — it doesn't consume. Same for `diff_document()`. So:

```rust
// Route for bitmap mutations (borrows doc)
accumulator.accumulate(slot, &doc, &config, &field_registry);
// OR
let ops = diff_document(slot, old_doc.as_ref(), &doc, &config, is_upsert, &field_registry);

// Then consume for persistence (moves doc.fields)
doc_batch.push((slot, StoredDoc { fields: doc.fields }));
```

This avoids cloning `doc.fields` — it's moved directly into the StoredDoc.

---

## 7. Snapshot Publishing

### Steady State (Levels 0-1)

Publish every cycle (Level 0) or every 4th cycle (Level 1). Identical to current behavior except for the cadence at Level 1. Readers see data that's at most 400us stale at Level 1 — negligible for any production use case.

### Under Pressure (Level 2)

Publish every 16th cycle = every ~1.6ms. Readers see data up to 1.6ms stale. Still acceptable — no production query needs sub-2ms write-to-read latency.

**Diff growth between publishes:** Filter diffs accumulate for 16 cycles instead of 1. At 100 docs/cycle with 12 fields/doc, that's ~19,200 bitmap mutations between publishes. Each mutation is one set or clear in a VersionedBitmap diff. Most diffs hit the same (field, value) pairs, so the actual diff bitmap cardinality grows by ~1,600 slots (100 docs * 16 cycles).

This is fine — `apply_diff()` at read time is O(diff cardinality), and 1,600 is trivial. The periodic compaction (every 200 cycles at Level 2) merges diffs into bases, keeping them bounded.

### Burst Mode (Level 3)

No publishing. Readers see the last snapshot published before the system entered Burst. This is identical to the current `loading_mode` behavior.

**Diff growth:** Unbounded during Burst. But channel backpressure limits the input rate — at 100K capacity and 300K docs/s target, the flush thread drains the channel in ~0.3s. During that time, diffs grow by ~300K slots. Each VersionedBitmap's diff holds at most `alive_count` entries, so this is bounded by the data itself.

**On de-escalation from Burst:** Full compaction (merge all dirty diffs) + invalidate all caches + publish. Same as current `exit_loading_mode()`.

### Merge Thread Under Pressure

The merge thread currently runs on a fixed interval (default 5s). Under pressure, diffs grow faster, so the merge thread should compact more frequently to prevent stale bases.

**Signal mechanism:** The flush thread writes the current pressure level to a shared `AtomicU8`. The merge thread reads this and adjusts its sleep:

```rust
let merge_sleep = match pressure_level.load(Ordering::Relaxed) {
    0 | 1 => Duration::from_millis(config.merge_interval_ms),  // default 5000ms
    2 => Duration::from_millis(config.merge_interval_ms / 2),   // 2500ms
    _ => Duration::from_millis(config.merge_interval_ms / 4),   // 1250ms
};
```

This is a simple, low-overhead way to make the merge thread more responsive under pressure without any channel or explicit signaling.

---

## 8. Loading Mode Removal

### What Loading Mode Currently Does

From `concurrent_engine.rs`:

1. `enter_loading_mode()` sets `loading_mode: Arc<AtomicBool>` to true.
2. The flush thread checks `flush_loading_mode.load(Ordering::Relaxed)` each cycle.
3. When true: skips ALL maintenance and snapshot publishing after applying mutations.
4. `exit_loading_mode()` sets flag to false.
5. The flush thread detects the transition (`was_loading && !is_loading`) and:
   - Flushes remaining bulk accumulator docs
   - Compacts all filter diffs
   - Invalidates all caches (trie, Tier 2)
   - Force-publishes snapshot

### How Backpressure Replaces It

**Equivalence:** Burst mode (Level 3) is functionally identical to loading mode:
- Skips snapshot publishing
- Skips cache invalidation
- Skips bound maintenance
- Skips compaction

**Automatic activation:** During a 104M record bulk load, the input channel fills to capacity within milliseconds. The pressure state immediately escalates to Burst. No manual `enter_loading_mode()` call needed.

**Automatic deactivation:** When the bulk load finishes, writers stop sending. The flush thread drains the channel. Channel depth drops below `high_watermark`. De-escalation triggers force-publish/compact/invalidate — identical to `exit_loading_mode()`. No manual call needed.

### What About rebuild_from_docstore()?

Current code:
```rust
pub fn rebuild_from_docstore(config, path, progress_fn) -> Result<Self> {
    let engine = Self::build(config, docstore)?;
    engine.enter_loading_mode();
    // ... iterate all docs, calling engine.put() for each
    engine.exit_loading_mode();
    // ... poll until alive_count stabilizes
    Ok(engine)
}
```

New code:
```rust
pub fn rebuild_from_docstore(config, path, progress_fn) -> Result<Self> {
    let engine = Self::build(config, docstore)?;
    // Just push docs through the channel. Backpressure auto-activates Burst mode.
    read_store.for_each(|slot_id, stored_doc| {
        let doc = Document { fields: stored_doc.fields };
        engine.put(slot_id, doc).unwrap();
        // ...
    })?;
    // Wait for flush to drain and publish
    engine.wait_for_drain(timeout)?;
    Ok(engine)
}
```

**`wait_for_drain()`:** A new helper that polls until the input channel is empty AND alive_count stabilizes. This replaces the current `exit_loading_mode()` + stabilization polling:

```rust
fn wait_for_drain(&self, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if self.input_rx_len() == 0 {
            // Channel empty — give flush thread time to process final batch + publish
            thread::sleep(Duration::from_millis(100));
            if self.input_rx_len() == 0 {
                return Ok(());
            }
        }
        if Instant::now() > deadline {
            return Err(BitdexError::Timeout("wait_for_drain timed out".into()));
        }
        thread::sleep(Duration::from_millis(50));
    }
}
```

### Hint for Callers

For callers who know they're about to do a bulk load and want immediate Burst behavior (no 0.3s ramp-up while channel fills):

```rust
/// Hint that a bulk load is about to start. Pre-fills the channel with
/// no-op messages to trigger immediate Burst pressure escalation.
/// This is advisory — the system works correctly without it.
pub fn hint_bulk_load(&self) {
    // Not needed in practice. At 300K docs/s, the channel fills in <0.5s.
    // The ramp-up cost is negligible relative to a 104M record load.
}
```

Actually, we probably don't even need this. At 300K docs/s with 100K capacity, the channel fills in ~0.3 seconds. The system spends <0.3s at Steady/Warm before escalating to Hot/Burst. Over a 5-minute bulk load, this is insignificant.

**Decision: No hint_bulk_load(). The auto-escalation is fast enough.**

---

## 9. Config Schema

```rust
/// Backpressure configuration for the input channel and flush thread.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackpressureConfig {
    /// Input channel capacity (number of InputMessage slots).
    /// Memory: ~1.2 KB per slot. Default 100,000 = ~120 MB.
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,  // default: 100_000

    /// Channel depth below which the system is in Steady state.
    /// Default: 1,000
    #[serde(default = "default_low_watermark")]
    pub low_watermark: usize,  // default: 1_000

    /// Channel depth above which the system enters Burst mode.
    /// Hot mode is between high_watermark/2 and high_watermark.
    /// Default: 50,000
    #[serde(default = "default_high_watermark")]
    pub high_watermark: usize,  // default: 50_000

    /// Number of flush cycles below target level before de-escalating.
    /// Prevents oscillation at threshold boundaries.
    /// Default: 5
    #[serde(default = "default_deescalation_delay")]
    pub deescalation_delay_cycles: u64,  // default: 5

    /// Maximum entries in the write-through doc buffer before
    /// blocking on persist thread. Safety valve.
    /// Default: 100,000
    #[serde(default = "default_write_through_cap")]
    pub write_through_cap: usize,  // default: 100_000

    /// Persist thread queue depth (number of doc batch slots).
    /// Default: 64 (same as current)
    #[serde(default = "default_persist_queue_depth")]
    pub persist_queue_depth: usize,  // default: 64
}
```

YAML representation:

```yaml
backpressure:
  channel_capacity: 100000
  low_watermark: 1000
  high_watermark: 50000
  deescalation_delay_cycles: 5
  write_through_cap: 100000
  persist_queue_depth: 64
```

**Relationship to existing config fields:**

| Current Field | Disposition |
|--------------|------------|
| `channel_capacity` | Replaced by `backpressure.channel_capacity` |
| `flush_interval_us` | Keep (controls flush thread sleep, orthogonal to backpressure) |
| `merge_interval_ms` | Keep (merge thread, adjusted by pressure signal) |

---

## 10. Migration Plan

### What We Keep

| Component | Status |
|-----------|--------|
| `BulkAccumulator` (src/bulk_accumulator.rs) | Keep entirely. Flush thread owns it directly (no Mutex). |
| `WriteBatch` (src/write_coalescer.rs) | Keep for diff_document ops. Remove WriteCoalescer wrapper. |
| `diff_document()`, `diff_patch()` (src/mutation.rs) | Keep entirely. Called by flush thread. |
| `DocStore` (src/docstore.rs) | Keep entirely. |
| `InnerEngine`, `ArcSwap<InnerEngine>` | Keep entirely. |
| Persist thread | Keep (already exists in concurrent_engine.rs). |
| Merge thread | Keep. Add pressure-aware sleep. |
| `PressureState` concept | New, from docs/brainstorm/channel-centric-backpressure.md |
| `TrieCache`, `BoundCacheManager`, `PendingBuffer` | Keep entirely. |

### What Changes

| Component | Change |
|-----------|--------|
| `ConcurrentEngine::put()` | Rewrite: channel send only |
| `ConcurrentEngine::delete()` | Rewrite: channel send only |
| `ConcurrentEngine::patch()` | Rewrite: channel send only |
| `ConcurrentEngine::put_many()` | Rewrite: loop of channel sends |
| `ConcurrentEngine` struct fields | Remove: `sender: MutationSender`, `doc_tx`, `in_flight`, `loading_mode`, `bulk_accum` (Mutex-wrapped). Add: `input_tx: Sender<InputMessage>` |
| Flush thread | Major rewrite: drain input channel, route messages, own BulkAccumulator, own WriteBatch, pressure state machine |
| `InFlightTracker` | Remove entirely (src/concurrency.rs kept for Guard type only) |
| `WriteCoalescer` | Remove. `WriteBatch` is used directly by flush thread. |
| `MutationSender` | Remove. Replaced by `Sender<InputMessage>`. |
| `loading_mode` | Remove. `enter_loading_mode()`, `exit_loading_mode()` removed. |
| `rebuild_from_docstore()` | Simplify: remove loading mode calls, use wait_for_drain() |
| Config | Add `BackpressureConfig`. Remove `channel_capacity` (moved). |
| Benchmark harness | Remove loading mode calls. |
| `post_validate()` | Remove (no in-flight tracking). |

### Order of Changes (Tests Pass at Each Step)

**Step 1: Add PressureState and BackpressureConfig** (non-breaking)
- New file: `src/pressure.rs` with `PressureLevel`, `PressureState`
- Add `BackpressureConfig` to `src/config.rs` with defaults
- Add `backpressure` field to `Config` with `#[serde(default)]`
- All existing tests pass (unused code).

**Step 2: Add InputMessage enum** (non-breaking)
- Define `InputMessage` in a new module or in `src/write_coalescer.rs`
- All existing tests pass (unused code).

**Step 3: Refactor flush thread to use PressureState** (behavior-preserving)
- Replace `was_loading` / `staging_dirty` / `is_loading` logic with PressureState
- Keep the same two-channel drain (MutationOp + doc) for now
- PressureState starts at Steady and stays there (no channel to measure yet)
- Loading mode still works (PressureState defers to it)
- All existing tests pass (same behavior).

**Step 4: Add input channel alongside existing channels** (dual path)
- Add `input_tx: Sender<InputMessage>` to ConcurrentEngine
- Add `input_rx` to flush thread
- Flush thread drains BOTH input channel (new) and MutationOp channel (old)
- Add `put_v2(&self, id: u32, doc: Document)` that sends through input channel
- Keep old `put()` working alongside
- Test: verify `put_v2()` produces same results as `put()`

**Step 5: Migrate flush thread to own routing logic** (the big change)
- Flush thread processes `InputMessage::Put` by doing alive check + routing
- Flush thread owns a BulkAccumulator directly (not behind Mutex)
- Remove Mutex-wrapped `bulk_accum` from ConcurrentEngine
- Old `put()` still works but `put_v2()` uses new path
- Test: 1000-doc insert test via both paths, verify identical results

**Step 6: Remove InFlightTracker** (simplification)
- Remove `in_flight` from ConcurrentEngine
- Remove `post_validate()` from query path
- Remove `mark_in_flight()` / `clear_in_flight()` from old `put()`
- All query tests should pass (no behavioral change — in-flight was a safety net, not a correctness requirement under snapshot architecture)
- Keep `src/concurrency.rs` file for the Guard type (if used elsewhere), or remove if unused

**Step 7: Replace old put() with new put()** (API change)
- Change `put(&self, id: u32, doc: &Document)` to `put(&self, id: u32, doc: Document)`
- Update all callers (benchmark, tests, rebuild_from_docstore)
- Remove old MutationOp channel (`sender: MutationSender`, `WriteCoalescer`)
- Remove old doc channel (`doc_tx`)
- Flush thread now only drains the single input channel
- All tests updated and passing

**Step 8: Remove loading mode** (cleanup)
- Remove `loading_mode: Arc<AtomicBool>`
- Remove `enter_loading_mode()`, `exit_loading_mode()`
- Update `rebuild_from_docstore()` to use `wait_for_drain()`
- Update benchmark harness
- All tests pass

**Step 9: Wire up pressure-driven behavior** (the payoff)
- PressureState reads `input_rx.len()` each cycle
- Accumulator activation at Hot+
- Publish/compact/maintain cadence driven by pressure
- De-escalation from Burst triggers force-publish
- Merge thread reads shared pressure AtomicU8
- Benchmark: verify 300K docs/s at 1M scale

### Risk Areas

1. **The put() signature change (Step 7)** touches every caller. The benchmark harness, all tests in concurrent_engine.rs, and rebuild_from_docstore all call put(). This is a mechanical change but error-prone. Mitigation: grep for all `\.put(` calls and update systematically.

2. **Removing InFlightTracker (Step 6)** removes a safety net. If there's a correctness scenario we haven't considered where partial state is visible to readers, removing the tracker could introduce bugs. Mitigation: the snapshot architecture makes this impossible (ArcSwap::store is atomic), but we should add a regression test: concurrent put + query with immediate filter check on the inserted slot.

3. **Flush thread complexity** increases significantly — it now handles routing, accumulation, doc batching, pressure state, AND all the existing maintenance. The flush loop is already ~300 lines; this could push it to ~500. Mitigation: extract routing logic into a separate `FlushRouter` struct that the flush loop calls.

4. **Docstore read contention.** The flush thread reads the docstore for upsert diffing. The persist thread writes to it. redb handles this via MVCC. But if there's a bug in redb's concurrent access on Windows (the STATUS_HEAP_CORRUPTION issue noted in MEMORY.md), this could surface. Mitigation: the current code already has this pattern (writers read docstore in put()), so this isn't a new risk.

5. **Accumulator flush timing at Hot/Burst boundary.** If pressure oscillates between Warm and Hot, the accumulator alternately activates and deactivates. Docs accumulated during Hot get flushed when dropping to Warm (the "pressure dropped below Hot" check in Step 5 of the pseudocode). This is correct but could produce many small accumulator flushes. Mitigation: the 5-cycle de-escalation delay prevents rapid oscillation.

6. **Doc batch memory.** At Burst with doc_send_interval=100, the flush thread accumulates ~100 cycles * ~300 docs/cycle = 30,000 docs in doc_batch (~36 MB). This is a new memory peak that didn't exist before (current code sends docs every cycle). Mitigation: the persist_queue_depth and write_through_cap provide hard limits.

---

## Appendix: Thread Interaction Diagram

```
Writer Thread 1 ──┐
Writer Thread 2 ──┤──► input_tx ──► [bounded channel, 100K] ──► input_rx
Writer Thread N ──┘
                                                                    │
                                                              Flush Thread
                                                                    │
                                  ┌─────────────────────────────────┤
                                  │                                 │
                                  ▼                                 ▼
                         BulkAccumulator              WriteBatch (from diff_document)
                         (owned, no Mutex)            (owned, no Mutex)
                                  │                                 │
                                  └────────────┬────────────────────┘
                                               │
                                          staging: InnerEngine (owned)
                                               │
                               ┌───────────────┼───────────────┐
                               │               │               │
                               ▼               ▼               ▼
                        ArcSwap::store()  persist_tx ──►  Merge Thread
                        (snapshot publish) [bounded, 64]  (reads AtomicU8
                               │               │          pressure level)
                               ▼               ▼               │
                          Query Readers   Persist Thread       ▼
                                               │          BitmapStore
                                               ▼          (redb)
                                          DocStore
                                          (redb)
```

**Key property: The flush thread has NO shared mutable state.** It owns staging, BulkAccumulator, and WriteBatch outright. The only synchronization points are:
- `input_rx.try_recv()` — lock-free (crossbeam MPMC)
- `doc_buffer.lock()` — brief lock to populate write-through buffer (~1us)
- `cache.lock()` — brief lock for invalidation (~1us)
- `bound_cache.lock()` — brief lock for maintenance (~1us)
- `pending.lock()` — brief lock for Tier 2 routing (~1us)
- `persist_tx.try_send()` — lock-free (crossbeam MPMC)
- `inner.store()` — atomic pointer swap (~10ns)
- `docstore.get()` — redb read transaction for upsert diffing (~1-5us)

All locks are `parking_lot::Mutex` with sub-microsecond hold times. The flush thread's hot path (drain + route + apply) touches no locks.
