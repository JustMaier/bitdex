# Hybrid Flush Architecture: Decoupling Drain+Apply from Publish+Persist

## Problem Statement

The current flush thread does everything sequentially in a single loop:

```
while !shutdown {
    sleep(current_sleep);                      // 100us default
    bitmap_count = coalescer.prepare();         // Drain + group/sort
    coalescer.apply_prepared(&mut staging...);  // Apply to staging InnerEngine
    // ... bound cache maintenance, cache invalidation ...
    inner.store(Arc::new(staging.clone()));     // Publish snapshot (ArcSwap)
    docstore.put_batch(&doc_batch);             // Write docs to redb
}
```

**The redb docstore write is the critical bottleneck.** At 104M records, the dataset produces 137 GB of on-disk data. Each `put_batch` call blocks the flush thread while redb serializes, compacts, and fsyncs. During this window:

- The crossbeam mutation channel fills up (100K capacity)
- Writer threads block on `sender.send()` (bounded channel backpressure)
- No mutations are drained, grouped, or applied
- No snapshots are published (readers see stale data)
- Effective throughput: **62K docs/s at 1M records**, far below the 300K target

The flush thread is a pipeline stall: one slow stage (redb persist) blocks all earlier stages (drain, apply, publish).

## Design: Three-Stage Pipeline

Decouple the flush thread into three independent stages connected by handoff queues:

```
Stage 1: DRAIN + APPLY          Stage 2: PUBLISH           Stage 3: PERSIST
(flush thread, tight loop)       (same thread or inline)    (dedicated persist thread)

 crossbeam channel               staging InnerEngine        redb docstore
 ┌──────────┐                    ┌──────────┐              ┌──────────┐
 │ MutationOp│──drain+group──►  │ apply to  │──clone──►   │ put_batch│
 │ channel   │                   │ staging   │  snapshot   │ (redb)   │
 └──────────┘                    └──────────┘              └──────────┘
                                      │                         ▲
                                      │ ArcSwap::store()        │
                                      ▼                         │
                                 ┌──────────┐              ┌──────────┐
                                 │ readers   │              │ doc_batch│
                                 │ see new   │              │ queue    │
                                 │ snapshot  │              │(crossbeam│
                                 └──────────┘              └──────────┘
```

### Stage 1: Drain + Apply (flush thread, hot loop)

**What it does:**
1. `coalescer.prepare()` — drain crossbeam channel, group and sort ops
2. Extract Tier 2 mutations
3. `coalescer.apply_prepared()` — apply to private staging `InnerEngine`
4. Activate deferred alive slots
5. Bound cache live maintenance (D3/E3)
6. Tier 2 / PendingBuffer routing
7. Cache invalidation (targeted, sort-only skip)
8. Periodic filter diff compaction
9. **Publish snapshot** via `ArcSwap::store(Arc::new(staging.clone()))`
10. **Enqueue doc batch** to persist thread's queue (non-blocking)

**Why publish is still in Stage 1:** Publishing is cheap (~microseconds for the ArcSwap store + the staging clone is O(num_fields) Arc refcount bumps). Moving it to a separate thread would add latency for no gain. The expensive operation is redb, not ArcSwap.

**Loop timing:** Adaptive sleep, same as today (100us min, 1ms max). But now the loop never blocks on redb — it just enqueues the doc batch and moves on.

### Stage 2: Publish (inline with Stage 1)

Not a separate thread. The `ArcSwap::store()` call is already fast enough (~nanoseconds) to stay inline. Separating it would add synchronization overhead without benefit.

The publish happens immediately after apply, ensuring readers see fresh data with minimal latency. This is unchanged from the current architecture.

### Stage 3: Persist (dedicated persist thread)

**What it does:**
1. Drain doc batch queue (non-blocking `try_recv` loop)
2. Call `docstore.put_batch(&batch)` on the accumulated batch
3. Sleep adaptively (fast when work is available, slow when idle)

**Key property:** The persist thread runs independently. It can fall behind the flush thread without blocking mutation draining or snapshot publishing. Documents in the queue are "in flight" — their bitmap mutations are already applied and visible to readers, but the on-disk docstore hasn't persisted them yet.

## Thread Architecture

### Current (1 flush thread + 1 merge thread)

```
Writer threads ──► [crossbeam channel] ──► Flush Thread ──► ArcSwap
                   [doc channel]       ──►    (serial)  ──► redb docstore
                                             Merge Thread ──► redb bitmaps
```

### Proposed (1 flush thread + 1 persist thread + 1 merge thread)

```
Writer threads ──► [mutation channel] ──► Flush Thread ──► ArcSwap
                   [doc channel]      ──►     │
                                              │ enqueue doc_batch
                                              ▼
                                        [persist queue] ──► Persist Thread ──► redb docstore
                                                            Merge Thread   ──► redb bitmaps
```

### Thread Responsibilities

| Thread | Owns | Does | Frequency |
|--------|------|------|-----------|
| **Flush** | staging `InnerEngine`, `WriteCoalescer` | Drain channel, group/sort, apply mutations, cache/bound maintenance, publish snapshot, enqueue docs to persist queue | 100us-1ms adaptive |
| **Persist** | `DocStore`, doc batch buffer | Drain persist queue, batch write to redb | Adaptive (fast when work, slow when idle) |
| **Merge** | Read-only snapshot access, `BitmapStore` | Compact filter diffs to redb, drain PendingBuffer | 5s interval |

## Double-Buffering the Doc Batch

### Handoff Mechanism

The flush thread and persist thread communicate via a **crossbeam bounded channel of doc batch `Vec`s** — not individual docs. This amortizes the per-message overhead:

```rust
// In flush thread:
let (persist_tx, persist_rx): (Sender<Vec<(u32, StoredDoc)>>, Receiver<Vec<(u32, StoredDoc)>>) =
    crossbeam_channel::bounded(4); // Small queue: 4 batches max

// At end of each flush cycle (after publish):
if !doc_batch.is_empty() {
    let batch = std::mem::take(&mut doc_batch); // Move, don't clone
    if persist_tx.try_send(batch).is_err() {
        // Persist thread falling behind — see backpressure section
    }
}
```

```rust
// In persist thread:
loop {
    match persist_rx.recv_timeout(Duration::from_millis(10)) {
        Ok(batch) => {
            docstore.put_batch(&batch).unwrap_or_else(|e| {
                eprintln!("persist thread: redb write failed: {e}");
            });
        }
        Err(RecvTimeoutError::Timeout) => continue,
        Err(RecvTimeoutError::Disconnected) => break,
    }
}
```

### Why Vec Batches, Not Individual Docs

- redb serializes write transactions internally — one big `put_batch` is far cheaper than many small `put` calls
- Sending `Vec<(u32, StoredDoc)>` through the channel moves the allocation, zero-copy
- The flush thread already accumulates `doc_batch` from the doc channel; just move it to the persist queue

### Queue Depth: 4 Batches

With the flush thread cycling at 100us-1ms and each batch containing hundreds to thousands of docs, a queue of 4 batches provides:
- ~4ms of buffering at maximum throughput
- Enough to absorb redb write variance (some writes are fast, some trigger compaction)
- Bounded memory: each batch is the docs from one flush cycle

## Adaptive Publish Frequency

Snapshot publishing (`ArcSwap::store(Arc::new(staging.clone()))`) is cheap but not free:
- `staging.clone()` is O(num_fields) Arc refcount bumps (~50 fields = ~50 atomic ops)
- Old snapshots are deallocated when last reader drops them

**Policy: Publish every flush cycle that has mutations.** This matches the current behavior. The cost is negligible compared to the drain+apply work.

For very high write rates, a future optimization could batch multiple drain+apply cycles before publishing (e.g., "publish at most every 500us or every 10K ops, whichever comes first"). But this is premature — the current per-cycle publish is fine.

## Merge Thread Under Load

The merge thread's role is unchanged: periodically snapshot the current state, compact filter diffs, and persist bitmaps to redb. However, the pipelined architecture has implications:

### Merge Frequency

- **Default:** 5s interval (unchanged)
- **Under heavy write load:** The merge thread sees more accumulated diffs per cycle, which makes each compaction heavier but more efficient (batching effect)
- **No change needed:** The merge thread reads snapshots via `ArcSwap::load()`, which is lock-free. It doesn't compete with the flush thread

### Interaction with Pipelining

The merge thread persists *bitmap state* to redb (via `BitmapStore`). The persist thread persists *documents* to redb (via `DocStore`). These are separate redb databases — no contention:

```
BitmapStore (redb #1): merge thread writes filter/sort/alive bitmaps
DocStore    (redb #2): persist thread writes document blobs
```

If they share a single redb database, the merge thread and persist thread would serialize on redb's write lock. **Recommendation: keep separate databases** (already the case today).

## VersionedBitmap Consistency Under Pipelining

### Invariant: Readers Always See Consistent Snapshots

The pipelined architecture preserves this invariant because **nothing changes about the publish mechanism**:

1. Flush thread applies mutations to private `staging` (no readers see this)
2. Flush thread calls `ArcSwap::store(Arc::new(staging.clone()))`
3. Readers load snapshots via `ArcSwap::load()` — they get a complete, consistent point-in-time view

The doc persist thread runs asynchronously, but it only writes to the on-disk docstore. Readers never see partial states because:
- Filter/sort bitmaps are in the ArcSwap snapshot (always consistent)
- The docstore is only read on the *write path* (for upsert diffing) and optionally for serving documents alongside results

### Edge Case: Crash Before Persist

If the process crashes after publishing a snapshot but before the persist thread writes all docs to redb:

- **Bitmap state:** Includes mutations for docs that aren't persisted yet
- **Docstore:** Missing some recently-written documents
- **On restart (rebuild_from_docstore):** Bitmaps are rebuilt from the docstore, so the "missing" docs are simply absent — the bitmaps are rebuilt consistently from what's persisted

This is **acceptable** because the current architecture has the same property: if the process crashes between `ArcSwap::store()` and `docstore.put_batch()`, the same inconsistency exists. The rebuild-from-docstore path handles this correctly.

### Edge Case: Upsert Reads Stale Docstore

When a writer calls `put(id, doc)`, it reads the old doc from the docstore for diffing:

```rust
let old_doc = if is_upsert || was_allocated {
    self.docstore.get(id)?  // Reads from redb
} else {
    None
};
```

With the persist thread running asynchronously, the docstore might not yet contain the most recent version of a document. This could cause an incorrect diff (computing ops against a stale version).

**Mitigation: Write-through doc buffer.** The flush thread maintains a `HashMap<u32, StoredDoc>` of recently-written docs that haven't been persisted yet. On upsert, check this buffer first:

```rust
// In put():
let old_doc = if is_upsert || was_allocated {
    // Check in-memory buffer first (not yet persisted)
    self.recent_docs.get(&id).cloned()
        .or_else(|| self.docstore.get(id).ok().flatten())
} else {
    None
};
```

The flush thread clears entries from this buffer once the persist thread confirms they've been written. Implementation options:
1. **Shared HashMap behind Mutex** — simple, brief lock
2. **Epoch-based clearing** — flush thread tags entries with a sequence number, persist thread reports highest persisted sequence

Option 1 is simpler and sufficient. The buffer holds at most `persist_queue_depth * avg_batch_size` entries (~4K-40K docs at peak). At ~500 bytes/doc, that's 2-20 MB — negligible.

## Pipeline Diagram

```
Time ──────────────────────────────────────────────────────────►

Flush Thread:
  ┌─drain─┬─apply─┬─maint─┬─pub─┬─enq─┐ ┌─drain─┬─apply─┬─maint─┬─pub─┬─enq─┐
  │  ch   │bitmap │cache  │snap │docs │ │  ch   │bitmap │cache  │snap │docs │
  └───────┴───────┴───────┴─────┴─────┘ └───────┴───────┴───────┴─────┴─────┘
  Cycle N                                Cycle N+1

Persist Thread:
                              ┌──────────put_batch──────────┐
                              │  (cycle N-2 docs)           │     ┌──────────put_batch──────────┐
                              └─────────────────────────────┘     │  (cycle N-1 docs)           │
                                                                  └─────────────────────────────┘

Readers:
  ──load()──snap_N-1──────────────load()──snap_N────────────────load()──snap_N+1──────
```

Key observation: **Flush cycles N and N+1 proceed while persist thread is still writing cycle N-2 docs.** The pipeline never stalls.

## Backpressure: What If Persist Falls Behind?

### Detection

The persist queue has bounded capacity (4 batches). When the flush thread calls `persist_tx.try_send(batch)`:
- **Success:** Normal operation, batch enqueued
- **Full queue:** Persist thread is behind

### Response to Backpressure

Three options, from least to most aggressive:

**Option A: Grow the buffer (recommended for V1)**

```rust
if persist_tx.try_send(batch).is_err() {
    // Persist thread can't keep up — accumulate in overflow buffer
    overflow_buffer.extend(batch);
    if overflow_buffer.len() > OVERFLOW_LIMIT {
        // Hard limit reached — block until persist thread catches up
        let combined = std::mem::take(&mut overflow_buffer);
        persist_tx.send(combined).unwrap(); // Blocks
    }
}
```

This absorbs burst writes (e.g., bulk import) while still providing a hard upper bound on memory. `OVERFLOW_LIMIT` could be 100K docs (~50 MB).

**Option B: Coalesce doc writes**

If the same slot is written multiple times while the persist thread is behind, only the latest version matters. The flush thread could maintain a `HashMap<u32, StoredDoc>` and only send the latest version when the persist thread is ready. This deduplicates redundant writes during burst upserts.

**Option C: Drop and re-derive**

If the docstore is purely for upsert diffing (not for serving), dropped doc writes are recoverable: on the next upsert of that slot, the diff will be computed against whatever version *is* in the docstore (possibly older). This produces a larger-than-necessary set of mutation ops but is still correct. This is the most aggressive option — only viable if the docstore is not serving documents to clients.

**Recommendation:** Option A for simplicity, with Option B as a follow-up optimization.

### Monitoring

Add a metric: `persist_queue_depth` (gauge). When it stays at max, the persist thread is the bottleneck. Solutions:
- Increase `persist_queue_depth` (more memory buffer)
- Use multiple persist threads with sharded writes (unlikely to help — redb serializes write transactions)
- Switch to a faster KV store (e.g., sled, fjall, or custom mmap)
- Skip docstore writes entirely in loading mode (already done)

## How This Eliminates redb as the Write-Path Bottleneck

### Current State

```
Flush cycle time = drain(50us) + apply(200us) + maintain(100us) + publish(50us) + redb(5-50ms)
                 = 5.4-50.4ms per cycle (dominated by redb)
```

At 1000 docs/batch and 50ms cycle time: **20K docs/s** (redb-limited).
At 1000 docs/batch and 5ms cycle time: **200K docs/s** (redb-limited on good days).
Actual measured: **62K docs/s** (somewhere in between).

### Proposed State

```
Flush cycle time = drain(50us) + apply(200us) + maintain(100us) + publish(50us) + enqueue(1us)
                 = 401us per cycle
```

At 1000 docs/batch and 401us cycle time: **~2.5M docs/s** (CPU-limited on apply).
Realistically, with larger batches and channel drain variance: **300K-500K docs/s** target achievable.

The persist thread runs at whatever speed redb allows, but it no longer gates the critical path. Readers see fresh bitmaps within microseconds of mutation, regardless of redb latency.

## Loading Mode Interaction

Loading mode already skips snapshot publishing to avoid the `staging.clone()` cascade. With the hybrid architecture:

1. **Enter loading mode:** Flush thread skips publish and cache maintenance (same as today). Also skips enqueuing docs to persist queue — there's no point writing 100M docs to redb during bulk load when we'll rebuild from the source data anyway.

2. **Exit loading mode:** Force-publish the staging state. Persist thread drains any queued batches. If loading mode skipped doc enqueuing, the docstore is populated separately (e.g., from the source NDJSON file).

3. **Alternative for loading mode:** Write docs to redb on the persist thread even during loading, but at lower priority (larger batches, less frequent commits). This keeps the docstore in sync for crash recovery without slowing the bitmap pipeline.

**Recommendation:** During loading mode, still enqueue docs to the persist thread. The persist thread doesn't affect bitmap throughput. Having the docstore populated means crash recovery via `rebuild_from_docstore` works even if loading is interrupted.

## Memory Cost Analysis

### New Allocations

| Component | Size | Notes |
|-----------|------|-------|
| Persist queue (4 batch slots) | ~2-20 MB | 4 x (1K-10K docs x ~500 bytes/doc) |
| Write-through doc buffer | ~2-20 MB | Same size as persist queue depth |
| Persist thread stack | ~8 MB | Default Rust thread stack |
| Overflow buffer (peak) | ~50 MB | Only during burst backpressure |

**Total additional memory: 12-100 MB** — negligible compared to the 14.5 GB RSS at 104M records.

### Saved Allocations

None — the same data flows through the system, just pipelined instead of serial.

## Implementation Plan

### Phase 1: Extract persist thread (minimal change)

1. Create a `PersistThread` struct that owns `DocStore` and the receive end of a new bounded crossbeam channel
2. Move the doc drain + `put_batch` logic from the flush loop into the persist thread
3. Flush thread sends `Vec<(u32, StoredDoc)>` batches to the persist channel via `try_send`
4. Fallback: if `try_send` fails (queue full), block with `send` (still an improvement because we've decoupled the fast path)
5. Update shutdown sequence: signal persist thread after flush thread, drain remaining docs

### Phase 2: Write-through doc buffer

1. Add `Arc<parking_lot::Mutex<HashMap<u32, StoredDoc>>>` shared between write path and persist thread
2. On `put()`: after sending ops, insert into write-through buffer
3. On `put()` upsert: check write-through buffer before docstore
4. Persist thread: after `put_batch` succeeds, remove persisted entries from buffer
5. Bounded by persist queue depth — no unbounded growth

### Phase 3: Backpressure refinement

1. Replace blocking `send` fallback with overflow buffer + hard limit
2. Add `persist_queue_depth` metric
3. Optional: coalesce duplicate slot writes in overflow buffer

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Crash loses unpersisted docs | Bitmap-docstore inconsistency on restart | `rebuild_from_docstore` handles this; same as current behavior |
| Upsert reads stale docstore | Incorrect diff, excess mutation ops | Write-through doc buffer (Phase 2) |
| Persist thread falls behind permanently | Unbounded memory growth | Bounded queue + overflow limit + monitoring |
| redb write transaction contention with merge thread | Persist thread and merge thread both write to redb | Already separate databases (DocStore vs BitmapStore) |
| Two threads writing to the same redb (if shared) | Serialized writes, no throughput gain | Keep DocStore and BitmapStore as separate redb instances |

## Alternative Considered: Multiple Persist Threads

redb serializes write transactions — only one writer can commit at a time. Multiple persist threads would just contend on the write lock. **Rejected.**

A sharding approach (persist thread 1 handles slots 0-N/2, persist thread 2 handles N/2-N, each with its own redb file) could work but adds significant complexity for modest gain. **Deferred to future work.**

## Alternative Considered: Async Persist (Tokio)

Using `tokio::spawn_blocking` for redb writes would save a dedicated thread but add the Tokio runtime as a dependency. Bitdex is a synchronous, thread-based architecture. **Rejected** to maintain simplicity.

## Summary

| Metric | Current | Proposed |
|--------|---------|----------|
| Flush cycle time | 5-50ms (redb-dominated) | ~400us (CPU-dominated) |
| Snapshot freshness | 5-50ms | ~400us |
| Write throughput ceiling | ~62K docs/s | ~300K-500K docs/s |
| Additional memory | 0 | ~12-100 MB |
| Thread count | 2 (flush + merge) | 3 (flush + persist + merge) |
| Crash recovery | Rebuild from docstore | Same (unchanged) |
| Code complexity | Single flush loop | Flush loop + persist thread + write-through buffer |

The hybrid architecture eliminates redb as the write-path bottleneck by moving document persistence to a dedicated thread that runs independently of the drain-apply-publish hot path. Readers see fresh data within microseconds of mutation regardless of disk I/O latency. The 5x throughput target (62K -> 300K docs/s) becomes achievable because the flush cycle is no longer gated by redb commit latency.
