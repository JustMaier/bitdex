# Bitdex V2 — CLAUDE.md

## What is Bitdex?

Bitdex is a purpose-built, in-memory bitmap index engine written in Rust. Its primary job is to take filter predicates and sort parameters and return an ordered list of integer IDs. Indexing is bitmaps all the way down.

**In:** Filter predicates + sort field + sort direction + limit
**Out:** Ordered `Vec<i64>` of IDs

Documents are stored on disk (via an embedded key-value store like redb) keyed by slot ID. This serves two purposes: (1) enabling efficient targeted bitmap updates on upsert by diffing old vs new field values, and (2) optionally serving document content alongside query results. Full-text search happens downstream.

---

## Inviolable Design Principles

These are non-negotiable. Any agent working on this project MUST follow these rules. Violating them is grounds for rejecting a PR.

1. **Bitmaps are the index.** All filtering and sorting is done via roaring bitmap operations. No Vecs for column storage. No skip lists. No sorted arrays. No forward maps. No reverse indexes as index structures.

2. **Documents are stored on disk.** An embedded key-value store (redb) stores documents keyed by slot ID. On upsert, the old document is read from disk, diffed against the new one, and only the changed bitmaps are updated. This makes writes O(changed fields) instead of O(all bitmaps). Documents can also be served alongside query results.

3. **No sorted data structures.** No sorted Vecs, no skip lists, no B-trees for maintaining sort order. Sorting is done via bit-layer bitmap traversal. Period.

4. **No in-memory forward maps or reverse indexes.** The on-disk document store replaces the need for these. On upsert, read old doc from disk to determine which bitmaps to update. For DELETE WHERE on high-cardinality fields, scan the bitmaps.

5. **Deletes only clear the alive bit.** No cleanup of other bitmaps on delete. Autovac handles that in the background.

6. **Slot = Postgres ID** for integer ID users. No mapping layer.

7. **Full precision sort layers first.** Do not implement log encoding or reduced bit depths until benchmarks prove it's necessary.

8. **JSON query parser only for V2.** OpenSearch and Meilisearch syntax plugins are future work.

9. **Single process, single node.** No clustering, no replication, no distributed consensus. A Postgres fallback in the API layer handles the (rare) downtime during restarts.

---

## Architecture Overview

### Slot Model

- Each document's Postgres ID IS the slot (its position in every bitmap)
- Slots are monotonically assigned via atomic counter on insert
- Deleted slots are NOT immediately recycled — the alive bitmap hides them
- An autovac process periodically produces a clean bitmap of recycled slots
- New inserts check the clean bitmap first (grab first set bit), append only if none available

### Document Store

- Embedded key-value store on disk (redb) keyed by slot ID
- Stores the full document fields (filter values, sort values, multi-value arrays)
- On PUT upsert: read old doc from disk, diff old vs new, update only changed bitmaps
- On fresh insert (slot not alive): write doc to disk, set bitmaps directly — no diff needed
- On DELETE: clear alive bit (doc stays on disk until autovac cleans it)
- NVMe random reads are microseconds — disk lookup adds negligible latency to writes
- Documents can optionally be returned alongside query result IDs

### Bitmap Categories

1. **Alive Bitmap** — One bitmap tracking all active documents. ANDed into every query implicitly. Delete = clear one bit here.
2. **Filter Bitmaps** — One roaring bitmap per distinct value per filterable field. Boolean fields: one bitmap per boolean. Multi-value fields: one bitmap per distinct value.
3. **Sort Layer Bitmaps** — Each sortable numeric field decomposed into N bitmaps (one per bit position). A u32 field = 32 bitmaps. Top-N retrieval via MSB-to-LSB traversal using AND operations.

### Concurrency Model — No Locks

- Writers atomically mark their target slot ID in an in-flight set BEFORE mutating bitmaps
- Writers clear the in-flight mark AFTER mutation is complete
- Readers execute their full query without coordination
- After computing results, readers check if any result IDs overlap with the in-flight set
- If overlap: revalidate only the overlapping IDs (re-check their filter/sort bits)

### Trie Cache

- Keyed by canonically sorted filter clauses (sorted by field name, then value)
- Supports prefix matching for partial cache hits
- Automatic promotion/demotion based on exponential-decay hit stats
- Lazy invalidation via generation counters per filter field
- Sort fields (reactionCount, etc.) are NOT part of cache keys — sort is applied after cache lookup

---

## Reference Materials

- **Full Project Brief & Development Guide**: `docs/in/prepared-prompt.md` — Contains complete architecture, API specs, config schemas, testing strategy, development phases, and team structure
- **Design Conversation**: `docs/in/Claude-Bitdex.md` — Full brainstorming conversation showing the evolution from OpenSearch to the bitmap-only architecture. Read this to understand WHY decisions were made.
- **V1 Codebase**: `C:\Dev\Repos\open-source\bitdex\` — Reference for reusable code (filter bitmaps, WAL consumer, server scaffolding). DO NOT bring over Vecs, skip lists, sorted arrays, forward maps, or reverse indexes.

---

## Development Phases

### Phase 1: Core Engine — COMPLETE (commit 7bc60fd)
Slot allocation, alive bitmap, filter bitmaps, sort layer bitmaps, mutation API (PUT/PATCH/DELETE/DELETE WHERE), query execution, JSON query parser, config loading. Full test coverage.

### Phase 2: Persistence — PARTIAL
On-disk document store via redb (commit 8e3c54a). Stores documents keyed by slot ID for upsert diffing. WAL, snapshot serialization, and sidecar snapshot builder are NOT yet implemented.

### Phase 3: Performance — COMPLETE (commits 95df2a5 through 1acfad7)
- Cardinality-based query planning (planner.rs)
- Trie cache with prefix matching and generation-counter invalidation (cache.rs)
- Concurrent engine with RwLock-based read/write separation (concurrent_engine.rs)
- Write coalescing via crossbeam channels with batched flush loop (write_coalescer.rs)
- Arc<str> field name interning for zero-copy mutation ops
- Benchmark harness with 20 query types, memory reporting, multi-stage benchmarking

### Phase 4: Operations
Prometheus metrics, autovac, admin API, graceful shutdown, health check. NOT yet started.

### Phase 5: Integration
Postgres WAL consumer, backfill pipeline, shadow mode, end-to-end tests. NOT yet started.

---

## Coding Standards

- **Language**: Rust
- **Bitmap Library**: `roaring-rs` (roaring bitmaps)
- **Every PR must include tests** for the code it adds
- **Property-based tests** using `proptest` or `quickcheck` for bitmap operations
- **Fuzz the JSON query parser** with arbitrary input — nothing should panic or corrupt state
- **Benchmark suite** must run on every PR — any PR that degrades benchmarks by >10% gets flagged
- Correctness first, performance second
- When in doubt, refer to `docs/in/prepared-prompt.md` for the authoritative specification

---

## Measured Memory (Civitai dataset, remapped IDs, 4 threads)

| Scale | Bitmap Memory | RSS | Worst Query p50 |
|------:|-------------:|----:|----------------:|
| 5M | 328 MB | 1.20 GB | 0.83ms |
| 50M | 2.95 GB | 6.09 GB | 13.5ms |
| 100M | 6.19 GB | 11.66 GB | 18.7ms |
| 104.6M | 6.49 GB | 12.14 GB | 21.1ms |

tagIds dominates filter memory at 79-80% across all scales.
Full results: `docs/benchmark-report.md`

### Extrapolation to 150M

| Component | Estimated Size |
|---|---|
| Filter bitmaps | ~8.1 GB |
| Sort bitmaps | ~1.1 GB |
| Trie cache | ~160 MB |
| **Total bitmap memory** | **~9.3 GB** |
| **Total RSS** | **~17.4 GB** |

Within the original 7-11 GB bitmap target. RSS overhead is ~48% from redb + allocator.

Document store on disk (redb): ~6 GB at 100M records.

---

## Future Roadmap (NOT V2 Scope — Do Not Build)

- LSH vector similarity search
- Postgres extension (pgrx)
- Log encoding for sort fields (unless benchmarks demand it)
- OpenSearch/Meilisearch query parser plugins
- Visual bitmap explorer
- Multi-index support
- Shared memory hot restarts
