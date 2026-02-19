# Bitdex V2 — CLAUDE.md

## What is Bitdex?

Bitdex is a purpose-built, in-memory bitmap index engine written in Rust. Its only job is to take filter predicates and sort parameters and return an ordered list of integer IDs. It stores no documents. It serves no content. It is bitmaps all the way down.

**In:** Filter predicates + sort field + sort direction + limit
**Out:** Ordered `Vec<i64>` of IDs

Everything else — document retrieval, content serving, full-text search — happens downstream.

---

## Inviolable Design Principles

These are non-negotiable. Any agent working on this project MUST follow these rules. Violating them is grounds for rejecting a PR.

1. **Everything is a bitmap.** There are no other data structures for indexed data. No Vecs for column storage. No skip lists. No sorted arrays. No forward maps. No reverse indexes. No document storage. The only non-bitmap structures are a HashMap for the trie cache and Prometheus metrics counters.

2. **No document storage.** Bitdex returns IDs only. If you're tempted to store field values, stop. That's what V1 did and it failed.

3. **No sorted data structures.** No sorted Vecs, no skip lists, no B-trees for maintaining sort order. Sorting is done via bit-layer bitmap traversal. Period.

4. **No forward maps, no reverse indexes.** WAL events carry old and new values. Diff from the event. For DELETE WHERE on high-cardinality fields, scan the bitmaps.

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

### Phase 1: Core Engine (Current Priority)
Slot allocation, alive bitmap, filter bitmaps, sort layer bitmaps, mutation API (PUT/PATCH/DELETE/DELETE WHERE), query execution, JSON query parser, config loading.

### Phase 2: Persistence
WAL, snapshot serialization/deserialization, sidecar snapshot builder, startup sequence.

### Phase 3: Performance
Cardinality-based query planning, trie cache with LRU, prefix matching, optimistic concurrency.

### Phase 4: Operations
Prometheus metrics, autovac, admin API, graceful shutdown, health check.

### Phase 5: Integration
Postgres WAL consumer, backfill pipeline, shadow mode, end-to-end tests.

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

## Key Memory Estimates (150M documents)

| Component | Estimated Size |
|---|---|
| Alive bitmap | ~20MB |
| Filter bitmaps (all fields) | ~4.6-6.8GB |
| Sort bitmaps (5 fields x 32 layers) | ~2.5-4GB |
| Trie cache (configurable) | ~1-2GB |
| **Total** | **~7-11GB** |

Snapshot on disk with zstd compression: ~1-3GB.

---

## Future Roadmap (NOT V2 Scope — Do Not Build)

- LSH vector similarity search
- Postgres extension (pgrx)
- Log encoding for sort fields (unless benchmarks demand it)
- OpenSearch/Meilisearch query parser plugins
- Visual bitmap explorer
- Multi-index support
- Shared memory hot restarts
