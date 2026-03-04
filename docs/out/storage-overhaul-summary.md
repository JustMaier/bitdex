# Storage Overhaul Summary

## What's changing

We're replacing redb (embedded B-tree KV store) with a fully filesystem-based storage model. redb added ~100% overhead at scale (73GB file for ~35GB of data at 105M records) with no compression and expensive compaction. The new approach uses individual files + mmap + OS page cache, eliminating the embedded database entirely.

---

## 5 major changes

### 1. Filesystem bitmaps (replaces redb bitmap store + moka cache)

**Before:** All bitmaps serialized into redb tables (`TABLE_BITMAPS`, `TABLE_SORT_LAYERS`, `TABLE_META`). Two-tier model: hot bitmaps in ArcSwap snapshot, cold bitmaps loaded via moka LRU cache over redb.

**After:** One `.roar` file per bitmap on the filesystem. Memory-mapped. OS page cache replaces moka entirely.

```
bitmaps/
  filter/{field_name}/{value_id}.roar
  sort/{field_name}/bit{00..31}.roar
  system/alive.roar, clean.roar
  cache/filters/{hash}.roar
  cache/bounds/{hash}.roar
```

**Code impact:**
- Delete `src/bitmap_store.rs` (redb-backed `BitmapStore`)
- Delete `src/tier2_cache.rs` (moka `Tier2Cache`)
- Remove `StorageMode` enum (`snapshot` / `cached` distinction gone)
- Remove `StorageConfig.bitmap_path`, `tier2_cache_size_mb`, `merge_diff_threshold`, `pending_drain_cap`
- New filesystem bitmap reader/writer (atomic write-tmp-rename pattern)
- Update `ConcurrentEngine`: snapshot load reads mmap'd files, flush writes individual files
- Update `save_snapshot()` / restore: iterate directory instead of redb scan

**Preserved (not redb-related):** The `VersionedBitmap` diff-based concurrency model (`src/versioned_bitmap.rs`) is independent of redb and must be kept. It provides the sets/clears diff layers that make the ArcSwap write path work — writers accumulate diffs, the flush thread merges them into base bitmaps, and `Arc::make_mut()` CoW ensures readers never block. This is core ArcSwap architecture, not a storage concern. The merge thread's role changes from "merge diffs + write to redb" to "merge diffs + write to filesystem," but the diff accumulation and merge logic itself is unchanged.

### 2. Filesystem document store (replaces redb docstore)

**Before:** Documents in redb (`DOCS_TABLE`) as msgpack+lz4 blobs with field dictionary in `META_TABLE`. Single 73GB+ redb file. Requires `Arc<Mutex<DocStore>>`.

**After:** Documents as individual msgpack+zstd files, hex-sharded by slot ID.

```
docs/{xx}/{yy}.bin    # shard_depth=2 → /docs/1B/82.bin
```

**New features:**
- **Default elision** — fields matching their schema default are omitted before serialization. ~12-14 fields elided per typical Civitai document.
- **Schema versioning** — `schema_version` byte prepended to each doc. Old docs decoded with historical schema. Lazy migration on next write.
- **Low-cardinality string dictionaries** — `type`, `baseModel`, `availability`, `needsReview`, `blockedFor` stored as u8/u16 integer IDs instead of strings. Dictionaries auto-built and persisted to `dictionaries/{field_name}.dict`.

**Size estimate:** ~100-150 bytes/doc (vs ~660 bytes/doc current) = ~13GB for 105M (vs 73GB redb).

**Code impact:**
- Rewrite `src/docstore.rs` — filesystem sharded store, no redb, no `Database`/`TableDefinition`
- New `src/dictionary.rs` — auto-built string dictionaries with configurable cardinality width
- New schema version tracking in `meta/schema/v{n}.yaml`
- `DocStore` no longer needs `&mut self` for reads — no field dictionary mutex
- Remove `parking_lot::Mutex` wrapper from `ConcurrentEngine.docstore` (or simplify significantly)

### 3. Persisted caches (replaces volatile trie/bound caches)

**Before:** Filter cache (`TrieCache`) and bound cache (`BoundCacheManager`) live purely in memory. Lost on restart — cold start penalty.

**After:** Cache bitmaps written to disk alongside source bitmaps.

```
bitmaps/cache/filters/{hash}.roar
bitmaps/cache/bounds/{hash}.roar
bitmaps/cache/meta/cache_stats.bin
```

**New config:**
- `filter_promotion_threshold` (default 10) — queries before caching
- `bound_promotion_threshold` (default 0) — immediate bound creation
- `gc_interval_secs`, `gc_min_hit_rate` — periodic cleanup of cold caches
- No `max_entries` memory cap needed — OS manages via page cache

**Code impact:**
- Update `src/cache.rs` — persist/restore trie cache entries to filesystem
- Update `src/bound_cache.rs` — persist/restore bound entries
- New GC thread/logic for cache eviction based on decay hit rates
- Remove application-level memory management (`max_entries`, moka eviction)

### 4. Backpressure-driven write pipeline (replaces manual loading mode)

**Before:** Manual `enter_loading_mode()` / `exit_loading_mode()` toggle. Caller must remember to call both. Missed `exit_loading_mode()` caused a production bug (empty bitmap snapshot).

**After:** Automatic detection based on mutation channel depth. System interpolates between steady-state and burst-mode parameters.

**New config section: `config.backpressure`**
- `low_watermark` / `high_watermark` — channel depth thresholds
- `publish_interval_steady_ms` / `publish_interval_burst_ms`
- `merge_interval_steady_ms` / `merge_interval_burst_ms`
- `batch_size_steady` / `batch_size_burst`

**Code impact:**
- Remove `enter_loading_mode()` / `exit_loading_mode()` from `ConcurrentEngine`
- Remove `loading_mode` flag and all conditional logic around it
- Update flush thread to read channel depth and interpolate parameters
- Update `src/loader.rs` — no more loading mode calls, just push mutations
- Remove `put_bulk_loading` (or unify with `put_bulk`)

### 5. Document schema changes

**Fields removed from stored docs:**
- `aiNsfwLevel` — removed entirely
- `combinedNsfwLevel` — removed (derived, redundant with `nsfwLevel`)
- `existedAtUnix` — removed
- `sortAtUnix` — removed (use `sortAt` directly)
- `publishedAtUnix` — removed (use `publishedAt` directly)

**Fields converted to low-cardinality string dictionaries:**
- `type` → u8 (image=1, video=2, audio=3)
- `baseModel` → u8 (~30 distinct values)
- `availability` → u8 (Public, Private, Unsearchable)
- `needsReview` → u8 (nullable, small set)
- `blockedFor` → u8 (nullable, small set)

**Timestamps normalized:** millisecond timestamps truncated to second-precision u32.

**Default elision candidates (typical Civitai doc):**
| Field | Default | Elide rate (est.) |
|-------|---------|-------------------|
| commentCount | 0 | ~80% |
| collectedCount | 0 | ~85% |
| hideMeta | false | ~90% |
| needsReview | null | ~95% |
| onSite | false | ~85% |
| toolIds | [] | ~70% |
| techniqueIds | [] | ~75% |
| minor | false | ~99% |
| blockedFor | null | ~99% |
| remixOfId | null | ~95% |
| poi | false | ~99% |
| acceptableMinor | false | ~99% |
| hasPositivePrompt | true | ~70% |
| availability | "Public" | ~90% |

---

## Storage size comparison (105M records)

| Component | Current (redb) | New (filesystem) |
|-----------|---------------|-----------------|
| Bitmaps | 13 GB (redb) | ~7-9 GB (raw .roar files) |
| Documents | 73 GB (redb, pre-compaction) | ~13 GB (msgpack+zstd+elision) |
| Caches | 0 (in-memory only) | ~1-2 GB (persisted) |
| **Total disk** | **~86 GB** | **~21-24 GB** |
| Cold start | Full bitmap deserialize (~30s) | mmap + page faults (~instant) |

---

## Dependencies changing

| Remove | Add |
|--------|-----|
| `redb` | `zstd` (compression) |
| `moka` | `memmap2` (memory-mapped I/O) |
| `lz4_flex` | — |

`rmp-serde` stays (msgpack serialization).

---

## Files to delete

- `src/bitmap_store.rs` — redb bitmap persistence
- `src/tier2_cache.rs` — moka LRU cache over redb

## Files to rewrite

- `src/docstore.rs` — filesystem sharded store with default elision + schema versioning
- `src/concurrent_engine.rs` — remove loading mode, remove redb/moka, add filesystem bitmap I/O + backpressure
- `src/write_coalescer.rs` — add channel-depth-based backpressure interpolation
- `src/config.rs` — new config schema (backpressure, docstore, cache GC, remove storage tiers)
- `src/server.rs` — remove loading mode calls, update restore logic
- `src/loader.rs` — remove loading mode calls, simplify

## Files to create

- `src/dictionary.rs` — auto-built low-cardinality string dictionaries
- `src/bitmap_fs.rs` (or similar) — filesystem bitmap read/write. Two core paths: **write** uses atomic tmp+fsync+rename (write to `{name}.roar.tmp`, fsync the file, rename over `{name}.roar` — rename is atomic on POSIX, readers always see complete files). **Read** uses `memmap2` to mmap the `.roar` file and deserialize the bitmap from the mapped region (zero-copy when possible, OS page cache manages eviction transparently).
- `src/backpressure.rs` — channel depth interpolation logic

## Files with moderate changes

- `src/cache.rs` — add disk persistence + GC
- `src/bound_cache.rs` — add disk persistence
- `src/meta_index.rs` — update for filesystem cache paths
- `src/engine.rs` — remove redb/docstore mutex threading
- `src/mutation.rs` — docstore no longer needs `&mut self`
- `src/executor.rs` — remove tier2 resolver
- `src/planner.rs` — remove tier2 references
- `Cargo.toml` — swap deps
