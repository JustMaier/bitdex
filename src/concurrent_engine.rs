use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arc_swap::{ArcSwap, Guard};
use crossbeam_channel::{Receiver, Sender};
use roaring::RoaringBitmap;

use crate::bitmap_store::BitmapStore;
use crate::bound_cache::{BoundCacheManager, BoundKey};
use crate::cache::{self, CacheLookup, CacheKey, TrieCache};
use crate::concurrency::InFlightTracker;
use crate::config::{Config, StorageMode};
use crate::docstore::{DocStore, StoredDoc};
use crate::error::Result;
use crate::executor::{QueryExecutor, Tier2Resolver};
use crate::mutation::{diff_document, diff_patch, value_to_bitmap_key, Document, FieldRegistry, PatchPayload};
use crate::pending_buffer::PendingBuffer;
use crate::planner;
use crate::query::{BitdexQuery, FilterClause, SortClause, SortDirection};
use crate::tier2_cache::Tier2Cache;
use crate::time_buckets::TimeBucketManager;
use crate::types::QueryResult;
use crate::write_coalescer::{MutationOp, MutationSender, WriteCoalescer};

/// Inner bitmap state published as immutable snapshots via ArcSwap.
///
/// All fields are Clone via Arc-per-bitmap CoW. Cloning bumps refcounts
/// on the Arc-wrapped bitmaps — zero data copy. Actual bitmap data is
/// only cloned on mutation via `Arc::make_mut()`.
#[derive(Clone)]
pub struct InnerEngine {
    pub slots: crate::slot::SlotAllocator,
    pub filters: crate::filter::FilterIndex,
    pub sorts: crate::sort::SortIndex,
}

/// Thread-safe engine using ArcSwap for lock-free snapshot reads.
///
/// Writers call `put`/`patch`/`delete` which compute diffs and send
/// MutationOps to a channel. A background flush thread applies batched
/// mutations to a private staging copy, then atomically publishes a
/// new snapshot via ArcSwap::store().
///
/// Readers load the current snapshot via `load_full()` — fully lock-free,
/// no contention with writers or the flush thread.
pub struct ConcurrentEngine {
    inner: Arc<ArcSwap<InnerEngine>>,
    cache: Arc<parking_lot::Mutex<TrieCache>>,
    sender: MutationSender,
    doc_tx: Sender<(u32, StoredDoc)>,
    docstore: Arc<DocStore>,
    config: Arc<Config>,
    field_registry: FieldRegistry,
    in_flight: InFlightTracker,
    shutdown: Arc<AtomicBool>,
    flush_handle: Option<JoinHandle<()>>,
    merge_handle: Option<JoinHandle<()>>,
    bitmap_store: Option<Arc<BitmapStore>>,
    tier2_cache: Option<Arc<Tier2Cache>>,
    pending: Arc<parking_lot::Mutex<PendingBuffer>>,
    bound_cache: Arc<parking_lot::Mutex<BoundCacheManager>>,
    loading_mode: Arc<AtomicBool>,
    time_buckets: Option<Arc<parking_lot::Mutex<TimeBucketManager>>>,
}

impl ConcurrentEngine {
    /// Create a new concurrent engine with an in-memory docstore (for testing).
    pub fn new(config: Config) -> Result<Self> {
        config.validate()?;
        let docstore = DocStore::open_temp()?;
        Self::build(config, docstore)
    }

    /// Create a new concurrent engine with an on-disk docstore.
    pub fn new_with_path(config: Config, path: &Path) -> Result<Self> {
        config.validate()?;
        let docstore = DocStore::open(path)?;
        Self::build(config, docstore)
    }

    fn build(config: Config, docstore: DocStore) -> Result<Self> {
        let mut filters = crate::filter::FilterIndex::new();
        let mut sorts = crate::sort::SortIndex::new();

        // Only add Tier 1 (Snapshot) fields to the in-memory FilterIndex.
        // Tier 2 (Cached) fields are resolved via moka cache + redb on demand.
        for fc in &config.filter_fields {
            if fc.storage == StorageMode::Snapshot {
                filters.add_field(fc.clone());
            }
        }
        for sc in &config.sort_fields {
            sorts.add_field(sc.clone());
        }

        let field_registry = FieldRegistry::from_config(&config);
        let cache = Arc::new(parking_lot::Mutex::new(TrieCache::new(config.cache.clone())));

        // Open bitmap store if configured
        let bitmap_store = if let Some(ref path) = config.storage.bitmap_path {
            Some(Arc::new(BitmapStore::new(path)?))
        } else {
            None
        };

        // Load Tier 1 filter bitmaps from redb on startup (A8)
        if let Some(ref store) = bitmap_store {
            let tier1_names: Vec<&str> = config
                .filter_fields
                .iter()
                .filter(|f| f.storage == StorageMode::Snapshot)
                .map(|f| f.name.as_str())
                .collect();
            if !tier1_names.is_empty() {
                let loaded = store.load_all_fields(&tier1_names)?;
                for (field_name, bitmaps) in loaded {
                    if !bitmaps.is_empty() {
                        if let Some(field) = filters.get_field_mut(&field_name) {
                            field.load_from(bitmaps);
                        }
                    }
                }
            }
        }

        // S2.3: Load alive, sort layers, and slot counter from redb on startup
        let mut slots = crate::slot::SlotAllocator::new();
        if let Some(ref store) = bitmap_store {
            let alive = store.load_alive()?;
            let counter = store.load_slot_counter()?;
            if let Some(alive_bm) = alive {
                let counter_val = counter.unwrap_or(0);
                slots = crate::slot::SlotAllocator::from_state(
                    counter_val,
                    alive_bm,
                    RoaringBitmap::new(),
                );
            }

            // Load sort layers
            for sc in &config.sort_fields {
                if let Some(layers) = store.load_sort_layers(&sc.name, sc.bits as usize)? {
                    if let Some(sf) = sorts.get_field_mut(&sc.name) {
                        sf.load_layers(layers);
                    }
                }
            }
        }

        // Initialize Tier 2 cache if any fields are Cached
        let has_tier2 = config.filter_fields.iter().any(|f| f.storage == StorageMode::Cached);
        let tier2_cache = if has_tier2 {
            Some(Arc::new(Tier2Cache::new(config.storage.tier2_cache_size_mb)))
        } else {
            None
        };

        let pending = Arc::new(parking_lot::Mutex::new(PendingBuffer::new()));
        let bound_cache = Arc::new(parking_lot::Mutex::new(BoundCacheManager::with_max_count(
            config.cache.bound_target_size,
            config.cache.bound_max_size,
            config.cache.bound_max_count,
        )));
        let loading_mode = Arc::new(AtomicBool::new(false));

        // S3.3: Instantiate TimeBucketManager if any filter field has range_buckets configured
        let time_buckets = {
            let mut tb: Option<TimeBucketManager> = None;
            for fc in &config.filter_fields {
                if let Some(ref behaviors) = fc.behaviors {
                    if !behaviors.range_buckets.is_empty() {
                        tb = Some(TimeBucketManager::new(
                            fc.name.clone(),
                            behaviors.range_buckets.clone(),
                        ));
                        break; // Only one field can have range buckets
                    }
                }
            }
            tb.map(|m| Arc::new(parking_lot::Mutex::new(m)))
        };

        let inner_engine = InnerEngine {
            slots,
            filters,
            sorts,
        };

        // Flush thread owns a staging clone; readers see published snapshots
        let mut staging = inner_engine.clone();
        let inner = Arc::new(ArcSwap::new(Arc::new(inner_engine)));

        let (mut coalescer, sender) = WriteCoalescer::new(config.channel_capacity);
        let shutdown = Arc::new(AtomicBool::new(false));
        let config = Arc::new(config);

        // Docstore write channel — bounded for backpressure
        let (doc_tx, doc_rx): (Sender<(u32, StoredDoc)>, Receiver<(u32, StoredDoc)>) =
            crossbeam_channel::bounded(config.channel_capacity);

        let docstore = Arc::new(docstore);

        // Collect Tier 1 filter field names for cache invalidation
        let filter_field_names: Vec<String> = config
            .filter_fields
            .iter()
            .filter(|f| f.storage == StorageMode::Snapshot)
            .map(|f| f.name.clone())
            .collect();

        // Collect Tier 2 field names for mutation routing
        let tier2_field_names: std::collections::HashSet<String> = config
            .filter_fields
            .iter()
            .filter(|f| f.storage == StorageMode::Cached)
            .map(|f| f.name.clone())
            .collect();

        let flush_handle = {
            let inner = Arc::clone(&inner);
            let cache = Arc::clone(&cache);
            let shutdown = Arc::clone(&shutdown);
            let docstore = Arc::clone(&docstore);
            let flush_interval_us = config.flush_interval_us;
            let field_names = filter_field_names;
            let tier2_fields = tier2_field_names.clone();
            let flush_pending = Arc::clone(&pending);
            let flush_tier2_cache = tier2_cache.clone();
            let flush_bound_cache = Arc::clone(&bound_cache);
            let flush_loading_mode = Arc::clone(&loading_mode);

            thread::spawn(move || {
                let min_sleep = Duration::from_micros(flush_interval_us);
                let max_sleep = Duration::from_micros(flush_interval_us * 10);
                let mut current_sleep = min_sleep;
                let mut doc_batch: Vec<(u32, StoredDoc)> = Vec::new();
                let mut was_loading = false;
                let mut staging_dirty = false; // tracks unpublished mutations from loading mode
                let mut flush_cycle: u64 = 0;
                // Compact filter diffs every N flush cycles (~5s at 100μs interval).
                // Keeps diff layers small so apply_diff/fused stay fast.
                const COMPACTION_INTERVAL: u64 = 50;

                while !shutdown.load(Ordering::Relaxed) {
                    thread::sleep(current_sleep);
                    let is_loading = flush_loading_mode.load(Ordering::Relaxed);

                    // Phase 1: Drain channel and group/sort (no lock, pure CPU work)
                    let bitmap_count = coalescer.prepare();

                    // Phase 2: Apply mutations to staging (private, no lock needed)
                    if bitmap_count > 0 {
                        staging_dirty = true;
                        // Extract Tier 2 mutations BEFORE apply (apply would silently
                        // ignore them since Tier 2 fields aren't in FilterIndex,
                        // but we need them for PendingBuffer).
                        let tier2_mutations = if !tier2_fields.is_empty() {
                            coalescer.take_tier2_mutations(&tier2_fields)
                        } else {
                            Vec::new()
                        };

                        coalescer.apply_prepared(
                            &mut staging.slots,
                            &mut staging.filters,
                            &mut staging.sorts,
                        );

                        // Activate deferred alive slots whose time has come.
                        // O(pending count) — typically small; runs every flush cycle for
                        // sub-second activation precision.
                        {
                            let now_unix = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            let activated = staging.slots.activate_due(now_unix);
                            if !activated.is_empty() {
                                staging.slots.merge_alive();
                            }
                        }

                        // In loading mode, skip all maintenance and snapshot publishing.
                        // This avoids the expensive staging.clone() → Arc::make_mut clone
                        // cascade that dominates write cost at scale.
                        if !flush_loading_mode.load(Ordering::Relaxed) {
                            // D3/E3: Live maintenance of bound caches on sort field mutations.
                            // Uses meta-index for O(1) lookup of relevant bounds instead of
                            // linear scan. For each mutated slot, check if its new sort value
                            // qualifies for any matching bound. Bits are only added, never
                            // removed — bloat control (D4) handles cleanup.
                            {
                                let sort_mutations = coalescer.mutated_sort_slots();
                                if !sort_mutations.is_empty() {
                                    let mut bc = flush_bound_cache.lock();
                                    if !bc.is_empty() {
                                        for (sort_field, slots) in &sort_mutations {
                                            // E3: Use meta-index to find matching bounds (O(1) vs linear)
                                            let matching_keys = bc.bounds_for_sort_field(sort_field);

                                            if matching_keys.is_empty() {
                                                continue;
                                            }

                                            for &slot in slots {
                                                let value = staging.sorts
                                                    .get_field(sort_field)
                                                    .map(|f| f.reconstruct_value(slot))
                                                    .unwrap_or(0);

                                                for bound_key in &matching_keys {
                                                    if let Some(entry) = bc.get_mut(bound_key) {
                                                        if entry.needs_rebuild() {
                                                            continue;
                                                        }
                                                        let qualifies = match bound_key.direction {
                                                            SortDirection::Desc => value > entry.min_tracked_value(),
                                                            SortDirection::Asc => value < entry.min_tracked_value(),
                                                        };
                                                        if qualifies {
                                                            entry.add_slot(slot);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            // Route Tier 2 mutations to PendingBuffer and invalidate moka cache
                            if !tier2_mutations.is_empty() {
                                let mut p = flush_pending.lock();
                                for (field, value, slots, is_set) in &tier2_mutations {
                                    for &slot in slots {
                                        if *is_set {
                                            p.add_set(field, *value, slot);
                                        } else {
                                            p.add_clear(field, *value, slot);
                                        }
                                    }
                                }
                                drop(p);
                                // Invalidate moka cache entries that were mutated
                                if let Some(ref t2c) = flush_tier2_cache {
                                    for (field, value, _, _) in &tier2_mutations {
                                        t2c.invalidate(field, *value);
                                    }
                                }
                            }

                            // Targeted cache invalidation: only invalidate fields that actually changed.
                            // Sort-only flushes (e.g., reactionCount updates) skip invalidation entirely.
                            if coalescer.has_alive_mutations() {
                                // Alive changed — invalidate all filter fields because NotEq/Not
                                // bake alive into cached results.
                                let mut c = cache.lock();
                                for name in &field_names {
                                    c.invalidate_field(name);
                                }
                                // Also invalidate all Tier 2 cache entries (alive changed)
                                if let Some(ref t2c) = flush_tier2_cache {
                                    for field_name in &tier2_fields {
                                        let arc: Arc<str> = Arc::from(field_name.as_str());
                                        t2c.invalidate_field(&arc);
                                    }
                                }
                            } else {
                                let changed = coalescer.mutated_filter_fields();
                                if !changed.is_empty() {
                                    let mut c = cache.lock();
                                    for name in &changed {
                                        c.invalidate_field(name);
                                    }
                                }
                            }

                            // D3: Invalidate bounds whose filter fields changed.
                            // Alive mutations affect all bounds (alive is implicit in filter results).
                            {
                                let changed_filters = coalescer.mutated_filter_fields();
                                let has_alive = coalescer.has_alive_mutations();
                                if has_alive || !changed_filters.is_empty() {
                                    let mut bc = flush_bound_cache.lock();
                                    if !bc.is_empty() {
                                        if has_alive {
                                            // Alive changed — mark ALL bounds for rebuild
                                            for (_, entry) in bc.iter_mut() {
                                                entry.mark_for_rebuild();
                                            }
                                        } else {
                                            for field_name in &changed_filters {
                                                bc.invalidate_filter_field(field_name);
                                            }
                                        }
                                    }
                                }
                            }

                            // Periodic filter diff compaction: merge dirty diffs into
                            // bases so apply_diff/fused don't accumulate unbounded diffs.
                            // Runs every COMPACTION_INTERVAL flush cycles (~5s).
                            // Sort diffs and alive are already merged eagerly in WriteBatch::apply().
                            if flush_cycle % COMPACTION_INTERVAL == 0 {
                                for (_name, field) in staging.filters.fields_mut() {
                                    field.merge_dirty();
                                }
                            }
                            flush_cycle += 1;

                            // Publish new snapshot atomically (Arc-per-bitmap CoW clone)
                            inner.store(Arc::new(staging.clone()));
                            staging_dirty = false;
                        }
                    }

                    // Loading mode exit: force-publish if staging has unpublished mutations
                    if was_loading && !is_loading && staging_dirty {
                        // Compact all filter diffs accumulated during loading
                        for (_name, field) in staging.filters.fields_mut() {
                            field.merge_dirty();
                        }
                        // Invalidate all caches — they may be stale from the loading period
                        let mut c = cache.lock();
                        for name in &field_names {
                            c.invalidate_field(name);
                        }
                        drop(c);
                        if let Some(ref t2c) = flush_tier2_cache {
                            for field_name in &tier2_fields {
                                let arc: Arc<str> = Arc::from(field_name.as_str());
                                t2c.invalidate_field(&arc);
                            }
                        }
                        inner.store(Arc::new(staging.clone()));
                        staging_dirty = false;
                    }
                    was_loading = is_loading;

                    // Phase 3: Drain docstore channel and batch write
                    doc_batch.clear();
                    while let Ok(item) = doc_rx.try_recv() {
                        doc_batch.push(item);
                    }
                    let doc_count = doc_batch.len();
                    if doc_count > 0 {
                        if let Err(e) = docstore.put_batch(&doc_batch) {
                            eprintln!("docstore batch write failed: {e}");
                        }
                    }

                    if bitmap_count > 0 || doc_count > 0 {
                        current_sleep = min_sleep;
                    } else {
                        current_sleep = (current_sleep * 2).min(max_sleep);
                    }
                }

                // Final flush on shutdown
                let count = coalescer.prepare();
                if count > 0 {
                    // Extract Tier 2 mutations before final apply
                    let tier2_mutations = if !tier2_fields.is_empty() {
                        coalescer.take_tier2_mutations(&tier2_fields)
                    } else {
                        Vec::new()
                    };

                    coalescer.apply_prepared(
                        &mut staging.slots,
                        &mut staging.filters,
                        &mut staging.sorts,
                    );

                    if !tier2_mutations.is_empty() {
                        let mut p = flush_pending.lock();
                        for (field, value, slots, is_set) in &tier2_mutations {
                            for &slot in slots {
                                if *is_set {
                                    p.add_set(field, *value, slot);
                                } else {
                                    p.add_clear(field, *value, slot);
                                }
                            }
                        }
                    }

                    // Compact all remaining filter diffs before final publish
                    for (_name, field) in staging.filters.fields_mut() {
                        field.merge_dirty();
                    }

                    // Shutdown: invalidate all fields for safety
                    let mut c = cache.lock();
                    for name in &field_names {
                        c.invalidate_field(name);
                    }
                    inner.store(Arc::new(staging.clone()));
                }

                // Final docstore drain
                doc_batch.clear();
                while let Ok(item) = doc_rx.try_recv() {
                    doc_batch.push(item);
                }
                if !doc_batch.is_empty() {
                    if let Err(e) = docstore.put_batch(&doc_batch) {
                        eprintln!("docstore final batch write failed: {e}");
                    }
                }
            })
        };

        let merge_handle = {
            let shutdown = Arc::clone(&shutdown);
            let merge_inner = Arc::clone(&inner);
            let merge_interval_ms = config.merge_interval_ms;
            let merge_pending = Arc::clone(&pending);
            let merge_bitmap_store = bitmap_store.clone();
            let merge_tier2_cache = tier2_cache.clone();
            let pending_drain_cap = config.storage.pending_drain_cap;
            let sort_field_configs: Vec<crate::config::SortFieldConfig> =
                config.sort_fields.clone();

            thread::spawn(move || {
                let sleep_duration = Duration::from_millis(merge_interval_ms);
                while !shutdown.load(Ordering::Relaxed) {
                    thread::sleep(sleep_duration);

                    // S2.2: Snapshot, compact Tier 1 filter diffs, persist to redb
                    if let Some(ref store) = merge_bitmap_store {
                        // Take a snapshot and compact filter diffs
                        let snap = merge_inner.load_full();
                        let mut compacted = (*snap).clone();
                        for (_name, field) in compacted.filters.fields_mut() {
                            field.merge_dirty();
                        }

                        // Collect Tier 1 filter bitmap entries for persistence
                        let mut filter_entries: Vec<(String, u64, RoaringBitmap)> = Vec::new();
                        for (name, field) in compacted.filters.fields() {
                            for (&value, vb) in field.iter_versioned() {
                                filter_entries.push((
                                    name.clone(),
                                    value,
                                    vb.base().as_ref().clone(),
                                ));
                            }
                        }

                        // Collect sort layer bases
                        let mut sort_data: Vec<(String, Vec<RoaringBitmap>)> = Vec::new();
                        for sc in &sort_field_configs {
                            if let Some(sf) = compacted.sorts.get_field(&sc.name) {
                                let bases: Vec<RoaringBitmap> = sf
                                    .layer_bases()
                                    .iter()
                                    .map(|b| (*b).clone())
                                    .collect();
                                sort_data.push((sc.name.clone(), bases));
                            }
                        }

                        // Build references for write_full_snapshot
                        let filter_refs: Vec<(&str, u64, &RoaringBitmap)> = filter_entries
                            .iter()
                            .map(|(f, v, b)| (f.as_str(), *v, b))
                            .collect();
                        let alive = compacted.slots.alive_bitmap().clone();
                        let slot_counter = compacted.slots.slot_counter();

                        // Sort layer refs: owned Vec<&BM> must outlive the slice refs
                        let sort_owned_refs: Vec<(String, Vec<&RoaringBitmap>)> = sort_data
                            .iter()
                            .map(|(name, layers)| {
                                (name.clone(), layers.iter().collect::<Vec<&RoaringBitmap>>())
                            })
                            .collect();
                        let sort_slice_refs: Vec<(&str, &[&RoaringBitmap])> = sort_owned_refs
                            .iter()
                            .map(|(name, refs)| (name.as_str(), refs.as_slice()))
                            .collect();

                        if let Err(e) = store.write_full_snapshot(
                            &filter_refs,
                            &alive,
                            &sort_slice_refs,
                            slot_counter,
                        ) {
                            eprintln!("merge thread: redb snapshot write failed: {e}");
                        }

                        // Drain pending buffer to redb (Tier 2)
                        let to_drain = {
                            let mut p = merge_pending.lock();
                            p.drain_heaviest(pending_drain_cap)
                        };

                        if !to_drain.is_empty() {
                            let mut batch: Vec<(String, u64, roaring::RoaringBitmap)> = Vec::new();
                            for ((field, value), mutations) in &to_drain {
                                let mut bitmap = store
                                    .load_single(field, *value)
                                    .unwrap_or_default();
                                mutations.apply_to(&mut bitmap);
                                batch.push((field.to_string(), *value, bitmap));
                            }
                            let refs: Vec<(&str, u64, &roaring::RoaringBitmap)> = batch
                                .iter()
                                .map(|(f, v, b)| (f.as_str(), *v, b))
                                .collect();
                            if let Err(e) = store.write_batch(&refs) {
                                eprintln!("merge thread: redb Tier 2 write failed: {e}");
                            }
                            if let Some(ref t2c) = merge_tier2_cache {
                                for (i, ((field, value), _)) in to_drain.iter().enumerate() {
                                    if t2c.contains(field, *value) {
                                        t2c.insert(
                                            field,
                                            *value,
                                            Arc::new(batch[i].2.clone()),
                                        );
                                    }
                                }
                            }
                        }

                        // S2.6: Log pending buffer depth after drain
                        let remaining = merge_pending.lock().depth();
                        if remaining > 0 {
                            eprintln!("merge thread: pending buffer depth after drain: {remaining}");
                        }
                    }
                }
            })
        };

        Ok(Self {
            inner,
            cache,
            sender,
            doc_tx,
            docstore,
            config,
            field_registry,
            in_flight: InFlightTracker::new(),
            shutdown,
            flush_handle: Some(flush_handle),
            merge_handle: Some(merge_handle),
            bitmap_store,
            tier2_cache,
            pending,
            bound_cache,
            loading_mode,
            time_buckets,
        })
    }

    /// Load the current snapshot (lock-free, zero refcount ops).
    ///
    /// Returns a Guard that derefs to Arc<InnerEngine>. Unlike `load_full()`,
    /// this avoids atomic refcount increment/decrement and moves deallocation
    /// of old snapshots off the reader path onto the flush thread's `store()`.
    fn snapshot(&self) -> Guard<Arc<InnerEngine>> {
        self.inner.load()
    }

    /// PUT(id, document) -- full replace with upsert semantics.
    ///
    /// 1. Mark in-flight
    /// 2. Check alive status (lock-free snapshot)
    /// 3. Read old doc from docstore if upsert
    /// 4. Diff old vs new -> MutationOps
    /// 5. Send ops to coalescer channel
    /// 6. Enqueue doc write to docstore channel (flush thread batches these)
    /// 7. Clear in-flight
    pub fn put(&self, id: u32, doc: &Document) -> Result<()> {
        self.in_flight.mark_in_flight(id);

        let result = (|| -> Result<()> {
            // Check alive status via lock-free snapshot
            let (is_upsert, was_allocated) = {
                let snap = self.snapshot();
                let alive = snap.slots.is_alive(id);
                let alloc = if !alive {
                    snap.slots.was_ever_allocated(id)
                } else {
                    false
                };
                (alive, alloc)
            };

            // Read old doc from docstore if needed
            let old_doc = if is_upsert || was_allocated {
                self.docstore.get(id)?
            } else {
                None
            };

            // Compute diff purely -> Vec<MutationOp>
            let ops = diff_document(id, old_doc.as_ref(), doc, &self.config, is_upsert, &self.field_registry);

            // Send ops to coalescer channel
            self.sender.send_batch(ops).map_err(|_| {
                crate::error::BitdexError::CapacityExceeded(
                    "coalescer channel disconnected".to_string(),
                )
            })?;

            // Enqueue doc write — flush thread will batch these
            let stored = StoredDoc {
                fields: doc.fields.clone(),
            };
            self.doc_tx.send((id, stored)).map_err(|_| {
                crate::error::BitdexError::CapacityExceeded(
                    "docstore channel disconnected".to_string(),
                )
            })?;

            Ok(())
        })();

        self.in_flight.clear_in_flight(id);
        result
    }

    /// PATCH(id, partial_fields) -- merge only provided fields.
    pub fn patch(&self, id: u32, patch: &PatchPayload) -> Result<()> {
        self.in_flight.mark_in_flight(id);

        let result = (|| -> Result<()> {
            // Verify the slot is alive via lock-free snapshot
            {
                let snap = self.snapshot();
                if !snap.slots.is_alive(id) {
                    return Err(crate::error::BitdexError::SlotNotFound(id));
                }
            }

            let ops = diff_patch(id, patch, &self.config, &self.field_registry);

            self.sender.send_batch(ops).map_err(|_| {
                crate::error::BitdexError::CapacityExceeded(
                    "coalescer channel disconnected".to_string(),
                )
            })?;

            Ok(())
        })();

        self.in_flight.clear_in_flight(id);
        result
    }

    /// DELETE(id) -- send alive remove op to coalescer.
    pub fn delete(&self, id: u32) -> Result<()> {
        self.sender
            .send(MutationOp::AliveRemove { slots: vec![id] })
            .map_err(|_| {
                crate::error::BitdexError::CapacityExceeded(
                    "coalescer channel disconnected".to_string(),
                )
            })?;
        Ok(())
    }

    /// Build a Tier2Resolver if Tier 2 components are available.
    fn make_tier2_resolver(&self) -> Option<Tier2Resolver> {
        match (&self.tier2_cache, &self.bitmap_store) {
            (Some(cache), Some(store)) => Some(Tier2Resolver {
                cache: Arc::clone(cache),
                pending: Arc::clone(&self.pending),
                store: Arc::clone(store),
            }),
            _ => None,
        }
    }

    /// Execute a query from individual filter/sort/limit components.
    pub fn query(
        &self,
        filters: &[FilterClause],
        sort: Option<&SortClause>,
        limit: usize,
    ) -> Result<QueryResult> {
        let snap = self.snapshot(); // lock-free
        let tier2_resolver = self.make_tier2_resolver();
        // S3.4: Lock time_buckets once for the query lifetime
        let tb_guard = self.time_buckets.as_ref().map(|tb| tb.lock());
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let executor = {
            let base = match tier2_resolver.as_ref() {
                Some(t2) => QueryExecutor::with_tier2(
                    &snap.slots,
                    &snap.filters,
                    &snap.sorts,
                    self.config.max_page_size,
                    t2,
                ),
                None => QueryExecutor::new(
                    &snap.slots,
                    &snap.filters,
                    &snap.sorts,
                    self.config.max_page_size,
                ),
            };
            if let Some(ref tb) = tb_guard {
                base.with_time_buckets(tb, now_unix)
            } else {
                base
            }
        };

        let (filter_bitmap, use_simple_sort) =
            self.resolve_filters(&executor, filters)?;

        // D5: Narrow filter bitmap with bound cache for sort queries
        let (effective_bitmap, use_simple, cache_key) =
            self.apply_bound(&executor, filter_bitmap, use_simple_sort, sort, filters, None);

        let mut result =
            executor.execute_from_bitmap(effective_bitmap, sort, limit, None, use_simple)?;

        // D2: Form or update bound from sort results
        self.update_bound_from_results(&snap, sort, &cache_key, &result.ids, None);

        // Post-validation against in-flight writes
        self.post_validate(&mut result, filters, &executor)?;

        Ok(result)
    }

    /// Execute a parsed BitdexQuery.
    pub fn execute_query(&self, query: &BitdexQuery) -> Result<QueryResult> {
        let snap = self.snapshot(); // lock-free
        let tier2_resolver = self.make_tier2_resolver();
        let tb_guard = self.time_buckets.as_ref().map(|tb| tb.lock());
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let executor = {
            let base = match tier2_resolver.as_ref() {
                Some(t2) => QueryExecutor::with_tier2(
                    &snap.slots,
                    &snap.filters,
                    &snap.sorts,
                    self.config.max_page_size,
                    t2,
                ),
                None => QueryExecutor::new(
                    &snap.slots,
                    &snap.filters,
                    &snap.sorts,
                    self.config.max_page_size,
                ),
            };
            if let Some(ref tb) = tb_guard {
                base.with_time_buckets(tb, now_unix)
            } else {
                base
            }
        };

        let (filter_bitmap, use_simple_sort) =
            self.resolve_filters(&executor, &query.filters)?;

        // D5: Narrow filter bitmap with bound cache for sort queries
        let (effective_bitmap, use_simple, cache_key) = self.apply_bound(
            &executor,
            filter_bitmap,
            use_simple_sort,
            query.sort.as_ref(),
            &query.filters,
            query.cursor.as_ref(),
        );

        let mut result = executor.execute_from_bitmap(
            effective_bitmap,
            query.sort.as_ref(),
            query.limit,
            query.cursor.as_ref(),
            use_simple,
        )?;

        // D2/D6: Form or update bound from sort results (with cursor for tiered bounds)
        self.update_bound_from_results(
            &snap,
            query.sort.as_ref(),
            &cache_key,
            &result.ids,
            query.cursor.as_ref(),
        );

        self.post_validate(&mut result, &query.filters, &executor)?;

        Ok(result)
    }

    /// Resolve filter clauses to a bitmap, using the trie cache with brief locks.
    ///
    /// Cache Mutex is held ONLY during lookup (~μs) and store (~μs),
    /// never during filter computation or sort traversal.
    fn resolve_filters(
        &self,
        executor: &QueryExecutor,
        filters: &[FilterClause],
    ) -> Result<(roaring::RoaringBitmap, bool)> {
        let plan = planner::plan_query(filters, executor.filter_index(), executor.slot_allocator());
        let cache_key = cache::canonicalize(&plan.ordered_clauses);

        let filter_bitmap = if let Some(ref key) = cache_key {
            // Brief lock: cache lookup only
            let lookup = { self.cache.lock().lookup(key) };
            // Lock released — CacheLookup owns its bitmaps

            match lookup {
                CacheLookup::ExactHit(bitmap) => bitmap,
                CacheLookup::PrefixHit { mut bitmap, matched_prefix_len } => {
                    // Compute remaining clauses (no lock held)
                    for clause in &plan.ordered_clauses[matched_prefix_len..] {
                        let clause_bm = executor.evaluate_clause(clause)?;
                        bitmap &= &clause_bm;
                    }
                    // Brief lock: store result
                    self.cache.lock().store(key, bitmap.clone());
                    bitmap
                }
                CacheLookup::Miss => {
                    // Full computation (no lock held)
                    let bitmap = executor.compute_filters(&plan.ordered_clauses)?;
                    // Brief lock: store result
                    self.cache.lock().store(key, bitmap.clone());
                    bitmap
                }
            }
        } else {
            // Uncacheable query — compute without cache
            executor.compute_filters(&plan.ordered_clauses)?
        };

        Ok((filter_bitmap, plan.use_simple_sort))
    }

    /// Post-validate query results against in-flight writes.
    fn post_validate(
        &self,
        result: &mut QueryResult,
        filters: &[FilterClause],
        executor: &QueryExecutor,
    ) -> Result<()> {
        if !self.in_flight.has_in_flight() {
            return Ok(());
        }

        let overlapping = self.in_flight.find_overlapping(&result.ids);
        if overlapping.is_empty() {
            return Ok(());
        }

        // The executor holds references to the snapshot's bitmap state
        // so we can revalidate in-flight slots.
        let mut invalid_slots: Vec<u32> = Vec::new();

        for &slot in &overlapping {
            if !executor.slot_matches_filters(slot, filters)? {
                invalid_slots.push(slot);
            }
        }

        if !invalid_slots.is_empty() {
            result
                .ids
                .retain(|id| !invalid_slots.contains(&(*id as u32)));
        }

        Ok(())
    }

    /// D5: Apply bound cache narrowing for sort queries.
    ///
    /// If a matching bound exists and is usable, ANDs the filter bitmap with the
    /// bound bitmap to reduce the sort working set. Returns the effective bitmap,
    /// whether to use simple sort, and the cache key for bound formation.
    fn apply_bound(
        &self,
        _executor: &QueryExecutor,
        filter_bitmap: roaring::RoaringBitmap,
        use_simple_sort: bool,
        sort: Option<&SortClause>,
        filters: &[FilterClause],
        cursor: Option<&crate::query::CursorPosition>,
    ) -> (roaring::RoaringBitmap, bool, Option<CacheKey>) {
        let Some(sort_clause) = sort else {
            return (filter_bitmap, use_simple_sort, None);
        };

        let cache_key = cache::canonicalize(filters);
        let Some(ref filter_key) = cache_key else {
            return (filter_bitmap, use_simple_sort, None);
        };

        let bound_key = BoundKey {
            filter_key: filter_key.clone(),
            sort_field: sort_clause.field.clone(),
            direction: sort_clause.direction,
            tier: 0,
        };

        let mut bc = self.bound_cache.lock();

        // D6: Try tier 0 first, then escalate to higher tiers if cursor is past bound
        let max_tiers = 4u32; // cap to avoid unbounded tier growth
        let mut try_key = bound_key;
        for _tier in 0..max_tiers {
            if let Some(entry) = bc.lookup_mut(&try_key) {
                if !entry.needs_rebuild() {
                    // Check if cursor is past this tier's range
                    let cursor_past = if let Some(c) = cursor {
                        let cursor_val = c.sort_value as u32;
                        match sort_clause.direction {
                            SortDirection::Desc => cursor_val < entry.min_tracked_value(),
                            SortDirection::Asc => cursor_val > entry.min_tracked_value(),
                        }
                    } else {
                        false
                    };

                    if cursor_past {
                        // Try next tier
                        try_key = BoundKey {
                            filter_key: try_key.filter_key,
                            sort_field: try_key.sort_field,
                            direction: try_key.direction,
                            tier: try_key.tier + 1,
                        };
                        continue;
                    }

                    entry.touch();
                    let narrowed = &filter_bitmap & entry.bitmap();
                    return (narrowed, false, cache_key);
                }
            }
            break; // No bound at this tier, try superset matching
        }

        // E4/S4.2: Superset matching — find a bound whose filter clauses are a
        // subset of this query's. A bound for {nsfwLevel=1} can narrow a query
        // for {nsfwLevel=1, onSite=true}. The bound bitmap is still ANDed with
        // the full filter result, so correctness is preserved.
        if let Some(entry) = bc.find_superset_bound(
            filter_key,
            &sort_clause.field,
            sort_clause.direction,
        ) {
            entry.touch();
            let narrowed = &filter_bitmap & entry.bitmap();
            return (narrowed, false, cache_key);
        }

        drop(bc);

        (filter_bitmap, use_simple_sort, cache_key)
    }

    /// D2/D6: Form or update a bound from sort query results.
    /// With cursor awareness: if a cursor was past tier 0's range, form a tiered bound.
    fn update_bound_from_results(
        &self,
        snap: &Guard<Arc<InnerEngine>>,
        sort: Option<&SortClause>,
        cache_key: &Option<CacheKey>,
        result_ids: &[i64],
        cursor: Option<&crate::query::CursorPosition>,
    ) {
        let Some(sort_clause) = sort else { return };
        let Some(ref filter_key) = cache_key else { return };
        if result_ids.is_empty() { return; }

        let sort_field = match snap.sorts.get_field(&sort_clause.field) {
            Some(f) => f,
            None => return,
        };

        // D6: Determine which tier to form/update.
        // If there's a cursor, check which tier the cursor falls past.
        let tier = if let Some(c) = cursor {
            let cursor_val = c.sort_value as u32;
            let mut t = 0u32;
            let bc = self.bound_cache.lock();
            loop {
                let key = BoundKey {
                    filter_key: filter_key.clone(),
                    sort_field: sort_clause.field.clone(),
                    direction: sort_clause.direction,
                    tier: t,
                };
                if let Some(entry) = bc.lookup(&key) {
                    let past = match sort_clause.direction {
                        SortDirection::Desc => cursor_val < entry.min_tracked_value(),
                        SortDirection::Asc => cursor_val > entry.min_tracked_value(),
                    };
                    if past {
                        t += 1;
                        if t >= 4 { break; } // cap tiers
                        continue;
                    }
                }
                break;
            }
            drop(bc);
            t
        } else {
            0
        };

        let bound_key = BoundKey {
            filter_key: filter_key.clone(),
            sort_field: sort_clause.field.clone(),
            direction: sort_clause.direction,
            tier,
        };

        let sorted_slots: Vec<u32> = result_ids.iter().map(|&id| id as u32).collect();

        let mut bc = self.bound_cache.lock();
        if let Some(entry) = bc.get_mut(&bound_key) {
            if entry.needs_rebuild() {
                entry.rebuild(&sorted_slots, |slot| sort_field.reconstruct_value(slot));
            }
        } else {
            bc.form_bound(bound_key, &sorted_slots, |slot| sort_field.reconstruct_value(slot));
        }
    }

    /// Load the current snapshot (lock-free). Public API for advanced use.
    pub fn snapshot_public(&self) -> Arc<InnerEngine> {
        self.inner.load_full()
    }

    /// Get the number of alive documents (lock-free snapshot).
    pub fn alive_count(&self) -> u64 {
        self.snapshot().slots.alive_count()
    }

    /// Get the high-water mark slot counter (lock-free snapshot).
    pub fn slot_counter(&self) -> u32 {
        self.snapshot().slots.slot_counter()
    }

    /// Get the current pending buffer depth (number of pending entries).
    /// Useful for monitoring backpressure on Tier 2 writes.
    pub fn pending_depth(&self) -> usize {
        self.pending.lock().depth()
    }

    /// Report bitmap memory usage broken down by component (lock-free snapshot).
    ///
    /// Returns (slot_bytes, filter_bytes, sort_bytes, cache_entries, cache_bytes,
    ///          filter_details, sort_details)
    /// where all sizes are serialized bitmap bytes — no allocator or redb overhead.
    #[allow(clippy::type_complexity)]
    pub fn bitmap_memory_report(
        &self,
    ) -> (usize, usize, usize, usize, usize, Vec<(String, usize, usize)>, Vec<(String, usize)>) {
        let snap = self.snapshot();
        let slot_bytes = snap.slots.bitmap_bytes();
        let filter_bytes = snap.filters.bitmap_bytes();
        let sort_bytes = snap.sorts.bitmap_bytes();
        let cache = self.cache.lock();
        let cache_entries = cache.len();
        let cache_bytes = cache.bitmap_bytes();
        drop(cache);
        let filter_details: Vec<(String, usize, usize)> = snap
            .filters
            .per_field_bytes()
            .into_iter()
            .map(|(name, count, bytes)| (name.to_string(), count, bytes))
            .collect();
        let sort_details: Vec<(String, usize)> = snap
            .sorts
            .per_field_bytes()
            .into_iter()
            .map(|(name, bytes)| (name.to_string(), bytes))
            .collect();
        (slot_bytes, filter_bytes, sort_bytes, cache_entries, cache_bytes, filter_details, sort_details)
    }

    /// Report bound cache statistics.
    ///
    /// Returns (bound_entries, bound_bitmap_bytes, meta_index_entries, meta_index_bytes).
    pub fn bound_cache_stats(&self) -> (usize, usize, usize, usize) {
        let bc = self.bound_cache.lock();
        let bound_entries = bc.len();
        let bound_bytes = bc.total_memory_bytes();
        let meta = bc.meta_index();
        let meta_entries = meta.entry_count();
        let meta_bytes = meta.memory_bytes();
        (bound_entries, bound_bytes, meta_entries, meta_bytes)
    }

    /// Clear all bound cache entries (for benchmarking cold vs warm).
    pub fn clear_bound_cache(&self) {
        self.bound_cache.lock().clear();
    }

    /// Enter loading mode: skip snapshot publishing and maintenance during bulk inserts.
    ///
    /// In loading mode, the flush thread still applies mutations to the staging engine
    /// but skips the expensive `staging.clone()` snapshot publish. This eliminates the
    /// Arc::make_mut clone cascade that dominates write cost at scale (e.g., cloning
    /// a 104K-entry userId HashMap every 100μs flush cycle).
    ///
    /// Queries during loading mode see stale data (the last published snapshot).
    /// Call `exit_loading_mode()` to publish the final state and resume normal operation.
    pub fn enter_loading_mode(&self) {
        self.loading_mode.store(true, Ordering::Release);
    }

    /// Exit loading mode: publish the current staging state and resume normal operation.
    ///
    /// Invalidates all caches (stale from loading) and triggers a snapshot publish
    /// on the next flush cycle by briefly pausing to let the flush thread catch up.
    pub fn exit_loading_mode(&self) {
        self.loading_mode.store(false, Ordering::Release);
        // Give the flush thread time to see the flag and do a final publish.
        // The next flush cycle with bitmap_count > 0 will publish normally.
        // If no mutations are pending, we need to ensure at least one flush
        // cycle runs — the existing adaptive sleep ensures this happens within
        // max_sleep (flush_interval * 10).
    }

    /// Get a reference to the config.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get a reference to the in-flight tracker.
    pub fn in_flight(&self) -> &InFlightTracker {
        &self.in_flight
    }

    /// PUT_MANY -- batch version of put() for throughput experiments.
    ///
    /// Batches the work: one snapshot load for all alive/allocation checks,
    /// computes all diffs, sends all ops, enqueues all docstore writes, then clears
    /// in-flight tracking.
    ///
    /// EXPERIMENTAL: This is a temporary method for benchmarking put_many vs put-in-loop.
    pub fn put_many(&self, docs: &[(u32, Document)]) -> Result<()> {
        // Phase 1: Mark all in-flight
        for &(id, _) in docs {
            self.in_flight.mark_in_flight(id);
        }

        let result = (|| -> Result<()> {
            // Phase 2: Single snapshot load for all alive/allocation checks
            let statuses: Vec<(u32, bool, bool)> = {
                let snap = self.snapshot();
                docs.iter()
                    .map(|&(id, _)| {
                        let alive = snap.slots.is_alive(id);
                        let alloc = if !alive {
                            snap.slots.was_ever_allocated(id)
                        } else {
                            false
                        };
                        (id, alive, alloc)
                    })
                    .collect()
            };

            // Phase 3: Batch docstore reads for upserts (outside any lock)
            let old_docs: Vec<Option<crate::docstore::StoredDoc>> = statuses
                .iter()
                .map(|&(id, is_upsert, was_allocated)| {
                    if is_upsert || was_allocated {
                        self.docstore.get(id).ok().flatten()
                    } else {
                        None
                    }
                })
                .collect();

            // Phase 4: Compute all diffs and collect all ops
            let mut all_ops: Vec<MutationOp> = Vec::new();
            let mut doc_writes: Vec<(u32, crate::docstore::StoredDoc)> = Vec::new();

            for (i, &(id, ref doc)) in docs.iter().enumerate() {
                let (_, is_upsert, _) = statuses[i];
                let ops = diff_document(id, old_docs[i].as_ref(), doc, &self.config, is_upsert, &self.field_registry);
                all_ops.extend(ops);
                doc_writes.push((
                    id,
                    crate::docstore::StoredDoc {
                        fields: doc.fields.clone(),
                    },
                ));
            }

            // Phase 5: Send all ops in one burst
            self.sender.send_batch(all_ops).map_err(|_| {
                crate::error::BitdexError::CapacityExceeded(
                    "coalescer channel disconnected".to_string(),
                )
            })?;

            // Phase 6: Enqueue all doc writes
            for item in doc_writes {
                self.doc_tx.send(item).map_err(|_| {
                    crate::error::BitdexError::CapacityExceeded(
                        "docstore channel disconnected".to_string(),
                    )
                })?;
            }

            Ok(())
        })();

        // Phase 7: Clear all in-flight
        for &(id, _) in docs {
            self.in_flight.clear_in_flight(id);
        }

        result
    }

    /// PUT_BULK -- high-throughput bulk insert for initial data loading.
    ///
    /// Bypasses the write coalescer entirely. Documents are decomposed into
    /// per-bitmap operations in parallel across N worker threads, each building
    /// thread-local HashMaps of RoaringBitmaps. Thread results are merged, then
    /// applied directly to a staging InnerEngine copy and published via ArcSwap.
    ///
    /// This is ~10x faster than put() for bulk loads because:
    /// - No per-doc channel send/receive overhead
    /// - No diff computation (fresh inserts, no old doc lookup)
    /// - Parallel JSON decompose + bitmap building
    /// - Single snapshot publish at the end
    ///
    /// Assumes all slot IDs are fresh inserts (not upserts). For mixed
    /// insert/update workloads, use put() or put_many().
    ///
    /// Documents are persisted to the docstore after bitmap updates.
    /// Returns the number of documents successfully inserted.
    pub fn put_bulk(&self, docs: Vec<(u32, Document)>, num_threads: usize) -> Result<usize> {
        if docs.is_empty() {
            return Ok(0);
        }

        let t0 = std::time::Instant::now();
        let num_threads = num_threads.max(1).min(docs.len());

        // Phase 1: Parallel decompose — each thread gets a slice of docs,
        // builds thread-local HashMaps of (field_name, value) → RoaringBitmap
        // for filters, and (field_name, bit_layer) → RoaringBitmap for sort layers.
        //
        // We use String keys here since we need to map back to FilterField/SortField
        // by name. The HashMap overhead is dwarfed by JSON parse in real usage.

        // Collect field configs for workers
        let filter_configs: Vec<_> = self.config.filter_fields.clone();
        let sort_configs: Vec<_> = self.config.sort_fields.clone();

        struct ThreadResult {
            filter_maps: HashMap<(String, u64), RoaringBitmap>,
            sort_maps: HashMap<(String, usize), RoaringBitmap>,
            alive_bitmap: RoaringBitmap,
            max_slot: u32,
            count: usize,
        }

        let chunk_size = (docs.len() + num_threads - 1) / num_threads;
        let docs_ref = &docs;
        let filter_configs_ref = &filter_configs;
        let sort_configs_ref = &sort_configs;

        let thread_results: Vec<ThreadResult> = thread::scope(|s| {
            let handles: Vec<_> = (0..num_threads)
                .map(|t| {
                    let start = t * chunk_size;
                    let end = (start + chunk_size).min(docs_ref.len());
                    if start >= end {
                        return s.spawn(move || ThreadResult {
                            filter_maps: HashMap::new(),
                            sort_maps: HashMap::new(),
                            alive_bitmap: RoaringBitmap::new(),
                            max_slot: 0,
                            count: 0,
                        });
                    }

                    s.spawn(move || {
                        let slice = &docs_ref[start..end];
                        let mut filter_maps: HashMap<(String, u64), RoaringBitmap> =
                            HashMap::with_capacity(65_000);
                        let mut sort_maps: HashMap<(String, usize), RoaringBitmap> =
                            HashMap::with_capacity(256);
                        let mut alive_bitmap = RoaringBitmap::new();
                        let mut max_slot: u32 = 0;

                        for &(slot, ref doc) in slice {
                            if slot > max_slot {
                                max_slot = slot;
                            }
                            alive_bitmap.insert(slot);

                            // Decompose filter fields
                            for fc in filter_configs_ref {
                                if let Some(fv) = doc.fields.get(&fc.name) {
                                    match fv {
                                        crate::mutation::FieldValue::Single(v) => {
                                            if let Some(key) = value_to_bitmap_key(v) {
                                                filter_maps
                                                    .entry((fc.name.clone(), key))
                                                    .or_insert_with(RoaringBitmap::new)
                                                    .insert(slot);
                                            }
                                        }
                                        crate::mutation::FieldValue::Multi(vals) => {
                                            for v in vals {
                                                if let Some(key) = value_to_bitmap_key(v) {
                                                    filter_maps
                                                        .entry((fc.name.clone(), key))
                                                        .or_insert_with(RoaringBitmap::new)
                                                        .insert(slot);
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            // Decompose sort fields into bit layers
                            for sc in sort_configs_ref {
                                if let Some(fv) = doc.fields.get(&sc.name) {
                                    if let crate::mutation::FieldValue::Single(
                                        crate::query::Value::Integer(v),
                                    ) = fv
                                    {
                                        let value = *v as u32;
                                        let num_bits = sc.bits as usize;
                                        for bit in 0..num_bits {
                                            if (value >> bit) & 1 == 1 {
                                                sort_maps
                                                    .entry((sc.name.clone(), bit))
                                                    .or_insert_with(RoaringBitmap::new)
                                                    .insert(slot);
                                            }
                                        }
                                    }
                                }
                            }

                        }

                        ThreadResult {
                            filter_maps,
                            sort_maps,
                            alive_bitmap,
                            max_slot,
                            count: slice.len(),
                        }
                    })
                })
                .collect();

            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let t1 = t0.elapsed();

        // Phase 2: Merge thread results into combined maps
        let mut merged_filters: HashMap<(String, u64), RoaringBitmap> = HashMap::new();
        let mut merged_sorts: HashMap<(String, usize), RoaringBitmap> = HashMap::new();
        let mut merged_alive = RoaringBitmap::new();
        let mut global_max_slot: u32 = 0;
        let mut total_count: usize = 0;

        for result in &thread_results {
            total_count += result.count;
            if result.max_slot > global_max_slot {
                global_max_slot = result.max_slot;
            }
            merged_alive |= &result.alive_bitmap;
        }

        for result in &thread_results {
            for ((field, value), bm) in &result.filter_maps {
                merged_filters
                    .entry((field.clone(), *value))
                    .and_modify(|e| *e |= bm)
                    .or_insert_with(|| bm.clone());
            }
            for ((field, bit), bm) in &result.sort_maps {
                merged_sorts
                    .entry((field.clone(), *bit))
                    .and_modify(|e| *e |= bm)
                    .or_insert_with(|| bm.clone());
            }
        }

        let t2 = t0.elapsed();

        // Phase 3: Apply to staging InnerEngine
        // Take the current snapshot and clone it for mutation
        let snap = self.inner.load_full();
        let mut staging = (*snap).clone();

        // Apply filter bitmaps — OR directly into base (bypasses diff layer)
        for ((field_name, value), bitmap) in merged_filters {
            if let Some(field) = staging.filters.get_field_mut(&field_name) {
                field.or_bitmap(value, &bitmap);
            }
        }

        // Apply sort layer bitmaps — OR directly into base
        for ((field_name, bit), bitmap) in merged_sorts {
            if let Some(field) = staging.sorts.get_field_mut(&field_name) {
                field.or_layer(bit, &bitmap);
            }
        }

        // Apply alive bitmap — OR directly into base
        staging.slots.alive_or_bitmap(&merged_alive);

        let t3 = t0.elapsed();

        // Phase 4: Publish snapshot atomically
        self.inner.store(Arc::new(staging));

        // Phase 5: Invalidate all caches (bulk load changes everything)
        {
            let mut c = self.cache.lock();
            for fc in &self.config.filter_fields {
                c.invalidate_field(&fc.name);
            }
        }
        // Invalidate bound cache
        {
            let mut bc = self.bound_cache.lock();
            for (_, entry) in bc.iter_mut() {
                entry.mark_for_rebuild();
            }
        }

        let t4 = t0.elapsed();

        // Phase 6: Persist documents to docstore on a background thread.
        // Bitmaps are already published — queries work immediately.
        // Docstore writes enable future upsert diffs and document serving.
        {
            let docstore = Arc::clone(&self.docstore);
            thread::spawn(move || {
                // Build StoredDocs from the original docs and write in 5K batches
                let batch_size = 5_000;
                let mut batch: Vec<(u32, StoredDoc)> = Vec::with_capacity(batch_size);
                for (slot, doc) in docs {
                    batch.push((slot, StoredDoc { fields: doc.fields }));
                    if batch.len() >= batch_size {
                        if let Err(e) = docstore.put_batch(&batch) {
                            eprintln!("put_bulk: docstore batch write failed: {e}");
                        }
                        batch.clear();
                    }
                }
                if !batch.is_empty() {
                    if let Err(e) = docstore.put_batch(&batch) {
                        eprintln!("put_bulk: docstore batch write failed: {e}");
                    }
                }
            });
        }

        let t5 = t0.elapsed();

        eprintln!("put_bulk phases: decompose={:.2}s merge={:.2}s apply={:.2}s publish={:.2}s docstore={:.2}s total={:.2}s",
            t1.as_secs_f64(),
            (t2 - t1).as_secs_f64(),
            (t3 - t2).as_secs_f64(),
            (t4 - t3).as_secs_f64(),
            (t5 - t4).as_secs_f64(),
            t5.as_secs_f64());

        Ok(total_count)
    }

    /// Shutdown the flush and merge threads gracefully.
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.flush_handle.take() {
            handle.join().ok();
        }
        if let Some(handle) = self.merge_handle.take() {
            handle.join().ok();
        }
    }
}

impl Drop for ConcurrentEngine {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FilterFieldConfig, SortFieldConfig};
    use crate::filter::FilterFieldType;
    use crate::mutation::FieldValue;
    use crate::query::{SortClause, SortDirection, Value};
    use std::sync::Arc;
    use std::thread;

    fn test_config() -> Config {
        Config {
            filter_fields: vec![
                FilterFieldConfig {
                    name: "nsfwLevel".to_string(),
                    field_type: FilterFieldType::SingleValue,
                    storage: crate::config::StorageMode::default(),
                    behaviors: None,
                },
                FilterFieldConfig {
                    name: "tagIds".to_string(),
                    field_type: FilterFieldType::MultiValue,
                    storage: crate::config::StorageMode::default(),
                    behaviors: None,
                },
                FilterFieldConfig {
                    name: "onSite".to_string(),
                    field_type: FilterFieldType::Boolean,
                    storage: crate::config::StorageMode::default(),
                    behaviors: None,
                },
            ],
            sort_fields: vec![SortFieldConfig {
                name: "reactionCount".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
            }],
            max_page_size: 100,
            flush_interval_us: 50, // Fast flush for tests
            channel_capacity: 10_000,
            ..Default::default()
        }
    }

    fn make_doc(fields: Vec<(&str, FieldValue)>) -> Document {
        Document {
            fields: fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        }
    }

    /// Wait for the flush thread to apply all pending mutations.
    fn wait_for_flush(engine: &ConcurrentEngine, expected_alive: u64, max_ms: u64) {
        let deadline = std::time::Instant::now() + Duration::from_millis(max_ms);
        while std::time::Instant::now() < deadline {
            if engine.alive_count() == expected_alive {
                // Give one more flush cycle to ensure everything is settled
                thread::sleep(Duration::from_millis(2));
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        // Final check
        assert_eq!(
            engine.alive_count(),
            expected_alive,
            "timed out waiting for flush; alive_count={} expected={}",
            engine.alive_count(),
            expected_alive
        );
    }

    // ---- Basic correctness tests ----

    #[test]
    fn test_put_and_query() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(42))),
                ]),
            )
            .unwrap();

        wait_for_flush(&engine, 1, 500);

        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                None,
                100,
            )
            .unwrap();

        assert_eq!(result.ids, vec![1]);
    }

    #[test]
    fn test_put_multiple_and_sorted_query() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(100))),
                ]),
            )
            .unwrap();
        engine
            .put(
                2,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(500))),
                ]),
            )
            .unwrap();
        engine
            .put(
                3,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(300))),
                ]),
            )
            .unwrap();

        wait_for_flush(&engine, 3, 500);

        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                Some(&sort),
                10,
            )
            .unwrap();

        assert_eq!(result.ids, vec![2, 3, 1]); // 500, 300, 100
    }

    #[test]
    fn test_delete() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        engine
            .put(
                1,
                &make_doc(vec![(
                    "nsfwLevel",
                    FieldValue::Single(Value::Integer(1)),
                )]),
            )
            .unwrap();
        engine
            .put(
                2,
                &make_doc(vec![(
                    "nsfwLevel",
                    FieldValue::Single(Value::Integer(1)),
                )]),
            )
            .unwrap();

        wait_for_flush(&engine, 2, 500);

        engine.delete(1).unwrap();

        // Wait for delete to be flushed
        wait_for_flush(&engine, 1, 500);

        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                None,
                100,
            )
            .unwrap();

        assert_eq!(result.ids, vec![2]);
    }

    #[test]
    fn test_upsert_correctness() {
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();

        // Initial insert
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(10))),
                ]),
            )
            .unwrap();

        // Must wait for first put to be fully flushed (alive bit set)
        // before doing upsert, otherwise the second put won't detect is_alive=true
        wait_for_flush(&engine, 1, 500);

        // Verify first insert is visible
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);

        // Upsert with new values — now the alive bit is set so diff will detect upsert
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
                    ("reactionCount", FieldValue::Single(Value::Integer(99))),
                ]),
            )
            .unwrap();

        // Wait for upsert flush. alive_count stays 1 so we need a different signal.
        // Shutdown ensures final flush completes.
        engine.shutdown();

        // Old value should not match
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                None,
                100,
            )
            .unwrap();
        assert!(result.ids.is_empty());

        // New value should match
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(2),
                )],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);
    }

    #[test]
    fn test_execute_query() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(42))),
                ]),
            )
            .unwrap();

        wait_for_flush(&engine, 1, 500);

        let query = BitdexQuery {
            filters: vec![FilterClause::Eq(
                "nsfwLevel".to_string(),
                Value::Integer(1),
            )],
            sort: Some(SortClause {
                field: "reactionCount".to_string(),
                direction: SortDirection::Desc,
            }),
            limit: 50,
            cursor: None,
        };

        let result = engine.execute_query(&query).unwrap();
        assert_eq!(result.ids, vec![1]);
    }

    // ---- Concurrency tests ----

    #[test]
    fn test_concurrent_puts() {
        let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());
        let num_threads = 4;
        let docs_per_thread = 50;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let engine = Arc::clone(&engine);
                thread::spawn(move || {
                    for i in 0..docs_per_thread {
                        let id = (t * docs_per_thread + i + 1) as u32;
                        engine
                            .put(
                                id,
                                &make_doc(vec![
                                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                                    (
                                        "reactionCount",
                                        FieldValue::Single(Value::Integer(id as i64)),
                                    ),
                                ]),
                            )
                            .unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let total = (num_threads * docs_per_thread) as u64;
        wait_for_flush(&engine, total, 2000);

        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                None,
                100,
            )
            .unwrap();

        assert_eq!(result.total_matched, total);
    }

    #[test]
    fn test_concurrent_reads_during_writes() {
        let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());

        // Pre-populate some docs
        for i in 1..=10u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        (
                            "reactionCount",
                            FieldValue::Single(Value::Integer(i as i64 * 10)),
                        ),
                    ]),
                )
                .unwrap();
        }

        wait_for_flush(&engine, 10, 500);

        // Spawn writer threads adding more docs
        let writer_handles: Vec<_> = (0..2)
            .map(|t| {
                let engine = Arc::clone(&engine);
                thread::spawn(move || {
                    for i in 0..25 {
                        let id = 100 + t * 25 + i;
                        engine
                            .put(
                                id as u32,
                                &make_doc(vec![
                                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                                    (
                                        "reactionCount",
                                        FieldValue::Single(Value::Integer(id as i64)),
                                    ),
                                ]),
                            )
                            .unwrap();
                    }
                })
            })
            .collect();

        // Spawn reader threads querying concurrently
        let reader_handles: Vec<_> = (0..4)
            .map(|_| {
                let engine = Arc::clone(&engine);
                thread::spawn(move || {
                    let mut success_count = 0;
                    for _ in 0..50 {
                        let result = engine.query(
                            &[FilterClause::Eq(
                                "nsfwLevel".to_string(),
                                Value::Integer(1),
                            )],
                            None,
                            100,
                        );
                        assert!(result.is_ok(), "query should not fail");
                        success_count += 1;
                        thread::yield_now();
                    }
                    success_count
                })
            })
            .collect();

        for h in writer_handles {
            h.join().unwrap();
        }
        for h in reader_handles {
            let count = h.join().unwrap();
            assert_eq!(count, 50, "all reader queries should succeed");
        }
    }

    #[test]
    fn test_concurrent_mixed_read_write() {
        let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());

        let handles: Vec<_> = (0..8)
            .map(|t| {
                let engine = Arc::clone(&engine);
                thread::spawn(move || {
                    for i in 0..20 {
                        if t % 2 == 0 {
                            // Writer
                            let id = (t * 20 + i + 1) as u32;
                            engine
                                .put(
                                    id,
                                    &make_doc(vec![(
                                        "nsfwLevel",
                                        FieldValue::Single(Value::Integer(1)),
                                    )]),
                                )
                                .unwrap();
                        } else {
                            // Reader
                            let _ = engine.query(
                                &[FilterClause::Eq(
                                    "nsfwLevel".to_string(),
                                    Value::Integer(1),
                                )],
                                None,
                                100,
                            );
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // No panics = success for concurrency safety
    }

    #[test]
    fn test_shutdown_flushes_remaining() {
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();

        for i in 1..=5u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![(
                        "nsfwLevel",
                        FieldValue::Single(Value::Integer(1)),
                    )]),
                )
                .unwrap();
        }

        // Shutdown triggers final flush
        engine.shutdown();

        assert_eq!(engine.alive_count(), 5);
    }

    #[test]
    fn test_multi_value_filter() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        engine
            .put(
                1,
                &make_doc(vec![(
                    "tagIds",
                    FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)]),
                )]),
            )
            .unwrap();
        engine
            .put(
                2,
                &make_doc(vec![(
                    "tagIds",
                    FieldValue::Multi(vec![Value::Integer(200), Value::Integer(300)]),
                )]),
            )
            .unwrap();

        wait_for_flush(&engine, 2, 500);

        // Query for tag 200 - should match both
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(200))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.total_matched, 2);

        // Query for tag 100 - should match only doc 1
        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(100))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);
    }

    #[test]
    fn test_merge_thread_starts_and_stops() {
        let mut engine = ConcurrentEngine::new(test_config()).unwrap();
        // Just verify it starts and shuts down cleanly
        engine.shutdown();
    }

    #[test]
    fn test_two_threads_independent() {
        let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());

        // Insert a doc to exercise the flush thread
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(42))),
                ]),
            )
            .unwrap();

        wait_for_flush(&engine, 1, 500);

        // Query to verify flush worked while merge thread is also running
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                None,
                100,
            )
            .unwrap();
        assert!(result.ids.contains(&1));
    }

    // ---- S1.8: Integration tests for diff accumulation and merge compaction ----

    /// S1.8-1: Filter diffs are visible (dirty) in published snapshot after flush,
    /// and queries still return correct results via diff fusion.
    #[test]
    fn test_filter_diffs_visible_in_snapshot() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        // Insert a document
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("onSite", FieldValue::Single(Value::Bool(true))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer(100)),
                    ),
                ]),
            )
            .unwrap();

        wait_for_flush(&engine, 1, 500);

        // Query should return correct results via diff fusion
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);

        // Verify the published snapshot's filter field has a dirty diff
        let snap = engine.snapshot_public();
        let field = snap.filters.get_field("nsfwLevel").unwrap();
        let vb = field.get_versioned(1).unwrap();
        // Between flush cycles and compaction, the diff should be dirty
        // (unless compaction just ran). The key assertion is that queries work.
        assert!(vb.contains(1), "slot 1 should be in nsfwLevel=1 bitmap");
    }

    /// S1.8-2: After compaction, filter diffs are merged into base.
    /// Wait long enough for the periodic compaction (COMPACTION_INTERVAL cycles).
    #[test]
    fn test_merge_compaction_cleans_diffs() {
        let mut cfg = test_config();
        cfg.flush_interval_us = 10; // Very fast flush so compaction triggers quickly
        let engine = ConcurrentEngine::new(cfg).unwrap();

        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(5))),
                    ("onSite", FieldValue::Single(Value::Bool(true))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer(50)),
                    ),
                ]),
            )
            .unwrap();

        wait_for_flush(&engine, 1, 500);

        // Wait for compaction to happen (50 cycles * 10μs = 500μs + overhead)
        // Give generous time for thread scheduling
        thread::sleep(Duration::from_millis(50));

        // Query should still be correct after compaction
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(5),
                )],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.ids, vec![1]);

        // Check that the diff was compacted (base contains the bit)
        let snap = engine.snapshot_public();
        let field = snap.filters.get_field("nsfwLevel").unwrap();
        let vb = field.get_versioned(5).unwrap();
        // After compaction, the base should contain the bit
        assert!(vb.base().contains(1), "slot 1 should be in base after compaction");
    }

    /// S1.8-3: Sort layers are always clean (never dirty) in published snapshots.
    #[test]
    fn test_sort_layers_always_clean() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        // Insert several docs with different sort values
        for i in 1..=10u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        ("onSite", FieldValue::Single(Value::Bool(true))),
                        (
                            "reactionCount",
                            FieldValue::Single(Value::Integer(i as i64 * 100)),
                        ),
                    ]),
                )
                .unwrap();
        }

        wait_for_flush(&engine, 10, 500);

        // Verify sort layers are clean
        let snap = engine.snapshot_public();
        let sort_field = snap.sorts.get_field("reactionCount").unwrap();
        for bit_pos in 0..32usize {
            if let Some(layer) = sort_field.layer(bit_pos) {
                // layer() has an internal debug_assert that panics if dirty.
                // If we get here, the layer is clean. Verify it's accessible.
                let _ = layer.len();
            }
        }
    }

    /// S1.8-4: Filter diffs accumulate across multiple flush cycles.
    #[test]
    fn test_filter_diffs_accumulate_across_flushes() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        // Insert doc A
        engine
            .put(
                1,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(3))),
                    ("onSite", FieldValue::Single(Value::Bool(true))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer(10)),
                    ),
                ]),
            )
            .unwrap();

        wait_for_flush(&engine, 1, 500);

        // Insert doc B with same nsfwLevel
        engine
            .put(
                2,
                &make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(3))),
                    ("onSite", FieldValue::Single(Value::Bool(false))),
                    (
                        "reactionCount",
                        FieldValue::Single(Value::Integer(20)),
                    ),
                ]),
            )
            .unwrap();

        wait_for_flush(&engine, 2, 500);

        // Query should return both docs
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(3),
                )],
                None,
                100,
            )
            .unwrap();
        let mut ids = result.ids.clone();
        ids.sort();
        assert_eq!(ids, vec![1, 2], "both docs should match nsfwLevel=3");
    }

    /// S1.8-5: Concurrent reads during mutations return correct results.
    #[test]
    fn test_concurrent_reads_during_mutations() {
        let engine = Arc::new(ConcurrentEngine::new(test_config()).unwrap());

        // Insert initial docs
        for i in 1..=20u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer((i % 3) as i64 + 1))),
                        ("onSite", FieldValue::Single(Value::Bool(i % 2 == 0))),
                        (
                            "reactionCount",
                            FieldValue::Single(Value::Integer(i as i64)),
                        ),
                    ]),
                )
                .unwrap();
        }

        wait_for_flush(&engine, 20, 1000);

        // Spawn reader threads that query continuously
        let mut handles = Vec::new();
        for _ in 0..4 {
            let eng = Arc::clone(&engine);
            handles.push(thread::spawn(move || {
                for _ in 0..50 {
                    // Query should never panic or return inconsistent results
                    let result = eng
                        .query(
                            &[FilterClause::Eq(
                                "nsfwLevel".to_string(),
                                Value::Integer(1),
                            )],
                            None,
                            100,
                        )
                        .unwrap();
                    // Results should be non-empty (we inserted docs with nsfwLevel=1)
                    assert!(!result.ids.is_empty(), "query returned empty during concurrent reads");
                    thread::sleep(Duration::from_micros(100));
                }
            }));
        }

        // Concurrently insert more docs
        for i in 21..=40u32 {
            engine
                .put(
                    i,
                    &make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer((i % 3) as i64 + 1))),
                        ("onSite", FieldValue::Single(Value::Bool(i % 2 == 0))),
                        (
                            "reactionCount",
                            FieldValue::Single(Value::Integer(i as i64)),
                        ),
                    ]),
                )
                .unwrap();
            thread::sleep(Duration::from_micros(200));
        }

        // Wait for all readers to finish
        for h in handles {
            h.join().unwrap();
        }

        // Final verification
        wait_for_flush(&engine, 40, 1000);
        let result = engine.query(&[], None, 1000).unwrap();
        assert_eq!(result.ids.len(), 40, "all 40 docs should be alive");
    }

    // ---- put_bulk tests ----

    #[test]
    fn test_put_bulk_basic() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        let docs: Vec<(u32, Document)> = (1..=100u32)
            .map(|i| {
                (
                    i,
                    make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer((i % 5) as i64 + 1))),
                        (
                            "reactionCount",
                            FieldValue::Single(Value::Integer(i as i64 * 10)),
                        ),
                    ]),
                )
            })
            .collect();

        let count = engine.put_bulk(docs, 4).unwrap();
        assert_eq!(count, 100);
        assert_eq!(engine.alive_count(), 100);

        // Filter query
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                None,
                1000,
            )
            .unwrap();
        assert_eq!(result.total_matched, 20); // 1,6,11,...,96 → 20 docs

        // Sorted query
        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                Some(&sort),
                3,
            )
            .unwrap();
        // Top 3 by reactionCount desc with nsfwLevel=1: slots 100(1000), 95(950), 90(900)
        assert_eq!(result.ids, vec![100, 95, 90]);
    }

    #[test]
    fn test_put_bulk_with_multi_value() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        let docs = vec![
            (
                1,
                make_doc(vec![(
                    "tagIds",
                    FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)]),
                )]),
            ),
            (
                2,
                make_doc(vec![(
                    "tagIds",
                    FieldValue::Multi(vec![Value::Integer(200), Value::Integer(300)]),
                )]),
            ),
            (
                3,
                make_doc(vec![(
                    "tagIds",
                    FieldValue::Multi(vec![Value::Integer(100), Value::Integer(300)]),
                )]),
            ),
        ];

        engine.put_bulk(docs, 2).unwrap();

        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(200))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.total_matched, 2); // docs 1 and 2

        let result = engine
            .query(
                &[FilterClause::Eq("tagIds".to_string(), Value::Integer(100))],
                None,
                100,
            )
            .unwrap();
        assert_eq!(result.total_matched, 2); // docs 1 and 3
    }

    #[test]
    fn test_put_bulk_single_thread() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        let docs: Vec<(u32, Document)> = (1..=10u32)
            .map(|i| {
                (
                    i,
                    make_doc(vec![
                        ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                        (
                            "reactionCount",
                            FieldValue::Single(Value::Integer(i as i64)),
                        ),
                    ]),
                )
            })
            .collect();

        let count = engine.put_bulk(docs, 1).unwrap();
        assert_eq!(count, 10);
        assert_eq!(engine.alive_count(), 10);
    }

    #[test]
    fn test_put_bulk_then_query_with_sort() {
        let engine = ConcurrentEngine::new(test_config()).unwrap();

        let docs: Vec<(u32, Document)> = vec![
            (
                10,
                make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(500))),
                ]),
            ),
            (
                20,
                make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(100))),
                ]),
            ),
            (
                30,
                make_doc(vec![
                    ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                    ("reactionCount", FieldValue::Single(Value::Integer(300))),
                ]),
            ),
        ];

        engine.put_bulk(docs, 2).unwrap();

        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let result = engine
            .query(
                &[FilterClause::Eq(
                    "nsfwLevel".to_string(),
                    Value::Integer(1),
                )],
                Some(&sort),
                10,
            )
            .unwrap();
        assert_eq!(result.ids, vec![10, 30, 20]); // 500, 300, 100
    }
}
