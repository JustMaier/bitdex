use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender};
use parking_lot::RwLock;

use crate::cache::TrieCache;
use crate::concurrency::InFlightTracker;
use crate::config::Config;
use crate::docstore::{DocStore, StoredDoc};
use crate::error::Result;
use crate::executor::QueryExecutor;
use crate::mutation::{diff_document, diff_patch, Document, FieldRegistry, PatchPayload};
use crate::query::{BitdexQuery, FilterClause, SortClause};
use crate::types::QueryResult;
use crate::write_coalescer::{MutationOp, MutationSender, WriteCoalescer};

/// Inner bitmap state protected by the RwLock.
/// Separated from ConcurrentEngine so the flush thread can take
/// a write lock on just the bitmap state.
pub struct InnerEngine {
    pub slots: crate::slot::SlotAllocator,
    pub filters: crate::filter::FilterIndex,
    pub sorts: crate::sort::SortIndex,
    pub cache: parking_lot::Mutex<TrieCache>,
}

/// Thread-safe engine wrapper that uses RwLock + WriteCoalescer
/// for concurrent reads and writes.
///
/// Writers call `put`/`patch`/`delete` which compute diffs and send
/// MutationOps to a channel. A background flush thread periodically
/// takes a write lock and applies batched mutations.
///
/// Readers take a shared read lock to execute queries.
pub struct ConcurrentEngine {
    inner: Arc<RwLock<InnerEngine>>,
    sender: MutationSender,
    doc_tx: Sender<(u32, StoredDoc)>,
    docstore: Arc<DocStore>,
    config: Arc<Config>,
    field_registry: FieldRegistry,
    in_flight: InFlightTracker,
    shutdown: Arc<AtomicBool>,
    flush_handle: Option<JoinHandle<()>>,
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

        for fc in &config.filter_fields {
            filters.add_field(fc.clone());
        }
        for sc in &config.sort_fields {
            sorts.add_field(sc.clone());
        }

        let field_registry = FieldRegistry::from_config(&config);
        let cache = TrieCache::new(config.cache.clone());

        let inner = Arc::new(RwLock::new(InnerEngine {
            slots: crate::slot::SlotAllocator::new(),
            filters,
            sorts,
            cache: parking_lot::Mutex::new(cache),
        }));

        let (mut coalescer, sender) = WriteCoalescer::new(config.channel_capacity);
        let shutdown = Arc::new(AtomicBool::new(false));
        let config = Arc::new(config);

        // Docstore write channel — bounded for backpressure, same capacity as mutation channel
        let (doc_tx, doc_rx): (Sender<(u32, StoredDoc)>, Receiver<(u32, StoredDoc)>) =
            crossbeam_channel::bounded(config.channel_capacity);

        let docstore = Arc::new(docstore);

        // Collect filter field names for cache invalidation in the flush thread
        let filter_field_names: Vec<String> = config
            .filter_fields
            .iter()
            .map(|f| f.name.clone())
            .collect();

        let flush_handle = {
            let inner = Arc::clone(&inner);
            let shutdown = Arc::clone(&shutdown);
            let docstore = Arc::clone(&docstore);
            let flush_interval_us = config.flush_interval_us;
            let field_names = filter_field_names;

            thread::spawn(move || {
                let min_sleep = Duration::from_micros(flush_interval_us);
                let max_sleep = Duration::from_micros(flush_interval_us * 10); // 10x base = ~1ms at default
                let mut current_sleep = min_sleep;
                let mut doc_batch: Vec<(u32, StoredDoc)> = Vec::new();

                while !shutdown.load(Ordering::Relaxed) {
                    thread::sleep(current_sleep);

                    // Phase 1: Take write lock, flush bitmap mutations
                    let bitmap_count = {
                        let mut guard = inner.write();
                        let ie = &mut *guard;
                        let count = coalescer.flush(
                            &mut ie.slots,
                            &mut ie.filters,
                            &mut ie.sorts,
                        );

                        if count > 0 {
                            let mut cache = ie.cache.lock();
                            for name in &field_names {
                                cache.invalidate_field(name);
                            }
                        }
                        count
                    };
                    // Write lock released here

                    // Phase 2: Drain docstore channel and batch write OUTSIDE the RwLock
                    // (redb has its own internal locking — no need to hold bitmap lock)
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
                {
                    let mut guard = inner.write();
                    let ie = &mut *guard;
                    let count = coalescer.flush(
                        &mut ie.slots,
                        &mut ie.filters,
                        &mut ie.sorts,
                    );
                    if count > 0 {
                        let mut cache = ie.cache.lock();
                        for name in &field_names {
                            cache.invalidate_field(name);
                        }
                    }
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

        Ok(Self {
            inner,
            sender,
            doc_tx,
            docstore,
            config,
            field_registry,
            in_flight: InFlightTracker::new(),
            shutdown,
            flush_handle: Some(flush_handle),
        })
    }

    /// PUT(id, document) -- full replace with upsert semantics.
    ///
    /// 1. Mark in-flight
    /// 2. Check alive status (read lock)
    /// 3. Read old doc from docstore if upsert
    /// 4. Diff old vs new -> MutationOps
    /// 5. Send ops to coalescer channel
    /// 6. Enqueue doc write to docstore channel (flush thread batches these)
    /// 7. Clear in-flight
    pub fn put(&self, id: u32, doc: &Document) -> Result<()> {
        self.in_flight.mark_in_flight(id);

        let result = (|| -> Result<()> {
            // Check alive status and allocation in a single read lock
            let (is_upsert, was_allocated) = {
                let guard = self.inner.read();
                let alive = guard.slots.is_alive(id);
                let alloc = if !alive {
                    guard.slots.was_ever_allocated(id)
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

            // Enqueue doc write — flush thread will batch these into a single redb transaction
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
            // Verify the slot is alive
            {
                let guard = self.inner.read();
                if !guard.slots.is_alive(id) {
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

    /// Execute a query from individual filter/sort/limit components.
    pub fn query(
        &self,
        filters: &[FilterClause],
        sort: Option<&SortClause>,
        limit: usize,
    ) -> Result<QueryResult> {
        let guard = self.inner.read();
        let executor = QueryExecutor::new(
            &guard.slots,
            &guard.filters,
            &guard.sorts,
            self.config.max_page_size,
        );
        let mut result = executor.execute_with_cache(
            filters,
            sort,
            limit,
            None,
            &mut guard.cache.lock(),
        )?;

        // Post-validation against in-flight writes
        self.post_validate(&mut result, filters, &executor)?;

        Ok(result)
    }

    /// Execute a parsed BitdexQuery.
    pub fn execute_query(&self, query: &BitdexQuery) -> Result<QueryResult> {
        let guard = self.inner.read();
        let executor = QueryExecutor::new(
            &guard.slots,
            &guard.filters,
            &guard.sorts,
            self.config.max_page_size,
        );
        let mut result = executor.execute_with_cache(
            &query.filters,
            query.sort.as_ref(),
            query.limit,
            query.cursor.as_ref(),
            &mut guard.cache.lock(),
        )?;

        self.post_validate(&mut result, &query.filters, &executor)?;

        Ok(result)
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

        // The executor holds references to the bitmap state (still under read lock)
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

    /// Get a reference to the inner engine (for advanced use cases).
    pub fn inner(&self) -> &Arc<RwLock<InnerEngine>> {
        &self.inner
    }

    /// Get the number of alive documents (requires read lock).
    pub fn alive_count(&self) -> u64 {
        self.inner.read().slots.alive_count()
    }

    /// Get the high-water mark slot counter (requires read lock).
    pub fn slot_counter(&self) -> u32 {
        self.inner.read().slots.slot_counter()
    }

    /// Report bitmap memory usage broken down by component (requires read lock).
    ///
    /// Returns (slot_bytes, filter_bytes, sort_bytes, filter_details, sort_details)
    /// where all sizes are serialized bitmap bytes — no allocator or redb overhead.
    pub fn bitmap_memory_report(
        &self,
    ) -> (usize, usize, usize, Vec<(String, usize, usize)>, Vec<(String, usize)>) {
        let guard = self.inner.read();
        let slot_bytes = guard.slots.bitmap_bytes();
        let filter_bytes = guard.filters.bitmap_bytes();
        let sort_bytes = guard.sorts.bitmap_bytes();
        let filter_details: Vec<(String, usize, usize)> = guard
            .filters
            .per_field_bytes()
            .into_iter()
            .map(|(name, count, bytes)| (name.to_string(), count, bytes))
            .collect();
        let sort_details: Vec<(String, usize)> = guard
            .sorts
            .per_field_bytes()
            .into_iter()
            .map(|(name, bytes)| (name.to_string(), bytes))
            .collect();
        (slot_bytes, filter_bytes, sort_bytes, filter_details, sort_details)
    }

    /// Get a reference to the config.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get a reference to the in-flight tracker.
    pub fn in_flight(&self) -> &InFlightTracker {
        &self.in_flight
    }

    /// Shutdown the flush thread gracefully.
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.flush_handle.take() {
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
                },
                FilterFieldConfig {
                    name: "tagIds".to_string(),
                    field_type: FilterFieldType::MultiValue,
                },
                FilterFieldConfig {
                    name: "onSite".to_string(),
                    field_type: FilterFieldType::Boolean,
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
}
