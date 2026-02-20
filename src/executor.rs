use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::bitmap_store::BitmapStore;
use crate::bound_cache::{BoundCacheManager, BoundKey};
use crate::cache::{self, CacheLookup, CacheKey, TrieCache};
use crate::error::{BitdexError, Result};
use crate::filter::FilterIndex;
use crate::pending_buffer::PendingBuffer;
use crate::planner;
use crate::query::{FilterClause, SortClause, SortDirection, Value};
use crate::slot::SlotAllocator;
use crate::sort::SortIndex;
use crate::tier2_cache::Tier2Cache;
use crate::types::QueryResult;

/// Convert a Value to a u64 bitmap key for filter indexing.
fn value_to_bitmap_key(val: &Value) -> Option<u64> {
    match val {
        Value::Bool(b) => Some(if *b { 1 } else { 0 }),
        Value::Integer(v) => Some(*v as u64),
        Value::Float(_) | Value::String(_) => None,
    }
}

/// Provides Tier 2 bitmap resolution: moka cache + pending buffer + redb store.
///
/// Passed to the executor so it can resolve filter clauses for high-cardinality
/// fields (tagIds, userId) that are not stored in the in-memory snapshot.
pub struct Tier2Resolver {
    pub cache: Arc<Tier2Cache>,
    pub pending: Arc<parking_lot::Mutex<PendingBuffer>>,
    pub store: Arc<BitmapStore>,
}

/// Query executor: computes filter intersections and sort traversals.
/// Uses the query planner for cardinality-based clause ordering.
pub struct QueryExecutor<'a> {
    slots: &'a SlotAllocator,
    filters: &'a FilterIndex,
    sorts: &'a SortIndex,
    max_page_size: usize,
    tier2: Option<&'a Tier2Resolver>,
    time_buckets: Option<&'a crate::time_buckets::TimeBucketManager>,
    now_unix: u64,
    bound_cache: Option<&'a parking_lot::Mutex<BoundCacheManager>>,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(
        slots: &'a SlotAllocator,
        filters: &'a FilterIndex,
        sorts: &'a SortIndex,
        max_page_size: usize,
    ) -> Self {
        Self {
            slots,
            filters,
            sorts,
            max_page_size,
            tier2: None,
            time_buckets: None,
            now_unix: 0,
            bound_cache: None,
        }
    }

    pub fn with_tier2(
        slots: &'a SlotAllocator,
        filters: &'a FilterIndex,
        sorts: &'a SortIndex,
        max_page_size: usize,
        tier2: &'a Tier2Resolver,
    ) -> Self {
        Self {
            slots,
            filters,
            sorts,
            max_page_size,
            tier2: Some(tier2),
            time_buckets: None,
            now_unix: 0,
            bound_cache: None,
        }
    }

    /// Attach a bound cache for sort working set reduction (D5).
    /// Bounds shrink the candidate set before sort traversal.
    pub fn with_bound_cache(mut self, bc: &'a parking_lot::Mutex<BoundCacheManager>) -> Self {
        self.bound_cache = Some(bc);
        self
    }

    /// Attach a time bucket manager for in-executor bucket snapping (C3).
    /// Range filters on the bucketed field will be snapped to pre-computed bitmaps.
    pub fn with_time_buckets(mut self, tb: &'a crate::time_buckets::TimeBucketManager, now: u64) -> Self {
        self.time_buckets = Some(tb);
        self.now_unix = now;
        self
    }

    /// Get a reference to the filter index.
    pub fn filter_index(&self) -> &'a FilterIndex {
        self.filters
    }

    /// Get a reference to the slot allocator.
    pub fn slot_allocator(&self) -> &'a SlotAllocator {
        self.slots
    }

    /// Execute a full query: plan -> filter -> sort -> paginate -> return IDs.
    pub fn execute(
        &self,
        filters: &[FilterClause],
        sort: Option<&SortClause>,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
    ) -> Result<QueryResult> {
        let limit = limit.min(self.max_page_size);

        // Step 1: Plan the query (reorder clauses by cardinality)
        let plan = planner::plan_query(filters, self.filters, self.slots, sort.is_some());

        // Step 2: Compute filter bitmap using planned clause order
        let filter_bitmap = self.compute_filters(&plan.ordered_clauses)?;

        // Step 3: AND with alive bitmap (implicit in every query)
        let alive = self.slots.alive_bitmap();
        let candidates = &filter_bitmap & alive;

        let total_matched = candidates.len();

        // Step 4: Sort and paginate
        let (ids, next_cursor) = if let Some(sort_clause) = sort {
            if plan.use_simple_sort {
                self.simple_sort_and_paginate(&candidates, sort_clause, limit, cursor)?
            } else {
                self.sort_and_paginate(&candidates, sort_clause, limit, cursor)?
            }
        } else {
            // No sort: return in descending slot order (newest first)
            self.slot_order_paginate(&candidates, limit, cursor)
        };

        Ok(QueryResult {
            ids,
            cursor: next_cursor,
            total_matched,
        })
    }

    /// Execute a query with trie cache integration.
    /// Checks cache first, falls back to computation, stores results.
    pub fn execute_with_cache(
        &self,
        filters: &[FilterClause],
        sort: Option<&SortClause>,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
        cache: &mut TrieCache,
    ) -> Result<QueryResult> {
        let limit = limit.min(self.max_page_size);

        // Step 1: Plan the query (reorder clauses by cardinality)
        let plan = planner::plan_query(filters, self.filters, self.slots, sort.is_some());

        // Step 2: Try cache lookup using canonical key (C5: use bucket-stable keys when available)
        let cache_key = if let Some(tb) = self.time_buckets {
            cache::canonicalize_with_buckets(&plan.ordered_clauses, Some(tb), self.now_unix)
        } else {
            cache::canonicalize(&plan.ordered_clauses)
        };

        let filter_bitmap = if let Some(ref key) = cache_key {
            match cache.lookup(key) {
                CacheLookup::ExactHit(cached_bitmap) => {
                    // Cache hit -- use cached bitmap directly
                    cached_bitmap
                }
                CacheLookup::PrefixHit { bitmap: prefix_bitmap, matched_prefix_len } => {
                    // Partial cache hit -- compute remaining clauses against cached prefix
                    let remaining = &plan.ordered_clauses[matched_prefix_len..];
                    let mut result = prefix_bitmap;
                    for clause in remaining {
                        let clause_bitmap = self.evaluate_clause(clause)?;
                        result &= &clause_bitmap;
                    }
                    // Store the full result in cache
                    cache.store(key, result.clone());
                    result
                }
                CacheLookup::Miss => {
                    // Full miss -- compute from scratch
                    let result = self.compute_filters(&plan.ordered_clauses)?;
                    cache.store(key, result.clone());
                    result
                }
            }
        } else {
            // Uncacheable query (contains compound clauses) -- compute without cache
            self.compute_filters(&plan.ordered_clauses)?
        };

        // Step 3: AND with alive bitmap (implicit in every query)
        let alive = self.slots.alive_bitmap();
        let candidates = &filter_bitmap & alive;

        let total_matched = candidates.len();

        // Step 4: Sort and paginate — with optional bound cache (D5)
        let (ids, next_cursor) = if let Some(sort_clause) = sort {
            // D5: Try to narrow working set with bound cache before sort traversal
            let (sort_candidates, did_use_bound) = self.apply_bound_if_available(
                &candidates,
                &cache_key,
                sort_clause,
                cursor,
            );

            let (result_ids, result_cursor) = if plan.use_simple_sort && !did_use_bound {
                self.simple_sort_and_paginate(&sort_candidates, sort_clause, limit, cursor)?
            } else {
                self.sort_and_paginate(&sort_candidates, sort_clause, limit, cursor)?
            };

            // D2: Form or update bound from sort results (promotion_threshold = 0)
            if let (Some(bc), Some(ref key)) = (&self.bound_cache, &cache_key) {
                self.form_or_update_bound(bc, key, sort_clause, &result_ids);
            }

            (result_ids, result_cursor)
        } else {
            // No sort: return in descending slot order (newest first)
            self.slot_order_paginate(&candidates, limit, cursor)
        };

        Ok(QueryResult {
            ids,
            cursor: next_cursor,
            total_matched,
        })
    }

    /// D5: Check bound cache and narrow candidates if a matching bound exists.
    /// Returns (narrowed_candidates, did_use_bound).
    fn apply_bound_if_available(
        &self,
        candidates: &RoaringBitmap,
        cache_key: &Option<CacheKey>,
        sort_clause: &SortClause,
        cursor: Option<&crate::query::CursorPosition>,
    ) -> (RoaringBitmap, bool) {
        let Some(bc_mutex) = self.bound_cache else {
            return (candidates.clone(), false);
        };
        let Some(ref filter_key) = cache_key else {
            return (candidates.clone(), false);
        };

        let mut try_key = BoundKey {
            filter_key: filter_key.clone(),
            sort_field: sort_clause.field.clone(),
            direction: sort_clause.direction,
            tier: 0,
        };

        let mut bc = bc_mutex.lock();
        let max_tiers = 4u32;
        for _tier in 0..max_tiers {
            let Some(entry) = bc.lookup_mut(&try_key) else {
                break;
            };

            if entry.needs_rebuild() {
                break; // Let query do full traversal and rebuild
            }

            // D6: If cursor is past this tier's range, try next tier
            let cursor_past = if let Some(cursor) = cursor {
                let cursor_val = cursor.sort_value as u32;
                match sort_clause.direction {
                    SortDirection::Desc => cursor_val < entry.min_tracked_value(),
                    SortDirection::Asc => cursor_val > entry.min_tracked_value(),
                }
            } else {
                false
            };

            if cursor_past {
                try_key = BoundKey {
                    filter_key: try_key.filter_key,
                    sort_field: try_key.sort_field,
                    direction: try_key.direction,
                    tier: try_key.tier + 1,
                };
                continue;
            }

            entry.touch();
            let narrowed = candidates & entry.bitmap();
            return (narrowed, true);
        }

        (candidates.clone(), false)
    }

    /// D2: Form or update a bound from sort results.
    /// Called after every sort query with promotion_threshold = 0.
    fn form_or_update_bound(
        &self,
        bc_mutex: &parking_lot::Mutex<BoundCacheManager>,
        filter_key: &CacheKey,
        sort_clause: &SortClause,
        result_ids: &[i64],
    ) {
        let bound_key = BoundKey {
            filter_key: filter_key.clone(),
            sort_field: sort_clause.field.clone(),
            direction: sort_clause.direction,
            tier: 0,
        };

        let sort_field = match self.sorts.get_field(&sort_clause.field) {
            Some(f) => f,
            None => return,
        };

        let sorted_slots: Vec<u32> = result_ids.iter().map(|&id| id as u32).collect();

        let mut bc = bc_mutex.lock();

        if let Some(entry) = bc.get_mut(&bound_key) {
            if entry.needs_rebuild() {
                // Rebuild with fresh results
                entry.rebuild(&sorted_slots, |slot| sort_field.reconstruct_value(slot));
            }
            // Otherwise, bound exists and doesn't need rebuild — leave it alone
        } else {
            // No bound exists — form one
            bc.form_bound(bound_key, &sorted_slots, |slot| sort_field.reconstruct_value(slot));
        }
    }

    /// Check if a single slot matches all the given filter clauses.
    /// Used by post-validation to revalidate slots that overlap with in-flight writes.
    pub fn slot_matches_filters(&self, slot: u32, clauses: &[FilterClause]) -> Result<bool> {
        for clause in clauses {
            let bitmap = self.evaluate_clause(clause)?;
            if !bitmap.contains(slot) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Execute from a pre-computed filter bitmap: alive AND + sort + paginate.
    /// Used when the caller handles cache interaction separately.
    pub fn execute_from_bitmap(
        &self,
        filter_bitmap: RoaringBitmap,
        sort: Option<&SortClause>,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
        use_simple_sort: bool,
    ) -> Result<QueryResult> {
        let limit = limit.min(self.max_page_size);

        // AND with alive bitmap
        let alive = self.slots.alive_bitmap();
        let candidates = &filter_bitmap & alive;
        let total_matched = candidates.len();

        // Sort and paginate
        let (ids, next_cursor) = if let Some(sort_clause) = sort {
            if use_simple_sort {
                self.simple_sort_and_paginate(&candidates, sort_clause, limit, cursor)?
            } else {
                self.sort_and_paginate(&candidates, sort_clause, limit, cursor)?
            }
        } else {
            // No sort: return in descending slot order (newest first)
            self.slot_order_paginate(&candidates, limit, cursor)
        };

        Ok(QueryResult {
            ids,
            cursor: next_cursor,
            total_matched,
        })
    }

    /// Compute the combined filter bitmap from a list of filter clauses.
    /// Top-level clauses are implicitly ANDed together.
    /// Clauses are expected to be pre-ordered by the planner for optimal evaluation.
    pub(crate) fn compute_filters(&self, clauses: &[FilterClause]) -> Result<RoaringBitmap> {
        if clauses.is_empty() {
            return Ok(self.slots.alive_bitmap().clone());
        }

        let mut result: Option<RoaringBitmap> = None;

        for clause in clauses {
            let bitmap = self.evaluate_clause(clause)?;
            result = Some(match result {
                Some(existing) => existing & &bitmap,
                None => bitmap,
            });
        }

        Ok(result.unwrap_or_default())
    }

    /// Evaluate a single filter clause to a bitmap.
    pub(crate) fn evaluate_clause(&self, clause: &FilterClause) -> Result<RoaringBitmap> {
        match clause {
            FilterClause::Eq(field, value) => {
                // Try Tier 1 (snapshot FilterIndex) first
                if let Some(filter_field) = self.filters.get_field(field) {
                    let key = value_to_bitmap_key(value)
                        .ok_or_else(|| BitdexError::InvalidValue {
                            field: field.clone(),
                            reason: "cannot convert to bitmap key".to_string(),
                        })?;
                    return Ok(filter_field.get(key).cloned().unwrap_or_default());
                }
                // Fall back to Tier 2 (moka cache + pending + redb)
                if let Some(tier2) = &self.tier2 {
                    return self.evaluate_tier2_eq(field, value, tier2);
                }
                Err(BitdexError::FieldNotFound(field.clone()))
            }

            FilterClause::NotEq(field, value) => {
                // Use andnot optimization: compute the small negated bitmap
                // and subtract from alive, instead of computing the large complement
                let eq_bitmap = self.evaluate_clause(&FilterClause::Eq(field.clone(), value.clone()))?;
                let alive = self.slots.alive_bitmap();
                let mut result = alive.clone();
                result -= &eq_bitmap;
                Ok(result)
            }

            FilterClause::In(field, values) => {
                // Try Tier 1 first
                if let Some(filter_field) = self.filters.get_field(field) {
                    let keys: Vec<u64> = values
                        .iter()
                        .filter_map(value_to_bitmap_key)
                        .collect();
                    return Ok(filter_field.union(&keys));
                }
                // Fall back to Tier 2
                if let Some(tier2) = &self.tier2 {
                    return self.evaluate_tier2_in(field, values, tier2);
                }
                Err(BitdexError::FieldNotFound(field.clone()))
            }

            FilterClause::Not(inner) => {
                // NOT uses andnot: compute inner bitmap and subtract from alive
                let inner_bitmap = self.evaluate_clause(inner)?;
                let alive = self.slots.alive_bitmap();
                let mut result = alive.clone();
                result -= &inner_bitmap;
                Ok(result)
            }

            FilterClause::And(clauses) => {
                // Optimize And sub-clauses by reordering by cardinality
                let optimized = planner::optimize_and_clause(
                    clauses,
                    self.filters,
                    self.slots.alive_count(),
                );
                let mut result: Option<RoaringBitmap> = None;
                for clause in &optimized {
                    let bitmap = self.evaluate_clause(clause)?;
                    result = Some(match result {
                        Some(existing) => existing & &bitmap,
                        None => bitmap,
                    });
                }
                Ok(result.unwrap_or_default())
            }

            FilterClause::Or(clauses) => {
                let mut result = RoaringBitmap::new();
                for clause in clauses {
                    let bitmap = self.evaluate_clause(clause)?;
                    result |= &bitmap;
                }
                Ok(result)
            }

            // C3: Try bucket snapping for Gt/Gte on timestamp fields before falling back to range_scan.
            FilterClause::Gt(field, value) | FilterClause::Gte(field, value) => {
                if let Some(tb) = &self.time_buckets {
                    if field == tb.field_name() {
                        if let Some(threshold) = value_to_bitmap_key(value) {
                            let duration = self.now_unix.saturating_sub(threshold);
                            if let Some(bucket_name) = tb.snap_duration(duration, 0.10) {
                                if let Some(bucket) = tb.get_bucket(bucket_name) {
                                    return Ok(bucket.bitmap().clone());
                                }
                            }
                        }
                    }
                }
                match clause {
                    FilterClause::Gt(..) => self.range_scan(field, value, |k, t| k > t),
                    FilterClause::Gte(..) => self.range_scan(field, value, |k, t| k >= t),
                    _ => unreachable!(),
                }
            }

            FilterClause::Lt(field, value) | FilterClause::Lte(field, value) => {
                match clause {
                    FilterClause::Lt(..) => self.range_scan(field, value, |k, t| k < t),
                    FilterClause::Lte(..) => self.range_scan(field, value, |k, t| k <= t),
                    _ => unreachable!(),
                }
            }

            // Pre-computed bucket bitmap from range snapping (C3): use the bitmap directly.
            FilterClause::BucketBitmap { bitmap, .. } => Ok(bitmap.as_ref().clone()),
        }
    }

    /// Resolve an Eq filter against a Tier 2 field via moka cache + pending + redb.
    fn evaluate_tier2_eq(
        &self,
        field: &str,
        value: &Value,
        tier2: &Tier2Resolver,
    ) -> Result<RoaringBitmap> {
        let key = value_to_bitmap_key(value).ok_or_else(|| BitdexError::InvalidValue {
            field: field.to_string(),
            reason: "cannot convert to bitmap key".to_string(),
        })?;
        let field_arc: Arc<str> = Arc::from(field);
        let store = Arc::clone(&tier2.store);
        let pending = Arc::clone(&tier2.pending);
        let field_for_loader = Arc::clone(&field_arc);
        let bm = tier2.cache.get_or_load(&field_arc, key, move || {
            let mut bitmap = store.load_single(&field_for_loader, key)?;
            // Apply any pending mutations (take() consumes them — moka caches the result)
            if let Some(mutations) = pending.lock().take(&field_for_loader, key) {
                mutations.apply_to(&mut bitmap);
            }
            Ok(bitmap)
        })?;
        Ok(bm.as_ref().clone())
    }

    /// Resolve an In filter against a Tier 2 field via moka cache + pending + redb.
    fn evaluate_tier2_in(
        &self,
        field: &str,
        values: &[Value],
        tier2: &Tier2Resolver,
    ) -> Result<RoaringBitmap> {
        let field_arc: Arc<str> = Arc::from(field);
        let mut result = RoaringBitmap::new();
        for val in values {
            let Some(key) = value_to_bitmap_key(val) else {
                continue;
            };
            let store = Arc::clone(&tier2.store);
            let pending = Arc::clone(&tier2.pending);
            let field_for_loader = Arc::clone(&field_arc);
            let bm = tier2.cache.get_or_load(&field_arc, key, move || {
                let mut bitmap = store.load_single(&field_for_loader, key)?;
                if let Some(mutations) = pending.lock().take(&field_for_loader, key) {
                    mutations.apply_to(&mut bitmap);
                }
                Ok(bitmap)
            })?;
            result |= bm.as_ref();
        }
        Ok(result)
    }

    /// Evaluate a range filter by scanning the filter field's bitmaps.
    fn range_scan<F>(
        &self,
        field: &str,
        value: &Value,
        predicate: F,
    ) -> Result<RoaringBitmap>
    where
        F: Fn(u64, u64) -> bool,
    {
        let filter_field = self
            .filters
            .get_field(field)
            .ok_or_else(|| BitdexError::FieldNotFound(field.to_string()))?;
        let target = value_to_bitmap_key(value)
            .ok_or_else(|| BitdexError::InvalidValue {
                field: field.to_string(),
                reason: "cannot convert to bitmap key for range filter".to_string(),
            })?;

        let mut result = RoaringBitmap::new();
        for (&key, bitmap) in filter_field.iter() {
            if predicate(key, target) {
                result |= bitmap;
            }
        }
        Ok(result)
    }

    /// Paginate by descending slot order (newest-first) for no-sort queries.
    /// Highest slot IDs come first since slots are monotonically assigned.
    fn slot_order_paginate(
        &self,
        candidates: &RoaringBitmap,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
    ) -> (Vec<i64>, Option<crate::query::CursorPosition>) {
        // For cursor-based pagination, narrow candidates to slots below cursor
        let effective = if let Some(cursor) = cursor {
            let mut narrowed = candidates.clone();
            // Remove slots >= cursor_slot_id (we want strictly less than cursor for desc order)
            narrowed.remove_range(cursor.slot_id..=u32::MAX);
            narrowed
        } else {
            candidates.clone()
        };

        if effective.is_empty() {
            return (Vec::new(), None);
        }

        // Take the last `limit` slots (highest slot IDs = newest)
        let total = effective.len() as usize;
        let skip = total.saturating_sub(limit);
        let slots: Vec<u32> = effective.iter().skip(skip).collect();
        // Reverse to get descending order
        let ids: Vec<i64> = slots.iter().rev().map(|&s| s as i64).collect();

        let next_cursor = ids.last().map(|&last_id| crate::query::CursorPosition {
            sort_value: 0,
            slot_id: last_id as u32,
        });
        (ids, next_cursor)
    }

    /// Sort candidates using bitmap sort layer traversal.
    fn sort_and_paginate(
        &self,
        candidates: &RoaringBitmap,
        sort: &SortClause,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
    ) -> Result<(Vec<i64>, Option<crate::query::CursorPosition>)> {
        let sort_field = self
            .sorts
            .get_field(&sort.field)
            .ok_or_else(|| BitdexError::FieldNotFound(sort.field.clone()))?;

        let descending = sort.direction == SortDirection::Desc;
        let cursor_param = cursor.map(|c| (c.sort_value, c.slot_id));

        let sorted_slots = sort_field.top_n(candidates, limit, descending, cursor_param);

        let ids: Vec<i64> = sorted_slots.iter().map(|&s| s as i64).collect();

        let next_cursor = sorted_slots.last().map(|&last_slot| {
            let sort_value = sort_field.reconstruct_value(last_slot) as u64;
            crate::query::CursorPosition {
                sort_value,
                slot_id: last_slot,
            }
        });

        Ok((ids, next_cursor))
    }

    /// Simple in-memory sort for small result sets.
    /// When the planner estimates the result set is small, this avoids walking 32 bit layers.
    fn simple_sort_and_paginate(
        &self,
        candidates: &RoaringBitmap,
        sort: &SortClause,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
    ) -> Result<(Vec<i64>, Option<crate::query::CursorPosition>)> {
        let sort_field = self
            .sorts
            .get_field(&sort.field)
            .ok_or_else(|| BitdexError::FieldNotFound(sort.field.clone()))?;

        let descending = sort.direction == SortDirection::Desc;

        // Reconstruct values and collect into Vec
        let mut entries: Vec<(u32, u32)> = candidates
            .iter()
            .map(|slot| (slot, sort_field.reconstruct_value(slot)))
            .collect();

        // Sort by value, tiebreak by slot ID
        if descending {
            entries.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
        } else {
            entries.sort_unstable_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        }

        // Apply cursor filtering
        if let Some(cursor) = cursor {
            let cursor_value = cursor.sort_value as u32;
            let cursor_slot = cursor.slot_id;
            entries.retain(|&(slot, value)| {
                if descending {
                    value < cursor_value || (value == cursor_value && slot < cursor_slot)
                } else {
                    value > cursor_value || (value == cursor_value && slot > cursor_slot)
                }
            });
        }

        // Take limit
        let result_slots: Vec<u32> = entries.iter().take(limit).map(|&(slot, _)| slot).collect();

        let ids: Vec<i64> = result_slots.iter().map(|&s| s as i64).collect();

        let next_cursor = result_slots.last().map(|&last_slot| {
            let sort_value = sort_field.reconstruct_value(last_slot) as u64;
            crate::query::CursorPosition {
                sort_value,
                slot_id: last_slot,
            }
        });

        Ok((ids, next_cursor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bound_cache::BoundCacheManager;
    use crate::config::{BucketConfig, Config, FilterFieldConfig, SortFieldConfig};
    use crate::filter::FilterFieldType;
    use crate::mutation::{Document, FieldValue, MutationEngine};
    use crate::time_buckets::TimeBucketManager;

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
                FilterFieldConfig {
                    name: "userId".to_string(),
                    field_type: FilterFieldType::SingleValue,
                    storage: crate::config::StorageMode::default(),
                    behaviors: None,
                },
            ],
            sort_fields: vec![
                SortFieldConfig {
                    name: "reactionCount".to_string(),
                    source_type: "uint32".to_string(),
                    encoding: "linear".to_string(),
                    bits: 32,
                },
            ],
            max_page_size: 100,
            ..Default::default()
        }
    }

    struct TestHarness {
        slots: SlotAllocator,
        filters: FilterIndex,
        sorts: SortIndex,
        config: Config,
        docstore: crate::docstore::DocStore,
    }

    impl TestHarness {
        fn new() -> Self {
            let config = test_config();
            let slots = SlotAllocator::new();
            let mut filters = FilterIndex::new();
            let mut sorts = SortIndex::new();
            let docstore = crate::docstore::DocStore::open_temp().unwrap();

            for fc in &config.filter_fields {
                filters.add_field(fc.clone());
            }
            for sc in &config.sort_fields {
                sorts.add_field(sc.clone());
            }

            Self { slots, filters, sorts, config, docstore }
        }

        fn put(&mut self, id: u32, doc: &Document) {
            let mut engine = MutationEngine::new(
                &mut self.slots,
                &mut self.filters,
                &mut self.sorts,
                &self.config,
                &self.docstore,
            );
            engine.put(id, doc).unwrap();
            // Eager merge: mirror Engine::put() behavior
            for (_name, field) in self.sorts.fields_mut() {
                field.merge_dirty();
            }
            for (_name, field) in self.filters.fields_mut() {
                field.merge_dirty();
            }
            self.slots.merge_alive();
        }

        fn query(
            &self,
            filters: &[FilterClause],
            sort: Option<&SortClause>,
            limit: usize,
            cursor: Option<&crate::query::CursorPosition>,
        ) -> QueryResult {
            let executor = QueryExecutor::new(
                &self.slots,
                &self.filters,
                &self.sorts,
                self.config.max_page_size,
            );
            executor.execute(filters, sort, limit, cursor).unwrap()
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

    fn make_time_bucket_manager(now: u64) -> TimeBucketManager {
        let configs = vec![
            BucketConfig { name: "24h".to_string(), duration_secs: 86400, refresh_interval_secs: 300 },
            BucketConfig { name: "7d".to_string(), duration_secs: 604800, refresh_interval_secs: 3600 },
        ];
        let mut mgr = TimeBucketManager::new("sortAt".to_string(), configs);
        // Slots 1-3 within 24h, slots 4-6 within 7d but outside 24h, slot 7 outside both
        let data: Vec<(u32, u64)> = vec![
            (1, now - 3600),
            (2, now - 7200),
            (3, now - 43200),
            (4, now - 90000),
            (5, now - 200000),
            (6, now - 500000),
            (7, now - 700000),
        ];
        mgr.rebuild_bucket("24h", data.iter().copied(), now);
        mgr.rebuild_bucket("7d", data.iter().copied(), now);
        mgr
    }

    #[test]
    fn test_basic_eq_filter() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(100))),
        ]));
        h.put(2, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
            ("reactionCount", FieldValue::Single(Value::Integer(200))),
        ]));
        h.put(3, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(300))),
        ]));

        let result = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 2);
        assert!(result.ids.contains(&1));
        assert!(result.ids.contains(&3));
    }

    #[test]
    fn test_not_eq_filter() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(28)))]));
        h.put(2, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));
        h.put(3, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));

        let result = h.query(
            &[FilterClause::NotEq("nsfwLevel".to_string(), Value::Integer(28))],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 2);
        assert!(result.ids.contains(&2));
        assert!(result.ids.contains(&3));
    }

    #[test]
    fn test_in_filter() {
        let mut h = TestHarness::new();

        for i in 1..=10u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer((i % 3) as i64))),
            ]));
        }

        let result = h.query(
            &[FilterClause::In("nsfwLevel".to_string(), vec![Value::Integer(0), Value::Integer(1)])],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 7);
    }

    #[test]
    fn test_and_filter() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("onSite", FieldValue::Single(Value::Bool(true))),
        ]));
        h.put(2, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("onSite", FieldValue::Single(Value::Bool(false))),
        ]));
        h.put(3, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
            ("onSite", FieldValue::Single(Value::Bool(true))),
        ]));

        let result = h.query(
            &[FilterClause::And(vec![
                FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),
                FilterClause::Eq("onSite".to_string(), Value::Bool(true)),
            ])],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 1);
        assert_eq!(result.ids, vec![1]);
    }

    #[test]
    fn test_or_filter() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));
        h.put(2, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(2)))]));
        h.put(3, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(3)))]));

        let result = h.query(
            &[FilterClause::Or(vec![
                FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),
                FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(3)),
            ])],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 2);
        assert!(result.ids.contains(&1));
        assert!(result.ids.contains(&3));
    }

    #[test]
    fn test_sort_descending() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(100))),
        ]));
        h.put(2, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(500))),
        ]));
        h.put(3, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(200))),
        ]));

        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let result = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            Some(&sort),
            3,
            None,
        );

        assert_eq!(result.ids, vec![2, 3, 1]); // 500, 200, 100
    }

    #[test]
    fn test_cursor_pagination() {
        let mut h = TestHarness::new();

        for i in 1..=10u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer((i * 10) as i64))),
            ]));
        }

        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };

        let page1 = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            Some(&sort),
            3,
            None,
        );
        assert_eq!(page1.ids, vec![10, 9, 8]);
        assert!(page1.cursor.is_some());

        let page2 = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            Some(&sort),
            3,
            page1.cursor.as_ref(),
        );
        assert_eq!(page2.ids, vec![7, 6, 5]);
    }

    #[test]
    fn test_deleted_invisible() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));
        h.put(2, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));

        {
            let mut engine = MutationEngine::new(
                &mut h.slots,
                &mut h.filters,
                &mut h.sorts,
                &h.config,
                &h.docstore,
            );
            engine.delete(1).unwrap();
        }

        let result = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 1);
        assert_eq!(result.ids, vec![2]);
    }

    #[test]
    fn test_no_filters_returns_all_alive() {
        let mut h = TestHarness::new();

        for i in 1..=5u32 {
            h.put(i, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));
        }

        let result = h.query(&[], None, 100, None);
        assert_eq!(result.total_matched, 5);
    }

    #[test]
    fn test_max_page_size_enforced() {
        let mut h = TestHarness::new();
        h.config.max_page_size = 5;

        for i in 1..=20u32 {
            h.put(i, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));
        }

        let executor = QueryExecutor::new(
            &h.slots,
            &h.filters,
            &h.sorts,
            h.config.max_page_size,
        );
        let result = executor.execute(&[], None, 1000, None).unwrap();
        assert_eq!(result.ids.len(), 5);
        assert_eq!(result.total_matched, 20);
    }

    #[test]
    fn test_no_sort_returns_descending_slot_order() {
        let mut h = TestHarness::new();

        for i in 1..=5u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ]));
        }

        let result = h.query(&[], None, 100, None);
        assert_eq!(result.ids, vec![5, 4, 3, 2, 1]);
    }

    #[test]
    fn test_no_sort_cursor_pagination() {
        let mut h = TestHarness::new();

        for i in 1..=10u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ]));
        }

        // First page: top 5 descending
        let page1 = h.query(&[], None, 5, None);
        assert_eq!(page1.ids, vec![10, 9, 8, 7, 6]);
        assert!(page1.cursor.is_some());

        // Second page: next 5 descending using cursor
        let page2 = h.query(&[], None, 5, page1.cursor.as_ref());
        assert_eq!(page2.ids, vec![5, 4, 3, 2, 1]);
    }

    #[test]
    fn test_no_sort_with_filter() {
        let mut h = TestHarness::new();

        for i in 1..=10u32 {
            let level = if i % 2 == 0 { 1 } else { 2 };
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(level))),
            ]));
        }

        // Filter for nsfwLevel=1 (even IDs: 2,4,6,8,10), no sort
        let result = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
            None,
        );
        assert_eq!(result.ids, vec![10, 8, 6, 4, 2]);
    }

    #[test]
    fn test_c3_bucket_snapping_gte_snaps_to_24h() {
        let now: u64 = 1_700_000_000;
        let mgr = make_time_bucket_manager(now);

        let h = TestHarness::new();
        let executor = QueryExecutor::new(
            &h.slots,
            &h.filters,
            &h.sorts,
            h.config.max_page_size,
        ).with_time_buckets(&mgr, now);

        // Gte("sortAt", now - 86400) — exact 24h match, not in FilterIndex, but bucket snapping
        // should return the 24h bucket bitmap (slots 1, 2, 3 from make_time_bucket_manager)
        let ts = (now - 86400) as i64;
        let clause = FilterClause::Gte("sortAt".to_string(), Value::Integer(ts));
        let bitmap = executor.evaluate_clause(&clause).unwrap();
        // Slots 1, 2, 3 are within 24h in make_time_bucket_manager
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(3));
        // Slot 4 is at 90000s (25h) — outside 24h bucket
        assert!(!bitmap.contains(4));
    }

    #[test]
    fn test_c3_bucket_snapping_gt_snaps_to_7d() {
        let now: u64 = 1_700_000_000;
        let mgr = make_time_bucket_manager(now);

        let h = TestHarness::new();
        let executor = QueryExecutor::new(
            &h.slots,
            &h.filters,
            &h.sorts,
            h.config.max_page_size,
        ).with_time_buckets(&mgr, now);

        // Gt("sortAt", now - 590000) — duration=590000, within 10% of 7d (604800)
        let ts = (now - 590000) as i64;
        let clause = FilterClause::Gt("sortAt".to_string(), Value::Integer(ts));
        let bitmap = executor.evaluate_clause(&clause).unwrap();
        // Slots 1-6 are within 7d
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(6));
        // Slot 7 at 700000s is outside 7d
        assert!(!bitmap.contains(7));
    }

    #[test]
    fn test_c3_no_snap_outside_tolerance() {
        let now: u64 = 1_700_000_000;
        let mgr = make_time_bucket_manager(now);

        let h = TestHarness::new();
        // Register "sortAt" as a filter field so range_scan won't error
        let mut filters = FilterIndex::new();
        filters.add_field(FilterFieldConfig {
            name: "sortAt".to_string(),
            field_type: FilterFieldType::SingleValue,
            storage: crate::config::StorageMode::default(),
            behaviors: None,
        });
        let executor = QueryExecutor::new(
            &h.slots,
            &filters,
            &h.sorts,
            h.config.max_page_size,
        ).with_time_buckets(&mgr, now);

        // Duration = 200000s — outside tolerance of both 24h and 7d, falls through to range_scan
        let ts = (now - 200000) as i64;
        let clause = FilterClause::Gte("sortAt".to_string(), Value::Integer(ts));
        // range_scan on empty filter index returns empty bitmap (no values stored)
        let bitmap = executor.evaluate_clause(&clause).unwrap();
        assert!(bitmap.is_empty());
    }

    #[test]
    fn test_c3_no_snap_for_non_bucketed_field() {
        let now: u64 = 1_700_000_000;
        let mgr = make_time_bucket_manager(now);

        let h = TestHarness::new();
        let executor = QueryExecutor::new(
            &h.slots,
            &h.filters,
            &h.sorts,
            h.config.max_page_size,
        ).with_time_buckets(&mgr, now);

        // nsfwLevel is not the bucketed field (sortAt), so should fall through to range_scan
        // which will return an error since nsfwLevel has no stored range values matching Gt
        let ts = (now - 86400) as i64;
        let clause = FilterClause::Gte("nsfwLevel".to_string(), Value::Integer(ts));
        // This should succeed (nsfwLevel is in FilterIndex), returning empty (no values >= ts)
        let bitmap = executor.evaluate_clause(&clause).unwrap();
        assert!(bitmap.is_empty());
    }

    #[test]
    fn test_with_time_buckets_builder() {
        let now: u64 = 1_700_000_000;
        let mgr = make_time_bucket_manager(now);
        let h = TestHarness::new();

        let executor = QueryExecutor::new(
            &h.slots,
            &h.filters,
            &h.sorts,
            h.config.max_page_size,
        ).with_time_buckets(&mgr, now);

        // Verify field_name from the manager is accessible via bucket snapping
        assert_eq!(mgr.field_name(), "sortAt");
        // Verify executor was constructed successfully (indirectly)
        let _ = executor.filter_index();
    }

    // --- D5/D2/D8: Bound cache integration tests ---

    #[test]
    fn test_d5_bound_forms_on_first_sort_query() {
        let mut h = TestHarness::new();

        // Insert docs with varying reactionCount
        for i in 1..=20u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer((i * 100) as i64))),
            ]));
        }

        let bc = parking_lot::Mutex::new(BoundCacheManager::new(10, 20));
        let mut cache = TrieCache::new(h.config.cache.clone());

        let executor = QueryExecutor::new(
            &h.slots, &h.filters, &h.sorts, h.config.max_page_size,
        ).with_bound_cache(&bc);

        let sort = SortClause { field: "reactionCount".to_string(), direction: SortDirection::Desc };
        let filters = vec![FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))];

        // First query — should form a bound
        let result = executor.execute_with_cache(&filters, Some(&sort), 5, None, &mut cache).unwrap();
        assert_eq!(result.ids.len(), 5);
        assert_eq!(result.ids[0], 20); // highest reactionCount

        // Bound should now exist
        let bc_guard = bc.lock();
        assert_eq!(bc_guard.len(), 1);
    }

    #[test]
    fn test_d5_bound_used_on_second_query() {
        let mut h = TestHarness::new();

        for i in 1..=50u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer((i * 10) as i64))),
            ]));
        }

        let bc = parking_lot::Mutex::new(BoundCacheManager::new(10, 20));
        let mut cache = TrieCache::new(h.config.cache.clone());

        let executor = QueryExecutor::new(
            &h.slots, &h.filters, &h.sorts, h.config.max_page_size,
        ).with_bound_cache(&bc);

        let sort = SortClause { field: "reactionCount".to_string(), direction: SortDirection::Desc };
        let filters = vec![FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))];

        // First query forms the bound
        let r1 = executor.execute_with_cache(&filters, Some(&sort), 5, None, &mut cache).unwrap();
        assert_eq!(r1.ids, vec![50, 49, 48, 47, 46]);

        // Second query should use the bound (same results expected)
        let r2 = executor.execute_with_cache(&filters, Some(&sort), 5, None, &mut cache).unwrap();
        assert_eq!(r2.ids, vec![50, 49, 48, 47, 46]);

        // Bound should still be present
        assert_eq!(bc.lock().len(), 1);
    }

    #[test]
    fn test_d5_no_bound_without_sort() {
        let mut h = TestHarness::new();

        for i in 1..=10u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ]));
        }

        let bc = parking_lot::Mutex::new(BoundCacheManager::new(5, 10));
        let mut cache = TrieCache::new(h.config.cache.clone());

        let executor = QueryExecutor::new(
            &h.slots, &h.filters, &h.sorts, h.config.max_page_size,
        ).with_bound_cache(&bc);

        // Query without sort — should NOT form a bound
        let _ = executor.execute_with_cache(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None, 10, None, &mut cache,
        ).unwrap();

        assert_eq!(bc.lock().len(), 0);
    }

    #[test]
    fn test_d5_different_filter_misses_bound() {
        let mut h = TestHarness::new();

        for i in 1..=20u32 {
            let level = if i <= 10 { 1 } else { 2 };
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(level))),
                ("reactionCount", FieldValue::Single(Value::Integer((i * 10) as i64))),
            ]));
        }

        let bc = parking_lot::Mutex::new(BoundCacheManager::new(5, 10));
        let mut cache = TrieCache::new(h.config.cache.clone());

        let executor = QueryExecutor::new(
            &h.slots, &h.filters, &h.sorts, h.config.max_page_size,
        ).with_bound_cache(&bc);

        let sort = SortClause { field: "reactionCount".to_string(), direction: SortDirection::Desc };

        // Query with nsfwLevel=1 — forms a bound
        let _ = executor.execute_with_cache(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            Some(&sort), 5, None, &mut cache,
        ).unwrap();

        // Query with nsfwLevel=2 — different filter, should form a separate bound
        let _ = executor.execute_with_cache(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(2))],
            Some(&sort), 5, None, &mut cache,
        ).unwrap();

        // Should have 2 separate bounds
        assert_eq!(bc.lock().len(), 2);
    }
}
