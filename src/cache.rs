// Trie Cache — Invalidation Model
//
// Bitdex uses a dual-cache architecture with intentionally different invalidation strategies:
//
// 1. **Trie cache** (this module): caches exact filter bitmap results keyed by canonical
//    filter clause combinations. Uses generation-counter lazy invalidation — each filter
//    field has a monotonic generation that increments on mutation. Cache entries store the
//    generation at creation time; lookups compare and discard stale entries. This is cheap
//    (one u64 compare per field) and correct for filter result caching.
//
// 2. **Bound cache** (`bound_cache.rs`): caches approximate sort working sets (top-N slot
//    windows) for filter+sort combinations. Uses live maintenance — the flush thread adds
//    newly inserted slots that pass the bound's filter predicate, and removes deleted slots.
//    Bounds that grow too bloated are rebuilt from scratch.
//
// These serve different purposes: the trie cache answers "which IDs match these filters?"
// (exact, invalidated on any filter change), while the bound cache answers "what's the
// approximate top-N sort window?" (approximate, maintained incrementally from writes).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use roaring::RoaringBitmap;

use crate::config::CacheConfig;
use crate::meta_index::{CacheEntryId, MetaIndex};
use crate::query::{FilterClause, Value};

/// A single canonicalized filter clause key component.
/// Clauses are sorted by field name, then by string representation of value.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CanonicalClause {
    pub field: String,
    pub op: String,
    pub value_repr: String,
}

impl CanonicalClause {
    pub(crate) fn from_filter(clause: &FilterClause) -> Option<Self> {
        match clause {
            FilterClause::Eq(field, value) => Some(CanonicalClause {
                field: field.clone(),
                op: "eq".to_string(),
                value_repr: value_to_string(value),
            }),
            FilterClause::NotEq(field, value) => Some(CanonicalClause {
                field: field.clone(),
                op: "neq".to_string(),
                value_repr: value_to_string(value),
            }),
            FilterClause::In(field, values) => {
                let mut sorted_values: Vec<String> = values.iter().map(value_to_string).collect();
                sorted_values.sort();
                Some(CanonicalClause {
                    field: field.clone(),
                    op: "in".to_string(),
                    value_repr: sorted_values.join(","),
                })
            }
            FilterClause::NotIn(field, values) => {
                let mut sorted_values: Vec<String> = values.iter().map(value_to_string).collect();
                sorted_values.sort();
                Some(CanonicalClause {
                    field: field.clone(),
                    op: "notin".to_string(),
                    value_repr: sorted_values.join(","),
                })
            }
            FilterClause::Gt(field, value) => Some(CanonicalClause {
                field: field.clone(),
                op: "gt".to_string(),
                value_repr: value_to_string(value),
            }),
            FilterClause::Gte(field, value) => Some(CanonicalClause {
                field: field.clone(),
                op: "gte".to_string(),
                value_repr: value_to_string(value),
            }),
            FilterClause::Lt(field, value) => Some(CanonicalClause {
                field: field.clone(),
                op: "lt".to_string(),
                value_repr: value_to_string(value),
            }),
            FilterClause::Lte(field, value) => Some(CanonicalClause {
                field: field.clone(),
                op: "lte".to_string(),
                value_repr: value_to_string(value),
            }),
            // BucketBitmap uses a stable key: op="bucket", value_repr=bucket_name.
            // This ensures queries snapped to "sortAt:7d" always produce the same cache key
            // regardless of the raw timestamp value supplied by the caller (C5).
            FilterClause::BucketBitmap { field, bucket_name, .. } => Some(CanonicalClause {
                field: field.clone(),
                op: "bucket".to_string(),
                value_repr: bucket_name.clone(),
            }),
            // Compound clauses (And/Or/Not) are not directly cacheable as trie keys
            FilterClause::Not(_) | FilterClause::And(_) | FilterClause::Or(_) => None,
        }
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => format!("{f}"),
        Value::Bool(b) => b.to_string(),
        Value::String(s) => s.clone(),
    }
}

/// Canonical key for the trie cache: a sorted sequence of filter clause keys.
/// This is the path through the trie.
pub type CacheKey = Vec<CanonicalClause>;

/// Convert a set of filter clauses into a canonical cache key.
/// Returns None if any clause cannot be canonicalized (compound clauses).
/// Flattens top-level clauses and sorts by (field, op, value).
pub fn canonicalize(clauses: &[FilterClause]) -> Option<CacheKey> {
    let mut key_parts: Vec<CanonicalClause> = Vec::new();
    for clause in clauses {
        match clause {
            // Flatten top-level And into individual clauses
            FilterClause::And(inner) => {
                for c in inner {
                    key_parts.push(CanonicalClause::from_filter(c)?);
                }
            }
            _ => {
                key_parts.push(CanonicalClause::from_filter(clause)?);
            }
        }
    }
    key_parts.sort();
    Some(key_parts)
}

/// Produce a stable canonical clause for a bucket-snapped range filter (C5).
/// Used by `canonicalize_with_buckets` to ensure range queries on bucketed fields
/// produce deterministic cache keys independent of the raw timestamp value.
pub fn snap_clause_key(
    clause: &FilterClause,
    tb: &crate::time_buckets::TimeBucketManager,
    now_unix: u64,
) -> Option<CanonicalClause> {
    match clause {
        FilterClause::Gt(field, value) | FilterClause::Gte(field, value)
            if field == tb.field_name() =>
        {
            let threshold = match value {
                Value::Integer(v) => Some(*v as u64),
                _ => None,
            }?;
            let duration = now_unix.saturating_sub(threshold);
            let bucket_name = tb.snap_duration(duration, 0.10)?;
            Some(CanonicalClause {
                field: field.clone(),
                op: "bucket_gte".to_string(),
                value_repr: bucket_name.to_string(),
            })
        }
        FilterClause::Lt(field, value) | FilterClause::Lte(field, value)
            if field == tb.field_name() =>
        {
            let threshold = match value {
                Value::Integer(v) => Some(*v as u64),
                _ => None,
            }?;
            let duration = threshold.saturating_sub(now_unix);
            let bucket_name = tb.snap_duration(duration, 0.10)?;
            Some(CanonicalClause {
                field: field.clone(),
                op: "bucket_lte".to_string(),
                value_repr: bucket_name.to_string(),
            })
        }
        _ => None,
    }
}

/// Canonicalize filter clauses with optional bucket snapping (C5).
/// Bucket-eligible range clauses produce stable keys (e.g., "sortAt:bucket_gte:7d")
/// instead of raw timestamps that change every second.
pub fn canonicalize_with_buckets(
    clauses: &[FilterClause],
    tb: Option<&crate::time_buckets::TimeBucketManager>,
    now_unix: u64,
) -> Option<CacheKey> {
    let mut canonical_clauses: Vec<CanonicalClause> = Vec::new();
    for clause in clauses {
        // Try bucket snapping first
        if let Some(tb) = tb {
            if let Some(snapped) = snap_clause_key(clause, tb, now_unix) {
                canonical_clauses.push(snapped);
                continue;
            }
        }
        // Fall through to normal canonicalization
        match clause {
            // Flatten top-level And into individual clauses
            FilterClause::And(inner) => {
                for c in inner {
                    canonical_clauses.push(CanonicalClause::from_filter(c)?);
                }
            }
            _ => {
                canonical_clauses.push(CanonicalClause::from_filter(clause)?);
            }
        }
    }
    canonical_clauses.sort();
    Some(canonical_clauses)
}

/// A cached entry in the trie.
struct CacheEntry {
    /// The cached filter result bitmap (Arc-wrapped to avoid deep clones on lookup).
    bitmap: Arc<RoaringBitmap>,
    /// Hit count with exponential decay.
    hit_score: f64,
    /// Last access time.
    last_access: Instant,
    /// Generation counters at the time this entry was cached.
    /// Maps field name -> generation at cache time.
    field_generations: HashMap<String, u64>,
    /// Meta-index entry ID for this cache entry (used for live updates).
    meta_id: Option<CacheEntryId>,
}

/// A trie node in the cache.
struct TrieNode {
    /// Children indexed by the next CanonicalClause in the key path.
    children: HashMap<CanonicalClause, TrieNode>,
    /// Cached result at this node (if any).
    entry: Option<CacheEntry>,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            entry: None,
        }
    }

    fn count_entries(&self) -> usize {
        let self_count = if self.entry.is_some() { 1 } else { 0 };
        let child_count: usize = self.children.values().map(|c| c.count_entries()).sum();
        self_count + child_count
    }
}

/// Result of a cache lookup.
pub enum CacheLookup {
    /// Exact match with valid bitmap (Arc clone — ~1ns, no deep copy).
    ExactHit(Arc<RoaringBitmap>),
    /// Prefix match: cached bitmap for a prefix of the query.
    /// The second element contains the remaining (uncached) filter clauses.
    PrefixHit {
        bitmap: Arc<RoaringBitmap>,
        matched_prefix_len: usize,
    },
    /// No cache match.
    Miss,
}

/// Check if an entry's field generations are still valid against current generations.
fn is_entry_valid(
    current_gens: &HashMap<String, u64>,
    entry_gens: &HashMap<String, u64>,
) -> bool {
    for (field, &entry_gen) in entry_gens {
        let current_gen = current_gens.get(field).copied().unwrap_or(0);
        if current_gen > entry_gen {
            return false;
        }
    }
    true
}

/// Trie-based filter result cache.
///
/// Keys are canonically sorted filter clause sequences. Sort fields are NOT
/// part of cache keys. Cache entries are lazily invalidated via per-field
/// generation counters.
pub struct TrieCache {
    root: TrieNode,
    /// Per-field generation counters. Bumped when any document's value for that field changes.
    field_generations: HashMap<String, u64>,
    /// Configuration.
    config: CacheConfig,
    /// Total number of cached entries.
    entry_count: usize,
    /// Meta-index for O(1) lookup of cache entries by clause during live updates.
    meta: MetaIndex,
    /// Reverse map: meta_id → cache key path (for live update traversal).
    id_to_key: HashMap<CacheEntryId, CacheKey>,
}

impl TrieCache {
    pub fn new(config: CacheConfig) -> Self {
        Self {
            root: TrieNode::new(),
            field_generations: HashMap::new(),
            config,
            entry_count: 0,
            meta: MetaIndex::new(),
            id_to_key: HashMap::new(),
        }
    }

    /// Look up a cache entry by canonical key.
    /// Checks generation counters for validity.
    /// Falls back to prefix matching if no exact hit.
    pub fn lookup(&mut self, key: &CacheKey) -> CacheLookup {
        if key.is_empty() {
            return CacheLookup::Miss;
        }

        let mut node = &mut self.root;
        let mut best_prefix_bitmap: Option<Arc<RoaringBitmap>> = None;
        let mut best_prefix_len = 0;

        for (i, clause_key) in key.iter().enumerate() {
            match node.children.get_mut(clause_key) {
                Some(child) => {
                    node = child;
                    // Check if this node has a valid cached entry
                    if let Some(entry) = &mut node.entry {
                        if is_entry_valid(&self.field_generations, &entry.field_generations) {
                            entry.hit_score += 1.0;
                            entry.last_access = Instant::now();
                            if i == key.len() - 1 {
                                // Exact match
                                return CacheLookup::ExactHit(entry.bitmap.clone());
                            }
                            // Valid prefix match
                            best_prefix_bitmap = Some(entry.bitmap.clone());
                            best_prefix_len = i + 1;
                        }
                    }
                }
                None => break,
            }
        }

        if let Some(bitmap) = best_prefix_bitmap {
            CacheLookup::PrefixHit {
                bitmap,
                matched_prefix_len: best_prefix_len,
            }
        } else {
            CacheLookup::Miss
        }
    }

    /// Store a result bitmap in the cache under the given key.
    pub fn store(&mut self, key: &CacheKey, bitmap: Arc<RoaringBitmap>) {
        if key.is_empty() {
            return;
        }

        // Evict if at capacity
        if self.entry_count >= self.config.max_entries {
            self.evict_lru();
        }

        let field_gens = self.current_field_generations(key);

        // Register with meta-index for live update lookups.
        // Only register Eq clauses (NotEq/In fall back to invalidation).
        let should_register = key.iter().all(|c| c.op == "eq");
        let meta_id = if should_register {
            let id = self.meta.register(key, None, None);
            self.id_to_key.insert(id, key.clone());
            Some(id)
        } else {
            None
        };

        let mut node = &mut self.root;
        for clause_key in key {
            node = node
                .children
                .entry(clause_key.clone())
                .or_insert_with(TrieNode::new);
        }

        // Deregister old entry's meta-id if replacing
        if let Some(old_entry) = &node.entry {
            if let Some(old_id) = old_entry.meta_id {
                self.meta.deregister(old_id);
                self.id_to_key.remove(&old_id);
            }
        }

        let was_none = node.entry.is_none();
        node.entry = Some(CacheEntry {
            bitmap,
            hit_score: 1.0,
            last_access: Instant::now(),
            field_generations: field_gens,
            meta_id,
        });

        if was_none {
            self.entry_count += 1;
        }
    }

    /// Bump the generation counter for a field.
    /// Called when any document's value for that field changes (insert/update/delete).
    pub fn invalidate_field(&mut self, field: &str) {
        let gen = self.field_generations.entry(field.to_string()).or_insert(0);
        *gen += 1;
    }

    /// Get the current generation for a field.
    pub fn field_generation(&self, field: &str) -> u64 {
        self.field_generations.get(field).copied().unwrap_or(0)
    }

    /// Access the meta-index (for live update clause lookups).
    pub fn meta(&self) -> &MetaIndex {
        &self.meta
    }

    /// Live-update a cache entry by its meta-index ID.
    ///
    /// Inserts or removes a slot from the entry's bitmap. Uses Arc::make_mut()
    /// which only deep-clones if refcount > 1 (typically = 1 during flush since
    /// readers hold the previous snapshot's Arc).
    pub fn update_entry_by_id(&mut self, id: CacheEntryId, slot: u32, is_insert: bool) {
        let Some(key) = self.id_to_key.get(&id).cloned() else { return };
        // Traverse trie to find the entry
        let mut node = &mut self.root;
        for clause_key in &key {
            node = match node.children.get_mut(clause_key) {
                Some(child) => child,
                None => return,
            };
        }
        let Some(entry) = &mut node.entry else { return };
        // Verify the entry is still valid (not stale from generation mismatch)
        if !is_entry_valid(&self.field_generations, &entry.field_generations) {
            return;
        }
        let bm = Arc::make_mut(&mut entry.bitmap);
        if is_insert {
            bm.insert(slot);
        } else {
            bm.remove(slot);
        }
    }

    /// Advance a live-updated entry's stored generation for a field to match the
    /// current generation. Called after live-updating an entry so that subsequent
    /// generation bumps (for non-Eq invalidation) don't falsely invalidate it.
    pub fn refresh_entry_generation(&mut self, id: CacheEntryId, field: &str) {
        let Some(key) = self.id_to_key.get(&id).cloned() else { return };
        let mut node = &mut self.root;
        for clause_key in &key {
            node = match node.children.get_mut(clause_key) {
                Some(child) => child,
                None => return,
            };
        }
        if let Some(entry) = &mut node.entry {
            let current_gen = self.field_generations.get(field).copied().unwrap_or(0);
            entry.field_generations.insert(field.to_string(), current_gen);
        }
    }

    /// Run the promotion/demotion cycle.
    /// Applies decay to all hit scores and evicts entries below a threshold.
    pub fn maintenance_cycle(&mut self) {
        let decay_rate = self.config.decay_rate;
        let mut evicted_meta_ids = Vec::new();
        Self::decay_node(&mut self.root, decay_rate, &mut self.entry_count, &mut evicted_meta_ids);
        for id in evicted_meta_ids {
            self.meta.deregister(id);
            self.id_to_key.remove(&id);
        }
    }

    /// Get the number of cached entries.
    pub fn len(&self) -> usize {
        self.entry_count
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Return the total serialized byte size of all cached bitmaps.
    pub fn bitmap_bytes(&self) -> usize {
        Self::node_bitmap_bytes(&self.root)
    }

    fn node_bitmap_bytes(node: &TrieNode) -> usize {
        let self_bytes = node
            .entry
            .as_ref()
            .map_or(0, |e| e.bitmap.serialized_size());
        let child_bytes: usize = node
            .children
            .values()
            .map(|c| Self::node_bitmap_bytes(c))
            .sum();
        self_bytes + child_bytes
    }

    /// Clear all cached entries.
    pub fn clear(&mut self) {
        self.root = TrieNode::new();
        self.entry_count = 0;
        self.meta = MetaIndex::new();
        self.id_to_key.clear();
    }

    /// Get the current field generations for the fields referenced in a cache key.
    fn current_field_generations(&self, key: &CacheKey) -> HashMap<String, u64> {
        let mut gens = HashMap::new();
        for clause in key {
            let gen = self.field_generations.get(&clause.field).copied().unwrap_or(0);
            gens.insert(clause.field.clone(), gen);
        }
        gens
    }

    /// Evict the least recently used entry.
    fn evict_lru(&mut self) {
        // Find the entry with lowest hit_score (LRU-like via decayed scores)
        let mut worst_score = f64::MAX;
        let mut worst_path: Option<Vec<CanonicalClause>> = None;

        Self::find_worst_entry(&self.root, &mut Vec::new(), &mut worst_score, &mut worst_path);

        if let Some(path) = worst_path {
            self.remove_entry(&path);
        }
    }

    fn find_worst_entry(
        node: &TrieNode,
        current_path: &mut Vec<CanonicalClause>,
        worst_score: &mut f64,
        worst_path: &mut Option<Vec<CanonicalClause>>,
    ) {
        if let Some(entry) = &node.entry {
            if entry.hit_score < *worst_score {
                *worst_score = entry.hit_score;
                *worst_path = Some(current_path.clone());
            }
        }
        for (key, child) in &node.children {
            current_path.push(key.clone());
            Self::find_worst_entry(child, current_path, worst_score, worst_path);
            current_path.pop();
        }
    }

    fn remove_entry(&mut self, path: &[CanonicalClause]) {
        if path.is_empty() {
            return;
        }
        let mut node = &mut self.root;
        for key in path {
            node = match node.children.get_mut(key) {
                Some(child) => child,
                None => return,
            };
        }
        if let Some(entry) = node.entry.take() {
            if let Some(id) = entry.meta_id {
                self.meta.deregister(id);
                self.id_to_key.remove(&id);
            }
            self.entry_count -= 1;
        }
    }

    /// Recursively decay hit scores and remove entries with near-zero scores.
    fn decay_node(node: &mut TrieNode, decay_rate: f64, entry_count: &mut usize, evicted_meta_ids: &mut Vec<CacheEntryId>) {
        if let Some(entry) = &mut node.entry {
            entry.hit_score *= decay_rate;
            if entry.hit_score < 0.01 {
                if let Some(id) = entry.meta_id {
                    evicted_meta_ids.push(id);
                }
                node.entry = None;
                *entry_count -= 1;
            }
        }

        // Process children, collecting empty ones to clean up
        let mut empty_children = Vec::new();
        for (key, child) in node.children.iter_mut() {
            Self::decay_node(child, decay_rate, entry_count, evicted_meta_ids);
            if child.entry.is_none() && child.children.is_empty() {
                empty_children.push(key.clone());
            }
        }
        for key in empty_children {
            node.children.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::FilterClause;

    fn make_config(max_entries: usize) -> CacheConfig {
        CacheConfig {
            max_entries,
            decay_rate: 0.95,
            ..Default::default()
        }
    }

    fn eq_clause(field: &str, value: i64) -> FilterClause {
        FilterClause::Eq(field.to_string(), Value::Integer(value))
    }

    fn make_bitmap(slots: &[u32]) -> Arc<RoaringBitmap> {
        let mut bm = RoaringBitmap::new();
        for &s in slots {
            bm.insert(s);
        }
        Arc::new(bm)
    }

    #[test]
    fn test_canonicalize_simple() {
        let clauses = vec![
            eq_clause("nsfwLevel", 1),
            eq_clause("userId", 42),
        ];
        let key = canonicalize(&clauses).unwrap();
        assert_eq!(key.len(), 2);
        // Should be sorted by field name: nsfwLevel < userId
        assert_eq!(key[0].field, "nsfwLevel");
        assert_eq!(key[1].field, "userId");
    }

    #[test]
    fn test_canonicalize_order_independent() {
        let clauses_a = vec![
            eq_clause("userId", 42),
            eq_clause("nsfwLevel", 1),
        ];
        let clauses_b = vec![
            eq_clause("nsfwLevel", 1),
            eq_clause("userId", 42),
        ];
        let key_a = canonicalize(&clauses_a).unwrap();
        let key_b = canonicalize(&clauses_b).unwrap();
        assert_eq!(key_a, key_b);
    }

    #[test]
    fn test_canonicalize_flattens_and() {
        let clauses = vec![
            FilterClause::And(vec![
                eq_clause("nsfwLevel", 1),
                eq_clause("userId", 42),
            ]),
        ];
        let key = canonicalize(&clauses).unwrap();
        assert_eq!(key.len(), 2);
    }

    #[test]
    fn test_canonicalize_rejects_compound_not() {
        let clauses = vec![
            FilterClause::Not(Box::new(eq_clause("nsfwLevel", 28))),
        ];
        assert!(canonicalize(&clauses).is_none());
    }

    #[test]
    fn test_canonicalize_in_clause_sorts_values() {
        let clauses = vec![
            FilterClause::In(
                "tagIds".to_string(),
                vec![Value::Integer(300), Value::Integer(100), Value::Integer(200)],
            ),
        ];
        let key = canonicalize(&clauses).unwrap();
        assert_eq!(key[0].value_repr, "100,200,300");
    }

    #[test]
    fn test_store_and_exact_hit() {
        let mut cache = TrieCache::new(make_config(100));
        let clauses = vec![eq_clause("nsfwLevel", 1)];
        let key = canonicalize(&clauses).unwrap();

        let bitmap = make_bitmap(&[1, 2, 3]);
        cache.store(&key, bitmap.clone());

        match cache.lookup(&key) {
            CacheLookup::ExactHit(bm) => assert_eq!(bm, bitmap),
            _ => panic!("expected exact hit"),
        }
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_miss() {
        let mut cache = TrieCache::new(make_config(100));
        let clauses = vec![eq_clause("nsfwLevel", 1)];
        let key = canonicalize(&clauses).unwrap();

        match cache.lookup(&key) {
            CacheLookup::Miss => {}
            _ => panic!("expected miss"),
        }
    }

    #[test]
    fn test_prefix_hit() {
        let mut cache = TrieCache::new(make_config(100));

        // Cache {nsfwLevel=1}
        let prefix_clauses = vec![eq_clause("nsfwLevel", 1)];
        let prefix_key = canonicalize(&prefix_clauses).unwrap();
        cache.store(&prefix_key, make_bitmap(&[1, 2, 3, 4, 5]));

        // Query {nsfwLevel=1, userId=42} -- should get prefix hit
        let full_clauses = vec![eq_clause("nsfwLevel", 1), eq_clause("userId", 42)];
        let full_key = canonicalize(&full_clauses).unwrap();

        match cache.lookup(&full_key) {
            CacheLookup::PrefixHit { bitmap, matched_prefix_len } => {
                assert_eq!(bitmap, make_bitmap(&[1, 2, 3, 4, 5]));
                assert_eq!(matched_prefix_len, 1);
            }
            _ => panic!("expected prefix hit"),
        }
    }

    #[test]
    fn test_generation_invalidation() {
        let mut cache = TrieCache::new(make_config(100));

        let clauses = vec![eq_clause("nsfwLevel", 1)];
        let key = canonicalize(&clauses).unwrap();
        cache.store(&key, make_bitmap(&[1, 2, 3]));

        // Bump the generation for nsfwLevel
        cache.invalidate_field("nsfwLevel");

        // Lookup should now miss (stale generation)
        match cache.lookup(&key) {
            CacheLookup::Miss => {}
            _ => panic!("expected miss after invalidation"),
        }
    }

    #[test]
    fn test_generation_other_field_no_invalidation() {
        let mut cache = TrieCache::new(make_config(100));

        let clauses = vec![eq_clause("nsfwLevel", 1)];
        let key = canonicalize(&clauses).unwrap();
        cache.store(&key, make_bitmap(&[1, 2, 3]));

        // Bump generation for a DIFFERENT field
        cache.invalidate_field("userId");

        // Lookup should still hit (different field)
        match cache.lookup(&key) {
            CacheLookup::ExactHit(_) => {}
            _ => panic!("expected exact hit -- different field invalidation should not affect"),
        }
    }

    #[test]
    fn test_eviction_at_capacity() {
        let mut cache = TrieCache::new(make_config(2));

        // Fill cache
        let k1 = canonicalize(&[eq_clause("nsfwLevel", 1)]).unwrap();
        let k2 = canonicalize(&[eq_clause("nsfwLevel", 2)]).unwrap();
        cache.store(&k1, make_bitmap(&[1]));
        cache.store(&k2, make_bitmap(&[2]));
        assert_eq!(cache.len(), 2);

        // Access k2 to boost its score
        let _ = cache.lookup(&k2);

        // Insert a third entry -- should evict k1 (lower score)
        let k3 = canonicalize(&[eq_clause("nsfwLevel", 3)]).unwrap();
        cache.store(&k3, make_bitmap(&[3]));

        assert_eq!(cache.len(), 2);
        // k1 should be evicted (lower score)
        match cache.lookup(&k1) {
            CacheLookup::Miss => {}
            _ => panic!("k1 should have been evicted"),
        }
    }

    #[test]
    fn test_maintenance_decay() {
        let config = CacheConfig {
            max_entries: 100,
            decay_rate: 0.5,
            ..Default::default()
        };
        let mut cache = TrieCache::new(config);

        let key = canonicalize(&[eq_clause("nsfwLevel", 1)]).unwrap();
        cache.store(&key, make_bitmap(&[1]));

        // Run several decay cycles -- hit_score starts at 1.0
        // After cycle: 0.5 -> 0.25 -> 0.125 -> 0.0625 -> 0.03125 -> 0.015625 -> 0.0078125
        for _ in 0..7 {
            cache.maintenance_cycle();
        }

        // Entry should be evicted after enough decay (below 0.01 threshold)
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_overwrite_existing_entry() {
        let mut cache = TrieCache::new(make_config(100));

        let key = canonicalize(&[eq_clause("nsfwLevel", 1)]).unwrap();
        cache.store(&key, make_bitmap(&[1, 2, 3]));
        cache.store(&key, make_bitmap(&[4, 5, 6]));

        assert_eq!(cache.len(), 1);
        match cache.lookup(&key) {
            CacheLookup::ExactHit(bm) => assert_eq!(bm, make_bitmap(&[4, 5, 6])),
            _ => panic!("expected exact hit with updated bitmap"),
        }
    }

    #[test]
    fn test_clear() {
        let mut cache = TrieCache::new(make_config(100));

        let k1 = canonicalize(&[eq_clause("nsfwLevel", 1)]).unwrap();
        let k2 = canonicalize(&[eq_clause("nsfwLevel", 2)]).unwrap();
        cache.store(&k1, make_bitmap(&[1]));
        cache.store(&k2, make_bitmap(&[2]));

        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_multi_field_cache_key() {
        let mut cache = TrieCache::new(make_config(100));

        let clauses = vec![
            eq_clause("nsfwLevel", 1),
            eq_clause("onSite", 1),
            eq_clause("userId", 42),
        ];
        let key = canonicalize(&clauses).unwrap();
        cache.store(&key, make_bitmap(&[10, 20, 30]));

        // Same query, different clause order -> same canonical key
        let clauses_reordered = vec![
            eq_clause("userId", 42),
            eq_clause("nsfwLevel", 1),
            eq_clause("onSite", 1),
        ];
        let key_reordered = canonicalize(&clauses_reordered).unwrap();

        match cache.lookup(&key_reordered) {
            CacheLookup::ExactHit(bm) => assert_eq!(bm, make_bitmap(&[10, 20, 30])),
            _ => panic!("expected exact hit with reordered clauses"),
        }
    }

    #[test]
    fn test_prefix_hit_with_multi_field_key() {
        let mut cache = TrieCache::new(make_config(100));

        // Cache {nsfwLevel=1, onSite=1}
        let prefix = vec![eq_clause("nsfwLevel", 1), eq_clause("onSite", 1)];
        let prefix_key = canonicalize(&prefix).unwrap();
        cache.store(&prefix_key, make_bitmap(&[1, 2, 3]));

        // Query {nsfwLevel=1, onSite=1, userId=42} -> prefix hit on 2 of 3
        let full = vec![
            eq_clause("nsfwLevel", 1),
            eq_clause("onSite", 1),
            eq_clause("userId", 42),
        ];
        let full_key = canonicalize(&full).unwrap();

        match cache.lookup(&full_key) {
            CacheLookup::PrefixHit { bitmap, matched_prefix_len } => {
                assert_eq!(bitmap, make_bitmap(&[1, 2, 3]));
                assert_eq!(matched_prefix_len, 2);
            }
            _ => panic!("expected prefix hit"),
        }
    }

    #[test]
    fn test_invalidation_of_prefix_entry() {
        let mut cache = TrieCache::new(make_config(100));

        // Cache prefix {nsfwLevel=1}
        let prefix = vec![eq_clause("nsfwLevel", 1)];
        let prefix_key = canonicalize(&prefix).unwrap();
        cache.store(&prefix_key, make_bitmap(&[1, 2, 3]));

        // Cache full {nsfwLevel=1, userId=42}
        let full = vec![eq_clause("nsfwLevel", 1), eq_clause("userId", 42)];
        let full_key = canonicalize(&full).unwrap();
        cache.store(&full_key, make_bitmap(&[1]));

        // Invalidate nsfwLevel -- both entries become stale
        cache.invalidate_field("nsfwLevel");

        match cache.lookup(&full_key) {
            CacheLookup::Miss => {}
            _ => panic!("expected miss after field invalidation"),
        }
    }

    #[test]
    fn test_empty_key() {
        let mut cache = TrieCache::new(make_config(100));
        let key: CacheKey = Vec::new();

        // Storing and looking up empty key should be no-ops
        cache.store(&key, make_bitmap(&[1, 2, 3]));
        assert_eq!(cache.len(), 0);

        match cache.lookup(&key) {
            CacheLookup::Miss => {}
            _ => panic!("expected miss for empty key"),
        }
    }

    #[test]
    fn test_not_eq_clause_in_key() {
        let clauses = vec![
            FilterClause::NotEq("nsfwLevel".to_string(), Value::Integer(28)),
        ];
        let key = canonicalize(&clauses).unwrap();
        assert_eq!(key.len(), 1);
        assert_eq!(key[0].op, "neq");
    }

    #[test]
    fn test_range_clauses_in_key() {
        let clauses = vec![
            FilterClause::Gte("nsfwLevel".to_string(), Value::Integer(1)),
            FilterClause::Lt("nsfwLevel".to_string(), Value::Integer(28)),
        ];
        let key = canonicalize(&clauses).unwrap();
        assert_eq!(key.len(), 2);
    }

    fn make_bucket_bitmap(field: &str, bucket_name: &str) -> FilterClause {
        FilterClause::BucketBitmap {
            field: field.to_string(),
            bucket_name: bucket_name.to_string(),
            bitmap: std::sync::Arc::new(RoaringBitmap::new()),
        }
    }

    #[test]
    fn test_bucket_bitmap_produces_stable_key() {
        // Two BucketBitmap clauses with the same field+bucket_name but different internal bitmaps
        // must produce the same cache key (C5).
        let clause_a = make_bucket_bitmap("sortAt", "7d");
        let clause_b = FilterClause::BucketBitmap {
            field: "sortAt".to_string(),
            bucket_name: "7d".to_string(),
            bitmap: std::sync::Arc::new({
                let mut bm = RoaringBitmap::new();
                bm.insert(42);
                bm
            }),
        };

        let key_a = canonicalize(&[clause_a]).unwrap();
        let key_b = canonicalize(&[clause_b]).unwrap();

        assert_eq!(key_a, key_b);
        assert_eq!(key_a[0].op, "bucket");
        assert_eq!(key_a[0].value_repr, "7d");
    }

    #[test]
    fn test_bucket_bitmap_key_field_name() {
        let clause = make_bucket_bitmap("sortAt", "24h");
        let key = canonicalize(&[clause]).unwrap();
        assert_eq!(key[0].field, "sortAt");
        assert_eq!(key[0].op, "bucket");
        assert_eq!(key[0].value_repr, "24h");
    }

    #[test]
    fn test_bucket_bitmap_different_bucket_names_different_keys() {
        let key_24h = canonicalize(&[make_bucket_bitmap("sortAt", "24h")]).unwrap();
        let key_7d = canonicalize(&[make_bucket_bitmap("sortAt", "7d")]).unwrap();
        assert_ne!(key_24h, key_7d);
    }

    #[test]
    fn test_bucket_bitmap_stable_key_cache_hit() {
        // Store under 7d bucket, look up with a new BucketBitmap (different bitmap content)
        // — should get an exact hit because cache key is based on bucket name, not bitmap content.
        let mut cache = TrieCache::new(make_config(100));

        let clause_store = make_bucket_bitmap("sortAt", "7d");
        let key = canonicalize(&[clause_store]).unwrap();
        cache.store(&key, make_bitmap(&[1, 2, 3]));

        // Look up with a different bitmap content but same field/bucket_name
        let clause_lookup = FilterClause::BucketBitmap {
            field: "sortAt".to_string(),
            bucket_name: "7d".to_string(),
            bitmap: std::sync::Arc::new({
                let mut bm = RoaringBitmap::new();
                bm.insert(99);
                bm
            }),
        };
        let lookup_key = canonicalize(&[clause_lookup]).unwrap();

        match cache.lookup(&lookup_key) {
            CacheLookup::ExactHit(bm) => assert_eq!(bm, make_bitmap(&[1, 2, 3])),
            _ => panic!("expected exact hit for stable bucket key"),
        }
    }

    #[test]
    fn test_bucket_bitmap_combined_with_eq_in_key() {
        // BucketBitmap + Eq should produce a 2-element canonical key, sorted by field.
        let bucket = make_bucket_bitmap("sortAt", "7d");
        let eq = eq_clause("nsfwLevel", 1);
        let key = canonicalize(&[bucket, eq]).unwrap();
        assert_eq!(key.len(), 2);
        // Sorted by field: "nsfwLevel" < "sortAt"
        assert_eq!(key[0].field, "nsfwLevel");
        assert_eq!(key[1].field, "sortAt");
        assert_eq!(key[1].op, "bucket");
    }

    fn make_bucket_manager(now: u64) -> crate::time_buckets::TimeBucketManager {
        use crate::config::BucketConfig;
        let configs = vec![
            BucketConfig { name: "24h".to_string(), duration_secs: 86400, refresh_interval_secs: 300 },
            BucketConfig { name: "7d".to_string(), duration_secs: 604800, refresh_interval_secs: 3600 },
        ];
        let mut mgr = crate::time_buckets::TimeBucketManager::new("sortAt".to_string(), configs);
        mgr.rebuild_bucket("24h", std::iter::empty(), now);
        mgr.rebuild_bucket("7d", std::iter::empty(), now);
        mgr
    }

    #[test]
    fn test_snap_clause_key_gte_snaps_to_24h() {
        let now: u64 = 1_700_000_000;
        let mgr = make_bucket_manager(now);

        // Gte("sortAt", now - 86400) — exact 24h, should snap to bucket_gte:24h
        let ts = (now - 86400) as i64;
        let clause = FilterClause::Gte("sortAt".to_string(), Value::Integer(ts));
        let snapped = snap_clause_key(&clause, &mgr, now).unwrap();

        assert_eq!(snapped.field, "sortAt");
        assert_eq!(snapped.op, "bucket_gte");
        assert_eq!(snapped.value_repr, "24h");
    }

    #[test]
    fn test_snap_clause_key_gt_snaps_to_7d() {
        let now: u64 = 1_700_000_000;
        let mgr = make_bucket_manager(now);

        // Gt("sortAt", now - 590000) — within 10% of 7d (604800)
        let ts = (now - 590000) as i64;
        let clause = FilterClause::Gt("sortAt".to_string(), Value::Integer(ts));
        let snapped = snap_clause_key(&clause, &mgr, now).unwrap();

        assert_eq!(snapped.op, "bucket_gte");
        assert_eq!(snapped.value_repr, "7d");
    }

    #[test]
    fn test_snap_clause_key_outside_tolerance_returns_none() {
        let now: u64 = 1_700_000_000;
        let mgr = make_bucket_manager(now);

        // Duration = 200000s — outside tolerance of both 24h and 7d
        let ts = (now - 200000) as i64;
        let clause = FilterClause::Gte("sortAt".to_string(), Value::Integer(ts));
        assert!(snap_clause_key(&clause, &mgr, now).is_none());
    }

    #[test]
    fn test_snap_clause_key_non_bucketed_field_returns_none() {
        let now: u64 = 1_700_000_000;
        let mgr = make_bucket_manager(now);

        // nsfwLevel is not the bucketed field (sortAt)
        let clause = FilterClause::Gte("nsfwLevel".to_string(), Value::Integer(1));
        assert!(snap_clause_key(&clause, &mgr, now).is_none());
    }

    #[test]
    fn test_snap_clause_key_non_range_clause_returns_none() {
        let now: u64 = 1_700_000_000;
        let mgr = make_bucket_manager(now);

        let clause = FilterClause::Eq("sortAt".to_string(), Value::Integer(1_700_000_000));
        assert!(snap_clause_key(&clause, &mgr, now).is_none());
    }

    #[test]
    fn test_canonicalize_with_buckets_snaps_gte() {
        let now: u64 = 1_700_000_000;
        let mgr = make_bucket_manager(now);

        let ts = (now - 86400) as i64;
        let clauses = vec![
            FilterClause::Gte("sortAt".to_string(), Value::Integer(ts)),
            eq_clause("nsfwLevel", 1),
        ];
        let key = canonicalize_with_buckets(&clauses, Some(&mgr), now).unwrap();

        assert_eq!(key.len(), 2);
        // Sorted by field: "nsfwLevel" < "sortAt"
        assert_eq!(key[0].field, "nsfwLevel");
        assert_eq!(key[0].op, "eq");
        assert_eq!(key[1].field, "sortAt");
        assert_eq!(key[1].op, "bucket_gte");
        assert_eq!(key[1].value_repr, "24h");
    }

    #[test]
    fn test_canonicalize_with_buckets_stable_key_across_different_timestamps() {
        let now: u64 = 1_700_000_000;
        let mgr = make_bucket_manager(now);

        // Two slightly different timestamps that both snap to 24h
        let ts1 = (now - 86400) as i64;
        let ts2 = (now - 83000) as i64; // within 10% tolerance of 24h

        let key1 = canonicalize_with_buckets(
            &[FilterClause::Gte("sortAt".to_string(), Value::Integer(ts1))],
            Some(&mgr),
            now,
        ).unwrap();
        let key2 = canonicalize_with_buckets(
            &[FilterClause::Gte("sortAt".to_string(), Value::Integer(ts2))],
            Some(&mgr),
            now,
        ).unwrap();

        // Both should produce the same stable cache key
        assert_eq!(key1, key2);
        assert_eq!(key1[0].value_repr, "24h");
    }

    #[test]
    fn test_canonicalize_with_buckets_falls_through_for_eq() {
        let now: u64 = 1_700_000_000;
        let mgr = make_bucket_manager(now);

        let clauses = vec![eq_clause("nsfwLevel", 1)];
        let key = canonicalize_with_buckets(&clauses, Some(&mgr), now).unwrap();

        assert_eq!(key.len(), 1);
        assert_eq!(key[0].op, "eq");
    }

    #[test]
    fn test_canonicalize_with_buckets_none_tb_behaves_like_canonicalize() {
        let clauses = vec![
            eq_clause("nsfwLevel", 1),
            eq_clause("userId", 42),
        ];
        let key_plain = canonicalize(&clauses).unwrap();
        let key_buckets = canonicalize_with_buckets(&clauses, None, 0).unwrap();
        assert_eq!(key_plain, key_buckets);
    }

    // === Live update tests ===

    #[test]
    fn test_live_update_insert_slot() {
        let mut cache = TrieCache::new(make_config(100));
        let clauses = vec![eq_clause("nsfwLevel", 1)];
        let key = canonicalize(&clauses).unwrap();

        // Store a bitmap with slots [10, 20, 30]
        cache.store(&key, make_bitmap(&[10, 20, 30]));

        // The entry should have a meta_id since it's all Eq clauses
        let meta_ids: Vec<u32> = cache.meta().entries_for_clause("nsfwLevel", "eq", "1")
            .map(|bm| bm.iter().collect())
            .unwrap_or_default();
        assert_eq!(meta_ids.len(), 1);

        // Live-update: insert slot 40
        cache.update_entry_by_id(meta_ids[0], 40, true);

        // Verify the entry now contains slot 40
        match cache.lookup(&key) {
            CacheLookup::ExactHit(bm) => {
                assert!(bm.contains(10));
                assert!(bm.contains(40));
                assert_eq!(bm.len(), 4);
            }
            _ => panic!("Expected exact hit"),
        }
    }

    #[test]
    fn test_live_update_remove_slot() {
        let mut cache = TrieCache::new(make_config(100));
        let clauses = vec![eq_clause("nsfwLevel", 1)];
        let key = canonicalize(&clauses).unwrap();

        cache.store(&key, make_bitmap(&[10, 20, 30]));

        let meta_ids: Vec<u32> = cache.meta().entries_for_clause("nsfwLevel", "eq", "1")
            .map(|bm| bm.iter().collect())
            .unwrap_or_default();

        // Live-update: remove slot 20
        cache.update_entry_by_id(meta_ids[0], 20, false);

        match cache.lookup(&key) {
            CacheLookup::ExactHit(bm) => {
                assert!(bm.contains(10));
                assert!(!bm.contains(20));
                assert!(bm.contains(30));
                assert_eq!(bm.len(), 2);
            }
            _ => panic!("Expected exact hit"),
        }
    }

    #[test]
    fn test_live_update_survives_generation_bump_with_refresh() {
        let mut cache = TrieCache::new(make_config(100));
        let clauses = vec![eq_clause("nsfwLevel", 1)];
        let key = canonicalize(&clauses).unwrap();

        cache.store(&key, make_bitmap(&[10, 20]));

        let meta_ids: Vec<u32> = cache.meta().entries_for_clause("nsfwLevel", "eq", "1")
            .map(|bm| bm.iter().collect())
            .unwrap_or_default();
        let id = meta_ids[0];

        // Live-update
        cache.update_entry_by_id(id, 30, true);

        // Bump generation (simulates non-Eq invalidation)
        cache.invalidate_field("nsfwLevel");

        // Refresh the entry's generation
        cache.refresh_entry_generation(id, "nsfwLevel");

        // Entry should still be valid
        match cache.lookup(&key) {
            CacheLookup::ExactHit(bm) => {
                assert_eq!(bm.len(), 3);
                assert!(bm.contains(30));
            }
            _ => panic!("Expected exact hit after refresh"),
        }
    }

    #[test]
    fn test_non_eq_entry_not_registered_in_meta() {
        let mut cache = TrieCache::new(make_config(100));
        // NotEq clause — should NOT be registered in meta-index
        let clauses = vec![FilterClause::NotEq("nsfwLevel".to_string(), Value::Integer(28))];
        let key = canonicalize(&clauses).unwrap();

        cache.store(&key, make_bitmap(&[10, 20]));

        // Meta-index should have no entries for "neq"
        assert!(cache.meta().entries_for_clause("nsfwLevel", "neq", "28").is_none());
        assert_eq!(cache.meta().entry_count(), 0);
    }

    #[test]
    fn test_meta_deregister_on_eviction() {
        let mut cache = TrieCache::new(make_config(2)); // max 2 entries
        let key1 = canonicalize(&[eq_clause("nsfwLevel", 1)]).unwrap();
        let key2 = canonicalize(&[eq_clause("nsfwLevel", 2)]).unwrap();
        let key3 = canonicalize(&[eq_clause("userId", 42)]).unwrap();

        cache.store(&key1, make_bitmap(&[10]));
        cache.store(&key2, make_bitmap(&[20]));
        assert_eq!(cache.meta().entry_count(), 2);

        // Storing a third entry triggers LRU eviction
        cache.store(&key3, make_bitmap(&[30]));
        assert_eq!(cache.len(), 2);
        // Meta-index should also have exactly 2 entries
        assert_eq!(cache.meta().entry_count(), 2);
    }

    #[test]
    fn test_meta_deregister_on_decay() {
        let mut cache = TrieCache::new(CacheConfig {
            max_entries: 100,
            decay_rate: 0.001, // aggressive decay — entries die quickly
            ..Default::default()
        });
        let key = canonicalize(&[eq_clause("nsfwLevel", 1)]).unwrap();
        cache.store(&key, make_bitmap(&[10]));
        assert_eq!(cache.meta().entry_count(), 1);

        // Run maintenance to decay the entry below threshold
        cache.maintenance_cycle();
        cache.maintenance_cycle();

        assert_eq!(cache.len(), 0);
        assert_eq!(cache.meta().entry_count(), 0);
    }

    #[test]
    fn test_live_update_stale_entry_skipped() {
        let mut cache = TrieCache::new(make_config(100));
        let clauses = vec![eq_clause("nsfwLevel", 1)];
        let key = canonicalize(&clauses).unwrap();

        cache.store(&key, make_bitmap(&[10, 20]));

        let meta_ids: Vec<u32> = cache.meta().entries_for_clause("nsfwLevel", "eq", "1")
            .map(|bm| bm.iter().collect())
            .unwrap_or_default();
        let id = meta_ids[0];

        // Bump generation WITHOUT refresh — entry is now stale
        cache.invalidate_field("nsfwLevel");

        // Live-update should be skipped (entry is stale)
        cache.update_entry_by_id(id, 30, true);

        // Lookup should miss (stale generation)
        match cache.lookup(&key) {
            CacheLookup::Miss => {} // correct — stale entry discarded
            _ => panic!("Expected miss for stale entry"),
        }
    }
}
