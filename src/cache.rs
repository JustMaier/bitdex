use std::collections::HashMap;
use std::time::Instant;

use roaring::RoaringBitmap;

use crate::config::CacheConfig;
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
    fn from_filter(clause: &FilterClause) -> Option<Self> {
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

/// A cached entry in the trie.
struct CacheEntry {
    /// The cached filter result bitmap.
    bitmap: RoaringBitmap,
    /// Hit count with exponential decay.
    hit_score: f64,
    /// Last access time.
    last_access: Instant,
    /// Generation counters at the time this entry was cached.
    /// Maps field name -> generation at cache time.
    field_generations: HashMap<String, u64>,
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
    /// Exact match with valid bitmap.
    ExactHit(RoaringBitmap),
    /// Prefix match: cached bitmap for a prefix of the query.
    /// The second element contains the remaining (uncached) filter clauses.
    PrefixHit {
        bitmap: RoaringBitmap,
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
}

impl TrieCache {
    pub fn new(config: CacheConfig) -> Self {
        Self {
            root: TrieNode::new(),
            field_generations: HashMap::new(),
            config,
            entry_count: 0,
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
        let mut best_prefix_bitmap: Option<RoaringBitmap> = None;
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
    pub fn store(&mut self, key: &CacheKey, bitmap: RoaringBitmap) {
        if key.is_empty() {
            return;
        }

        // Evict if at capacity
        if self.entry_count >= self.config.max_entries {
            self.evict_lru();
        }

        let field_gens = self.current_field_generations(key);

        let mut node = &mut self.root;
        for clause_key in key {
            node = node
                .children
                .entry(clause_key.clone())
                .or_insert_with(TrieNode::new);
        }

        let was_none = node.entry.is_none();
        node.entry = Some(CacheEntry {
            bitmap,
            hit_score: 1.0,
            last_access: Instant::now(),
            field_generations: field_gens,
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

    /// Run the promotion/demotion cycle.
    /// Applies decay to all hit scores and evicts entries below a threshold.
    pub fn maintenance_cycle(&mut self) {
        let decay_rate = self.config.decay_rate;
        Self::decay_node(&mut self.root, decay_rate, &mut self.entry_count);
    }

    /// Get the number of cached entries.
    pub fn len(&self) -> usize {
        self.entry_count
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Clear all cached entries.
    pub fn clear(&mut self) {
        self.root = TrieNode::new();
        self.entry_count = 0;
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
        if node.entry.is_some() {
            node.entry = None;
            self.entry_count -= 1;
        }
    }

    /// Recursively decay hit scores and remove entries with near-zero scores.
    fn decay_node(node: &mut TrieNode, decay_rate: f64, entry_count: &mut usize) {
        if let Some(entry) = &mut node.entry {
            entry.hit_score *= decay_rate;
            if entry.hit_score < 0.01 {
                node.entry = None;
                *entry_count -= 1;
            }
        }

        // Process children, collecting empty ones to clean up
        let mut empty_children = Vec::new();
        for (key, child) in node.children.iter_mut() {
            Self::decay_node(child, decay_rate, entry_count);
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
        }
    }

    fn eq_clause(field: &str, value: i64) -> FilterClause {
        FilterClause::Eq(field.to_string(), Value::Integer(value))
    }

    fn make_bitmap(slots: &[u32]) -> RoaringBitmap {
        let mut bm = RoaringBitmap::new();
        for &s in slots {
            bm.insert(s);
        }
        bm
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
}
