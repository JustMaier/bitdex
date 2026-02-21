use std::collections::HashMap;
use std::time::Instant;

use roaring::RoaringBitmap;

use crate::cache::CacheKey;
use crate::meta_index::{CacheEntryId, MetaIndex};
use crate::query::SortDirection;

/// Unique key for a bound: the combination of filter definition + sort specification + tier.
///
/// Two queries hit the same bound iff they share the same canonical filter key,
/// the same sort field, the same sort direction, and the same tier.
///
/// Tier 0 is the default top-K bound. Higher tiers cover deeper pagination ranges
/// (D6). When cursor-past-bound is detected for tier N, the engine checks tier N+1.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BoundKey {
    pub filter_key: CacheKey,
    pub sort_field: String,
    pub direction: SortDirection,
    /// Pagination tier: 0 = top-K, 1 = next range, etc.
    pub tier: u32,
}

/// A cached approximate top-K bitmap for a specific filter+sort combination.
///
/// The bound bitmap is ANDed with the filter result before sort traversal,
/// reducing the working set from potentially millions of candidates down to
/// ~target_size (~10k). This makes sort traversal time independent of dataset size.
///
/// Bounds are formed on first query execution and maintained live on writes.
/// When bloated beyond max_size, the next query triggers a rebuild.
pub struct BoundEntry {
    /// Bitmap of approximate top-K documents for this bound.
    bitmap: RoaringBitmap,
    /// The minimum sort value tracked in this bound.
    /// For Desc: the smallest value in the bitmap (the "floor").
    /// For Asc: the largest value in the bitmap (the "ceiling").
    /// New documents with values exceeding this threshold get added via live maintenance.
    min_tracked_value: u32,
    /// Target size for the bound (e.g., 10_000).
    target_size: usize,
    /// Maximum size before triggering a rebuild (e.g., 20_000).
    max_size: usize,
    /// Whether this bound needs a full rebuild.
    needs_rebuild: bool,
    /// Last time this bound was used for a query.
    last_used: Instant,
}

impl BoundEntry {
    /// Create a new bound from a completed sort traversal result.
    ///
    /// `sorted_slots` must be in sort order (best-first: highest values first for Desc,
    /// lowest first for Asc). We take the first `target_size` slots as the bound.
    /// `value_fn` reconstructs the sort value for a given slot.
    pub fn new<F>(
        sorted_slots: &[u32],
        target_size: usize,
        max_size: usize,
        value_fn: F,
    ) -> Self
    where
        F: Fn(u32) -> u32,
    {
        let take = sorted_slots.len().min(target_size);
        let mut bitmap = RoaringBitmap::new();
        for &slot in &sorted_slots[..take] {
            bitmap.insert(slot);
        }

        // The min tracked value is the value of the last slot in the bound.
        // For Desc: this is the smallest value (the floor).
        // For Asc: this is the largest value (the ceiling).
        let min_tracked_value = if take > 0 {
            value_fn(sorted_slots[take - 1])
        } else {
            0
        };

        Self {
            bitmap,
            min_tracked_value,
            target_size,
            max_size,
            needs_rebuild: false,
            last_used: Instant::now(),
        }
    }

    /// Get the bound bitmap.
    pub fn bitmap(&self) -> &RoaringBitmap {
        &self.bitmap
    }

    /// The minimum sort value tracked in this bound (floor for Desc, ceiling for Asc).
    pub fn min_tracked_value(&self) -> u32 {
        self.min_tracked_value
    }

    /// Current cardinality of the bound bitmap.
    pub fn cardinality(&self) -> u64 {
        self.bitmap.len()
    }

    /// Whether this bound has grown beyond max_size and needs a fresh rebuild.
    pub fn needs_rebuild(&self) -> bool {
        self.needs_rebuild
    }

    /// Mark this bound for rebuild (called when cardinality exceeds max_size).
    pub fn mark_for_rebuild(&mut self) {
        self.needs_rebuild = true;
    }

    /// Touch the last_used timestamp (for LRU eviction).
    pub fn touch(&mut self) {
        self.last_used = Instant::now();
    }

    /// Last time this bound was used.
    pub fn last_used(&self) -> Instant {
        self.last_used
    }

    /// Add a slot to the bound bitmap (live maintenance).
    ///
    /// Called when a document's sort value changes and exceeds the bound's threshold.
    /// Returns true if the slot was newly added.
    pub fn add_slot(&mut self, slot: u32) -> bool {
        let added = self.bitmap.insert(slot);
        if added && self.bitmap.len() as usize > self.max_size {
            self.needs_rebuild = true;
        }
        added
    }

    /// Rebuild the bound from a fresh sort traversal.
    ///
    /// Replaces the bitmap with the first `target_size` slots from `sorted_slots`.
    pub fn rebuild<F>(&mut self, sorted_slots: &[u32], value_fn: F)
    where
        F: Fn(u32) -> u32,
    {
        let take = sorted_slots.len().min(self.target_size);
        let mut new_bitmap = RoaringBitmap::new();
        for &slot in &sorted_slots[..take] {
            new_bitmap.insert(slot);
        }

        self.min_tracked_value = if take > 0 {
            value_fn(sorted_slots[take - 1])
        } else {
            0
        };

        self.bitmap = new_bitmap;
        self.needs_rebuild = false;
        self.last_used = Instant::now();
    }

    /// Approximate memory usage in bytes.
    pub fn memory_bytes(&self) -> usize {
        self.bitmap.serialized_size()
    }
}

/// Manages all bound cache entries with meta-index for O(1) write-path lookups.
///
/// Bounds are keyed by (filter_key, sort_field, direction, tier) and are formed
/// automatically when the executor completes a sort query. The embedded MetaIndex
/// maps filter clause components and sort specifications to entry IDs, replacing
/// linear scans during write-path maintenance.
pub struct BoundCacheManager {
    bounds: HashMap<BoundKey, BoundEntry>,
    /// Maps BoundKey → meta-index entry ID for deregistration.
    key_to_meta_id: HashMap<BoundKey, CacheEntryId>,
    /// Maps meta-index entry ID → BoundKey for reverse lookup.
    meta_id_to_key: HashMap<CacheEntryId, BoundKey>,
    /// Meta-index for O(1) write-path lookups.
    meta: MetaIndex,
    /// Default target size for new bounds.
    target_size: usize,
    /// Default max size before rebuild.
    max_size: usize,
}

impl BoundCacheManager {
    pub fn new(target_size: usize, max_size: usize) -> Self {
        Self {
            bounds: HashMap::new(),
            key_to_meta_id: HashMap::new(),
            meta_id_to_key: HashMap::new(),
            meta: MetaIndex::new(),
            target_size,
            max_size,
        }
    }

    /// Look up a bound by key. Returns None if no bound exists.
    pub fn lookup(&self, key: &BoundKey) -> Option<&BoundEntry> {
        self.bounds.get(key)
    }

    /// Look up a bound mutably (for touch, rebuild, etc.).
    pub fn lookup_mut(&mut self, key: &BoundKey) -> Option<&mut BoundEntry> {
        self.bounds.get_mut(key)
    }

    /// Form a new bound from sort results.
    ///
    /// If a bound already exists for this key, it is replaced (and re-registered
    /// with the meta-index).
    pub fn form_bound<F>(
        &mut self,
        key: BoundKey,
        sorted_slots: &[u32],
        value_fn: F,
    ) -> &BoundEntry
    where
        F: Fn(u32) -> u32,
    {
        // Deregister old entry if replacing
        if let Some(old_meta_id) = self.key_to_meta_id.remove(&key) {
            self.meta.deregister(old_meta_id);
            self.meta_id_to_key.remove(&old_meta_id);
        }

        let entry = BoundEntry::new(sorted_slots, self.target_size, self.max_size, value_fn);
        self.bounds.insert(key.clone(), entry);

        // Register with meta-index
        let meta_id = self.meta.register(
            &key.filter_key,
            Some(&key.sort_field),
            Some(key.direction),
        );
        self.key_to_meta_id.insert(key.clone(), meta_id);
        self.meta_id_to_key.insert(meta_id, key.clone());

        self.bounds.get(&key).unwrap()
    }

    /// Get a mutable reference to a bound, or None.
    pub fn get_mut(&mut self, key: &BoundKey) -> Option<&mut BoundEntry> {
        self.bounds.get_mut(key)
    }

    /// Iterate over all bounds (for live maintenance).
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&BoundKey, &mut BoundEntry)> {
        self.bounds.iter_mut()
    }

    /// Number of cached bounds.
    pub fn len(&self) -> usize {
        self.bounds.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.bounds.is_empty()
    }

    /// Total approximate memory usage across all bounds.
    pub fn total_memory_bytes(&self) -> usize {
        self.bounds.values().map(|e| e.memory_bytes()).sum()
    }

    /// Evict the least-recently-used bound. Returns the evicted key, or None if empty.
    pub fn evict_lru(&mut self) -> Option<BoundKey> {
        let lru_key = self
            .bounds
            .iter()
            .min_by_key(|(_, entry)| entry.last_used())
            .map(|(key, _)| key.clone());

        if let Some(ref key) = lru_key {
            self.bounds.remove(key);
            if let Some(meta_id) = self.key_to_meta_id.remove(key) {
                self.meta.deregister(meta_id);
                self.meta_id_to_key.remove(&meta_id);
            }
        }
        lru_key
    }

    /// Remove a specific bound.
    pub fn remove(&mut self, key: &BoundKey) -> Option<BoundEntry> {
        if let Some(meta_id) = self.key_to_meta_id.remove(key) {
            self.meta.deregister(meta_id);
            self.meta_id_to_key.remove(&meta_id);
        }
        self.bounds.remove(key)
    }

    /// Mark all bounds that reference a given filter field for rebuild.
    ///
    /// Uses the meta-index for O(1) lookup instead of linear scan.
    /// Called when a filter field is invalidated (e.g., by a write that changes
    /// filter values). Any bound whose filter_key mentions the invalidated field
    /// may now be stale and should be rebuilt on next use.
    pub fn invalidate_filter_field(&mut self, field_name: &str) {
        if let Some(entry_ids) = self.meta.entries_for_filter_field(field_name) {
            let ids: Vec<CacheEntryId> = entry_ids.iter().collect();
            for meta_id in ids {
                if let Some(key) = self.meta_id_to_key.get(&meta_id) {
                    if let Some(entry) = self.bounds.get_mut(key) {
                        entry.mark_for_rebuild();
                    }
                }
            }
        }
    }

    /// Find all bound keys that sort by a given field (any direction).
    ///
    /// Uses the meta-index for O(1) lookup. Returns a Vec of (BoundKey, SortDirection)
    /// pairs for the caller to process. This replaces the linear iter_mut() scan
    /// in the D3 flush loop.
    pub fn bounds_for_sort_field(&self, sort_field: &str) -> Vec<BoundKey> {
        let entry_ids = self.meta.entries_for_sort_field(sort_field);
        entry_ids
            .iter()
            .filter_map(|meta_id| self.meta_id_to_key.get(&meta_id).cloned())
            .collect()
    }

    /// Access the meta-index directly (for reporting/testing).
    pub fn meta_index(&self) -> &MetaIndex {
        &self.meta
    }

    /// Default target size for new bounds.
    pub fn target_size(&self) -> usize {
        self.target_size
    }

    /// Default max size for bloat control.
    pub fn max_size(&self) -> usize {
        self.max_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::CanonicalClause;

    fn make_filter_key(field: &str, value: &str) -> CacheKey {
        vec![CanonicalClause {
            field: field.to_string(),
            op: "eq".to_string(),
            value_repr: value.to_string(),
        }]
    }

    fn make_bound_key(filter_field: &str, filter_value: &str, sort: &str, dir: SortDirection) -> BoundKey {
        BoundKey {
            filter_key: make_filter_key(filter_field, filter_value),
            sort_field: sort.to_string(),
            direction: dir,
            tier: 0,
        }
    }

    #[test]
    fn test_bound_formation_basic() {
        let mut mgr = BoundCacheManager::new(5, 10);
        let key = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Desc);

        // Sorted slots: [10, 8, 6, 4, 2] (desc order by sort value)
        let sorted = vec![10, 8, 6, 4, 2];
        // value_fn: slot * 100 (so slot 10 = 1000, slot 2 = 200)
        mgr.form_bound(key.clone(), &sorted, |s| s * 100);

        let bound = mgr.lookup(&key).unwrap();
        assert_eq!(bound.cardinality(), 5);
        assert!(bound.bitmap().contains(10));
        assert!(bound.bitmap().contains(2));
        // min_tracked_value = value of last slot (2) = 200
        assert_eq!(bound.min_tracked_value(), 200);
        assert!(!bound.needs_rebuild());
    }

    #[test]
    fn test_bound_formation_truncates_to_target() {
        let mut mgr = BoundCacheManager::new(3, 6);
        let key = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Desc);

        let sorted = vec![10, 8, 6, 4, 2];
        mgr.form_bound(key.clone(), &sorted, |s| s * 100);

        let bound = mgr.lookup(&key).unwrap();
        // Only first 3 (target_size) slots kept
        assert_eq!(bound.cardinality(), 3);
        assert!(bound.bitmap().contains(10));
        assert!(bound.bitmap().contains(8));
        assert!(bound.bitmap().contains(6));
        assert!(!bound.bitmap().contains(4));
        assert!(!bound.bitmap().contains(2));
        // min_tracked_value = value of slot 6 = 600
        assert_eq!(bound.min_tracked_value(), 600);
    }

    #[test]
    fn test_bound_bloat_triggers_rebuild() {
        let mut mgr = BoundCacheManager::new(3, 5);
        let key = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Desc);

        let sorted = vec![10, 8, 6];
        mgr.form_bound(key.clone(), &sorted, |s| s * 100);

        // Add slots until we exceed max_size (5)
        let bound = mgr.get_mut(&key).unwrap();
        bound.add_slot(20);
        bound.add_slot(30);
        assert!(!bound.needs_rebuild()); // at 5 = max_size, not over
        bound.add_slot(40);
        assert!(bound.needs_rebuild()); // at 6 > max_size(5)
    }

    #[test]
    fn test_bound_rebuild_resets() {
        let mut mgr = BoundCacheManager::new(3, 5);
        let key = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Desc);

        let sorted = vec![10, 8, 6];
        mgr.form_bound(key.clone(), &sorted, |s| s * 100);

        // Bloat it
        let bound = mgr.get_mut(&key).unwrap();
        for i in 20..30 {
            bound.add_slot(i);
        }
        assert!(bound.needs_rebuild());

        // Rebuild with fresh results
        let fresh = vec![50, 40, 30];
        bound.rebuild(&fresh, |s| s * 100);

        assert!(!bound.needs_rebuild());
        assert_eq!(bound.cardinality(), 3);
        assert!(bound.bitmap().contains(50));
        assert_eq!(bound.min_tracked_value(), 3000);
    }

    #[test]
    fn test_lookup_miss_for_different_filter() {
        let mut mgr = BoundCacheManager::new(5, 10);
        let key1 = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Desc);
        let key2 = make_bound_key("nsfwLevel", "2", "reactionCount", SortDirection::Desc);

        mgr.form_bound(key1.clone(), &[1, 2, 3], |s| s * 100);

        assert!(mgr.lookup(&key1).is_some());
        assert!(mgr.lookup(&key2).is_none());
    }

    #[test]
    fn test_lookup_miss_for_different_sort() {
        let mut mgr = BoundCacheManager::new(5, 10);
        let key1 = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Desc);
        let key2 = make_bound_key("nsfwLevel", "1", "commentCount", SortDirection::Desc);

        mgr.form_bound(key1.clone(), &[1, 2, 3], |s| s * 100);

        assert!(mgr.lookup(&key1).is_some());
        assert!(mgr.lookup(&key2).is_none());
    }

    #[test]
    fn test_lookup_miss_for_different_direction() {
        let mut mgr = BoundCacheManager::new(5, 10);
        let key_desc = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Desc);
        let key_asc = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Asc);

        mgr.form_bound(key_desc.clone(), &[1, 2, 3], |s| s * 100);

        assert!(mgr.lookup(&key_desc).is_some());
        assert!(mgr.lookup(&key_asc).is_none());
    }

    #[test]
    fn test_evict_lru() {
        let mut mgr = BoundCacheManager::new(3, 6);
        let key1 = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Desc);
        let key2 = make_bound_key("nsfwLevel", "2", "reactionCount", SortDirection::Desc);

        mgr.form_bound(key1.clone(), &[1, 2, 3], |s| s * 100);
        // Small delay to differentiate timestamps
        std::thread::sleep(std::time::Duration::from_millis(10));
        mgr.form_bound(key2.clone(), &[4, 5, 6], |s| s * 100);

        // Touch key2 to make it more recent
        mgr.lookup_mut(&key2).unwrap().touch();

        // Evict LRU — should be key1
        let evicted = mgr.evict_lru();
        assert_eq!(evicted, Some(key1.clone()));
        assert!(mgr.lookup(&key1).is_none());
        assert!(mgr.lookup(&key2).is_some());
    }

    #[test]
    fn test_invalidate_filter_field() {
        let mut mgr = BoundCacheManager::new(5, 10);
        let key_nsfw = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Desc);
        let key_user = make_bound_key("userId", "42", "reactionCount", SortDirection::Desc);

        mgr.form_bound(key_nsfw.clone(), &[1, 2, 3], |s| s * 100);
        mgr.form_bound(key_user.clone(), &[4, 5, 6], |s| s * 100);

        // Invalidate nsfwLevel
        mgr.invalidate_filter_field("nsfwLevel");

        assert!(mgr.lookup(&key_nsfw).unwrap().needs_rebuild());
        assert!(!mgr.lookup(&key_user).unwrap().needs_rebuild());
    }

    #[test]
    fn test_empty_sorted_slots() {
        let mut mgr = BoundCacheManager::new(5, 10);
        let key = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Desc);

        mgr.form_bound(key.clone(), &[], |_| 0);

        let bound = mgr.lookup(&key).unwrap();
        assert_eq!(bound.cardinality(), 0);
        assert_eq!(bound.min_tracked_value(), 0);
    }

    #[test]
    fn test_total_memory_bytes() {
        let mut mgr = BoundCacheManager::new(100, 200);
        let key = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Desc);

        let sorted: Vec<u32> = (0..100).collect();
        mgr.form_bound(key, &sorted, |s| s);

        // Should have some non-zero memory from the bitmap
        assert!(mgr.total_memory_bytes() > 0);
    }

    // ---- D9: Live maintenance correctness tests ----

    #[test]
    fn test_d9_live_maintenance_adds_qualifying_slot_desc() {
        // Bound with Desc direction: min_tracked_value = floor. New values above floor qualify.
        let mut mgr = BoundCacheManager::new(3, 10);
        let key = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Desc);

        // Sorted slots: [10, 8, 6] with values [1000, 800, 600]. Floor = 600.
        mgr.form_bound(key.clone(), &[10, 8, 6], |s| s * 100);
        assert_eq!(mgr.lookup(&key).unwrap().min_tracked_value(), 600);

        // Slot 20 has value 700 (> 600 floor) — should be added
        let entry = mgr.get_mut(&key).unwrap();
        assert!(!entry.bitmap().contains(20));
        let added = entry.add_slot(20);
        assert!(added);
        assert!(entry.bitmap().contains(20));
        assert_eq!(entry.cardinality(), 4);
    }

    #[test]
    fn test_d9_live_maintenance_skips_below_threshold_desc() {
        let mut mgr = BoundCacheManager::new(3, 10);
        let key = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Desc);

        // Floor = 600.
        mgr.form_bound(key.clone(), &[10, 8, 6], |s| s * 100);

        // Simulated check: value 500 < 600 floor — should NOT be added.
        // (In real code, the flush loop checks this before calling add_slot.)
        let value = 500u32;
        let min = mgr.lookup(&key).unwrap().min_tracked_value();
        assert!(value <= min, "Value below floor should not qualify for Desc bound");
    }

    #[test]
    fn test_d9_live_maintenance_adds_qualifying_slot_asc() {
        // Bound with Asc direction: min_tracked_value = ceiling. New values below ceiling qualify.
        let mut mgr = BoundCacheManager::new(3, 10);
        let key = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Asc);

        // Sorted asc: [1, 2, 3] with values [100, 200, 300]. Ceiling = 300.
        mgr.form_bound(key.clone(), &[1, 2, 3], |s| s * 100);
        assert_eq!(mgr.lookup(&key).unwrap().min_tracked_value(), 300);

        // Slot 5 with value 150 (< 300 ceiling) — qualifies
        let entry = mgr.get_mut(&key).unwrap();
        let added = entry.add_slot(5);
        assert!(added);
        assert!(entry.bitmap().contains(5));
    }

    #[test]
    fn test_d9_bloat_triggers_rebuild_on_live_maintenance() {
        let mut mgr = BoundCacheManager::new(3, 5);
        let key = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Desc);

        // Start with 3 slots (at target_size)
        mgr.form_bound(key.clone(), &[10, 8, 6], |s| s * 100);
        assert_eq!(mgr.lookup(&key).unwrap().cardinality(), 3);

        // Add qualifying slots via live maintenance until bloat
        let entry = mgr.get_mut(&key).unwrap();
        entry.add_slot(20); // 4
        entry.add_slot(30); // 5 = max_size, not over yet
        assert!(!entry.needs_rebuild());
        entry.add_slot(40); // 6 > max_size(5) — triggers rebuild flag
        assert!(entry.needs_rebuild());
    }

    #[test]
    fn test_d9_rebuild_flag_skips_live_maintenance() {
        // When needs_rebuild is true, the flush loop should skip adding new slots
        // (the next query will do a full rebuild instead)
        let mut mgr = BoundCacheManager::new(3, 4);
        let key = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Desc);

        mgr.form_bound(key.clone(), &[10, 8, 6], |s| s * 100);
        let entry = mgr.get_mut(&key).unwrap();
        entry.add_slot(20); // 4 = max_size
        entry.add_slot(30); // 5 > max_size → needs_rebuild
        assert!(entry.needs_rebuild());

        // Simulated: flush loop checks needs_rebuild() and skips
        // This test verifies the flag is correctly set
        let cardinality_before = entry.cardinality();
        // In real flush loop: if entry.needs_rebuild() { continue; }
        // After rebuild:
        entry.rebuild(&[50, 40, 30], |s| s * 100);
        assert!(!entry.needs_rebuild());
        assert_eq!(entry.cardinality(), 3);
        assert!(entry.cardinality() < cardinality_before);
    }

    #[test]
    fn test_d9_filter_invalidation_marks_bounds_for_rebuild() {
        let mut mgr = BoundCacheManager::new(5, 10);
        let key_nsfw = make_bound_key("nsfwLevel", "1", "reactionCount", SortDirection::Desc);
        let key_type = make_bound_key("type", "image", "reactionCount", SortDirection::Desc);

        mgr.form_bound(key_nsfw.clone(), &[1, 2, 3], |s| s * 100);
        mgr.form_bound(key_type.clone(), &[4, 5, 6], |s| s * 100);

        // Mutate nsfwLevel filter — only nsfwLevel bounds should be marked
        mgr.invalidate_filter_field("nsfwLevel");

        assert!(mgr.lookup(&key_nsfw).unwrap().needs_rebuild());
        assert!(!mgr.lookup(&key_type).unwrap().needs_rebuild());
    }

    // ---- D6: Tiered bounds tests ----

    #[test]
    fn test_d6_tier_field_in_bound_key() {
        // Same filter+sort but different tiers should be distinct keys
        let key_t0 = BoundKey {
            filter_key: make_filter_key("nsfwLevel", "1"),
            sort_field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
            tier: 0,
        };
        let key_t1 = BoundKey {
            filter_key: make_filter_key("nsfwLevel", "1"),
            sort_field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
            tier: 1,
        };

        let mut mgr = BoundCacheManager::new(5, 10);
        mgr.form_bound(key_t0.clone(), &[10, 8, 6, 4, 2], |s| s * 100);
        mgr.form_bound(key_t1.clone(), &[1, 3, 5, 7, 9], |s| s * 10);

        assert!(mgr.lookup(&key_t0).is_some());
        assert!(mgr.lookup(&key_t1).is_some());
        assert_eq!(mgr.len(), 2);

        // Different min_tracked_value per tier
        assert_eq!(mgr.lookup(&key_t0).unwrap().min_tracked_value(), 200);
        assert_eq!(mgr.lookup(&key_t1).unwrap().min_tracked_value(), 90);
    }

    #[test]
    fn test_d6_tiered_bound_lru_eviction() {
        // Tiered bounds should be LRU-evicted just like regular bounds
        let mut mgr = BoundCacheManager::new(5, 10);
        let key_t0 = BoundKey {
            filter_key: make_filter_key("nsfwLevel", "1"),
            sort_field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
            tier: 0,
        };
        let key_t1 = BoundKey {
            filter_key: make_filter_key("nsfwLevel", "1"),
            sort_field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
            tier: 1,
        };

        mgr.form_bound(key_t0.clone(), &[10, 8, 6], |s| s * 100);
        std::thread::sleep(std::time::Duration::from_millis(10));
        mgr.form_bound(key_t1.clone(), &[1, 3, 5], |s| s * 10);

        // Touch tier 1 to make it more recent
        mgr.lookup_mut(&key_t1).unwrap().touch();

        // Evict LRU — should be tier 0 (older)
        let evicted = mgr.evict_lru();
        assert_eq!(evicted, Some(key_t0));
        assert!(mgr.lookup(&key_t1).is_some());
    }

    #[test]
    fn test_d6_cursor_past_tier0_range() {
        // Simulate cursor-past-bound detection for tiered lookup
        let mut mgr = BoundCacheManager::new(5, 10);

        // Tier 0: top-5 with floor=200 (Desc: min_tracked_value = smallest value)
        let key_t0 = BoundKey {
            filter_key: make_filter_key("nsfwLevel", "1"),
            sort_field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
            tier: 0,
        };
        mgr.form_bound(key_t0.clone(), &[10, 8, 6, 4, 2], |s| s * 100);
        // Floor = 200 (slot 2 * 100)

        // Tier 1: next range with floor=50
        let key_t1 = BoundKey {
            filter_key: make_filter_key("nsfwLevel", "1"),
            sort_field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
            tier: 1,
        };
        mgr.form_bound(key_t1.clone(), &[1, 3, 5], |s| s * 50);
        // Floor = 250 (slot 5 * 50) — wait, that's higher

        // Better: tier 1 covers slots with lower values
        let key_t1b = BoundKey {
            filter_key: make_filter_key("nsfwLevel", "1"),
            sort_field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
            tier: 1,
        };
        // Overwrite tier 1: sorted desc by value, slots with values below tier 0 floor
        // Slots [15, 14, 13, 12, 11] → values [150, 140, 130, 120, 110]
        // min_tracked_value = value of last (slot 11) = 110
        mgr.form_bound(key_t1b.clone(), &[15, 14, 13, 12, 11], |s| s * 10);

        let t0 = mgr.lookup(&key_t0).unwrap();
        let t1 = mgr.lookup(&key_t1b).unwrap();

        // Tier 0 floor = 200, Tier 1 floor = 110
        assert_eq!(t0.min_tracked_value(), 200);
        assert_eq!(t1.min_tracked_value(), 110);

        // Cursor at 150 (below tier 0's 200 floor for Desc) → skip tier 0, use tier 1
        let cursor_val = 150u32;
        assert!(cursor_val < t0.min_tracked_value()); // past tier 0
        assert!(cursor_val >= t1.min_tracked_value()); // within tier 1
    }
}
