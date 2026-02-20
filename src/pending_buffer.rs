use std::collections::HashMap;
use std::sync::Arc;

use roaring::RoaringBitmap;

/// Key for a pending mutation: (field_name, bitmap_value).
pub type PendingKey = (Arc<str>, u64);

/// Accumulated mutations for a single bitmap that hasn't been loaded into memory.
#[derive(Debug, Default)]
pub struct PendingMutations {
    /// Slots to set (insert).
    pub sets: RoaringBitmap,
    /// Slots to clear (remove).
    pub clears: RoaringBitmap,
}

impl PendingMutations {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a set (insert) mutation.
    pub fn add_set(&mut self, slot: u32) {
        self.clears.remove(slot);
        self.sets.insert(slot);
    }

    /// Add a clear (remove) mutation.
    pub fn add_clear(&mut self, slot: u32) {
        self.sets.remove(slot);
        self.clears.insert(slot);
    }

    /// Apply these pending mutations to a bitmap in place.
    pub fn apply_to(&self, bitmap: &mut RoaringBitmap) {
        *bitmap |= &self.sets;
        *bitmap -= &self.clears;
    }

    /// Returns true if there are no pending mutations.
    pub fn is_empty(&self) -> bool {
        self.sets.is_empty() && self.clears.is_empty()
    }

    /// Total number of pending operations (sets + clears).
    pub fn op_count(&self) -> u64 {
        self.sets.len() + self.clears.len()
    }
}

/// Buffer for mutations to Tier 2 bitmaps that aren't loaded in memory.
///
/// The flush thread adds mutations here when a Tier 2 bitmap isn't in the moka cache.
/// Mutations are consumed either:
/// 1. On read: when a query needs a Tier 2 bitmap, load from redb + apply pending
/// 2. On merge: periodically drain the oldest/heaviest entries and write to redb
pub struct PendingBuffer {
    entries: HashMap<PendingKey, PendingMutations>,
}

impl PendingBuffer {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Add a set mutation for a specific field/value pair.
    pub fn add_set(&mut self, field: &Arc<str>, value: u64, slot: u32) {
        self.entries
            .entry((Arc::clone(field), value))
            .or_default()
            .add_set(slot);
    }

    /// Add a clear mutation for a specific field/value pair.
    pub fn add_clear(&mut self, field: &Arc<str>, value: u64, slot: u32) {
        self.entries
            .entry((Arc::clone(field), value))
            .or_default()
            .add_clear(slot);
    }

    /// Remove and return pending mutations for a key.
    /// Used on cache miss to apply before serving a bitmap to a reader.
    pub fn take(&mut self, field: &Arc<str>, value: u64) -> Option<PendingMutations> {
        let key = (Arc::clone(field), value);
        let mutations = self.entries.remove(&key)?;
        if mutations.is_empty() {
            None
        } else {
            Some(mutations)
        }
    }

    /// Check if there are pending mutations for a key.
    pub fn has_pending(&self, field: &Arc<str>, value: u64) -> bool {
        self.entries
            .get(&(Arc::clone(field), value))
            .map_or(false, |m| !m.is_empty())
    }

    /// Drain up to `cap` entries, prioritized by op_count (heaviest first).
    /// Used by merge thread to batch-flush the most impactful pending mutations.
    pub fn drain_heaviest(&mut self, cap: usize) -> Vec<(PendingKey, PendingMutations)> {
        if cap == 0 || self.entries.is_empty() {
            return Vec::new();
        }

        // Collect (key, op_count) pairs and sort by op_count descending
        let mut ranked: Vec<(PendingKey, u64)> = self
            .entries
            .iter()
            .map(|(k, v)| (k.clone(), v.op_count()))
            .collect();
        ranked.sort_unstable_by(|a, b| b.1.cmp(&a.1));
        ranked.truncate(cap);

        let mut result = Vec::with_capacity(ranked.len());
        for (key, _) in ranked {
            if let Some(mutations) = self.entries.remove(&key) {
                result.push((key, mutations));
            }
        }
        result
    }

    /// Number of keys with pending mutations.
    pub fn depth(&self) -> usize {
        self.entries.len()
    }

    /// Total pending operations across all keys.
    pub fn total_ops(&self) -> u64 {
        self.entries.values().map(|m| m.op_count()).sum()
    }
}

impl Default for PendingBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_take() {
        let mut buf = PendingBuffer::new();
        let field: Arc<str> = Arc::from("tagIds");

        buf.add_set(&field, 42, 100);
        buf.add_set(&field, 42, 200);
        buf.add_clear(&field, 42, 50);

        assert!(buf.has_pending(&field, 42));
        assert_eq!(buf.depth(), 1);

        let pending = buf.take(&field, 42).unwrap();
        assert!(pending.sets.contains(100));
        assert!(pending.sets.contains(200));
        assert!(pending.clears.contains(50));
        assert!(!buf.has_pending(&field, 42));
    }

    #[test]
    fn test_apply_to_bitmap() {
        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        bm.insert(2);
        bm.insert(3);

        let mut pending = PendingMutations::new();
        pending.add_set(4);
        pending.add_set(5);
        pending.add_clear(2);

        pending.apply_to(&mut bm);
        assert!(bm.contains(1));
        assert!(!bm.contains(2));
        assert!(bm.contains(3));
        assert!(bm.contains(4));
        assert!(bm.contains(5));
    }

    #[test]
    fn test_drain_heaviest() {
        let mut buf = PendingBuffer::new();
        let field: Arc<str> = Arc::from("tagIds");

        // Add 3 entries with different op counts
        buf.add_set(&field, 1, 100); // 1 op

        for i in 0..5 {
            buf.add_set(&field, 2, i); // 5 ops
        }

        for i in 0..3 {
            buf.add_set(&field, 3, i); // 3 ops
        }

        // Drain 2 heaviest
        let drained = buf.drain_heaviest(2);
        assert_eq!(drained.len(), 2);
        // Heaviest first: value 2 (5 ops), then value 3 (3 ops)
        assert_eq!(drained[0].0 .1, 2);
        assert_eq!(drained[1].0 .1, 3);
        // Value 1 should remain
        assert_eq!(buf.depth(), 1);
    }

    #[test]
    fn test_set_then_clear_same_slot() {
        let mut pending = PendingMutations::new();
        pending.add_set(10);
        pending.add_clear(10);
        // Clear should win (later operation)
        assert!(!pending.sets.contains(10));
        assert!(pending.clears.contains(10));
    }
}
