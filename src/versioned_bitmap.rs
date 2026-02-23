use std::sync::Arc;

use roaring::RoaringBitmap;

/// A diff representing pending changes to a bitmap.
///
/// Tracks bits to add (`sets`) and bits to remove (`clears`) separately.
/// These are mutually exclusive per bit: a bit is in sets OR clears, never both.
/// This invariant is maintained by `insert()` and `remove()`.
#[derive(Debug, Clone)]
pub struct BitmapDiff {
    pub sets: RoaringBitmap,
    pub clears: RoaringBitmap,
}

impl BitmapDiff {
    pub fn new() -> Self {
        Self {
            sets: RoaringBitmap::new(),
            clears: RoaringBitmap::new(),
        }
    }

    /// Returns true if neither sets nor clears contain any bits.
    pub fn is_empty(&self) -> bool {
        self.sets.is_empty() && self.clears.is_empty()
    }

    /// Mark a bit for insertion. Removes it from clears if present.
    pub fn insert(&mut self, bit: u32) {
        self.clears.remove(bit);
        self.sets.insert(bit);
    }

    /// Mark a bit for removal. Removes it from sets if present.
    pub fn remove(&mut self, bit: u32) {
        self.sets.remove(bit);
        self.clears.insert(bit);
    }

    /// Reset both sets and clears to empty.
    pub fn clear(&mut self) {
        self.sets = RoaringBitmap::new();
        self.clears = RoaringBitmap::new();
    }

    /// Sum of serialized sizes of sets and clears bitmaps.
    pub fn serialized_size(&self) -> usize {
        self.sets.serialized_size() + self.clears.serialized_size()
    }
}

impl Default for BitmapDiff {
    fn default() -> Self {
        Self::new()
    }
}

/// A bitmap with a separate diff layer for deferred compaction.
///
/// The base bitmap is the last-compacted state. The diff accumulates changes
/// (inserts and removes) that haven't been merged into the base yet.
///
/// Both `base` and `diff` are `Arc`-wrapped for cheap snapshot cloning:
/// publishing a new snapshot just copies the Arc pointers. `Arc::make_mut()`
/// provides clone-on-write when the flush thread mutates while readers
/// still hold references to the previous snapshot.
///
/// Query-time fusion via `apply_diff()` applies the diff to a small candidate
/// set, avoiding a full base clone.
#[derive(Debug, Clone)]
pub struct VersionedBitmap {
    base: Arc<RoaringBitmap>,
    diff: Arc<BitmapDiff>,
    generation: u64,
}

impl VersionedBitmap {
    /// Create a new VersionedBitmap wrapping the given base bitmap.
    pub fn new(base: RoaringBitmap) -> Self {
        Self {
            base: Arc::new(base),
            diff: Arc::new(BitmapDiff::new()),
            generation: 0,
        }
    }

    /// Create a new VersionedBitmap with an empty base.
    pub fn new_empty() -> Self {
        Self::new(RoaringBitmap::new())
    }

    /// Create a new VersionedBitmap from an existing Arc<RoaringBitmap>.
    pub fn from_arc(base: Arc<RoaringBitmap>) -> Self {
        Self {
            base,
            diff: Arc::new(BitmapDiff::new()),
            generation: 0,
        }
    }

    /// Insert a bit. Delegates to the diff layer via Arc::make_mut (CoW).
    pub fn insert(&mut self, bit: u32) {
        Arc::make_mut(&mut self.diff).insert(bit);
    }

    /// Remove a bit. Delegates to the diff layer via Arc::make_mut (CoW).
    pub fn remove(&mut self, bit: u32) {
        Arc::make_mut(&mut self.diff).remove(bit);
    }

    /// Check if a bit is logically present (base + diff).
    ///
    /// A bit is present if:
    /// - It's in the diff's sets, OR
    /// - It's in the base AND NOT in the diff's clears
    pub fn contains(&self, bit: u32) -> bool {
        if self.diff.sets.contains(bit) {
            return true;
        }
        self.base.contains(bit) && !self.diff.clears.contains(bit)
    }

    /// Apply the diff to a candidate set. This is the hot path for queries.
    ///
    /// Fuses the diff into an intersection with candidates without touching the
    /// full base bitmap:
    /// 1. Start with candidates AND base
    /// 2. OR in candidates AND diff.sets (newly added bits that are in candidates)
    /// 3. Subtract diff.clears (removed bits)
    pub fn apply_diff(&self, candidates: &RoaringBitmap) -> RoaringBitmap {
        let mut result = candidates & self.base.as_ref();
        result |= candidates & &self.diff.sets;
        result -= &self.diff.clears;
        result
    }

    /// Return the fully fused bitmap: base | sets - clears.
    /// Used when a standalone bitmap is needed (e.g., single-clause evaluation
    /// where no candidate set is available for apply_diff).
    /// When the diff is empty, returns a clone of the base (cheap Arc refcount bump).
    pub fn fused(&self) -> RoaringBitmap {
        if self.diff.is_empty() {
            return self.base.as_ref().clone();
        }
        let mut result = self.base.as_ref().clone();
        result |= &self.diff.sets;
        result -= &self.diff.clears;
        result
    }

    /// Access the base bitmap directly. Sort layers always use merged bases.
    pub fn base(&self) -> &Arc<RoaringBitmap> {
        &self.base
    }

    /// Cardinality of the base bitmap (does not account for diff).
    pub fn base_len(&self) -> u64 {
        self.base.len()
    }

    /// Access the diff layer.
    pub fn diff(&self) -> &Arc<BitmapDiff> {
        &self.diff
    }

    /// Returns true if the diff contains any pending changes.
    pub fn is_dirty(&self) -> bool {
        !self.diff.is_empty()
    }

    /// Get the generation counter. Bumped on each merge.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Compact the diff into the base bitmap.
    ///
    /// Uses Arc::make_mut on the base for clone-on-write: only clones if
    /// readers still hold references to the current base. After merge,
    /// replaces the diff with a fresh empty Arc<BitmapDiff> and bumps
    /// the generation counter.
    pub fn merge(&mut self) {
        if self.diff.is_empty() {
            return;
        }
        let base = Arc::make_mut(&mut self.base);
        *base |= &self.diff.sets;
        *base -= &self.diff.clears;
        self.diff = Arc::new(BitmapDiff::new());
        self.generation += 1;
    }

    /// Total serialized byte size of base + diff bitmaps.
    pub fn bitmap_bytes(&self) -> usize {
        self.base.serialized_size() + self.diff.serialized_size()
    }

    /// Bulk-insert bits into the diff layer.
    pub fn insert_bulk(&mut self, bits: impl IntoIterator<Item = u32>) {
        let diff = Arc::make_mut(&mut self.diff);
        for bit in bits {
            diff.clears.remove(bit);
            diff.sets.insert(bit);
        }
    }

    /// OR a bitmap directly into the base, bypassing the diff layer.
    ///
    /// Used by put_bulk() during initial data loading where we know:
    /// 1. All inserts are fresh (no existing bits to clear)
    /// 2. The staging copy is private (no readers holding refs)
    /// 3. We want maximum throughput without diff overhead
    ///
    /// This is orders of magnitude faster than insert_bulk() for large bitmaps
    /// because RoaringBitmap's |= operates on compressed containers directly
    /// instead of per-bit Arc::make_mut + clears.remove + sets.insert.
    pub fn or_into_base(&mut self, bitmap: &RoaringBitmap) {
        let base = Arc::make_mut(&mut self.base);
        *base |= bitmap;
    }

    /// Replace the diff with a new Arc. Used by the flush thread publish pattern
    /// to swap in a fresh diff after snapshotting the current one.
    pub fn swap_diff(&mut self, new_diff: Arc<BitmapDiff>) {
        self.diff = new_diff;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_insert_remove() {
        let mut vb = VersionedBitmap::new_empty();

        // Insert bits
        vb.insert(10);
        vb.insert(20);
        vb.insert(30);
        assert!(vb.contains(10));
        assert!(vb.contains(20));
        assert!(vb.contains(30));
        assert!(!vb.contains(40));

        // Remove a bit
        vb.remove(20);
        assert!(vb.contains(10));
        assert!(!vb.contains(20));
        assert!(vb.contains(30));
    }

    #[test]
    fn apply_diff_correctness() {
        // Base has bits 1, 2, 3, 4, 5
        let mut base = RoaringBitmap::new();
        for i in 1..=5 {
            base.insert(i);
        }
        let mut vb = VersionedBitmap::new(base);

        // Diff: add 6, 7; remove 2, 4
        vb.insert(6);
        vb.insert(7);
        vb.remove(2);
        vb.remove(4);

        // Candidates: 1, 2, 3, 5, 6, 8
        let mut candidates = RoaringBitmap::new();
        for &c in &[1, 2, 3, 5, 6, 8] {
            candidates.insert(c);
        }

        let result = vb.apply_diff(&candidates);

        // Expected: 1 (base, in candidates), 3 (base, in candidates),
        //           5 (base, in candidates), 6 (diff.sets, in candidates)
        // Not: 2 (removed), 4 (not in candidates), 7 (not in candidates), 8 (not in base or diff)
        assert!(result.contains(1));
        assert!(!result.contains(2)); // removed by diff
        assert!(result.contains(3));
        assert!(!result.contains(4)); // not in candidates
        assert!(result.contains(5));
        assert!(result.contains(6)); // added by diff, in candidates
        assert!(!result.contains(7)); // added by diff, NOT in candidates
        assert!(!result.contains(8)); // not in base or diff
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn merge_compaction() {
        let mut base = RoaringBitmap::new();
        for i in 1..=5 {
            base.insert(i);
        }
        let mut vb = VersionedBitmap::new(base);
        assert_eq!(vb.generation(), 0);

        // Apply some diff operations
        vb.insert(6);
        vb.insert(7);
        vb.remove(2);
        vb.remove(4);
        assert!(vb.is_dirty());

        // Merge
        vb.merge();

        // Verify base is correct after merge
        assert!(vb.base().contains(1));
        assert!(!vb.base().contains(2)); // was removed
        assert!(vb.base().contains(3));
        assert!(!vb.base().contains(4)); // was removed
        assert!(vb.base().contains(5));
        assert!(vb.base().contains(6)); // was added
        assert!(vb.base().contains(7)); // was added

        // Diff should be empty
        assert!(!vb.is_dirty());
        assert!(vb.diff().is_empty());

        // Generation should be bumped
        assert_eq!(vb.generation(), 1);
    }

    #[test]
    fn merge_strong_count() {
        let base = RoaringBitmap::new();
        let mut vb = VersionedBitmap::new(base);
        vb.insert(1);

        // strong_count == 1 → no clone needed
        let base_ptr_before = Arc::as_ptr(vb.base());
        vb.merge();
        let base_ptr_after = Arc::as_ptr(vb.base());
        // When strong_count is 1, Arc::make_mut doesn't allocate a new Arc
        assert_eq!(base_ptr_before, base_ptr_after);

        // Now clone to bump strong_count > 1
        vb.insert(2);
        let _snapshot = vb.clone();
        assert!(Arc::strong_count(vb.base()) > 1);

        let base_ptr_before = Arc::as_ptr(vb.base());
        vb.merge();
        let base_ptr_after = Arc::as_ptr(vb.base());
        // When strong_count > 1, Arc::make_mut clones → different pointer
        assert_ne!(base_ptr_before, base_ptr_after);
    }

    #[test]
    fn clone_shares_arcs() {
        let mut base = RoaringBitmap::new();
        base.insert(1);
        base.insert(2);
        let mut vb = VersionedBitmap::new(base);
        vb.insert(3);

        let clone = vb.clone();

        // Both base and diff Arc pointers should be the same (cheap clone)
        assert!(Arc::ptr_eq(vb.base(), clone.base()));
        assert!(Arc::ptr_eq(vb.diff(), clone.diff()));
    }

    #[test]
    fn empty_diff_noop() {
        let base = RoaringBitmap::new();
        let mut vb = VersionedBitmap::new(base);
        assert_eq!(vb.generation(), 0);

        // Merge with empty diff should not bump generation
        vb.merge();
        assert_eq!(vb.generation(), 0);
    }

    #[test]
    fn apply_diff_empty() {
        // Base has bits 1, 2, 3
        let mut base = RoaringBitmap::new();
        for i in 1..=3 {
            base.insert(i);
        }
        let vb = VersionedBitmap::new(base);

        // No diff applied — apply_diff should be base & candidates
        let mut candidates = RoaringBitmap::new();
        for &c in &[1, 3, 5] {
            candidates.insert(c);
        }

        let result = vb.apply_diff(&candidates);
        assert_eq!(result.len(), 2);
        assert!(result.contains(1));
        assert!(result.contains(3));
        assert!(!result.contains(5)); // not in base
    }

    #[test]
    fn insert_then_remove_same_bit() {
        let mut vb = VersionedBitmap::new_empty();

        vb.insert(5);
        assert!(vb.diff().sets.contains(5));
        assert!(!vb.diff().clears.contains(5));

        vb.remove(5);
        // After remove: 5 should be in clears, not sets
        assert!(!vb.diff().sets.contains(5));
        assert!(vb.diff().clears.contains(5));
        assert!(!vb.contains(5));
    }

    #[test]
    fn remove_then_insert_same_bit() {
        let mut vb = VersionedBitmap::new_empty();

        vb.remove(5);
        assert!(!vb.diff().sets.contains(5));
        assert!(vb.diff().clears.contains(5));

        vb.insert(5);
        // After insert: 5 should be in sets, not clears
        assert!(vb.diff().sets.contains(5));
        assert!(!vb.diff().clears.contains(5));
        assert!(vb.contains(5));
    }

    #[test]
    fn contains_base_overridden_by_diff() {
        // Base has bit 10
        let mut base = RoaringBitmap::new();
        base.insert(10);
        let mut vb = VersionedBitmap::new(base);

        // Bit 10 is in base
        assert!(vb.contains(10));

        // Remove via diff
        vb.remove(10);
        assert!(!vb.contains(10));

        // Re-insert via diff
        vb.insert(10);
        assert!(vb.contains(10));
    }

    #[test]
    fn swap_diff() {
        let mut vb = VersionedBitmap::new_empty();
        vb.insert(1);
        vb.insert(2);
        assert!(vb.is_dirty());

        // Swap in a fresh diff
        let new_diff = Arc::new(BitmapDiff::new());
        vb.swap_diff(new_diff);
        assert!(!vb.is_dirty());
        assert!(!vb.contains(1)); // diff was swapped out, base is empty
    }

    #[test]
    fn bitmap_bytes_reporting() {
        let mut base = RoaringBitmap::new();
        for i in 0..1000 {
            base.insert(i);
        }
        let mut vb = VersionedBitmap::new(base);
        vb.insert(2000);
        vb.remove(500);

        let bytes = vb.bitmap_bytes();
        assert!(bytes > 0);
    }

    #[test]
    fn from_arc_constructor() {
        let mut bm = RoaringBitmap::new();
        bm.insert(42);
        let arc = Arc::new(bm);
        let arc_clone = Arc::clone(&arc);

        let vb = VersionedBitmap::from_arc(arc);
        assert!(vb.contains(42));
        // The Arc should be shared
        assert!(Arc::ptr_eq(vb.base(), &arc_clone));
    }

    #[test]
    fn base_len() {
        let mut base = RoaringBitmap::new();
        base.insert(1);
        base.insert(2);
        base.insert(3);
        let vb = VersionedBitmap::new(base);
        assert_eq!(vb.base_len(), 3);
    }
}
