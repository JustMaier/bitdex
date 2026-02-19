use std::sync::atomic::{AtomicU32, Ordering};

use roaring::RoaringBitmap;

use crate::error::{BitdexError, Result};

/// Manages slot allocation, the alive bitmap, and the clean bitmap for slot recycling.
///
/// Design:
/// - Each document's Postgres ID IS the slot (its position in every bitmap).
/// - Slots are monotonically assigned via atomic counter on insert.
/// - Deleted slots are NOT immediately recycled -- the alive bitmap hides them.
/// - An autovac process periodically produces a clean bitmap of recycled slots.
/// - New inserts check the clean bitmap first (grab first set bit), append only if none available.
pub struct SlotAllocator {
    /// Next slot to assign if no recycled slots are available.
    next_slot: AtomicU32,

    /// Bitmap of all alive (active) documents. ANDed into every query.
    alive: RoaringBitmap,

    /// Bitmap of slots that have been cleaned by autovac and are available for reuse.
    clean: RoaringBitmap,
}

impl SlotAllocator {
    pub fn new() -> Self {
        Self {
            next_slot: AtomicU32::new(0),
            alive: RoaringBitmap::new(),
            clean: RoaringBitmap::new(),
        }
    }

    /// Restore from a known state (e.g., after snapshot load).
    pub fn from_state(next_slot: u32, alive: RoaringBitmap, clean: RoaringBitmap) -> Self {
        Self {
            next_slot: AtomicU32::new(next_slot),
            alive,
            clean,
        }
    }

    /// Allocate a slot for the given Postgres ID.
    ///
    /// Since slot = Postgres ID, we just set the alive bit at that position.
    /// The atomic counter tracks the high-water mark for sequential inserts.
    pub fn allocate(&mut self, postgres_id: u32) -> Result<u32> {
        let slot = postgres_id;

        // Update high-water mark if this ID is beyond current counter
        let current = self.next_slot.load(Ordering::Relaxed);
        if slot >= current {
            self.next_slot.store(slot + 1, Ordering::Relaxed);
        }

        // Remove from clean bitmap if it was a recycled slot
        self.clean.remove(slot);

        // Mark as alive
        self.alive.insert(slot);

        Ok(slot)
    }

    /// Allocate the next sequential slot (when no specific ID is needed).
    /// Checks the clean bitmap first for recycled slots.
    pub fn allocate_next(&mut self) -> u32 {
        // Try to reuse a cleaned slot first
        if let Some(recycled) = self.clean.min() {
            self.clean.remove(recycled);
            self.alive.insert(recycled);
            return recycled;
        }

        // No recycled slots, use next sequential
        let slot = self.next_slot.fetch_add(1, Ordering::Relaxed);
        self.alive.insert(slot);
        slot
    }

    /// Delete a document by clearing its alive bit. This is the ENTIRE delete operation.
    /// Stale bits in filter/sort bitmaps are invisible behind the alive gate.
    pub fn delete(&mut self, slot: u32) -> Result<()> {
        if !self.alive.contains(slot) {
            return Err(BitdexError::SlotNotFound(slot));
        }
        self.alive.remove(slot);
        Ok(())
    }

    /// Check if a slot is alive.
    pub fn is_alive(&self, slot: u32) -> bool {
        self.alive.contains(slot)
    }

    /// Check if a slot was ever allocated (alive or dead with stale bits).
    /// Returns true if the slot ID is below the high-water mark, meaning it
    /// was previously used and may have stale filter/sort bits that need clearing.
    pub fn was_ever_allocated(&self, slot: u32) -> bool {
        slot < self.next_slot.load(Ordering::Relaxed)
    }

    /// Get a reference to the alive bitmap. This is ANDed into every query.
    pub fn alive_bitmap(&self) -> &RoaringBitmap {
        &self.alive
    }

    /// Get a mutable reference to the alive bitmap.
    pub fn alive_bitmap_mut(&mut self) -> &mut RoaringBitmap {
        &mut self.alive
    }

    /// Get the clean bitmap (recycled slots available for reuse).
    pub fn clean_bitmap(&self) -> &RoaringBitmap {
        &self.clean
    }

    /// Get a mutable reference to the clean bitmap.
    pub fn clean_bitmap_mut(&mut self) -> &mut RoaringBitmap {
        &mut self.clean
    }

    /// Mark a slot as clean (recycled by autovac, available for reuse).
    pub fn mark_clean(&mut self, slot: u32) {
        debug_assert!(
            !self.alive.contains(slot),
            "cannot mark alive slot as clean"
        );
        self.clean.insert(slot);
    }

    /// Get the number of alive documents.
    pub fn alive_count(&self) -> u64 {
        self.alive.len()
    }

    /// Get the number of dead (deleted but not yet cleaned) slots.
    ///
    /// Note: In the slot=ID model, this includes "gap" slots that were never
    /// allocated but fall below the high-water mark. For exact dead count,
    /// autovac should scan `NOT alive AND NOT clean AND slot < counter`.
    pub fn dead_count(&self) -> u64 {
        let counter = self.next_slot.load(Ordering::Relaxed) as u64;
        counter.saturating_sub(self.alive.len()).saturating_sub(self.clean.len())
    }

    /// Get the number of clean (recycled) slots available for reuse.
    pub fn clean_count(&self) -> u64 {
        self.clean.len()
    }

    /// Get the high-water mark (total slots ever assigned).
    pub fn slot_counter(&self) -> u32 {
        self.next_slot.load(Ordering::Relaxed)
    }
}

impl Default for SlotAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate_specific_id() {
        let mut alloc = SlotAllocator::new();
        let slot = alloc.allocate(42).unwrap();
        assert_eq!(slot, 42);
        assert!(alloc.is_alive(42));
        assert_eq!(alloc.alive_count(), 1);
        assert_eq!(alloc.slot_counter(), 43);
    }

    #[test]
    fn test_allocate_next_sequential() {
        let mut alloc = SlotAllocator::new();
        let s0 = alloc.allocate_next();
        let s1 = alloc.allocate_next();
        let s2 = alloc.allocate_next();
        assert_eq!(s0, 0);
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);
        assert_eq!(alloc.alive_count(), 3);
        assert_eq!(alloc.slot_counter(), 3);
    }

    #[test]
    fn test_delete_clears_alive() {
        let mut alloc = SlotAllocator::new();
        alloc.allocate(10).unwrap();
        assert!(alloc.is_alive(10));

        alloc.delete(10).unwrap();
        assert!(!alloc.is_alive(10));
        assert_eq!(alloc.alive_count(), 0);
    }

    #[test]
    fn test_delete_nonexistent_fails() {
        let mut alloc = SlotAllocator::new();
        assert!(alloc.delete(999).is_err());
    }

    #[test]
    fn test_delete_only_clears_alive_bit() {
        // Verify that delete does NOT touch clean bitmap
        let mut alloc = SlotAllocator::new();
        // Allocate slots 0..5, then delete slot 5 to get an exact dead count
        for i in 0..=5 {
            alloc.allocate(i).unwrap();
        }
        alloc.delete(5).unwrap();

        // Slot is dead but NOT clean (autovac hasn't run)
        assert!(!alloc.is_alive(5));
        assert!(!alloc.clean_bitmap().contains(5));
        assert_eq!(alloc.dead_count(), 1);
    }

    #[test]
    fn test_clean_bitmap_reuse() {
        let mut alloc = SlotAllocator::new();

        // Allocate and delete slot 0
        alloc.allocate(0).unwrap();
        alloc.delete(0).unwrap();

        // Simulate autovac marking slot 0 as clean
        alloc.mark_clean(0);
        assert_eq!(alloc.clean_count(), 1);

        // Next allocation should reuse the cleaned slot
        let reused = alloc.allocate_next();
        assert_eq!(reused, 0);
        assert!(alloc.is_alive(0));
        assert_eq!(alloc.clean_count(), 0);
    }

    #[test]
    fn test_high_water_mark_updates() {
        let mut alloc = SlotAllocator::new();
        alloc.allocate(100).unwrap();
        assert_eq!(alloc.slot_counter(), 101);

        alloc.allocate(50).unwrap();
        assert_eq!(alloc.slot_counter(), 101); // Doesn't decrease

        alloc.allocate(200).unwrap();
        assert_eq!(alloc.slot_counter(), 201);
    }

    #[test]
    fn test_alive_bitmap_query_gate() {
        let mut alloc = SlotAllocator::new();
        for i in 0..10 {
            alloc.allocate(i).unwrap();
        }
        // Delete evens
        for i in (0..10).step_by(2) {
            alloc.delete(i).unwrap();
        }

        let alive = alloc.alive_bitmap();
        assert_eq!(alive.len(), 5);
        for i in 0..10 {
            if i % 2 == 0 {
                assert!(!alive.contains(i));
            } else {
                assert!(alive.contains(i));
            }
        }
    }

    #[test]
    fn test_dead_count() {
        let mut alloc = SlotAllocator::new();
        for i in 0..10 {
            alloc.allocate(i).unwrap();
        }
        alloc.delete(3).unwrap();
        alloc.delete(7).unwrap();
        assert_eq!(alloc.dead_count(), 2);

        // Clean one of them
        alloc.mark_clean(3);
        assert_eq!(alloc.dead_count(), 1);
        assert_eq!(alloc.clean_count(), 1);
    }

    #[test]
    fn test_from_state_restore() {
        let mut alive = RoaringBitmap::new();
        alive.insert(0);
        alive.insert(5);
        alive.insert(10);

        let mut clean = RoaringBitmap::new();
        clean.insert(3);

        let alloc = SlotAllocator::from_state(11, alive, clean);
        assert_eq!(alloc.alive_count(), 3);
        assert_eq!(alloc.clean_count(), 1);
        assert_eq!(alloc.slot_counter(), 11);
        assert!(alloc.is_alive(5));
        assert!(!alloc.is_alive(3));
    }
}
