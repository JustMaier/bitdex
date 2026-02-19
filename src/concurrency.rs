use dashmap::DashSet;
use roaring::RoaringBitmap;

/// Tracks in-flight write operations for optimistic concurrency.
///
/// Writers atomically mark their target slot ID in the in-flight set BEFORE
/// mutating bitmaps, and clear the mark AFTER mutation is complete.
///
/// Readers execute queries without coordination, then post-validate their
/// results against the in-flight set. If any result IDs overlap with
/// in-flight writes, only those IDs need revalidation.
pub struct InFlightTracker {
    /// Set of slot IDs currently being written to.
    /// Uses DashSet for lock-free concurrent access.
    in_flight: DashSet<u32>,
}

impl InFlightTracker {
    pub fn new() -> Self {
        Self {
            in_flight: DashSet::new(),
        }
    }

    /// Mark a slot as in-flight (being written to).
    /// Must be called BEFORE starting the mutation.
    pub fn mark_in_flight(&self, slot_id: u32) {
        self.in_flight.insert(slot_id);
    }

    /// Clear a slot from the in-flight set.
    /// Must be called AFTER the mutation is complete.
    pub fn clear_in_flight(&self, slot_id: u32) {
        self.in_flight.remove(&slot_id);
    }

    /// Check if a slot is currently in-flight.
    pub fn is_in_flight(&self, slot_id: u32) -> bool {
        self.in_flight.contains(&slot_id)
    }

    /// Find which IDs from a result set overlap with in-flight writes.
    /// Returns the overlapping slot IDs that need revalidation.
    pub fn find_overlapping(&self, result_ids: &[i64]) -> Vec<u32> {
        result_ids
            .iter()
            .filter_map(|&id| {
                let slot = id as u32;
                if self.in_flight.contains(&slot) {
                    Some(slot)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Find which IDs from a result bitmap overlap with in-flight writes.
    pub fn find_overlapping_bitmap(&self, candidates: &RoaringBitmap) -> RoaringBitmap {
        let mut overlapping = RoaringBitmap::new();
        for slot in self.in_flight.iter() {
            if candidates.contains(*slot) {
                overlapping.insert(*slot);
            }
        }
        overlapping
    }

    /// Get the number of in-flight writes.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    /// Check if there are any in-flight writes.
    pub fn has_in_flight(&self) -> bool {
        !self.in_flight.is_empty()
    }
}

impl Default for InFlightTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Guard that automatically clears the in-flight mark when dropped.
/// Ensures in-flight marks are always cleaned up, even on panic.
pub struct InFlightGuard<'a> {
    tracker: &'a InFlightTracker,
    slot_id: u32,
}

impl<'a> InFlightGuard<'a> {
    /// Create a new guard that marks the slot as in-flight.
    pub fn new(tracker: &'a InFlightTracker, slot_id: u32) -> Self {
        tracker.mark_in_flight(slot_id);
        Self { tracker, slot_id }
    }
}

impl<'a> Drop for InFlightGuard<'a> {
    fn drop(&mut self) {
        self.tracker.clear_in_flight(self.slot_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_basic_mark_and_clear() {
        let tracker = InFlightTracker::new();

        assert!(!tracker.is_in_flight(42));
        assert_eq!(tracker.in_flight_count(), 0);

        tracker.mark_in_flight(42);
        assert!(tracker.is_in_flight(42));
        assert_eq!(tracker.in_flight_count(), 1);

        tracker.clear_in_flight(42);
        assert!(!tracker.is_in_flight(42));
        assert_eq!(tracker.in_flight_count(), 0);
    }

    #[test]
    fn test_multiple_in_flight() {
        let tracker = InFlightTracker::new();

        tracker.mark_in_flight(1);
        tracker.mark_in_flight(2);
        tracker.mark_in_flight(3);

        assert_eq!(tracker.in_flight_count(), 3);
        assert!(tracker.is_in_flight(1));
        assert!(tracker.is_in_flight(2));
        assert!(tracker.is_in_flight(3));
        assert!(!tracker.is_in_flight(4));

        tracker.clear_in_flight(2);
        assert_eq!(tracker.in_flight_count(), 2);
        assert!(!tracker.is_in_flight(2));
    }

    #[test]
    fn test_find_overlapping() {
        let tracker = InFlightTracker::new();

        tracker.mark_in_flight(5);
        tracker.mark_in_flight(10);

        let results = vec![1i64, 5, 7, 10, 15];
        let overlapping = tracker.find_overlapping(&results);

        assert_eq!(overlapping.len(), 2);
        assert!(overlapping.contains(&5));
        assert!(overlapping.contains(&10));
    }

    #[test]
    fn test_find_overlapping_none() {
        let tracker = InFlightTracker::new();

        tracker.mark_in_flight(100);

        let results = vec![1i64, 2, 3, 4, 5];
        let overlapping = tracker.find_overlapping(&results);
        assert!(overlapping.is_empty());
    }

    #[test]
    fn test_find_overlapping_bitmap() {
        let tracker = InFlightTracker::new();

        tracker.mark_in_flight(5);
        tracker.mark_in_flight(10);

        let mut candidates = RoaringBitmap::new();
        for i in 1..=20 {
            candidates.insert(i);
        }

        let overlapping = tracker.find_overlapping_bitmap(&candidates);
        assert_eq!(overlapping.len(), 2);
        assert!(overlapping.contains(5));
        assert!(overlapping.contains(10));
    }

    #[test]
    fn test_guard_auto_clear() {
        let tracker = InFlightTracker::new();

        {
            let _guard = InFlightGuard::new(&tracker, 42);
            assert!(tracker.is_in_flight(42));
        }
        // Guard dropped, should be cleared
        assert!(!tracker.is_in_flight(42));
    }

    #[test]
    fn test_guard_clears_on_panic_recovery() {
        let tracker = InFlightTracker::new();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = InFlightGuard::new(&tracker, 99);
            assert!(tracker.is_in_flight(99));
            panic!("simulated panic");
        }));

        assert!(result.is_err());
        // Guard should have been dropped during unwind
        assert!(!tracker.is_in_flight(99));
    }

    #[test]
    fn test_concurrent_writers() {
        let tracker = Arc::new(InFlightTracker::new());
        let mut handles = Vec::new();

        // Spawn 10 writer threads, each marking/clearing its own slot
        for i in 0..10u32 {
            let tracker = Arc::clone(&tracker);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    tracker.mark_in_flight(i);
                    assert!(tracker.is_in_flight(i));
                    tracker.clear_in_flight(i);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(tracker.in_flight_count(), 0);
    }

    #[test]
    fn test_concurrent_readers_and_writers() {
        let tracker = Arc::new(InFlightTracker::new());
        let mut handles = Vec::new();

        // Writer threads
        for i in 0..5u32 {
            let tracker = Arc::clone(&tracker);
            handles.push(thread::spawn(move || {
                for _ in 0..500 {
                    let _guard = InFlightGuard::new(&tracker, i);
                    // Simulate a short write operation
                    std::thread::yield_now();
                }
            }));
        }

        // Reader threads that check for overlaps
        for _ in 0..5 {
            let tracker = Arc::clone(&tracker);
            handles.push(thread::spawn(move || {
                for _ in 0..500 {
                    let result_ids: Vec<i64> = (0..50).collect();
                    let _overlapping = tracker.find_overlapping(&result_ids);
                    // Just verifying no panics/data races
                    std::thread::yield_now();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // All writes should be complete
        assert_eq!(tracker.in_flight_count(), 0);
    }

    #[test]
    fn test_has_in_flight() {
        let tracker = InFlightTracker::new();

        assert!(!tracker.has_in_flight());

        tracker.mark_in_flight(1);
        assert!(tracker.has_in_flight());

        tracker.clear_in_flight(1);
        assert!(!tracker.has_in_flight());
    }

    #[test]
    fn test_idempotent_mark() {
        let tracker = InFlightTracker::new();

        tracker.mark_in_flight(42);
        tracker.mark_in_flight(42);
        assert_eq!(tracker.in_flight_count(), 1); // DashSet deduplicates

        tracker.clear_in_flight(42);
        assert_eq!(tracker.in_flight_count(), 0);
    }

    #[test]
    fn test_clear_nonexistent() {
        let tracker = InFlightTracker::new();
        // Should not panic
        tracker.clear_in_flight(999);
        assert_eq!(tracker.in_flight_count(), 0);
    }
}
