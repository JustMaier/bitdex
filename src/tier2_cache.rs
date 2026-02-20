use std::sync::Arc;

use moka::sync::Cache;
use roaring::RoaringBitmap;

use crate::error::Result;

/// Cache key: (field_name, bitmap_value)
type CacheKey = (Arc<str>, u64);

/// Tier 2 concurrent cache for on-demand filter bitmaps.
///
/// Wraps moka::sync::Cache with thundering herd protection via get_with().
/// Bitmaps are loaded from redb on cache miss and cached in memory up to
/// the configured size budget.
pub struct Tier2Cache {
    cache: Cache<CacheKey, Arc<RoaringBitmap>>,
}

impl Tier2Cache {
    /// Create a new Tier 2 cache with a memory budget in megabytes.
    ///
    /// Uses moka's weigher to approximate memory cost per entry based on
    /// `RoaringBitmap::serialized_size()`. Eviction is driven by total
    /// weight rather than entry count.
    pub fn new(max_size_mb: u64) -> Self {
        let cache = Cache::builder()
            .weigher(|_key: &CacheKey, value: &Arc<RoaringBitmap>| -> u32 {
                let size = value.serialized_size();
                u32::try_from(size).unwrap_or(u32::MAX)
            })
            // When a weigher is set, max_capacity acts as max weight (in weigher units).
            .max_capacity(max_size_mb * 1024 * 1024)
            .support_invalidation_closures()
            .build();

        Self { cache }
    }

    /// Get a bitmap from the cache, loading it on miss via the provided closure.
    ///
    /// Uses moka's `get_with()` for thundering herd protection — concurrent
    /// requests for the same key will coalesce into a single loader call.
    /// If the loader returns an error, an empty bitmap is cached (the value
    /// may not exist in redb yet for cold mutations).
    pub fn get_or_load<F>(&self, field: &Arc<str>, value: u64, loader: F) -> Result<Arc<RoaringBitmap>>
    where
        F: FnOnce() -> Result<RoaringBitmap>,
    {
        let key = (Arc::clone(field), value);
        let result = self.cache.get_with(key, || {
            match loader() {
                Ok(bm) => Arc::new(bm),
                Err(_) => Arc::new(RoaringBitmap::new()),
            }
        });
        Ok(result)
    }

    /// Simple cache lookup without loading on miss.
    pub fn get(&self, field: &Arc<str>, value: u64) -> Option<Arc<RoaringBitmap>> {
        let key = (Arc::clone(field), value);
        self.cache.get(&key)
    }

    /// Insert a bitmap directly (used after merge compaction).
    pub fn insert(&self, field: &Arc<str>, value: u64, bitmap: Arc<RoaringBitmap>) {
        let key = (Arc::clone(field), value);
        self.cache.insert(key, bitmap);
    }

    /// Remove a single cached entry.
    pub fn invalidate(&self, field: &Arc<str>, value: u64) {
        let key = (Arc::clone(field), value);
        self.cache.invalidate(&key);
    }

    /// Remove all cached entries for a given field.
    pub fn invalidate_field(&self, field: &Arc<str>) {
        let field = Arc::clone(field);
        self.cache
            .invalidate_entries_if(move |key: &CacheKey, _value: &Arc<RoaringBitmap>| key.0 == field)
            .expect("support_invalidation_closures is enabled");
    }

    /// Check if an entry exists in the cache without loading.
    pub fn contains(&self, field: &Arc<str>, value: u64) -> bool {
        let key = (Arc::clone(field), value);
        self.cache.contains_key(&key)
    }

    /// Number of cached entries.
    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Total weighted size of cache (approximate memory usage in bytes).
    pub fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_or_load_caches() {
        let cache = Tier2Cache::new(100); // 100 MB
        let field: Arc<str> = Arc::from("tagIds");

        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        bm.insert(2);

        let result = cache.get_or_load(&field, 42, || Ok(bm.clone())).unwrap();
        assert_eq!(result.len(), 2);

        // Second call should hit cache (loader not called)
        let result2 = cache
            .get_or_load(&field, 42, || panic!("should not be called"))
            .unwrap();
        assert_eq!(result2.len(), 2);
    }

    #[test]
    fn test_invalidate() {
        let cache = Tier2Cache::new(100);
        let field: Arc<str> = Arc::from("tagIds");

        let bm = Arc::new(RoaringBitmap::new());
        cache.insert(&field, 42, bm);
        assert!(cache.get(&field, 42).is_some());

        cache.invalidate(&field, 42);
        assert!(cache.get(&field, 42).is_none());
    }

    #[test]
    fn test_entry_count() {
        let cache = Tier2Cache::new(100);
        let field: Arc<str> = Arc::from("tagIds");

        cache.insert(&field, 1, Arc::new(RoaringBitmap::new()));
        cache.insert(&field, 2, Arc::new(RoaringBitmap::new()));
        // moka processes inserts lazily; flush pending tasks for accurate count
        cache.cache.run_pending_tasks();
        assert_eq!(cache.entry_count(), 2);
    }

    #[test]
    fn test_invalidate_field() {
        let cache = Tier2Cache::new(100);
        let tag_field: Arc<str> = Arc::from("tagIds");
        let user_field: Arc<str> = Arc::from("userId");

        cache.insert(&tag_field, 1, Arc::new(RoaringBitmap::new()));
        cache.insert(&tag_field, 2, Arc::new(RoaringBitmap::new()));
        cache.insert(&user_field, 100, Arc::new(RoaringBitmap::new()));

        cache.invalidate_field(&tag_field);

        // tagIds entries should be gone
        assert!(cache.get(&tag_field, 1).is_none());
        assert!(cache.get(&tag_field, 2).is_none());
        // userId entry should remain
        assert!(cache.get(&user_field, 100).is_some());
    }

    #[test]
    fn test_loader_error_returns_empty_bitmap() {
        let cache = Tier2Cache::new(100);
        let field: Arc<str> = Arc::from("tagIds");

        let result = cache
            .get_or_load(&field, 999, || {
                Err(crate::error::BitdexError::DocStore(
                    "not found".to_string(),
                ))
            })
            .unwrap();
        assert_eq!(result.len(), 0);

        // Subsequent call should get the cached empty bitmap
        let result2 = cache
            .get_or_load(&field, 999, || panic!("should not be called"))
            .unwrap();
        assert_eq!(result2.len(), 0);
    }

    #[test]
    fn test_weighted_size() {
        let cache = Tier2Cache::new(100);
        let field: Arc<str> = Arc::from("tagIds");

        let mut bm = RoaringBitmap::new();
        for i in 0..1000 {
            bm.insert(i);
        }
        let size_before = cache.weighted_size();
        cache.insert(&field, 1, Arc::new(bm));
        // run_pending to make sure the weigher has been applied
        cache.cache.run_pending_tasks();
        let size_after = cache.weighted_size();
        assert!(size_after > size_before);
    }

    // A10: Concurrent reads / thundering herd
    // Spawn multiple threads that all call get_or_load() for the same cold key.
    // The loader must only be called ONCE; all threads should get the same bitmap.
    #[test]
    fn test_thundering_herd_loader_called_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;

        let cache = Arc::new(Tier2Cache::new(100));
        let field: Arc<str> = Arc::from("tagIds");
        let call_count = Arc::new(AtomicUsize::new(0));

        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        bm.insert(2);
        bm.insert(3);
        let expected_bm = Arc::new(bm);

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let field = Arc::clone(&field);
                let call_count = Arc::clone(&call_count);
                let expected_bm = Arc::clone(&expected_bm);
                thread::spawn(move || {
                    let result = cache
                        .get_or_load(&field, 42, || {
                            call_count.fetch_add(1, Ordering::SeqCst);
                            Ok((*expected_bm).clone())
                        })
                        .unwrap();
                    assert_eq!(result.len(), 3);
                    assert!(result.contains(1));
                    assert!(result.contains(2));
                    assert!(result.contains(3));
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // moka's get_with() coalesces concurrent loaders — loader called exactly once
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    // A10: Pending mutation correctness
    // Load a bitmap from BitmapStore (open_temp), apply PendingMutations via apply_to(),
    // verify the result is correct.
    #[test]
    fn test_pending_mutation_applied_on_load() {
        use crate::bitmap_store::BitmapStore;
        use crate::pending_buffer::PendingBuffer;

        // Seed the store with a base bitmap
        let store = BitmapStore::open_temp().unwrap();
        let mut base_bm = RoaringBitmap::new();
        base_bm.insert(10);
        base_bm.insert(20);
        base_bm.insert(30);
        store
            .write_batch(&[("tagIds", 42, &base_bm)])
            .unwrap();

        // Build pending mutations: set 40, clear 10
        let field: Arc<str> = Arc::from("tagIds");
        let mut buf = PendingBuffer::new();
        buf.add_set(&field, 42, 40);
        buf.add_clear(&field, 42, 10);

        // Simulate cache miss: load from store then apply pending
        let mut loaded = store.load_single("tagIds", 42).unwrap();
        if let Some(pending) = buf.take(&field, 42) {
            pending.apply_to(&mut loaded);
        }

        // 10 was cleared, 20 and 30 remain, 40 was added
        assert!(!loaded.contains(10));
        assert!(loaded.contains(20));
        assert!(loaded.contains(30));
        assert!(loaded.contains(40));
        assert_eq!(loaded.len(), 3);
    }

    // A10: Cache eviction behavior
    // Verify that moka enforces its memory budget by checking weighted_size after
    // inserting entries that collectively exceed the budget.
    #[test]
    fn test_cache_eviction_under_small_budget() {
        // 1 MB budget
        let cache = Tier2Cache::new(1);
        let field: Arc<str> = Arc::from("tagIds");

        let mut total_inserted_weight: u64 = 0;
        for i in 0u64..20 {
            let mut bm = RoaringBitmap::new();
            // Use a simple hash-like scatter: bit = i*prime mod large_range
            // This defeats all roaring compression (array containers, no runs).
            let base = (i as u32) * 7_919; // prime offset per bitmap
            for j in 0u32..200_000 {
                // Scatter across a large range to avoid bitset containers
                let bit = base.wrapping_add(j.wrapping_mul(104_729)); // prime stride
                bm.insert(bit);
            }
            total_inserted_weight += bm.serialized_size() as u64;
            cache.insert(&field, i, Arc::new(bm));
            cache.cache.run_pending_tasks();
        }

        // Sanity check: we did insert more than the budget
        assert!(
            total_inserted_weight > 1024 * 1024,
            "total inserted weight {} should exceed 1 MB budget",
            total_inserted_weight
        );

        // moka should have evicted enough to stay near budget.
        // Use weighted_size which is authoritative — entry_count may lag.
        let weighted = cache.weighted_size();
        assert!(
            weighted < total_inserted_weight,
            "weighted size {} should be less than total inserted {}",
            weighted,
            total_inserted_weight
        );
    }
}
