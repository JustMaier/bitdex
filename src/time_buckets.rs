use std::collections::HashMap;

use roaring::RoaringBitmap;

use crate::config::BucketConfig;

/// A single time range bucket with its pre-computed bitmap.
pub struct TimeBucket {
    /// Human-readable name (e.g., "24h", "7d", "30d").
    pub name: String,
    /// Duration of the bucket in seconds.
    pub duration_secs: u64,
    /// How often to rebuild this bucket's bitmap, in seconds.
    pub refresh_interval_secs: u64,
    /// Pre-computed bitmap of slots whose timestamp value falls within [now - duration, now].
    bitmap: RoaringBitmap,
    /// Unix timestamp (seconds) when this bucket was last rebuilt.
    last_refreshed: u64,
}

impl TimeBucket {
    pub fn new(name: String, duration_secs: u64, refresh_interval_secs: u64) -> Self {
        Self {
            name,
            duration_secs,
            refresh_interval_secs,
            bitmap: RoaringBitmap::new(),
            last_refreshed: 0,
        }
    }

    /// Returns true if the bucket needs a rebuild based on time elapsed since last refresh.
    pub fn needs_refresh(&self, now: u64) -> bool {
        now.saturating_sub(self.last_refreshed) >= self.refresh_interval_secs
    }

    pub fn bitmap(&self) -> &RoaringBitmap {
        &self.bitmap
    }

    pub fn len(&self) -> u64 {
        self.bitmap.len()
    }
}

/// Manages all time buckets for a single timestamp field.
pub struct TimeBucketManager {
    /// The sort field name this manager is associated with (e.g., "sortAt").
    field_name: String,
    /// Buckets keyed by name for fast lookup.
    buckets: HashMap<String, TimeBucket>,
    /// Bucket names sorted by duration (shortest first) for snapping.
    sorted_names: Vec<String>,
}

impl TimeBucketManager {
    pub fn new(field_name: String, bucket_configs: Vec<BucketConfig>) -> Self {
        let mut buckets = HashMap::new();
        let mut sorted_names: Vec<String> = Vec::new();

        for config in bucket_configs {
            let bucket = TimeBucket::new(
                config.name.clone(),
                config.duration_secs,
                config.refresh_interval_secs,
            );
            buckets.insert(config.name.clone(), bucket);
            sorted_names.push(config.name);
        }

        // Sort by duration ascending (shortest first) for snapping.
        sorted_names.sort_by(|a, b| {
            let da = buckets[a].duration_secs;
            let db = buckets[b].duration_secs;
            da.cmp(&db)
        });

        Self {
            field_name,
            buckets,
            sorted_names,
        }
    }

    /// Returns the names of buckets that need a refresh at the given time.
    pub fn refresh_due(&self, now: u64) -> Vec<&str> {
        self.sorted_names
            .iter()
            .filter(|name| {
                self.buckets
                    .get(name.as_str())
                    .map(|b| b.needs_refresh(now))
                    .unwrap_or(false)
            })
            .map(|s| s.as_str())
            .collect()
    }

    /// Rebuilds a bucket's bitmap from an iterator of (slot, timestamp_value) pairs.
    /// A slot qualifies if its timestamp falls in [now - duration_secs, now].
    pub fn rebuild_bucket(
        &mut self,
        bucket_name: &str,
        value_iter: impl Iterator<Item = (u32, u64)>,
        now: u64,
    ) {
        let Some(bucket) = self.buckets.get_mut(bucket_name) else {
            return;
        };

        let duration = bucket.duration_secs;
        let cutoff = now.saturating_sub(duration);

        let mut new_bitmap = RoaringBitmap::new();
        for (slot, ts) in value_iter {
            if ts >= cutoff && ts <= now {
                new_bitmap.insert(slot);
            }
        }

        bucket.bitmap = new_bitmap;
        bucket.last_refreshed = now;
    }

    /// Look up a bucket by name.
    pub fn get_bucket(&self, name: &str) -> Option<&TimeBucket> {
        self.buckets.get(name)
    }

    /// Given a duration from a range filter (e.g., now - filter_value), find the closest bucket
    /// within `tolerance_pct` (as a fraction, e.g., 0.10 for 10%). Returns the bucket name.
    pub fn snap_duration(&self, duration_secs: u64, tolerance_pct: f64) -> Option<&str> {
        let mut best_name: Option<&str> = None;
        let mut best_delta = u64::MAX;

        for name in &self.sorted_names {
            let bucket = &self.buckets[name];
            let bd = bucket.duration_secs;
            let delta = if duration_secs > bd {
                duration_secs - bd
            } else {
                bd - duration_secs
            };

            // Tolerance is relative to the bucket duration.
            let threshold = (bd as f64 * tolerance_pct).round() as u64;
            if delta <= threshold && delta < best_delta {
                best_delta = delta;
                best_name = Some(name.as_str());
            }
        }

        best_name
    }

    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    pub fn total_bitmap_bytes(&self) -> usize {
        self.buckets
            .values()
            .map(|b| b.bitmap.serialized_size())
            .sum()
    }

    pub fn field_name(&self) -> &str {
        &self.field_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_manager(configs: Vec<(&str, u64, u64)>) -> TimeBucketManager {
        let configs = configs
            .into_iter()
            .map(|(name, dur, refresh)| BucketConfig {
                name: name.to_string(),
                duration_secs: dur,
                refresh_interval_secs: refresh,
            })
            .collect();
        TimeBucketManager::new("sortAt".to_string(), configs)
    }

    #[test]
    fn test_basic_lifecycle() {
        let mut mgr = make_manager(vec![
            ("24h", 86400, 300),
            ("7d", 604800, 3600),
        ]);

        let now: u64 = 1_700_000_000;

        // Slots with timestamps within 24h window.
        let data: Vec<(u32, u64)> = vec![
            (1, now - 3600),       // 1h ago — in 24h
            (2, now - 86400 + 1),  // just inside 24h
            (3, now - 86401),      // just outside 24h
            (4, now),              // exactly now — in 24h
            (5, now - 200000),     // way outside 24h
        ];

        mgr.rebuild_bucket("24h", data.iter().copied(), now);

        let bucket = mgr.get_bucket("24h").unwrap();
        assert!(bucket.bitmap().contains(1));
        assert!(bucket.bitmap().contains(2));
        assert!(!bucket.bitmap().contains(3));
        assert!(bucket.bitmap().contains(4));
        assert!(!bucket.bitmap().contains(5));
        assert_eq!(bucket.len(), 3);
    }

    #[test]
    fn test_7d_window() {
        let mut mgr = make_manager(vec![
            ("24h", 86400, 300),
            ("7d", 604800, 3600),
        ]);

        let now: u64 = 1_700_000_000;

        let data: Vec<(u32, u64)> = vec![
            (10, now - 86400),      // 1d ago — in 7d, in 24h boundary
            (11, now - 604800 + 1), // just inside 7d
            (12, now - 604801),     // just outside 7d
            (13, now - 200000),     // 2.3d ago — in 7d
        ];

        mgr.rebuild_bucket("7d", data.iter().copied(), now);

        let bucket = mgr.get_bucket("7d").unwrap();
        assert!(bucket.bitmap().contains(10));
        assert!(bucket.bitmap().contains(11));
        assert!(!bucket.bitmap().contains(12));
        assert!(bucket.bitmap().contains(13));
        assert_eq!(bucket.len(), 3);
    }

    #[test]
    fn test_refresh_timing() {
        let mut mgr = make_manager(vec![
            ("24h", 86400, 300),
        ]);

        let now: u64 = 1_700_000_000;

        // Before any rebuild, last_refreshed = 0, so needs_refresh = true.
        assert!(mgr.get_bucket("24h").unwrap().needs_refresh(now));

        // After rebuild, last_refreshed = now.
        mgr.rebuild_bucket("24h", std::iter::empty(), now);
        let bucket = mgr.get_bucket("24h").unwrap();
        assert!(!bucket.needs_refresh(now));
        assert!(!bucket.needs_refresh(now + 299)); // 299s later, not yet
        assert!(bucket.needs_refresh(now + 300));  // exactly at interval
        assert!(bucket.needs_refresh(now + 500));  // past interval
    }

    #[test]
    fn test_refresh_due() {
        let mut mgr = make_manager(vec![
            ("24h", 86400, 300),
            ("7d", 604800, 3600),
        ]);

        let now: u64 = 1_700_000_000;

        // Before any rebuild, both need refresh.
        let due = mgr.refresh_due(now);
        assert_eq!(due.len(), 2);

        // Rebuild 24h only.
        mgr.rebuild_bucket("24h", std::iter::empty(), now);

        let due = mgr.refresh_due(now);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0], "7d");

        // After refresh interval passes, 24h needs refresh again.
        let due = mgr.refresh_due(now + 301);
        assert_eq!(due.len(), 2);
    }

    #[test]
    fn test_snap_duration_exact() {
        let mgr = make_manager(vec![
            ("24h", 86400, 300),
            ("7d", 604800, 3600),
        ]);

        // Exact match for 24h.
        let snapped = mgr.snap_duration(86400, 0.10);
        assert_eq!(snapped, Some("24h"));

        // Exact match for 7d.
        let snapped = mgr.snap_duration(604800, 0.10);
        assert_eq!(snapped, Some("7d"));
    }

    #[test]
    fn test_snap_duration_within_tolerance() {
        let mgr = make_manager(vec![
            ("24h", 86400, 300),
            ("7d", 604800, 3600),
        ]);

        // 90000s is 86400 + 3600 — delta = 3600, threshold = 86400 * 0.10 = 8640 → within tolerance.
        let snapped = mgr.snap_duration(90000, 0.10);
        assert_eq!(snapped, Some("24h"));

        // 80000s — delta = 6400, threshold = 8640 → within tolerance.
        let snapped = mgr.snap_duration(80000, 0.10);
        assert_eq!(snapped, Some("24h"));
    }

    #[test]
    fn test_snap_duration_outside_tolerance() {
        let mgr = make_manager(vec![
            ("24h", 86400, 300),
            ("7d", 604800, 3600),
        ]);

        // 200000s — far from both.
        // Delta from 24h: 113600, threshold 8640 → miss.
        // Delta from 7d: 404800, threshold 60480 → miss.
        let snapped = mgr.snap_duration(200000, 0.10);
        assert_eq!(snapped, None);
    }

    #[test]
    fn test_empty_rebuild() {
        let mut mgr = make_manager(vec![
            ("24h", 86400, 300),
        ]);

        let now: u64 = 1_700_000_000;
        mgr.rebuild_bucket("24h", std::iter::empty(), now);

        let bucket = mgr.get_bucket("24h").unwrap();
        assert_eq!(bucket.len(), 0);
        assert!(bucket.bitmap().is_empty());
    }

    #[test]
    fn test_sorted_order() {
        // Insert in reverse order (7d first, then 24h).
        let mgr = make_manager(vec![
            ("7d", 604800, 3600),
            ("24h", 86400, 300),
            ("30d", 2592000, 86400),
        ]);

        assert_eq!(mgr.sorted_names[0], "24h");
        assert_eq!(mgr.sorted_names[1], "7d");
        assert_eq!(mgr.sorted_names[2], "30d");
    }

    #[test]
    fn test_bucket_count_and_bytes() {
        let mgr = make_manager(vec![
            ("24h", 86400, 300),
            ("7d", 604800, 3600),
        ]);
        assert_eq!(mgr.bucket_count(), 2);
        // Empty bitmaps — serialized size is minimal but non-negative.
        assert!(mgr.total_bitmap_bytes() < 100);
    }

    #[test]
    fn test_rebuild_unknown_bucket() {
        let mut mgr = make_manager(vec![
            ("24h", 86400, 300),
        ]);
        // Should not panic.
        mgr.rebuild_bucket("nonexistent", std::iter::empty(), 1_700_000_000);
    }
}
