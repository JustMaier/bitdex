use std::collections::HashMap;

use roaring::RoaringBitmap;

use crate::config::FilterFieldConfig;

/// Filter bitmap storage for a single field.
///
/// Each distinct value gets its own RoaringBitmap containing all slot positions
/// that have that value. This is the core of Bitdex's filtering.
///
/// Field types:
/// - single_value: each slot appears in exactly one bitmap per field
/// - multi_value: each slot can appear in multiple bitmaps (e.g., tags)
/// - boolean: two bitmaps (true/false), stored as values 0 and 1
pub struct FilterField {
    /// One bitmap per distinct value. Key is the u64 bitmap key.
    bitmaps: HashMap<u64, RoaringBitmap>,
    /// Field configuration.
    config: FilterFieldConfig,
}

impl FilterField {
    pub fn new(config: FilterFieldConfig) -> Self {
        Self {
            bitmaps: HashMap::new(),
            config,
        }
    }

    /// Get the field name.
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Get the field type.
    pub fn field_type(&self) -> &FilterFieldType {
        &self.config.field_type
    }

    /// Set a slot's bit in the bitmap for the given value.
    pub fn insert(&mut self, value: u64, slot: u32) {
        self.bitmaps
            .entry(value)
            .or_insert_with(RoaringBitmap::new)
            .insert(slot);
    }

    /// Clear a slot's bit from the bitmap for the given value.
    pub fn remove(&mut self, value: u64, slot: u32) {
        if let Some(bm) = self.bitmaps.get_mut(&value) {
            bm.remove(slot);
            if bm.is_empty() {
                self.bitmaps.remove(&value);
            }
        }
    }

    /// Clear a slot's bit from ALL bitmaps in this field.
    /// Used by autovac to clean dead slots from filter bitmaps.
    pub fn remove_from_all(&mut self, slot: u32) {
        let mut empty_keys = Vec::new();
        for (&key, bm) in self.bitmaps.iter_mut() {
            bm.remove(slot);
            if bm.is_empty() {
                empty_keys.push(key);
            }
        }
        for key in empty_keys {
            self.bitmaps.remove(&key);
        }
    }

    /// Get the bitmap for a specific value.
    pub fn get(&self, value: u64) -> Option<&RoaringBitmap> {
        self.bitmaps.get(&value)
    }

    /// Get the cardinality (number of set bits) for a specific value.
    pub fn cardinality(&self, value: u64) -> u64 {
        self.bitmaps.get(&value).map_or(0, |bm| bm.len())
    }

    /// Get the number of distinct values tracked.
    pub fn distinct_count(&self) -> usize {
        self.bitmaps.len()
    }

    /// Compute the union of bitmaps for multiple values (OR).
    pub fn union(&self, values: &[u64]) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();
        for value in values {
            if let Some(bm) = self.bitmaps.get(value) {
                result |= bm;
            }
        }
        result
    }

    /// Compute the intersection of bitmaps for multiple values (AND).
    /// Returns None if any value has no bitmap.
    pub fn intersection(&self, values: &[u64]) -> Option<RoaringBitmap> {
        let mut iter = values.iter();
        let first = iter.next()?;
        let mut result = self.bitmaps.get(first)?.clone();
        for value in iter {
            match self.bitmaps.get(value) {
                Some(bm) => result &= bm,
                None => return Some(RoaringBitmap::new()), // Empty intersection
            }
        }
        Some(result)
    }

    /// Iterate over all (value, bitmap) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&u64, &RoaringBitmap)> {
        self.bitmaps.iter()
    }

    /// Get the total number of bitmaps.
    pub fn bitmap_count(&self) -> usize {
        self.bitmaps.len()
    }
}

/// The type of a filter field, determining how values map to bitmaps.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterFieldType {
    /// Each slot has exactly one value for this field.
    SingleValue,
    /// Each slot can have multiple values (e.g., tags).
    MultiValue,
    /// Boolean field: two bitmaps (true=1, false=0).
    Boolean,
}

/// Manages all filter fields.
pub struct FilterIndex {
    /// Map from field name to FilterField.
    fields: HashMap<String, FilterField>,
}

impl FilterIndex {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    /// Add a filter field from configuration.
    pub fn add_field(&mut self, config: FilterFieldConfig) {
        let name = config.name.clone();
        self.fields.insert(name, FilterField::new(config));
    }

    /// Get a reference to a filter field by name.
    pub fn get_field(&self, name: &str) -> Option<&FilterField> {
        self.fields.get(name)
    }

    /// Get a mutable reference to a filter field by name.
    pub fn get_field_mut(&mut self, name: &str) -> Option<&mut FilterField> {
        self.fields.get_mut(name)
    }

    /// Iterate over all fields.
    pub fn fields(&self) -> impl Iterator<Item = (&String, &FilterField)> {
        self.fields.iter()
    }

    /// Iterate mutably over all fields.
    pub fn fields_mut(&mut self) -> impl Iterator<Item = (&String, &mut FilterField)> {
        self.fields.iter_mut()
    }

    /// Get the total number of bitmaps across all fields.
    pub fn total_bitmap_count(&self) -> usize {
        self.fields.values().map(|f| f.bitmap_count()).sum()
    }
}

impl Default for FilterIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_single_value_config(name: &str) -> FilterFieldConfig {
        FilterFieldConfig {
            name: name.to_string(),
            field_type: FilterFieldType::SingleValue,
        }
    }

    fn make_multi_value_config(name: &str) -> FilterFieldConfig {
        FilterFieldConfig {
            name: name.to_string(),
            field_type: FilterFieldType::MultiValue,
        }
    }

    fn make_bool_config(name: &str) -> FilterFieldConfig {
        FilterFieldConfig {
            name: name.to_string(),
            field_type: FilterFieldType::Boolean,
        }
    }

    #[test]
    fn test_insert_and_get() {
        let mut field = FilterField::new(make_single_value_config("nsfwLevel"));
        field.insert(1, 100);
        field.insert(1, 200);
        field.insert(2, 300);

        let bm = field.get(1).unwrap();
        assert_eq!(bm.len(), 2);
        assert!(bm.contains(100));
        assert!(bm.contains(200));

        assert_eq!(field.cardinality(1), 2);
        assert_eq!(field.cardinality(2), 1);
        assert_eq!(field.cardinality(99), 0);
    }

    #[test]
    fn test_remove_specific_value() {
        let mut field = FilterField::new(make_single_value_config("userId"));
        field.insert(42, 10);
        field.insert(42, 20);
        field.insert(42, 30);

        field.remove(42, 20);
        assert_eq!(field.cardinality(42), 2);
        assert!(!field.get(42).unwrap().contains(20));
    }

    #[test]
    fn test_remove_last_cleans_up() {
        let mut field = FilterField::new(make_single_value_config("status"));
        field.insert(1, 10);
        field.remove(1, 10);
        assert!(field.get(1).is_none());
        assert_eq!(field.distinct_count(), 0);
    }

    #[test]
    fn test_remove_from_all() {
        let mut field = FilterField::new(make_multi_value_config("tagIds"));
        field.insert(100, 5);
        field.insert(200, 5);
        field.insert(300, 5);
        field.insert(100, 10);

        field.remove_from_all(5);

        assert!(!field.get(100).unwrap().contains(5));
        assert!(field.get(100).unwrap().contains(10));
        assert!(field.get(200).is_none()); // Was only slot 5
        assert!(field.get(300).is_none()); // Was only slot 5
    }

    #[test]
    fn test_multi_value_field() {
        let mut field = FilterField::new(make_multi_value_config("tagIds"));
        // Document at slot 5 has tags 100, 200, 300
        field.insert(100, 5);
        field.insert(200, 5);
        field.insert(300, 5);
        // Document at slot 10 has tags 200, 400
        field.insert(200, 10);
        field.insert(400, 10);

        assert!(field.get(100).unwrap().contains(5));
        assert!(field.get(200).unwrap().contains(5));
        assert!(field.get(200).unwrap().contains(10));
        assert!(!field.get(100).unwrap().contains(10));
    }

    #[test]
    fn test_boolean_field() {
        let mut field = FilterField::new(make_bool_config("onSite"));
        field.insert(1, 10); // true
        field.insert(1, 20); // true
        field.insert(0, 30); // false

        assert_eq!(field.cardinality(1), 2);
        assert_eq!(field.cardinality(0), 1);
    }

    #[test]
    fn test_union() {
        let mut field = FilterField::new(make_single_value_config("status"));
        field.insert(1, 10);
        field.insert(1, 20);
        field.insert(2, 30);
        field.insert(2, 40);
        field.insert(3, 50);

        let result = field.union(&[1, 2]);
        assert_eq!(result.len(), 4);
        assert!(result.contains(10));
        assert!(result.contains(20));
        assert!(result.contains(30));
        assert!(result.contains(40));
    }

    #[test]
    fn test_intersection() {
        let mut field = FilterField::new(make_multi_value_config("tagIds"));
        // Slot 5 has tags 100, 200
        field.insert(100, 5);
        field.insert(200, 5);
        // Slot 10 has tags 200, 300
        field.insert(200, 10);
        field.insert(300, 10);
        // Slot 15 has tag 100
        field.insert(100, 15);

        let result = field.intersection(&[100, 200]).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains(5)); // Only slot 5 has both 100 and 200
    }

    #[test]
    fn test_intersection_missing_value() {
        let mut field = FilterField::new(make_single_value_config("status"));
        field.insert(1, 10);

        let result = field.intersection(&[1, 999]).unwrap();
        assert!(result.is_empty()); // 999 doesn't exist, so intersection is empty
    }

    #[test]
    fn test_filter_index_multi_field() {
        let mut index = FilterIndex::new();
        index.add_field(make_single_value_config("nsfwLevel"));
        index.add_field(make_multi_value_config("tagIds"));
        index.add_field(make_bool_config("onSite"));

        // Insert some data
        index.get_field_mut("nsfwLevel").unwrap().insert(1, 100);
        index.get_field_mut("tagIds").unwrap().insert(456, 100);
        index.get_field_mut("tagIds").unwrap().insert(789, 100);
        index.get_field_mut("onSite").unwrap().insert(1, 100);

        // Verify
        assert_eq!(index.get_field("nsfwLevel").unwrap().cardinality(1), 1);
        assert_eq!(index.get_field("tagIds").unwrap().cardinality(456), 1);
        assert_eq!(index.get_field("onSite").unwrap().cardinality(1), 1);
    }

    #[test]
    fn test_filter_and_alive_gate() {
        // Simulate the query pattern: filter bitmap AND alive bitmap
        let mut field = FilterField::new(make_single_value_config("status"));
        field.insert(1, 10);
        field.insert(1, 20);
        field.insert(1, 30);

        let mut alive = RoaringBitmap::new();
        alive.insert(10);
        alive.insert(20);
        // Slot 30 is deleted (not in alive)

        let filter_result = field.get(1).unwrap();
        let gated = filter_result & &alive;
        assert_eq!(gated.len(), 2);
        assert!(gated.contains(10));
        assert!(gated.contains(20));
        assert!(!gated.contains(30)); // Filtered out by alive gate
    }
}
