use roaring::RoaringBitmap;

use crate::config::SortFieldConfig;

/// Sort layer bitmaps for a single sortable field.
///
/// Each sortable numeric field is decomposed into N bitmaps, one per bit position.
/// A u32 field = 32 bitmaps (bit 0 through bit 31).
///
/// Bitmap `bit_layers[5]` has a 1 for every slot where bit 5 of that field's value is set.
///
/// Top-N retrieval: start at the most significant bit layer, AND with the filter result.
/// If the intersection has >= N results, narrow to that set and descend.
/// If < N, keep both groups and continue. This is binary search across all documents
/// simultaneously. All sort operations are bitmap AND operations.
pub struct SortField {
    /// One bitmap per bit position. Index 0 = LSB, index 31 = MSB (for 32-bit).
    bit_layers: Vec<RoaringBitmap>,
    /// Number of bit layers (typically 32 for u32).
    num_bits: usize,
    /// Field configuration.
    config: SortFieldConfig,
}

impl SortField {
    pub fn new(config: SortFieldConfig) -> Self {
        let num_bits = config.bits as usize;
        let bit_layers = (0..num_bits).map(|_| RoaringBitmap::new()).collect();
        Self {
            bit_layers,
            num_bits,
            config,
        }
    }

    /// Get the field name.
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Get the number of bit layers.
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Insert a value for a slot. Sets the appropriate bits in each layer.
    pub fn insert(&mut self, slot: u32, value: u32) {
        for bit in 0..self.num_bits {
            if (value >> bit) & 1 == 1 {
                self.bit_layers[bit].insert(slot);
            }
        }
    }

    /// Remove a slot from all bit layers. Used by autovac.
    pub fn remove(&mut self, slot: u32) {
        for layer in &mut self.bit_layers {
            layer.remove(slot);
        }
    }

    /// Update a slot's value using XOR diff.
    /// Only flips the bit layers where old and new values differ.
    pub fn update(&mut self, slot: u32, old_value: u32, new_value: u32) {
        let diff = old_value ^ new_value;
        for bit in 0..self.num_bits {
            if (diff >> bit) & 1 == 1 {
                // This bit changed, flip it in the layer
                if self.bit_layers[bit].contains(slot) {
                    self.bit_layers[bit].remove(slot);
                } else {
                    self.bit_layers[bit].insert(slot);
                }
            }
        }
    }

    /// Get a reference to a specific bit layer.
    pub fn layer(&self, bit: usize) -> Option<&RoaringBitmap> {
        self.bit_layers.get(bit)
    }

    /// Perform top-N sort traversal on a candidate set.
    ///
    /// Traverses from MSB to LSB. At each layer:
    /// - For descending: prefer slots with the bit SET (higher values first)
    /// - For ascending: prefer slots with the bit CLEAR (lower values first)
    ///
    /// Returns ordered slot IDs.
    pub fn top_n(
        &self,
        candidates: &RoaringBitmap,
        limit: usize,
        descending: bool,
        cursor: Option<(u64, u32)>,
    ) -> Vec<u32> {
        if candidates.is_empty() || limit == 0 {
            return Vec::new();
        }

        // Apply cursor: partition candidates into groups based on sort value relative to cursor
        if let Some((cursor_sort_value, cursor_slot_id)) = cursor {
            return self.top_n_with_cursor(
                candidates,
                limit,
                descending,
                cursor_sort_value,
                cursor_slot_id,
            );
        }

        // Collect all candidates with their reconstructed sort values
        let mut entries: Vec<(u32, u32)> = Vec::new(); // (slot, value)
        for slot in candidates.iter() {
            let value = self.reconstruct_value(slot);
            entries.push((slot, value));
        }

        // Sort by value
        if descending {
            entries.sort_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
        } else {
            entries.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        }

        entries.iter().take(limit).map(|(slot, _)| *slot).collect()
    }

    /// Top-N with cursor-based pagination.
    fn top_n_with_cursor(
        &self,
        candidates: &RoaringBitmap,
        limit: usize,
        descending: bool,
        cursor_sort_value: u64,
        cursor_slot_id: u32,
    ) -> Vec<u32> {
        let cursor_value = cursor_sort_value as u32;

        let mut entries: Vec<(u32, u32)> = Vec::new();
        for slot in candidates.iter() {
            let value = self.reconstruct_value(slot);
            let include = if descending {
                // For descending: value < cursor_value, OR (value == cursor_value AND slot < cursor_slot_id)
                value < cursor_value
                    || (value == cursor_value && slot < cursor_slot_id)
            } else {
                // For ascending: value > cursor_value, OR (value == cursor_value AND slot > cursor_slot_id)
                value > cursor_value
                    || (value == cursor_value && slot > cursor_slot_id)
            };
            if include {
                entries.push((slot, value));
            }
        }

        if descending {
            entries.sort_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
        } else {
            entries.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        }

        entries.iter().take(limit).map(|(slot, _)| *slot).collect()
    }

    /// Reconstruct the sort value for a given slot by reading its bits.
    pub fn reconstruct_value(&self, slot: u32) -> u32 {
        let mut value = 0u32;
        for bit in 0..self.num_bits {
            if self.bit_layers[bit].contains(slot) {
                value |= 1 << bit;
            }
        }
        value
    }
}

/// Manages all sort fields.
pub struct SortIndex {
    /// Map from field name to SortField.
    fields: std::collections::HashMap<String, SortField>,
}

impl SortIndex {
    pub fn new() -> Self {
        Self {
            fields: std::collections::HashMap::new(),
        }
    }

    /// Add a sort field from configuration.
    pub fn add_field(&mut self, config: SortFieldConfig) {
        let name = config.name.clone();
        self.fields.insert(name, SortField::new(config));
    }

    /// Get a reference to a sort field by name.
    pub fn get_field(&self, name: &str) -> Option<&SortField> {
        self.fields.get(name)
    }

    /// Get a mutable reference to a sort field by name.
    pub fn get_field_mut(&mut self, name: &str) -> Option<&mut SortField> {
        self.fields.get_mut(name)
    }

    /// Iterate over all fields.
    pub fn fields(&self) -> impl Iterator<Item = (&String, &SortField)> {
        self.fields.iter()
    }

    /// Iterate mutably over all fields.
    pub fn fields_mut(&mut self) -> impl Iterator<Item = (&String, &mut SortField)> {
        self.fields.iter_mut()
    }
}

impl Default for SortIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(name: &str) -> SortFieldConfig {
        SortFieldConfig {
            name: name.to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
        }
    }

    #[test]
    fn test_insert_and_reconstruct() {
        let mut sf = SortField::new(make_config("reactionCount"));
        sf.insert(10, 42);
        assert_eq!(sf.reconstruct_value(10), 42);
    }

    #[test]
    fn test_insert_zero() {
        let mut sf = SortField::new(make_config("count"));
        sf.insert(5, 0);
        assert_eq!(sf.reconstruct_value(5), 0);
    }

    #[test]
    fn test_insert_max_u32() {
        let mut sf = SortField::new(make_config("count"));
        sf.insert(5, u32::MAX);
        assert_eq!(sf.reconstruct_value(5), u32::MAX);
    }

    #[test]
    fn test_bit_layers_correctness() {
        let mut sf = SortField::new(make_config("count"));
        // Value 5 = binary 101 -> bits 0 and 2 are set
        sf.insert(10, 5);

        assert!(sf.layer(0).unwrap().contains(10)); // bit 0
        assert!(!sf.layer(1).unwrap().contains(10)); // bit 1
        assert!(sf.layer(2).unwrap().contains(10)); // bit 2
        for bit in 3..32 {
            assert!(!sf.layer(bit).unwrap().contains(10));
        }
    }

    #[test]
    fn test_update_xor_diff() {
        let mut sf = SortField::new(make_config("reactionCount"));
        sf.insert(10, 100);
        assert_eq!(sf.reconstruct_value(10), 100);

        sf.update(10, 100, 200);
        assert_eq!(sf.reconstruct_value(10), 200);
    }

    #[test]
    fn test_update_only_changed_bits() {
        let mut sf = SortField::new(make_config("count"));
        // Value 5 = 101, Value 6 = 110 -> diff = 011 (bits 0 and 1 flip)
        sf.insert(10, 5);

        // Before update: bit 0 = 1, bit 1 = 0, bit 2 = 1
        assert!(sf.layer(0).unwrap().contains(10));
        assert!(!sf.layer(1).unwrap().contains(10));
        assert!(sf.layer(2).unwrap().contains(10));

        sf.update(10, 5, 6);

        // After update: bit 0 = 0, bit 1 = 1, bit 2 = 1
        assert!(!sf.layer(0).unwrap().contains(10));
        assert!(sf.layer(1).unwrap().contains(10));
        assert!(sf.layer(2).unwrap().contains(10));
        assert_eq!(sf.reconstruct_value(10), 6);
    }

    #[test]
    fn test_update_same_value_noop() {
        let mut sf = SortField::new(make_config("count"));
        sf.insert(10, 42);
        sf.update(10, 42, 42); // No change
        assert_eq!(sf.reconstruct_value(10), 42);
    }

    #[test]
    fn test_remove() {
        let mut sf = SortField::new(make_config("count"));
        sf.insert(10, 255);
        sf.remove(10);
        assert_eq!(sf.reconstruct_value(10), 0);
    }

    #[test]
    fn test_top_n_descending() {
        let mut sf = SortField::new(make_config("reactionCount"));
        sf.insert(1, 100);
        sf.insert(2, 500);
        sf.insert(3, 200);
        sf.insert(4, 50);
        sf.insert(5, 300);

        let mut candidates = RoaringBitmap::new();
        for i in 1..=5 {
            candidates.insert(i);
        }

        let result = sf.top_n(&candidates, 3, true, None);
        assert_eq!(result, vec![2, 5, 3]); // 500, 300, 200
    }

    #[test]
    fn test_top_n_ascending() {
        let mut sf = SortField::new(make_config("reactionCount"));
        sf.insert(1, 100);
        sf.insert(2, 500);
        sf.insert(3, 200);
        sf.insert(4, 50);
        sf.insert(5, 300);

        let mut candidates = RoaringBitmap::new();
        for i in 1..=5 {
            candidates.insert(i);
        }

        let result = sf.top_n(&candidates, 3, false, None);
        assert_eq!(result, vec![4, 1, 3]); // 50, 100, 200
    }

    #[test]
    fn test_top_n_with_limit_larger_than_candidates() {
        let mut sf = SortField::new(make_config("count"));
        sf.insert(1, 10);
        sf.insert(2, 20);

        let mut candidates = RoaringBitmap::new();
        candidates.insert(1);
        candidates.insert(2);

        let result = sf.top_n(&candidates, 100, true, None);
        assert_eq!(result.len(), 2);
        assert_eq!(result, vec![2, 1]); // 20, 10
    }

    #[test]
    fn test_top_n_tiebreak_by_slot_id() {
        let mut sf = SortField::new(make_config("count"));
        // Multiple slots with the same value
        sf.insert(10, 42);
        sf.insert(20, 42);
        sf.insert(30, 42);

        let mut candidates = RoaringBitmap::new();
        candidates.insert(10);
        candidates.insert(20);
        candidates.insert(30);

        // Descending: higher slot ID first for tiebreak
        let result = sf.top_n(&candidates, 3, true, None);
        assert_eq!(result, vec![30, 20, 10]);

        // Ascending: lower slot ID first for tiebreak
        let result = sf.top_n(&candidates, 3, false, None);
        assert_eq!(result, vec![10, 20, 30]);
    }

    #[test]
    fn test_top_n_empty_candidates() {
        let sf = SortField::new(make_config("count"));
        let candidates = RoaringBitmap::new();
        let result = sf.top_n(&candidates, 10, true, None);
        assert!(result.is_empty());
    }

    #[test]
    fn test_cursor_pagination_descending() {
        let mut sf = SortField::new(make_config("reactionCount"));
        for i in 1..=10u32 {
            sf.insert(i, i * 10);
        }

        let mut candidates = RoaringBitmap::new();
        for i in 1..=10 {
            candidates.insert(i);
        }

        // First page: top 3 descending
        let page1 = sf.top_n(&candidates, 3, true, None);
        assert_eq!(page1, vec![10, 9, 8]); // values 100, 90, 80

        // Second page: cursor at (80, 8)
        let page2 = sf.top_n(&candidates, 3, true, Some((80, 8)));
        assert_eq!(page2, vec![7, 6, 5]); // values 70, 60, 50
    }

    #[test]
    fn test_cursor_pagination_ascending() {
        let mut sf = SortField::new(make_config("count"));
        for i in 1..=10u32 {
            sf.insert(i, i * 10);
        }

        let mut candidates = RoaringBitmap::new();
        for i in 1..=10 {
            candidates.insert(i);
        }

        // First page: top 3 ascending
        let page1 = sf.top_n(&candidates, 3, false, None);
        assert_eq!(page1, vec![1, 2, 3]); // values 10, 20, 30

        // Second page: cursor at (30, 3)
        let page2 = sf.top_n(&candidates, 3, false, Some((30, 3)));
        assert_eq!(page2, vec![4, 5, 6]); // values 40, 50, 60
    }

    #[test]
    fn test_sort_index_multi_field() {
        let mut index = SortIndex::new();
        index.add_field(make_config("reactionCount"));
        index.add_field(make_config("commentCount"));

        index.get_field_mut("reactionCount").unwrap().insert(1, 100);
        index.get_field_mut("commentCount").unwrap().insert(1, 5);

        assert_eq!(
            index
                .get_field("reactionCount")
                .unwrap()
                .reconstruct_value(1),
            100
        );
        assert_eq!(
            index
                .get_field("commentCount")
                .unwrap()
                .reconstruct_value(1),
            5
        );
    }

    #[test]
    fn test_multiple_slots_independent() {
        let mut sf = SortField::new(make_config("count"));
        sf.insert(1, 100);
        sf.insert(2, 200);
        sf.insert(3, 300);

        assert_eq!(sf.reconstruct_value(1), 100);
        assert_eq!(sf.reconstruct_value(2), 200);
        assert_eq!(sf.reconstruct_value(3), 300);

        // Update one doesn't affect others
        sf.update(2, 200, 999);
        assert_eq!(sf.reconstruct_value(1), 100);
        assert_eq!(sf.reconstruct_value(2), 999);
        assert_eq!(sf.reconstruct_value(3), 300);
    }
}
