use std::collections::HashMap;

use roaring::RoaringBitmap;

use crate::config::Config;
use crate::error::{BitdexError, Result};
use crate::filter::FilterIndex;
use crate::query::Value;
use crate::slot::SlotAllocator;
use crate::sort::SortIndex;

/// A document mutation payload for PUT operations.
/// Contains field name -> value mappings for both filter and sort fields.
/// Bitdex does NOT store these values; they are consumed to set bitmap bits.
#[derive(Debug, Clone)]
pub struct Document {
    pub fields: HashMap<String, FieldValue>,
}

/// A field value in a mutation payload.
#[derive(Debug, Clone)]
pub enum FieldValue {
    /// Single value for single_value and boolean fields.
    Single(Value),
    /// Multiple values for multi_value fields (e.g., tags).
    Multi(Vec<Value>),
}

/// A partial update payload for PATCH operations.
/// Contains only the changed fields with old and new values.
#[derive(Debug, Clone)]
pub struct PatchPayload {
    pub fields: HashMap<String, PatchField>,
}

/// A single field change in a PATCH operation.
/// Both old and new values come from the WAL event -- we never look up stored state.
#[derive(Debug, Clone)]
pub struct PatchField {
    pub old: FieldValue,
    pub new: FieldValue,
}

/// Convert a Value to a u64 bitmap key for filter indexing.
fn value_to_bitmap_key(val: &Value) -> Option<u64> {
    match val {
        Value::Bool(b) => Some(if *b { 1 } else { 0 }),
        Value::Integer(v) => Some(*v as u64),
        Value::Float(_) | Value::String(_) => None,
    }
}

/// Convert a Value to a u32 for sort layer bit decomposition.
fn value_to_sort_u32(val: &Value) -> Option<u32> {
    match val {
        Value::Integer(v) => Some(*v as u32),
        _ => None,
    }
}

/// The core mutation engine. Applies PUT/PATCH/DELETE/DELETE WHERE to bitmaps.
pub struct MutationEngine<'a> {
    slots: &'a mut SlotAllocator,
    filters: &'a mut FilterIndex,
    sorts: &'a mut SortIndex,
    config: &'a Config,
}

impl<'a> MutationEngine<'a> {
    pub fn new(
        slots: &'a mut SlotAllocator,
        filters: &'a mut FilterIndex,
        sorts: &'a mut SortIndex,
        config: &'a Config,
    ) -> Self {
        Self {
            slots,
            filters,
            sorts,
            config,
        }
    }

    /// PUT(id, document) -- full replace with upsert semantics.
    ///
    /// 1. Clear any old bits for this slot (handles both live updates and
    ///    re-inserts of previously deleted slots with stale bits)
    /// 2. Allocate slot (sets alive bit)
    /// 3. Set filter bitmaps based on document fields
    /// 4. Set sort layer bitmaps
    pub fn put(&mut self, id: u32, doc: &Document) -> Result<()> {
        // Always clear old bits -- covers both live updates (upsert) and
        // re-inserts of deleted slots that still have stale filter/sort bits.
        // remove_from_all is a no-op for slots with no bits set.
        for (_, field) in self.filters.fields_mut() {
            field.remove_from_all(id);
        }
        for (_, sort_field) in self.sorts.fields_mut() {
            sort_field.remove(id);
        }

        // Allocate slot (sets alive bit)
        self.slots.allocate(id)?;

        // Set filter bitmaps
        for filter_config in &self.config.filter_fields {
            if let Some(field_value) = doc.fields.get(&filter_config.name) {
                if let Some(filter_field) = self.filters.get_field_mut(&filter_config.name) {
                    match field_value {
                        FieldValue::Single(val) => {
                            if let Some(key) = value_to_bitmap_key(val) {
                                filter_field.insert(key, id);
                            }
                        }
                        FieldValue::Multi(vals) => {
                            for val in vals {
                                if let Some(key) = value_to_bitmap_key(val) {
                                    filter_field.insert(key, id);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Set sort layer bitmaps
        for sort_config in &self.config.sort_fields {
            if let Some(field_value) = doc.fields.get(&sort_config.name) {
                if let Some(sort_field) = self.sorts.get_field_mut(&sort_config.name) {
                    if let FieldValue::Single(val) = field_value {
                        if let Some(sort_val) = value_to_sort_u32(val) {
                            sort_field.insert(id, sort_val);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// PATCH(id, partial_fields) -- merge only provided fields.
    ///
    /// For each changed filter field: clear old bitmap bit, set new bitmap bit.
    /// For each changed sort field: XOR old and new values, flip only changed bit layers.
    /// The WAL event provides old and new values -- we do NOT look up old values.
    pub fn patch(&mut self, id: u32, patch: &PatchPayload) -> Result<()> {
        if !self.slots.is_alive(id) {
            return Err(BitdexError::SlotNotFound(id));
        }

        for (field_name, change) in &patch.fields {
            // Update filter bitmaps
            if let Some(filter_field) = self.filters.get_field_mut(field_name) {
                // Clear old values
                match &change.old {
                    FieldValue::Single(val) => {
                        if let Some(key) = value_to_bitmap_key(val) {
                            filter_field.remove(key, id);
                        }
                    }
                    FieldValue::Multi(vals) => {
                        for val in vals {
                            if let Some(key) = value_to_bitmap_key(val) {
                                filter_field.remove(key, id);
                            }
                        }
                    }
                }
                // Set new values
                match &change.new {
                    FieldValue::Single(val) => {
                        if let Some(key) = value_to_bitmap_key(val) {
                            filter_field.insert(key, id);
                        }
                    }
                    FieldValue::Multi(vals) => {
                        for val in vals {
                            if let Some(key) = value_to_bitmap_key(val) {
                                filter_field.insert(key, id);
                            }
                        }
                    }
                }
            }

            // Update sort layer bitmaps
            if let Some(sort_field) = self.sorts.get_field_mut(field_name) {
                if let (FieldValue::Single(old_val), FieldValue::Single(new_val)) =
                    (&change.old, &change.new)
                {
                    if let (Some(old_sort), Some(new_sort)) =
                        (value_to_sort_u32(old_val), value_to_sort_u32(new_val))
                    {
                        sort_field.update(id, old_sort, new_sort);
                    }
                }
            }
        }

        Ok(())
    }

    /// DELETE(id) -- clear the alive bit. That's the entire operation.
    ///
    /// All other bitmaps retain stale bits -- they're invisible behind the alive gate.
    pub fn delete(&mut self, id: u32) -> Result<()> {
        self.slots.delete(id)
    }

    /// DELETE WHERE(predicate) -- resolve predicate, clear alive bits.
    ///
    /// Takes a pre-computed bitmap of matching slots (the caller resolves the predicate
    /// using the query engine).
    pub fn delete_where(&mut self, matching_slots: &RoaringBitmap) -> Result<u64> {
        let mut count = 0u64;
        for slot in matching_slots.iter() {
            if self.slots.is_alive(slot) {
                self.slots.delete(slot)?;
                count += 1;
            }
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FilterFieldConfig, SortFieldConfig};
    use crate::filter::FilterFieldType;

    fn test_config() -> Config {
        Config {
            filter_fields: vec![
                FilterFieldConfig {
                    name: "nsfwLevel".to_string(),
                    field_type: FilterFieldType::SingleValue,
                },
                FilterFieldConfig {
                    name: "tagIds".to_string(),
                    field_type: FilterFieldType::MultiValue,
                },
                FilterFieldConfig {
                    name: "onSite".to_string(),
                    field_type: FilterFieldType::Boolean,
                },
            ],
            sort_fields: vec![SortFieldConfig {
                name: "reactionCount".to_string(),
                source_type: "uint32".to_string(),
                encoding: "linear".to_string(),
                bits: 32,
            }],
            ..Default::default()
        }
    }

    fn setup() -> (SlotAllocator, FilterIndex, SortIndex, Config) {
        let config = test_config();
        let slots = SlotAllocator::new();
        let mut filters = FilterIndex::new();
        let mut sorts = SortIndex::new();

        for fc in &config.filter_fields {
            filters.add_field(fc.clone());
        }
        for sc in &config.sort_fields {
            sorts.add_field(sc.clone());
        }

        (slots, filters, sorts, config)
    }

    fn make_doc(fields: Vec<(&str, FieldValue)>) -> Document {
        Document {
            fields: fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        }
    }

    #[test]
    fn test_put_insert() {
        let (mut slots, mut filters, mut sorts, config) = setup();
        let mut engine = MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config);

        let doc = make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            (
                "tagIds",
                FieldValue::Multi(vec![Value::Integer(456), Value::Integer(789)]),
            ),
            ("onSite", FieldValue::Single(Value::Bool(true))),
            ("reactionCount", FieldValue::Single(Value::Integer(42))),
        ]);

        engine.put(100, &doc).unwrap();

        assert!(slots.is_alive(100));
        assert_eq!(slots.alive_count(), 1);

        assert!(filters
            .get_field("nsfwLevel")
            .unwrap()
            .get(1)
            .unwrap()
            .contains(100));
        assert!(filters
            .get_field("tagIds")
            .unwrap()
            .get(456)
            .unwrap()
            .contains(100));
        assert!(filters
            .get_field("tagIds")
            .unwrap()
            .get(789)
            .unwrap()
            .contains(100));
        assert!(filters
            .get_field("onSite")
            .unwrap()
            .get(1)
            .unwrap()
            .contains(100));

        assert_eq!(
            sorts
                .get_field("reactionCount")
                .unwrap()
                .reconstruct_value(100),
            42
        );
    }

    #[test]
    fn test_put_upsert_replaces_old_values() {
        let (mut slots, mut filters, mut sorts, config) = setup();
        let mut engine = MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config);

        let doc1 = make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(10))),
        ]);
        engine.put(100, &doc1).unwrap();

        let doc2 = make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
            ("reactionCount", FieldValue::Single(Value::Integer(99))),
        ]);
        engine.put(100, &doc2).unwrap();

        // Old filter value gone
        assert!(filters.get_field("nsfwLevel").unwrap().get(1).is_none()
            || !filters
                .get_field("nsfwLevel")
                .unwrap()
                .get(1)
                .unwrap()
                .contains(100));

        // New filter value set
        assert!(filters
            .get_field("nsfwLevel")
            .unwrap()
            .get(2)
            .unwrap()
            .contains(100));

        assert_eq!(
            sorts
                .get_field("reactionCount")
                .unwrap()
                .reconstruct_value(100),
            99
        );
    }

    #[test]
    fn test_patch_filter_field() {
        let (mut slots, mut filters, mut sorts, config) = setup();
        let mut engine = MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config);

        let doc = make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(10))),
        ]);
        engine.put(100, &doc).unwrap();

        let patch = PatchPayload {
            fields: vec![(
                "nsfwLevel".to_string(),
                PatchField {
                    old: FieldValue::Single(Value::Integer(1)),
                    new: FieldValue::Single(Value::Integer(28)),
                },
            )]
            .into_iter()
            .collect(),
        };
        engine.patch(100, &patch).unwrap();

        assert!(filters.get_field("nsfwLevel").unwrap().get(1).is_none()
            || !filters
                .get_field("nsfwLevel")
                .unwrap()
                .get(1)
                .unwrap()
                .contains(100));

        assert!(filters
            .get_field("nsfwLevel")
            .unwrap()
            .get(28)
            .unwrap()
            .contains(100));
    }

    #[test]
    fn test_patch_sort_field_uses_xor() {
        let (mut slots, mut filters, mut sorts, config) = setup();
        let mut engine = MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config);

        let doc = make_doc(vec![
            ("reactionCount", FieldValue::Single(Value::Integer(100))),
        ]);
        engine.put(10, &doc).unwrap();

        let patch = PatchPayload {
            fields: vec![(
                "reactionCount".to_string(),
                PatchField {
                    old: FieldValue::Single(Value::Integer(100)),
                    new: FieldValue::Single(Value::Integer(200)),
                },
            )]
            .into_iter()
            .collect(),
        };
        engine.patch(10, &patch).unwrap();

        assert_eq!(
            sorts
                .get_field("reactionCount")
                .unwrap()
                .reconstruct_value(10),
            200
        );
    }

    #[test]
    fn test_patch_multi_value_field() {
        let (mut slots, mut filters, mut sorts, config) = setup();
        let mut engine = MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config);

        let doc = make_doc(vec![(
            "tagIds",
            FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)]),
        )]);
        engine.put(10, &doc).unwrap();

        let patch = PatchPayload {
            fields: vec![(
                "tagIds".to_string(),
                PatchField {
                    old: FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)]),
                    new: FieldValue::Multi(vec![Value::Integer(200), Value::Integer(300)]),
                },
            )]
            .into_iter()
            .collect(),
        };
        engine.patch(10, &patch).unwrap();

        assert!(filters.get_field("tagIds").unwrap().get(100).is_none()
            || !filters
                .get_field("tagIds")
                .unwrap()
                .get(100)
                .unwrap()
                .contains(10));

        assert!(filters
            .get_field("tagIds")
            .unwrap()
            .get(200)
            .unwrap()
            .contains(10));

        assert!(filters
            .get_field("tagIds")
            .unwrap()
            .get(300)
            .unwrap()
            .contains(10));
    }

    #[test]
    fn test_delete_only_clears_alive() {
        let (mut slots, mut filters, mut sorts, config) = setup();
        let mut engine = MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config);

        let doc = make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(42))),
        ]);
        engine.put(100, &doc).unwrap();
        engine.delete(100).unwrap();

        assert!(!slots.is_alive(100));

        // Filter bitmap still has stale bit (by design!)
        assert!(filters
            .get_field("nsfwLevel")
            .unwrap()
            .get(1)
            .unwrap()
            .contains(100));

        // Sort bitmap still has stale bits (by design!)
        assert_eq!(
            sorts
                .get_field("reactionCount")
                .unwrap()
                .reconstruct_value(100),
            42
        );
    }

    #[test]
    fn test_delete_nonexistent() {
        let (mut slots, mut filters, mut sorts, config) = setup();
        let mut engine = MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config);
        assert!(engine.delete(999).is_err());
    }

    #[test]
    fn test_patch_nonexistent() {
        let (mut slots, mut filters, mut sorts, config) = setup();
        let mut engine = MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config);
        let patch = PatchPayload {
            fields: HashMap::new(),
        };
        assert!(engine.patch(999, &patch).is_err());
    }

    #[test]
    fn test_delete_where() {
        let (mut slots, mut filters, mut sorts, config) = setup();

        // Insert docs
        {
            let mut engine = MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config);
            for i in 0..10u32 {
                let doc = make_doc(vec![(
                    "nsfwLevel",
                    FieldValue::Single(Value::Integer(if i < 5 { 1 } else { 2 })),
                )]);
                engine.put(i, &doc).unwrap();
            }
        }

        // Get matching bitmap, then delete
        let matching = filters.get_field("nsfwLevel").unwrap().get(1).unwrap().clone();
        let mut engine = MutationEngine::new(&mut slots, &mut filters, &mut sorts, &config);
        let deleted = engine.delete_where(&matching).unwrap();
        assert_eq!(deleted, 5);
        assert_eq!(slots.alive_count(), 5);

        for i in 0..5 {
            assert!(!slots.is_alive(i));
        }
        for i in 5..10 {
            assert!(slots.is_alive(i));
        }
    }
}
