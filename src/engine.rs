use crate::config::Config;
use crate::error::Result;
use crate::executor::QueryExecutor;
use crate::filter::FilterIndex;
use crate::mutation::{Document, MutationEngine, PatchPayload};
use crate::query::{BitdexQuery, FilterClause, SortClause};
use crate::slot::SlotAllocator;
use crate::sort::SortIndex;
use crate::types::QueryResult;

/// The top-level Bitdex engine tying all components together.
///
/// This struct owns all bitmap state and provides the public API
/// for mutations and queries.
pub struct Engine {
    slots: SlotAllocator,
    filters: FilterIndex,
    sorts: SortIndex,
    config: Config,
}

impl Engine {
    /// Create a new engine from configuration.
    pub fn new(config: Config) -> Result<Self> {
        config.validate()?;

        let slots = SlotAllocator::new();
        let mut filters = FilterIndex::new();
        let mut sorts = SortIndex::new();

        for fc in &config.filter_fields {
            filters.add_field(fc.clone());
        }
        for sc in &config.sort_fields {
            sorts.add_field(sc.clone());
        }

        Ok(Self {
            slots,
            filters,
            sorts,
            config,
        })
    }

    /// PUT(id, document) -- full replace with upsert semantics.
    pub fn put(&mut self, id: u32, doc: &Document) -> Result<()> {
        let mut engine = MutationEngine::new(
            &mut self.slots,
            &mut self.filters,
            &mut self.sorts,
            &self.config,
        );
        engine.put(id, doc)
    }

    /// PATCH(id, partial_fields) -- merge only provided fields.
    pub fn patch(&mut self, id: u32, patch: &PatchPayload) -> Result<()> {
        let mut engine = MutationEngine::new(
            &mut self.slots,
            &mut self.filters,
            &mut self.sorts,
            &self.config,
        );
        engine.patch(id, patch)
    }

    /// DELETE(id) -- clear the alive bit. That's it.
    pub fn delete(&mut self, id: u32) -> Result<()> {
        let mut engine = MutationEngine::new(
            &mut self.slots,
            &mut self.filters,
            &mut self.sorts,
            &self.config,
        );
        engine.delete(id)
    }

    /// DELETE WHERE(query) -- resolve query, clear alive bits for matches.
    pub fn delete_where(&mut self, filters: &[FilterClause]) -> Result<u64> {
        // First, resolve the filter to get matching slot IDs
        let executor = QueryExecutor::new(
            &self.slots,
            &self.filters,
            &self.sorts,
            u32::MAX as usize,
        );
        let result = executor.execute(filters, None, u32::MAX as usize, None)?;

        // Build a bitmap of matching slots
        let mut matching = roaring::RoaringBitmap::new();
        for id in &result.ids {
            matching.insert(*id as u32);
        }

        // Now delete them
        let mut engine = MutationEngine::new(
            &mut self.slots,
            &mut self.filters,
            &mut self.sorts,
            &self.config,
        );
        engine.delete_where(&matching)
    }

    /// Execute a parsed query.
    pub fn execute_query(&self, query: &BitdexQuery) -> Result<QueryResult> {
        let executor = QueryExecutor::new(
            &self.slots,
            &self.filters,
            &self.sorts,
            self.config.max_page_size,
        );
        executor.execute(
            &query.filters,
            query.sort.as_ref(),
            query.limit,
            query.cursor.as_ref(),
        )
    }

    /// Execute a query from individual components.
    pub fn query(
        &self,
        filters: &[FilterClause],
        sort: Option<&SortClause>,
        limit: usize,
    ) -> Result<QueryResult> {
        let executor = QueryExecutor::new(
            &self.slots,
            &self.filters,
            &self.sorts,
            self.config.max_page_size,
        );
        executor.execute(filters, sort, limit, None)
    }

    /// Get the number of alive documents.
    pub fn alive_count(&self) -> u64 {
        self.slots.alive_count()
    }

    /// Get the number of dead (deleted but not cleaned) slots.
    pub fn dead_count(&self) -> u64 {
        self.slots.dead_count()
    }

    /// Get the high-water mark slot counter.
    pub fn slot_counter(&self) -> u32 {
        self.slots.slot_counter()
    }

    /// Get a reference to the config.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get a reference to the slot allocator.
    pub fn slots(&self) -> &SlotAllocator {
        &self.slots
    }

    /// Get a mutable reference to the slot allocator (for autovac).
    pub fn slots_mut(&mut self) -> &mut SlotAllocator {
        &mut self.slots
    }

    /// Get a reference to the filter index.
    pub fn filters(&self) -> &FilterIndex {
        &self.filters
    }

    /// Get a mutable reference to the filter index (for autovac).
    pub fn filters_mut(&mut self) -> &mut FilterIndex {
        &mut self.filters
    }

    /// Get a reference to the sort index.
    pub fn sorts(&self) -> &SortIndex {
        &self.sorts
    }

    /// Get a mutable reference to the sort index (for autovac).
    pub fn sorts_mut(&mut self) -> &mut SortIndex {
        &mut self.sorts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FilterFieldConfig, SortFieldConfig};
    use crate::filter::FilterFieldType;
    use crate::mutation::FieldValue;
    use crate::query::{SortDirection, Value};

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
            max_page_size: 100,
            ..Default::default()
        }
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
    fn test_engine_put_and_query() {
        let mut engine = Engine::new(test_config()).unwrap();

        engine
            .put(1, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer(42))),
            ]))
            .unwrap();

        assert_eq!(engine.alive_count(), 1);

        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                None,
                100,
            )
            .unwrap();

        assert_eq!(result.ids, vec![1]);
    }

    #[test]
    fn test_engine_delete_and_query() {
        let mut engine = Engine::new(test_config()).unwrap();

        engine.put(1, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))])).unwrap();
        engine.put(2, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))])).unwrap();

        engine.delete(1).unwrap();

        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                None,
                100,
            )
            .unwrap();

        assert_eq!(result.ids, vec![2]);
    }

    #[test]
    fn test_engine_delete_where() {
        let mut engine = Engine::new(test_config()).unwrap();

        for i in 1..=10u32 {
            engine.put(
                i,
                &make_doc(vec![(
                    "nsfwLevel",
                    FieldValue::Single(Value::Integer(if i <= 5 { 1 } else { 2 })),
                )]),
            ).unwrap();
        }

        let deleted = engine
            .delete_where(&[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))])
            .unwrap();

        assert_eq!(deleted, 5);
        assert_eq!(engine.alive_count(), 5);
    }

    #[test]
    fn test_engine_sorted_query() {
        let mut engine = Engine::new(test_config()).unwrap();

        engine.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(100))),
        ])).unwrap();
        engine.put(2, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(500))),
        ])).unwrap();
        engine.put(3, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(300))),
        ])).unwrap();

        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let result = engine
            .query(
                &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
                Some(&sort),
                10,
            )
            .unwrap();

        assert_eq!(result.ids, vec![2, 3, 1]); // 500, 300, 100
    }

    #[test]
    fn test_engine_full_workflow() {
        let mut engine = Engine::new(test_config()).unwrap();

        for i in 1..=5u32 {
            engine.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("tagIds", FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)])),
                ("onSite", FieldValue::Single(Value::Bool(true))),
                ("reactionCount", FieldValue::Single(Value::Integer((i * 10) as i64))),
            ])).unwrap();
        }

        assert_eq!(engine.alive_count(), 5);

        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let result = engine.query(
            &[
                FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),
                FilterClause::Eq("tagIds".to_string(), Value::Integer(100)),
                FilterClause::Eq("onSite".to_string(), Value::Bool(true)),
            ],
            Some(&sort),
            3,
        ).unwrap();

        assert_eq!(result.total_matched, 5);
        assert_eq!(result.ids, vec![5, 4, 3]);

        engine.delete(5).unwrap();
        assert_eq!(engine.alive_count(), 4);

        let result = engine.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            Some(&sort),
            3,
        ).unwrap();

        assert_eq!(result.ids, vec![4, 3, 2]);
    }

    #[test]
    fn test_execute_parsed_query() {
        let mut engine = Engine::new(test_config()).unwrap();

        engine.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(42))),
        ])).unwrap();

        let query = BitdexQuery {
            filters: vec![FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            sort: Some(SortClause {
                field: "reactionCount".to_string(),
                direction: SortDirection::Desc,
            }),
            limit: 50,
            cursor: None,
        };

        let result = engine.execute_query(&query).unwrap();
        assert_eq!(result.ids, vec![1]);
    }
}
