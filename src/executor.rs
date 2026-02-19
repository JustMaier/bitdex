use roaring::RoaringBitmap;

use crate::cache::{self, CacheLookup, TrieCache};
use crate::error::{BitdexError, Result};
use crate::filter::FilterIndex;
use crate::planner;
use crate::query::{FilterClause, SortClause, SortDirection, Value};
use crate::slot::SlotAllocator;
use crate::sort::SortIndex;
use crate::types::QueryResult;

/// Convert a Value to a u64 bitmap key for filter indexing.
fn value_to_bitmap_key(val: &Value) -> Option<u64> {
    match val {
        Value::Bool(b) => Some(if *b { 1 } else { 0 }),
        Value::Integer(v) => Some(*v as u64),
        Value::Float(_) | Value::String(_) => None,
    }
}

/// Query executor: computes filter intersections and sort traversals.
/// Uses the query planner for cardinality-based clause ordering.
pub struct QueryExecutor<'a> {
    slots: &'a SlotAllocator,
    filters: &'a FilterIndex,
    sorts: &'a SortIndex,
    max_page_size: usize,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(
        slots: &'a SlotAllocator,
        filters: &'a FilterIndex,
        sorts: &'a SortIndex,
        max_page_size: usize,
    ) -> Self {
        Self {
            slots,
            filters,
            sorts,
            max_page_size,
        }
    }

    /// Execute a full query: plan -> filter -> sort -> paginate -> return IDs.
    pub fn execute(
        &self,
        filters: &[FilterClause],
        sort: Option<&SortClause>,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
    ) -> Result<QueryResult> {
        let limit = limit.min(self.max_page_size);

        // Step 1: Plan the query (reorder clauses by cardinality)
        let plan = planner::plan_query(filters, self.filters, self.slots);

        // Step 2: Compute filter bitmap using planned clause order
        let filter_bitmap = self.compute_filters(&plan.ordered_clauses)?;

        // Step 3: AND with alive bitmap (implicit in every query)
        let alive = self.slots.alive_bitmap();
        let candidates = &filter_bitmap & alive;

        let total_matched = candidates.len();

        // Step 4: Sort and paginate
        let (ids, next_cursor) = if let Some(sort_clause) = sort {
            if plan.use_simple_sort {
                self.simple_sort_and_paginate(&candidates, sort_clause, limit, cursor)?
            } else {
                self.sort_and_paginate(&candidates, sort_clause, limit, cursor)?
            }
        } else {
            // No sort: return first N slot IDs as-is
            let ids: Vec<i64> = candidates.iter().take(limit).map(|s| s as i64).collect();
            let next_cursor = ids.last().map(|&last_id| crate::query::CursorPosition {
                sort_value: 0,
                slot_id: last_id as u32,
            });
            (ids, next_cursor)
        };

        Ok(QueryResult {
            ids,
            cursor: next_cursor,
            total_matched,
        })
    }

    /// Execute a query with trie cache integration.
    /// Checks cache first, falls back to computation, stores results.
    pub fn execute_with_cache(
        &self,
        filters: &[FilterClause],
        sort: Option<&SortClause>,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
        cache: &mut TrieCache,
    ) -> Result<QueryResult> {
        let limit = limit.min(self.max_page_size);

        // Step 1: Plan the query (reorder clauses by cardinality)
        let plan = planner::plan_query(filters, self.filters, self.slots);

        // Step 2: Try cache lookup using canonical key
        let cache_key = cache::canonicalize(&plan.ordered_clauses);

        let filter_bitmap = if let Some(ref key) = cache_key {
            match cache.lookup(key) {
                CacheLookup::ExactHit(cached_bitmap) => {
                    // Cache hit -- use cached bitmap directly
                    cached_bitmap
                }
                CacheLookup::PrefixHit { bitmap: prefix_bitmap, matched_prefix_len } => {
                    // Partial cache hit -- compute remaining clauses against cached prefix
                    let remaining = &plan.ordered_clauses[matched_prefix_len..];
                    let mut result = prefix_bitmap;
                    for clause in remaining {
                        let clause_bitmap = self.evaluate_clause(clause)?;
                        result &= &clause_bitmap;
                    }
                    // Store the full result in cache
                    cache.store(key, result.clone());
                    result
                }
                CacheLookup::Miss => {
                    // Full miss -- compute from scratch
                    let result = self.compute_filters(&plan.ordered_clauses)?;
                    cache.store(key, result.clone());
                    result
                }
            }
        } else {
            // Uncacheable query (contains compound clauses) -- compute without cache
            self.compute_filters(&plan.ordered_clauses)?
        };

        // Step 3: AND with alive bitmap (implicit in every query)
        let alive = self.slots.alive_bitmap();
        let candidates = &filter_bitmap & alive;

        let total_matched = candidates.len();

        // Step 4: Sort and paginate
        let (ids, next_cursor) = if let Some(sort_clause) = sort {
            if plan.use_simple_sort {
                self.simple_sort_and_paginate(&candidates, sort_clause, limit, cursor)?
            } else {
                self.sort_and_paginate(&candidates, sort_clause, limit, cursor)?
            }
        } else {
            let ids: Vec<i64> = candidates.iter().take(limit).map(|s| s as i64).collect();
            let next_cursor = ids.last().map(|&last_id| crate::query::CursorPosition {
                sort_value: 0,
                slot_id: last_id as u32,
            });
            (ids, next_cursor)
        };

        Ok(QueryResult {
            ids,
            cursor: next_cursor,
            total_matched,
        })
    }

    /// Compute the combined filter bitmap from a list of filter clauses.
    /// Top-level clauses are implicitly ANDed together.
    /// Clauses are expected to be pre-ordered by the planner for optimal evaluation.
    fn compute_filters(&self, clauses: &[FilterClause]) -> Result<RoaringBitmap> {
        if clauses.is_empty() {
            return Ok(self.slots.alive_bitmap().clone());
        }

        let mut result: Option<RoaringBitmap> = None;

        for clause in clauses {
            let bitmap = self.evaluate_clause(clause)?;
            result = Some(match result {
                Some(existing) => existing & &bitmap,
                None => bitmap,
            });
        }

        Ok(result.unwrap_or_default())
    }

    /// Evaluate a single filter clause to a bitmap.
    fn evaluate_clause(&self, clause: &FilterClause) -> Result<RoaringBitmap> {
        match clause {
            FilterClause::Eq(field, value) => {
                let filter_field = self
                    .filters
                    .get_field(field)
                    .ok_or_else(|| BitdexError::FieldNotFound(field.clone()))?;
                let key = value_to_bitmap_key(value)
                    .ok_or_else(|| BitdexError::InvalidValue {
                        field: field.clone(),
                        reason: "cannot convert to bitmap key".to_string(),
                    })?;
                Ok(filter_field.get(key).cloned().unwrap_or_default())
            }

            FilterClause::NotEq(field, value) => {
                // Use andnot optimization: compute the small negated bitmap
                // and subtract from alive, instead of computing the large complement
                let eq_bitmap = self.evaluate_clause(&FilterClause::Eq(field.clone(), value.clone()))?;
                let alive = self.slots.alive_bitmap();
                let mut result = alive.clone();
                result -= &eq_bitmap;
                Ok(result)
            }

            FilterClause::In(field, values) => {
                let filter_field = self
                    .filters
                    .get_field(field)
                    .ok_or_else(|| BitdexError::FieldNotFound(field.clone()))?;
                let keys: Vec<u64> = values
                    .iter()
                    .filter_map(value_to_bitmap_key)
                    .collect();
                Ok(filter_field.union(&keys))
            }

            FilterClause::Not(inner) => {
                // NOT uses andnot: compute inner bitmap and subtract from alive
                let inner_bitmap = self.evaluate_clause(inner)?;
                let alive = self.slots.alive_bitmap();
                let mut result = alive.clone();
                result -= &inner_bitmap;
                Ok(result)
            }

            FilterClause::And(clauses) => {
                // Optimize And sub-clauses by reordering by cardinality
                let optimized = planner::optimize_and_clause(
                    clauses,
                    self.filters,
                    self.slots.alive_count(),
                );
                let mut result: Option<RoaringBitmap> = None;
                for clause in &optimized {
                    let bitmap = self.evaluate_clause(clause)?;
                    result = Some(match result {
                        Some(existing) => existing & &bitmap,
                        None => bitmap,
                    });
                }
                Ok(result.unwrap_or_default())
            }

            FilterClause::Or(clauses) => {
                let mut result = RoaringBitmap::new();
                for clause in clauses {
                    let bitmap = self.evaluate_clause(clause)?;
                    result |= &bitmap;
                }
                Ok(result)
            }

            FilterClause::Gt(field, value) => self.range_scan(field, value, |k, t| k > t),
            FilterClause::Gte(field, value) => self.range_scan(field, value, |k, t| k >= t),
            FilterClause::Lt(field, value) => self.range_scan(field, value, |k, t| k < t),
            FilterClause::Lte(field, value) => self.range_scan(field, value, |k, t| k <= t),
        }
    }

    /// Evaluate a range filter by scanning the filter field's bitmaps.
    fn range_scan<F>(
        &self,
        field: &str,
        value: &Value,
        predicate: F,
    ) -> Result<RoaringBitmap>
    where
        F: Fn(u64, u64) -> bool,
    {
        let filter_field = self
            .filters
            .get_field(field)
            .ok_or_else(|| BitdexError::FieldNotFound(field.to_string()))?;
        let target = value_to_bitmap_key(value)
            .ok_or_else(|| BitdexError::InvalidValue {
                field: field.to_string(),
                reason: "cannot convert to bitmap key for range filter".to_string(),
            })?;

        let mut result = RoaringBitmap::new();
        for (&key, bitmap) in filter_field.iter() {
            if predicate(key, target) {
                result |= bitmap;
            }
        }
        Ok(result)
    }

    /// Sort candidates using bitmap sort layer traversal.
    fn sort_and_paginate(
        &self,
        candidates: &RoaringBitmap,
        sort: &SortClause,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
    ) -> Result<(Vec<i64>, Option<crate::query::CursorPosition>)> {
        let sort_field = self
            .sorts
            .get_field(&sort.field)
            .ok_or_else(|| BitdexError::FieldNotFound(sort.field.clone()))?;

        let descending = sort.direction == SortDirection::Desc;
        let cursor_param = cursor.map(|c| (c.sort_value, c.slot_id));

        let sorted_slots = sort_field.top_n(candidates, limit, descending, cursor_param);

        let ids: Vec<i64> = sorted_slots.iter().map(|&s| s as i64).collect();

        let next_cursor = sorted_slots.last().map(|&last_slot| {
            let sort_value = sort_field.reconstruct_value(last_slot) as u64;
            crate::query::CursorPosition {
                sort_value,
                slot_id: last_slot,
            }
        });

        Ok((ids, next_cursor))
    }

    /// Simple in-memory sort for small result sets.
    /// When the planner estimates the result set is small, this avoids walking 32 bit layers.
    fn simple_sort_and_paginate(
        &self,
        candidates: &RoaringBitmap,
        sort: &SortClause,
        limit: usize,
        cursor: Option<&crate::query::CursorPosition>,
    ) -> Result<(Vec<i64>, Option<crate::query::CursorPosition>)> {
        let sort_field = self
            .sorts
            .get_field(&sort.field)
            .ok_or_else(|| BitdexError::FieldNotFound(sort.field.clone()))?;

        let descending = sort.direction == SortDirection::Desc;

        // Reconstruct values and collect into Vec
        let mut entries: Vec<(u32, u32)> = candidates
            .iter()
            .map(|slot| (slot, sort_field.reconstruct_value(slot)))
            .collect();

        // Sort by value, tiebreak by slot ID
        if descending {
            entries.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
        } else {
            entries.sort_unstable_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        }

        // Apply cursor filtering
        if let Some(cursor) = cursor {
            let cursor_value = cursor.sort_value as u32;
            let cursor_slot = cursor.slot_id;
            entries.retain(|&(slot, value)| {
                if descending {
                    value < cursor_value || (value == cursor_value && slot < cursor_slot)
                } else {
                    value > cursor_value || (value == cursor_value && slot > cursor_slot)
                }
            });
        }

        // Take limit
        let result_slots: Vec<u32> = entries.iter().take(limit).map(|&(slot, _)| slot).collect();

        let ids: Vec<i64> = result_slots.iter().map(|&s| s as i64).collect();

        let next_cursor = result_slots.last().map(|&last_slot| {
            let sort_value = sort_field.reconstruct_value(last_slot) as u64;
            crate::query::CursorPosition {
                sort_value,
                slot_id: last_slot,
            }
        });

        Ok((ids, next_cursor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, FilterFieldConfig, SortFieldConfig};
    use crate::filter::FilterFieldType;
    use crate::mutation::{Document, FieldValue, MutationEngine};

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
                FilterFieldConfig {
                    name: "userId".to_string(),
                    field_type: FilterFieldType::SingleValue,
                },
            ],
            sort_fields: vec![
                SortFieldConfig {
                    name: "reactionCount".to_string(),
                    source_type: "uint32".to_string(),
                    encoding: "linear".to_string(),
                    bits: 32,
                },
            ],
            max_page_size: 100,
            ..Default::default()
        }
    }

    struct TestHarness {
        slots: SlotAllocator,
        filters: FilterIndex,
        sorts: SortIndex,
        config: Config,
    }

    impl TestHarness {
        fn new() -> Self {
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

            Self { slots, filters, sorts, config }
        }

        fn put(&mut self, id: u32, doc: &Document) {
            let mut engine = MutationEngine::new(
                &mut self.slots,
                &mut self.filters,
                &mut self.sorts,
                &self.config,
            );
            engine.put(id, doc).unwrap();
        }

        fn query(
            &self,
            filters: &[FilterClause],
            sort: Option<&SortClause>,
            limit: usize,
            cursor: Option<&crate::query::CursorPosition>,
        ) -> QueryResult {
            let executor = QueryExecutor::new(
                &self.slots,
                &self.filters,
                &self.sorts,
                self.config.max_page_size,
            );
            executor.execute(filters, sort, limit, cursor).unwrap()
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
    fn test_basic_eq_filter() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(100))),
        ]));
        h.put(2, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
            ("reactionCount", FieldValue::Single(Value::Integer(200))),
        ]));
        h.put(3, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(300))),
        ]));

        let result = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 2);
        assert!(result.ids.contains(&1));
        assert!(result.ids.contains(&3));
    }

    #[test]
    fn test_not_eq_filter() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(28)))]));
        h.put(2, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));
        h.put(3, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));

        let result = h.query(
            &[FilterClause::NotEq("nsfwLevel".to_string(), Value::Integer(28))],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 2);
        assert!(result.ids.contains(&2));
        assert!(result.ids.contains(&3));
    }

    #[test]
    fn test_in_filter() {
        let mut h = TestHarness::new();

        for i in 1..=10u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer((i % 3) as i64))),
            ]));
        }

        let result = h.query(
            &[FilterClause::In("nsfwLevel".to_string(), vec![Value::Integer(0), Value::Integer(1)])],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 7);
    }

    #[test]
    fn test_and_filter() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("onSite", FieldValue::Single(Value::Bool(true))),
        ]));
        h.put(2, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("onSite", FieldValue::Single(Value::Bool(false))),
        ]));
        h.put(3, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(2))),
            ("onSite", FieldValue::Single(Value::Bool(true))),
        ]));

        let result = h.query(
            &[FilterClause::And(vec![
                FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),
                FilterClause::Eq("onSite".to_string(), Value::Bool(true)),
            ])],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 1);
        assert_eq!(result.ids, vec![1]);
    }

    #[test]
    fn test_or_filter() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));
        h.put(2, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(2)))]));
        h.put(3, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(3)))]));

        let result = h.query(
            &[FilterClause::Or(vec![
                FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1)),
                FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(3)),
            ])],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 2);
        assert!(result.ids.contains(&1));
        assert!(result.ids.contains(&3));
    }

    #[test]
    fn test_sort_descending() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(100))),
        ]));
        h.put(2, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(500))),
        ]));
        h.put(3, &make_doc(vec![
            ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
            ("reactionCount", FieldValue::Single(Value::Integer(200))),
        ]));

        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };
        let result = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            Some(&sort),
            3,
            None,
        );

        assert_eq!(result.ids, vec![2, 3, 1]); // 500, 200, 100
    }

    #[test]
    fn test_cursor_pagination() {
        let mut h = TestHarness::new();

        for i in 1..=10u32 {
            h.put(i, &make_doc(vec![
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                ("reactionCount", FieldValue::Single(Value::Integer((i * 10) as i64))),
            ]));
        }

        let sort = SortClause {
            field: "reactionCount".to_string(),
            direction: SortDirection::Desc,
        };

        let page1 = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            Some(&sort),
            3,
            None,
        );
        assert_eq!(page1.ids, vec![10, 9, 8]);
        assert!(page1.cursor.is_some());

        let page2 = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            Some(&sort),
            3,
            page1.cursor.as_ref(),
        );
        assert_eq!(page2.ids, vec![7, 6, 5]);
    }

    #[test]
    fn test_deleted_invisible() {
        let mut h = TestHarness::new();

        h.put(1, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));
        h.put(2, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));

        {
            let mut engine = MutationEngine::new(
                &mut h.slots,
                &mut h.filters,
                &mut h.sorts,
                &h.config,
            );
            engine.delete(1).unwrap();
        }

        let result = h.query(
            &[FilterClause::Eq("nsfwLevel".to_string(), Value::Integer(1))],
            None,
            100,
            None,
        );

        assert_eq!(result.total_matched, 1);
        assert_eq!(result.ids, vec![2]);
    }

    #[test]
    fn test_no_filters_returns_all_alive() {
        let mut h = TestHarness::new();

        for i in 1..=5u32 {
            h.put(i, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));
        }

        let result = h.query(&[], None, 100, None);
        assert_eq!(result.total_matched, 5);
    }

    #[test]
    fn test_max_page_size_enforced() {
        let mut h = TestHarness::new();
        h.config.max_page_size = 5;

        for i in 1..=20u32 {
            h.put(i, &make_doc(vec![("nsfwLevel", FieldValue::Single(Value::Integer(1)))]));
        }

        let executor = QueryExecutor::new(
            &h.slots,
            &h.filters,
            &h.sorts,
            h.config.max_page_size,
        );
        let result = executor.execute(&[], None, 1000, None).unwrap();
        assert_eq!(result.ids.len(), 5);
        assert_eq!(result.total_matched, 20);
    }
}
