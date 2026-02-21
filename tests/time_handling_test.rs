//! S3.7: Time handling integration tests.
//!
//! Verifies deferred alive lifecycle and TimeBucketManager integration.

use std::thread;
use std::time::Duration;

use bitdex_v2::concurrent_engine::ConcurrentEngine;
use bitdex_v2::config::{Config, FilterFieldConfig, SortFieldConfig};
use bitdex_v2::filter::FilterFieldType;
use bitdex_v2::mutation::{Document, FieldValue};
use bitdex_v2::query::Value;

fn make_doc(fields: Vec<(&str, FieldValue)>) -> Document {
    Document {
        fields: fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    }
}

fn wait_for_flush(engine: &ConcurrentEngine, expected_alive: u64, max_ms: u64) {
    let deadline = std::time::Instant::now() + Duration::from_millis(max_ms);
    while std::time::Instant::now() < deadline {
        if engine.alive_count() == expected_alive {
            thread::sleep(Duration::from_millis(2));
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    // Don't assert — caller checks
}

#[test]
fn test_deferred_alive_far_future_invisible() {
    // A document with publishedAt far in the future should NOT be visible.
    let config = Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "publishedAt".to_string(),
                field_type: FilterFieldType::SingleValue,
                storage: Default::default(),
                behaviors: Some(bitdex_v2::config::FieldBehaviors {
                    deferred_alive: true,
                    range_buckets: vec![],
                }),
            },
            FilterFieldConfig {
                name: "nsfwLevel".to_string(),
                field_type: FilterFieldType::SingleValue,
                storage: Default::default(),
                behaviors: None,
            },
        ],
        sort_fields: vec![SortFieldConfig {
            name: "reactionCount".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
        }],
        max_page_size: 1000,
        flush_interval_us: 50,
        merge_interval_ms: 100,
        channel_capacity: 10_000,
        ..Default::default()
    };

    let engine = ConcurrentEngine::new(config).unwrap();

    // Insert doc with publishedAt far in the future
    engine
        .put(
            1,
            &make_doc(vec![
                (
                    "publishedAt",
                    FieldValue::Single(Value::Integer(i64::MAX)),
                ),
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                (
                    "reactionCount",
                    FieldValue::Single(Value::Integer(100)),
                ),
            ]),
        )
        .unwrap();

    // Wait for flush
    thread::sleep(Duration::from_millis(200));

    // Should NOT be visible (deferred alive not activated yet)
    assert_eq!(
        engine.alive_count(),
        0,
        "far-future doc should not be alive"
    );

    let result = engine.query(&[], None, 100).unwrap();
    assert!(
        result.ids.is_empty(),
        "far-future doc should not appear in queries"
    );
}

#[test]
fn test_deferred_alive_past_timestamp_visible() {
    // A document with publishedAt in the past should be immediately visible.
    let config = Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "publishedAt".to_string(),
                field_type: FilterFieldType::SingleValue,
                storage: Default::default(),
                behaviors: Some(bitdex_v2::config::FieldBehaviors {
                    deferred_alive: true,
                    range_buckets: vec![],
                }),
            },
            FilterFieldConfig {
                name: "nsfwLevel".to_string(),
                field_type: FilterFieldType::SingleValue,
                storage: Default::default(),
                behaviors: None,
            },
        ],
        sort_fields: vec![SortFieldConfig {
            name: "reactionCount".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
        }],
        max_page_size: 1000,
        flush_interval_us: 50,
        merge_interval_ms: 100,
        channel_capacity: 10_000,
        ..Default::default()
    };

    let engine = ConcurrentEngine::new(config).unwrap();

    // Insert doc with publishedAt in the past (timestamp 1 = long ago)
    engine
        .put(
            1,
            &make_doc(vec![
                ("publishedAt", FieldValue::Single(Value::Integer(1))),
                ("nsfwLevel", FieldValue::Single(Value::Integer(1))),
                (
                    "reactionCount",
                    FieldValue::Single(Value::Integer(100)),
                ),
            ]),
        )
        .unwrap();

    wait_for_flush(&engine, 1, 2000);
    assert_eq!(engine.alive_count(), 1, "past-timestamp doc should be alive");

    let result = engine.query(&[], None, 100).unwrap();
    assert_eq!(result.ids, vec![1], "past-timestamp doc should be visible");
}

#[test]
fn test_mixed_deferred_and_immediate() {
    // Mix of deferred and immediate documents.
    let config = Config {
        filter_fields: vec![
            FilterFieldConfig {
                name: "publishedAt".to_string(),
                field_type: FilterFieldType::SingleValue,
                storage: Default::default(),
                behaviors: Some(bitdex_v2::config::FieldBehaviors {
                    deferred_alive: true,
                    range_buckets: vec![],
                }),
            },
        ],
        sort_fields: vec![SortFieldConfig {
            name: "reactionCount".to_string(),
            source_type: "uint32".to_string(),
            encoding: "linear".to_string(),
            bits: 32,
        }],
        max_page_size: 1000,
        flush_interval_us: 50,
        merge_interval_ms: 100,
        channel_capacity: 10_000,
        ..Default::default()
    };

    let engine = ConcurrentEngine::new(config).unwrap();

    // Doc 1: past timestamp (visible immediately)
    engine
        .put(
            1,
            &make_doc(vec![
                ("publishedAt", FieldValue::Single(Value::Integer(1))),
                (
                    "reactionCount",
                    FieldValue::Single(Value::Integer(100)),
                ),
            ]),
        )
        .unwrap();

    // Doc 2: far future (invisible)
    engine
        .put(
            2,
            &make_doc(vec![
                (
                    "publishedAt",
                    FieldValue::Single(Value::Integer(i64::MAX)),
                ),
                (
                    "reactionCount",
                    FieldValue::Single(Value::Integer(200)),
                ),
            ]),
        )
        .unwrap();

    // Doc 3: past timestamp (visible immediately)
    engine
        .put(
            3,
            &make_doc(vec![
                ("publishedAt", FieldValue::Single(Value::Integer(100))),
                (
                    "reactionCount",
                    FieldValue::Single(Value::Integer(300)),
                ),
            ]),
        )
        .unwrap();

    wait_for_flush(&engine, 2, 2000);

    // Only docs 1 and 3 should be visible
    assert_eq!(engine.alive_count(), 2);
    let result = engine.query(&[], None, 100).unwrap();
    let mut ids = result.ids.clone();
    ids.sort();
    assert_eq!(ids, vec![1, 3]);
}
