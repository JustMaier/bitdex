//! Generic NDJSON loader — converts arbitrary NDJSON files to engine Documents
//! using a DataSchema definition.
//!
//! Reuses the proven pipelined loading pattern:
//!   reader thread → crossbeam channel → rayon parallel parse → put_bulk_loading

use std::collections::HashMap;
use std::fs::File;
use std::io::Read as _;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use rayon::prelude::*;

use crate::concurrent_engine::ConcurrentEngine;
use crate::config::{DataSchema, FieldMapping, FieldValueType};
use crate::mutation::{Document, FieldValue};
use crate::query::Value;

/// Statistics from a completed load operation.
#[derive(Debug, Clone)]
pub struct LoadStats {
    pub records_loaded: u64,
    pub elapsed: Duration,
    pub errors_skipped: u64,
}

/// Load an NDJSON file into an engine using the given data schema.
///
/// - `engine`: target ConcurrentEngine (must already be constructed with the right config)
/// - `schema`: field mapping rules for converting raw JSON → Documents
/// - `path`: path to the NDJSON file
/// - `limit`: optional max records to load
/// - `threads`: number of rayon threads for parallel parsing
/// - `progress`: atomic counter updated as records are loaded (for progress polling)
pub fn load_ndjson(
    engine: &ConcurrentEngine,
    schema: &DataSchema,
    path: &Path,
    limit: Option<usize>,
    threads: usize,
    progress: Arc<AtomicU64>,
) -> Result<LoadStats, String> {
    let record_limit = limit.unwrap_or(usize::MAX);
    let chunk_size: usize = if record_limit < 5_000_000 { record_limit } else { 5_000_000 };
    let read_batch_size: usize = 500_000;
    let target_batch_bytes = read_batch_size * 600;

    let mut staging = engine.clone_staging();

    // Build indexed field set for stripping doc-only fields from bitmap accumulator
    let indexed_fields = engine.indexed_field_names();

    let data_path_owned = path.to_owned();
    let (block_tx, block_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(2);

    let reader_handle = thread::spawn(move || {
        let file = File::open(&data_path_owned).expect("Failed to open data file");
        let mut reader = std::io::BufReader::with_capacity(16 * 1024 * 1024, file);
        let mut buf = vec![0u8; 4 * 1024 * 1024];
        let mut accum = Vec::<u8>::with_capacity(target_batch_bytes + 4 * 1024 * 1024);

        loop {
            let bytes_read = reader.read(&mut buf).unwrap_or(0);
            if bytes_read == 0 {
                if !accum.is_empty() {
                    let _ = block_tx.send(accum);
                }
                break;
            }
            accum.extend_from_slice(&buf[..bytes_read]);

            if accum.len() >= target_batch_bytes {
                if let Some(last_nl) = memrchr_newline(&accum) {
                    let remainder = accum[last_nl + 1..].to_vec();
                    accum.truncate(last_nl + 1);
                    let batch = std::mem::replace(
                        &mut accum,
                        Vec::with_capacity(target_batch_bytes + 4 * 1024 * 1024),
                    );
                    accum = remainder;
                    if block_tx.send(batch).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let schema_ref = schema.clone();
    let mut doc_chunk: Vec<(u32, Document)> = Vec::with_capacity(chunk_size);
    let mut id_counter: u32 = 0;
    let mut total_inserted: usize = 0;
    let mut total_errors: u64 = 0;
    let mut chunks_processed: usize = 0;
    let wall_start = Instant::now();
    let mut ds_handles: Vec<thread::JoinHandle<()>> = Vec::new();

    while let Ok(raw_block) = block_rx.recv() {
        if total_inserted >= record_limit {
            break;
        }

        let block_str = std::str::from_utf8(&raw_block).map_err(|e| format!("NDJSON not valid UTF-8: {e}"))?;
        let base_id = id_counter;

        let lines: Vec<&str> = block_str
            .split('\n')
            .map(|l| l.trim_end_matches('\r'))
            .filter(|l| !l.is_empty())
            .collect();
        let line_count = lines.len() as u32;

        let results: Vec<Option<(u32, Document)>> = lines
            .into_par_iter()
            .enumerate()
            .map(|(i, line)| {
                let id = base_id + i as u32;
                match serde_json::from_str::<serde_json::Value>(line) {
                    Ok(json) => {
                        let doc = json_to_document(&json, &schema_ref);
                        Some((id, doc))
                    }
                    Err(_) => None,
                }
            })
            .collect();

        id_counter += line_count;

        let mut parsed: Vec<(u32, Document)> = Vec::with_capacity(results.len());
        for r in results {
            if let Some(pair) = r {
                parsed.push(pair);
            } else {
                total_errors += 1;
            }
        }

        // Respect limit
        if total_inserted + parsed.len() > record_limit {
            parsed.truncate(record_limit - total_inserted);
        }

        // Strip doc-only fields for bitmap accumulator
        let stripped: Vec<(u32, Document)> = parsed
            .iter()
            .map(|(id, doc)| {
                let fields = doc
                    .fields
                    .iter()
                    .filter(|(k, _)| indexed_fields.contains(k.as_str()))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                (*id, Document { fields })
            })
            .collect();

        // Move full docs to background docstore writer
        if let Some(h) = ds_handles.pop() {
            h.join().unwrap();
        }
        ds_handles.push(engine.spawn_docstore_writer(parsed));

        doc_chunk.extend(stripped);

        if doc_chunk.len() >= chunk_size {
            let count = engine.put_bulk_loading(&mut staging, &doc_chunk, threads);
            total_inserted += count;
            progress.store(total_inserted as u64, Ordering::Release);
            chunks_processed += 1;
            let elapsed = wall_start.elapsed();
            let rate = total_inserted as f64 / elapsed.as_secs_f64();
            eprintln!(
                "  chunk {}: {} total ({:.0}/s)",
                chunks_processed, total_inserted, rate
            );
            doc_chunk = Vec::with_capacity(chunk_size);
        }
    }

    if !doc_chunk.is_empty() {
        let count = engine.put_bulk_loading(&mut staging, &doc_chunk, threads);
        total_inserted += count;
        progress.store(total_inserted as u64, Ordering::Release);
        chunks_processed += 1;
        let rate = total_inserted as f64 / wall_start.elapsed().as_secs_f64();
        eprintln!(
            "  chunk {}: {} total ({:.0}/s)",
            chunks_processed, total_inserted, rate
        );
    }

    // Drop the receiver so the reader thread stops
    drop(block_rx);
    reader_handle.join().unwrap();

    // Wait for all outstanding docstore writes
    for h in ds_handles {
        h.join().unwrap();
    }

    // Publish staging snapshot
    engine.publish_staging(staging);

    let elapsed = wall_start.elapsed();
    let rate = total_inserted as f64 / elapsed.as_secs_f64();
    eprintln!(
        "Loaded {} records in {:.1}s ({:.0}/s), errors skipped: {}",
        total_inserted,
        elapsed.as_secs_f64(),
        rate,
        total_errors
    );

    Ok(LoadStats {
        records_loaded: total_inserted as u64,
        elapsed,
        errors_skipped: total_errors,
    })
}

/// Convert a raw JSON value to a Document using the DataSchema field mappings.
fn json_to_document(json: &serde_json::Value, schema: &DataSchema) -> Document {
    let mut fields = HashMap::new();

    // Always include the ID field
    if let Some(id_val) = json.get(&schema.id_field) {
        if let Some(n) = id_val.as_i64() {
            fields.insert("id".to_string(), FieldValue::Single(Value::Integer(n)));
        } else if let Some(n) = id_val.as_u64() {
            fields.insert("id".to_string(), FieldValue::Single(Value::Integer(n as i64)));
        }
    }

    for mapping in &schema.fields {
        // Try primary source, then fallback
        let raw = json
            .get(&mapping.source)
            .or_else(|| mapping.fallback.as_ref().and_then(|fb| json.get(fb)));

        let raw = match raw {
            Some(v) if !v.is_null() => v,
            _ => {
                // For ExistsBoolean, missing/null means false
                match mapping.value_type {
                    FieldValueType::ExistsBoolean => {
                        fields.insert(
                            mapping.target.clone(),
                            FieldValue::Single(Value::Bool(false)),
                        );
                    }
                    _ => {}
                }
                continue;
            }
        };

        if let Some(fv) = convert_field(raw, mapping) {
            fields.insert(mapping.target.clone(), fv);
        }
    }

    Document { fields }
}

/// Convert a raw JSON value to a FieldValue based on the mapping rules.
fn convert_field(raw: &serde_json::Value, mapping: &FieldMapping) -> Option<FieldValue> {
    match mapping.value_type {
        FieldValueType::Integer => {
            let n = if let Some(n) = raw.as_i64() {
                n
            } else if let Some(n) = raw.as_u64() {
                n as i64
            } else if let Some(n) = raw.as_f64() {
                n as i64
            } else {
                return None;
            };
            let n = if mapping.truncate_u32 { (n as u32) as i64 } else { n };
            Some(FieldValue::Single(Value::Integer(n)))
        }
        FieldValueType::Boolean => {
            let b = raw.as_bool()?;
            Some(FieldValue::Single(Value::Bool(b)))
        }
        FieldValueType::String => {
            let s = raw.as_str()?;
            Some(FieldValue::Single(Value::String(s.to_string())))
        }
        FieldValueType::MappedString => {
            let s = raw.as_str()?;
            let map = mapping.string_map.as_ref()?;
            let n = map.get(s).copied().unwrap_or(0);
            Some(FieldValue::Single(Value::Integer(n)))
        }
        FieldValueType::IntegerArray => {
            let arr = raw.as_array()?;
            if arr.is_empty() {
                return None; // skip empty arrays (same as current behavior)
            }
            let values: Vec<Value> = arr
                .iter()
                .filter_map(|v| {
                    v.as_i64()
                        .or_else(|| v.as_u64().map(|n| n as i64))
                        .map(Value::Integer)
                })
                .collect();
            if values.is_empty() {
                None
            } else {
                Some(FieldValue::Multi(values))
            }
        }
        FieldValueType::ExistsBoolean => {
            // True if field exists and is non-null (raw already checked for null)
            Some(FieldValue::Single(Value::Bool(true)))
        }
    }
}

fn memrchr_newline(data: &[u8]) -> Option<usize> {
    data.iter().rposition(|&b| b == b'\n')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_to_document_integer() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "count".into(),
                target: "count".into(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 42, "count": 100});
        let doc = json_to_document(&json, &schema);
        assert_eq!(doc.fields.get("id"), Some(&FieldValue::Single(Value::Integer(42))));
        assert_eq!(doc.fields.get("count"), Some(&FieldValue::Single(Value::Integer(100))));
    }

    #[test]
    fn test_json_to_document_fallback() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "primary".into(),
                target: "val".into(),
                value_type: FieldValueType::Integer,
                fallback: Some("secondary".into()),
                string_map: None,
                doc_only: false,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 1, "secondary": 99});
        let doc = json_to_document(&json, &schema);
        assert_eq!(doc.fields.get("val"), Some(&FieldValue::Single(Value::Integer(99))));
    }

    #[test]
    fn test_json_to_document_mapped_string() {
        let mut map = HashMap::new();
        map.insert("image".into(), 1);
        map.insert("video".into(), 2);

        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "type".into(),
                target: "type".into(),
                value_type: FieldValueType::MappedString,
                fallback: None,
                string_map: Some(map),
                doc_only: false,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 1, "type": "image"});
        let doc = json_to_document(&json, &schema);
        assert_eq!(doc.fields.get("type"), Some(&FieldValue::Single(Value::Integer(1))));
    }

    #[test]
    fn test_json_to_document_boolean() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "hasMeta".into(),
                target: "hasMeta".into(),
                value_type: FieldValueType::Boolean,
                fallback: None,
                string_map: None,
                doc_only: false,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 1, "hasMeta": true});
        let doc = json_to_document(&json, &schema);
        assert_eq!(doc.fields.get("hasMeta"), Some(&FieldValue::Single(Value::Bool(true))));
    }

    #[test]
    fn test_json_to_document_integer_array() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "tagIds".into(),
                target: "tagIds".into(),
                value_type: FieldValueType::IntegerArray,
                fallback: None,
                string_map: None,
                doc_only: false,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 1, "tagIds": [10, 20, 30]});
        let doc = json_to_document(&json, &schema);
        assert_eq!(
            doc.fields.get("tagIds"),
            Some(&FieldValue::Multi(vec![
                Value::Integer(10),
                Value::Integer(20),
                Value::Integer(30),
            ]))
        );
    }

    #[test]
    fn test_json_to_document_truncate_u32() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "ts".into(),
                target: "ts".into(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                truncate_u32: true,
            }],
        };
        let big_val: i64 = 5_000_000_000;
        let json: serde_json::Value = serde_json::json!({"id": 1, "ts": big_val});
        let doc = json_to_document(&json, &schema);
        let expected = (big_val as u32) as i64;
        assert_eq!(doc.fields.get("ts"), Some(&FieldValue::Single(Value::Integer(expected))));
    }

    #[test]
    fn test_json_to_document_string() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "url".into(),
                target: "url".into(),
                value_type: FieldValueType::String,
                fallback: None,
                string_map: None,
                doc_only: true,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 1, "url": "http://example.com"});
        let doc = json_to_document(&json, &schema);
        assert_eq!(
            doc.fields.get("url"),
            Some(&FieldValue::Single(Value::String("http://example.com".into())))
        );
    }

    #[test]
    fn test_json_to_document_missing_field_skipped() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "missing".into(),
                target: "val".into(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 1});
        let doc = json_to_document(&json, &schema);
        assert!(doc.fields.get("val").is_none());
    }

    #[test]
    fn test_json_to_document_null_field_skipped() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "val".into(),
                target: "val".into(),
                value_type: FieldValueType::Integer,
                fallback: None,
                string_map: None,
                doc_only: false,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 1, "val": null});
        let doc = json_to_document(&json, &schema);
        assert!(doc.fields.get("val").is_none());
    }

    #[test]
    fn test_json_to_document_empty_array_skipped() {
        let schema = DataSchema {
            id_field: "id".into(),
            fields: vec![FieldMapping {
                source: "tags".into(),
                target: "tags".into(),
                value_type: FieldValueType::IntegerArray,
                fallback: None,
                string_map: None,
                doc_only: false,
                truncate_u32: false,
            }],
        };
        let json: serde_json::Value = serde_json::json!({"id": 1, "tags": []});
        let doc = json_to_document(&json, &schema);
        assert!(doc.fields.get("tags").is_none());
    }
}
