use std::collections::HashMap;
use std::path::Path;

use redb::{Database, TableDefinition};

use crate::error::{BitdexError, Result};
use crate::mutation::FieldValue;
use crate::query::Value;

const DOCS_TABLE: TableDefinition<u32, &[u8]> = TableDefinition::new("docs");
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

/// A stored document containing all field values needed for bitmap diffing on upsert.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredDoc {
    pub fields: HashMap<String, FieldValue>,
}

/// Compact binary format for document storage.
///
/// Instead of serializing full field name strings per document, we maintain a
/// field dictionary (name → u16 index) stored once in a `meta` table. Each
/// document is then serialized as a msgpack array of (field_index, value) pairs,
/// compressed with lz4.
///
/// This reduces per-record overhead from ~375 bytes (field name strings) to
/// ~50 bytes (u16 indices), plus lz4 compresses the remaining payload.
pub struct DocStore {
    db: Database,
    /// field name → compact index (0-based)
    field_to_idx: HashMap<String, u16>,
    /// compact index → field name
    idx_to_field: Vec<String>,
}

impl DocStore {
    /// Open a docstore at the given path. Creates the file if it doesn't exist.
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::create(path).map_err(|e| BitdexError::DocStore(e.to_string()))?;
        // Ensure tables exist
        let write_txn = db
            .begin_write()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        {
            let _table = write_txn
                .open_table(DOCS_TABLE)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            let _meta = write_txn
                .open_table(META_TABLE)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        }
        write_txn
            .commit()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;

        // Load field dictionary from meta table
        let (field_to_idx, idx_to_field) = Self::load_field_dict(&db)?;

        Ok(Self {
            db,
            field_to_idx,
            idx_to_field,
        })
    }

    /// Open a docstore using an in-memory backend (for testing).
    pub fn open_temp() -> Result<Self> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        let write_txn = db
            .begin_write()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        {
            let _table = write_txn
                .open_table(DOCS_TABLE)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            let _meta = write_txn
                .open_table(META_TABLE)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        }
        write_txn
            .commit()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        Ok(Self {
            db,
            field_to_idx: HashMap::new(),
            idx_to_field: Vec::new(),
        })
    }

    /// Load field dictionary from the meta table.
    fn load_field_dict(db: &Database) -> Result<(HashMap<String, u16>, Vec<String>)> {
        let read_txn = db
            .begin_read()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        let meta = read_txn
            .open_table(META_TABLE)
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;

        match meta
            .get("field_dict")
            .map_err(|e| BitdexError::DocStore(e.to_string()))?
        {
            Some(data) => {
                let names: Vec<String> = rmp_serde::from_slice(data.value())
                    .map_err(|e| BitdexError::DocStore(format!("field dict decode: {e}")))?;
                let map: HashMap<String, u16> = names
                    .iter()
                    .enumerate()
                    .map(|(i, n)| (n.clone(), i as u16))
                    .collect();
                Ok((map, names))
            }
            None => Ok((HashMap::new(), Vec::new())),
        }
    }

    /// Save field dictionary to the meta table.
    fn save_field_dict(&self) -> Result<()> {
        let bytes = rmp_serde::to_vec(&self.idx_to_field)
            .map_err(|e| BitdexError::DocStore(format!("field dict encode: {e}")))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        {
            let mut meta = write_txn
                .open_table(META_TABLE)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            meta.insert("field_dict", bytes.as_slice())
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        }
        write_txn
            .commit()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        Ok(())
    }

    /// Get or assign a field index. Returns true if the dictionary grew.
    fn ensure_field_idx(&mut self, name: &str) -> u16 {
        if let Some(&idx) = self.field_to_idx.get(name) {
            return idx;
        }
        let idx = self.idx_to_field.len() as u16;
        self.idx_to_field.push(name.to_string());
        self.field_to_idx.insert(name.to_string(), idx);
        idx
    }

    /// Serialize a document to compact binary format:
    /// msgpack array of (u16, value) pairs, then lz4-compressed.
    fn encode_doc(&mut self, doc: &StoredDoc) -> Result<Vec<u8>> {
        let mut dict_changed = false;

        // Build (field_idx, packed_value) pairs
        let mut pairs: Vec<(u16, PackedValue)> = Vec::with_capacity(doc.fields.len());
        for (name, fv) in &doc.fields {
            let old_len = self.idx_to_field.len();
            let idx = self.ensure_field_idx(name);
            if self.idx_to_field.len() > old_len {
                dict_changed = true;
            }
            pairs.push((idx, pack_field_value(fv)));
        }

        // Save dict if it grew
        if dict_changed {
            self.save_field_dict()?;
        }

        // Msgpack encode
        let raw = rmp_serde::to_vec(&pairs)
            .map_err(|e| BitdexError::DocStore(format!("msgpack encode: {e}")))?;

        // Lz4 compress
        let compressed = lz4_flex::compress_prepend_size(&raw);
        Ok(compressed)
    }

    /// Encode a document using the current dictionary without mutating it.
    /// Fields not in the dictionary are skipped (should not happen in practice
    /// since encode_doc_batch ensures all fields are registered first).
    fn encode_doc_readonly(&self, doc: &StoredDoc) -> Result<Vec<u8>> {
        let mut pairs: Vec<(u16, PackedValue)> = Vec::with_capacity(doc.fields.len());
        for (name, fv) in &doc.fields {
            if let Some(&idx) = self.field_to_idx.get(name.as_str()) {
                pairs.push((idx, pack_field_value(fv)));
            }
        }
        let raw = rmp_serde::to_vec(&pairs)
            .map_err(|e| BitdexError::DocStore(format!("msgpack encode: {e}")))?;
        let compressed = lz4_flex::compress_prepend_size(&raw);
        Ok(compressed)
    }

    /// Decode a document from compact binary format.
    fn decode_doc(&self, data: &[u8]) -> Result<StoredDoc> {
        // Lz4 decompress
        let raw = lz4_flex::decompress_size_prepended(data)
            .map_err(|e| BitdexError::DocStore(format!("lz4 decompress: {e}")))?;

        // Msgpack decode
        let pairs: Vec<(u16, PackedValue)> = rmp_serde::from_slice(&raw)
            .map_err(|e| BitdexError::DocStore(format!("msgpack decode: {e}")))?;

        // Reconstruct HashMap
        let mut fields = HashMap::with_capacity(pairs.len());
        for (idx, pv) in pairs {
            if let Some(name) = self.idx_to_field.get(idx as usize) {
                fields.insert(name.clone(), unpack_field_value(pv));
            }
        }

        Ok(StoredDoc { fields })
    }

    /// Get a stored document by slot ID.
    pub fn get(&self, id: u32) -> Result<Option<StoredDoc>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        let table = read_txn
            .open_table(DOCS_TABLE)
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        match table
            .get(id)
            .map_err(|e| BitdexError::DocStore(e.to_string()))?
        {
            Some(data) => {
                let doc = self.decode_doc(data.value())?;
                Ok(Some(doc))
            }
            None => Ok(None),
        }
    }

    /// Store a document at the given slot ID.
    pub fn put(&mut self, id: u32, doc: &StoredDoc) -> Result<()> {
        let bytes = self.encode_doc(doc)?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        {
            let mut table = write_txn
                .open_table(DOCS_TABLE)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            table
                .insert(id, bytes.as_slice())
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        }
        write_txn
            .commit()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        Ok(())
    }

    /// Store multiple documents in a single write transaction.
    /// Much faster than calling put() in a loop since redb serializes write transactions.
    pub fn put_batch(&mut self, docs: &[(u32, StoredDoc)]) -> Result<()> {
        if docs.is_empty() {
            return Ok(());
        }

        // First pass: ensure all field names are in the dictionary
        let mut dict_changed = false;
        for (_, doc) in docs {
            for name in doc.fields.keys() {
                let old_len = self.idx_to_field.len();
                self.ensure_field_idx(name);
                if self.idx_to_field.len() > old_len {
                    dict_changed = true;
                }
            }
        }
        if dict_changed {
            self.save_field_dict()?;
        }

        // Second pass: encode and write
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        {
            let mut table = write_txn
                .open_table(DOCS_TABLE)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            for (id, doc) in docs {
                let bytes = self.encode_doc_readonly(doc)?;
                table
                    .insert(*id, bytes.as_slice())
                    .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        Ok(())
    }

    /// Compact the database, reclaiming space from old transactions.
    /// Should be called after large bulk writes to reduce file size.
    pub fn compact(&mut self) -> Result<bool> {
        self.db
            .compact()
            .map_err(|e| BitdexError::DocStore(format!("compaction failed: {e}")))
    }

    /// Delete a document by slot ID.
    pub fn delete(&self, id: u32) -> Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        {
            let mut table = write_txn
                .open_table(DOCS_TABLE)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            table
                .remove(id)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        }
        write_txn
            .commit()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Compact value encoding — avoids serde enum tags + string field names
// ---------------------------------------------------------------------------

/// Compact packed representation for storage. Uses msgpack-friendly types
/// to minimize per-value overhead vs bincode's enum discriminants.
#[derive(serde::Serialize, serde::Deserialize)]
enum PackedValue {
    /// i64 value
    I(i64),
    /// f64 value
    F(f64),
    /// bool value
    B(bool),
    /// String value
    S(String),
    /// Multi-value: Vec<i64> (most multi-value fields are integer arrays)
    Mi(Vec<i64>),
    /// Multi-value: mixed (rare — fallback)
    Mm(Vec<PackedValue>),
}

fn pack_field_value(fv: &FieldValue) -> PackedValue {
    match fv {
        FieldValue::Single(v) => pack_value(v),
        FieldValue::Multi(vs) => {
            // Optimize: if all values are integers, use compact Mi variant
            if vs.iter().all(|v| matches!(v, Value::Integer(_))) {
                PackedValue::Mi(
                    vs.iter()
                        .map(|v| match v {
                            Value::Integer(i) => *i,
                            _ => unreachable!(),
                        })
                        .collect(),
                )
            } else {
                PackedValue::Mm(vs.iter().map(pack_value).collect())
            }
        }
    }
}

fn pack_value(v: &Value) -> PackedValue {
    match v {
        Value::Integer(i) => PackedValue::I(*i),
        Value::Float(f) => PackedValue::F(*f),
        Value::Bool(b) => PackedValue::B(*b),
        Value::String(s) => PackedValue::S(s.clone()),
    }
}

fn unpack_field_value(pv: PackedValue) -> FieldValue {
    match pv {
        PackedValue::I(i) => FieldValue::Single(Value::Integer(i)),
        PackedValue::F(f) => FieldValue::Single(Value::Float(f)),
        PackedValue::B(b) => FieldValue::Single(Value::Bool(b)),
        PackedValue::S(s) => FieldValue::Single(Value::String(s)),
        PackedValue::Mi(is) => {
            FieldValue::Multi(is.into_iter().map(Value::Integer).collect())
        }
        PackedValue::Mm(pvs) => {
            FieldValue::Multi(pvs.into_iter().map(|pv| match unpack_field_value(pv) {
                FieldValue::Single(v) => v,
                _ => Value::Integer(0), // shouldn't happen
            }).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let mut store = DocStore::open_temp().unwrap();
        let doc = StoredDoc {
            fields: vec![
                ("nsfwLevel".to_string(), FieldValue::Single(Value::Integer(1))),
                (
                    "tagIds".to_string(),
                    FieldValue::Multi(vec![Value::Integer(100), Value::Integer(200)]),
                ),
            ]
            .into_iter()
            .collect(),
        };
        store.put(42, &doc).unwrap();
        let got = store.get(42).unwrap().unwrap();
        assert_eq!(got.fields.len(), 2);
        match &got.fields["nsfwLevel"] {
            FieldValue::Single(Value::Integer(1)) => {}
            other => panic!("unexpected: {:?}", other),
        }
        match &got.fields["tagIds"] {
            FieldValue::Multi(vs) => {
                assert_eq!(vs.len(), 2);
                assert_eq!(vs[0], Value::Integer(100));
                assert_eq!(vs[1], Value::Integer(200));
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn test_get_missing() {
        let store = DocStore::open_temp().unwrap();
        assert!(store.get(999).unwrap().is_none());
    }

    #[test]
    fn test_delete() {
        let mut store = DocStore::open_temp().unwrap();
        let doc = StoredDoc {
            fields: vec![("x".to_string(), FieldValue::Single(Value::Integer(1)))]
                .into_iter()
                .collect(),
        };
        store.put(1, &doc).unwrap();
        store.delete(1).unwrap();
        assert!(store.get(1).unwrap().is_none());
    }

    #[test]
    fn test_overwrite() {
        let mut store = DocStore::open_temp().unwrap();
        let doc1 = StoredDoc {
            fields: vec![("x".to_string(), FieldValue::Single(Value::Integer(1)))]
                .into_iter()
                .collect(),
        };
        store.put(1, &doc1).unwrap();
        let doc2 = StoredDoc {
            fields: vec![("x".to_string(), FieldValue::Single(Value::Integer(2)))]
                .into_iter()
                .collect(),
        };
        store.put(1, &doc2).unwrap();
        let got = store.get(1).unwrap().unwrap();
        match &got.fields["x"] {
            FieldValue::Single(Value::Integer(2)) => {}
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn test_batch_put() {
        let mut store = DocStore::open_temp().unwrap();
        let docs: Vec<(u32, StoredDoc)> = (0..100)
            .map(|i| {
                let doc = StoredDoc {
                    fields: vec![
                        ("id".to_string(), FieldValue::Single(Value::Integer(i as i64))),
                        ("url".to_string(), FieldValue::Single(Value::String(format!("guid-{}", i)))),
                        (
                            "tagIds".to_string(),
                            FieldValue::Multi(vec![Value::Integer(i as i64 * 10), Value::Integer(i as i64 * 10 + 1)]),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                };
                (i, doc)
            })
            .collect();
        store.put_batch(&docs).unwrap();

        // Verify field dictionary has 3 entries
        assert_eq!(store.idx_to_field.len(), 3);

        // Verify documents
        let got = store.get(50).unwrap().unwrap();
        assert_eq!(got.fields.len(), 3);
        match &got.fields["id"] {
            FieldValue::Single(Value::Integer(50)) => {}
            other => panic!("unexpected: {:?}", other),
        }
        match &got.fields["url"] {
            FieldValue::Single(Value::String(s)) => assert_eq!(s, "guid-50"),
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn test_string_and_bool_fields() {
        let mut store = DocStore::open_temp().unwrap();
        let doc = StoredDoc {
            fields: vec![
                ("url".to_string(), FieldValue::Single(Value::String("abc-123".to_string()))),
                ("hasMeta".to_string(), FieldValue::Single(Value::Bool(true))),
                ("score".to_string(), FieldValue::Single(Value::Float(3.14))),
            ]
            .into_iter()
            .collect(),
        };
        store.put(1, &doc).unwrap();
        let got = store.get(1).unwrap().unwrap();
        match &got.fields["url"] {
            FieldValue::Single(Value::String(s)) => assert_eq!(s, "abc-123"),
            other => panic!("unexpected: {:?}", other),
        }
        match &got.fields["hasMeta"] {
            FieldValue::Single(Value::Bool(true)) => {}
            other => panic!("unexpected: {:?}", other),
        }
        match &got.fields["score"] {
            FieldValue::Single(Value::Float(f)) => assert!((f - 3.14).abs() < 0.001),
            other => panic!("unexpected: {:?}", other),
        }
    }
}
