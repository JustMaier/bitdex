use std::collections::HashMap;
use std::path::Path;

use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use roaring::RoaringBitmap;

use crate::error::{BitdexError, Result};

/// Table: composite key "field_name:value_u64" -> serialized bitmap bytes
const TABLE_BITMAPS: TableDefinition<&str, &[u8]> = TableDefinition::new("bitmaps");

/// Table: sort layer key "__sort:field:layer_idx" -> serialized bitmap bytes
const TABLE_SORT_LAYERS: TableDefinition<&str, &[u8]> = TableDefinition::new("sort_layers");

/// Table: metadata key -> value bytes
/// Keys: "__alive" -> serialized alive bitmap, "__slot_counter" -> u32 LE bytes
const TABLE_META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

/// Persistent storage for filter bitmaps backed by redb.
///
/// Stores serialized RoaringBitmaps keyed by (field_name, value).
/// Used by the merge thread to write compacted bases and by startup
/// to load Tier 1 bitmaps.
pub struct BitmapStore {
    db: Database,
}

impl BitmapStore {
    /// Open a bitmap store at the given path. Creates the file if it doesn't exist.
    pub fn new(path: &Path) -> Result<Self> {
        let db = Database::create(path).map_err(|e| BitdexError::DocStore(e.to_string()))?;
        let write_txn = db
            .begin_write()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        {
            let _t1 = write_txn
                .open_table(TABLE_BITMAPS)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            let _t2 = write_txn
                .open_table(TABLE_SORT_LAYERS)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            let _t3 = write_txn
                .open_table(TABLE_META)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        }
        write_txn
            .commit()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        Ok(Self { db })
    }

    /// Open a bitmap store using an in-memory backend (for testing).
    pub fn open_temp() -> Result<Self> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        let write_txn = db
            .begin_write()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        {
            let _t1 = write_txn
                .open_table(TABLE_BITMAPS)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            let _t2 = write_txn
                .open_table(TABLE_SORT_LAYERS)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            let _t3 = write_txn
                .open_table(TABLE_META)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        }
        write_txn
            .commit()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        Ok(Self { db })
    }

    /// Build the composite key for a (field, value) pair.
    /// Format: "field_name:value" (e.g., "tagIds:42")
    fn make_key(field: &str, value: u64) -> String {
        format!("{}:{}", field, value)
    }

    /// Load a single bitmap by field name and value.
    ///
    /// Returns an empty bitmap if the key doesn't exist in the store.
    pub fn load_single(&self, field: &str, value: u64) -> Result<RoaringBitmap> {
        let key = Self::make_key(field, value);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        let table = read_txn
            .open_table(TABLE_BITMAPS)
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        match table.get(key.as_str()) {
            Ok(Some(data)) => {
                let bytes = data.value();
                RoaringBitmap::deserialize_from(bytes)
                    .map_err(|e| BitdexError::DocStore(format!("bitmap deserialize: {e}")))
            }
            Ok(None) => Ok(RoaringBitmap::new()),
            Err(e) => Err(BitdexError::DocStore(e.to_string())),
        }
    }

    /// Load all bitmaps for a single field.
    ///
    /// Scans all entries with keys starting with "field_name:" and returns
    /// them as a map from value to deserialized bitmap.
    pub fn load_field(&self, field_name: &str) -> Result<HashMap<u64, RoaringBitmap>> {
        let prefix = format!("{}:", field_name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        let table = read_txn
            .open_table(TABLE_BITMAPS)
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;

        let mut result = HashMap::new();
        let iter = table
            .iter()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;

        for entry in iter {
            let entry = entry.map_err(|e| BitdexError::DocStore(e.to_string()))?;
            let key = entry.0.value();
            if let Some(suffix) = key.strip_prefix(&prefix) {
                let value: u64 = suffix.parse().map_err(|e: std::num::ParseIntError| {
                    BitdexError::DocStore(format!("invalid bitmap key '{}': {}", key, e))
                })?;
                let bitmap = RoaringBitmap::deserialize_from(entry.1.value())
                    .map_err(|e| BitdexError::DocStore(e.to_string()))?;
                result.insert(value, bitmap);
            }
        }
        Ok(result)
    }

    /// Write multiple bitmap entries in a single transaction.
    ///
    /// Each entry is (field_name, value, bitmap). All writes are committed
    /// atomically.
    pub fn write_batch(&self, entries: &[(&str, u64, &RoaringBitmap)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        {
            let mut table = write_txn
                .open_table(TABLE_BITMAPS)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            for &(field, value, bitmap) in entries {
                let key = Self::make_key(field, value);
                let mut buf = Vec::with_capacity(bitmap.serialized_size());
                bitmap
                    .serialize_into(&mut buf)
                    .map_err(|e| BitdexError::DocStore(e.to_string()))?;
                table
                    .insert(key.as_str(), buf.as_slice())
                    .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        Ok(())
    }

    /// Delete a single bitmap entry.
    pub fn delete_field_value(&self, field: &str, value: u64) -> Result<()> {
        let key = Self::make_key(field, value);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        {
            let mut table = write_txn
                .open_table(TABLE_BITMAPS)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            table
                .remove(key.as_str())
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        }
        write_txn
            .commit()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        Ok(())
    }

    /// Load multiple fields at once in a single read transaction.
    ///
    /// Returns a map from field name to (value -> bitmap) map.
    pub fn load_all_fields(
        &self,
        field_names: &[&str],
    ) -> Result<HashMap<String, HashMap<u64, RoaringBitmap>>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        let table = read_txn
            .open_table(TABLE_BITMAPS)
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;

        // Build prefix set for matching
        let prefixes: Vec<String> = field_names.iter().map(|f| format!("{}:", f)).collect();
        let mut result: HashMap<String, HashMap<u64, RoaringBitmap>> = field_names
            .iter()
            .map(|f| (f.to_string(), HashMap::new()))
            .collect();

        let iter = table
            .iter()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;

        for entry in iter {
            let entry = entry.map_err(|e| BitdexError::DocStore(e.to_string()))?;
            let key = entry.0.value();
            for prefix in &prefixes {
                if let Some(suffix) = key.strip_prefix(prefix.as_str()) {
                    let field_name = &prefix[..prefix.len() - 1]; // strip trailing ':'
                    let value: u64 =
                        suffix.parse().map_err(|e: std::num::ParseIntError| {
                            BitdexError::DocStore(format!("invalid bitmap key '{}': {}", key, e))
                        })?;
                    let bitmap = RoaringBitmap::deserialize_from(entry.1.value())
                        .map_err(|e| BitdexError::DocStore(e.to_string()))?;
                    result.get_mut(field_name).unwrap().insert(value, bitmap);
                    break;
                }
            }
        }
        Ok(result)
    }

    // ---- Alive bitmap persistence ----

    /// Write the alive bitmap to the meta table.
    pub fn write_alive(&self, bitmap: &RoaringBitmap) -> Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        {
            let mut table = write_txn
                .open_table(TABLE_META)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            let mut buf = Vec::with_capacity(bitmap.serialized_size());
            bitmap
                .serialize_into(&mut buf)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            table
                .insert("__alive", buf.as_slice())
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        }
        write_txn
            .commit()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        Ok(())
    }

    /// Load the alive bitmap from the meta table.
    pub fn load_alive(&self) -> Result<Option<RoaringBitmap>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        let table = read_txn
            .open_table(TABLE_META)
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        match table.get("__alive") {
            Ok(Some(data)) => {
                let bitmap = RoaringBitmap::deserialize_from(data.value())
                    .map_err(|e| BitdexError::DocStore(format!("alive deserialize: {e}")))?;
                Ok(Some(bitmap))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(BitdexError::DocStore(e.to_string())),
        }
    }

    // ---- Sort layer persistence ----

    /// Write all sort layers for a field in a single transaction.
    pub fn write_sort_layers(&self, field: &str, layers: &[&RoaringBitmap]) -> Result<()> {
        if layers.is_empty() {
            return Ok(());
        }
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        {
            let mut table = write_txn
                .open_table(TABLE_SORT_LAYERS)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            for (i, bm) in layers.iter().enumerate() {
                let key = format!("{}:{}", field, i);
                let mut buf = Vec::with_capacity(bm.serialized_size());
                bm.serialize_into(&mut buf)
                    .map_err(|e| BitdexError::DocStore(e.to_string()))?;
                table
                    .insert(key.as_str(), buf.as_slice())
                    .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        Ok(())
    }

    /// Load sort layers for a field. Returns None if the field has no persisted layers.
    pub fn load_sort_layers(
        &self,
        field: &str,
        num_layers: usize,
    ) -> Result<Option<Vec<RoaringBitmap>>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        let table = read_txn
            .open_table(TABLE_SORT_LAYERS)
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;

        let mut layers = Vec::with_capacity(num_layers);
        let mut found_any = false;

        for i in 0..num_layers {
            let key = format!("{}:{}", field, i);
            match table.get(key.as_str()) {
                Ok(Some(data)) => {
                    found_any = true;
                    let bitmap = RoaringBitmap::deserialize_from(data.value()).map_err(|e| {
                        BitdexError::DocStore(format!("sort layer deserialize: {e}"))
                    })?;
                    layers.push(bitmap);
                }
                Ok(None) => {
                    layers.push(RoaringBitmap::new());
                }
                Err(e) => return Err(BitdexError::DocStore(e.to_string())),
            }
        }

        if found_any {
            Ok(Some(layers))
        } else {
            Ok(None)
        }
    }

    // ---- Slot counter persistence ----

    /// Write the slot counter (high-water mark) to the meta table.
    pub fn write_slot_counter(&self, counter: u32) -> Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        {
            let mut table = write_txn
                .open_table(TABLE_META)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            table
                .insert("__slot_counter", counter.to_le_bytes().as_slice())
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        }
        write_txn
            .commit()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        Ok(())
    }

    /// Load the slot counter from the meta table.
    pub fn load_slot_counter(&self) -> Result<Option<u32>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        let table = read_txn
            .open_table(TABLE_META)
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        match table.get("__slot_counter") {
            Ok(Some(data)) => {
                let bytes = data.value();
                if bytes.len() >= 4 {
                    let counter = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    Ok(Some(counter))
                } else {
                    Err(BitdexError::DocStore(
                        "slot counter has invalid length".to_string(),
                    ))
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(BitdexError::DocStore(e.to_string())),
        }
    }

    // ---- Batch persistence (all state in one transaction) ----

    /// Write all engine state atomically: filter bitmaps, alive, sort layers, slot counter.
    /// This is used by the merge thread to persist the entire snapshot.
    pub fn write_full_snapshot(
        &self,
        filter_entries: &[(&str, u64, &RoaringBitmap)],
        alive: &RoaringBitmap,
        sort_layers: &[(&str, &[&RoaringBitmap])],
        slot_counter: u32,
    ) -> Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        {
            // Filter bitmaps
            let mut bitmaps_table = write_txn
                .open_table(TABLE_BITMAPS)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            for &(field, value, bitmap) in filter_entries {
                let key = Self::make_key(field, value);
                let mut buf = Vec::with_capacity(bitmap.serialized_size());
                bitmap
                    .serialize_into(&mut buf)
                    .map_err(|e| BitdexError::DocStore(e.to_string()))?;
                bitmaps_table
                    .insert(key.as_str(), buf.as_slice())
                    .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            }
            drop(bitmaps_table);

            // Sort layers
            let mut sort_table = write_txn
                .open_table(TABLE_SORT_LAYERS)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            for &(field, layers) in sort_layers {
                for (i, bm) in layers.iter().enumerate() {
                    let key = format!("{}:{}", field, i);
                    let mut buf = Vec::with_capacity(bm.serialized_size());
                    bm.serialize_into(&mut buf)
                        .map_err(|e| BitdexError::DocStore(e.to_string()))?;
                    sort_table
                        .insert(key.as_str(), buf.as_slice())
                        .map_err(|e| BitdexError::DocStore(e.to_string()))?;
                }
            }
            drop(sort_table);

            // Meta: alive + slot counter
            let mut meta_table = write_txn
                .open_table(TABLE_META)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            let mut alive_buf = Vec::with_capacity(alive.serialized_size());
            alive
                .serialize_into(&mut alive_buf)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            meta_table
                .insert("__alive", alive_buf.as_slice())
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            meta_table
                .insert("__slot_counter", slot_counter.to_le_bytes().as_slice())
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        }
        write_txn
            .commit()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        Ok(())
    }

    /// Count total stored bitmaps (for metrics).
    pub fn bitmap_count(&self) -> Result<usize> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        let table = read_txn
            .open_table(TABLE_BITMAPS)
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        let count = table
            .len()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        Ok(count as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bitmap(values: &[u32]) -> RoaringBitmap {
        values.iter().copied().collect()
    }

    #[test]
    fn test_write_and_load_field() {
        let store = BitmapStore::open_temp().unwrap();

        let bm1 = make_bitmap(&[1, 2, 3]);
        let bm2 = make_bitmap(&[10, 20, 30]);

        store
            .write_batch(&[("tagIds", 42, &bm1), ("tagIds", 99, &bm2)])
            .unwrap();

        let loaded = store.load_field("tagIds").unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[&42], bm1);
        assert_eq!(loaded[&99], bm2);
    }

    #[test]
    fn test_write_batch() {
        let store = BitmapStore::open_temp().unwrap();

        let bm_a = make_bitmap(&[1, 5]);
        let bm_b = make_bitmap(&[10]);
        let bm_c = make_bitmap(&[100, 200]);

        store
            .write_batch(&[
                ("nsfwLevel", 1, &bm_a),
                ("nsfwLevel", 2, &bm_b),
                ("type", 0, &bm_c),
            ])
            .unwrap();

        let nsfw = store.load_field("nsfwLevel").unwrap();
        assert_eq!(nsfw.len(), 2);
        assert_eq!(nsfw[&1], bm_a);
        assert_eq!(nsfw[&2], bm_b);

        let typ = store.load_field("type").unwrap();
        assert_eq!(typ.len(), 1);
        assert_eq!(typ[&0], bm_c);
    }

    #[test]
    fn test_load_nonexistent_field() {
        let store = BitmapStore::open_temp().unwrap();
        let loaded = store.load_field("doesNotExist").unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_delete_field_value() {
        let store = BitmapStore::open_temp().unwrap();

        let bm1 = make_bitmap(&[1, 2, 3]);
        let bm2 = make_bitmap(&[4, 5, 6]);

        store
            .write_batch(&[("tagIds", 42, &bm1), ("tagIds", 99, &bm2)])
            .unwrap();

        store.delete_field_value("tagIds", 42).unwrap();

        let loaded = store.load_field("tagIds").unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(!loaded.contains_key(&42));
        assert_eq!(loaded[&99], bm2);
    }

    #[test]
    fn test_composite_key_format() {
        assert_eq!(BitmapStore::make_key("tagIds", 42), "tagIds:42");
        assert_eq!(BitmapStore::make_key("nsfwLevel", 0), "nsfwLevel:0");
        assert_eq!(
            BitmapStore::make_key("userId", 123456789),
            "userId:123456789"
        );
    }

    #[test]
    fn test_load_all_fields() {
        let store = BitmapStore::open_temp().unwrap();

        let bm1 = make_bitmap(&[1, 2]);
        let bm2 = make_bitmap(&[3, 4]);
        let bm3 = make_bitmap(&[5, 6]);

        store
            .write_batch(&[
                ("tagIds", 10, &bm1),
                ("nsfwLevel", 1, &bm2),
                ("type", 0, &bm3),
            ])
            .unwrap();

        let all = store
            .load_all_fields(&["tagIds", "nsfwLevel"])
            .unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all["tagIds"].len(), 1);
        assert_eq!(all["tagIds"][&10], bm1);
        assert_eq!(all["nsfwLevel"].len(), 1);
        assert_eq!(all["nsfwLevel"][&1], bm2);
        // "type" was not requested, should not appear
        assert!(!all.contains_key("type"));
    }

    #[test]
    fn test_bitmap_count() {
        let store = BitmapStore::open_temp().unwrap();
        assert_eq!(store.bitmap_count().unwrap(), 0);

        let bm = make_bitmap(&[1]);
        store
            .write_batch(&[("a", 1, &bm), ("b", 2, &bm), ("a", 3, &bm)])
            .unwrap();
        assert_eq!(store.bitmap_count().unwrap(), 3);
    }

    #[test]
    fn test_overwrite_bitmap() {
        let store = BitmapStore::open_temp().unwrap();

        let bm1 = make_bitmap(&[1, 2, 3]);
        store.write_batch(&[("tagIds", 42, &bm1)]).unwrap();

        let bm2 = make_bitmap(&[100, 200]);
        store.write_batch(&[("tagIds", 42, &bm2)]).unwrap();

        let loaded = store.load_field("tagIds").unwrap();
        assert_eq!(loaded[&42], bm2);
    }

    #[test]
    fn test_empty_batch() {
        let store = BitmapStore::open_temp().unwrap();
        store.write_batch(&[]).unwrap();
        assert_eq!(store.bitmap_count().unwrap(), 0);
    }

    #[test]
    fn test_new_with_tempdir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bitmaps.redb");
        let store = BitmapStore::new(&path).unwrap();

        let bm = make_bitmap(&[1, 2, 3]);
        store.write_batch(&[("tagIds", 42, &bm)]).unwrap();

        // Reopen and verify persistence
        drop(store);
        let store2 = BitmapStore::new(&path).unwrap();
        let loaded = store2.load_field("tagIds").unwrap();
        assert_eq!(loaded[&42], bm);
    }

    // A11: Write → close → reopen → verify all bitmaps survive.
    // Writes multiple bitmaps across fields, drops the store, reopens, and
    // verifies load_field() and load_all_fields() return the correct data.
    #[test]
    fn test_restart_write_close_reopen_verify() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bitmaps.redb");

        let bm_tag1 = make_bitmap(&[1, 2, 3, 100]);
        let bm_tag2 = make_bitmap(&[50, 60]);
        let bm_nsfw = make_bitmap(&[10, 20]);
        let bm_type = make_bitmap(&[5]);

        {
            let store = BitmapStore::new(&path).unwrap();
            store
                .write_batch(&[
                    ("tagIds", 10, &bm_tag1),
                    ("tagIds", 20, &bm_tag2),
                    ("nsfwLevel", 1, &bm_nsfw),
                    ("type", 0, &bm_type),
                ])
                .unwrap();
            // store dropped here — file closed
        }

        let store2 = BitmapStore::new(&path).unwrap();

        // Verify via load_field
        let tags = store2.load_field("tagIds").unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[&10], bm_tag1);
        assert_eq!(tags[&20], bm_tag2);

        let nsfw = store2.load_field("nsfwLevel").unwrap();
        assert_eq!(nsfw.len(), 1);
        assert_eq!(nsfw[&1], bm_nsfw);

        // Verify via load_all_fields
        let all = store2
            .load_all_fields(&["tagIds", "nsfwLevel", "type"])
            .unwrap();
        assert_eq!(all["tagIds"].len(), 2);
        assert_eq!(all["nsfwLevel"].len(), 1);
        assert_eq!(all["type"].len(), 1);
        assert_eq!(all["type"][&0], bm_type);
    }

    // A11: Multiple write batches → reopen — later writes win for overlapping keys.
    #[test]
    fn test_restart_multiple_batches_later_writes_win() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bitmaps.redb");

        let bm_v1 = make_bitmap(&[1, 2, 3]);
        let bm_v2 = make_bitmap(&[100, 200, 300]);
        let bm_other = make_bitmap(&[9, 8, 7]);

        {
            let store = BitmapStore::new(&path).unwrap();

            // Batch 1: tagIds:42 = v1, tagIds:99 = other
            store
                .write_batch(&[("tagIds", 42, &bm_v1), ("tagIds", 99, &bm_other)])
                .unwrap();

            // Batch 2: overwrite tagIds:42 with v2
            store.write_batch(&[("tagIds", 42, &bm_v2)]).unwrap();
            // store dropped
        }

        let store2 = BitmapStore::new(&path).unwrap();
        let tags = store2.load_field("tagIds").unwrap();
        assert_eq!(tags.len(), 2);
        // Later write (v2) wins for tagIds:42
        assert_eq!(tags[&42], bm_v2);
        // Unmodified entry survives
        assert_eq!(tags[&99], bm_other);
    }

    // A11: Delete → reopen → verify deletions persisted.
    #[test]
    fn test_restart_delete_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bitmaps.redb");

        let bm1 = make_bitmap(&[1, 2, 3]);
        let bm2 = make_bitmap(&[4, 5, 6]);
        let bm3 = make_bitmap(&[7, 8, 9]);

        {
            let store = BitmapStore::new(&path).unwrap();
            store
                .write_batch(&[
                    ("tagIds", 1, &bm1),
                    ("tagIds", 2, &bm2),
                    ("tagIds", 3, &bm3),
                ])
                .unwrap();

            // Delete tagIds:2
            store.delete_field_value("tagIds", 2).unwrap();
            // store dropped
        }

        let store2 = BitmapStore::new(&path).unwrap();
        let tags = store2.load_field("tagIds").unwrap();

        // Only 2 entries remain
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[&1], bm1);
        assert!(!tags.contains_key(&2), "deleted entry should not survive restart");
        assert_eq!(tags[&3], bm3);

        // load_single on the deleted key returns empty
        let deleted = store2.load_single("tagIds", 2).unwrap();
        assert!(deleted.is_empty());
    }

    // ---- S2.1: Alive, sort layer, and slot counter persistence tests ----

    #[test]
    fn test_alive_round_trip() {
        let store = BitmapStore::open_temp().unwrap();

        // No alive initially
        assert!(store.load_alive().unwrap().is_none());

        let alive = make_bitmap(&[1, 2, 5, 100, 9999]);
        store.write_alive(&alive).unwrap();

        let loaded = store.load_alive().unwrap().unwrap();
        assert_eq!(alive, loaded);
    }

    #[test]
    fn test_alive_overwrite() {
        let store = BitmapStore::open_temp().unwrap();

        let v1 = make_bitmap(&[1, 2, 3]);
        store.write_alive(&v1).unwrap();

        let v2 = make_bitmap(&[10, 20, 30]);
        store.write_alive(&v2).unwrap();

        let loaded = store.load_alive().unwrap().unwrap();
        assert_eq!(v2, loaded);
    }

    #[test]
    fn test_sort_layers_round_trip() {
        let store = BitmapStore::open_temp().unwrap();

        // No layers initially
        assert!(store.load_sort_layers("score", 32).unwrap().is_none());

        let l0 = make_bitmap(&[1, 3, 5]);
        let l1 = make_bitmap(&[2, 4]);
        let l2 = RoaringBitmap::new();
        let layers: Vec<&RoaringBitmap> = vec![&l0, &l1, &l2];

        store.write_sort_layers("score", &layers).unwrap();

        let loaded = store.load_sort_layers("score", 3).unwrap().unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0], l0);
        assert_eq!(loaded[1], l1);
        assert_eq!(loaded[2], l2);
    }

    #[test]
    fn test_sort_layers_multiple_fields() {
        let store = BitmapStore::open_temp().unwrap();

        let la = make_bitmap(&[1, 2]);
        let lb = make_bitmap(&[10, 20]);

        store.write_sort_layers("score", &[&la]).unwrap();
        store.write_sort_layers("date", &[&lb]).unwrap();

        let score = store.load_sort_layers("score", 1).unwrap().unwrap();
        assert_eq!(score[0], la);

        let date = store.load_sort_layers("date", 1).unwrap().unwrap();
        assert_eq!(date[0], lb);
    }

    #[test]
    fn test_slot_counter_round_trip() {
        let store = BitmapStore::open_temp().unwrap();

        // No counter initially
        assert!(store.load_slot_counter().unwrap().is_none());

        store.write_slot_counter(12345).unwrap();
        assert_eq!(store.load_slot_counter().unwrap().unwrap(), 12345);

        // Overwrite
        store.write_slot_counter(99999).unwrap();
        assert_eq!(store.load_slot_counter().unwrap().unwrap(), 99999);
    }

    #[test]
    fn test_full_snapshot_atomic() {
        let store = BitmapStore::open_temp().unwrap();

        let bm1 = make_bitmap(&[1, 2, 3]);
        let bm2 = make_bitmap(&[4, 5]);
        let alive = make_bitmap(&[1, 2, 3, 4, 5]);
        let sl0 = make_bitmap(&[1, 3, 5]);
        let sl1 = make_bitmap(&[2, 4]);
        let sort_refs: Vec<&RoaringBitmap> = vec![&sl0, &sl1];

        store
            .write_full_snapshot(
                &[("nsfwLevel", 1, &bm1), ("nsfwLevel", 2, &bm2)],
                &alive,
                &[("score", &sort_refs)],
                42,
            )
            .unwrap();

        // Verify all data
        let loaded_alive = store.load_alive().unwrap().unwrap();
        assert_eq!(loaded_alive, alive);

        let loaded_counter = store.load_slot_counter().unwrap().unwrap();
        assert_eq!(loaded_counter, 42);

        let loaded_filters = store.load_field("nsfwLevel").unwrap();
        assert_eq!(loaded_filters[&1], bm1);
        assert_eq!(loaded_filters[&2], bm2);

        let loaded_sort = store.load_sort_layers("score", 2).unwrap().unwrap();
        assert_eq!(loaded_sort[0], sl0);
        assert_eq!(loaded_sort[1], sl1);
    }

    #[test]
    fn test_full_snapshot_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bitmaps.redb");

        let bm = make_bitmap(&[1, 2, 3]);
        let alive = make_bitmap(&[1, 2, 3]);
        let sl = make_bitmap(&[1, 3]);
        let sort_refs: Vec<&RoaringBitmap> = vec![&sl];

        {
            let store = BitmapStore::new(&path).unwrap();
            store
                .write_full_snapshot(
                    &[("field", 10, &bm)],
                    &alive,
                    &[("sort", &sort_refs)],
                    100,
                )
                .unwrap();
        }

        let store2 = BitmapStore::new(&path).unwrap();
        assert_eq!(store2.load_alive().unwrap().unwrap(), alive);
        assert_eq!(store2.load_slot_counter().unwrap().unwrap(), 100);
        assert_eq!(store2.load_field("field").unwrap()[&10], bm);
        assert_eq!(store2.load_sort_layers("sort", 1).unwrap().unwrap()[0], sl);
    }
}
