use std::collections::HashMap;
use std::path::Path;

use redb::{Database, TableDefinition};

use crate::error::{BitdexError, Result};
use crate::mutation::FieldValue;

const DOCS_TABLE: TableDefinition<u32, &[u8]> = TableDefinition::new("docs");

/// A stored document containing all field values needed for bitmap diffing on upsert.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredDoc {
    pub fields: HashMap<String, FieldValue>,
}

/// On-disk document store backed by redb.
///
/// Stores documents keyed by slot ID. Used to:
/// 1. Enable targeted bitmap updates on upsert (diff old vs new field values)
/// 2. Optionally serve document content alongside query results
pub struct DocStore {
    db: Database,
}

impl DocStore {
    /// Open a docstore at the given path. Creates the file if it doesn't exist.
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::create(path).map_err(|e| BitdexError::DocStore(e.to_string()))?;
        // Ensure the table exists by doing an empty write txn
        let write_txn = db
            .begin_write()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        {
            let _table = write_txn
                .open_table(DOCS_TABLE)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        }
        write_txn
            .commit()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        Ok(Self { db })
    }

    /// Open a docstore using a temporary file (for testing).
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
        }
        write_txn
            .commit()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        Ok(Self { db })
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
                let doc: StoredDoc = bincode::deserialize(data.value())
                    .map_err(|e| BitdexError::DocStore(e.to_string()))?;
                Ok(Some(doc))
            }
            None => Ok(None),
        }
    }

    /// Store a document at the given slot ID.
    pub fn put(&self, id: u32, doc: &StoredDoc) -> Result<()> {
        let bytes =
            bincode::serialize(doc).map_err(|e| BitdexError::DocStore(e.to_string()))?;
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
    pub fn put_batch(&self, docs: &[(u32, StoredDoc)]) -> Result<()> {
        if docs.is_empty() {
            return Ok(());
        }
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| BitdexError::DocStore(e.to_string()))?;
        {
            let mut table = write_txn
                .open_table(DOCS_TABLE)
                .map_err(|e| BitdexError::DocStore(e.to_string()))?;
            for (id, doc) in docs {
                let bytes =
                    bincode::serialize(doc).map_err(|e| BitdexError::DocStore(e.to_string()))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Value;

    #[test]
    fn test_put_and_get() {
        let store = DocStore::open_temp().unwrap();
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
    }

    #[test]
    fn test_get_missing() {
        let store = DocStore::open_temp().unwrap();
        assert!(store.get(999).unwrap().is_none());
    }

    #[test]
    fn test_delete() {
        let store = DocStore::open_temp().unwrap();
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
        let store = DocStore::open_temp().unwrap();
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
}
