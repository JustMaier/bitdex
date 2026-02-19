use serde::{Deserialize, Serialize};
use std::fmt;

/// A parsed query ready for execution by the bitmap engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitdexQuery {
    pub filters: Vec<FilterClause>,
    pub sort: Option<SortClause>,
    pub limit: usize,
    pub cursor: Option<CursorPosition>,
}

/// A filter clause representing a predicate on indexed data.
///
/// The engine evaluates these against roaring bitmaps:
/// - `Eq`/`NotEq`/`In`/`Gt`/`Lt`/`Gte`/`Lte` operate on individual field bitmaps
/// - `And`/`Or`/`Not` compose sub-clauses via bitmap intersection/union/complement
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FilterClause {
    Eq(String, Value),
    NotEq(String, Value),
    In(String, Vec<Value>),
    Gt(String, Value),
    Lt(String, Value),
    Gte(String, Value),
    Lte(String, Value),
    Not(Box<FilterClause>),
    And(Vec<FilterClause>),
    Or(Vec<FilterClause>),
}

/// A dynamically-typed value used in filter predicates and cursors.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Integer(i64),
    Float(f64),
    Bool(bool),
    String(String),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Integer(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::Bool(v) => write!(f, "{v}"),
            Value::String(v) => write!(f, "{v}"),
        }
    }
}

/// Sort specification for query results.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SortClause {
    pub field: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// Cursor position for keyset pagination.
///
/// The cursor encodes the last seen sort value and slot ID, enabling
/// the engine to resume traversal from the exact position.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CursorPosition {
    pub sort_value: u64,
    pub slot_id: u32,
}

/// Errors produced by query parsers.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct ParseError {
    pub message: String,
}

impl ParseError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Trait for query parsers that convert raw input into `BitdexQuery`.
///
/// V2 ships with a JSON parser. Future plugins (OpenSearch DSL,
/// Meilisearch syntax) will implement this same trait.
pub trait QueryParser: Send + Sync {
    fn parse(&self, input: &[u8]) -> Result<BitdexQuery, ParseError>;
    fn content_type(&self) -> &str;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_clause_construction() {
        let clause = FilterClause::And(vec![
            FilterClause::Eq("nsfwLevel".into(), Value::Integer(1)),
            FilterClause::Or(vec![
                FilterClause::Eq("type".into(), Value::String("image".into())),
                FilterClause::Eq("type".into(), Value::String("video".into())),
            ]),
        ]);
        match clause {
            FilterClause::And(clauses) => assert_eq!(clauses.len(), 2),
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn test_value_display() {
        assert_eq!(Value::Integer(42).to_string(), "42");
        assert_eq!(Value::Bool(true).to_string(), "true");
        assert_eq!(Value::String("hello".into()).to_string(), "hello");
    }

    #[test]
    fn test_parse_error_display() {
        let err = ParseError::new("bad input");
        assert_eq!(err.to_string(), "bad input");
    }

    #[test]
    fn test_bitdex_query_serde_roundtrip() {
        let query = BitdexQuery {
            filters: vec![FilterClause::Eq("x".into(), Value::Integer(1))],
            sort: Some(SortClause {
                field: "score".into(),
                direction: SortDirection::Desc,
            }),
            limit: 50,
            cursor: Some(CursorPosition {
                sort_value: 100,
                slot_id: 42,
            }),
        };
        let json = serde_json::to_string(&query).unwrap();
        let roundtrip: BitdexQuery = serde_json::from_str(&json).unwrap();
        assert_eq!(roundtrip.limit, 50);
        assert_eq!(roundtrip.cursor.unwrap().slot_id, 42);
    }
}
