// Re-export shared types from query module for convenience.
// The canonical definitions live in query.rs (built by parser-config agent).
// Core engine modules should import from crate::query directly.

pub use crate::query::{
    BitdexQuery, CursorPosition, FilterClause, SortClause, SortDirection, Value,
};

// -- Value conversion helpers for core engine use --

impl Value {
    /// Convert to a u64 bitmap key for filter bitmap indexing.
    /// Returns None for values that can't be used as bitmap keys.
    pub fn as_bitmap_key(&self) -> Option<u64> {
        match self {
            Value::Bool(b) => Some(if *b { 1 } else { 0 }),
            Value::Integer(v) => Some(*v as u64),
            Value::Float(_) | Value::String(_) => None,
        }
    }

    /// Convert to a u32 for sort layer bit decomposition.
    pub fn as_sort_u32(&self) -> Option<u32> {
        match self {
            Value::Integer(v) => Some(*v as u32),
            _ => None,
        }
    }
}

/// The result of a query: an ordered list of slot IDs (which ARE the Postgres IDs).
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub ids: Vec<i64>,
    pub cursor: Option<CursorPosition>,
    pub total_matched: u64,
}
